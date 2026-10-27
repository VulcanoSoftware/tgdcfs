import datetime
import logging
from typing import (
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)
from uuid import uuid4 as uuid

from tgdcfs.backends.base import IStore
from tgdcfs.core.mirror import MirrorGroup
from tgdcfs.core.model import TGFSDirectory, TGFSFileDesc, TGFSFileRef, TGFSFileVersion
from tgdcfs.core.repository.interface import FDRepositoryResp
from tgdcfs.errors import FileOrDirectoryDoesNotExist
from tgdcfs.reqres import (
    FileContent,
    FileMessage,
    FileMessageEmpty,
)

from .file_desc import FileDescApi
from .metadata import MetaDataApi

logger = logging.getLogger(__name__)

# {mirror store key -> message ids to delete there}
MirrorMessageIds = Dict[str, List[int]]


def _merge_mirror_ids(target: MirrorMessageIds, source: Dict[str, int]) -> None:
    for channel_key, mid in source.items():
        if mid > 0:
            target.setdefault(channel_key, []).append(mid)


class FileApi:
    def __init__(
        self,
        metadata_api: MetaDataApi,
        file_desc_api: FileDescApi,
        message_api: IStore,
        mirror_group: Optional[MirrorGroup] = None,
        inline_mirroring: bool = True,
        on_written: Optional[Callable[[TGFSFileRef], Awaitable[None]]] = None,
        written_gaps_only: bool = False,
    ):
        self._metadata_api = metadata_api
        self._file_desc_api = file_desc_api
        self._message_api = message_api
        self._mirror_group = mirror_group
        self._inline_mirroring = inline_mirroring
        # Called after a file's content changed and the metadata is
        # pushed; the replication queue hooks in here for
        # ``sync: background``.
        self._on_written = on_written
        # ``sync: tee``: the write itself filled the mirrors that re-upload,
        # so the queue is only told about a file some store still lacks a
        # copy of (a mirror that failed mid-stream, a server-side copy
        # that was refused, a copy of a file made without a stream).
        self._written_gaps_only = written_gaps_only

    async def _written(self, fr: TGFSFileRef, fd: Optional[TGFSFileDesc]) -> None:
        if self._on_written is None:
            return
        if self._written_gaps_only and fd is not None and self._mirror_group:
            if not any(
                self._mirror_group.missing_stores(version)
                for version in fd.get_versions()
                if version.is_valid()
            ):
                return
        await self._on_written(fr)

    async def collect_message_ids(self, fr: TGFSFileRef) -> List[int]:
        """Return every primary-store message id backing ``fr``.

        Includes the file descriptor message itself plus every content
        message across all known versions. Returns just the descriptor
        id if the descriptor can no longer be read (it may already be
        gone), so callers can still try to delete that one message.
        """
        primary, _ = await self.collect_all_message_ids(fr)
        return primary

    async def collect_all_message_ids(
        self, fr: TGFSFileRef, version_ids: Optional[List[str]] = None
    ) -> Tuple[List[int], MirrorMessageIds]:
        """Like :meth:`collect_message_ids`, but also returns the message
        ids of every mirrored copy, grouped by mirror store. With
        ``version_ids`` given, the ids of the versions seen are appended
        to it (for dropping their local cache entries)."""
        ids: List[int] = []
        mirror_ids: MirrorMessageIds = {}
        try:
            fd = await self._file_desc_api.get_file_desc(fr)
        except Exception as ex:
            logger.warning(
                f"Could not load file descriptor for {fr.name} "
                f"(message_id={fr.message_id}): {ex}"
            )
            fd = None
        # After get_file_desc the ref is relocated, so its ids are sorted
        # into the right store below.
        self._collect_ref_ids(fr, ids, mirror_ids)
        if fd is None:
            return ids, mirror_ids
        for version in fd.get_versions():
            self._collect_version_ids(version, ids, mirror_ids)
            if version_ids is not None:
                version_ids.append(version.id)
        return ids, mirror_ids

    @property
    def _store_key(self) -> str:
        return self._message_api.key

    def _collect_ref_ids(
        self, fr: TGFSFileRef, ids: List[int], mirror_ids: MirrorMessageIds
    ) -> None:
        """Sort the descriptor id into the primary or the store it belongs to.

        An id that belongs to a former primary must never be deleted
        from the current one: it names an unrelated message there.
        """
        if fr.message_id > 0:
            if fr.owned_by(self._store_key):
                ids.append(fr.message_id)
            elif fr.store:
                mirror_ids.setdefault(fr.store, []).append(fr.message_id)
        _merge_mirror_ids(mirror_ids, fr.mirrors)

    def _collect_version_ids(
        self, version: TGFSFileVersion, ids: List[int], mirror_ids: MirrorMessageIds
    ) -> None:
        owned = [mid for mid in version.message_ids if mid > 0]
        if version.owned_by(self._store_key):
            ids.extend(owned)
        elif version.store:
            mirror_ids.setdefault(version.store, []).extend(owned)
        for channel_key, version_mirror_ids in version.mirrors.items():
            mirror_ids.setdefault(channel_key, []).extend(
                mid for mid in version_mirror_ids if mid > 0
            )
        for channel_key, replica in version.replicas.items():
            target = (
                ids
                if channel_key == self._store_key
                else (mirror_ids.setdefault(channel_key, []))
            )
            target.extend(mid for mid in replica.message_ids if mid > 0)

    async def _collect_version_message_ids(
        self, fr: TGFSFileRef, version_id: str
    ) -> Tuple[List[int], MirrorMessageIds]:
        try:
            fd = await self._file_desc_api.get_file_desc(fr)
            version = fd.get_version(version_id)
        except Exception as ex:
            logger.warning(f"Could not load version {version_id} of {fr.name}: {ex}")
            return [], {}
        ids: List[int] = []
        mirror_ids: MirrorMessageIds = {}
        self._collect_version_ids(version, ids, mirror_ids)
        return ids, mirror_ids

    async def delete_mirrored(
        self, mirror_ids: MirrorMessageIds, force: bool = False
    ) -> None:
        if self._mirror_group and mirror_ids:
            await self._mirror_group.delete(mirror_ids, force=force)

    @classmethod
    def _iter_file_refs(cls, directory: TGFSDirectory) -> Iterator[TGFSFileRef]:
        yield from directory.find_files()
        for child in directory.find_dirs():
            yield from cls._iter_file_refs(child)

    def _descriptors_referenced_elsewhere(
        self, removing: Iterable[TGFSFileRef]
    ) -> Set[int]:
        """Descriptor message ids that survive removing ``removing``.

        Copies used to be made by pointing a second file ref at the *same*
        descriptor message, so both files were backed by one set of Telegram
        messages. Removing either of them would then leave the other one
        pointing at messages that no longer exist -- reported by WebDAV as a
        0-byte file. New copies own their content, but metadata written
        before that still holds such pairs, so removals keep checking.
        """
        removing_ids = {id(fr) for fr in removing}
        return {
            fr.message_id
            for fr in self._iter_file_refs(self._metadata_api.get_root_directory())
            if id(fr) not in removing_ids and fr.message_id > 0
        }

    async def collect_deletable_message_ids(
        self, frs: Sequence[TGFSFileRef], version_ids: Optional[List[str]] = None
    ) -> Tuple[List[int], MirrorMessageIds]:
        """Message ids that become garbage once ``frs`` are removed.

        Refs whose descriptor is still referenced by a file outside ``frs``
        contribute nothing: their messages are shared and must stay.
        ``version_ids`` collects the ids of the versions that go with them.
        """
        shared = self._descriptors_referenced_elsewhere(frs)
        ids: List[int] = []
        mirror_ids: MirrorMessageIds = {}
        for fr in frs:
            if fr.message_id in shared:
                logger.info(
                    f"Keeping the telegram messages of {fr.name} "
                    f"(descriptor {fr.message_id}): another file still refers to them"
                )
                continue
            fr_ids, fr_mirror_ids = await self.collect_all_message_ids(fr, version_ids)
            ids.extend(fr_ids)
            for channel_key, channel_ids in fr_mirror_ids.items():
                mirror_ids.setdefault(channel_key, []).extend(channel_ids)
        return ids, mirror_ids

    async def _duplicate_versions(self, fd: TGFSFileDesc) -> TGFSFileDesc:
        """Build a descriptor whose versions own fresh content messages.

        Every part of every version is duplicated inside the channel in one
        server-side call, so the copy costs the same whether the file is a
        kilobyte or a hundred gigabytes. Timestamps and part sizes carry
        over, so the copy keeps the full history; the versions get fresh ids
        because two files that are meant to be independent should not share
        an identifier with each other.
        """
        versions = fd.get_versions(sort=True)
        parts = [mid for version in versions for mid in version.message_ids]
        new_parts = await self._message_api.copy_within(parts)

        copied = TGFSFileDesc(name=fd.name)
        version_ids: Dict[str, str] = {}
        offset = 0
        for version in versions:
            n = len(version.message_ids)
            version_ids[version.id] = str(uuid())
            new_version = TGFSFileVersion(
                id=version_ids[version.id],
                updated_at=version.updated_at,
                message_ids=new_parts[offset : offset + n],
                part_sizes=list(version.part_sizes),
                store=self._store_key,
            )
            # Copies are mirrored version by version: a replica covers one
            # version's bytes, not the whole file's.
            if self._mirror_group and self._inline_mirroring and n:
                copies = await self._mirror_group.mirror_parts(
                    new_version.message_ids, new_version.part_sizes or None
                )
                copies.apply_to(new_version)
            copied.add_version(new_version)
            offset += n
        # add_version derives created_at from the versions it is given; the
        # copy should carry the original's creation date instead.
        copied.created_at = fd.created_at
        if latest := version_ids.get(fd.latest_version_id):
            copied.latest_version_id = latest
        return copied

    async def copy(
        self, where: TGFSDirectory, fr: TGFSFileRef, name: Optional[str] = None
    ) -> TGFSFileRef:
        """Copy a file without moving a single byte of its content.

        The copy gets its own descriptor and its own content messages, so
        writing to it -- or deleting it -- leaves the original alone.
        """
        fd = await self._file_desc_api.get_file_desc(fr, include_all_versions=True)
        copied_fd = await self._duplicate_versions(fd)
        resp: Optional[FDRepositoryResp] = None
        try:
            resp = await self._file_desc_api.save_new_file_desc(copied_fd)
            copied_fr = where.create_file_ref(name or fr.name, resp.message_id)
        except Exception:
            await self._discard_orphans(copied_fd, resp)
            raise
        copied_fr.mirrors = dict(resp.mirrors)
        copied_fr.store = resp.store
        await self._metadata_api.push()
        await self._written(copied_fr, copied_fd)
        return copied_fr

    async def _discard_orphans(
        self, fd: TGFSFileDesc, resp: Optional[FDRepositoryResp] = None
    ) -> None:
        """Drop the messages of a copy that never made it into the metadata.

        Nothing refers to them, and they were created moments ago by this
        very call, so they are cleaned up regardless of the user's setting
        for keeping removed file messages.
        """
        message_ids: List[int] = []
        mirror_ids: MirrorMessageIds = {}
        if resp is not None:
            message_ids.append(resp.message_id)
            _merge_mirror_ids(mirror_ids, resp.mirrors)
        for version in fd.get_versions():
            self._collect_version_ids(version, message_ids, mirror_ids)
        try:
            await self._message_api.delete_messages(message_ids, force=True)
            await self.delete_mirrored(mirror_ids, force=True)
        except Exception as ex:
            logger.warning(f"Could not clean up after a failed copy: {ex}")

    async def move(
        self, fr: TGFSFileRef, where: TGFSDirectory, name: Optional[str] = None
    ) -> TGFSFileRef:
        """Relocate ``fr`` without re-uploading or deleting anything.

        Implementing a move as copy-then-remove would delete the very
        messages the copy points at, so the moved file has to be relocated in
        the metadata instead.
        """
        moved_fr = fr.location.relocate_file_ref(fr, where, name)
        await self._metadata_api.push()
        return moved_fr

    async def _create_new_file(
        self, where: TGFSDirectory, file_msg: FileMessage
    ) -> TGFSFileDesc:
        resp = await self._file_desc_api.create_file_desc(file_msg)
        fr = where.create_file_ref(file_msg.name, resp.message_id)
        fr.mirrors = dict(resp.mirrors)
        fr.store = resp.store
        await self._metadata_api.push()
        await self._written(fr, resp.fd)
        return resp.fd

    async def _sync_file_ref(self, fr: TGFSFileRef, resp: FDRepositoryResp) -> None:
        """
        Sync the file ref with the descriptor's current location: the primary
        message_id may change if the original message went missing (e.g. it was
        manually deleted), and the mirror map changes whenever mirror copies
        are (re)written.
        """
        if (
            fr.message_id != resp.message_id
            or fr.mirrors != resp.mirrors
            or fr.store != resp.store
        ):
            fr.message_id = resp.message_id
            fr.mirrors = dict(resp.mirrors)
            fr.store = resp.store
            await self._metadata_api.push()

    async def _update_existing_file(
        self, fr: TGFSFileRef, file_msg: FileMessage, version_id: Optional[str]
    ) -> TGFSFileDesc:
        if version_id:
            resp = await self._file_desc_api.update_file_version(
                fr, file_msg, version_id
            )
        else:
            resp = await self._file_desc_api.append_file_version(file_msg, fr)
        await self._sync_file_ref(fr, resp)
        await self._written(fr, resp.fd)
        return resp.fd

    async def set_last_modified(
        self, fr: TGFSFileRef, when: datetime.datetime
    ) -> TGFSFileDesc:
        """Give ``fr``'s latest version the modification time ``when``.

        WebDAV PROPPATCH after an upload; the descriptor message changes,
        the metadata only when the descriptor moved.
        """
        resp = await self._file_desc_api.set_last_modified(fr, when)
        await self._sync_file_ref(fr, resp)
        return resp.fd

    async def rm(self, fr: TGFSFileRef, version_id: Optional[str] = None) -> None:
        if not version_id:
            version_ids: List[str] = []
            message_ids, mirror_ids = await self.collect_deletable_message_ids(
                [fr], version_ids
            )
            fr.delete()
            await self._metadata_api.push()
            await self._message_api.delete_messages(message_ids)
            await self.delete_mirrored(mirror_ids)
            if version_ids:
                await self._file_desc_api.forget_versions(version_ids)
        else:
            message_ids, mirror_ids = await self._collect_version_message_ids(
                fr, version_id
            )
            resp = await self._file_desc_api.delete_file_version(fr, version_id)
            await self._sync_file_ref(fr, resp)
            await self._message_api.delete_messages(message_ids)
            await self.delete_mirrored(mirror_ids)

    async def upload(
        self,
        under: TGFSDirectory,
        file_msg: FileMessage,
        version_id: Optional[str] = None,
    ) -> TGFSFileDesc:
        try:
            fr = under.find_file(file_msg.name)
            return await self._update_existing_file(fr, file_msg, version_id)
        except FileOrDirectoryDoesNotExist:
            return await self._create_new_file(under, file_msg)

    async def desc(
        self,
        fr: TGFSFileRef,
        include_all_versions: bool = False,
        validate: bool = True,
    ) -> TGFSFileDesc:
        return await self._file_desc_api.get_file_desc(
            fr, include_all_versions, validate=validate
        )

    async def retrieve(
        self,
        fr: TGFSFileRef,
        begin: int,
        end: int,
        as_name: str,
    ) -> FileContent:
        fd = await self.desc(fr)
        if isinstance(fd, FileMessageEmpty):

            async def empty_file() -> FileContent:
                yield b""

            return empty_file()
        fv = fd.get_latest_version()

        async def chunks():
            try:
                async for chunk in await self._file_desc_api.download_file_at_version(
                    fv, begin, end, as_name or fr.name
                ):
                    yield chunk

            except Exception as ex:
                raise ex

        return chunks()

    async def retrieve_version(
        self,
        fv: TGFSFileVersion,
        begin: int,
        end: int,
        as_name: str,
    ) -> FileContent:
        return await self._file_desc_api.download_file_at_version(
            fv,
            begin,
            end,
            as_name,
        )
