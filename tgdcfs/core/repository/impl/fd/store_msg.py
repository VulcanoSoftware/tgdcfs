import json
import logging
from itertools import chain
from typing import Callable, Dict, List, Optional, Tuple

from tgdcfs.backends.base import IStore
from tgdcfs.core.mirror import MirrorGroup
from tgdcfs.core.model import TGFSFileDesc, TGFSFileRef, TGFSFileVersion
from tgdcfs.core.model.file import INVALID_FILE_SIZE
from tgdcfs.core.repository.interface import (
    FDRepositoryResp,
    IFDRepository,
)
from tgdcfs.errors import MessageNotFound

logger = logging.getLogger(__name__)


class StoreFDRepository(IFDRepository):
    """File descriptors as JSON text messages in a store.

    Backend-agnostic; mirror copies of the descriptor are kept by the
    :class:`MirrorGroup` and read back when the primary copy is gone.
    """

    def __init__(
        self,
        store: IStore,
        mirror_group: Optional[MirrorGroup] = None,
        local_versions: Optional[Callable[[str], bool]] = None,
    ):
        self._message_api = store
        self._mirror_group = mirror_group
        # Whether this instance's local cache holds a pending version in
        # full (``write_ack: cache``). Such a version is readable here and
        # nowhere else.
        self._local_versions = local_versions

    def _readable_locally(self, version: TGFSFileVersion) -> bool:
        return (
            version.pending
            and self._local_versions is not None
            and self._local_versions(version.id)
        )

    @property
    def key(self) -> str:
        return self._message_api.key

    def _stamp(self, fd: TGFSFileDesc) -> None:
        """Record which store the versions' ids belong to.

        Versions without a store were written by tgfs or duplicated into
        this primary a moment ago; either way their ids are this store's.
        Versions relocated from a former primary keep their store.
        """
        for version in fd.get_versions():
            if version.store is None and version.is_valid():
                version.store = self.key

    async def save(
        self, fd: TGFSFileDesc, fr: Optional[TGFSFileRef] = None
    ) -> FDRepositoryResp:
        self._stamp(fd)

        # If file_content referer is None, create a new file_content descriptor message.
        if fr is None:
            return FDRepositoryResp(
                message_id=await self._message_api.send_text(fd.to_json()),
                fd=fd,
                mirrors=await self._mirror_fd(fd, existing=None),
                store=self.key,
            )

        # A descriptor that lives in a former primary (the mirror was
        # promoted) must not be edited by id here -- the id names some
        # other message in this store. Send a fresh copy and keep the old
        # one as that store's mirror entry.
        if not fr.owned_by(self.key):
            existing = dict(fr.mirrors)
            if fr.store:
                existing.setdefault(fr.store, fr.message_id)
            return FDRepositoryResp(
                message_id=await self._message_api.send_text(fd.to_json()),
                fd=fd,
                mirrors=await self._mirror_fd(fd, existing=existing),
                store=self.key,
            )

        # If file_content referer is provided, try to update the existing file_content descriptor.
        # But if the message is not found (probably got deleted manually), a new file_content descriptor will be created.
        try:
            return FDRepositoryResp(
                message_id=await self._message_api.edit_message_text(
                    message_id=fr.message_id, message=fd.to_json()
                ),
                fd=fd,
                mirrors=await self._mirror_fd(fd, existing=fr.mirrors),
                store=self.key,
            )
        except MessageNotFound:
            return await self.save(fd)

    async def _mirror_fd(
        self, fd: TGFSFileDesc, existing: Optional[Dict[str, int]]
    ) -> Dict[str, int]:
        """Keep a copy of the FD text message in every mirror store.

        Descriptors are sent as fresh text messages (not forwarded)
        because they are edited on every new version and a forwarded
        message cannot be edited in the target channel.
        """
        if not self._mirror_group:
            return dict(existing or {})
        return await self._mirror_group.mirror_fd(fd.to_json(), existing)

    async def _lookup_mirror_parts(
        self, missing: List[Tuple[str, int]]
    ) -> Dict[Tuple[str, int], int]:
        """Fetch mirror copies of content messages, batched per channel.

        ``missing`` is a list of (store_key, message_id) pairs; the
        result maps each found pair to the document size of the copy.
        """
        res: Dict[Tuple[str, int], int] = {}
        if not self._mirror_group:
            return res
        by_channel: Dict[str, List[int]] = {}
        for store_key, mid in missing:
            by_channel.setdefault(store_key, []).append(mid)
        for store_key, mids in by_channel.items():
            if (api := self._mirror_group.store_for(store_key)) is None:
                continue
            try:
                messages = await api.get_messages(mids)
            except Exception as ex:
                logger.warning(
                    f"Could not check mirror store {store_key} for "
                    f"messages {mids}: {ex}"
                )
                continue
            for mid, message in zip(mids, messages):
                if message and message.document:
                    res[(store_key, mid)] = message.document.size
        return res

    async def _verify_replicas(self, version: TGFSFileVersion) -> bool:
        """Whether some replica of ``version`` is complete; fills in the
        replica's part sizes when they were not recorded."""
        if not self._mirror_group:
            return False
        complete = False
        for store_key, replica in version.replicas.items():
            store = self._mirror_group.store_for(store_key)
            if store is None or not replica.message_ids:
                continue
            try:
                messages = await store.get_messages(replica.message_ids)
            except Exception as ex:
                logger.warning(
                    f"Could not check replica in store {store_key} for "
                    f"version {version.id}: {ex}"
                )
                continue
            if any(m is None or m.document is None for m in messages):
                continue
            replica.part_sizes = [m.document.size for m in messages]  # type: ignore[union-attr]
            complete = True
        return complete

    async def _validate_fv(
        self, fd: TGFSFileDesc, include_all_versions: bool
    ) -> TGFSFileDesc:
        versions = fd.get_versions(exclude_invalid=True)

        # Files in the channel may be deleted manually, so we need to check if the messages for the versions exist.
        # Only ids that belong to this store are looked up here; a version
        # relocated from a former primary is checked against the mirrors.

        all_ids = list(
            chain(
                *(
                    version.message_ids
                    for version in versions
                    if version.owned_by(self.key)
                )
            )
        )
        try:
            file_messages = await self._message_api.get_messages(all_ids)
        except Exception as ex:
            # An unreachable primary (banned channel, network down) must
            # not make every file look invalid while a mirror has it: treat
            # every part as missing here and let the mirror lookup below
            # decide.
            if not self._mirror_group:
                raise
            logger.warning(
                f"Could not check the primary store for the parts of {fd.name}: "
                f"{ex}; checking the mirrors"
            )
            self._mirror_group.mark_primary_dead()
            file_messages = [None] * len(all_ids)

        message_map = {msg.message_id: msg for msg in file_messages if msg}

        has_valid_version = False

        for i, version in enumerate(versions):
            owned = version.owned_by(self.key)
            version.part_sizes = []
            primary_layout_ok = True
            for j, message_id in enumerate(version.message_ids):
                if (
                    owned
                    and (file_message := message_map.get(message_id, None))
                    and file_message.document
                ):
                    version.part_sizes.append(file_message.document.size)
                    continue

                # The primary copy of this part is gone -- before declaring
                # the version invalid, check whether a mirror still has it.
                mirror_candidates = [
                    (store_key, mirror_ids[j])
                    for store_key, mirror_ids in version.mirrors.items()
                    if j < len(mirror_ids) and mirror_ids[j] > 0
                ]
                mirror_sizes = await self._lookup_mirror_parts(mirror_candidates)
                if mirror_sizes:
                    logger.warning(
                        f"File message {message_id} for part {j + 1} of "
                        f"{fd.name}@{version.id} not found in the primary "
                        f"store, serving from mirror"
                    )
                    version.part_sizes.append(next(iter(mirror_sizes.values())))
                    continue

                primary_layout_ok = False
                break

            if not primary_layout_ok:
                # Neither the primary nor an aligned mirror has every part.
                # A replica with its own layout still makes the version
                # readable; the primary layout is marked unusable by
                # leaving its part sizes empty.
                if await self._verify_replicas(version):
                    logger.warning(
                        f"{fd.name}@{version.id} has no complete aligned copy, "
                        f"serving from a replica"
                    )
                    version.part_sizes = []
                    version._size = INVALID_FILE_SIZE
                else:
                    logger.warning(
                        f"File messages of {fd.name}@{version.id} not found in any store"
                    )
                    version.set_invalid()
            if version.is_valid():
                has_valid_version = True
                if not include_all_versions:
                    # Found a valid version, no need to check further
                    break

        # A pending version is readable on the instance whose cache holds
        # it; elsewhere it counts as invalid, like a version whose parts
        # are gone. The version served is the latest one, so when that one
        # is unreadable the pointer moves to the newest readable version
        # (in memory only: the pointer is derived, never written). A
        # readable latest version is left alone, ties and all.
        def readable(version: TGFSFileVersion) -> bool:
            return version.is_valid() or self._readable_locally(version)

        if any(self._readable_locally(v) for v in fd.get_versions()):
            has_valid_version = True
        current = fd.versions.get(fd.latest_version_id)
        if current is None or not readable(current):
            candidates = [v for v in fd.versions.values() if readable(v)]
            if candidates:
                # ``max`` keeps the first of equally dated versions, as
                # ``from_dict`` does when it derives the pointer.
                fd.latest_version_id = max(
                    candidates, key=lambda v: v.updated_at_timestamp
                ).id

        return fd if has_valid_version else TGFSFileDesc.empty(fd.name)

    async def _get_fd_text(self, fr: TGFSFileRef) -> Optional[str]:
        """Read the FD JSON, falling back to mirror copies if needed."""
        message = None
        try:
            if fr.owned_by(self.key):
                message = (await self._message_api.get_messages([fr.message_id]))[0]
        except Exception as ex:
            logger.warning(
                f"Could not read file descriptor {fr.message_id} for "
                f"{fr.name} from the primary store: {ex}"
            )
            message = None
            if self._mirror_group:
                self._mirror_group.mark_primary_dead()

        if message and message.text:
            return message.text

        if not self._mirror_group:
            return None

        for store_key, mirror_mid in fr.mirrors.items():
            if (api := self._mirror_group.store_for(store_key)) is None:
                continue
            try:
                mirror_message = (await api.get_messages([mirror_mid]))[0]
            except Exception as ex:
                logger.warning(
                    f"Could not read FD mirror {mirror_mid} in store "
                    f"{store_key} for {fr.name}: {ex}"
                )
                continue
            if mirror_message and mirror_message.text:
                logger.warning(
                    f"Serving file descriptor for {fr.name} from mirror "
                    f"store {store_key}"
                )
                return mirror_message.text
        return None

    async def get(
        self,
        fr: TGFSFileRef,
        include_all_versions: bool = False,
        validate: bool = True,
    ) -> TGFSFileDesc:
        # After a promotion the ref may still describe the former primary;
        # re-express it relative to this store before touching anything.
        fr.relocate(self.key)
        text = await self._get_fd_text(fr)

        if not text:
            logging.error(
                f"File descriptor (message_id: {fr.message_id}) for {fr.name} not found"
            )
            return TGFSFileDesc.empty(fr.name)

        fd = TGFSFileDesc.from_dict(json.loads(text), name=fr.name)
        for version in fd.get_versions():
            version.relocate(self.key)
        if not validate:
            return fd
        return await self._validate_fv(fd, include_all_versions)
