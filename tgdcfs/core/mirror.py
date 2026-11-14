"""Redundancy: RAID-1-style mirroring of a file system's primary store.

A :class:`MirrorGroup` binds the primary :class:`IStore` to one store
per mirror and provides the write-side primitives used by the
repositories:

* ``mirror_parts`` copies a version's content into every mirror store.
  A store whose messages can hold the primary's parts receives an
  *aligned* copy (one message per part, recorded in ``mirrors``):
  server-side when the pair of stores supports it (Telegram forwarding,
  no re-upload bandwidth), otherwise by streaming each part down and up
  again. A store whose messages are smaller than the primary's parts (a
  Discord mirror of a Telegram primary) receives a *replica*: the whole
  version streamed through the store's own partitioning, recorded in
  ``replicas`` with its own part layout.
* ``copy_into_primary`` gives a version that still lives in a former
  primary (the mirror was promoted) a copy in the current primary.
* ``mirror_fd`` keeps a copy of a file descriptor (JSON text message)
  in each mirror store. Descriptors are *sent* rather than copied
  because a forwarded message cannot be edited later, and descriptors
  are edited on every new file version.
* ``mirror_pinned`` maintains a copy of the pinned metadata document.
* ``delete`` fans message deletion out to the mirror stores.

Copy strategy (``mode``): ``auto`` (default) tries the server-side copy
and falls back to re-uploading; ``forward`` insists on the server-side
copy and fails otherwise; ``reupload`` never asks for one (for channels
with "restrict saving content", where forwarding is refused).

Failure policy: with ``strict=False`` (default) a failing mirror write
is logged and the affected store is simply omitted from the result --
the primary write has already succeeded and the backfill task or the
replication queue can close the gap later. With ``strict=True`` the
error propagates.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AsyncGenerator, Dict, List, Literal, Optional

from tgdcfs.backends.base import IStore
from tgdcfs.core.tee import TeeBranch, TeeFileMessage, TeeUpload
from tgdcfs.errors import MessageNotFound, TechnicalError
from tgdcfs.reqres import FileMessageFromStream, Replica, UploadableFileMessage

if TYPE_CHECKING:
    from tgdcfs.core.local_cache import VersionBytes, VersionCache
    from tgdcfs.core.model import TGFSFileVersion

logger = logging.getLogger(__name__)

MirrorMode = Literal["auto", "forward", "reupload"]
MIRROR_MODES: tuple[MirrorMode, ...] = ("auto", "forward", "reupload")

# How long reads prefer the mirrors after a primary-store failure.
PRIMARY_DEAD_SECONDS = 60.0


@dataclass
class MirrorStore:
    # Serialization key of the store (``tg:<channel>``) -- the key used in
    # the metadata, NOT the lib-specific resolved id and not the config
    # name, which may be renamed.
    key: str
    store: IStore


@dataclass
class MirrorResult:
    """Copies made by one ``mirror_parts`` call, per store key."""

    # Aligned copies: ids in the same order as the source parts.
    mirrors: Dict[str, List[int]] = field(default_factory=dict)
    # Re-partitioned copies with their own layout.
    replicas: Dict[str, Replica] = field(default_factory=dict)

    @property
    def store_keys(self) -> List[str]:
        return list(self.mirrors) + list(self.replicas)

    def apply_to(self, version: "TGFSFileVersion") -> None:
        version.mirrors.update(self.mirrors)
        version.replicas.update(self.replicas)


class MirrorGroup:
    def __init__(
        self,
        primary: IStore,
        stores: List[MirrorStore],
        mode: MirrorMode = "auto",
        strict: bool = False,
    ):
        if mode not in MIRROR_MODES:
            raise ValueError(f"Unknown mirror mode: {mode}")
        self._primary = primary
        self._stores = stores
        self._mode = mode
        self._strict = strict
        self._by_key = {ch.key: ch for ch in stores}
        self._primary_dead_until = 0.0

    @property
    def primary(self) -> IStore:
        return self._primary

    @property
    def mode(self) -> MirrorMode:
        return self._mode

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def store_keys(self) -> List[str]:
        return [ch.key for ch in self._stores]

    def store_for(self, key: str) -> Optional[IStore]:
        """The store behind ``key``: a mirror, or the primary itself."""
        if key == self._primary.key:
            return self._primary
        ch = self._by_key.get(key)
        return ch.store if ch else None

    # -- primary health (read-path circuit breaker) ------------------------

    def mark_primary_dead(self) -> None:
        self._primary_dead_until = time.monotonic() + PRIMARY_DEAD_SECONDS

    def primary_dead(self) -> bool:
        return time.monotonic() < self._primary_dead_until

    # -- content parts -----------------------------------------------------

    def missing_stores(self, version: "TGFSFileVersion") -> List[str]:
        """Mirror stores that hold no complete copy of ``version``."""
        return [key for key in self.store_keys if not version.has_copy_in(key)]

    async def mirror_parts(
        self,
        message_ids: List[int],
        part_sizes: Optional[List[int]] = None,
        only_stores: Optional[List[str]] = None,
        source: Optional[IStore] = None,
        cache: Optional["VersionCache"] = None,
    ) -> MirrorResult:
        """Copy the given parts into the mirror stores.

        ``message_ids`` name messages of ``source`` (the primary unless
        given). ``part_sizes`` decide whether a store gets an aligned copy
        or a replica; when omitted they are looked up from the source.
        With ``cache`` the bytes come from the local copy of the version
        when there is one, and a download that has to happen anyway fills
        the cache for the stores that follow. Returns the copies per
        store that succeeded.
        """
        res = MirrorResult()
        if not message_ids:
            return res
        source = source or self._primary
        sizes = part_sizes
        for ch in self._stores:
            if only_stores is not None and ch.key not in only_stores:
                continue
            if ch.store is source:
                continue
            try:
                if sizes is None or len(sizes) != len(message_ids):
                    sizes = await self._part_sizes(source, message_ids)
                if self._fits_aligned(ch.store, source, sizes):
                    res.mirrors[ch.key] = await self._copy_to(
                        ch, source, message_ids, sizes, cache
                    )
                else:
                    res.replicas[ch.key] = await self._replicate(
                        ch, source, message_ids, sizes, cache
                    )
            except Exception as ex:
                self._handle_write_error(ch.key, "content parts", ex)
        return res

    @property
    def stores(self) -> List[MirrorStore]:
        return list(self._stores)

    # -- tee: mirrors fed from the upload stream ----------------------------

    def must_reupload(self, target: MirrorStore) -> bool:
        """Whether ``target`` can only be filled by re-uploading the bytes:
        ``mode: reupload``, another backend, or no server-side copy on
        either side (Discord)."""
        if self._mode == "reupload":
            return True
        if target.store.backend != self._primary.backend:
            return True
        return not (
            self._primary.caps.supports_server_copy
            and target.store.caps.supports_server_copy
        )

    def tee_targets(self) -> List[MirrorStore]:
        """The mirrors a tee'd upload feeds from the stream: the ones that
        would otherwise download the version from the primary again."""
        return [ch for ch in self._stores if self.must_reupload(ch)]

    def tee(
        self, file_msg: UploadableFileMessage, buffer_bytes: int
    ) -> Optional[TeeUpload]:
        """Split ``file_msg`` for the primary and every re-uploading mirror.

        Returns ``None`` when no mirror needs the stream or the size is
        zero; the caller then uploads ``file_msg`` as it is. An unknown
        size (an SFTP upload, ``-1``) is fine: the branches end at the
        stream's own EOF instead of at a byte count.
        """
        size = file_msg.get_size()
        targets = self.tee_targets()
        if not targets or size == 0:
            return None
        name = file_msg.file_name()
        branches = [TeeBranch(ch.key, buffer_bytes) for ch in targets]
        message = TeeFileMessage.wrap(file_msg, branches)

        def upload_into(ch: MirrorStore):
            async def run(branch: TeeBranch) -> Replica:
                stream = branch.stream()
                try:
                    sent = await ch.store.upload(
                        FileMessageFromStream.new(
                            stream=stream, size=size, name=f"replica-{name}"
                        )
                    )
                finally:
                    await stream.aclose()
                return Replica(
                    message_ids=[m.message_id for m in sent],
                    part_sizes=[m.size for m in sent],
                )

            return run

        return TeeUpload(message, {ch.key: upload_into(ch) for ch in targets})

    def collect_tee(
        self, results: Dict[str, "Replica | Exception"]
    ) -> Dict[str, Replica]:
        """Sort the outcome of a tee: replicas are returned, a failure is
        handled like any other mirror write error (logged, or raised
        with ``strict``)."""
        replicas: Dict[str, Replica] = {}
        for key, outcome in results.items():
            if isinstance(outcome, Exception):
                self._handle_write_error(key, "tee'd content", outcome)
            else:
                replicas[key] = outcome
        return replicas

    def needs_replica(self, target: MirrorStore, part_sizes: List[int]) -> bool:
        """Whether ``target`` gets a replica (own layout) rather than an
        aligned copy of parts of these sizes from the primary."""
        return not self._fits_aligned(target.store, self._primary, part_sizes)

    async def replicate_local(
        self, target: MirrorStore, local: "VersionBytes", name: str
    ) -> Replica:
        """Upload a version that sits in the local cache into ``target`` as
        a replica, independent of the primary (write-back distribution)."""
        from tgdcfs.core.local_cache import FileMessageFromCache

        sent = await target.store.upload(FileMessageFromCache.new(local, name))
        return Replica(
            message_ids=[m.message_id for m in sent],
            part_sizes=[m.size for m in sent],
        )

    async def copy_into_primary(self, version: "TGFSFileVersion") -> bool:
        """Copy a version that lives in a former primary into the current one.

        Returns whether the version now has its primary layout in the
        current primary. Nothing happens for versions the primary already
        owns or whose old store is not configured anymore.
        """
        if version.owned_by(self._primary.key) or version.store is None:
            return version.owned_by(self._primary.key)
        source = self.store_for(version.store)
        if source is None:
            logger.warning(
                f"Cannot copy version {version.id} into the primary: its store "
                f"{version.store} is not configured"
            )
            return False
        sizes = version.part_sizes
        if len(sizes) != len(version.message_ids):
            sizes = await self._part_sizes(source, version.message_ids)
        target = MirrorStore(key=self._primary.key, store=self._primary)
        if self._fits_aligned(self._primary, source, sizes):
            ids = await self._copy_to(target, source, version.message_ids)
            version.mirrors[self._primary.key] = ids
        else:
            version.replicas[self._primary.key] = await self._replicate(
                target, source, version.message_ids, sizes
            )
        version.part_sizes = list(sizes)
        version.relocate(self._primary.key)
        return version.owned_by(self._primary.key)

    @staticmethod
    def _fits_aligned(target: IStore, source: IStore, part_sizes: List[int]) -> bool:
        """Whether every source part fits one message of ``target``.

        Same-backend copies are always aligned (a server-side copy keeps
        the message as it is); otherwise the target's part limit decides.
        """
        if target.backend == source.backend:
            return True
        return max(part_sizes, default=0) <= target.caps.max_part_bytes

    async def _part_sizes(self, source: IStore, message_ids: List[int]) -> List[int]:
        messages = await source.get_messages(message_ids)
        sizes: List[int] = []
        for mid, message in zip(message_ids, messages):
            if not message or not message.document:
                raise MessageNotFound(message_id=mid)
            sizes.append(message.document.size)
        return sizes

    async def _copy_to(
        self,
        target: MirrorStore,
        source: IStore,
        message_ids: List[int],
        part_sizes: Optional[List[int]] = None,
        cache: Optional["VersionCache"] = None,
    ) -> List[int]:
        """Aligned copy of ``source`` messages into ``target`` per ``mode``."""
        if not message_ids:
            return []
        if self._mode != "reupload":
            try:
                ids = await target.store.copy_from(source, message_ids)
            except Exception as ex:
                if self._mode == "forward":
                    raise
                logger.warning(
                    f"Server-side copy into store {target.key} failed ({ex}); "
                    f"falling back to re-upload"
                )
                ids = None
            if ids is not None:
                return ids
            if self._mode == "forward":
                raise TechnicalError(
                    f"Store {target.key} cannot receive server-side copies from "
                    f"{source.key}; use mode 'auto' or 'reupload'"
                )
        # A complete local copy spares the download of every part; the
        # part offsets into it follow from the part sizes.
        local = cache.local() if cache is not None else None
        offsets: List[Optional[tuple["VersionBytes", int, int]]] = [None] * len(
            message_ids
        )
        if local is not None and part_sizes and len(part_sizes) == len(message_ids):
            begin = 0
            for i, size in enumerate(part_sizes):
                offsets[i] = (local, begin, begin + size - 1)
                begin += size
        return [
            await self._reupload_one(target, source, mid, local_range)
            for mid, local_range in zip(message_ids, offsets)
        ]

    async def _reupload_one(
        self,
        target: MirrorStore,
        source: IStore,
        message_id: int,
        local_range: Optional[tuple["VersionBytes", int, int]] = None,
    ) -> int:
        """Bandwidth-bound aligned copy: stream one part down and up again.

        Bytes are copied verbatim -- this sits below the encryption
        decorator, so ciphertext stays ciphertext. The caller has checked
        that the part fits one message of the target. With ``local_range``
        the part is read from the local cache instead of the source.
        """
        if local_range is not None:
            local, begin, end = local_range
            size = end - begin + 1
            chunks = local.read(begin, end)
        else:
            message = (await source.get_messages([message_id]))[0]
            if not message or not message.document:
                raise MessageNotFound(message_id=message_id)
            size = message.document.size
            chunks = (await source.download_file(message_id, 0, size - 1)).chunks
        sent = await target.store.upload(
            FileMessageFromStream.new(
                stream=chunks, size=size, name=f"part-{message_id}"
            )
        )
        if len(sent) != 1:
            raise TechnicalError(
                f"Store {target.key} split the {size}-byte part {message_id} into "
                f"{len(sent)} messages although it reported room for it"
            )
        return sent[0].message_id

    async def _replicate(
        self,
        target: MirrorStore,
        source: IStore,
        message_ids: List[int],
        part_sizes: List[int],
        cache: Optional["VersionCache"] = None,
    ) -> Replica:
        """Re-partitioned copy: stream the whole version through the target.

        The target cuts the byte stream at its own part size; the result
        is a replica with the target's layout. The bytes come from the
        local cache when it holds the version; a download that has to
        happen fills the cache on the way when the cache keeps entries.
        """
        total = sum(part_sizes)
        local = cache.local() if cache is not None else None
        filler = cache.filler() if cache is not None and local is None else None

        async def content() -> AsyncGenerator[bytes, None]:
            if local is not None:
                async for chunk in local.read(0, total - 1):
                    yield chunk
                return
            for mid, size in zip(message_ids, part_sizes):
                if size <= 0:
                    continue
                resp = await source.download_file(mid, 0, size - 1)
                try:
                    async for chunk in resp.chunks:
                        if filler is not None:
                            await filler.write(chunk)
                        yield chunk
                finally:
                    if (aclose := getattr(resp.chunks, "aclose", None)) is not None:
                        await aclose()

        stream = content()
        try:
            sent = await target.store.upload(
                FileMessageFromStream.new(
                    stream=stream, size=total, name=f"replica-{message_ids[0]}"
                )
            )
        finally:
            # The target reads exactly ``total`` bytes and leaves the
            # generator suspended; close it so its downloads are released.
            await stream.aclose()
            if filler is not None:
                await filler.close()
        return Replica(
            message_ids=[m.message_id for m in sent],
            part_sizes=[m.size for m in sent],
        )

    # -- file descriptors --------------------------------------------------

    async def mirror_fd(
        self, text: str, existing: Optional[Dict[str, int]] = None
    ) -> Dict[str, int]:
        """Send or update the FD text message in every mirror store.

        Returns the map of current FD message ids per store: existing
        entries merged with this round's successful writes, so a store
        that fails transiently keeps its (stale but recoverable) copy.
        """
        res: Dict[str, int] = dict(existing or {})
        for ch in self._stores:
            try:
                if mid := res.get(ch.key):
                    try:
                        res[ch.key] = await ch.store.edit_message_text(
                            message_id=mid, message=text
                        )
                        continue
                    except MessageNotFound:
                        logger.warning(
                            f"FD mirror message {mid} in store {ch.key} is "
                            f"gone, sending a fresh copy"
                        )
                res[ch.key] = await ch.store.send_text(text)
            except Exception as ex:
                self._handle_write_error(ch.key, "file descriptor", ex)
        return res

    # -- pinned metadata ---------------------------------------------------

    async def mirror_pinned(
        self, primary_message_id: int, state: Dict[str, int]
    ) -> None:
        """Maintain a pinned copy of the metadata document in each mirror.

        The metadata blob is copied (preserving encryption), pinned, and
        the previous copy is deleted. ``state`` maps store key to the
        current metadata message id in that store and is updated in
        place. The blob fits one message of every store, the metadata
        repository checks that before writing it.
        """
        size: Optional[int] = None
        for ch in self._stores:
            try:
                if ch.store.backend != self._primary.backend:
                    if size is None:
                        size = (
                            await self._part_sizes(self._primary, [primary_message_id])
                        )[0]
                    if size > ch.store.caps.max_part_bytes:
                        # A pinned copy has to be one message; a store with
                        # smaller messages cannot hold it. Content and
                        # descriptors are still mirrored, only promotion
                        # needs the github_repo metadata type then.
                        logger.warning(
                            f"The metadata blob ({size} bytes) does not fit one "
                            f"message of mirror store {ch.key}; no pinned copy there"
                        )
                        continue
                new_id = (await self._copy_to(ch, self._primary, [primary_message_id]))[
                    0
                ]
                await ch.store.pin_message(new_id)
                if (old := state.get(ch.key)) and old != new_id:
                    await ch.store.delete_messages([old], force=True)
                state[ch.key] = new_id
            except Exception as ex:
                self._handle_write_error(ch.key, "pinned metadata", ex)

    async def adopt_pinned(
        self, new_ids: Dict[str, int], state: Dict[str, int]
    ) -> None:
        """Pin already-mirrored metadata copies (no copying needed).

        Used right after ``save`` has replicated the metadata blob as
        ordinary content: ``new_ids`` are the fresh copies per store.
        """
        for store_key, new_id in new_ids.items():
            if (ch := self._by_key.get(store_key)) is None or new_id <= 0:
                continue
            try:
                await ch.store.pin_message(new_id)
                if (old := state.get(store_key)) and old != new_id:
                    await ch.store.delete_messages([old], force=True)
                state[store_key] = new_id
            except Exception as ex:
                self._handle_write_error(store_key, "pinned metadata", ex)

    # -- deletion ----------------------------------------------------------

    async def delete(
        self, per_store: Dict[str, List[int]], force: bool = False
    ) -> None:
        """Best-effort deletion of mirrored messages, per store.

        Honors the backend's ``delete_messages_on_remove`` exactly like
        the primary-store deletion does (the gate lives inside
        ``IStore.delete_messages``); ``force`` bypasses that gate for
        internal bookkeeping.
        """
        for key, ids in per_store.items():
            if not ids:
                continue
            if (store := self.store_for(key)) is None:
                logger.warning(
                    f"Cannot delete mirrored messages in store {key}: "
                    f"store is not configured as a mirror anymore"
                )
                continue
            await store.delete_messages(ids, force=force)

    # -- internals ---------------------------------------------------------

    def _handle_write_error(self, store_key: str, what: str, ex: Exception) -> None:
        if self._strict:
            if isinstance(ex, TechnicalError):
                raise ex
            raise TechnicalError(
                f"Mirroring {what} to store {store_key} failed: {ex}"
            ) from ex
        logger.error(
            f"Mirroring {what} to store {store_key} failed (non-strict, "
            f"continuing): {ex}"
        )
