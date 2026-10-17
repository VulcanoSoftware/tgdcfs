import logging
from dataclasses import dataclass
from typing import AsyncIterator, Dict, Generator, Iterable, List, Optional, Sequence

from tgdcfs.backends.base import IStore
from tgdcfs.core.local_cache import LocalCache, StagedFileMessage, version_cache_for
from tgdcfs.core.mirror import MirrorGroup
from tgdcfs.core.model import TGFSFileVersion
from tgdcfs.core.multisource import MultiSourceRead, StoreView, read_slots
from tgdcfs.core.repository.interface import IFileContentRepository
from tgdcfs.errors import TechnicalError
from tgdcfs.reqres import (
    FileContent,
    SentFileMessage,
    UploadableFileMessage,
    aclose_content,
)
from tgdcfs.utils.prefetching_chain import prefetching_chain

logger = logging.getLogger(__name__)

# How many parts of one file are fetched at a time. Each part may itself
# be split across bots, so this multiplies with the per-message
# concurrency -- keep it small.
PART_PREFETCH_CONCURRENCY = 2
# How much of a write-back upload is taken off the request at a time.
STAGE_READ_CHUNK = 1024 * 1024


@dataclass
class Layout:
    """One way of cutting a version into messages.

    ``candidates[i]`` lists the ``(store, message id)`` pairs that hold
    part ``i``; the primary layout has the primary plus every aligned
    mirror per part, a replica layout has exactly one store.
    """

    part_sizes: List[int]
    candidates: List[List[tuple[IStore, int]]]
    label: str

    @property
    def size(self) -> int:
        return sum(self.part_sizes)


class StoreFileContentRepository(IFileContentRepository):
    """File content as part messages in a store, mirrored into others.

    Backend-agnostic: the store partitions and moves the bytes, this
    class maps byte ranges onto parts and fails reads over to copies in
    other stores -- aligned mirrors part by part, replicas with their
    own layout from the byte where the previous layout stopped.
    """

    def __init__(
        self,
        store: IStore,
        mirror_group: Optional[MirrorGroup] = None,
        inline_mirroring: bool = True,
        read_preference: Optional[Sequence[str]] = None,
        cache: Optional[LocalCache] = None,
        cache_scope: str = "",
        read_parallel: bool = False,
        read_sources: Optional[Sequence[str]] = None,
        piece_size: int = 4 * 1024 * 1024,
        parallel_threshold: int = 10 * 1024 * 1024,
        read_window: int = 8,
        tee_mirroring: bool = False,
        tee_buffer: int = 64 * 1024 * 1024,
    ):
        self._store = store
        self._mirror_group = mirror_group
        # ``sync: tee``: mirrors that would re-upload the bytes are fed
        # from the upload stream while the primary uploads it, buffering
        # ``tee_buffer`` bytes per mirror; the other mirrors are copied
        # inline afterwards (server-side, one call per part).
        self._tee_mirroring = tee_mirroring
        self._tee_buffer = tee_buffer
        # Multi-source reads (design plan 4.11): a range above the
        # threshold is cut into pieces of ``piece_size`` that every store
        # holding the version fetches side by side; ``read_sources`` (store
        # keys) limits the stores that take part.
        self._read_parallel = read_parallel
        self._read_sources = list(read_sources or [])
        self._piece_size = piece_size
        self._parallel_threshold = parallel_threshold
        self._read_window = read_window
        # False: writes record the primary copy only and the replication
        # queue copies to the mirrors later (``sync: background``).
        self._inline_mirroring = inline_mirroring
        self._read_preference = list(read_preference or [])
        # The local cache (design plan 4.10) and the file system name its
        # entries are filed under. ``None`` when the cache is off.
        self._cache = cache if cache is not None and cache.config.enabled else None
        self._cache_scope = cache_scope

    @property
    def cache(self) -> Optional[LocalCache]:
        return self._cache

    async def save(self, file_msg: UploadableFileMessage) -> List[SentFileMessage]:
        # Stage the bytes on disk while they stream to the primary, so the
        # mirrors read the version from here instead of from the primary.
        # Not with ``sync: tee``: the mirrors that re-upload take the bytes
        # from the stream and the others are copied server-side, so nothing
        # would read the staged copy and the upload goes to the stores only.
        writer = None
        version_id = file_msg.version_id
        if (
            self._cache is not None
            and self._cache.config.stage_uploads
            and version_id
            and self._mirror_group is not None
            and not self._tee_mirroring
        ):
            writer = self._cache.open_staging(
                self._cache_scope, version_id, file_msg.get_size()
            )
            if writer is not None:
                self._cache.pin(version_id)
                file_msg = StagedFileMessage.wrap(file_msg, writer)

        # Split the stream for the mirrors that would otherwise download
        # it from the primary again (``sync: tee``): their uploads run
        # while the primary's does, fed by the reads it takes.
        tee = None
        if self._tee_mirroring and self._mirror_group is not None:
            tee = self._mirror_group.tee(file_msg, self._tee_buffer)
            if tee is not None:
                file_msg = tee.message

        try:
            res = await self._store.upload(file_msg)
        except BaseException:
            if tee is not None:
                await tee.abort()
            if writer is not None:
                await writer.abort()
            raise
        if writer is not None:
            await writer.close()

        # Replicate the freshly uploaded parts into the mirror stores. The
        # mapping travels back to the caller inside the SentFileMessage
        # objects and ends up in TGFSFileVersion.mirrors / .replicas.
        if self._mirror_group and res and (self._inline_mirroring or tee):
            only_stores = None
            if tee is not None:
                # The tee'd mirrors finish on their own; the rest are the
                # server-side copies, which are cheap enough to make inline.
                res[0].replicas.update(
                    self._mirror_group.collect_tee(await tee.collect())
                )
                only_stores = [
                    key
                    for key in self._mirror_group.store_keys
                    if key not in tee.store_keys
                ]
            copies = await self._mirror_group.mirror_parts(
                [m.message_id for m in res],
                [m.size for m in res],
                only_stores=only_stores,
                cache=version_cache_for(
                    self._cache, self._cache_scope, version_id, sum(m.size for m in res)
                ),
            )
            for store_key, mirror_ids in copies.mirrors.items():
                for sent, mirror_id in zip(res, mirror_ids):
                    sent.mirrors[store_key] = mirror_id
            res[0].replicas.update(copies.replicas)
            # Mirroring is complete here; with a partial result the
            # replication queue is not involved, so nothing would unpin.
            if self._cache is not None and version_id:
                self._cache.release(version_id)
        return res

    async def stage(
        self, file_msg: UploadableFileMessage, version_id: str
    ) -> Optional[int]:
        """Write the whole upload into the cache; no store is touched.

        The entry is pinned until the distribution worker has moved the
        bytes into every store. ``None`` when the cache refuses the entry
        up front (off, over budget, too large): the caller then uploads
        the stream directly. A disk error while the body is already being
        consumed cannot fall back -- the body is gone -- so it fails the
        write, which is the honest answer.
        """
        if self._cache is None:
            return None
        size = file_msg.get_size()
        writer = self._cache.open_staging(self._cache_scope, version_id, size)
        if writer is None:
            return None
        self._cache.pin(version_id)
        await file_msg.open()
        try:
            remaining = size
            while remaining > 0:
                chunk = await file_msg.read(min(STAGE_READ_CHUNK, remaining))
                if not chunk:
                    break
                await writer.write(chunk)
                remaining -= len(chunk)
                if writer.failed:
                    break
        except BaseException:
            await writer.abort()
            raise
        finally:
            await file_msg.close()
        await writer.close()
        if writer.failed or remaining > 0:
            self._cache.remove(version_id)
            raise TechnicalError(
                f"Could not stage version {version_id} in the local cache "
                f"({size - remaining} of {size} bytes written)"
            )
        return size

    async def update(self, message_id: int, buffer: bytes, name: str) -> int:
        return await self._store.replace_document(message_id, buffer, name)

    # -- range mapping -------------------------------------------------------

    @staticmethod
    def _get_file_part_to_download(
        fv: TGFSFileVersion, begin: int, end: int
    ) -> Generator[tuple[int, int, int], None, None]:
        for (
            _,
            message_id,
            part_begin,
            part_end,
        ) in StoreFileContentRepository._get_file_parts_indexed(fv, begin, end):
            yield message_id, part_begin, part_end

    @staticmethod
    def _get_file_parts_indexed(
        fv: TGFSFileVersion, begin: int, end: int
    ) -> Generator[tuple[int, int, int, int], None, None]:
        """Map ``[begin, end]`` onto the primary layout of ``fv``."""
        yield from StoreFileContentRepository._map_range(
            fv.message_ids, fv.part_sizes, fv.size, fv.id, begin, end
        )

    @staticmethod
    def _map_range(
        message_ids: List[int],
        part_sizes: List[int],
        size: int,
        version_id: str,
        begin: int,
        end: int,
    ) -> Generator[tuple[int, int, int, int], None, None]:
        """Map the inclusive byte range ``[begin, end]`` onto parts.

        Yields ``(part index, message id, begin, end)`` with the bounds
        relative to the part and, like the input, inclusive. ``end < 0``
        means the end of the file, and an ``end`` past it is clamped there:
        clients do ask past the end, and the body then stops at the last
        byte that exists. A part is never asked for more bytes than it
        holds: that could only be served by a download stopping short,
        which the backends reject.
        """
        if size <= 0:
            return
        if end < 0 or end >= size:
            end = size - 1
        if begin < 0:
            raise TechnicalError(
                f"Invalid begin value {begin} for file version {version_id} with size {size}"
            )
        if begin > end:
            raise TechnicalError(
                f"Invalid range: begin {begin} is greater than end {end} for file version {version_id}"
            )

        offset = 0
        i_part = 0

        while i_part < len(part_sizes) and offset + part_sizes[i_part] <= begin:
            offset += part_sizes[i_part]
            i_part += 1

        if i_part >= len(part_sizes):
            raise TechnicalError(
                f"Begin offset {begin} exceeds total file size {size} for file version {version_id}"
            )

        while i_part < len(part_sizes) and offset <= end:
            part_size = part_sizes[i_part]
            part_begin = max(0, begin - offset)
            part_end = min(part_size - 1, end - offset)
            if part_begin <= part_end:
                yield i_part, message_ids[i_part], part_begin, part_end
            offset += part_size
            i_part += 1

    # -- sources -------------------------------------------------------------

    def _layouts(self, fv: TGFSFileVersion) -> List[Layout]:
        """Every layout a read can be served from, in order of preference.

        The primary layout comes first, with the primary tried before
        the aligned mirrors for every part (reversed while the primary is
        marked dead). Replicas follow. A store named in the read
        preference moves to the front of its list.
        """
        layouts: List[Layout] = []
        group = self._mirror_group
        primary_key = self._store.key

        if fv.message_ids and len(fv.part_sizes) == len(fv.message_ids):
            candidates: List[List[tuple[IStore, int]]] = []
            for i, mid in enumerate(fv.message_ids):
                sources: List[tuple[IStore, int]] = []
                if fv.owned_by(primary_key) and mid > 0:
                    sources.append((self._store, mid))
                if group:
                    for key, ids in fv.mirrors.items():
                        store = group.store_for(key)
                        if store and i < len(ids) and ids[i] > 0:
                            sources.append((store, ids[i]))
                    if group.primary_dead() and len(sources) > 1:
                        sources = sources[1:] + sources[:1]
                sources.sort(key=lambda pair: self._preference_rank(pair[0].key))
                candidates.append(sources)
            layouts.append(Layout(list(fv.part_sizes), candidates, "primary"))

        if group:
            for key, replica in fv.replicas.items():
                store = group.store_for(key)
                if (
                    store is None
                    or not replica.message_ids
                    or len(replica.part_sizes) != len(replica.message_ids)
                ):
                    continue
                layouts.append(
                    Layout(
                        list(replica.part_sizes),
                        [[(store, mid)] for mid in replica.message_ids],
                        f"replica {key}",
                    )
                )

        layouts.sort(
            key=lambda layout: min(
                (
                    self._preference_rank(store.key)
                    for sources in layout.candidates
                    for store, _ in sources
                ),
                default=len(self._read_preference),
            )
        )
        return layouts

    def _preference_rank(self, store_key: str) -> int:
        try:
            return self._read_preference.index(store_key)
        except ValueError:
            return len(self._read_preference)

    async def _download_part(
        self,
        layout: Layout,
        part_idx: int,
        begin: int,
        end: int,
    ) -> AsyncIterator[bytes]:
        """Stream one part, failing over to the next candidate on error.

        Failover also works mid-stream: bytes already delivered are
        skipped by advancing ``begin`` before retrying the next source.
        """
        sources = layout.candidates[part_idx]
        served = 0
        last_ex: Optional[Exception] = None
        for store, mid in sources:
            try:
                resp = await store.download_file(mid, begin + served, end)
                async for chunk in resp.chunks:
                    yield chunk
                    served += len(chunk)
                return
            except Exception as ex:
                last_ex = ex
                if store is self._store and self._mirror_group:
                    self._mirror_group.mark_primary_dead()
                logger.warning(
                    f"Downloading part {part_idx} (message {mid}) of layout "
                    f"'{layout.label}' from store {store.key} failed: {ex}"
                )
        if last_ex:
            raise last_ex
        raise TechnicalError(
            f"No download source available for part {part_idx} ({layout.label})"
        )

    def _stream_layout(
        self, layout: Layout, version_id: str, begin: int, end: int
    ) -> FileContent:
        parts = [
            self._download_part(layout, part_idx, part_begin, part_end)
            for part_idx, _, part_begin, part_end in self._map_range(
                [mid for sources in layout.candidates for _, mid in sources[:1]],
                layout.part_sizes,
                layout.size,
                version_id,
                begin,
                end,
            )
        ]
        return prefetching_chain(parts, concurrency=PART_PREFETCH_CONCURRENCY)

    async def get(
        self,
        fv: TGFSFileVersion,
        begin: int,
        end: int,
        name: str,
        *,
        admit: bool = True,
    ) -> FileContent:
        logger.info(f"Retrieving file content for {name}@{fv.id} from {begin} to {end}")
        if fv.pending:
            # No store has the bytes yet; the instance that accepted the
            # upload serves them from its cache, nobody else can.
            if self._cache is not None and self._cache.complete(fv.id):
                if fv.size <= 0:
                    return self._empty()
                last = fv.size - 1 if end < 0 else min(end, fv.size - 1)
                if begin < 0 or begin > last:
                    raise TechnicalError(
                        f"Invalid range {begin}-{end} for {name}@{fv.id} ({fv.size} bytes)"
                    )
                return self._cache.read(fv.id, begin, last)
            raise TechnicalError(
                f"{name}@{fv.id} is still being distributed from another instance"
            )
        layouts = self._layouts(fv)
        if not layouts:
            if fv.size <= 0:
                return self._empty()
            raise TechnicalError(f"No readable copy of {name}@{fv.id}")

        # Validate the range once, against the first layout, so bad
        # requests fail here rather than inside the stream.
        list(
            self._map_range(
                [0] * len(layouts[0].part_sizes),
                layouts[0].part_sizes,
                layouts[0].size,
                fv.id,
                begin,
                end,
            )
        )

        size = layouts[0].size
        last = size - 1 if end < 0 or end >= size else end
        if (
            self._cache is not None
            and self._cache.config.keep_for_reads
            and fv.cacheable
            and fv.id
        ):
            return self._stream_through_cache(
                fv, layouts, begin, last, name, admit=admit
            )
        return self._stream_layouts(layouts, fv.id, begin, end, name)

    def _store_views(self, layouts: List[Layout]) -> List[StoreView]:
        """Every store that holds the version completely, with its layout.

        The primary layout yields one view per store that has a candidate
        for every part; each replica layout yields its store. Read
        preference decides the order, ``read_sources`` who takes part.
        """
        views: List[StoreView] = []
        seen: set[str] = set()
        for layout in layouts:
            per_store: Dict[str, List[int]] = {}
            stores: Dict[str, IStore] = {}
            for sources in layout.candidates:
                for store, mid in sources:
                    per_store.setdefault(store.key, []).append(mid)
                    stores[store.key] = store
            for key, ids in per_store.items():
                if key in seen or len(ids) != len(layout.part_sizes):
                    continue
                if self._read_sources and key not in self._read_sources:
                    continue
                seen.add(key)
                views.append(
                    StoreView(
                        stores[key],
                        ids,
                        list(layout.part_sizes),
                        slots=read_slots(stores[key]),
                    )
                )
        views.sort(key=lambda view: self._preference_rank(view.key))
        return views

    def _stream_layouts(
        self, layouts: List[Layout], version_id: str, begin: int, end: int, name: str
    ) -> FileContent:
        """Stream ``[begin, end]`` from the first layout that can serve it,
        failing over to the next from the byte where the previous stopped.

        With ``read_parallel`` and more than one store holding the version,
        a range above the threshold is shared between the stores instead.
        """
        size = layouts[0].size
        last = size - 1 if end < 0 or end >= size else end
        if self._read_parallel and last - begin + 1 > self._parallel_threshold:
            views = self._store_views(layouts)
            if len(views) > 1:
                return MultiSourceRead(
                    views,
                    size,
                    version_id,
                    begin,
                    last,
                    self._piece_size,
                    self._read_window,
                    self._map_range,
                    name,
                ).stream()

        async def stream() -> AsyncIterator[bytes]:
            served = 0
            last_ex: Optional[Exception] = None
            for layout in layouts:
                try:
                    async for chunk in self._stream_layout(
                        layout, version_id, begin + served, end
                    ):
                        yield chunk
                        served += len(chunk)
                    return
                except Exception as ex:
                    last_ex = ex
                    logger.warning(
                        f"Layout '{layout.label}' of {name}@{version_id} failed after "
                        f"{served} bytes: {ex}; trying the next one"
                    )
            if last_ex:
                raise last_ex

        return stream()

    def _stream_through_cache(
        self,
        fv: TGFSFileVersion,
        layouts: List[Layout],
        begin: int,
        end: int,
        name: str,
        *,
        admit: bool = True,
    ) -> FileContent:
        """Serve ``[begin, end]`` with the read cache in front of the stores.

        Blocks already on disk are read from there; every missing run is
        fetched from the stores widened to whole blocks, stored block by
        block as it arrives, and only the requested bytes are handed out.
        A cache that refuses the entry (budget, size) leaves the read on
        the plain store path.

        ``admit=False`` reads through an entry that exists but opens none:
        a probe that abandons the stream after a few bytes would leave an
        entry sized to the whole version and never fill a block of it.
        """
        cache = self._cache
        if cache is None:  # pragma: no cover - callers check first
            return self._stream_layouts(layouts, fv.id, begin, end, name)
        size = layouts[0].size

        async def stream() -> AsyncIterator[bytes]:
            entry = cache.entry(fv.id)
            if entry is None and admit:
                entry = cache.reserve(self._cache_scope, fv.id, size)
            if entry is None:
                plain = self._stream_layouts(layouts, fv.id, begin, end, name)
                try:
                    async for chunk in plain:
                        yield chunk
                finally:
                    # A probe reading a few bytes with ``admit=False`` lands
                    # here and abandons the stream.
                    await aclose_content(plain)
                return

            position = begin
            for run_begin, run_end in entry.missing_runs(begin, end):
                # Whatever is on disk before this run.
                if position < run_begin:
                    cache.hits += 1
                    async for chunk in cache.read(fv.id, position, run_begin - 1):
                        yield chunk
                    position = run_begin
                cache.misses += 1
                async for chunk in self._fetch_run(
                    fv.id,
                    entry.block_size,
                    layouts,
                    run_begin,
                    run_end,
                    begin,
                    end,
                    name,
                ):
                    yield chunk
                position = min(run_end, end) + 1
            if position <= end:
                cache.hits += 1
                async for chunk in cache.read(fv.id, position, end):
                    yield chunk

        return stream()

    async def _fetch_run(
        self,
        version_id: str,
        block_size: int,
        layouts: List[Layout],
        run_begin: int,
        run_end: int,
        begin: int,
        end: int,
        name: str,
    ) -> AsyncIterator[bytes]:
        """Fetch the block-aligned run ``[run_begin, run_end]`` from the
        stores, store each completed block, yield the part inside
        ``[begin, end]``."""
        cache = self._cache
        if cache is None:  # pragma: no cover - callers check first
            raise TechnicalError("no cache to fetch into")
        block: bytearray = bytearray()
        block_begin = run_begin
        position = run_begin
        async for chunk in self._stream_layouts(
            layouts, version_id, run_begin, run_end, name
        ):
            # Hand out only the requested bytes of this chunk.
            chunk_end = position + len(chunk) - 1
            lo, hi = max(position, begin), min(chunk_end, end)
            if lo <= hi:
                yield bytes(chunk[lo - position : hi - position + 1])
            position = chunk_end + 1
            # Store whole blocks as they complete.
            block.extend(chunk)
            while len(block) >= block_size:
                await cache.write_blocks(
                    version_id, block_begin, bytes(block[:block_size])
                )
                del block[:block_size]
                block_begin += block_size
        if block and position > run_end:
            # The final, short block of the version.
            await cache.write_blocks(version_id, block_begin, bytes(block))

    async def forget(self, version_ids: Iterable[str]) -> None:
        if self._cache is not None:
            self._cache.remove_many(version_ids)

    @staticmethod
    async def _empty() -> AsyncIterator[bytes]:
        if False:  # pragma: no cover - makes this an async generator
            yield b""
