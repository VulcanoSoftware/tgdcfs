"""Local cache on the data volume: write staging and read cache.

Every byte a mirror needs used to be read back from the primary store
first: a Telegram primary with a Discord mirror downloads each gigabyte
once per mirror and uploads it again. The cache keeps a copy of a
version on the local disk while it is written, so the mirrors read it
from there; kept after replication it serves repeated reads as well.

Where it sits
-------------

Inside the file-content repository, below the encryption decorator, at
the level the replication engine copies bytes verbatim. The cache holds
exactly what the stores hold: ciphertext when encryption is on, never
plaintext.

Unit and layout
---------------

Entries are keyed by *version id*, not by store message, because the
stores cut a version differently (2 GiB Telegram parts, 10 MB Discord
messages). Each entry is a sparse data file ``<id>.bin`` plus a map file
``<id>.map`` with one byte per block of ``block_size``: ``1`` when the
block is on disk. A staged upload fills every block; a download fills
the blocks it fetched; a read is served for the blocks that are present.
Versions are immutable, so an entry can never be stale.

Budget
------

``max_size`` is a hard ceiling: an entry being staged is charged its
full size from the moment it is admitted, a read-cache entry is charged
the blocks it holds plus the blocks a fill is writing right now, and
every write claims its bytes before it touches the disk, so the sum
never exceeds the budget. ``max_files`` bounds the entries and
``max_file_size`` the largest version *staged* (an upload or a mirror
download, which land whole and are charged whole). A read-cache fill
is admitted per block: the entry costs nothing until a block lands, so
a version above ``max_file_size``, or larger than the whole budget, is
still cached for the blocks a reader touches. Eviction is LRU by last
access over *unpinned* entries; a pinned entry (its mirrors still need
it) is never evicted. When the budget cannot be met a new entry is
refused, or a fill stops caching, and the caller carries on without the
cache. ``min_free`` keeps that much of the disk free for everything else
in the data directory: an entry that would eat into it is evicted for
or refused like one over the budget. Every disk error is handled the
same way: the cache steps aside, the transfer goes on.

Sweep
-----

Eviction on demand keeps the budget, but nothing else moves on its own,
so a ``CacheSweeper`` runs :meth:`LocalCache.sweep` every
``SWEEP_INTERVAL``: orphaned files go, pins nobody will release any more
(a day old, nothing queued for their file system) are released, entries
unread for ``max_age`` are dropped, and the cache is evicted down to
``target_fill`` of the budget and to ``min_free`` on the disk, so the
next upload finds its room ready instead of making it first.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Dict, Iterable, List, Optional, Protocol

from tgdcfs.config import CacheConfig
from tgdcfs.reqres import FileTags, UploadableFileMessage

logger = logging.getLogger(__name__)

INDEX_FILE = "index.json"
INDEX_VERSION = 1
# How much of a cached entry a single read hands out per chunk.
READ_CHUNK = 1024 * 1024
# A warning per refused entry would flood the log during a bulk upload
# into a full cache; one every so often is enough to notice.
REFUSAL_LOG_INTERVAL = 60.0
# How often the background sweep runs.
SWEEP_INTERVAL = 15 * 60.0
# A pin this old with nothing queued for its file system belongs to a
# replication that will never report back (a crash mid-write, a queue
# file lost); the sweep releases it. A mirror that still wants the
# version downloads it from the primary instead.
STALE_PIN_SECONDS = 24 * 3600.0


def format_bytes(size: int) -> str:
    """``size`` in the largest binary unit that keeps it at or above 1."""
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(value) < 1024 or unit == "GiB":
            return f"{int(value)} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


class VersionBytes(Protocol):
    """A complete, seekable copy of a version's bytes."""

    @property
    def size(self) -> int: ...

    def read(self, begin: int, end: int) -> AsyncIterator[bytes]:
        """Stream the inclusive byte range ``[begin, end]``."""
        ...


@dataclass
class CacheEntry:
    version_id: str
    fs: str
    size: int
    block_size: int
    present: bytearray
    last_access: float
    pins: int = 0
    # True while a StagingWriter appends to it: the whole size is charged
    # against the budget, not just the blocks written so far.
    writing: bool = False
    # Bytes a read-cache fill has claimed from the budget but not yet
    # marked present: the write is on its way to disk. Never persisted.
    inflight: int = 0
    # When the entry went from unpinned to pinned; the sweep uses it to
    # spot pins nobody will release.
    pinned_at: float = 0.0

    @property
    def evictable(self) -> bool:
        return self.pins == 0 and not self.writing and not self.inflight

    @property
    def blocks(self) -> int:
        return len(self.present)

    def block_length(self, index: int) -> int:
        if index < self.blocks - 1:
            return self.block_size
        return self.size - index * self.block_size

    def present_bytes(self) -> int:
        return sum(self.block_length(i) for i, p in enumerate(self.present) if p)

    def charged_bytes(self) -> int:
        return self.size if self.writing else self.present_bytes() + self.inflight

    def is_complete(self) -> bool:
        return self.size == 0 or all(self.present)

    def has_range(self, begin: int, end: int) -> bool:
        if self.size == 0:
            return True
        if begin < 0 or end >= self.size or begin > end:
            return False
        first, last = begin // self.block_size, end // self.block_size
        return all(self.present[first : last + 1])

    def missing_runs(self, begin: int, end: int) -> List[tuple[int, int]]:
        """Block-aligned byte ranges of ``[begin, end]`` that are not on disk.

        The first and last run are widened to whole blocks, so a caller can
        fetch and store them as complete blocks. Ranges are inclusive.
        """
        runs: List[tuple[int, int]] = []
        if self.size == 0:
            return runs
        end = min(end, self.size - 1)
        first, last = begin // self.block_size, end // self.block_size
        run_start: Optional[int] = None
        for index in range(first, last + 2):
            missing = index <= last and not self.present[index]
            if missing and run_start is None:
                run_start = index
            elif not missing and run_start is not None:
                runs.append(
                    (
                        run_start * self.block_size,
                        (index - 1) * self.block_size
                        + self.block_length(index - 1)
                        - 1,
                    )
                )
                run_start = None
        return runs

    def to_dict(self) -> dict:
        return {
            "fs": self.fs,
            "size": self.size,
            "block_size": self.block_size,
            "last_access": self.last_access,
            "pins": self.pins,
            "pinned_at": self.pinned_at,
        }


def blocks_for(size: int, block_size: int) -> int:
    return max(0, (size + block_size - 1) // block_size)


class StagingWriter:
    """Sequential writer into one entry; marks blocks as they complete.

    A disk error disables the writer: further writes are dropped, the
    entry is removed, and the caller never sees the failure -- the upload
    it accompanies must not fail because the cache did.
    """

    def __init__(self, cache: "LocalCache", entry: CacheEntry):
        self._cache = cache
        self._entry = entry
        self._offset = 0
        self._fd: Optional[int] = None
        self.failed = False

    @property
    def version_id(self) -> str:
        return self._entry.version_id

    @property
    def written(self) -> int:
        return self._offset

    @property
    def complete(self) -> bool:
        return not self.failed and self._offset >= self._entry.size

    async def write(self, data: bytes) -> None:
        if self.failed or not data:
            return
        try:
            await asyncio.to_thread(self._write_sync, data)
        except OSError as ex:
            self._cache._throttled_warning(
                f"Cache: writing {self._entry.version_id} failed ({ex}); "
                f"continuing without the cache"
            )
            await self.abort()
            return
        blocks_done = self._offset // self._entry.block_size
        if self._offset >= self._entry.size:
            blocks_done = self._entry.blocks
        self._cache._mark_present(self._entry, range(0, blocks_done))
        if self._offset >= self._entry.size:
            await self._finish()

    def _write_sync(self, data: bytes) -> None:
        if self._fd is None:
            self._fd = os.open(
                self._cache.data_path(self._entry.version_id), os.O_WRONLY
            )
        view = memoryview(data)
        while view:
            n = os.pwrite(self._fd, view, self._offset)
            self._offset += n
            view = view[n:]

    async def _finish(self) -> None:
        if self._fd is not None:
            await asyncio.to_thread(os.close, self._fd)
            self._fd = None
        self._entry.writing = False
        self._cache._save_index()

    async def abort(self) -> None:
        """Drop the entry; nothing of it is trusted any more."""
        self.failed = True
        if self._fd is not None:
            try:
                await asyncio.to_thread(os.close, self._fd)
            except OSError:
                pass
            self._fd = None
        self._entry.writing = False
        self._cache.remove(self._entry.version_id)

    async def close(self) -> None:
        """Called when the source is exhausted; a short write is discarded."""
        if self.failed:
            return
        if self._offset >= self._entry.size:
            if self._fd is not None:
                await self._finish()
            return
        logger.warning(
            f"Cache: {self._entry.version_id} ended after {self._offset} of "
            f"{self._entry.size} bytes; dropping the entry"
        )
        await self.abort()


@dataclass
class StagedFileMessage(UploadableFileMessage):
    """An upload message whose reads are copied into the cache as they go.

    Wraps the message the uploader consumes; every ``read`` also lands in
    the staging writer, so the client's upload streams to the primary at
    full speed while the copy on disk grows alongside it.
    """

    inner: UploadableFileMessage = field(init=False)
    writer: StagingWriter = field(init=False)

    @classmethod
    def wrap(
        cls, inner: UploadableFileMessage, writer: StagingWriter
    ) -> "StagedFileMessage":
        obj = cls(
            name=inner.name,
            size=inner.get_size(),
            caption=inner.caption,
            tags=inner.tags,
            _offset=0,
            _read_size=0,
            task_tracker=inner.task_tracker,
            version_id=inner.version_id,
        )
        obj.inner = inner
        obj.writer = writer
        return obj

    async def open(self) -> None:
        await self.inner.open()

    async def read(self, length: int) -> bytes:
        data = await self.inner.read(length)
        if data:
            await self.writer.write(data)
        return data

    async def close(self) -> None:
        await self.inner.close()

    def file_name(self) -> str:
        return self.name or self.inner.file_name()

    def next_part(self, part_size: int) -> None:
        self.inner.next_part(part_size)
        self._offset += part_size
        self._read_size = 0


class CachedVersionBytes:
    """``VersionBytes`` over a complete cache entry."""

    def __init__(self, cache: "LocalCache", entry: CacheEntry):
        self._cache = cache
        self._entry = entry

    @property
    def version_id(self) -> str:
        return self._entry.version_id

    @property
    def size(self) -> int:
        return self._entry.size

    def read(self, begin: int, end: int) -> AsyncIterator[bytes]:
        return self._cache.read(self._entry.version_id, begin, end)

    async def read_bytes(self, begin: int, end: int) -> bytes:
        return await self._cache.read_bytes(self._entry.version_id, begin, end)


@dataclass
class FileMessageFromCache(UploadableFileMessage):
    """An upload message over a complete cache entry.

    Seekable, unlike a stream: ``part`` cuts out an independent message
    for one part, which is what lets a store upload several parts of the
    same version at once.
    """

    source: "VersionBytes" = field(init=False)
    begin: int = field(default=0, init=False)

    @classmethod
    def new(
        cls,
        source: "VersionBytes",
        name: str,
        begin: int = 0,
        size: Optional[int] = None,
    ) -> "FileMessageFromCache":
        obj = cls(
            name=name,
            size=source.size - begin if size is None else size,
            caption="",
            tags=FileTags(),
            _offset=0,
            _read_size=0,
            task_tracker=None,
        )
        obj.source = source
        obj.begin = begin
        return obj

    @property
    def seekable(self) -> bool:
        return True

    def part(self, offset: int, size: int) -> "FileMessageFromCache":
        """An independent message for ``size`` bytes from ``offset`` of this one."""
        return FileMessageFromCache.new(
            self.source, self.name, begin=self.begin + offset, size=size
        )

    async def read(self, length: int) -> bytes:
        position = self.begin + self._offset + self._read_size
        end = min(position + length, self.begin + self.get_size()) - 1
        if end < position:
            return b""
        read_bytes = getattr(self.source, "read_bytes", None)
        if read_bytes is not None:
            data = await read_bytes(position, end)
        else:
            data = b"".join([c async for c in self.source.read(position, end)])
        self._read_size += len(data)
        return data


class LocalCache:
    def __init__(self, config: CacheConfig, directory: Optional[str] = None):
        self.config = config
        self.directory = directory or config.directory
        self._entries: Dict[str, CacheEntry] = {}
        self._last_refusal_log = 0.0
        self.hits = 0
        self.misses = 0
        self.last_sweep: Optional[float] = None

    # -- paths and index ---------------------------------------------------

    def data_path(self, version_id: str) -> str:
        return os.path.join(self.directory, f"{version_id}.bin")

    def map_path(self, version_id: str) -> str:
        return os.path.join(self.directory, f"{version_id}.map")

    @property
    def index_path(self) -> str:
        return os.path.join(self.directory, INDEX_FILE)

    def load(self) -> None:
        """Read the index and the block maps; rebuild what the index lacks.

        A data file without an index entry, or without a map, is removed:
        nothing can say which of its bytes are real.
        """
        os.makedirs(self.directory, exist_ok=True)
        index: dict = {}
        try:
            with open(self.index_path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
            if loaded.get("version") == INDEX_VERSION:
                index = loaded.get("entries") or {}
        except FileNotFoundError:
            pass
        except Exception as ex:
            logger.error(f"Cache: could not read {self.index_path} ({ex}); rebuilding")

        self._entries = {}
        for version_id, data in index.items():
            entry = self._load_entry(version_id, data)
            if entry is not None:
                self._entries[version_id] = entry
        self._sweep_orphans()
        self._save_index()
        logger.info(
            f"Cache: {len(self._entries)} entr{'y' if len(self._entries) == 1 else 'ies'}, "
            f"{self.used_bytes()} of {self.config.max_size_bytes or 'unlimited'} bytes "
            f"in {self.directory}"
        )

    def _load_entry(self, version_id: str, data: dict) -> Optional[CacheEntry]:
        try:
            size = int(data["size"])
            block_size = int(data["block_size"])
            with open(self.map_path(version_id), "rb") as fh:
                present = bytearray(fh.read())
            if len(present) != blocks_for(size, block_size):
                raise ValueError("block map does not match the size")
            if os.path.getsize(self.data_path(version_id)) < size:
                raise ValueError("data file is shorter than the entry")
            mtime = os.path.getmtime(self.data_path(version_id))
        except Exception as ex:
            logger.warning(f"Cache: dropping entry {version_id}: {ex}")
            self._unlink(version_id)
            return None
        # An entry that was still being written when the process died holds
        # an unknown tail and is dropped like any short write. Pins survive
        # the restart: the replication queue that will release them does.
        if data.get("writing"):
            self._unlink(version_id)
            return None
        pinned = int(data.get("pins", 0) or 0) > 0
        return CacheEntry(
            version_id=version_id,
            fs=str(data.get("fs", "")),
            size=size,
            block_size=block_size,
            present=present,
            last_access=float(data.get("last_access", mtime)),
            pins=1 if pinned else 0,
            pinned_at=float(data.get("pinned_at") or mtime) if pinned else 0.0,
        )

    def _sweep_orphans(self) -> int:
        """Remove data and map files no entry refers to; returns how many."""
        try:
            names = os.listdir(self.directory)
        except OSError:
            return 0
        removed = 0
        for name in names:
            stem, dot, ext = name.rpartition(".")
            if ext not in ("bin", "map") or not dot:
                continue
            if stem not in self._entries:
                try:
                    os.remove(os.path.join(self.directory, name))
                    removed += 1
                except OSError:
                    pass
        return removed

    def _save_index(self) -> None:
        data = {
            "version": INDEX_VERSION,
            "entries": {
                version_id: {**entry.to_dict(), "writing": entry.writing}
                for version_id, entry in self._entries.items()
            },
        }
        try:
            os.makedirs(self.directory, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=self.directory, prefix=".index-")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh)
            os.replace(tmp, self.index_path)
        except OSError as ex:
            logger.warning(f"Cache: could not write {self.index_path}: {ex}")

    # -- lookups -------------------------------------------------------------

    def entry(self, version_id: str) -> Optional[CacheEntry]:
        return self._entries.get(version_id)

    def complete(self, version_id: str) -> bool:
        entry = self._entries.get(version_id)
        return entry is not None and not entry.writing and entry.is_complete()

    def has_range(self, version_id: str, begin: int, end: int) -> bool:
        entry = self._entries.get(version_id)
        return entry is not None and entry.has_range(begin, end)

    def local(self, version_id: str) -> Optional[VersionBytes]:
        """The version as a seekable source, when it is complete on disk."""
        entry = self._entries.get(version_id)
        if entry is None or entry.writing or not entry.is_complete():
            return None
        return CachedVersionBytes(self, entry)

    def used_bytes(self) -> int:
        return sum(entry.charged_bytes() for entry in self._entries.values())

    def stats(self) -> dict:
        pinned = [e for e in self._entries.values() if e.pins > 0]
        return {
            "enabled": self.config.enabled,
            "directory": self.directory,
            "entries": len(self._entries),
            "complete_entries": sum(
                1 for e in self._entries.values() if e.is_complete() and not e.writing
            ),
            "used_bytes": self.used_bytes(),
            "max_size_bytes": self.config.max_size_bytes,
            "max_files": self.config.max_files,
            "pinned_entries": len(pinned),
            "pinned_bytes": sum(e.charged_bytes() for e in pinned),
            "writing_entries": sum(1 for e in self._entries.values() if e.writing),
            "hits": self.hits,
            "misses": self.misses,
            "disk_free_bytes": self._disk_free(),
            "min_free_bytes": self.config.min_free_bytes,
            "target_bytes": self.config.target_bytes,
            "max_age_hours": self.config.max_age_hours,
            "last_sweep": self.last_sweep,
            "per_filesystem": self._per_filesystem(),
        }

    def _per_filesystem(self) -> Dict[str, dict]:
        res: Dict[str, dict] = {}
        for entry in self._entries.values():
            fs = res.setdefault(entry.fs, {"entries": 0, "bytes": 0, "pinned": 0})
            fs["entries"] += 1
            fs["bytes"] += entry.charged_bytes()
            if entry.pins > 0:
                fs["pinned"] += 1
        return res

    # -- budget ---------------------------------------------------------------

    def _disk_free(self) -> Optional[int]:
        """Free bytes on the disk that holds the cache; ``None`` if unknown."""
        try:
            return shutil.disk_usage(self.directory).free
        except OSError:
            return None

    def _lru_order(self, keep: Optional[CacheEntry] = None) -> List[CacheEntry]:
        """The evictable entries, the one read longest ago *last*, so an
        eviction run pops its victims off the end.

        Sorted once per run: picking each victim by scanning every entry
        made a run over a cache of thousands of small entries quadratic.
        """
        victims = [e for e in self._entries.values() if e.evictable and e is not keep]
        victims.sort(key=lambda e: e.last_access, reverse=True)
        return victims

    def _evict(self, victim: CacheEntry, why: str) -> int:
        """Drop ``victim`` without writing the index; returns the bytes it
        was charged. The caller saves the index once its run is over."""
        charged = victim.charged_bytes()
        logger.debug(f"Cache: evicting {victim.version_id} ({charged} bytes) {why}")
        self._drop(victim.version_id)
        return charged

    @staticmethod
    def _log_evictions(count: int, freed: int, why: str) -> None:
        if count:
            logger.info(f"Cache: evicted {count} entries ({format_bytes(freed)}) {why}")

    def _fits(self, size: int) -> bool:
        cfg = self.config
        if cfg.max_file_size_bytes and size > cfg.max_file_size_bytes:
            return False
        return True

    def _make_room(
        self, size: int, new_entry: bool = True, keep: Optional[CacheEntry] = None
    ) -> Optional[str]:
        """Evict unpinned entries, oldest first, until ``size`` more bytes fit
        the budget and leave ``min_free`` on the disk.

        ``new_entry`` also claims one of ``max_files``; ``keep`` is the
        entry the bytes are for, which is never its own victim. Entries
        being written or filled right now are not victims either.

        Returns ``None`` once the bytes fit, otherwise why they cannot:
        which limit is hit and what holds the room that is left, so the
        log tells a cache full of pinned entries from a disk filled by
        something else.
        """
        cfg = self.config
        # Everything here runs on the event loop: one pass over a sorted
        # snapshot, the used bytes kept as a running total and the index
        # written once at the end, so making room for one large upload in
        # a cache of thousands of small entries does not stall every
        # other request.
        used = self.used_bytes()
        victims: Optional[List[CacheEntry]] = None
        evicted = {"to make room": [0, 0], "for the disk headroom": [0, 0]}
        try:
            while True:
                over_bytes = bool(
                    cfg.max_size_bytes and used + size > cfg.max_size_bytes
                )
                over_files = bool(
                    new_entry
                    and cfg.max_files
                    and len(self._entries) + 1 > cfg.max_files
                )
                over_disk = False
                free: Optional[int] = None
                if cfg.min_free_bytes:
                    free = self._disk_free()
                    over_disk = free is not None and free - size < cfg.min_free_bytes
                if not over_bytes and not over_files and not over_disk:
                    return None
                if victims is None:
                    victims = self._lru_order(keep)
                if not victims:
                    return self._shortfall(size, over_bytes, over_files, free, keep)
                why = "for the disk headroom" if over_disk else "to make room"
                freed = self._evict(victims.pop(), why)
                used -= freed
                evicted[why][0] += 1
                evicted[why][1] += freed
        finally:
            if any(count for count, _ in evicted.values()):
                self._save_index()
                for why, (count, freed) in evicted.items():
                    self._log_evictions(count, freed, why)

    def _shortfall(
        self,
        size: int,
        over_bytes: bool,
        over_files: bool,
        free: Optional[int],
        keep: Optional[CacheEntry] = None,
    ) -> str:
        """Why ``size`` more bytes do not fit once nothing is left to evict."""
        cfg = self.config
        limits = []
        if over_bytes:
            limits.append(
                f"the budget is met ({format_bytes(self.used_bytes())} of "
                f"{format_bytes(cfg.max_size_bytes)} used, "
                f"{format_bytes(size)} more needed)"
            )
        if over_files:
            limits.append(f"max_files is met ({len(self._entries)} entries)")
        if free is not None and free - size < cfg.min_free_bytes:
            limits.append(
                f"the disk headroom is met ({format_bytes(free)} free, "
                f"min_free {format_bytes(cfg.min_free_bytes)}, "
                f"{format_bytes(size)} more needed)"
            )
        pinned = [e for e in self._entries.values() if e.pins > 0]
        # ``keep`` is the entry being filled, so it counts as written to.
        busy = [
            e
            for e in self._entries.values()
            if e.pins == 0 and (not e.evictable or e is keep)
        ]
        if not pinned and not busy:
            holders = (
                "the cache holds nothing it could evict, so the disk is "
                "filled outside the cache"
            )
        else:
            holders = (
                f"nothing is left to evict: {len(pinned)} entries "
                f"({format_bytes(sum(e.charged_bytes() for e in pinned))}) are "
                f"pinned until the mirrors have them, {len(busy)} entries "
                f"({format_bytes(sum(e.charged_bytes() for e in busy))}) are "
                f"being written"
            )
        return f"{' and '.join(limits)}; {holders}"

    def _refuse(self, reason: str) -> None:
        self._throttled_warning(f"Cache: not caching a version: {reason}")

    def _throttled_warning(self, message: str) -> None:
        """One warning per ``REFUSAL_LOG_INTERVAL``: a full cache or a full
        disk would otherwise write a line per transfer."""
        now = time.monotonic()
        if now - self._last_refusal_log >= REFUSAL_LOG_INTERVAL:
            self._last_refusal_log = now
            logger.warning(message)

    # -- entries ---------------------------------------------------------------

    def open_staging(
        self, fs: str, version_id: str, size: int
    ) -> Optional[StagingWriter]:
        """Start an entry for ``version_id`` to be written front to back.

        The whole version lands, so the whole size is charged up front:
        ``None`` when the cache is off, the version is above
        ``max_file_size`` or the budget cannot be met without evicting
        pinned entries. Read-cache fills go through :meth:`reserve`.
        """
        if not self.config.enabled or size < 0:
            return None
        if version_id in self._entries:
            self.remove(version_id)
        if not self._fits(size):
            self._refuse(f"{size} bytes exceed max_file_size")
            return None
        if (why := self._make_room(size)) is not None:
            self._throttled_warning(f"Cache: not caching {version_id}: {why}")
            return None
        entry = CacheEntry(
            version_id=version_id,
            fs=fs,
            size=size,
            block_size=self.config.block_bytes,
            present=bytearray(blocks_for(size, self.config.block_bytes)),
            last_access=time.time(),
            writing=True,
        )
        if not self._create_files(entry):
            return None
        self._entries[version_id] = entry
        self._save_index()
        return StagingWriter(self, entry)

    def _create_files(self, entry: CacheEntry) -> bool:
        """The sparse data file at the version's size and the empty block
        map; ``False`` (with the files removed) when the disk refuses."""
        try:
            os.makedirs(self.directory, exist_ok=True)
            with open(self.data_path(entry.version_id), "wb") as fh:
                fh.truncate(entry.size)
            with open(self.map_path(entry.version_id), "wb") as fh:
                fh.write(bytes(entry.present))
        except OSError as ex:
            self._refuse(f"cannot create files in {self.directory}: {ex}")
            self._unlink(entry.version_id)
            return False
        return True

    def _mark_present(self, entry: CacheEntry, blocks: Iterable[int]) -> None:
        if self._entries.get(entry.version_id) is not entry:
            return  # evicted while the bytes were on their way
        changed = [i for i in blocks if not entry.present[i]]
        if not changed:
            return
        for i in changed:
            entry.present[i] = 1
        try:
            fd = os.open(self.map_path(entry.version_id), os.O_WRONLY)
            try:
                for i in changed:
                    os.pwrite(fd, b"\x01", i)
            finally:
                os.close(fd)
        except OSError as ex:
            logger.warning(
                f"Cache: could not update the block map of {entry.version_id}: {ex}"
            )

    async def write_blocks(self, version_id: str, begin: int, data: bytes) -> None:
        """Store whole blocks starting at the block-aligned offset ``begin``.

        For read-cache fills. Only blocks the data covers completely (or
        the final block of the version) are marked present; a short tail
        is written but not trusted.
        """
        entry = self._entries.get(version_id)
        if entry is None or entry.writing or not data:
            return
        if begin % entry.block_size != 0 or begin + len(data) > entry.size:
            return
        first = begin // entry.block_size
        end_offset = begin + len(data)
        last_full = end_offset // entry.block_size  # exclusive
        if end_offset >= entry.size:
            last_full = entry.blocks
        blocks = range(first, last_full)
        if not blocks:
            return  # no whole block to keep; never write bytes the budget ignores
        # Claim the budget before the bytes reach the disk: the write
        # suspends, and an entry admitted meanwhile must not count on
        # the same room. Blocks already present are already charged.
        added = sum(entry.block_length(i) for i in blocks if not entry.present[i])
        why = self._make_room(added, new_entry=False, keep=entry) if added else None
        if why is not None:
            self._throttled_warning(f"Cache: {version_id} is only partly cached: {why}")
            return
        entry.inflight += added
        try:
            await asyncio.to_thread(
                self._pwrite, self.data_path(version_id), begin, data
            )
        except OSError as ex:
            self._throttled_warning(
                f"Cache: writing blocks of {version_id} failed ({ex}); "
                f"reads go on without the cache"
            )
            entry.inflight -= added
            self.remove(version_id)
            return
        entry.inflight -= added
        self._mark_present(entry, blocks)
        entry.last_access = time.time()

    @staticmethod
    def _pwrite(path: str, offset: int, data: bytes) -> None:
        fd = os.open(path, os.O_WRONLY)
        try:
            view = memoryview(data)
            while view:
                n = os.pwrite(fd, view, offset)
                offset += n
                view = view[n:]
        finally:
            os.close(fd)

    def reserve(self, fs: str, version_id: str, size: int) -> Optional[CacheEntry]:
        """An empty entry for read-cache fills; ``None`` when the cache is off
        or the entry cannot be created.

        Admission is per block, not per version: nothing is charged here,
        every block claims its own bytes in :meth:`write_blocks`, so a
        version larger than ``max_file_size`` or than the whole budget is
        still cached for the blocks a reader actually touches. The data
        file is sparse, its logical size is the version's. ``max_files``
        still bounds the entries.
        """
        if not self.config.enabled or size < 0:
            return None
        if version_id in self._entries:
            self.remove(version_id)
        if (why := self._make_room(0)) is not None:
            self._throttled_warning(
                f"Cache: {version_id} is read without the cache: {why}"
            )
            return None
        entry = CacheEntry(
            version_id=version_id,
            fs=fs,
            size=size,
            block_size=self.config.block_bytes,
            present=bytearray(blocks_for(size, self.config.block_bytes)),
            last_access=time.time(),
        )
        if not self._create_files(entry):
            return None
        self._entries[version_id] = entry
        self._save_index()
        return entry

    async def read(self, version_id: str, begin: int, end: int) -> AsyncIterator[bytes]:
        """Stream ``[begin, end]`` of an entry; the range must be present."""
        entry = self._entries.get(version_id)
        if entry is None:
            raise KeyError(version_id)
        if end < 0 or end >= entry.size:
            end = entry.size - 1
        if not entry.has_range(begin, end):
            raise KeyError(f"{version_id}: bytes {begin}-{end} are not cached")
        entry.last_access = time.time()
        offset = begin
        path = self.data_path(version_id)
        while offset <= end:
            length = min(READ_CHUNK, end - offset + 1)
            chunk = await asyncio.to_thread(self._pread, path, offset, length)
            if not chunk:
                raise OSError(f"{path}: short read at {offset}")
            offset += len(chunk)
            yield chunk

    async def read_bytes(self, version_id: str, begin: int, end: int) -> bytes:
        """``[begin, end]`` of an entry in one piece; the range must be present."""
        return b"".join([chunk async for chunk in self.read(version_id, begin, end)])

    @staticmethod
    def _pread(path: str, offset: int, length: int) -> bytes:
        fd = os.open(path, os.O_RDONLY)
        try:
            return os.pread(fd, length, offset)
        finally:
            os.close(fd)

    def pin(self, version_id: str) -> None:
        if (entry := self._entries.get(version_id)) is not None:
            if entry.pins == 0:
                entry.pinned_at = time.time()
            entry.pins += 1
            self._save_index()

    def unpin(self, version_id: str) -> None:
        if (entry := self._entries.get(version_id)) is not None:
            entry.pins = max(0, entry.pins - 1)

    def release(self, version_id: str) -> None:
        """The mirrors are done with a version: unpin it, and drop it unless
        the entry is kept for reads."""
        entry = self._entries.get(version_id)
        if entry is None:
            return
        entry.pins = 0
        entry.pinned_at = 0.0
        if not self.config.keep_for_reads:
            self.remove(version_id)
        else:
            self._save_index()

    def remove(self, version_id: str) -> None:
        if self._drop(version_id):
            self._save_index()

    def remove_many(self, version_ids: Iterable[str]) -> None:
        """Remove several entries and write the index once."""
        dropped = [self._drop(version_id) for version_id in list(version_ids)]
        if any(dropped):
            self._save_index()

    def _drop(self, version_id: str) -> bool:
        """Forget an entry and delete its files, leaving the index to the
        caller; ``False`` when there was no such entry."""
        if self._entries.pop(version_id, None) is None:
            return False
        self._unlink(version_id)
        return True

    def _unlink(self, version_id: str) -> None:
        for path in (self.data_path(version_id), self.map_path(version_id)):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
            except OSError as ex:
                logger.warning(f"Cache: could not remove {path}: {ex}")

    def evict_unpinned(self) -> int:
        """Drop every entry no mirror is waiting for; returns how many."""
        victims = [e.version_id for e in self._entries.values() if e.evictable]
        self.remove_many(victims)
        return len(victims)

    # -- sweep -----------------------------------------------------------------

    def sweep(
        self,
        queue_empty: Optional[Callable[[str], bool]] = None,
        now: Optional[float] = None,
    ) -> dict:
        """The housekeeping that admission does not do on its own.

        Orphaned files are removed; a pin older than ``STALE_PIN_SECONDS``
        is released when ``queue_empty`` says nothing is queued for its
        file system (or when no queue is known); entries unread for
        ``max_age`` are dropped; then the cache is evicted, oldest first,
        down to ``target_fill`` of the budget and to ``min_free`` on the
        disk. Returns what was done, for the log and the API.
        """
        cfg = self.config
        now = time.time() if now is None else now
        report = {"orphans": 0, "released": 0, "expired": 0, "evicted": 0}
        report["orphans"] = self._sweep_orphans()

        for entry in list(self._entries.values()):
            if (
                entry.pins > 0
                and not entry.writing
                and now - entry.pinned_at >= STALE_PIN_SECONDS
                and (queue_empty is None or queue_empty(entry.fs))
            ):
                logger.info(
                    f"Cache: releasing the pin on {entry.version_id}: nothing is "
                    f"queued for '{entry.fs}' and the pin is a day old"
                )
                self.release(entry.version_id)
                report["released"] += 1

        if cfg.max_age_seconds:
            for entry in list(self._entries.values()):
                if entry.evictable and now - entry.last_access >= cfg.max_age_seconds:
                    self._evict(entry, f"unread for {cfg.max_age_hours} h")
                    report["expired"] += 1

        # One sorted snapshot for both passes, the used bytes as a running
        # total and the index written once below, as in ``_make_room``.
        victims = self._lru_order()
        target = cfg.target_bytes
        used = self.used_bytes()
        while target is not None and used > target and victims:
            used -= self._evict(
                victims.pop(), f"down to {cfg.target_fill_percent}% of the budget"
            )
            report["evicted"] += 1

        while cfg.min_free_bytes and victims:
            free = self._disk_free()
            if free is None or free >= cfg.min_free_bytes:
                break
            self._evict(victims.pop(), "for the disk headroom")
            report["evicted"] += 1

        self.last_sweep = now
        self._save_index()
        if any(report.values()):
            logger.info(
                f"Cache sweep: {report['orphans']} orphaned files removed, "
                f"{report['released']} stale pins released, {report['expired']} "
                f"entries expired, {report['evicted']} evicted; "
                f"{self.used_bytes()} bytes in {len(self._entries)} entries"
            )
        return report


class CacheSweeper:
    """Runs :meth:`LocalCache.sweep` in the background, every ``interval``
    seconds and once right after start. Runs on the event loop: the cache
    is not thread-safe and a sweep is quick."""

    def __init__(
        self,
        cache: LocalCache,
        queue_empty: Optional[Callable[[str], bool]] = None,
        interval: float = SWEEP_INTERVAL,
    ):
        self._cache = cache
        self._queue_empty = queue_empty
        self._interval = interval
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="cache-sweeper")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def run_once(self) -> dict:
        return self._cache.sweep(self._queue_empty)

    async def run(self) -> None:
        while True:
            try:
                self.run_once()
            except Exception:
                logger.exception("Cache sweep failed")
            await asyncio.sleep(self._interval)


class VersionCache:
    """The cache as seen by one version being written or mirrored.

    Bundles what the replication engine needs: the local copy when there
    is one, a writer that fills the cache from a download when there is
    none yet, and the release once every mirror has its copy.
    """

    def __init__(self, cache: LocalCache, fs: str, version_id: str, size: int):
        self._cache = cache
        self._fs = fs
        self.version_id = version_id
        self.size = size
        self._filler: Optional[StagingWriter] = None

    @property
    def cache(self) -> LocalCache:
        return self._cache

    def local(self) -> Optional[VersionBytes]:
        return self._cache.local(self.version_id)

    def filler(self) -> Optional[StagingWriter]:
        """A writer to fill the cache from a download, when worth keeping."""
        if not self._cache.config.keep_for_reads or self.size <= 0:
            return None
        if self._cache.entry(self.version_id) is not None:
            return None
        self._filler = self._cache.open_staging(self._fs, self.version_id, self.size)
        if self._filler is not None:
            self._cache.pin(self.version_id)
        return self._filler

    def release(self) -> None:
        self._cache.release(self.version_id)


def version_cache_for(
    cache: Optional[LocalCache], fs: str, version_id: Optional[str], size: int
) -> Optional[VersionCache]:
    """The version's view of the cache; ``None`` when there is nothing to
    cache under: the cache is off, or the bytes are no version (the
    metadata blob travels through the same repository)."""
    if cache is None or not cache.config.enabled or not version_id or size <= 0:
        return None
    return VersionCache(cache, fs, version_id, size)


CacheProvider = Callable[[], Optional[LocalCache]]
