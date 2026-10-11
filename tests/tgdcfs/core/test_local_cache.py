"""The local cache: entries on disk, the budget, and mirrors reading from it.

The unit tests drive ``LocalCache`` directly against a temporary
directory. The integration tests run the real ``FileApi`` /
``StoreFileContentRepository`` / ``MirrorGroup`` stack on in-memory
stores, the way ``test_replication.py`` does, and check that a staged
upload spares the mirror every download from the primary.
"""

import asyncio
import os
import time
from typing import cast

import pytest

from tgdcfs.backends.base import IStore
from tgdcfs.config import CacheConfig
from tgdcfs.core.api import DirectoryApi, FileApi, FileDescApi, MetaDataApi
from tgdcfs.core.local_cache import (
    STALE_PIN_SECONDS,
    CacheEntry,
    CacheSweeper,
    LocalCache,
    StagedFileMessage,
    format_bytes,
)
from tgdcfs.core.mirror import MirrorGroup, MirrorStore
from tgdcfs.core.model import TGFSDirectory, TGFSFileVersion
from tgdcfs.core.replication import ReplicationQueue, ReplicationWorker, file_path
from tgdcfs.core.repository.impl import (
    PinnedMessageMetadataRepository,
    StoreFDRepository,
    StoreFileContentRepository,
)
from tgdcfs.crypto.repository import EncryptingFileContentRepository
from tgdcfs.reqres import FileMessageFromBuffer
from tests.fakes.store import FakeStore

BLOCK = 64 * 1024  # the smallest block the config accepts


def cache_config(**overrides) -> CacheConfig:
    data = {
        "enabled": True,
        "max_size_mb": 0,
        "max_files": 0,
        "max_file_size_mb": 0,
        "block_kb": 64,
        "stage_uploads": True,
        "keep_for_reads": True,
    }
    data.update(overrides)
    return CacheConfig.from_dict(data)


def make_cache(tmp_path, **overrides) -> LocalCache:
    cache = LocalCache(cache_config(**overrides), directory=str(tmp_path / "cache"))
    cache.load()
    return cache


async def stage(cache: LocalCache, version_id: str, data: bytes, fs: str = "fs"):
    writer = cache.open_staging(fs, version_id, len(data))
    assert writer is not None
    # Chunks that do not line up with the blocks, as a network stream.
    for i in range(0, len(data), 100_000):
        await writer.write(data[i : i + 100_000])
    await writer.close()
    return writer


async def read_all(cache: LocalCache, version_id: str, begin=0, end=-1) -> bytes:
    return b"".join([c async for c in cache.read(version_id, begin, end)])


def entry_of(cache: LocalCache, version_id: str) -> CacheEntry:
    entry = cache.entry(version_id)
    assert entry is not None
    return entry


class TestEntries:
    async def test_staged_bytes_come_back(self, tmp_path):
        cache = make_cache(tmp_path)
        data = os.urandom(3 * BLOCK + 123)

        await stage(cache, "v1", data)

        assert cache.complete("v1")
        assert await read_all(cache, "v1") == data
        assert (
            await read_all(cache, "v1", BLOCK + 5, 2 * BLOCK + 7)
            == data[BLOCK + 5 : 2 * BLOCK + 8]
        )

    async def test_blocks_become_present_as_the_write_advances(self, tmp_path):
        cache = make_cache(tmp_path)
        writer = cache.open_staging("fs", "v1", 3 * BLOCK)
        assert writer is not None

        await writer.write(b"a" * (BLOCK + 10))

        entry = cache.entry("v1")
        assert entry is not None
        assert list(entry.present) == [1, 0, 0]
        assert not cache.complete("v1")
        assert cache.has_range("v1", 0, BLOCK - 1)
        assert not cache.has_range("v1", 0, BLOCK)

    async def test_a_short_write_is_dropped_on_close(self, tmp_path):
        cache = make_cache(tmp_path)
        writer = cache.open_staging("fs", "v1", 2 * BLOCK)
        assert writer is not None
        await writer.write(b"a" * BLOCK)

        await writer.close()

        assert cache.entry("v1") is None
        assert not os.path.exists(cache.data_path("v1"))

    async def test_entries_survive_a_restart(self, tmp_path):
        cache = make_cache(tmp_path)
        data = os.urandom(2 * BLOCK)
        await stage(cache, "v1", data)
        partial = cache.open_staging("fs", "v2", 2 * BLOCK)
        assert partial is not None
        await partial.write(b"b" * (2 * BLOCK))
        await partial.close()

        reloaded = make_cache(tmp_path)

        assert reloaded.complete("v1") and reloaded.complete("v2")
        assert await read_all(reloaded, "v1") == data
        # Pins are not persisted; the replication queue re-derives them.
        assert entry_of(reloaded, "v1").pins == 0

    async def test_an_entry_still_being_written_is_dropped_on_restart(self, tmp_path):
        cache = make_cache(tmp_path)
        writer = cache.open_staging("fs", "v1", 2 * BLOCK)
        assert writer is not None
        await writer.write(b"a" * BLOCK)

        reloaded = make_cache(tmp_path)

        assert reloaded.entry("v1") is None
        assert not os.path.exists(cache.data_path("v1"))

    async def test_files_without_an_index_entry_are_swept(self, tmp_path):
        cache = make_cache(tmp_path)
        (tmp_path / "cache" / "orphan.bin").write_bytes(b"x")
        (tmp_path / "cache" / "orphan.map").write_bytes(b"\x01")
        (tmp_path / "cache" / "notes.txt").write_text("keep")

        make_cache(tmp_path)

        assert not (tmp_path / "cache" / "orphan.bin").exists()
        assert (tmp_path / "cache" / "notes.txt").exists()

    async def test_disabled_cache_stages_nothing(self, tmp_path):
        cache = make_cache(tmp_path, enabled=False)
        assert cache.open_staging("fs", "v1", 10) is None

    async def test_a_disk_error_disables_the_writer_not_the_upload(
        self, tmp_path, monkeypatch
    ):
        cache = make_cache(tmp_path)
        writer = cache.open_staging("fs", "v1", 2 * BLOCK)
        assert writer is not None

        def broken_pwrite(*args, **kwargs):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(os, "pwrite", broken_pwrite)
        await writer.write(b"a" * BLOCK)

        assert writer.failed
        assert cache.entry("v1") is None
        # Further writes are silently dropped.
        await writer.write(b"b" * BLOCK)
        await writer.close()


class TestBudget:
    async def test_lru_eviction_makes_room(self, tmp_path):
        cache = make_cache(tmp_path, max_size_mb=1, block_kb=256)
        quarter = 256 * 1024
        await stage(cache, "old", b"a" * quarter)
        await stage(cache, "mid", b"b" * quarter)
        await stage(cache, "new", b"c" * quarter)
        entry_of(cache, "old").last_access = 1
        entry_of(cache, "mid").last_access = 2
        entry_of(cache, "new").last_access = 3

        writer = cache.open_staging("fs", "big", 2 * quarter)

        assert writer is not None
        assert cache.entry("old") is None
        assert cache.entry("mid") is not None and cache.entry("new") is not None

    async def test_making_room_writes_the_index_once(
        self, tmp_path, monkeypatch, caplog
    ):
        """A large upload into a cache full of small entries evicts many of
        them; the index used to be rewritten after every single one, which
        held the event loop for minutes and timed the clients out."""
        cache = make_cache(tmp_path, min_free_mb=1)
        for i in range(200):
            await stage(cache, f"small-{i}", b"s" * 10)
            entry_of(cache, f"small-{i}").last_access = i
        saves = []
        save_index = cache._save_index

        def counting_save():
            saves.append(1)
            save_index()

        monkeypatch.setattr(cache, "_save_index", counting_save)
        # Each eviction frees its 10 bytes; 150 of them make the headroom.
        free = 1024 * 1024 + 100

        def freeing():
            return free + 10 * (200 - len(cache._entries))

        monkeypatch.setattr(cache, "_disk_free", freeing)

        with caplog.at_level("INFO", logger="tgdcfs.core.local_cache"):
            writer = cache.open_staging("fs", "big", 1600)

        assert writer is not None
        assert cache.entry("small-149") is None
        assert cache.entry("small-150") is not None
        # One write for the evictions, one for the new entry.
        assert len(saves) == 2
        assert "Cache: evicted 150 entries (1.5 KiB) for the disk headroom" in (
            caplog.text
        )
        assert "evicting" not in caplog.text

    async def test_removing_many_entries_survives_a_restart(self, tmp_path):
        cache = make_cache(tmp_path, max_files=3)
        for i in range(3):
            await stage(cache, f"v{i}", b"v" * 10)
            entry_of(cache, f"v{i}").last_access = i

        cache.remove_many(["v0", "v1"])
        await stage(cache, "w", b"w" * 10)

        reloaded = make_cache(tmp_path, max_files=3)
        assert sorted(reloaded._entries) == ["v2", "w"]

    async def test_pinned_entries_are_never_evicted(self, tmp_path):
        cache = make_cache(tmp_path, max_size_mb=1, block_kb=256)
        await stage(cache, "pinned", b"a" * (768 * 1024))
        cache.pin("pinned")

        assert cache.open_staging("fs", "next", 512 * 1024) is None
        assert cache.entry("pinned") is not None

        cache.release("pinned")
        assert cache.open_staging("fs", "next", 512 * 1024) is not None

    async def test_max_files(self, tmp_path):
        cache = make_cache(tmp_path, max_files=2)
        await stage(cache, "a", b"a" * 10)
        await stage(cache, "b", b"b" * 10)
        entry_of(cache, "a").last_access = 1

        await stage(cache, "c", b"c" * 10)

        assert cache.entry("a") is None
        assert cache.entry("b") is not None and cache.entry("c") is not None

    async def test_read_fills_never_push_the_cache_over_the_budget(self, tmp_path):
        """Two reads reserve entries while nothing is on disk yet; filling
        them must evict, or stop caching, rather than exceed max_size."""
        cache = make_cache(tmp_path, max_size_mb=1, block_kb=256)
        quarter = 256 * 1024
        a = cache.reserve("fs", "a", 4 * quarter)
        b = cache.reserve("fs", "b", 4 * quarter)
        assert a is not None and b is not None

        for i in range(4):
            await cache.write_blocks("a", i * quarter, b"a" * quarter)
            assert cache.used_bytes() <= cache.config.max_size_bytes
        for i in range(4):
            await cache.write_blocks("b", i * quarter, b"b" * quarter)
            assert cache.used_bytes() <= cache.config.max_size_bytes

        # The older entry made room for the newer one.
        assert cache.entry("a") is None
        assert cache.complete("b")
        assert await read_all(cache, "b") == b"b" * (4 * quarter)

    async def test_a_read_fill_stops_caching_when_only_pinned_entries_remain(
        self, tmp_path
    ):
        cache = make_cache(tmp_path, max_size_mb=1, block_kb=256)
        quarter = 256 * 1024
        assert cache.reserve("fs", "a", 4 * quarter) is not None
        assert cache.reserve("fs", "b", 4 * quarter) is not None
        cache.pin("a")
        for i in range(4):
            await cache.write_blocks("a", i * quarter, b"a" * quarter)

        await cache.write_blocks("b", 0, b"b" * quarter)

        assert cache.used_bytes() == cache.config.max_size_bytes
        assert cache.complete("a")
        assert entry_of(cache, "b").present_bytes() == 0
        assert not cache.has_range("b", 0, quarter - 1)

    async def test_a_fill_in_flight_is_charged_before_it_lands(self, tmp_path):
        """The budget is claimed when a write starts, not when it is on
        disk, so a concurrent admission cannot use the same bytes."""
        cache = make_cache(tmp_path, max_size_mb=1, block_kb=256)
        quarter = 256 * 1024
        assert cache.reserve("fs", "a", 4 * quarter) is not None
        started = cache.write_blocks("a", 0, b"a" * (4 * quarter))
        task = asyncio.ensure_future(started)
        await asyncio.sleep(0)  # the write is on its way to disk

        assert cache.used_bytes() == 4 * quarter
        assert cache.open_staging("fs", "b", quarter) is None
        await task
        assert cache.used_bytes() == 4 * quarter

    async def test_the_disk_headroom_is_kept(self, tmp_path, monkeypatch):
        """min_free_mb is room for everything else in the data directory:
        an entry that would eat into it is refused before any byte lands."""
        cache = make_cache(tmp_path, min_free_mb=1)
        monkeypatch.setattr(cache, "_disk_free", lambda: 1024 * 1024 + 10)

        assert cache.open_staging("fs", "a", 100) is None
        assert cache.entry("a") is None

        monkeypatch.setattr(cache, "_disk_free", lambda: 1024 * 1024 + 200)
        assert cache.open_staging("fs", "a", 100) is not None

    async def test_the_headroom_evicts_before_refusing(self, tmp_path, monkeypatch):
        cache = make_cache(tmp_path, min_free_mb=1)
        free = 1024 * 1024 + 100
        monkeypatch.setattr(cache, "_disk_free", lambda: free)
        await stage(cache, "old", b"o" * 50)
        entry_of(cache, "old").last_access = 1

        # Only 50 bytes fit now; evicting "old" (as if freeing its bytes)
        # makes the difference.
        def freeing():
            return free + (0 if cache.entry("old") else 50)

        monkeypatch.setattr(cache, "_disk_free", freeing)
        assert cache.open_staging("fs", "new", 120) is not None
        assert cache.entry("old") is None

    async def test_an_unknown_disk_state_does_not_block(self, tmp_path, monkeypatch):
        cache = make_cache(tmp_path, min_free_mb=1)
        monkeypatch.setattr(cache, "_disk_free", lambda: None)
        assert cache.open_staging("fs", "a", 100) is not None

    async def test_a_refusal_names_the_pinned_entries(self, tmp_path, caplog):
        cache = make_cache(tmp_path, max_size_mb=1, block_kb=256)
        await stage(cache, "pinned", b"a" * (768 * 1024))
        cache.pin("pinned")

        with caplog.at_level("WARNING", logger="tgdcfs.core.local_cache"):
            assert cache.open_staging("fs", "next", 512 * 1024) is None

        assert (
            "Cache: not caching next: the budget is met (768.0 KiB of 1.0 MiB "
            "used, 512.0 KiB more needed); nothing is left to evict: 1 entries "
            "(768.0 KiB) are pinned until the mirrors have them, 0 entries "
            "(0 B) are being written"
        ) in caplog.text

    async def test_a_refusal_tells_a_disk_filled_outside_the_cache(
        self, tmp_path, monkeypatch, caplog
    ):
        cache = make_cache(tmp_path, min_free_mb=1)
        monkeypatch.setattr(cache, "_disk_free", lambda: 1024 * 1024 + 10)

        with caplog.at_level("WARNING", logger="tgdcfs.core.local_cache"):
            assert cache.open_staging("fs", "a", 100) is None

        assert (
            "Cache: not caching a: the disk headroom is met (1.0 MiB free, "
            "min_free 1.0 MiB, 100 B more needed); the cache holds nothing it "
            "could evict, so the disk is filled outside the cache"
        ) in caplog.text

    async def test_a_partial_fill_counts_its_own_entry_as_written(
        self, tmp_path, monkeypatch, caplog
    ):
        cache = make_cache(tmp_path, min_free_mb=1, block_kb=256)
        quarter = 256 * 1024
        assert cache.reserve("fs", "a", 2 * quarter) is not None
        monkeypatch.setattr(cache, "_disk_free", lambda: 1024 * 1024)

        with caplog.at_level("WARNING", logger="tgdcfs.core.local_cache"):
            await cache.write_blocks("a", 0, b"a" * quarter)

        assert "Cache: a is only partly cached: the disk headroom is met" in (
            caplog.text
        )
        assert "0 entries (0 B) are pinned" in caplog.text
        assert "1 entries (0 B) are being written" in caplog.text

    async def test_format_bytes(self):
        assert format_bytes(0) == "0 B"
        assert format_bytes(1023) == "1023 B"
        assert format_bytes(1536) == "1.5 KiB"
        assert format_bytes(20 * 1024**3) == "20.0 GiB"
        assert format_bytes(3000 * 1024**3) == "3000.0 GiB"

    async def test_a_disk_write_failure_is_warned_once_a_minute(self, tmp_path, caplog):
        cache = make_cache(tmp_path, block_kb=256)
        quarter = 256 * 1024
        for name in ("a", "b", "c"):
            assert cache.reserve("fs", name, quarter) is not None
            os.remove(cache.data_path(name))  # the write will fail
        with caplog.at_level("WARNING", logger="tgdcfs.core.local_cache"):
            for name in ("a", "b", "c"):
                await cache.write_blocks(name, 0, b"x" * quarter)

        warnings = [r for r in caplog.records if "writing blocks" in r.message]
        assert len(warnings) == 1
        assert cache.stats()["entries"] == 0

    async def test_versions_above_the_file_limit_are_not_staged(self, tmp_path):
        cache = make_cache(tmp_path, max_file_size_mb=1)
        assert cache.open_staging("fs", "huge", 2 * 1024 * 1024) is None
        assert cache.open_staging("fs", "small", 1024) is not None

    async def test_a_read_fill_ignores_the_file_limit(self, tmp_path):
        """A read fill is admitted per block: the entry costs nothing until
        a block lands, so a version above max_file_size_mb is cached for
        the blocks a reader touches."""
        cache = make_cache(tmp_path, max_file_size_mb=1, block_kb=256)
        quarter = 256 * 1024
        huge = 8 * quarter  # 2 MiB, twice the limit
        entry = cache.reserve("fs", "huge", huge)
        assert entry is not None
        assert cache.used_bytes() == 0

        await cache.write_blocks("huge", 4 * quarter, b"h" * quarter)

        assert cache.used_bytes() == quarter
        assert cache.has_range("huge", 4 * quarter, 5 * quarter - 1)
        assert not cache.has_range("huge", 0, quarter - 1)
        assert await read_all(cache, "huge", 4 * quarter, 5 * quarter - 1) == (
            b"h" * quarter
        )

    async def test_a_read_fill_of_a_version_larger_than_the_budget(self, tmp_path):
        """The budget is claimed block by block, so a version that would
        never fit whole is still cached up to the budget and read from the
        stores for the rest."""
        cache = make_cache(tmp_path, max_size_mb=1, block_kb=256)
        quarter = 256 * 1024
        assert cache.reserve("fs", "huge", 16 * quarter) is not None  # 4 MiB

        for i in range(16):
            await cache.write_blocks("huge", i * quarter, bytes([i]) * quarter)
            assert cache.used_bytes() <= cache.config.max_size_bytes

        entry = entry_of(cache, "huge")
        # The entry is never its own victim: the first four blocks stayed,
        # later ones found no room and were not written.
        assert entry.present_bytes() == 4 * quarter
        assert list(entry.present[:4]) == [1, 1, 1, 1]
        assert not any(entry.present[4:])
        assert await read_all(cache, "huge", 0, quarter - 1) == bytes([0]) * quarter

    async def test_a_read_fill_is_not_measured_against_the_headroom_whole(
        self, tmp_path, monkeypatch
    ):
        cache = make_cache(tmp_path, min_free_mb=1, block_kb=256)
        quarter = 256 * 1024
        monkeypatch.setattr(cache, "_disk_free", lambda: 1024 * 1024 + 2 * quarter)

        # A whole copy would eat into the headroom; blocks are admitted
        # one at a time until they would.
        assert cache.open_staging("fs", "staged", 16 * quarter) is None
        assert cache.reserve("fs", "huge", 16 * quarter) is not None
        await cache.write_blocks("huge", 0, b"a" * quarter)
        await cache.write_blocks("huge", quarter, b"b" * quarter)
        assert cache.used_bytes() == 2 * quarter

        monkeypatch.setattr(cache, "_disk_free", lambda: 1024 * 1024)
        await cache.write_blocks("huge", 2 * quarter, b"c" * quarter)
        assert cache.used_bytes() == 2 * quarter
        assert not cache.has_range("huge", 2 * quarter, 3 * quarter - 1)

    async def test_max_files_still_bounds_read_fills(self, tmp_path):
        cache = make_cache(tmp_path, max_files=1)
        assert cache.reserve("fs", "a", 10) is not None
        cache.pin("a")
        assert cache.reserve("fs", "b", 10) is None
        cache.release("a")
        assert cache.reserve("fs", "b", 10) is not None
        assert cache.entry("a") is None

    async def test_a_reserved_entry_survives_a_restart_empty(self, tmp_path):
        cache = make_cache(tmp_path, block_kb=256)
        quarter = 256 * 1024
        assert cache.reserve("fs", "huge", 8 * quarter) is not None
        await cache.write_blocks("huge", quarter, b"x" * quarter)

        again = make_cache(tmp_path, block_kb=256)
        entry = entry_of(again, "huge")
        assert entry.size == 8 * quarter
        assert list(entry.present) == [0, 1, 0, 0, 0, 0, 0, 0]
        assert again.used_bytes() == quarter

    async def test_release_without_keep_for_reads_drops_the_entry(self, tmp_path):
        cache = make_cache(tmp_path, keep_for_reads=False)
        await stage(cache, "v1", b"x" * 10)
        cache.pin("v1")

        cache.release("v1")

        assert cache.entry("v1") is None

    async def test_stats(self, tmp_path):
        cache = make_cache(tmp_path)
        await stage(cache, "v1", b"x" * 10)
        cache.pin("v1")

        stats = cache.stats()

        assert stats["entries"] == 1 and stats["pinned_entries"] == 1
        assert stats["used_bytes"] == 10 and stats["pinned_bytes"] == 10
        assert stats["per_filesystem"] == {
            "fs": {"entries": 1, "bytes": 10, "pinned": 1}
        }


class TestStagedFileMessage:
    async def test_reads_pass_through_and_land_in_the_cache(self, tmp_path):
        cache = make_cache(tmp_path)
        data = os.urandom(2 * BLOCK + 17)
        inner = FileMessageFromBuffer.new(buffer=data, name="a.bin")
        inner.version_id = "v1"
        writer = cache.open_staging("fs", "v1", len(data))
        assert writer is not None
        msg = StagedFileMessage.wrap(inner, writer)

        await msg.open()
        out = b""
        while len(out) < len(data):
            out += await msg.read(50_000)
        await msg.close()
        await writer.close()

        assert out == data
        assert msg.version_id == "v1"
        assert await read_all(cache, "v1") == data


# -- integration --------------------------------------------------------------


class Stack:
    def __init__(
        self,
        primary: "RecordingStore",
        mirror: FakeStore,
        cache: LocalCache | None,
        inline: bool = True,
        queue: ReplicationQueue | None = None,
        write_ack: str = "primary",
    ):
        self.name = "fs"
        self.primary = primary
        self.mirror = mirror
        self.cache = cache
        self.store = primary
        self.mirror_group = MirrorGroup(
            primary=primary,
            stores=[MirrorStore(key=mirror.key, store=mirror)],
            mode="auto",
            strict=True,
        )
        self.fc_repo = StoreFileContentRepository(
            primary,
            mirror_group=self.mirror_group,
            inline_mirroring=inline,
            cache=cache,
            cache_scope=self.name,
        )
        self.fd_repo = StoreFDRepository(
            primary,
            mirror_group=self.mirror_group,
            local_versions=cache.complete if cache is not None else None,
        )
        self.metadata_repo = PinnedMessageMetadataRepository(
            primary, self.fc_repo, mirror_group=self.mirror_group
        )
        self.metadata_api = MetaDataApi(self.metadata_repo)
        self.queue = queue

        async def enqueue(fr):
            assert queue is not None
            queue.enqueue(self.name, file_path(fr))

        self.file_api = FileApi(
            self.metadata_api,
            FileDescApi(self.fd_repo, self.fc_repo, write_ack=write_ack),  # type: ignore[arg-type]
            cast(IStore, primary),
            mirror_group=self.mirror_group,
            inline_mirroring=inline,
            on_written=enqueue if queue is not None else None,
        )
        self.dir_api = DirectoryApi(self.metadata_api, self.file_api, primary)

    async def init(self) -> "Stack":
        await self.metadata_api.init()
        return self

    @property
    def root(self) -> TGFSDirectory:
        return self.metadata_api.get_root_directory()

    async def put(self, name: str, data: bytes):
        return await self.file_api.upload(
            self.root, FileMessageFromBuffer.new(buffer=data, name=name)
        )

    async def read(self, name: str) -> bytes:
        fr = self.root.find_file(name)
        stream = await self.file_api.retrieve(fr, 0, -1, name)
        return b"".join([chunk async for chunk in stream])

    async def version(self, name: str) -> TGFSFileVersion:
        fr = self.root.find_file(name)
        return (await self.fd_repo.get(fr)).get_latest_version()

    def worker_client(self):
        return type(
            "C",
            (),
            {
                "name": self.name,
                "mirror_group": self.mirror_group,
                "fd_repo": self.fd_repo,
                "metadata_api": self.metadata_api,
                "dir_api": self.dir_api,
                "cache": self.cache,
                "store": self.primary,
            },
        )()


class RecordingStore(FakeStore):
    """A FakeStore that remembers which messages were downloaded.

    The metadata blob travels through the same repository and is read
    with ``download_file`` too, so "no download from the primary" has to
    be asked per message id, not per method.
    """

    def __post_init__(self):
        super().__post_init__()
        self.downloaded: list[int] = []

    async def download_file(self, message_id: int, begin: int, end: int):
        self.downloaded.append(message_id)
        return await super().download_file(message_id, begin, end)


def telegram_like() -> RecordingStore:
    return RecordingStore(key="tg:1", backend_name="telegram", max_part_bytes=2048)


def discord_like() -> FakeStore:
    return FakeStore(
        key="dc:9", backend_name="discord", max_part_bytes=64, server_copy=False
    )


def telegram_spare() -> FakeStore:
    return FakeStore(key="tg:2", backend_name="telegram", max_part_bytes=2048)


def downloaded_parts(store: RecordingStore, version: TGFSFileVersion) -> list[int]:
    return [mid for mid in store.downloaded if mid in version.message_ids]


DATA = bytes(range(256)) * 20  # 5120 bytes: 3 primary parts, 80 replica parts


class TestInlineMirroringFromTheCache:
    async def test_the_replica_is_built_without_downloading_from_the_primary(
        self, tmp_path
    ):
        cache = make_cache(tmp_path)
        stack = await Stack(telegram_like(), discord_like(), cache).init()

        await stack.put("a.bin", DATA)

        version = await stack.version("a.bin")
        assert len(version.replicas["dc:9"].message_ids) == 80
        assert downloaded_parts(stack.primary, version) == []
        # The replica holds the right bytes.
        stack.primary.fail.update({"download_file", "get_messages"})
        assert await stack.read("a.bin") == DATA
        # Kept for reads and no longer pinned once the mirror has its copy.
        assert cache.complete(version.id)
        assert entry_of(cache, version.id).pins == 0

    async def test_without_keep_for_reads_the_entry_is_gone_afterwards(self, tmp_path):
        cache = make_cache(tmp_path, keep_for_reads=False)
        stack = await Stack(telegram_like(), discord_like(), cache).init()

        await stack.put("a.bin", DATA)

        version = await stack.version("a.bin")
        assert cache.entry(version.id) is None
        assert downloaded_parts(stack.primary, version) == []

    async def test_a_re_uploaded_aligned_copy_reads_locally_too(self, tmp_path):
        cache = make_cache(tmp_path)
        spare = telegram_spare()
        spare.server_copy = False
        stack = await Stack(telegram_like(), spare, cache).init()

        await stack.put("a.bin", DATA)

        version = await stack.version("a.bin")
        assert len(version.mirrors["tg:2"]) == 3
        assert downloaded_parts(stack.primary, version) == []
        assert b"".join(spare.document_of(m) for m in version.mirrors["tg:2"]) == DATA

    async def test_a_full_cache_leaves_the_upload_unstaged(self, tmp_path):
        cache = make_cache(tmp_path, max_size_mb=1, block_kb=256)
        await stage(cache, "hog", b"h" * (1024 * 1024))
        cache.pin("hog")
        stack = await Stack(telegram_like(), discord_like(), cache).init()

        await stack.put("a.bin", DATA)

        version = await stack.version("a.bin")
        assert len(version.replicas["dc:9"].message_ids) == 80
        assert downloaded_parts(stack.primary, version) != []
        assert cache.entry(version.id) is None
        assert await stack.read("a.bin") == DATA

    async def test_a_failed_upload_leaves_no_entry(self, tmp_path):
        cache = make_cache(tmp_path)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        stack.primary.fail.add("upload")

        with pytest.raises(Exception):
            await stack.put("a.bin", DATA)

        assert cache.stats()["entries"] == 0


class TestSweep:
    async def test_entries_unread_for_max_age_are_dropped(self, tmp_path):
        cache = make_cache(tmp_path, max_age_hours=1)
        await stage(cache, "old", b"o" * 10)
        await stage(cache, "fresh", b"f" * 10)
        now = time.time()
        entry_of(cache, "old").last_access = now - 2 * 3600

        report = cache.sweep(now=now)

        assert report["expired"] == 1
        assert cache.entry("old") is None and cache.entry("fresh") is not None

    async def test_max_age_off_by_default(self, tmp_path):
        cache = make_cache(tmp_path)
        await stage(cache, "old", b"o" * 10)
        entry_of(cache, "old").last_access = 1

        assert cache.sweep()["expired"] == 0
        assert cache.entry("old") is not None

    async def test_the_sweep_evicts_down_to_the_target_fill(self, tmp_path):
        cache = make_cache(
            tmp_path, max_size_mb=1, block_kb=256, target_fill_percent=50
        )
        quarter = 256 * 1024
        for i, name in enumerate(("a", "b", "c", "d")):
            await stage(cache, name, bytes([i]) * quarter)
            entry_of(cache, name).last_access = i

        report = cache.sweep()

        assert report["evicted"] == 2
        assert cache.entry("a") is None and cache.entry("b") is None
        assert cache.used_bytes() == 2 * quarter

    async def test_a_target_of_100_percent_evicts_nothing(self, tmp_path):
        cache = make_cache(
            tmp_path, max_size_mb=1, block_kb=256, target_fill_percent=100
        )
        for name in ("a", "b", "c", "d"):
            await stage(cache, name, b"x" * (256 * 1024))

        assert cache.sweep()["evicted"] == 0
        assert cache.stats()["entries"] == 4

    async def test_pinned_entries_survive_the_sweep(self, tmp_path):
        cache = make_cache(
            tmp_path,
            max_size_mb=1,
            block_kb=256,
            target_fill_percent=25,
            max_age_hours=1,
        )
        await stage(cache, "pinned", b"p" * (1024 * 1024))
        cache.pin("pinned")
        entry_of(cache, "pinned").last_access = 1

        report = cache.sweep()

        assert report == {"orphans": 0, "released": 0, "expired": 0, "evicted": 0}
        assert cache.entry("pinned") is not None

    async def test_a_stale_pin_is_released_when_nothing_is_queued(self, tmp_path):
        cache = make_cache(tmp_path)
        await stage(cache, "v", b"v" * 10)
        cache.pin("v")
        now = time.time()
        entry_of(cache, "v").pinned_at = now - STALE_PIN_SECONDS - 1

        assert cache.sweep(queue_empty=lambda fs: False, now=now)["released"] == 0
        assert entry_of(cache, "v").pins == 1

        assert cache.sweep(queue_empty=lambda fs: fs == "fs", now=now)["released"] == 1
        assert entry_of(cache, "v").pins == 0
        assert cache.complete("v")  # kept for reads

    async def test_a_young_pin_is_left_alone(self, tmp_path):
        cache = make_cache(tmp_path)
        await stage(cache, "v", b"v" * 10)
        cache.pin("v")

        assert cache.sweep(queue_empty=lambda fs: True)["released"] == 0
        assert entry_of(cache, "v").pins == 1

    async def test_the_pin_time_survives_a_restart(self, tmp_path):
        cache = make_cache(tmp_path)
        await stage(cache, "v", b"v" * 10)
        cache.pin("v")
        pinned_at = entry_of(cache, "v").pinned_at
        assert pinned_at > 0

        reloaded = make_cache(tmp_path)

        assert entry_of(reloaded, "v").pins == 1
        assert entry_of(reloaded, "v").pinned_at == pinned_at

    async def test_orphaned_files_are_removed(self, tmp_path):
        cache = make_cache(tmp_path)
        await stage(cache, "v", b"v" * 10)
        (tmp_path / "cache" / "gone.bin").write_bytes(b"x")
        (tmp_path / "cache" / "gone.map").write_bytes(b"\x01")

        assert cache.sweep()["orphans"] == 2
        assert not (tmp_path / "cache" / "gone.bin").exists()
        assert cache.complete("v")

    async def test_the_sweep_frees_the_disk_headroom(self, tmp_path, monkeypatch):
        cache = make_cache(tmp_path, min_free_mb=1)
        await stage(cache, "a", b"a" * 10)
        await stage(cache, "b", b"b" * 10)
        entry_of(cache, "a").last_access = 1
        free = [1024 * 1024 - 5]

        def freeing():
            return free[0] + (10 if cache.entry("a") is None else 0)

        monkeypatch.setattr(cache, "_disk_free", freeing)
        report = cache.sweep()

        assert report["evicted"] == 1
        assert cache.entry("a") is None and cache.entry("b") is not None

    async def test_the_sweeper_runs_on_start_and_then_periodically(self, tmp_path):
        cache = make_cache(tmp_path, max_age_hours=1)
        await stage(cache, "old", b"o" * 10)
        entry_of(cache, "old").last_access = 1
        sweeper = CacheSweeper(cache, interval=0.01)

        sweeper.start()
        await asyncio.sleep(0.05)
        await sweeper.stop()

        assert cache.entry("old") is None
        assert cache.last_sweep is not None
        assert cache.stats()["last_sweep"] == cache.last_sweep

    def test_stats_show_the_disk_and_the_sweep(self, tmp_path):
        cache = make_cache(
            tmp_path, min_free_mb=2, target_fill_percent=80, max_size_mb=10
        )
        stats = cache.stats()
        assert stats["min_free_bytes"] == 2 * 1024 * 1024
        assert stats["target_bytes"] == 8 * 1024 * 1024
        assert stats["disk_free_bytes"] is not None and stats["disk_free_bytes"] > 0
        assert stats["last_sweep"] is None


class TestBackgroundReplicationFromTheCache:
    async def test_the_worker_reads_the_staged_copy(self, tmp_path):
        cache = make_cache(tmp_path)
        queue = ReplicationQueue(None)
        stack = await Stack(
            telegram_like(), discord_like(), cache, inline=False, queue=queue
        ).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")
        assert cache.complete(version.id) and entry_of(cache, version.id).pins == 1

        worker = ReplicationWorker(stack.worker_client(), queue)  # type: ignore[arg-type]
        assert await worker.run_once() == 1

        version = await stack.version("a.bin")
        assert len(version.replicas["dc:9"].message_ids) == 80
        assert downloaded_parts(stack.primary, version) == []
        assert entry_of(cache, version.id).pins == 0
        stack.primary.fail.update({"download_file", "get_messages"})
        assert await stack.read("a.bin") == DATA

    async def test_an_evicted_entry_falls_back_to_the_download(self, tmp_path):
        cache = make_cache(tmp_path, keep_for_reads=False)
        queue = ReplicationQueue(None)
        stack = await Stack(
            telegram_like(), discord_like(), cache, inline=False, queue=queue
        ).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")
        cache.remove(version.id)

        worker = ReplicationWorker(stack.worker_client(), queue)  # type: ignore[arg-type]
        assert await worker.run_once() == 1

        version = await stack.version("a.bin")
        assert downloaded_parts(stack.primary, version) != []
        assert len(version.replicas["dc:9"].message_ids) == 80

    async def test_a_download_fills_the_cache_when_entries_are_kept(self, tmp_path):
        cache = make_cache(tmp_path)
        queue = ReplicationQueue(None)
        stack = await Stack(
            telegram_like(), discord_like(), cache, inline=False, queue=queue
        ).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")
        cache.remove(version.id)

        worker = ReplicationWorker(stack.worker_client(), queue)  # type: ignore[arg-type]
        assert await worker.run_once() == 1

        assert cache.complete(version.id)
        assert await read_all(cache, version.id) == DATA
        assert entry_of(cache, version.id).pins == 0

    async def test_without_a_cache_everything_works_as_before(self):
        queue = ReplicationQueue(None)
        stack = await Stack(
            telegram_like(), discord_like(), None, inline=False, queue=queue
        ).init()
        await stack.put("a.bin", DATA)

        worker = ReplicationWorker(stack.worker_client(), queue)  # type: ignore[arg-type]
        assert await worker.run_once() == 1

        assert downloaded_parts(stack.primary, await stack.version("a.bin")) != []
        assert await stack.read("a.bin") == DATA


BIG = bytes(range(256)) * 900  # 230400 bytes: 3 full 64 KiB blocks and a tail


class TestReadCache:
    async def test_the_first_read_fills_the_cache_and_the_second_reads_from_disk(
        self, tmp_path
    ):
        cache = make_cache(tmp_path, stage_uploads=False)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")
        # Inline mirroring filled the entry from its own download; start
        # from an empty cache, as a read of an older file would.
        cache.remove(version.id)

        assert await stack.read("a.bin") == BIG

        assert cache.complete(version.id)
        stack.primary.downloaded.clear()
        assert await stack.read("a.bin") == BIG
        assert downloaded_parts(stack.primary, version) == []
        assert cache.hits >= 1 and cache.misses >= 1

    async def test_a_small_read_fetches_whole_blocks_and_serves_the_neighbours(
        self, tmp_path
    ):
        cache = make_cache(tmp_path, stage_uploads=False)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")
        cache.remove(version.id)
        fr = stack.root.find_file("a.bin")

        async def read(begin, end):
            stream = await stack.file_api.retrieve(fr, begin, end, "a.bin")
            return b"".join([c async for c in stream])

        assert await read(BLOCK + 10, BLOCK + 99) == BIG[BLOCK + 10 : BLOCK + 100]
        entry = entry_of(cache, version.id)
        assert list(entry.present) == [0, 1, 0, 0]

        stack.primary.downloaded.clear()
        assert await read(BLOCK + 200, 2 * BLOCK - 1) == BIG[BLOCK + 200 : 2 * BLOCK]
        assert downloaded_parts(stack.primary, version) == []

        # A range that straddles a cached and an uncached block.
        assert await read(BLOCK + 5, 2 * BLOCK + 5) == BIG[BLOCK + 5 : 2 * BLOCK + 6]
        assert list(entry_of(cache, version.id).present) == [0, 1, 1, 0]

    async def test_the_tail_block_is_cached_short(self, tmp_path):
        cache = make_cache(tmp_path, stage_uploads=False)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")
        cache.remove(version.id)
        fr = stack.root.find_file("a.bin")

        stream = await stack.file_api.retrieve(fr, len(BIG) - 10, -1, "a.bin")
        assert b"".join([c async for c in stream]) == BIG[-10:]

        assert list(entry_of(cache, version.id).present) == [0, 0, 0, 1]
        assert (
            await read_all(cache, version.id, 3 * BLOCK, len(BIG) - 1)
            == BIG[3 * BLOCK :]
        )

    async def test_a_staged_upload_is_read_from_disk_right_away(self, tmp_path):
        cache = make_cache(tmp_path)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")
        stack.primary.downloaded.clear()

        assert await stack.read("a.bin") == BIG

        assert downloaded_parts(stack.primary, version) == []

    async def test_without_keep_for_reads_reads_bypass_the_cache(self, tmp_path):
        cache = make_cache(tmp_path, keep_for_reads=False)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")

        assert await stack.read("a.bin") == BIG

        assert cache.entry(version.id) is None
        assert downloaded_parts(stack.primary, version) != []

    async def test_a_full_cache_leaves_the_read_on_the_stores(self, tmp_path):
        cache = make_cache(tmp_path, max_size_mb=1, block_kb=256, stage_uploads=False)
        hog = cache.open_staging("fs", "hog", 1024 * 1024)
        assert hog is not None
        await hog.write(b"h" * (1024 * 1024))
        await hog.close()
        cache.pin("hog")
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")

        assert await stack.read("a.bin") == BIG
        # The entry is admitted (it costs nothing), but no block found room.
        assert entry_of(cache, version.id).present_bytes() == 0
        assert cache.used_bytes() == 1024 * 1024

    async def test_a_version_above_the_file_limit_is_read_through_the_cache(
        self, tmp_path, monkeypatch
    ):
        """max_file_size_mb keeps a large version from being staged whole,
        not from being cached block by block as it is read."""
        cache = make_cache(tmp_path, stage_uploads=False)
        # The limit is whole megabytes; the test file is smaller, so lower
        # the limit itself to one block.
        monkeypatch.setattr(
            CacheConfig, "max_file_size_bytes", property(lambda self: BLOCK)
        )
        assert cache.open_staging("fs", "whole", len(BIG)) is None
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")
        assert cache.entry(version.id) is None  # nothing staged, nothing filled
        fr = stack.root.find_file("a.bin")

        async def read(begin, end):
            stream = await stack.file_api.retrieve(fr, begin, end, "a.bin")
            return b"".join([c async for c in stream])

        assert await read(BLOCK + 10, BLOCK + 99) == BIG[BLOCK + 10 : BLOCK + 100]
        assert list(entry_of(cache, version.id).present) == [0, 1, 0, 0]

        stack.primary.downloaded.clear()
        assert await read(BLOCK + 200, 2 * BLOCK - 1) == BIG[BLOCK + 200 : 2 * BLOCK]
        assert downloaded_parts(stack.primary, version) == []

    async def test_the_metadata_blob_is_never_cached(self, tmp_path):
        cache = make_cache(tmp_path)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")

        assert set(cache._entries) == {version.id}

    async def test_a_cached_read_falls_over_like_a_plain_one(self, tmp_path):
        cache = make_cache(tmp_path, stage_uploads=False)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        stack.primary.fail.update({"download_file"})

        assert await stack.read("a.bin") == BIG


class TestTheContentLengthProbe:
    """A PROPFIND asks the encryption decorator for the length of every
    listed file, and the decorator answers by reading the first bytes of
    the version and abandoning the stream. That probe must not open a
    read-cache entry: the entry would be sized to the whole version and
    never receive a single block, so a directory listing would leave one
    empty entry per file behind.
    """

    def _crypto(self, stack: Stack) -> EncryptingFileContentRepository:
        return EncryptingFileContentRepository(
            stack.fc_repo, b"\x11" * 32, chunk_size=4096
        )

    async def test_a_probe_opens_no_entry(self, tmp_path):
        cache = make_cache(tmp_path, stage_uploads=False)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")
        # Inline mirroring filled the entry from its own download; start
        # from an empty cache, the way a listing of an older file would.
        cache.remove(version.id)

        assert await self._crypto(stack).content_length(version) == len(BIG)

        assert cache.entry(version.id) is None

    async def test_a_read_after_a_probe_still_caches(self, tmp_path):
        """The refusal is scoped to the probe, not to the file."""
        cache = make_cache(tmp_path, stage_uploads=False)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")
        cache.remove(version.id)
        crypto = self._crypto(stack)
        await crypto.content_length(version)

        stream = await crypto.get(version, 0, -1, "a.bin")
        assert b"".join([c async for c in stream]) == BIG

        assert cache.complete(version.id)

    async def test_a_probe_reads_through_an_entry_that_exists(self, tmp_path):
        """An entry that is already there is served from disk, not refused."""
        cache = make_cache(tmp_path, stage_uploads=False)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", BIG)
        version = await stack.version("a.bin")
        assert cache.complete(version.id)
        stack.primary.downloaded.clear()

        assert await self._crypto(stack).content_length(version) == len(BIG)

        assert downloaded_parts(stack.primary, version) == []


class TestDeleteFanOut:
    async def test_removing_a_file_drops_its_entries(self, tmp_path):
        cache = make_cache(tmp_path)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", DATA)
        await stack.put("a.bin", DATA[:100])
        fr = stack.root.find_file("a.bin")
        fd = await stack.fd_repo.get(fr, include_all_versions=True)
        ids = {v.id for v in fd.get_versions()}
        assert len(ids) == 2 and ids <= set(cache._entries)

        await stack.file_api.rm(fr)

        assert not ids & set(cache._entries)

    async def test_removing_a_version_drops_its_entry_only(self, tmp_path):
        cache = make_cache(tmp_path)
        stack = await Stack(telegram_like(), discord_like(), cache).init()
        await stack.put("a.bin", DATA)
        old = await stack.version("a.bin")
        await stack.put("a.bin", DATA[:100])
        new = await stack.version("a.bin")

        await stack.file_api.rm(stack.root.find_file("a.bin"), version_id=old.id)

        assert cache.entry(old.id) is None
        assert cache.entry(new.id) is not None
