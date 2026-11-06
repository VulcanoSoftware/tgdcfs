"""``listing.stat_in_metadata``: listings answered from the metadata.

Drives the real ``Ops`` / ``FileApi`` / ``FileStatApi`` /
``StoreFDRepository`` stack against the fake channel of
``test_copy_and_move``. The metadata repository is the default one, the
behaviour of a backend that serializes the whole tree (``pinned_message``).
"""

import asyncio
from typing import cast

import pytest

from tests.tgdcfs.core.test_copy_and_move import (
    FakeChannel,
    FakeFileContentRepository,
    FakeMetadataRepository,
    FakeTelegramStore,
)
from tgdcfs.backends.telegram.store import TelegramStore
from tgdcfs.core.api import stat as stat_module
from tgdcfs.core.api.directory import DirectoryApi
from tgdcfs.core.api.file import FileApi
from tgdcfs.core.api.file_desc import FileDescApi
from tgdcfs.core.api.metadata import MetaDataApi
from tgdcfs.core.api.stat import FileStatApi
from tgdcfs.core.client import Client
from tgdcfs.core.model import FileStat
from tgdcfs.core.ops import Ops
from tgdcfs.core.repository.impl.fd.store_msg import StoreFDRepository


class CountingFDRepository(StoreFDRepository):
    def __init__(self, store) -> None:
        super().__init__(store)
        self.reads = 0
        self.gate: asyncio.Event | None = None

    async def get(self, fr, include_all_versions=False, validate=True):
        self.reads += 1
        if self.gate is not None:
            await self.gate.wait()
        return await super().get(fr, include_all_versions, validate)


class StatClient:
    def __init__(self, persist: bool = True) -> None:
        self.name = "test"
        self.channel = FakeChannel()
        store = cast(TelegramStore, FakeTelegramStore(self.channel))
        self.fd_repo = CountingFDRepository(store)
        self.fc_repo = FakeFileContentRepository(self.channel)
        file_desc_api = FileDescApi(self.fd_repo, self.fc_repo)
        self.metadata_repo = FakeMetadataRepository()
        self.metadata_api = MetaDataApi(self.metadata_repo)
        self.stats = FileStatApi(
            file_desc_api, metadata_api=self.metadata_api, persist=persist
        )
        self.file_api = FileApi(
            self.metadata_api, file_desc_api, store, stat_api=self.stats
        )
        self.dir_api = DirectoryApi(self.metadata_api, self.file_api, store)


@pytest.fixture(autouse=True)
def no_flush_delay(monkeypatch):
    """Write marked folders right away instead of after seconds."""
    for name in ("FLUSH_QUIET", "FLUSH_MAX_DELAY", "URGENT_QUIET", "URGENT_MAX_DELAY"):
        monkeypatch.setattr(stat_module, name, 0.0)


@pytest.fixture
def client() -> StatClient:
    return StatClient()


@pytest.fixture
def ops(client) -> Ops:
    return Ops(cast(Client, client))


async def settle(client: StatClient) -> None:
    """Let the background writer run."""
    for _ in range(20):
        await asyncio.sleep(0)
    await client.stats.close()


class TestFilling:
    @pytest.mark.asyncio
    async def test_the_first_listing_stores_the_stat(self, client, ops):
        await ops.upload_from_bytes(b"x" * 1234, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        pushes = client.metadata_repo.pushes

        stat = await client.stats.get(fr)
        await settle(client)

        assert stat.size == 1234
        assert fr.stat == stat
        assert client.metadata_repo.pushes > pushes
        assert fr.to_dict()["stat"]["size"] == 1234

    @pytest.mark.asyncio
    async def test_a_stored_stat_reads_no_descriptor(self, client, ops):
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        await client.stats.get(fr)
        reads = client.fd_repo.reads

        await client.stats.get(fr)

        assert client.fd_repo.reads == reads

    @pytest.mark.asyncio
    async def test_concurrent_listings_read_the_descriptor_once(self, client, ops):
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        reads = client.fd_repo.reads

        await asyncio.gather(*(client.stats.get(fr) for _ in range(5)))

        assert client.fd_repo.reads == reads + 1

    @pytest.mark.asyncio
    async def test_an_abandoned_listing_still_stores_what_it_read(self, client, ops):
        await ops.upload_from_bytes(b"x" * 77, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        client.fd_repo.gate = asyncio.Event()

        listing = asyncio.ensure_future(client.stats.get(fr))
        await asyncio.sleep(0)
        listing.cancel()  # the WebDAV client gave up
        with pytest.raises(asyncio.CancelledError):
            await listing
        client.fd_repo.gate.set()
        await settle(client)

        assert fr.stat is not None and fr.stat.size == 77

    @pytest.mark.asyncio
    async def test_an_unreadable_descriptor_is_not_stored(self, client, ops):
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        del client.channel.messages[fr.message_id]

        stat = await client.stats.get(fr)

        assert stat.size == 0
        assert fr.stat is None

    @pytest.mark.asyncio
    async def test_off_reads_the_descriptor_every_time(self):
        client = StatClient(persist=False)
        ops = Ops(cast(Client, client))
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        reads = client.fd_repo.reads

        await client.stats.get(fr)
        await client.stats.get(fr)

        assert client.fd_repo.reads == reads + 2
        assert fr.stat is None


class TestRecordedOnUpload:
    @pytest.mark.asyncio
    async def test_an_upload_keeps_its_stat(self, client, ops):
        await ops.upload_from_bytes(b"x" * 321, "/a.txt")
        fr = ops.stat_file("/a.txt")

        assert fr.stat is not None and fr.stat.size == 321

    @pytest.mark.asyncio
    async def test_the_first_listing_reads_no_descriptor(self, client, ops):
        for i in range(5):
            await ops.upload_from_bytes(b"x" * 10, f"/f{i}.txt")
        reads = client.fd_repo.reads

        stats = [await client.stats.get(ops.stat_file(f"/f{i}.txt")) for i in range(5)]

        assert [s.size for s in stats] == [10] * 5
        assert client.fd_repo.reads == reads

    @pytest.mark.asyncio
    async def test_the_stats_of_uploads_are_written_to_the_metadata(self, client, ops):
        await ops.upload_from_bytes(b"x" * 1234, "/a.txt")
        pushes = client.metadata_repo.pushes

        await settle(client)

        assert client.metadata_repo.pushes > pushes
        assert ops.stat_file("/a.txt").to_dict()["stat"]["size"] == 1234

    @pytest.mark.asyncio
    async def test_a_descriptor_that_reads_incomplete_is_still_kept(
        self, client, ops, monkeypatch
    ):
        """What was just written is complete whatever a read would say."""
        stat_of = client.stats.stat_of

        async def incomplete(fd):
            fd.complete = False
            return await stat_of(fd)

        monkeypatch.setattr(client.stats, "stat_of", incomplete)

        await ops.upload_from_bytes(b"x" * 55, "/a.txt")

        stat = ops.stat_file("/a.txt").stat
        assert stat is not None and stat.size == 55

    @pytest.mark.asyncio
    async def test_a_skipped_stat_is_logged(self, client, ops, monkeypatch, caplog):
        async def boom(fd):
            raise RuntimeError("store unreachable")

        monkeypatch.setattr(client.stats, "stat_of", boom)

        await ops.upload_from_bytes(b"x" * 10, "/a.txt")

        assert "Did not keep the stat of a.txt on upload" in caplog.text

    @pytest.mark.asyncio
    async def test_off_keeps_nothing(self):
        client = StatClient(persist=False)
        ops = Ops(cast(Client, client))
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")

        assert ops.stat_file("/a.txt").stat is None

    @pytest.mark.asyncio
    async def test_an_unreadable_size_just_leaves_it_to_the_listing(
        self, client, ops, monkeypatch
    ):
        async def boom(fd):
            raise RuntimeError("store unreachable")

        monkeypatch.setattr(client.stats, "stat_of", boom)

        await ops.upload_from_bytes(b"x" * 10, "/a.txt")  # still succeeds

        assert ops.stat_file("/a.txt").stat is None


class TestWriteLoad:
    @pytest.mark.asyncio
    async def test_a_write_is_not_followed_at_once_by_another(
        self, client, ops, monkeypatch
    ):
        """Writes happen on the one metadata writer that uploads need too."""
        monkeypatch.setattr(stat_module, "FLUSH_DUTY", 4.0)
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        await asyncio.sleep(0.05)  # the upload's own write
        folder = ops.stat_file("/a.txt").location
        starts: list[float] = []
        ends: list[float] = []
        save = client.metadata_api.save_stats

        async def slow_save(directories, keep):
            starts.append(asyncio.get_running_loop().time())
            await asyncio.sleep(0.05)
            await save(directories, keep)
            ends.append(asyncio.get_running_loop().time())

        monkeypatch.setattr(client.metadata_api, "save_stats", slow_save)

        client.stats._mark(folder)
        await asyncio.sleep(0.08)  # the first write is done
        client.stats._mark(folder)
        await asyncio.sleep(0.1)  # still inside the pause: 4 x 0.05 s
        assert len(starts) == 1
        await asyncio.sleep(0.2)

        assert len(starts) == 2
        assert starts[1] - ends[0] >= 0.18
        await client.stats.close()

    @pytest.mark.asyncio
    async def test_missing_stats_are_read_a_few_at_a_time(
        self, client, ops, monkeypatch
    ):
        monkeypatch.setattr(stat_module, "MAX_STAT_READS", 4)
        client.stats._read_slots = asyncio.Semaphore(4)
        refs = []
        for i in range(20):
            await ops.upload_from_bytes(b"x" * 10, f"/f{i}.txt")
            fr = ops.stat_file(f"/f{i}.txt")
            fr.stat = None
            refs.append(fr)
        running = peak = 0
        read = client.fd_repo.get

        async def counting_get(fr, include_all_versions=False, validate=True):
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            try:
                await asyncio.sleep(0.01)
                return await read(fr, include_all_versions, validate)
            finally:
                running -= 1

        monkeypatch.setattr(client.fd_repo, "get", counting_get)

        stats = await asyncio.gather(*(client.stats.get(fr) for fr in refs))

        assert [s.size for s in stats] == [10] * 20
        assert peak == 4
        await settle(client)


class TestChanges:
    @pytest.mark.asyncio
    async def test_an_overwrite_drops_the_stat(self, client, ops):
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        await client.stats.get(fr)

        await ops.upload_from_bytes(b"y" * 20, "/a.txt")

        assert fr.stat is None
        assert (await client.stats.get(fr)).size == 20

    @pytest.mark.asyncio
    async def test_a_read_from_before_the_overwrite_is_not_stored(self, client, ops):
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        client.fd_repo.gate = asyncio.Event()
        reads = client.fd_repo.reads
        listing = asyncio.ensure_future(client.stats.get(fr))
        while client.fd_repo.reads == reads:
            await asyncio.sleep(0)

        client.stats.invalidate(fr)  # the file changed meanwhile
        client.fd_repo.gate.set()
        await listing

        assert fr.stat is None

    @pytest.mark.asyncio
    async def test_a_new_date_drops_the_stat(self, client, ops):
        import datetime

        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        await client.stats.get(fr)

        await ops.set_last_modified("/a.txt", datetime.datetime(2020, 1, 1))

        assert fr.stat is None

    @pytest.mark.asyncio
    async def test_a_move_keeps_the_stat(self, client, ops):
        client.dir_api.root.create_dir("dest")
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        await client.stats.get(ops.stat_file("/a.txt"))
        reads = client.fd_repo.reads

        await ops.mv_file("/a.txt", "/dest/b.txt")
        moved = ops.stat_file("/dest/b.txt")

        assert moved.stat is not None and moved.stat.size == 10
        await client.stats.get(moved)
        assert client.fd_repo.reads == reads


class TestTurnedOff:
    @pytest.mark.asyncio
    async def test_stored_stats_are_removed_on_the_first_write(self):
        client = StatClient(persist=False)
        ops = Ops(cast(Client, client))
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        await ops.upload_from_bytes(b"x" * 10, "/b.txt")
        # Left behind by an earlier run with the option on.
        for name in ("a.txt", "b.txt"):
            ops.stat_file(f"/{name}").stat = FileStat(10, 1, 2)

        await ops.upload_from_bytes(b"y" * 30, "/a.txt")
        await settle(client)

        assert ops.stat_file("/a.txt").stat is None
        assert ops.stat_file("/b.txt").stat is None


class TestWriter:
    @pytest.mark.asyncio
    async def test_a_failed_write_is_kept_for_the_next_attempt(
        self, client, ops, monkeypatch
    ):
        # Keep the background writer out of the way; flush by hand.
        monkeypatch.setattr(stat_module, "FLUSH_QUIET", 3600.0)
        monkeypatch.setattr(stat_module, "FLUSH_MAX_DELAY", 3600.0)
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        fr.stat = None  # an upload records it; start from a ref without one
        fail = {"left": 1}
        push = client.metadata_repo.push

        async def flaky_push():
            if fail["left"]:
                fail["left"] -= 1
                raise RuntimeError("store down")
            await push()

        client.metadata_repo.push = flaky_push  # type: ignore[method-assign]
        await client.stats.get(fr)

        assert await client.stats.flush() is False
        assert await client.stats.flush() is True
        await client.stats.close()
