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
        await client.stats.get(fr)
        reads = client.fd_repo.reads

        await client.stats.get(fr)

        assert client.fd_repo.reads == reads

    @pytest.mark.asyncio
    async def test_concurrent_listings_read_the_descriptor_once(self, client, ops):
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        reads = client.fd_repo.reads

        await asyncio.gather(*(client.stats.get(fr) for _ in range(5)))

        assert client.fd_repo.reads == reads + 1

    @pytest.mark.asyncio
    async def test_an_abandoned_listing_still_stores_what_it_read(self, client, ops):
        await ops.upload_from_bytes(b"x" * 77, "/a.txt")
        fr = ops.stat_file("/a.txt")
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
        reads = client.fd_repo.reads

        await client.stats.get(fr)
        await client.stats.get(fr)

        assert client.fd_repo.reads == reads + 2
        assert fr.stat is None


class TestChanges:
    @pytest.mark.asyncio
    async def test_an_overwrite_drops_the_stat(self, client, ops):
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
        await client.stats.get(fr)

        await ops.upload_from_bytes(b"y" * 20, "/a.txt")

        assert fr.stat is None
        assert (await client.stats.get(fr)).size == 20

    @pytest.mark.asyncio
    async def test_a_read_from_before_the_overwrite_is_not_stored(self, client, ops):
        await ops.upload_from_bytes(b"x" * 10, "/a.txt")
        fr = ops.stat_file("/a.txt")
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
