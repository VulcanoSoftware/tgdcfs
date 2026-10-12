"""Tee mirroring (``sync: tee``): re-uploading mirrors fed from the
upload stream while the primary uploads it, without a read-back."""

import asyncio
import os
from dataclasses import dataclass, field
from typing import List, cast

import pytest

from tgdcfs.backends.base import IStore
from tgdcfs.config import CacheConfig
from tgdcfs.core.api import DirectoryApi, FileApi, FileDescApi, MetaDataApi
from tgdcfs.core.local_cache import LocalCache
from tgdcfs.core.mirror import MirrorGroup, MirrorStore
from tgdcfs.core.model import TGFSDirectory, TGFSFileVersion
from tgdcfs.core.replication import ReplicationQueue, file_path
from tgdcfs.core.repository.impl import (
    PinnedMessageMetadataRepository,
    StoreFDRepository,
    StoreFileContentRepository,
)
from tgdcfs.core.tee import TeeBranch, TeeFileMessage
from tgdcfs.errors import TechnicalError
from tgdcfs.reqres import FileMessageFromBuffer, Replica
from tests.fakes.store import FakeStore

pytestmark = pytest.mark.asyncio


async def drain(branch: TeeBranch) -> bytes:
    return b"".join([chunk async for chunk in branch.stream()])


class TestTeeBranch:
    async def test_bytes_come_out_in_order(self):
        branch = TeeBranch("dc", capacity=1024)
        await branch.feed(b"abc")
        await branch.feed(b"def")
        await branch.finish()
        assert await drain(branch) == b"abcdef"

    async def test_a_full_branch_holds_the_producer_until_the_consumer_takes(self):
        branch = TeeBranch("dc", capacity=4)
        await branch.feed(b"abcd")
        feeding = asyncio.ensure_future(branch.feed(b"ef"))
        await asyncio.sleep(0)
        assert not feeding.done()
        assert branch.buffered == 4

        stream = branch.stream()
        assert await anext(stream) == b"abcd"
        await feeding
        assert branch.buffered == 2
        await branch.finish()
        assert await anext(stream) == b"ef"
        with pytest.raises(StopAsyncIteration):
            await anext(stream)

    async def test_a_chunk_larger_than_the_capacity_passes_an_empty_branch(self):
        branch = TeeBranch("dc", capacity=2)
        await branch.feed(b"abcdef")  # would deadlock otherwise
        await branch.finish()
        assert await drain(branch) == b"abcdef"

    async def test_detach_releases_a_waiting_producer_and_fails_the_consumer(self):
        branch = TeeBranch("dc", capacity=4)
        await branch.feed(b"abcd")
        feeding = asyncio.ensure_future(branch.feed(b"ef"))
        consuming = asyncio.ensure_future(drain(branch))
        await asyncio.sleep(0)

        await branch.detach()

        await feeding  # not held up any more
        with pytest.raises(TechnicalError, match="detached"):
            await consuming
        assert branch.buffered == 0
        await branch.feed(b"ignored")  # dropped, never blocks
        assert branch.buffered == 0

    async def test_the_consumer_waits_for_more_until_finish(self):
        branch = TeeBranch("dc", capacity=1024)
        consuming = asyncio.ensure_future(drain(branch))
        await asyncio.sleep(0)
        assert not consuming.done()
        await branch.feed(b"x")
        await branch.finish()
        assert await consuming == b"x"


class TestTeeFileMessage:
    async def test_reads_pass_through_and_feed_every_branch(self):
        data = os.urandom(1000)
        inner = FileMessageFromBuffer.new(buffer=data, name="a.bin")
        branches = [TeeBranch("dc", 4096), TeeBranch("other", 4096)]
        msg = TeeFileMessage.wrap(inner, branches)
        drained = [asyncio.ensure_future(drain(b)) for b in branches]

        out = b""
        await msg.open()
        while chunk := await msg.read(300):
            out += chunk
        await msg.close()

        assert out == data
        assert [await d for d in drained] == [data, data]
        assert msg.fed == len(data)

    async def test_close_is_not_the_end_of_the_stream(self):
        """The Telegram uploader closes the message after every part."""
        data = b"0123456789"
        inner = FileMessageFromBuffer.new(buffer=data, name="a.bin")
        branch = TeeBranch("dc", 4096)
        msg = TeeFileMessage.wrap(inner, [branch])
        drained = asyncio.ensure_future(drain(branch))

        # One open/read/close cycle per part, as the Telegram uploader does.
        await msg.open()
        assert await msg.read(5) == b"01234"
        await msg.close()
        await asyncio.sleep(0)
        assert not drained.done()
        msg.next_part(5)
        await msg.open()
        assert await msg.read(5) == b"56789"
        await msg.close()
        assert await drained == data


@dataclass
class RecordingStore(FakeStore):
    """A FakeStore that remembers which messages were downloaded and can
    hold every upload until released, to observe the tee mid-flight."""

    downloaded: List[int] = field(default_factory=list)
    hold_uploads: asyncio.Event | None = None

    async def download_file(self, message_id: int, begin: int, end: int):
        self.downloaded.append(message_id)
        return await super().download_file(message_id, begin, end)

    async def upload(self, file_msg):
        if self.hold_uploads is not None:
            await self.hold_uploads.wait()
        return await super().upload(file_msg)


def telegram_like() -> RecordingStore:
    return RecordingStore(key="tg:1", backend_name="telegram", max_part_bytes=2048)


def telegram_spare() -> RecordingStore:
    return RecordingStore(key="tg:2", backend_name="telegram", max_part_bytes=2048)


def discord_like(key: str = "dc:9") -> RecordingStore:
    return RecordingStore(
        key=key, backend_name="discord", max_part_bytes=64, server_copy=False
    )


DATA = bytes(range(256)) * 20  # 5120 bytes: 3 primary parts, 80 replica parts


def make_group(primary: FakeStore, *mirrors: FakeStore, strict=False, mode="auto"):
    return MirrorGroup(
        primary=primary,
        stores=[MirrorStore(key=m.key, store=m) for m in mirrors],
        mode=mode,
        strict=strict,
    )


class TestMirrorGroupTee:
    async def test_targets_are_the_mirrors_that_re_upload(self):
        tg, spare, dc = telegram_like(), telegram_spare(), discord_like()
        group = make_group(tg, spare, dc)
        assert [ch.key for ch in group.tee_targets()] == ["dc:9"]

        assert [
            ch.key for ch in make_group(tg, spare, mode="reupload").tee_targets()
        ] == ["tg:2"]
        assert [ch.key for ch in make_group(dc, tg).tee_targets()] == ["tg:1"]
        assert make_group(tg, spare).tee_targets() == []

    async def test_no_targets_or_no_bytes_means_no_tee(self):
        group = make_group(telegram_like(), telegram_spare())
        assert (
            group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a"), 1 << 20) is None
        )
        group = make_group(telegram_like(), discord_like())
        assert (
            group.tee(FileMessageFromBuffer.new(buffer=b"", name="e"), 1 << 20) is None
        )

    async def test_the_mirror_gets_a_replica_in_its_own_layout(self):
        tg, dc = telegram_like(), discord_like()
        group = make_group(tg, dc)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 1 << 20)
        assert tee is not None

        sent = await tg.upload(tee.message)
        replicas = group.collect_tee(await tee.collect())

        assert [m.size for m in sent] == [2048, 2048, 1024]
        replica = replicas["dc:9"]
        assert isinstance(replica, Replica)
        assert len(replica.message_ids) == 80 and set(replica.part_sizes) == {64}
        assert b"".join(dc.document_of(m) for m in replica.message_ids) == DATA
        assert tg.downloaded == []  # nothing was read back

    async def test_the_mirror_uploads_while_the_primary_does(self):
        tg, dc = telegram_like(), discord_like()
        tg.hold_uploads = asyncio.Event()
        group = make_group(tg, dc)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 1 << 20)
        assert tee is not None

        primary = asyncio.ensure_future(tg.upload(tee.message))
        await asyncio.sleep(0)
        # The primary has not read a byte yet, so the mirror has nothing.
        assert dc.messages == {}
        tg.hold_uploads.set()
        await primary
        # By the time the primary is done the mirror has consumed the same
        # stream; collect only waits for its last part.
        replicas = group.collect_tee(await tee.collect())
        assert len(replicas["dc:9"].message_ids) == 80

    async def test_a_failing_mirror_leaves_the_primary_upload_intact(self):
        tg, dc = telegram_like(), discord_like()
        dc.fail.add("upload")
        group = make_group(tg, dc)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 16)
        assert tee is not None

        sent = await tg.upload(tee.message)  # never held up by the dead branch
        outcome = await tee.collect()

        assert [m.size for m in sent] == [2048, 2048, 1024]
        assert isinstance(outcome["dc:9"], TechnicalError)
        assert group.collect_tee(outcome) == {}

    async def test_a_failing_mirror_raises_with_strict(self):
        tg, dc = telegram_like(), discord_like()
        dc.fail.add("upload")
        group = make_group(tg, dc, strict=True)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 16)
        assert tee is not None
        await tg.upload(tee.message)
        with pytest.raises(TechnicalError):
            group.collect_tee(await tee.collect())

    async def test_abort_stops_the_mirror_uploads(self):
        tg, dc = telegram_like(), discord_like()
        group = make_group(tg, dc)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 16)
        assert tee is not None
        await tee.message.read(100)  # the mirror has started
        await asyncio.sleep(0)

        await tee.abort()

        outcome = await tee.collect()
        assert all(isinstance(o, Exception) for o in outcome.values())
        assert all(b.detached for b in tee.message.branches)

    async def test_two_mirrors_share_one_stream(self):
        tg, a, b = telegram_like(), discord_like("dc:1"), discord_like("dc:2")
        group = make_group(tg, a, b)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 200)
        assert tee is not None
        await tg.upload(tee.message)
        replicas = group.collect_tee(await tee.collect())
        for store, key in ((a, "dc:1"), (b, "dc:2")):
            assert b"".join(
                store.document_of(m) for m in replicas[key].message_ids
            ) == (DATA)


class Stack:
    """The write path from the file API down, with a tee'd repository."""

    def __init__(
        self,
        primary: FakeStore,
        *mirrors: FakeStore,
        queue: ReplicationQueue | None = None,
        inline: bool = False,
        cache: LocalCache | None = None,
    ):
        self.name = "fs"
        self.primary = primary
        self.mirror_group = make_group(primary, *mirrors)
        self.fc_repo = StoreFileContentRepository(
            primary,
            mirror_group=self.mirror_group,
            inline_mirroring=inline,
            tee_mirroring=True,
            tee_buffer=256,
            cache=cache,
            cache_scope="fs",
        )
        self.fd_repo = StoreFDRepository(primary, mirror_group=self.mirror_group)
        self.metadata_api = MetaDataApi(
            PinnedMessageMetadataRepository(
                primary, self.fc_repo, mirror_group=self.mirror_group
            )
        )
        self.queue = queue

        async def enqueue(fr):
            assert queue is not None
            queue.enqueue(self.name, file_path(fr))

        self.file_api = FileApi(
            self.metadata_api,
            FileDescApi(self.fd_repo, self.fc_repo),
            cast(IStore, primary),
            mirror_group=self.mirror_group,
            inline_mirroring=inline,
            on_written=enqueue if queue is not None else None,
            written_gaps_only=True,
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


class TestWritePath:
    async def test_an_upload_lands_in_both_stores_without_a_read_back(self):
        tg, dc = telegram_like(), discord_like()
        stack = await Stack(tg, dc).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")

        assert version.has_copy_in("dc:9")
        assert len(version.replicas["dc:9"].message_ids) == 80
        # The primary was never asked for the content parts.
        assert not any(mid in version.message_ids for mid in tg.downloaded)
        assert await stack.read("a.bin") == DATA

    async def test_nothing_is_staged_in_the_cache(self, tmp_path):
        """The mirrors take the bytes from the stream, so an upload goes to
        the stores only, even with ``stage_uploads`` on."""
        cache = LocalCache(
            CacheConfig.from_dict(
                {
                    "enabled": True,
                    "max_size_mb": 0,
                    "max_files": 0,
                    "max_file_size_mb": 0,
                    "block_kb": 64,
                    "stage_uploads": True,
                    "keep_for_reads": True,
                }
            ),
            directory=str(tmp_path / "cache"),
        )
        cache.load()
        staged: List[str] = []
        open_staging = cache.open_staging

        def recording_open_staging(fs, version_id, size):
            staged.append(version_id)
            return open_staging(fs, version_id, size)

        cache.open_staging = recording_open_staging  # type: ignore[method-assign]
        stack = await Stack(telegram_like(), discord_like(), cache=cache).init()
        await stack.put("a.bin", DATA)

        assert staged == []
        assert cache.stats()["entries"] == 0
        assert (await stack.version("a.bin")).has_copy_in("dc:9")

    async def test_a_forwarding_mirror_is_still_copied_inline(self):
        tg, spare, dc = telegram_like(), telegram_spare(), discord_like()
        stack = await Stack(tg, spare, dc).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")

        assert version.has_copy_in("tg:2") and version.has_copy_in("dc:9")
        assert "tg:2" in version.mirrors and "dc:9" in version.replicas

    async def test_a_complete_write_is_not_queued(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        stack = await Stack(telegram_like(), discord_like(), queue=queue).init()
        await stack.put("a.bin", DATA)
        assert queue.pending("fs") == []

    async def test_a_mirror_that_failed_mid_stream_is_queued(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        tg, dc = telegram_like(), discord_like()
        dc.fail.add("upload")
        stack = await Stack(tg, dc, queue=queue).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")

        assert not version.has_copy_in("dc:9")
        assert [item.path for item in queue.pending("fs")] == ["/a.bin"]
        assert await stack.read("a.bin") == DATA

    async def test_a_failing_primary_fails_the_write_and_stops_the_mirror(self):
        tg, dc = telegram_like(), discord_like()
        stack = await Stack(tg, dc).init()
        before = set(dc.messages)  # the metadata blob and its pinned copy
        tg.fail.add("upload")
        with pytest.raises(TechnicalError):
            await stack.put("a.bin", DATA)
        assert set(dc.messages) == before

    async def test_a_copy_has_no_stream_and_goes_to_the_queue(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        stack = await Stack(telegram_like(), discord_like(), queue=queue).init()
        await stack.put("a.bin", DATA)
        fr = stack.root.find_file("a.bin")

        await stack.file_api.copy(stack.root, fr, "b.bin")

        assert [item.path for item in queue.pending("fs")] == ["/b.bin"]
