"""DiscordStore on a fake bot: overflow, partitioning, retries, deletion."""

from dataclasses import dataclass, field
import asyncio
from typing import AsyncIterator, Dict, List, Optional
from unittest.mock import AsyncMock

import discord
import pytest

from tgdcfs.backends.discord.client import DiscordBotAPI, SentMessage
from tgdcfs.backends.discord.store import (
    MAX_TEXT_CHARS,
    OVERFLOW_CAPTION,
    OVERFLOW_FILENAME,
    DiscordStore,
)
from tgdcfs.errors import MessageNotFound, NoPinnedMessage, TechnicalError
from tgdcfs.reqres import (
    Document,
    FileMessageFromBuffer,
    FileMessageFromStream,
    MessageResp,
)
from tgdcfs.utils.message_cache import channel_cache

CHANNEL = 4242


@dataclass
class FakeChannel:
    """What the bots of one test share: the messages and who sent them."""

    messages: Dict[int, dict] = field(default_factory=dict)
    authors: Dict[int, int] = field(default_factory=dict)
    next_id: int = 1000


class FakeBot(DiscordBotAPI):
    """Records what the store asks for and serves it from memory.

    Bots given the same ``FakeChannel`` see each other's messages, like
    real bots in one channel; each message remembers which bot sent it,
    and only that bot may edit it, as on Discord.
    """

    def __init__(self, user_id: int = 1, channel: Optional[FakeChannel] = None):
        self.name = f"fake{user_id}"
        self._user_id = user_id
        self.channel = channel if channel is not None else FakeChannel()
        self.calls: List[str] = []
        self.fail_next: Dict[str, List[Exception]] = {}
        self.pinned: List[int] = []
        self.deleted: List[List[int]] = []
        self.honour_range = True

    @property
    def user_id(self) -> Optional[int]:
        return self._user_id

    @property
    def messages(self) -> Dict[int, dict]:
        return self.channel.messages

    @property
    def authors(self) -> Dict[int, int]:
        return self.channel.authors

    @property
    def next_id(self) -> int:
        return self.channel.next_id

    @next_id.setter
    def next_id(self, value: int) -> None:
        self.channel.next_id = value

    def _maybe_fail(self, method: str) -> None:
        self.calls.append(method)
        if queue := self.fail_next.get(method):
            raise queue.pop(0)

    def _add(self, text: str = "", data: Optional[bytes] = None, name: str = "") -> int:
        mid = self.channel.next_id
        self.channel.next_id += 1
        self.messages[mid] = {"text": text, "data": data, "name": name}
        self.authors[mid] = self._user_id
        return mid

    def _check_author(self, message_id: int) -> None:
        if message_id not in self.messages:
            raise MessageNotFound(message_id=message_id)
        if self.authors.get(message_id, self._user_id) != self._user_id:
            raise ForeignMessage()

    async def get_author_id(self, channel_id, message_id):
        self._maybe_fail("get_author_id")
        if message_id not in self.messages:
            raise MessageNotFound(message_id=message_id)
        return self.authors.get(message_id, self._user_id)

    def _resp(self, mid: int) -> MessageResp:
        m = self.messages[mid]
        doc = None
        if m["data"] is not None:
            doc = Document(
                size=len(m["data"]),
                id=mid,
                access_hash=0,
                file_reference=b"",
                mime_type=None,
            )
        return MessageResp(message_id=mid, text=m["text"], document=doc)

    async def send_text(self, channel_id, text):
        self._maybe_fail("send_text")
        return SentMessage(self._add(text=text))

    async def send_file(self, channel_id, data, name, caption=""):
        self._maybe_fail("send_file")
        return SentMessage(self._add(text=caption, data=data, name=name))

    async def edit_text(self, channel_id, message_id, text):
        self._maybe_fail("edit_text")
        self._check_author(message_id)
        self.messages[message_id] = {"text": text, "data": None, "name": ""}
        return message_id

    async def edit_file(self, channel_id, message_id, data, name, caption=None):
        self._maybe_fail("edit_file")
        self._check_author(message_id)
        self.messages[message_id] = {"text": caption or "", "data": data, "name": name}
        return message_id

    async def get_messages(self, channel_id, message_ids):
        self._maybe_fail("get_messages")
        return [
            self._resp(mid) if mid in self.messages else None for mid in message_ids
        ]

    async def get_pinned_messages(self, channel_id):
        self._maybe_fail("get_pinned_messages")
        return [self._resp(mid) for mid in self.pinned if mid in self.messages]

    async def pin_message(self, channel_id, message_id):
        self._maybe_fail("pin_message")
        if message_id not in self.messages:
            raise MessageNotFound(message_id=message_id)
        self.pinned.append(message_id)

    async def delete_messages(self, channel_id, message_ids):
        self._maybe_fail("delete_messages")
        self.deleted.append(list(message_ids))
        for mid in message_ids:
            self.messages.pop(mid, None)

    async def download(self, channel_id, message_id, begin, end):
        self._maybe_fail("download")
        if message_id not in self.messages or self.messages[message_id]["data"] is None:
            raise MessageNotFound(message_id=message_id)
        data = self.messages[message_id]["data"]
        if end < 0 or end >= len(data):
            end = len(data) - 1
        payload = data[begin : end + 1]

        async def chunks() -> AsyncIterator[bytes]:
            for i in range(0, len(payload), 3):
                yield payload[i : i + 3]

        return chunks(), len(payload)

    async def close(self):
        self.calls.append("close")


class RateLimited(Exception):
    status = 429


class Forbidden(Exception):
    status = 403


class ForeignMessage(Exception):
    """Discord's answer to an edit of another user's message."""

    status = 403
    code = 50005


@pytest.fixture(autouse=True)
def clear_cache():
    channel_cache(CHANNEL).id._lru.clear()
    yield
    channel_cache(CHANNEL).id._lru.clear()


@pytest.fixture
def bot():
    return FakeBot()


@pytest.fixture
def store(bot):
    return DiscordStore(
        [bot], CHANNEL, max_part_bytes=10, retry_interval=0.0, max_retries=3
    )


class TestBasics:
    def test_key_and_caps(self, store):
        assert store.key == "dc:4242"
        assert store.backend == "discord"
        caps = store.caps
        assert caps.max_part_bytes == 10
        assert caps.max_text_chars == MAX_TEXT_CHARS
        assert caps.supports_server_copy is False
        assert caps.delete_age_limit_days == 14

    def test_explicit_key(self, bot):
        assert DiscordStore([bot], CHANNEL, key="dc:custom").key == "dc:custom"

    def test_needs_a_bot(self):
        with pytest.raises(ValueError):
            DiscordStore([], CHANNEL)

    def test_bots_rotate(self):
        a, b = FakeBot(1), FakeBot(2)
        store = DiscordStore([a, b], CHANNEL)
        assert store.next_bot is a
        assert store.next_bot is b
        assert store.next_bot is a


def pool(*user_ids: int) -> List[FakeBot]:
    """Bots that share one channel, so each sees the others' messages."""
    channel = FakeChannel()
    return [FakeBot(uid, channel) for uid in user_ids]


class TestEditsGoToTheAuthor:
    """A bot may edit its own messages only; the pool rotates for sends."""

    async def test_edit_uses_the_bot_that_sent_the_message(self):
        a, b = pool(1, 2)
        store = DiscordStore([a, b], CHANNEL, retry_interval=0.0, max_retries=3)

        mid = await store.send_text("v1")  # sent by a, the rotation moves on
        assert a.calls == ["send_text"]

        assert await store.edit_message_text(mid, "v2") == mid
        assert a.messages[mid]["text"] == "v2"
        assert "edit_text" in a.calls
        assert "edit_text" not in b.calls
        # Known author: no lookup was needed.
        assert "get_author_id" not in a.calls + b.calls

    async def test_overflow_edit_and_replace_document_too(self):
        a, b = pool(1, 2)
        store = DiscordStore(
            [a, b], CHANNEL, max_part_bytes=10, retry_interval=0.0, max_retries=3
        )
        mid = await store.send_text("short")
        long = "z" * (MAX_TEXT_CHARS + 1)
        assert await store.edit_message_text(mid, long) == mid
        assert a.messages[mid]["data"] == long.encode()
        assert "edit_file" not in b.calls

        sent = await store.upload(FileMessageFromBuffer.new(b"0123456789", "f"))
        part = sent[0].message_id
        author, other = (a, b) if a.authors[part] == 1 else (b, a)
        other.calls.clear()
        assert await store.replace_document(part, b"abc", "f") == part
        assert author.messages[part]["data"] == b"abc"
        assert "edit_file" not in other.calls

    async def test_author_of_an_older_message_is_looked_up(self):
        a, b = pool(1, 2)
        first = DiscordStore([a, b], CHANNEL, retry_interval=0.0, max_retries=3)
        mid = await first.send_text("before the restart")

        # A fresh store (a restart) knows nothing about who sent what.
        store = DiscordStore([b, a], CHANNEL, retry_interval=0.0, max_retries=3)
        assert await store.edit_message_text(mid, "after") == mid
        assert a.messages[mid]["text"] == "after"
        assert "get_author_id" in b.calls  # the lookup went to the next bot
        assert "edit_text" not in b.calls
        assert "edit_text" in a.calls

        # Looked up once, remembered from then on.
        a.calls.clear()
        b.calls.clear()
        await store.edit_message_text(mid, "again")
        assert "get_author_id" not in a.calls + b.calls

    async def test_message_of_a_removed_bot_is_reported_missing(self, caplog):
        a, b = pool(1, 2)
        mid = await DiscordStore([a, b], CHANNEL).send_text("by a")

        store = DiscordStore([b], CHANNEL, retry_interval=0.0, max_retries=3)
        with pytest.raises(MessageNotFound):
            await store.edit_message_text(mid, "x")
        assert "edit_text" not in b.calls
        assert "none of the configured bots" in caplog.text

    async def test_a_wrong_remembered_author_is_corrected(self):
        a, b = pool(1, 2)
        store = DiscordStore([a, b], CHANNEL, retry_interval=0.0, max_retries=3)
        mid = await store.send_text("v1")
        store._authors[mid] = b  # a stale entry

        assert await store.edit_message_text(mid, "v2") == mid
        assert a.messages[mid]["text"] == "v2"
        assert b.calls.count("edit_text") == 1  # refused once, not retried
        assert "get_author_id" in a.calls + b.calls

    async def test_foreign_message_error_is_recognised(self):
        from tgdcfs.backends.discord.store import is_foreign_message_error

        assert is_foreign_message_error(ForeignMessage())
        assert not is_foreign_message_error(Forbidden())
        assert not is_foreign_message_error(RateLimited())


class TestText:
    async def test_short_text_is_sent_as_content(self, store, bot):
        mid = await store.send_text("hello")
        assert bot.messages[mid] == {"text": "hello", "data": None, "name": ""}
        assert (await store.get_messages([mid]))[0].text == "hello"

    async def test_long_text_overflows_into_an_attachment(self, store, bot):
        text = "x" * (MAX_TEXT_CHARS + 1)
        mid = await store.send_text(text)

        stored = bot.messages[mid]
        assert stored["text"] == OVERFLOW_CAPTION
        assert stored["name"] == OVERFLOW_FILENAME
        assert stored["data"] == text.encode()

    async def test_overflow_is_resolved_on_read(self, store, bot):
        text = "y" * (MAX_TEXT_CHARS + 5)
        mid = await store.send_text(text)
        channel_cache(CHANNEL).id._lru.clear()

        message = (await store.get_messages([mid]))[0]

        assert message is not None
        assert message.text == text
        assert message.document is not None  # the attachment stays visible
        assert "download" in bot.calls

    async def test_edit_switches_between_content_and_overflow(self, store, bot):
        mid = await store.send_text("short")

        long = "z" * (MAX_TEXT_CHARS + 1)
        assert await store.edit_message_text(mid, long) == mid
        assert bot.messages[mid]["text"] == OVERFLOW_CAPTION
        assert bot.messages[mid]["data"] == long.encode()

        assert await store.edit_message_text(mid, "short again") == mid
        assert bot.messages[mid] == {"text": "short again", "data": None, "name": ""}

    async def test_edit_of_a_missing_message(self, store):
        with pytest.raises(MessageNotFound):
            await store.edit_message_text(1, "x")

    async def test_get_messages_uses_the_cache(self, store, bot):
        mid = await store.send_text("cached")
        bot.calls.clear()

        assert (await store.get_messages([mid]))[0].text == "cached"
        assert "get_messages" not in bot.calls

    async def test_get_messages_reports_missing_ones(self, store, bot):
        mid = await store.send_text("here")
        assert await store.get_messages([mid, 999]) == [
            MessageResp(message_id=mid, text="here", document=None),
            None,
        ]

    async def test_a_batch_larger_than_the_cache_is_returned_whole(self, store, bot):
        # Regression (issue #8): reading a folder with thousands of files asks
        # for more messages than the LRU cache holds; the first results used
        # to be evicted before they were returned.
        count = 3000
        for mid in range(1, count + 1):
            bot.messages[mid] = {"text": f"fd-{mid}", "data": None, "name": ""}
        channel_cache(CHANNEL).id._lru.clear()

        result = await store.get_messages(list(range(1, count + 1)))

        assert [m.text for m in result if m is not None] == [
            f"fd-{mid}" for mid in range(1, count + 1)
        ]

    async def test_search_is_unsupported(self, store):
        assert await store.search_messages("x") == []


class TestUpload:
    async def test_partitions_at_the_part_size(self, store, bot):
        data = b"0123456789" * 2 + b"abc"  # 23 bytes -> 10 + 10 + 3
        sent = await store.upload(FileMessageFromBuffer.new(buffer=data, name="f.bin"))

        assert [s.size for s in sent] == [10, 10, 3]
        assert b"".join(bot.messages[s.message_id]["data"] for s in sent) == data
        assert [bot.messages[s.message_id]["name"] for s in sent] == [
            "[part1]f.bin",
            "[part2]f.bin",
            "[part3]f.bin",
        ]

    async def test_exact_multiple(self, store, bot):
        data = b"0123456789" * 2
        sent = await store.upload(FileMessageFromBuffer.new(buffer=data, name="f"))
        assert [s.size for s in sent] == [10, 10]

    async def test_empty_file_is_one_empty_part(self, store, bot):
        sent = await store.upload(FileMessageFromBuffer.new(buffer=b"", name="e"))
        assert [s.size for s in sent] == [0]

    async def test_stream_without_size(self, store, bot):
        async def source():
            yield b"abcdefgh"
            yield b"ijklmnop"
            yield b"q"

        file_msg = FileMessageFromStream.new(stream=source(), size=17, name="s")
        sent = await store.upload(file_msg)
        assert [s.size for s in sent] == [10, 7]
        assert b"".join(bot.messages[s.message_id]["data"] for s in sent) == (
            b"abcdefghijklmnopq"
        )

    async def test_reads_no_further_ahead_than_the_upload_slots(self, bot):
        """A source faster than Discord is held back, not buffered whole:
        at most ``max_concurrent_uploads`` parts are read before one is
        sent, so the tee'd upload stream and a cache file are consumed at
        the pace of the bots."""
        gate = asyncio.Event()
        original = bot.send_file

        async def slow_send(channel_id, data, name, caption=""):
            await gate.wait()
            return await original(channel_id, data, name, caption)

        bot.send_file = slow_send  # type: ignore[method-assign]
        store = DiscordStore(
            [bot], CHANNEL, max_part_bytes=10, max_concurrent_uploads=2
        )
        reads: List[int] = []

        async def source():
            for i in range(6):
                reads.append(i)
                yield bytes([i]) * 10

        file_msg = FileMessageFromStream.new(stream=source(), size=60, name="s")
        upload = asyncio.ensure_future(store.upload(file_msg))
        for _ in range(20):
            await asyncio.sleep(0)
        assert len(reads) == 2  # two parts in memory, the rest not yet read

        gate.set()
        sent = await upload
        assert [s.size for s in sent] == [10] * 6
        assert len(reads) == 6

    async def test_transient_failures_are_retried(self, store, bot):
        bot.fail_next["send_file"] = [RateLimited(), RateLimited()]
        sent = await store.upload(FileMessageFromBuffer.new(buffer=b"data", name="f"))
        assert len(sent) == 1
        assert bot.calls.count("send_file") == 3

    async def test_permanent_failures_are_not_retried(self, store, bot):
        bot.fail_next["send_file"] = [Forbidden()]
        with pytest.raises(Forbidden):
            await store.upload(FileMessageFromBuffer.new(buffer=b"data", name="f"))
        assert bot.calls.count("send_file") == 1

    async def test_gives_up_after_max_retries(self, store, bot):
        bot.fail_next["send_file"] = [RateLimited()] * 5
        with pytest.raises(TechnicalError, match="after 3 attempts"):
            await store.upload(FileMessageFromBuffer.new(buffer=b"data", name="f"))


class TestDownload:
    async def test_ranges(self, store, bot):
        data = b"0123456789abcdef"
        mid = bot._add(data=data, name="f")

        async def read(begin, end):
            resp = await store.download_file(mid, begin, end)
            return b"".join([c async for c in resp.chunks]), resp.size

        assert await read(0, -1) == (data, 16)
        assert await read(3, 7) == (b"34567", 5)
        assert await read(10, 100) == (b"abcdef", 6)

    async def test_missing_document(self, store):
        with pytest.raises(MessageNotFound):
            await store.download_file(1, 0, -1)


class TestReplaceAndCopy:
    async def test_replace_document(self, store, bot):
        mid = bot._add(data=b"old", name="f")
        channel_cache(CHANNEL).id[mid] = bot._resp(mid)

        assert await store.replace_document(mid, b"new!", "g") == mid
        assert bot.messages[mid]["data"] == b"new!"
        cached = channel_cache(CHANNEL).id.get(mid)
        assert cached is not None and cached.document is not None
        assert cached.document.size == 4

    async def test_replace_document_too_large(self, store):
        with pytest.raises(TechnicalError, match="does not fit"):
            await store.replace_document(1, b"x" * 11, "g")

    async def test_copy_within_reuploads(self, store, bot):
        mid = bot._add(data=b"copy me", name="f")
        [new] = await store.copy_within([mid])
        assert new != mid
        assert bot.messages[new]["data"] == b"copy me"

    async def test_copy_from_is_unsupported(self, store):
        assert await store.copy_from(store, [1]) is None


class TestDeleteAndPins:
    async def test_delete_is_gated(self, store, bot):
        mid = bot._add(text="x")
        await store.delete_messages([mid])
        assert mid in bot.messages
        await store.delete_messages([mid], force=True)
        assert mid not in bot.messages

    async def test_delete_when_enabled(self, bot):
        store = DiscordStore([bot], CHANNEL, delete_on_remove=True)
        mid = bot._add(text="x")
        channel_cache(CHANNEL).id[mid] = bot._resp(mid)
        await store.delete_messages([mid, 0, mid])
        assert bot.deleted == [[mid]]
        assert channel_cache(CHANNEL).id.get(mid) is None

    async def test_delete_failures_are_logged_not_raised(self, bot):
        store = DiscordStore([bot], CHANNEL, delete_on_remove=True, max_retries=1)
        bot.fail_next["delete_messages"] = [Forbidden()]
        await store.delete_messages([1])

    async def test_pinned_message(self, store, bot):
        text_mid = bot._add(text="just text")
        doc_mid = bot._add(data=b"{}", name="metadata.json")
        await store.pin_message(text_mid)
        await store.pin_message(doc_mid)

        pinned = await store.get_pinned_message()
        assert pinned.message_id == doc_mid
        assert pinned.document is not None and pinned.document.size == 2

    async def test_overflow_pins_are_not_metadata(self, store, bot):
        mid = bot._add(text=OVERFLOW_CAPTION, data=b"{}", name=OVERFLOW_FILENAME)
        await store.pin_message(mid)
        with pytest.raises(NoPinnedMessage):
            await store.get_pinned_message()

    async def test_no_pins(self, store):
        with pytest.raises(NoPinnedMessage):
            await store.get_pinned_message()

    async def test_pin_missing(self, store):
        with pytest.raises(MessageNotFound):
            await store.pin_message(1)

    async def test_close_closes_the_bots(self, store, bot):
        await store.close()
        assert "close" in bot.calls


class TestTransientClassification:
    def test_rate_limited(self):
        from tgdcfs.backends.discord.client import is_transient

        assert is_transient(discord.RateLimited(1.0))
        assert is_transient(RateLimited())
        assert not is_transient(Forbidden())
        assert is_transient(ConnectionError())
        assert not is_transient(ValueError())


class TestEndToEnd:
    """The store under the real repositories: a Discord-only file system."""

    async def test_upload_read_and_descriptor_overflow(self, bot):
        from tgdcfs.core.api import FileApi, FileDescApi, MetaDataApi
        from tgdcfs.core.model import TGFSDirectory, TGFSMetadata
        from tgdcfs.core.repository.impl import (
            StoreFDRepository,
            StoreFileContentRepository,
        )
        from tgdcfs.core.repository.interface import IMetaDataRepository

        class MemoryMetadata(IMetaDataRepository):
            async def push(self) -> None:
                pass

            async def get(self) -> TGFSMetadata:
                return TGFSMetadata(dir=TGFSDirectory.root_dir())

        # Real message ids are 19-digit snowflakes; 160 of them do not fit
        # a 2000 character message.
        bot.next_id = 1_000_000_000_000_000_000
        store = DiscordStore([bot], CHANNEL, max_part_bytes=64, retry_interval=0.0)
        fc_repo = StoreFileContentRepository(store)
        fd_repo = StoreFDRepository(store)
        metadata_api = MetaDataApi(MemoryMetadata())
        await metadata_api.init()
        file_api = FileApi(metadata_api, FileDescApi(fd_repo, fc_repo), store)
        root = metadata_api.get_root_directory()

        data = bytes(range(256)) * 40  # 10240 bytes -> 160 parts of 64
        await file_api.upload(root, FileMessageFromBuffer.new(buffer=data, name="big"))
        fr = root.find_file("big")

        # 160 snowflake-sized ids do not fit 2000 characters: the
        # descriptor overflowed into an attachment and reads back whole.
        assert bot.messages[fr.message_id]["text"] == OVERFLOW_CAPTION
        fd = await fd_repo.get(fr)
        version = fd.get_latest_version()
        assert len(version.message_ids) == 160
        assert version.store == "dc:4242"

        stream = await file_api.retrieve(fr, 100, 5000, "big")
        assert b"".join([c async for c in stream]) == data[100:5001]
