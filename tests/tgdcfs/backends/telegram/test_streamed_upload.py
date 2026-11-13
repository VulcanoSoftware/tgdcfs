"""Uploads of unknown length, the way Telegram describes them.

https://core.telegram.org/api/files ("Streamed uploads"): when the length of
a file is not known before the upload starts, every part is sent with
``file_total_parts = -1`` except the last one, which carries the real count,
and ``upload.saveBigFilePart`` is used even when the stream turns out to be
small. These tests drive the real ``FileUploader`` and ``TelegramStore``
against a client that records every call it gets.
"""

import asyncio
from typing import AsyncIterator, List

import pytest
from tgdcfs.backends.telegram import store as store_module
from tgdcfs.backends.telegram import uploader as uploader_module
from tgdcfs.backends.telegram.interface import ITDLibClient
from tgdcfs.backends.telegram.store import TelegramStore
from tgdcfs.backends.telegram.uploader import (
    MAX_PART_ATTEMPTS,
    FileUploader,
    WorkersConfig,
)
from tgdcfs.config import get_config
from tgdcfs.errors import TechnicalError
from tgdcfs.reqres import (
    FileMessageFromStream,
    SaveBigFilePartReq,
    SaveFilePartResp,
    SendMessageResp,
)

CHUNK = get_config().tgdcfs.transfer.upload_part_size_bytes


def payload(size: int) -> bytes:
    return bytes((i * 13 + 5) % 251 for i in range(size))


async def pieces(data: bytes, piece: int = 100_000) -> AsyncIterator[bytes]:
    for i in range(0, len(data), piece):
        yield data[i : i + piece]


def unsized(data: bytes, name: str = "a.bin", piece: int = 100_000):
    return FileMessageFromStream.new(
        stream=pieces(data, piece), size=None, name=name, segment_size=1
    )


class RecordingClient:
    """Stands in for Telegram: records the parts and the final send."""

    def __init__(self):
        self.big: List[SaveBigFilePartReq] = []
        self.small_calls = 0
        self.sent: List[tuple] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self.delay = 0.0
        self.fail_part: int | None = None
        self.fail_always = False

    async def save_big_file_part(self, req: SaveBigFilePartReq) -> SaveFilePartResp:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.fail_always or req.file_part == self.fail_part:
                raise RuntimeError(f"part {req.file_part} refused")
            self.big.append(req)
            return SaveFilePartResp(success=True)
        finally:
            self.in_flight -= 1

    async def save_file_part(self, req) -> SaveFilePartResp:
        self.small_calls += 1
        return SaveFilePartResp(success=True)

    async def send_big_file(self, req) -> SendMessageResp:
        self.sent.append((req.name, req.file.parts))
        return SendMessageResp(message_id=1000 + len(self.sent))

    async def send_small_file(self, req) -> SendMessageResp:  # pragma: no cover
        raise AssertionError("a stream must never use the small-file calls")

    def assembled(self) -> bytes:
        return b"".join(r.bytes for r in sorted(self.big, key=lambda r: r.file_part))


@pytest.fixture
def client() -> RecordingClient:
    return RecordingClient()


@pytest.fixture
def no_backoff(mocker):
    return mocker.patch.object(uploader_module.asyncio, "sleep", new=mocker.AsyncMock())


def uploader_for(client, msg, **kwargs) -> FileUploader:
    return FileUploader(
        client,  # type: ignore[arg-type]
        msg,
        workers=WorkersConfig(small=2, big=3),
        **kwargs,
    )


class TestFileUploaderStreamed:
    async def test_every_part_but_the_last_has_an_open_total(self, client):
        data = payload(4 * CHUNK + 1234)
        up = uploader_for(client, unsized(data))

        taken = await up.upload()

        assert taken == len(data)
        by_part = {r.file_part: r for r in client.big}
        assert sorted(by_part) == [0, 1, 2, 3, 4]
        # -1 is what core.telegram.org/api/files prescribes, not our choice.
        assert [by_part[i].file_total_parts for i in range(4)] == [-1, -1, -1, -1]
        assert by_part[4].file_total_parts == 5
        assert client.assembled() == data

    async def test_the_final_count_is_what_the_document_is_sent_with(self, client):
        up = uploader_for(client, unsized(payload(2 * CHUNK + 7)))
        await up.upload()

        await up.send(chat_id=1)

        assert up.get_uploaded_file().parts == 3
        assert client.sent == [("a.bin", 3)]

    async def test_a_small_stream_still_uses_the_big_file_calls(self, client):
        data = payload(1000)
        up = uploader_for(client, unsized(data))

        await up.upload()

        assert client.small_calls == 0
        assert [(r.file_part, r.file_total_parts) for r in client.big] == [(0, 1)]
        assert client.assembled() == data

    async def test_a_stream_of_whole_parts_has_no_empty_part_at_the_end(self, client):
        data = payload(3 * CHUNK)

        await uploader_for(client, unsized(data)).upload()

        by_part = {r.file_part: r for r in client.big}
        assert sorted(by_part) == [0, 1, 2]
        assert all(len(r.bytes) == CHUNK for r in by_part.values())
        assert by_part[2].file_total_parts == 3

    async def test_an_empty_stream_sends_nothing(self, client):
        up = uploader_for(client, unsized(b""))

        assert await up.upload() == 0
        assert client.big == []

    async def test_the_last_part_waits_for_the_others(self, client):
        client.delay = 0.02
        order: List[int] = []
        original = client.save_big_file_part

        async def spy(req):
            res = await original(req)
            order.append(req.file_part)
            return res

        client.save_big_file_part = spy  # type: ignore[method-assign]

        await uploader_for(client, unsized(payload(4 * CHUNK + 5))).upload()

        # The real total is only said once everything before it is in.
        assert order[-1] == 4
        assert sorted(order[:-1]) == [0, 1, 2, 3]

    async def test_parts_go_out_in_parallel_up_to_the_worker_count(self, client):
        client.delay = 0.02

        await uploader_for(client, unsized(payload(10 * CHUNK))).upload()

        assert 1 < client.max_in_flight <= 3

    async def test_the_stream_is_read_at_the_pace_of_the_upload(self, client):
        client.delay = 0.05
        consumed = 0

        async def counting() -> AsyncIterator[bytes]:
            nonlocal consumed
            for _ in range(40):
                consumed += 1
                yield payload(CHUNK)

        msg = FileMessageFromStream.new(
            stream=counting(), size=None, name="a.bin", segment_size=1
        )
        task = asyncio.create_task(uploader_for(client, msg).upload())
        await asyncio.sleep(0.1)

        # Slow Telegram, fast client: the client is not read all at once.
        assert consumed < 40
        await task
        assert consumed == 40

    async def test_progress_is_reported_for_every_byte(self, client, mocker):
        data = payload(3 * CHUNK + 99)
        msg = unsized(data)
        tracker = mocker.Mock()
        tracker.cancelled = mocker.AsyncMock(return_value=False)
        tracker.update_progress = mocker.AsyncMock()
        msg.task_tracker = tracker

        await uploader_for(client, msg).upload()

        total = sum(
            c.kwargs["size_delta"] for c in tracker.update_progress.await_args_list
        )
        assert total == len(data)

    async def test_a_part_that_keeps_failing_fails_the_upload(self, client, no_backoff):
        client.fail_part = 2

        with pytest.raises(RuntimeError, match="part 2 refused"):
            await uploader_for(client, unsized(payload(5 * CHUNK))).upload()

        # The sleeps are the retries between attempts.
        assert no_backoff.await_count >= MAX_PART_ATTEMPTS - 1

    async def test_a_failure_stops_the_parts_still_in_flight(self, client, no_backoff):
        client.fail_always = True

        with pytest.raises(RuntimeError):
            await uploader_for(client, unsized(payload(8 * CHUNK))).upload()

        assert client.in_flight == 0

    async def test_a_broken_stream_fails_the_upload(self, client):
        async def breaks() -> AsyncIterator[bytes]:
            yield payload(2 * CHUNK)
            raise OSError("client went away")

        msg = FileMessageFromStream.new(
            stream=breaks(), size=None, name="a.bin", segment_size=1
        )

        with pytest.raises(OSError, match="client went away"):
            await uploader_for(client, msg).upload()

    async def test_a_cancelled_task_stops_the_upload(self, client, mocker):
        msg = unsized(payload(5 * CHUNK))
        tracker = mocker.Mock()
        tracker.cancelled = mocker.AsyncMock(return_value=True)
        tracker.update_progress = mocker.AsyncMock()
        msg.task_tracker = tracker

        with pytest.raises(TechnicalError, match="cancelled"):
            await uploader_for(client, msg).upload()

        assert client.big == []


class TestDocumentLimit:
    """A message holds one document; a longer stream goes on in the next."""

    async def test_the_stream_is_left_unread_beyond_the_limit(self, client):
        data = payload(5 * CHUNK + 10)
        msg = unsized(data)
        await msg.open()

        first = uploader_for(client, msg, max_bytes=2 * CHUNK, manage_message=False)
        assert await first.upload() == 2 * CHUNK
        assert first.get_uploaded_file().parts == 2

        second = uploader_for(client, msg, max_bytes=2 * CHUNK, manage_message=False)
        assert await second.upload() == 2 * CHUNK

        third = uploader_for(client, msg, max_bytes=2 * CHUNK, manage_message=False)
        assert await third.upload() == CHUNK + 10
        assert third.get_uploaded_file().parts == 2

        fourth = uploader_for(client, msg, max_bytes=2 * CHUNK, manage_message=False)
        assert await fourth.upload() == 0  # nothing left, no empty document

        # Each document counted its parts from zero.
        assert sorted(r.file_part for r in client.big) == [0, 0, 0, 1, 1, 1]

    async def test_a_stream_ending_on_the_limit_has_no_empty_document(self, client):
        msg = unsized(payload(4 * CHUNK))
        await msg.open()

        taken = []
        for _ in range(3):
            up = uploader_for(client, msg, max_bytes=2 * CHUNK, manage_message=False)
            taken.append(await up.upload())

        assert taken == [2 * CHUNK, 2 * CHUNK, 0]


class TestTelegramStoreStreamed:
    @pytest.fixture
    def telegram(self, mocker, client):
        tdlib = mocker.Mock()
        tdlib.account = None
        tdlib.next_bot = client
        return TelegramStore(tdlib, 123, key="tg:123")

    def test_it_says_it_can_stream(self, telegram):
        assert telegram.streams_unsized_uploads is True

    async def test_a_stream_becomes_one_document(self, telegram, client):
        data = payload(3 * CHUNK + 50)

        sent = await telegram.upload(unsized(data, name="a.bin"))

        assert [s.size for s in sent] == [len(data)]
        assert client.assembled() == data
        assert client.sent == [("[part1]a.bin", 4)]

    async def test_a_long_stream_goes_on_in_the_next_document(
        self, telegram, client, mocker
    ):
        mocker.patch.object(store_module, "PART_SIZE_DEFAULT", 2 * CHUNK)
        data = payload(5 * CHUNK + 10)

        sent = await telegram.upload(unsized(data, name="a.bin"))

        assert [s.size for s in sent] == [2 * CHUNK, 2 * CHUNK, CHUNK + 10]
        assert [name for name, _ in client.sent] == [
            "[part1]a.bin",
            "[part2]a.bin",
            "[part3]a.bin",
        ]

    async def test_a_stream_ending_on_a_document_limit_stops_there(
        self, telegram, client, mocker
    ):
        mocker.patch.object(store_module, "PART_SIZE_DEFAULT", 2 * CHUNK)

        sent = await telegram.upload(unsized(payload(4 * CHUNK), name="a.bin"))

        assert [s.size for s in sent] == [2 * CHUNK, 2 * CHUNK]

    async def test_the_parts_continue_the_numbering_of_an_earlier_one(
        self, telegram, client
    ):
        msg = unsized(payload(CHUNK), name="a.bin")
        msg.part_offset = 3

        await telegram.upload(msg)

        assert client.sent == [("[part4]a.bin", 1)]

    async def test_the_name_is_restored_afterwards(self, telegram):
        msg = unsized(payload(CHUNK), name="a.bin")

        await telegram.upload(msg)

        assert msg.name == "a.bin"

    async def test_an_empty_stream_uploads_nothing(self, telegram, client):
        assert await telegram.upload(unsized(b"")) == []
        assert client.sent == []

    async def test_a_failure_deletes_the_documents_already_sent(
        self, telegram, client, mocker, no_backoff
    ):
        mocker.patch.object(store_module, "PART_SIZE_DEFAULT", 2 * CHUNK)
        delete = mocker.patch.object(
            telegram, "delete_messages", new_callable=mocker.AsyncMock
        )
        # Fails in the third document, after two have been sent.
        client.fail_part = None
        original = client.save_big_file_part
        calls = 0

        async def flaky(req):
            nonlocal calls
            calls += 1
            if calls > 4:
                raise RuntimeError("telegram said no")
            return await original(req)

        client.save_big_file_part = flaky  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="telegram said no"):
            await telegram.upload(unsized(payload(6 * CHUNK), name="a.bin"))

        delete.assert_awaited_once()
        ids, kwargs = delete.await_args.args[0], delete.await_args.kwargs
        assert sorted(ids) == [1001, 1002]
        assert kwargs == {"force": True}

    async def test_a_sized_upload_is_not_affected(self, telegram, client, mocker):
        """The streamed branch must leave the normal upload alone."""
        spy = mocker.spy(TelegramStore, "_upload_unsized")
        msg = FileMessageFromStream.new(
            stream=pieces(payload(100)), size=100, name="a.bin"
        )

        assert not msg.unsized
        try:
            await telegram.upload(msg)
        except Exception:  # the recording client is not built for the sized path
            pass

        spy.assert_not_called()


class TestStoreFlag:
    def test_a_store_does_not_stream_unless_it_says_so(self):
        from tgdcfs.backends.base import IStore

        assert IStore.streams_unsized_uploads.fget(object()) is False  # type: ignore[attr-defined]


def test_the_client_interface_has_the_calls_the_uploader_needs():
    assert hasattr(ITDLibClient, "save_big_file_part")
    assert hasattr(ITDLibClient, "send_big_file")
