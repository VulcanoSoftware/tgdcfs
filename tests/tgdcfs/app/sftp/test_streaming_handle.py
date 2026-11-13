"""``WriteHandle`` with ``stream_segment_bytes``: uploading while receiving.

Telegram needs a part's size before its first byte and SFTP never says how
large a file is, so once a full segment has arrived the handle starts
feeding the stores from the spool while the client keeps writing, instead
of waiting for the close.
"""

import asyncio
from typing import Callable

import pytest
from tgdcfs.app.sftp.handles import WriteHandle

SEGMENT = 100


class StreamingOps:
    """An ``Ops`` stand-in that consumes the unsized stream like the stores do."""

    def __init__(self, fail_with: Exception | None = None):
        self.received = bytearray()
        self.started = asyncio.Event()
        self.finished = asyncio.Event()
        self.args: tuple = ()
        self.fail_with = fail_with
        self.sized_uploads = 0

    async def upload_from_unsized_stream(self, stream, path, segment, spool_dir):
        self.args = (path, segment, spool_dir)
        self.started.set()
        async for chunk in stream:
            self.received.extend(chunk)
            if self.fail_with is not None:
                raise self.fail_with
        self.finished.set()

    async def upload_from_stream(self, stream, size, path):
        self.sized_uploads += 1
        async for _ in stream:
            pass


def handle(ops, segment: int = SEGMENT) -> WriteHandle:
    return WriteHandle(
        ops,  # type: ignore[arg-type]
        "/big.bin",
        spool_max_bytes=64,
        stream_segment_bytes=segment,
    )


async def until(condition: Callable[[], bool], timeout: float = 5) -> None:
    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout)


def data(size: int, start: int = 0) -> bytes:
    return bytes((start + i) % 251 for i in range(size))


class TestStartsWhileReceiving:
    async def test_a_small_file_is_not_streamed(self):
        ops = StreamingOps()
        h = handle(ops)

        await h.write(0, data(SEGMENT - 1))
        await h.close()

        assert not ops.started.is_set()
        assert ops.sized_uploads == 1

    async def test_the_upload_starts_once_a_segment_has_arrived(self):
        ops = StreamingOps()
        h = handle(ops)

        await h.write(0, data(SEGMENT - 1))
        await asyncio.sleep(0.05)
        assert not ops.started.is_set()

        await h.write(SEGMENT - 1, data(1, SEGMENT - 1))
        await asyncio.wait_for(ops.started.wait(), timeout=5)

        # Before the close: the stores already have the first segment.
        await until(lambda: len(ops.received) >= SEGMENT)
        assert bytes(ops.received[:SEGMENT]) == data(SEGMENT)
        assert ops.args == ("/big.bin", SEGMENT, None)
        assert ops.sized_uploads == 0

        await h.close()

    async def test_the_stream_follows_the_client(self):
        ops = StreamingOps()
        h = handle(ops)
        written = bytearray()

        for i in range(10):
            chunk = data(37, i * 37)
            await h.write(len(written), chunk)
            written.extend(chunk)
            if len(written) >= SEGMENT:
                await until(lambda: len(ops.received) == len(written))

        await h.close()

        assert bytes(ops.received) == bytes(written)
        assert ops.finished.is_set()

    async def test_close_waits_for_the_last_segment(self):
        ops = StreamingOps()
        h = handle(ops)
        await h.write(0, data(250))

        await asyncio.wait_for(h.close(), timeout=5)

        assert ops.finished.is_set()
        assert bytes(ops.received) == data(250)

    async def test_a_file_of_exactly_one_segment_is_streamed(self):
        ops = StreamingOps()
        h = handle(ops)
        await h.write(0, data(SEGMENT))

        await h.close()

        assert bytes(ops.received) == data(SEGMENT)


class TestWritesOutOfOrder:
    async def test_writes_before_the_first_segment_may_arrive_in_any_order(self):
        ops = StreamingOps()
        h = handle(ops)

        await h.write(60, data(60, 60))
        await h.write(0, data(60))
        await h.close()

        assert bytes(ops.received) == data(120)

    async def test_the_stream_waits_for_a_hole_to_be_filled(self):
        ops = StreamingOps()
        h = handle(ops)

        await h.write(SEGMENT, data(SEGMENT, SEGMENT))  # beyond a hole
        await asyncio.sleep(0.05)
        assert not ops.started.is_set()

        await h.write(0, data(SEGMENT))
        await until(lambda: len(ops.received) == 2 * SEGMENT)
        await h.close()

        assert bytes(ops.received) == data(2 * SEGMENT)

    async def test_a_hole_at_the_end_reads_as_zeros(self):
        ops = StreamingOps()
        h = handle(ops)

        await h.write(0, data(SEGMENT))
        await h.write(SEGMENT + 20, b"tail")
        await h.close()

        assert bytes(ops.received) == data(SEGMENT) + bytes(20) + b"tail"

    async def test_bytes_not_yet_sent_on_can_still_be_rewritten(self):
        ops = StreamingOps()
        h = handle(ops)
        await h.write(0, data(SEGMENT))
        await until(lambda: len(ops.received) == SEGMENT)

        await h.write(SEGMENT, data(50, 9))
        await h.write(SEGMENT + 10, b"XX")  # still only in the spool
        await h.close()

        expected = bytearray(data(SEGMENT) + data(50, 9))
        expected[SEGMENT + 10 : SEGMENT + 12] = b"XX"
        assert bytes(ops.received) == bytes(expected)


class TestRewritingSentBytes:
    async def test_it_is_refused(self):
        ops = StreamingOps()
        h = handle(ops)
        await h.write(0, data(SEGMENT))
        await until(lambda: len(ops.received) == SEGMENT)

        with pytest.raises(ValueError, match="already on their way"):
            await h.write(0, b"nope")

        await h.close()
        assert bytes(ops.received) == data(SEGMENT)  # unchanged

    async def test_the_default_path_still_allows_it(self):
        ops = StreamingOps()
        h = handle(ops)

        await h.write(0, data(SEGMENT - 10))
        await h.write(0, b"rewritten")  # below one segment: nothing has gone out
        await h.close()

        assert ops.sized_uploads == 1


class TestFailureAndAbort:
    async def test_a_failing_upload_surfaces_on_the_next_write(self):
        ops = StreamingOps(fail_with=RuntimeError("telegram said no"))
        h = handle(ops)
        await h.write(0, data(SEGMENT))
        await ops.started.wait()
        await asyncio.sleep(0.05)

        with pytest.raises(RuntimeError, match="telegram said no"):
            await h.write(SEGMENT, b"more")

        await h.abort()

    async def test_a_failing_upload_surfaces_on_close(self):
        ops = StreamingOps(fail_with=RuntimeError("telegram said no"))
        h = handle(ops)
        await h.write(0, data(SEGMENT))

        with pytest.raises(RuntimeError, match="telegram said no"):
            await h.close()

    async def test_abort_stops_the_stream_without_committing(self):
        ops = StreamingOps()
        h = handle(ops)
        await h.write(0, data(SEGMENT + 30))
        await until(lambda: len(ops.received) >= SEGMENT)

        await asyncio.wait_for(h.abort(), timeout=5)

        assert not ops.finished.is_set()

    async def test_abort_before_the_stream_started_is_plain(self):
        ops = StreamingOps()
        h = handle(ops)
        await h.write(0, data(10))

        await h.abort()

        assert not ops.started.is_set()
        assert ops.sized_uploads == 0

    async def test_writes_after_close_are_refused(self):
        ops = StreamingOps()
        h = handle(ops)
        await h.write(0, data(SEGMENT))
        await h.close()

        with pytest.raises(ValueError, match="closed"):
            await h.write(0, b"x")


class TestOff:
    async def test_without_a_segment_size_everything_is_spooled(self):
        ops = StreamingOps()
        h = handle(ops, segment=0)

        await h.write(0, data(5 * SEGMENT))
        await asyncio.sleep(0.05)
        assert not ops.started.is_set()

        await h.close()

        assert ops.sized_uploads == 1
        assert not ops.started.is_set()


class SlowOps(StreamingOps):
    """Takes the stream only once the test lets it."""

    def __init__(self):
        super().__init__()
        self.gate = asyncio.Event()

    async def upload_from_unsized_stream(self, stream, path, segment, spool_dir):
        await self.gate.wait()
        await super().upload_from_unsized_stream(stream, path, segment, spool_dir)


class TestKeepsTheDiskClear:
    async def test_a_client_that_outruns_the_upload_is_held_back(self, monkeypatch):
        monkeypatch.setattr("tgdcfs.app.sftp.handles.STREAM_WINDOW_BYTES", 1000)
        ops = SlowOps()
        h = handle(ops)
        await h.write(0, data(SEGMENT))  # starts the stream, which is stuck

        far_ahead = asyncio.create_task(h.write(5000, data(1000, 5000)))
        await asyncio.sleep(0.1)
        assert not far_ahead.done()

        # The write that closes the gap is never held up by the one behind it.
        await asyncio.wait_for(h.write(SEGMENT, data(4900, SEGMENT)), timeout=1)
        ops.gate.set()
        await asyncio.wait_for(far_ahead, timeout=5)
        await h.close()
        assert bytes(ops.received) == data(6000)

    async def test_a_write_just_ahead_is_not_held_back(self, monkeypatch):
        monkeypatch.setattr("tgdcfs.app.sftp.handles.STREAM_WINDOW_BYTES", 1000)
        ops = SlowOps()
        h = handle(ops)
        await h.write(0, data(SEGMENT))

        await asyncio.wait_for(h.write(SEGMENT, data(500, SEGMENT)), timeout=1)

        ops.gate.set()
        await h.close()

    async def test_a_failed_upload_does_not_leave_a_writer_waiting(self, monkeypatch):
        monkeypatch.setattr("tgdcfs.app.sftp.handles.STREAM_WINDOW_BYTES", 1000)
        ops = SlowOps()
        ops.fail_with = RuntimeError("telegram said no")
        h = handle(ops)
        await h.write(0, data(SEGMENT))
        waiting = asyncio.create_task(h.write(5000, data(1000, 5000)))
        await asyncio.sleep(0.05)

        ops.gate.set()

        with pytest.raises(RuntimeError, match="telegram said no"):
            await asyncio.wait_for(waiting, timeout=5)
        await h.abort()

    async def test_what_has_been_sent_is_given_back_to_the_disk(self, monkeypatch):
        monkeypatch.setattr("tgdcfs.app.sftp.handles._PUNCH_ALIGN", 4096)
        ops = StreamingOps()
        h = handle(ops)

        await h.write(0, data(40_000))
        await until(lambda: len(ops.received) == 40_000)
        await until(lambda: h._punched >= 32_768)

        if not h._can_punch:
            pytest.skip("this filesystem cannot punch holes")
        h._spool.seek(0)
        assert h._spool.read(4096) == bytes(4096)  # gone from the disk
        h._spool.seek(36_864)
        assert h._spool.read(10) == data(40_000)[36_864:36_874]  # not yet sent
        await h.close()
        assert bytes(ops.received) == data(40_000)

    async def test_a_filesystem_without_hole_punching_still_works(self, monkeypatch):
        monkeypatch.setattr("tgdcfs.app.sftp.handles._punch_hole", lambda *a: False)
        monkeypatch.setattr("tgdcfs.app.sftp.handles._PUNCH_ALIGN", 4096)
        ops = StreamingOps()
        h = handle(ops)

        await h.write(0, data(40_000))
        await h.close()

        assert bytes(ops.received) == data(40_000)
        assert h._can_punch is False
