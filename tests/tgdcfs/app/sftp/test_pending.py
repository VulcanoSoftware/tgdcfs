import asyncio
import tempfile

import pytest
from tgdcfs.app.sftp.handles import SpoolReadHandle, WriteHandle
from tgdcfs.app.sftp.pending import PendingUploads
from tgdcfs.config import SFTPConfig
from tgdcfs.core import Ops

DATA = bytes(range(256)) * 40  # 10 KiB


def make_spool(data: bytes = DATA) -> "tempfile.SpooledTemporaryFile[bytes]":
    spool = tempfile.SpooledTemporaryFile(max_size=1024 * 1024)
    spool.write(data)
    return spool


class GatedOps:
    """An ``Ops`` stand-in whose uploads wait for the test to let them finish."""

    def __init__(self, fail: bool = False):
        self.gate = asyncio.Event()
        self.started = asyncio.Event()
        self.uploaded: list[tuple[str, bytes]] = []
        self.fail = fail

    async def upload_from_stream(self, stream, size, remote):
        self.started.set()
        await self.gate.wait()
        if self.fail:
            raise RuntimeError("telegram said no")
        data = b"".join([chunk async for chunk in stream])
        assert len(data) == size
        self.uploaded.append((remote, data))


class TestSubmit:
    async def test_returns_before_the_upload_has_finished(self):
        pending, ops = PendingUploads(), GatedOps()

        entry = await pending.submit(ops, "notes", "/a.bin", make_spool(), len(DATA))

        assert not entry.done.is_set()
        assert pending.get("notes", "/a.bin") is entry
        assert ops.uploaded == []

        ops.gate.set()
        await pending.drain()
        assert ops.uploaded == [("/a.bin", DATA)]
        assert pending.get("notes", "/a.bin") is None
        assert pending.held_bytes == 0

    async def test_the_spool_is_readable_while_pending(self):
        pending, ops = PendingUploads(), GatedOps()
        entry = await pending.submit(ops, "notes", "/a.bin", make_spool(), len(DATA))

        assert entry.read_at(256, 4) == DATA[256:260]

        ops.gate.set()
        await pending.drain()

    async def test_a_failed_upload_is_recorded_and_forgotten(self, caplog):
        pending, ops = PendingUploads(), GatedOps(fail=True)
        entry = await pending.submit(ops, "notes", "/a.bin", make_spool(), len(DATA))

        ops.gate.set()
        await pending.drain()

        assert isinstance(entry.error, RuntimeError)
        assert entry.done.is_set()
        assert pending.get("notes", "/a.bin") is None
        assert pending.held_bytes == 0
        assert "telegram said no" in caplog.text

    async def test_uploads_of_one_path_run_in_the_order_they_were_closed(self):
        order: list[bytes] = []
        first_gate = asyncio.Event()

        class Ops:
            def __init__(self, data: bytes, gate: asyncio.Event):
                self.data, self.gate = data, gate

            async def upload_from_stream(self, stream, size, remote):
                await self.gate.wait()
                order.append(b"".join([c async for c in stream]))

        pending = PendingUploads()
        done = asyncio.Event()
        done.set()
        await pending.submit(Ops(b"one", first_gate), "n", "/f", make_spool(b"one"), 3)
        await pending.submit(Ops(b"two", done), "n", "/f", make_spool(b"two"), 3)

        await asyncio.sleep(0.05)
        assert order == []  # the second one is not allowed to overtake

        first_gate.set()
        await pending.drain()
        assert order == [b"one", b"two"]


class TestBackpressure:
    async def test_waits_for_room_when_the_cap_is_reached(self):
        pending, first, second = PendingUploads(max_bytes=100), GatedOps(), GatedOps()
        await pending.submit(first, "n", "/a", make_spool(b"x" * 80), 80)

        waiting = asyncio.create_task(
            pending.submit(second, "n", "/b", make_spool(b"y" * 80), 80)
        )
        await asyncio.sleep(0.05)
        assert not waiting.done()

        first.gate.set()
        await asyncio.wait_for(waiting, timeout=2)
        second.gate.set()
        await pending.drain()
        assert [r for r, _ in first.uploaded + second.uploaded] == ["/a", "/b"]

    async def test_a_file_above_the_cap_is_accepted_when_nothing_is_pending(self):
        pending, ops = PendingUploads(max_bytes=10), GatedOps()

        await asyncio.wait_for(
            pending.submit(ops, "n", "/big", make_spool(b"z" * 50), 50), timeout=2
        )

        ops.gate.set()
        await pending.drain()
        assert len(ops.uploaded) == 1

    async def test_a_cap_of_zero_means_no_cap(self):
        pending, first, second = PendingUploads(max_bytes=0), GatedOps(), GatedOps()

        await asyncio.wait_for(
            pending.submit(first, "n", "/a", make_spool(b"x" * 80), 80), timeout=2
        )
        await asyncio.wait_for(
            pending.submit(second, "n", "/b", make_spool(b"y" * 80), 80), timeout=2
        )

        first.gate.set()
        second.gate.set()
        await pending.drain()


class TestWaiting:
    async def test_wait_returns_when_the_path_is_done(self):
        pending, ops = PendingUploads(), GatedOps()
        await pending.submit(ops, "n", "/dir/a", make_spool(), len(DATA))

        waiter = asyncio.create_task(pending.wait("n", "/dir/a"))
        await asyncio.sleep(0.05)
        assert not waiter.done()

        ops.gate.set()
        await asyncio.wait_for(waiter, timeout=2)

    async def test_wait_under_covers_everything_below_a_directory(self):
        pending, ops = PendingUploads(), GatedOps()
        await pending.submit(ops, "n", "/dir/sub/a", make_spool(), len(DATA))

        under = asyncio.create_task(pending.wait_under("n", "/dir"))
        sibling = asyncio.create_task(pending.wait_under("n", "/dir/sub/a2"))
        other_client = asyncio.create_task(pending.wait_under("other", "/dir"))
        await asyncio.sleep(0.05)

        assert not under.done()
        assert sibling.done() and other_client.done()

        ops.gate.set()
        await asyncio.wait_for(under, timeout=2)

    async def test_waiting_for_an_idle_path_returns_at_once(self):
        pending = PendingUploads()

        await asyncio.wait_for(pending.wait("n", "/nothing"), timeout=1)
        await asyncio.wait_for(pending.wait_under("n", "/"), timeout=1)


class TestSpoolReadHandle:
    async def test_reads_the_spooled_bytes(self):
        pending, ops = PendingUploads(), GatedOps()
        entry = await pending.submit(ops, "n", "/a", make_spool(), len(DATA))
        handle = SpoolReadHandle(entry)

        assert handle.size == len(DATA)
        assert await handle.read(0, 100) == DATA[:100]
        assert await handle.read(len(DATA) - 5, 100) == DATA[-5:]
        assert await handle.read(len(DATA), 10) == b""
        assert await handle.read(0, 0) == b""

        await handle.close()
        ops.gate.set()
        await pending.drain()

    async def test_stays_readable_after_the_upload_finished(self):
        pending, ops = PendingUploads(), GatedOps()
        entry = await pending.submit(ops, "n", "/a", make_spool(), len(DATA))
        handle = SpoolReadHandle(entry)

        ops.gate.set()
        await pending.drain()

        assert await handle.read(10, 20) == DATA[10:30]
        await handle.close()
        with pytest.raises(ValueError):
            entry.read_at(0, 1)  # now the last reader is gone, the spool is too

    async def test_close_is_idempotent(self):
        pending, ops = PendingUploads(), GatedOps()
        entry = await pending.submit(ops, "n", "/a", make_spool(), len(DATA))
        handle = SpoolReadHandle(entry)

        await handle.close()
        await handle.close()
        assert await handle.read(0, 10) == b""

        ops.gate.set()
        await pending.drain()


class TestWriteHandleWriteBehind:
    @pytest.fixture
    def ops(self, mocker):
        ops = mocker.Mock(spec=Ops)
        ops.upload_from_stream = mocker.AsyncMock()
        return ops

    async def test_close_hands_the_spool_over_instead_of_uploading(self, ops):
        pending = PendingUploads()
        gated = GatedOps()
        handle = WriteHandle(
            gated,  # type: ignore[arg-type]
            "/a.bin",
            spool_max_bytes=1024,
            pending=pending,
            client_name="notes",
        )
        await handle.write(0, DATA)

        await asyncio.wait_for(handle.close(), timeout=1)

        entry = pending.get("notes", "/a.bin")
        assert entry is not None and entry.size == len(DATA)
        assert entry.read_at(0, 4) == DATA[:4]  # the spool survived the close

        gated.gate.set()
        await pending.drain()
        assert gated.uploaded == [("/a.bin", DATA)]

    async def test_an_untouched_handle_submits_nothing(self, ops):
        pending = PendingUploads()
        handle = WriteHandle(
            ops, "/a.bin", spool_max_bytes=1024, pending=pending, client_name="n"
        )

        await handle.close()

        assert pending.get("n", "/a.bin") is None
        ops.upload_from_stream.assert_not_awaited()

    async def test_an_aborted_handle_submits_nothing(self, ops):
        pending = PendingUploads()
        handle = WriteHandle(
            ops, "/a.bin", spool_max_bytes=1024, pending=pending, client_name="n"
        )
        await handle.write(0, b"partial")

        await handle.abort()

        assert pending.get("n", "/a.bin") is None
        ops.upload_from_stream.assert_not_awaited()

    async def test_the_spool_is_released_when_the_hand_over_fails(self, ops, mocker):
        pending = mocker.Mock(spec=PendingUploads)
        pending.submit = mocker.AsyncMock(side_effect=RuntimeError("no room"))
        handle = WriteHandle(
            ops, "/a.bin", spool_max_bytes=1024, pending=pending, client_name="n"
        )
        await handle.write(0, b"data")

        with pytest.raises(RuntimeError):
            await handle.close()

        assert handle._spool.closed


class TestConfig:
    def test_write_behind_is_off_by_default(self):
        cfg = SFTPConfig.from_dict({})

        assert cfg.write_behind is False
        assert cfg.write_behind_max_mb == 2048
        assert cfg.write_behind_max_bytes == 2048 * 1024 * 1024

    def test_reads_the_values(self):
        cfg = SFTPConfig.from_dict({"write_behind": True, "write_behind_max_mb": 512})

        assert cfg.write_behind is True
        assert cfg.write_behind_max_bytes == 512 * 1024 * 1024

    def test_zero_means_no_cap(self):
        assert (
            SFTPConfig.from_dict({"write_behind_max_mb": 0}).write_behind_max_bytes == 0
        )

    def test_a_negative_cap_is_rejected(self):
        with pytest.raises(ValueError, match="write_behind_max_mb"):
            SFTPConfig.from_dict({"write_behind_max_mb": -1})


class TestStreamConfig:
    def test_the_default_segment_is_256_mb(self):
        cfg = SFTPConfig.from_dict({})

        assert cfg.stream_segment_mb == 256
        assert cfg.stream_segment_bytes == 256 * 1024 * 1024

    def test_the_segment_size_can_be_set(self):
        cfg = SFTPConfig.from_dict({"stream_segment_mb": 64})

        assert cfg.stream_segment_bytes == 64 * 1024 * 1024

    def test_there_is_no_switch_to_turn_streaming_off(self):
        assert not hasattr(SFTPConfig.from_dict({}), "stream_uploads")

    @pytest.mark.parametrize("value", [0, -1, 1901, 4096])
    def test_a_segment_must_fit_one_telegram_document(self, value):
        with pytest.raises(ValueError, match="stream_segment_mb"):
            SFTPConfig.from_dict({"stream_segment_mb": value})

    def test_the_largest_segment_is_accepted(self):
        cfg = SFTPConfig.from_dict({"stream_segment_mb": 1900})

        assert cfg.stream_segment_mb == 1900
