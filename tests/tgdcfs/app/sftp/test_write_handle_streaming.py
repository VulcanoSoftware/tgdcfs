"""Stress tests for the streaming SFTP write handle.

Every scenario feeds a ``WriteHandle`` through a fake ``Ops`` that
consumes the upload stream the way the real stores do and collects a
hash of what it received, so the bytes can be compared exactly. The
point of the collection is the ordering guarantee: whatever order the
writes arrive in, the uploader must see the file from byte 0 onwards,
gaps included only once they are provably holes.
"""

import asyncio
import hashlib
import logging
import os
import random
import resource
import tempfile
import tracemalloc
from unittest.mock import Mock

import pytest

from tgdcfs.app.sftp.handles import WriteHandle
from tgdcfs.core import Ops
from tgdcfs.errors import TechnicalError

KIB = 1024
MIB = 1024 * 1024
CHUNK = 32 * KIB


class CollectingOps:
    """A fake ``Ops`` whose uploads hash the stream instead of buffering it.

    ``slow`` sleeps per chunk, so the queue between the writes and the
    upload applies real backpressure. ``gate`` holds the upload until it
    is set, which lets a test observe the handle mid-flight.
    """

    def __init__(self, slow: float = 0.0, gate: "asyncio.Event | None" = None):
        self.digests: dict = {}
        self.slow = slow
        self.gate = gate
        self.consumed = 0

    def as_mock(self) -> Mock:
        ops = Mock(spec=Ops)

        async def upload_from_stream(stream, size, remote):
            assert size == -1
            if self.gate is not None:
                await self.gate.wait()
            digest = hashlib.sha256()
            async for chunk in stream:
                digest.update(chunk)
                self.consumed += len(chunk)
                if self.slow:
                    await asyncio.sleep(self.slow)
            self.digests[remote] = digest.hexdigest()

        ops.upload_from_stream = upload_from_stream
        return ops


async def feed(handle: WriteHandle, chunks, offset: int = 0) -> None:
    for data in chunks:
        await handle.write(offset, data)
        offset += len(data)


async def feed_shuffled(
    handle: WriteHandle, payload: bytes, chunk: int, seed: int, concurrent: bool = False
):
    """Feed the payload in a random order, all writes in flight at once
    or one awaited write at a time."""
    offsets = list(range(0, len(payload), chunk))
    random.Random(seed).shuffle(offsets)
    if concurrent:
        await asyncio.gather(
            *[handle.write(off, payload[off : off + chunk]) for off in offsets]
        )
    else:
        for off in offsets:
            await handle.write(off, payload[off : off + chunk])


def digest_of(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def rss_mib() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


class TestSequentialStreaming:
    async def test_24_mib_sequential_in_32_kib_blocks(self):
        payload = os.urandom(24 * MIB)
        expected = digest_of(payload)
        ops = CollectingOps()
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        await feed(
            handle, (payload[o : o + CHUNK] for o in range(0, len(payload), CHUNK))
        )
        await handle.close()

        assert ops.digests == {"/file.bin": expected}
        assert handle._staging is None  # nothing ever needed staging

    async def test_3_mib_small_file(self):
        payload = os.urandom(3 * MIB)
        ops = CollectingOps()
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        await feed(
            handle, (payload[o : o + CHUNK] for o in range(0, len(payload), CHUNK))
        )
        await handle.close()

        assert ops.digests == {"/file.bin": digest_of(payload)}


class TestScatteredWrites:
    async def test_64_concurrent_writes_within_2_mib(self):
        payload = os.urandom(2 * MIB)
        ops = CollectingOps()
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        await feed_shuffled(handle, payload, CHUNK, seed=1)
        await handle.close()

        assert ops.digests == {"/file.bin": digest_of(payload)}

    async def test_random_order_within_6_mib(self):
        payload = os.urandom(6 * MIB)
        ops = CollectingOps()
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        await feed_shuffled(handle, payload, CHUNK, seed=2)
        await handle.close()

        assert ops.digests == {"/file.bin": digest_of(payload)}

    async def test_random_order_within_16_mib(self):
        payload = os.urandom(16 * MIB)
        ops = CollectingOps()
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        await feed_shuffled(handle, payload, CHUNK, seed=3)
        await handle.close()

        assert ops.digests == {"/file.bin": digest_of(payload)}

    async def test_random_order_within_64_mib_stays_within_the_budget(self, tmp_path):
        """Writes scattered across 64 MiB: more than the buffer budget, so
        the staging has to spill to disk and still come out byte-exact."""
        payload = os.urandom(64 * MIB)
        budget = 8 * MIB
        ops = CollectingOps()
        handle = WriteHandle(
            ops.as_mock(),
            "/file.bin",
            buffer_max_bytes=budget,
            buffer_dir=str(tmp_path),
        )

        tracemalloc.start()
        rss_before = rss_mib()
        await feed_shuffled(handle, payload, CHUNK, seed=4)

        traced_peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        rss_after = rss_mib()

        assert handle._staging is not None and handle._staging._rolled  # type: ignore[attr-defined]
        # TemporaryFile lives unlinked (O_TMPFILE), so measure the open
        # descriptor: st_blocks is the sparse-correct disk usage.
        st = os.fstat(handle._staging._file.fileno())
        logical, on_disk = st.st_size, st.st_blocks * 512
        await handle.close()

        assert ops.digests == {"/file.bin": digest_of(payload)}
        # The in-memory part of the staging honoured the budget.
        assert traced_peak < budget + 4 * MIB, f"{traced_peak} traced bytes"
        # The overflow really went to disk in upload_buffer_dir.
        assert on_disk >= 32 * MIB, f"only {on_disk} bytes on disk"
        print(
            f"\n64 MiB scattered: traced peak {traced_peak / MIB:.1f} MiB "
            f"(budget {budget / MIB:.0f} MiB), rss {rss_before:.0f} -> "
            f"{rss_after:.0f} MiB, staged file {logical / MIB:.1f} MiB "
            f"logical, {on_disk / MIB:.1f} MiB on disk"
        )


class TestUploadLogging:
    async def test_start_and_end_are_logged_with_bytes_and_speed(self, caplog):
        payload = os.urandom(2 * MIB)
        ops = CollectingOps()
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        with caplog.at_level(logging.INFO, logger="tgdcfs.app.sftp.handles"):
            await handle.write(0, payload)
            await handle.close()

        messages = [r.getMessage() for r in caplog.records]
        started = [m for m in messages if "started" in m and "/file.bin" in m]
        finished = [m for m in messages if "finished" in m and "/file.bin" in m]
        assert len(started) == 1 and len(finished) == 1
        assert str(len(payload)) in finished[0]
        assert "MiB/s" in finished[0]

    async def test_a_failure_is_logged_with_the_reason(self, caplog):
        ops = CollectingOps()
        mock = ops.as_mock()

        async def refuse(stream, size, remote):
            async for _ in stream:
                pass
            raise TechnicalError("store refused")

        mock.upload_from_stream = refuse
        handle = WriteHandle(mock, "/file.bin", buffer_max_bytes=8 * MIB)

        with caplog.at_level(logging.INFO, logger="tgdcfs.app.sftp.handles"):
            await handle.write(0, b"data")
            with pytest.raises(TechnicalError):
                await handle.close()

        failed = [
            r.getMessage()
            for r in caplog.records
            if r.levelname == "ERROR" and "/file.bin" in r.getMessage()
        ]
        assert len(failed) == 1
        assert "store refused" in failed[0]


class TestRewriteAfterStreaming:
    async def test_rewriting_the_begin_fails_clearly_and_stores_nothing(self):
        payload = os.urandom(4 * MIB)
        ops = CollectingOps()
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        await handle.write(0, payload[:MIB])
        with pytest.raises(TechnicalError, match="already streamed"):
            await handle.write(0, payload[:CHUNK])

        await handle.abort()
        assert ops.digests == {}


class TestBackpressure:
    async def test_a_slow_store_keeps_the_memory_bounded(self):
        payload = os.urandom(8 * MIB)
        ops = CollectingOps(slow=0.001)
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        tracemalloc.start()
        writer = asyncio.ensure_future(
            feed(
                handle,
                (payload[o : o + CHUNK] for o in range(0, len(payload), CHUNK)),
            )
        )
        await writer
        traced_peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        await handle.close()

        assert ops.digests == {"/file.bin": digest_of(payload)}
        # Without backpressure the whole payload would sit in the queue;
        # with it only a couple of chunks are ever alive at once.
        assert traced_peak < 2 * MIB, f"{traced_peak} traced bytes"
        assert handle._staging is None

    async def test_a_gated_store_does_not_lose_bytes(self):
        payload = os.urandom(2 * MIB)
        gate = asyncio.Event()
        ops = CollectingOps(gate=gate)
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        writer = asyncio.ensure_future(
            feed(
                handle, (payload[o : o + CHUNK] for o in range(0, len(payload), CHUNK))
            )
        )
        await asyncio.sleep(0.05)
        assert ops.consumed < len(payload)  # the upload is being held up
        gate.set()
        await writer
        await handle.close()

        assert ops.digests == {"/file.bin": digest_of(payload)}


class TestStagedOverflow:
    async def test_staged_writes_beyond_the_budget_roll_over_to_disk(self, tmp_path):
        payload = os.urandom(12 * MIB)
        ops = CollectingOps()
        handle = WriteHandle(
            ops.as_mock(),
            "/file.bin",
            buffer_max_bytes=4 * MIB,
            buffer_dir=str(tmp_path),
        )

        # One write far ahead fills the staging past the budget; the rest
        # of the file follows sequentially.
        await handle.write(8 * MIB, payload[8 * MIB :])
        assert handle._staging is not None and handle._staging._rolled  # type: ignore[attr-defined]
        await feed(handle, iter([payload[: 8 * MIB]]))
        await handle.close()

        assert ops.digests == {"/file.bin": digest_of(payload)}

    async def test_a_gap_that_is_never_filled_is_zero_filled_on_close(self):
        ops = CollectingOps()
        handle = WriteHandle(ops.as_mock(), "/file.bin", buffer_max_bytes=8 * MIB)

        await handle.write(2 * MIB, b"tail")
        await handle.close()

        expected = b"\x00" * (2 * MIB) + b"tail"
        assert ops.digests == {"/file.bin": digest_of(expected)}

    async def test_a_jump_beyond_the_max_gap_fails(self):
        ops = CollectingOps()
        handle = WriteHandle(
            ops.as_mock(),
            "/file.bin",
            buffer_max_bytes=8 * MIB,
            max_gap_bytes=4 * MIB,
        )

        with pytest.raises(TechnicalError, match="ahead of the upload stream"):
            await handle.write(5 * MIB, b"far")

        await handle.abort()
        assert ops.digests == {}
