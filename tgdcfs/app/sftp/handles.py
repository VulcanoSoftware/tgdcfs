import asyncio
import errno
import logging
import os
import tempfile
import time
from typing import AsyncIterator, List, Optional

from tgdcfs.core import Ops
from tgdcfs.errors import TechnicalError

logger = logging.getLogger(__name__)

MIB = 1024 * 1024

SEEK_DATA = getattr(os, "SEEK_DATA", 3)
SEEK_HOLE = getattr(os, "SEEK_HOLE", 4)

# A forward jump shorter than this is bridged by reading through the open
# stream. Re-opening costs a Telegram round trip, reading through costs
# bandwidth we have already paid for -- one mebibyte is where the two meet
# for the clients that leave small gaps (they typically skip a few kibibytes).
SEEK_FORWARD_THRESHOLD = 1024 * 1024

# Staged bytes are handed to the uploader in pieces of this size, and the
# zero-fill of a gap that no write ever covered goes out in pieces too.
STAGED_CHUNK = 1024 * 1024

# A write this far ahead of the upload stream is treated as hostile and
# fails the upload. Everything short of it is staged: in memory up to
# ``upload_buffer_size_mb``, on disk beyond that. Configurable through
# ``sftp.max_ahead_gap_mb``.
DEFAULT_MAX_AHEAD_GAP_BYTES = 1024 * 1024 * 1024


class Handle:
    """Shared behaviour of the objects handed back to the SFTP layer."""

    @property
    def size(self) -> int:
        raise NotImplementedError

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Answer the sparse-region probing done on a transfer's source.

        Before copying, an SFTP client may walk the source with
        ``SEEK_DATA``/``SEEK_HOLE`` to find out which regions it can skip.
        Nothing stored here is ever sparse, so report a single solid data
        region spanning the whole file. Ordinary seeking is not offered --
        reads carry their own offset.
        """
        size = self.size
        if whence in (SEEK_DATA, SEEK_HOLE):
            if offset >= size:
                raise OSError(errno.ENXIO, os.strerror(errno.ENXIO))
            return offset if whence == SEEK_DATA else size
        raise OSError(errno.ESPIPE, os.strerror(errno.ESPIPE))


class ReadHandle(Handle):
    """Serves random-access SFTP reads from a one-shot download stream.

    ``Ops.download`` yields the file from a given offset onwards and cannot
    seek, while SFTP clients read at arbitrary offsets and pipeline their
    requests. So we keep one stream plus the offset it has reached: reads
    that continue where the last one stopped -- the overwhelming majority --
    are served straight from it, and anything else re-opens the stream at
    the requested offset, which is exactly what a WebDAV range request does.
    """

    def __init__(self, ops: Ops, path: str, name: str, size: int):
        self._ops = ops
        self._path = path
        self._name = name
        self._size = size

        self._stream: Optional[AsyncIterator[bytes]] = None
        self._pos = 0
        self._buffer = bytearray()
        self._lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return self._size

    async def read(self, offset: int, size: int) -> bytes:
        """Return up to ``size`` bytes at ``offset``; empty bytes mean EOF."""
        if size <= 0 or offset >= self._size:
            return b""

        async with self._lock:
            if self._stream is None or offset < self._pos:
                await self._reopen(offset)
            elif offset > self._pos:
                gap = offset - self._pos
                if gap > SEEK_FORWARD_THRESHOLD:
                    await self._reopen(offset)
                elif len(await self._consume(gap)) < gap:
                    # The gap ran past the end of the file.
                    return b""

            return await self._consume(size)

    async def close(self) -> None:
        async with self._lock:
            await self._close_stream()
            self._buffer = bytearray()

    async def _reopen(self, offset: int) -> None:
        await self._close_stream()
        self._buffer = bytearray()
        self._pos = offset
        self._stream = await self._ops.download(self._path, offset, -1, self._name)

    async def _close_stream(self) -> None:
        stream, self._stream = self._stream, None
        aclose = getattr(stream, "aclose", None)
        if aclose is None:
            return
        try:
            await aclose()
        except Exception as ex:  # pragma: no cover - best effort cleanup
            logger.debug("Failed to close download stream for %s: %s", self._path, ex)

    async def _consume(self, count: int) -> bytes:
        while len(self._buffer) < count:
            chunk = await self._next_chunk()
            if not chunk:
                break
            self._buffer.extend(chunk)

        data = bytes(self._buffer[:count])
        del self._buffer[: len(data)]
        self._pos += len(data)
        return data

    async def _next_chunk(self) -> bytes:
        if self._stream is None:
            return b""
        try:
            return await self._stream.__anext__()
        except StopAsyncIteration:
            return b""


class WriteHandle(Handle):
    """Streams an SFTP upload to the stores while the client is still writing.

    SFTP never announces the size of an upload, and the stores accept a
    stream of unknown length (Telegram documents the protocol officially,
    core.telegram.org/api/files, "Streamed uploads"), so writes at the
    stream's position are forwarded to the upload as they arrive: the
    one-slot queue between the writes and the uploader applies
    backpressure, and nothing touches the disk.

    Writes that arrive ahead of the position -- pipelined clients send
    several requests at once, and sshfs or rclone may have many in flight
    -- are staged in a spooled buffer: in memory up to
    ``buffer_max_bytes``, on disk in ``buffer_dir`` beyond that. The
    upload moves through the staged region only while it is contiguous
    from the stream's position, so the bytes always reach the stores in
    file order and the stream waits for a gap to be filled by the write
    that belongs there. A gap that is never filled is zero-filled on
    close, exactly as a sparse write deserves.

    A write that lands behind the stream's position needs bytes that are
    already on the wire, so the upload fails rather than store a corrupted
    file. A forward jump wider than ``max_gap_bytes`` is treated as
    hostile and fails the same way; within that bound the client may write
    as far ahead as it likes, the disk takes the overflow.
    """

    def __init__(
        self,
        ops: Ops,
        path: str,
        buffer_max_bytes: int,
        buffer_dir: Optional[str] = None,
        max_gap_bytes: int = DEFAULT_MAX_AHEAD_GAP_BYTES,
    ):
        self._ops = ops
        self._path = path
        self._buffer_max_bytes = buffer_max_bytes
        self._buffer_dir = buffer_dir
        self._max_gap_bytes = max_gap_bytes

        self._queue: Optional[asyncio.Queue[Optional[bytes]]] = None
        self._task: Optional[asyncio.Task] = None
        self._cond: Optional[asyncio.Condition] = None
        self._queued_bytes = 0

        # ``_pos`` is what the upload stream has been handed, ``_size`` the
        # highest byte the client wrote; the two differ by whatever is
        # queued or staged.
        self._pos = 0
        self._size = 0
        # The written-but-not-yet-streamed ranges, merged and sorted; the
        # bytes themselves sit in ``_staging`` at their file offsets.
        self._staged: List[List[int]] = []
        self._staging: Optional[tempfile.SpooledTemporaryFile] = None

        self._dirty = False
        self._closed = False
        self._error: Optional[Exception] = None
        self._lock = asyncio.Lock()

    @property
    def path(self) -> str:
        return self._path

    @property
    def size(self) -> int:
        return self._size

    async def write(self, offset: int, data: bytes) -> None:
        async with self._lock:
            if self._closed:
                raise ValueError("write on a closed handle")
            self._raise_if_failed()
            if not data:
                return
            if offset < self._pos:
                raise TechnicalError(
                    f"Write at offset {offset} of {self._path} lands behind bytes "
                    f"that were already streamed"
                )
            if offset > self._pos or self._overlaps_staged(offset, len(data)):
                await self._stage(offset, data)
            else:
                self._dirty = True
                await self._enqueue(data)
                self._advance(offset + len(data))
            # A staged write may have filled the gap at the position.
            await self._drain()

    async def close(self) -> None:
        """Finish the upload as a new version of the file."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                if not self._dirty:
                    if self._task is not None:
                        await asyncio.gather(self._task, return_exceptions=True)
                    return
                await self._flush_staged()
                self._raise_if_failed()
                _, task = self._ensure_task()
                await self._enqueue(None)
                await task
                self._raise_if_failed()
            finally:
                await self._close_staging()

    async def abort(self) -> None:
        """Drop the upload without touching the stored file."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._task is not None:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            await self._close_staging()

    # -- streaming ---------------------------------------------------------

    async def _stage(self, offset: int, data: bytes) -> None:
        span = offset + len(data) - self._pos
        if span > self._max_gap_bytes:
            raise TechnicalError(
                f"Write at offset {offset} of {self._path} runs "
                f"{span} bytes ahead of the upload stream; the most that "
                f"is accepted is {self._max_gap_bytes}"
            )
        await asyncio.to_thread(self._write_staging, offset, data)
        self._add_staged(offset, offset + len(data))
        self._dirty = True
        self._size = max(self._size, offset + len(data))

    async def _drain(self) -> None:
        # Only a region contiguous from the position may stream: a gap can
        # still be filled by the write that belongs there.
        while self._staged and self._staged[0][0] == self._pos:
            start, end = self._staged.pop(0)
            await self._enqueue_staged(start, end)
            self._advance(end)

    async def _flush_staged(self) -> None:
        """Hand every staged write to the uploader, zero-filling the gaps."""
        while self._staged:
            start, end = self._staged.pop(0)
            if start > self._pos:
                await self._enqueue_zeros(start - self._pos)
                self._advance(start)
            await self._enqueue_staged(start, end)
            self._advance(end)

    def _advance(self, end: int) -> None:
        self._size = max(self._size, end)
        self._pos = end

    def _add_staged(self, start: int, end: int) -> None:
        intervals: List[List[int]] = []
        for s, e in self._staged:
            if e < start or s > end:
                intervals.append([s, e])
            else:
                start, end = min(start, s), max(end, e)
        intervals.append([start, end])
        intervals.sort()
        self._staged = intervals

    def _overlaps_staged(self, offset: int, length: int) -> bool:
        return any(s < offset + length and offset < e for s, e in self._staged)

    def _write_staging(self, offset: int, data: bytes) -> None:
        if self._staging is None:
            self._staging = tempfile.SpooledTemporaryFile(
                max_size=self._buffer_max_bytes, dir=self._buffer_dir
            )
        if offset + len(data) > self._buffer_max_bytes:
            # A seek far beyond the end inflates the in-memory buffer with
            # the gap before the rollover check ever sees it; go to disk
            # first, where the gap costs nothing.
            self._staging.rollover()
        self._staging.seek(offset)
        self._staging.write(data)

    def _read_staging(self, offset: int, count: int) -> bytes:
        staging = self._staging
        if staging is None:
            raise TechnicalError(
                f"The upload of {self._path} has no staging buffer to read from"
            )
        staging.seek(offset)
        return staging.read(count)

    async def _enqueue_staged(self, start: int, end: int) -> None:
        offset = start
        while offset < end:
            count = min(STAGED_CHUNK, end - offset)
            data = await asyncio.to_thread(self._read_staging, offset, count)
            if not data:
                raise TechnicalError(
                    f"The staged bytes at {offset} of {self._path} are gone"
                )
            await self._enqueue(data)
            offset += len(data)

    async def _enqueue_zeros(self, count: int) -> None:
        while count > 0:
            size = min(STAGED_CHUNK, count)
            await self._enqueue(b"\x00" * size)
            count -= size

    async def _close_staging(self) -> None:
        staging, self._staging = self._staging, None
        self._staged = []
        if staging is not None:
            await asyncio.to_thread(staging.close)

    def _ensure_task(
        self,
    ) -> "tuple[asyncio.Queue[Optional[bytes]], asyncio.Task]":
        if self._task is None or self._queue is None:
            self._queue = asyncio.Queue(maxsize=1)
            self._cond = asyncio.Condition()
            self._task = asyncio.create_task(
                self._run_upload(), name=f"sftp-upload-{self._path}"
            )
        return self._queue, self._task

    async def _enqueue(self, data: Optional[bytes]) -> None:
        queue, task = self._ensure_task()
        while True:
            self._raise_if_failed()
            if task.done():
                self._raise_if_failed()
                raise TechnicalError(
                    f"The upload of {self._path} ended before the file was complete"
                )
            if queue.qsize() == 0:
                queue.put_nowait(data)
                if data is not None:
                    self._queued_bytes += len(data)
                return
            cond = self._cond
            if cond is None:
                raise TechnicalError(f"The upload of {self._path} is not running")
            async with cond:
                await cond.wait()

    async def _run_upload(self) -> None:
        queue, cond = self._queue, self._cond
        if queue is None or cond is None:
            raise TechnicalError(
                f"The upload of {self._path} was started without a queue"
            )

        sent = 0
        started = time.monotonic()

        async def stream() -> AsyncIterator[bytes]:
            nonlocal sent
            while True:
                chunk = await queue.get()
                if chunk is None:
                    return
                sent += len(chunk)
                self._queued_bytes -= len(chunk)
                async with cond:
                    cond.notify_all()
                yield chunk

        logger.info("Streaming upload of %s started", self._path)
        try:
            await self._ops.upload_from_stream(stream(), -1, self._path)
        except Exception as ex:
            self._error = ex
            logger.error(
                "Streaming upload of %s failed after %d bytes in %.1fs: %s",
                self._path,
                sent,
                time.monotonic() - started,
                ex,
            )
        else:
            elapsed = time.monotonic() - started
            rate = sent / MIB / elapsed if elapsed > 0 else 0.0
            logger.info(
                "Streaming upload of %s finished: %d bytes in %.1fs " "(%.1f MiB/s)",
                self._path,
                sent,
                elapsed,
                rate,
            )
        finally:
            async with cond:
                cond.notify_all()

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise self._error
