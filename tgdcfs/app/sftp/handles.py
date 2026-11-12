import asyncio
import errno
import logging
import os
import tempfile
from typing import AsyncIterator, Dict, Optional

from tgdcfs.core import Ops
from tgdcfs.errors import TechnicalError

logger = logging.getLogger(__name__)

SEEK_DATA = getattr(os, "SEEK_DATA", 3)
SEEK_HOLE = getattr(os, "SEEK_HOLE", 4)

# A forward jump shorter than this is bridged by reading through the open
# stream. Re-opening costs a Telegram round trip, reading through costs
# bandwidth we have already paid for -- one mebibyte is where the two meet
# for the clients that leave small gaps (they typically skip a few kibibytes).
SEEK_FORWARD_THRESHOLD = 1024 * 1024

# How much of the spooled upload we hand to the uploader at a time.
SPOOL_READ_CHUNK = 1024 * 1024

# Writes that arrive ahead of the stream position are held until the gap
# fills. Pipelined clients skip a few requests' worth of bytes at most; a
# gap wider than this is treated as hostile and fails the upload.
AHEAD_WINDOW_BYTES = 8 * 1024 * 1024


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
    core.telegram.org/api/files, "Streamed uploads"), so sequential writes
    are forwarded to the upload as they arrive. Nothing is buffered beyond
    a small window: the queue between the writes and the uploader applies
    backpressure, and writes that arrive slightly ahead of the stream --
    pipelined clients routinely do -- are held until the gap fills.

    A write that lands behind the stream's position would need bytes that
    are already on the wire. Before anything has been streamed the handle
    falls back to the old spool -- in memory up to ``spool_max_bytes``, on
    disk beyond that -- and commits on close, as it always did; once bytes
    have left, the upload fails rather than store a corrupted file.
    """

    def __init__(
        self,
        ops: Ops,
        path: str,
        spool_max_bytes: int,
        spool_dir: Optional[str] = None,
    ):
        self._ops = ops
        self._path = path
        self._spool_max_bytes = spool_max_bytes
        self._spool_dir = spool_dir

        self._queue: Optional[asyncio.Queue[Optional[bytes]]] = None
        self._task: Optional[asyncio.Task] = None
        self._cond: Optional[asyncio.Condition] = None
        self._queued_bytes = 0

        # ``_pos`` is what the upload stream has been handed, ``_size`` the
        # highest byte the client wrote; the two differ by whatever is
        # queued or held in ``_pending``.
        self._pos = 0
        self._size = 0
        self._pending: Dict[int, bytes] = {}
        self._pending_bytes = 0

        self._spool: Optional[tempfile.SpooledTemporaryFile] = None
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
            if self._spool is not None:
                await asyncio.to_thread(self._write_sync, offset, data)
                return
            if offset < self._pos or self._overlaps_pending(offset, len(data)):
                self._fallback_or_fail(offset, data)
                return
            if offset > self._pos:
                self._stage_ahead(offset, data)
                return
            await self._stream(offset, data)

    async def close(self) -> None:
        """Finish the upload as a new version of the file."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._spool is not None:
                try:
                    if self._dirty:
                        await self._ops.upload_from_stream(
                            self._iter_spool(), self._size, self._path
                        )
                finally:
                    await asyncio.to_thread(self._spool.close)
                return
            if not self._dirty:
                if self._task is not None:
                    await asyncio.gather(self._task, return_exceptions=True)
                return
            await self._flush_pending()
            self._raise_if_failed()
            _, task = self._ensure_task()
            await self._enqueue(None)
            await task
            self._raise_if_failed()

    async def abort(self) -> None:
        """Drop the upload without touching the stored file."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._task is not None:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            if self._spool is not None:
                await asyncio.to_thread(self._spool.close)

    # -- streaming ---------------------------------------------------------

    async def _stream(self, offset: int, data: bytes) -> None:
        if data:
            await self._enqueue(data)
        self._dirty = True
        self._advance(offset + len(data))
        # Writes that arrived ahead of the stream may line up now.
        while self._pending and min(self._pending) == self._pos:
            chunk = self._pending.pop(min(self._pending))
            self._pending_bytes -= len(chunk)
            await self._enqueue(chunk)
            self._advance(self._pos + len(chunk))

    def _advance(self, end: int) -> None:
        self._size = max(self._size, end)
        self._pos = end

    def _stage_ahead(self, offset: int, data: bytes) -> None:
        # The window bounds the whole unstreamed span -- the gap plus the
        # held writes -- since the gap is zero-filled into the queue when
        # the missing writes never come.
        if offset + len(data) - self._pos > AHEAD_WINDOW_BYTES:
            raise TechnicalError(
                f"Write at offset {offset} of {self._path} is too far ahead "
                f"of the upload stream to be buffered"
            )
        self._pending[offset] = data
        self._pending_bytes += len(data)
        self._dirty = True
        self._size = max(self._size, offset + len(data))

    def _overlaps_pending(self, offset: int, length: int) -> bool:
        return any(
            start < offset + length and offset < start + len(data)
            for start, data in self._pending.items()
        )

    def _fallback_or_fail(self, offset: int, data: bytes) -> None:
        if self._pos == 0:
            # Nothing has been handed to the uploader: the whole payload
            # still sits in ``_pending``, so the spool can take over.
            self._spool = tempfile.SpooledTemporaryFile(
                max_size=self._spool_max_bytes, dir=self._spool_dir
            )
            for start in sorted(self._pending):
                self._write_sync(start, self._pending[start])
            self._pending.clear()
            self._pending_bytes = 0
            self._write_sync(offset, data)
            return
        raise TechnicalError(
            f"Write at offset {offset} of {self._path} lands behind bytes "
            f"that were already streamed"
        )

    async def _flush_pending(self) -> None:
        """Hand every held write to the uploader, zero-filling the gaps."""
        while self._pending:
            start = min(self._pending)
            if start > self._pos:
                await self._enqueue(b"\x00" * (start - self._pos))
            chunk = self._pending.pop(start)
            self._pending_bytes -= len(chunk)
            await self._enqueue(chunk)
            self._advance(start + len(chunk))

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

        async def stream() -> AsyncIterator[bytes]:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    return
                self._queued_bytes -= len(chunk)
                async with cond:
                    cond.notify_all()
                yield chunk

        try:
            await self._ops.upload_from_stream(stream(), -1, self._path)
        except Exception as ex:
            self._error = ex
            logger.error("Streaming upload of %s failed: %s", self._path, ex)
        finally:
            async with cond:
                cond.notify_all()

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise self._error

    # -- spool fallback ------------------------------------------------------

    def _write_sync(self, offset: int, data: bytes) -> None:
        spool = self._spool
        if spool is None:
            raise TechnicalError(f"The upload of {self._path} has no spool to write to")
        spool.seek(offset)
        spool.write(data)
        self._size = max(self._size, offset + len(data))
        self._dirty = True

    async def _iter_spool(self) -> AsyncIterator[bytes]:
        spool = self._spool
        if spool is None:
            raise TechnicalError(
                f"The upload of {self._path} has no spool to read from"
            )
        await asyncio.to_thread(spool.seek, 0)
        remaining = self._size
        while remaining > 0:
            chunk = await asyncio.to_thread(
                spool.read, min(SPOOL_READ_CHUNK, remaining)
            )
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk
