import asyncio
import errno
import logging
import os
from typing import Any, AsyncIterator, Optional

from tgdcfs.core import Ops

logger = logging.getLogger(__name__)

SEEK_DATA = getattr(os, "SEEK_DATA", 3)
SEEK_HOLE = getattr(os, "SEEK_HOLE", 4)

# A forward jump shorter than this is bridged by reading through the open
# stream. Re-opening costs a Telegram round trip, reading through costs
# bandwidth we have already paid for -- one mebibyte is where the two meet
# for the clients that leave small gaps (they typically skip a few kibibytes).
SEEK_FORWARD_THRESHOLD = 1024 * 1024


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
    """Stream SFTP writes through a bounded in-memory queue.

    The final size is not announced by SFTP. Contiguous data is forwarded
    immediately; a small bounded reorder buffer accommodates pipelined
    writes. Sparse gaps are zero-filled when the handle closes. Once bytes
    have reached the backend, overwriting them is not possible without
    retaining the complete file, so overlapping/late writes are rejected.
    """

    QUEUE_CHUNK_SIZE = 512 * 1024
    QUEUE_DEPTH = 8
    MAX_PENDING_BYTES = 8 * 1024 * 1024
    MAX_PENDING_WRITES = 1024
    _EOF = object()

    def __init__(
        self,
        ops: Ops,
        path: str,
        spool_max_bytes: Optional[int] = None,
        spool_dir: Optional[str] = None,
    ):
        # Retain the old optional arguments for compatibility; no temporary
        # file is created or used.
        self._ops = ops
        self._path = path
        self._size = 0
        self._stream_offset = 0
        self._pending: dict[int, bytes] = {}
        self._pending_bytes = 0
        self._closed = False
        self._lock = asyncio.Lock()
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=self.QUEUE_DEPTH)
        self._upload_task: Optional[asyncio.Task[Any]] = None

    @property
    def path(self) -> str:
        return self._path

    @property
    def size(self) -> int:
        return self._size

    async def _stream(self) -> AsyncIterator[bytes]:
        while True:
            item = await self._queue.get()
            if item is self._EOF:
                return
            if not isinstance(item, bytes):
                raise RuntimeError("invalid item in upload stream")
            yield item

    def _start_upload(self) -> None:
        if self._upload_task is None:
            self._upload_task = asyncio.create_task(
                self._ops.upload_from_stream(self._stream(), -1, self._path)
            )

    async def _put(self, item: object) -> None:
        task = self._upload_task
        if task is None:
            raise RuntimeError("upload stream has not been started")
        if task.done():
            task.result()
            raise RuntimeError("upload stream stopped before EOF")
        put_task = asyncio.create_task(self._queue.put(item))
        done, _ = await asyncio.wait(
            (put_task, task), return_when=asyncio.FIRST_COMPLETED
        )
        if put_task in done:
            await put_task
            if task.done():
                # The consumer may finish in the same event-loop turn in
                # which it receives EOF.
                task.result()
                if item is not self._EOF:
                    raise RuntimeError("upload stream stopped before EOF")
            return
        put_task.cancel()
        await asyncio.gather(put_task, return_exceptions=True)
        task.result()
        raise RuntimeError("upload stream stopped before EOF")

    async def _emit(self, data: bytes) -> None:
        self._start_upload()
        for start in range(0, len(data), self.QUEUE_CHUNK_SIZE):
            chunk = data[start : start + self.QUEUE_CHUNK_SIZE]
            await self._put(chunk)
            self._stream_offset += len(chunk)

    async def _flush_contiguous(self) -> None:
        while (data := self._pending.pop(self._stream_offset, None)) is not None:
            self._pending_bytes -= len(data)
            await self._emit(data)

    async def write(self, offset: int, data: bytes) -> None:
        async with self._lock:
            if self._closed:
                raise ValueError("write on a closed handle")
            if not data:
                return
            end = offset + len(data)
            if offset < self._stream_offset:
                raise OSError(errno.EINVAL, "write overlaps data already streamed")
            for pending_offset, pending_data in self._pending.items():
                pending_end = pending_offset + len(pending_data)
                if offset < pending_end and end > pending_offset:
                    raise OSError(errno.EINVAL, "overlapping SFTP writes are unsupported")
            if (
                self._pending_bytes + len(data) > self.MAX_PENDING_BYTES
                or len(self._pending) >= self.MAX_PENDING_WRITES
            ):
                raise OSError(errno.ENOBUFS, "SFTP reorder buffer is full")
            self._pending[offset] = bytes(data)
            self._pending_bytes += len(data)
            self._size = max(self._size, end)
            await self._flush_contiguous()

    async def close(self) -> None:
        """Flush pending ranges, fill sparse gaps, then commit the stream."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            if not self._pending and self._upload_task is None:
                return
            try:
                for offset in sorted(self._pending):
                    if offset > self._stream_offset:
                        gap = offset - self._stream_offset
                        zeroes = bytes(min(self.QUEUE_CHUNK_SIZE, gap))
                        while gap:
                            chunk = zeroes[: min(len(zeroes), gap)]
                            await self._emit(chunk)
                            gap -= len(chunk)
                    data = self._pending.pop(offset)
                    self._pending_bytes -= len(data)
                    await self._emit(data)
                await self._put(self._EOF)
                task = self._upload_task
                if task is not None:
                    await task
            except BaseException:
                task = self._upload_task
                if task is not None and not task.done():
                    task.cancel()
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)
                raise

    async def abort(self) -> None:
        """Cancel an incomplete stream without committing a partial file."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            self._pending.clear()
            self._pending_bytes = 0
            task, self._upload_task = self._upload_task, None
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
