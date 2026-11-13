import asyncio
import errno
import logging
import os
from typing import AsyncIterator, Optional

from tgdcfs.core import Ops

logger = logging.getLogger(__name__)

SEEK_DATA = getattr(os, "SEEK_DATA", 3)
SEEK_HOLE = getattr(os, "SEEK_HOLE", 4)

# A forward jump shorter than this is bridged by reading through the open
# stream. Re-opening costs a Telegram round trip, reading through costs
# bandwidth we have already paid for -- one mebibyte is where the two meet
# for the clients that leave small gaps (they typically skip a few kibibytes).
SEEK_FORWARD_THRESHOLD = 1024 * 1024

# How much of a streamed upload we hand to the uploader at a time.
STREAM_CHUNK = 1024 * 1024


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
    """Streams an SFTP upload to the stores while the bytes arrive.

    SFTP never announces a file's length, but the stores take a stream of
    unknown length -- Telegram through its streamed-upload rules
    (``upload.saveBigFilePart`` with ``file_total_parts`` -1 for every
    part but the last), Discord as a plain stream of parts -- so the
    payload is piped straight into :meth:`Ops.upload_from_stream` instead
    of being spooled whole. The upload starts with the first write and
    ends when the client closes the handle.

    Writes land in a bounded buffer that the upload drains; a write
    returns only once the buffer has room for it, which is the
    backpressure that holds back a client writing faster than the stores
    take. Writes are expected front to back: a chunk arriving ahead of the
    stream is kept until the gap before it fills, and a write that would
    rewrite bytes the upload already consumed, or overflow the buffer,
    fails the transfer -- a stream cannot be rewound.
    """

    def __init__(self, ops: Ops, path: str, buffer_max_bytes: int = 0) -> None:
        self._ops = ops
        self._path = path
        # ``<= 0`` disables the cap: the upload still streams, but a
        # client that outruns the stores is then no longer held back.
        self._buffer_max = buffer_max_bytes
        # Contiguous bytes from ``self._streamed`` onward, plus chunks
        # that arrived ahead of the stream, waiting for their gap.
        self._buffer = bytearray()
        self._pending: dict[int, bytes] = {}
        self._streamed = 0
        self._cond = asyncio.Condition()
        self._upload_task: Optional[asyncio.Task[None]] = None
        self._failed: Optional[BaseException] = None
        self._closed = False
        self._eof = False

    @property
    def path(self) -> str:
        return self._path

    @property
    def size(self) -> int:
        """Bytes accepted so far; what fstat answers mid-transfer."""
        return (
            self._streamed
            + len(self._buffer)
            + sum(len(chunk) for chunk in self._pending.values())
        )

    async def write(self, offset: int, data: bytes) -> None:
        async with self._cond:
            if self._closed:
                raise ValueError("write on a closed handle")
            if self._failed is not None:
                raise ValueError(
                    f"the upload of {self._path} failed: {self._failed}"
                ) from self._failed

            if offset < self._streamed:
                # Bytes the upload has already consumed; a stream cannot
                # be rewritten.
                raise ValueError(
                    f"cannot write at offset {offset}: {self._streamed} bytes "
                    "of the upload have already been streamed"
                )

            buffer_end = self._streamed + len(self._buffer)
            if offset > buffer_end:
                # Ahead of the stream: keep it until the gap before it
                # fills, if it fits the buffer budget.
                self._pending[offset] = data
            else:
                # Continues the stream or patches bytes still buffered:
                # both are a plain splice at the write's offset.
                at = offset - self._streamed
                self._buffer[at : at + len(data)] = data
            self._coalesce()

            if self._upload_task is None and self._streamed == 0 and self._buffer:
                # The stream can begin: bytes from offset 0 are in.
                self._upload_task = asyncio.create_task(self._run_upload())
            # Wake the upload, which may be waiting for data or room.
            self._cond.notify_all()

            while self._over_capacity() and self._upload_task is not None:
                if self._failed is not None:
                    # The upload died; nothing will ever drain the buffer.
                    raise ValueError(
                        f"the upload of {self._path} failed: {self._failed}"
                    )
                if self._closed:
                    raise ValueError("write on a closed handle")
                # The upload drains the buffer; wait for room instead of
                # acknowledging writes the stores cannot keep up with.
                await self._cond.wait()

    async def close(self) -> None:
        """Finish the upload: end the stream and wait for the stores."""
        async with self._cond:
            if self._closed:
                return
            self._closed = True
            self._eof = True
            self._cond.notify_all()
        task = self._upload_task
        if task is not None:
            await task
            if self._failed is not None:
                raise self._failed
        if self._pending:
            gap_at = min(self._pending)
            raise ValueError(
                f"the upload of {self._path} never filled the gap "
                f"before offset {gap_at}"
            )

    async def abort(self) -> None:
        """Drop the transfer without committing anything."""
        async with self._cond:
            if self._closed:
                return
            self._closed = True
            task, self._upload_task = self._upload_task, None
            self._buffer = bytearray()
            self._pending = {}
            self._cond.notify_all()
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def _coalesce(self) -> None:
        """Pull ahead-of-stream chunks into the buffer as gaps fill."""
        while self._pending:
            next_at = self._streamed + len(self._buffer)
            start = min(self._pending)
            if start > next_at:
                break
            chunk = self._pending.pop(start)
            at = start - self._streamed
            self._buffer[at : at + len(chunk)] = chunk

    def _in_flight(self) -> int:
        return len(self._buffer) + sum(len(c) for c in self._pending.values())

    def _over_capacity(self) -> bool:
        return self._buffer_max > 0 and self._in_flight() > self._buffer_max

    async def _run_upload(self) -> None:
        try:
            await self._ops.upload_from_stream(self._iter_stream(), -1, self._path)
        except asyncio.CancelledError:
            raise
        except BaseException as ex:
            # Surfaced by the next write and by close(): the client should
            # see the transfer fail, not a silent half-written file.
            self._failed = ex
            async with self._cond:
                self._cond.notify_all()

    async def _iter_stream(self) -> AsyncIterator[bytes]:
        while True:
            async with self._cond:
                while not self._buffer and not self._eof:
                    await self._cond.wait()
                chunk = bytes(self._buffer[:STREAM_CHUNK])
                del self._buffer[: len(chunk)]
                self._streamed += len(chunk)
                self._cond.notify_all()
            if not chunk:
                return
            yield chunk
