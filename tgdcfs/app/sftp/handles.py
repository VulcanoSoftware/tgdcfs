import asyncio
import errno
import logging
import os
import tempfile
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

# How much of the spooled upload we hand to the uploader at a time.
SPOOL_READ_CHUNK = 1024 * 1024

# Sentinel placed on the stream queue when the SFTP client closes the handle.
_STREAM_EOF = object()


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
    """Accepts an SFTP upload and commits it as a new file version.

    Prefer a live stream for sequential writes (the common case for
    OpenSSH ``sftp``, rclone, WinSCP, etc.). Bytes are handed to the
    uploader via an asyncio.Queue as soon as they arrive, so Telegram
    parts can be sent without waiting for the whole file. The total size
    is only known when the client closes the handle; the Telegram backend
    then uses the official streamed-upload path (file_total_parts=-1).

    Non-sequential or sparse writes fall back to the original spool
    (memory + disk) so correctness is preserved for clients that seek.
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

        # Streaming path (sequential writes).
        self._queue: Optional[asyncio.Queue[object]] = None
        self._upload_task: Optional[asyncio.Task[None]] = None
        self._next_offset = 0
        self._stream_failed = False

        # Fallback spool path (random / sparse writes).
        self._spool: Optional[tempfile.SpooledTemporaryFile[Any]] = None
        self._use_spool = False

        self._size = 0
        self._dirty = False
        self._closed = False
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
            if not data:
                return

            if self._use_spool:
                await asyncio.to_thread(self._spool_write, offset, data)
                return

            # First write or continued sequential write?
            if offset != self._next_offset:
                # Switch to spool mode for the rest of this handle.
                logger.debug(
                    "SFTP write at offset %s while expecting %s for %s; "
                    "falling back to spool", offset, self._next_offset, self._path
                )
                await self._switch_to_spool()
                await asyncio.to_thread(self._spool_write, offset, data)
                return

            # Ensure the streaming pipeline is running.
            if self._queue is None:
                self._queue = asyncio.Queue(maxsize=8)
                self._upload_task = asyncio.create_task(self._run_stream_upload())

            await self._queue.put(data)
            self._next_offset += len(data)
            self._size = self._next_offset
            self._dirty = True

    async def close(self) -> None:
        """Commit the payload as a new version of the file."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                if not self._dirty:
                    return

                if self._use_spool and self._spool is not None:
                    await self._ops.upload_from_stream(
                        self._iter_spool(), self._size, self._path
                    )
                    return

                # Streaming path: signal EOF and wait for the uploader.
                if self._queue is not None:
                    await self._queue.put(_STREAM_EOF)
                    if self._upload_task is not None:
                        await self._upload_task
                        if self._stream_failed:
                            raise TechnicalErrorFromStream()
            finally:
                if self._spool is not None:
                    await asyncio.to_thread(self._spool.close)

    async def abort(self) -> None:
        """Drop the payload without touching the stored file."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._queue is not None:
                # Drain / cancel the uploader.
                try:
                    self._queue.put_nowait(_STREAM_EOF)
                except asyncio.QueueFull:
                    pass
                if self._upload_task is not None:
                    self._upload_task.cancel()
                    try:
                        await self._upload_task
                    except (asyncio.CancelledError, Exception) as ex:
                        logger.debug("upload task cleanup after abort: %s", ex)
            if self._spool is not None:
                await asyncio.to_thread(self._spool.close)

    async def _switch_to_spool(self) -> None:
        """Move already-streamed bytes into a spool and continue there."""
        self._use_spool = True
        self._spool = tempfile.SpooledTemporaryFile(
            max_size=self._spool_max_bytes, dir=self._spool_dir
        )
        # Any bytes already handed to the queue are lost for the spool;
        # in practice the first non-sequential write happens early (or
        # never). We accept that the partial stream is abandoned.
        if self._queue is not None:
            try:
                self._queue.put_nowait(_STREAM_EOF)
            except asyncio.QueueFull:
                pass
            if self._upload_task is not None:
                self._upload_task.cancel()
                try:
                    await self._upload_task
                except (asyncio.CancelledError, Exception) as ex:
                    logger.debug("upload task cleanup after switch to spool: %s", ex)
            self._queue = None
            self._upload_task = None

    def _spool_write(self, offset: int, data: bytes) -> None:
        if self._spool is None:
            raise RuntimeError("spool not initialized")
        # Explicitly zero-fill gaps so sparse SFTP writes match the
        # expected solid file (SpooledTemporaryFile seek alone is not
        # guaranteed to materialize zeros on every platform).
        if offset > self._size:
            self._spool.seek(self._size)
            self._spool.write(b"\x00" * (offset - self._size))
        self._spool.seek(offset)
        self._spool.write(data)
        self._size = max(self._size, offset + len(data))
        self._dirty = True

    async def _iter_spool(self) -> AsyncIterator[bytes]:
        if self._spool is None:
            raise RuntimeError("spool not initialized")
        await asyncio.to_thread(self._spool.seek, 0)
        remaining = self._size
        while remaining > 0:
            chunk = await asyncio.to_thread(
                self._spool.read, min(SPOOL_READ_CHUNK, remaining)
            )
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk

    async def _run_stream_upload(self) -> None:
        """Consume the queue and feed Ops.upload_from_stream (size unknown)."""

        async def stream() -> AsyncIterator[bytes]:
            if self._queue is None:
                raise RuntimeError("stream queue not initialized")
            while True:
                item = await self._queue.get()
                if item is _STREAM_EOF:
                    return
                if not isinstance(item, (bytes, bytearray)):
                    raise TypeError(f"expected bytes on stream queue, got {type(item)}")
                yield bytes(item)

        try:
            await self._ops.upload_from_stream(stream(), None, self._path)
        except Exception as ex:
            logger.exception("Streamed SFTP upload failed for %s: %s", self._path, ex)
            self._stream_failed = True
            raise


class TechnicalErrorFromStream(Exception):
    """Raised when the background stream upload failed."""
    pass
