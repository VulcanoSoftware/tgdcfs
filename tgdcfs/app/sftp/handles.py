import asyncio
import errno
import logging
import os
import tempfile
import threading
from typing import AsyncIterator, Optional

from tgdcfs.core import Ops

from .pending import PendingUpload, PendingUploads

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

# How much of a file has to have arrived before it is worth uploading while it
# is still being received. Below it the file is uploaded on close like any
# other, with the whole size known.
STREAM_START_BYTES = 8 * 1024 * 1024

# How far the client may run ahead of the upload before its writes wait, and
# the granularity at which sent bytes are handed back to the filesystem.
STREAM_WINDOW_BYTES = 128 * 1024 * 1024
_PUNCH_ALIGN = 1024 * 1024


def _punch_hole(fd: int, offset: int, length: int) -> bool:
    """Give ``length`` bytes at ``offset`` of an open file back to the disk.

    The file keeps its size and the range reads as zeros. Returns False when
    the platform or filesystem cannot do it; the caller then stops trying.
    """
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        libc.fallocate.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_longlong,
            ctypes.c_longlong,
        ]
        keep_size, punch = 0x01, 0x02
        return libc.fallocate(fd, keep_size | punch, offset, length) == 0
    except (AttributeError, OSError):
        return False


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


class SpoolReadHandle(Handle):
    """Reads a file whose upload has been acknowledged but not finished yet.

    With ``sftp.write_behind`` the stores do not have the file for a while
    after the client closed it, but the client may well read it straight
    back (``rsync`` and ``rclone`` verify what they wrote). The spool is
    complete and seekable, so it serves that directly.
    """

    def __init__(self, pending: PendingUpload):
        self._pending: Optional[PendingUpload] = pending
        self._size = pending.size
        pending.retain()

    @property
    def size(self) -> int:
        return self._size

    async def read(self, offset: int, size: int) -> bytes:
        """Return up to ``size`` bytes at ``offset``; empty bytes mean EOF."""
        if size <= 0 or offset >= self._size or self._pending is None:
            return b""
        size = min(size, self._size - offset)
        return await asyncio.to_thread(self._pending.read_at, offset, size)

    async def close(self) -> None:
        pending, self._pending = self._pending, None
        if pending is not None:
            pending.release()


class WriteHandle(Handle):
    """Receives an SFTP upload and gets it into the stores.

    The normal Telegram upload needs the size of a file before its first
    byte and SFTP never announces it, so the payload is spooled -- in memory up to
    ``spool_max_bytes``, on disk beyond that. Buffering also makes
    out-of-order and sparse writes work, since the spool can be seeked.

    What happens to the spool depends on the settings:

    * By default it is uploaded when the client closes the handle, and the
      client waits for the answer to its CLOSE, which takes as long again
      as the transfer itself.
    * With a ``pending`` registry (``sftp.write_behind``) the finished spool
      is handed over to it and the CLOSE is answered right away.
    * With ``stream_segment_bytes`` (always set by the server) the upload
      to the stores starts as soon as a few MB have arrived and keeps pace
      with the client, so at CLOSE only the end of the file is left. A
      store that can take a stream of unknown length (Telegram) gets it as
      it is; one that cannot gets segments of ``stream_segment_bytes``.
      The price is that data already sent on can no longer be rewritten:
      a write into it fails. Files that stay below the start threshold are
      not affected and go the default way.
    """

    def __init__(
        self,
        ops: Ops,
        path: str,
        spool_max_bytes: int,
        spool_dir: Optional[str] = None,
        pending: Optional[PendingUploads] = None,
        client_name: str = "",
        stream_segment_bytes: int = 0,
    ):
        self._ops = ops
        self._path = path
        self._pending = pending
        self._client_name = client_name
        self._spool_dir = spool_dir
        self._spool = tempfile.SpooledTemporaryFile(
            max_size=spool_max_bytes, dir=spool_dir
        )
        self._size = 0
        self._dirty = False
        self._closed = False
        self._lock = asyncio.Lock()

        # Streaming state. ``_hwm`` is the length of the prefix that has
        # been written without holes, ``_islands`` the ranges written
        # beyond it, ``_yielded`` how much of the prefix went to the stores
        # (and so must not change any more).
        self._segment_bytes = stream_segment_bytes
        # Streaming has a cost (every part goes out as it comes, and bytes
        # sent on cannot change), so a small file is left to the plain path.
        self._start_bytes = min(stream_segment_bytes, STREAM_START_BYTES)
        self._io = threading.Lock()
        self._claim = asyncio.Lock()
        self._cond = asyncio.Condition()
        self._hwm = 0
        self._islands: list[tuple[int, int]] = []
        self._yielded = 0
        self._punched = 0
        self._can_punch = True
        self._finished = False
        self._aborted = False
        self._stream: Optional["asyncio.Task[object]"] = None

    @property
    def path(self) -> str:
        return self._path

    @property
    def size(self) -> int:
        return self._size

    async def _wait_for_room(self, offset: int) -> None:
        """Hold a write back while the upload is far behind the client.

        Without this a client that is faster than the stores fills the
        scratch disk. Judged on the write's offset, not its end, so the
        write that closes a gap is never held up by the data behind it.
        """
        if self._stream is None:
            return
        async with self._cond:
            await self._cond.wait_for(
                lambda: offset - self._yielded <= STREAM_WINDOW_BYTES
                or self._stream is None
                or self._stream.done()
                or self._aborted
            )

    async def write(self, offset: int, data: bytes) -> None:
        await self._wait_for_room(offset)
        async with self._lock:
            if self._closed:
                raise ValueError("write on a closed handle")
            self._raise_if_stream_failed()
            async with self._claim:
                if self._stream is not None and offset < self._yielded:
                    raise ValueError(
                        f"cannot rewrite the first {self._yielded} bytes of "
                        f"{self._path}: they are already on their way to the "
                        "stores"
                    )
                await asyncio.to_thread(self._write_sync, offset, data)
            if self._segment_bytes:
                await self._note_written(offset, offset + len(data))

    async def close(self) -> None:
        """Commit the received payload as a new version of the file."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            handed_over = False
            try:
                if self._stream is not None:
                    await self._finish_stream()
                elif self._dirty and self._pending is not None:
                    await self._pending.submit(
                        self._ops,
                        self._client_name,
                        self._path,
                        self._spool,
                        self._size,
                    )
                    handed_over = True
                elif self._dirty:
                    await self._ops.upload_from_stream(
                        self._iter_spool(), self._size, self._path
                    )
            finally:
                if not handed_over:
                    await asyncio.to_thread(self._spool.close)

    async def abort(self) -> None:
        """Drop the received payload without touching the stored file."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._stream is not None:
                async with self._cond:
                    self._aborted = True
                    self._cond.notify_all()
                # The segments already sent are deleted again on the way out.
                await asyncio.gather(self._stream, return_exceptions=True)
            await asyncio.to_thread(self._spool.close)

    def _write_sync(self, offset: int, data: bytes) -> None:
        with self._io:
            self._spool.seek(offset)
            self._spool.write(data)
        self._size = max(self._size, offset + len(data))
        self._dirty = True

    def _read_sync(self, offset: int, size: int) -> bytes:
        with self._io:
            self._spool.seek(offset)
            return self._spool.read(size)

    # -- streaming ----------------------------------------------------------

    async def _note_written(self, start: int, end: int) -> None:
        async with self._cond:
            if end > self._hwm:
                if start <= self._hwm:
                    self._hwm = end
                else:
                    self._islands.append((start, end))
                    self._islands.sort()
                while self._islands and self._islands[0][0] <= self._hwm:
                    self._hwm = max(self._hwm, self._islands.pop(0)[1])
            if self._stream is None and self._hwm >= self._start_bytes:
                # Sent bytes are released on disk, which needs a real file.
                await asyncio.to_thread(self._spool.rollover)
                self._stream = asyncio.create_task(
                    self._ops.upload_from_unsized_stream(
                        self._growing(),
                        self._path,
                        self._segment_bytes,
                        self._spool_dir,
                    )
                )
            self._cond.notify_all()

    def _raise_if_stream_failed(self) -> None:
        stream = self._stream
        if stream is not None and stream.done() and not stream.cancelled():
            if (error := stream.exception()) is not None:
                raise error

    async def _finish_stream(self) -> None:
        assert self._stream is not None  # noqa: S101
        async with self._cond:
            # Whatever was never written is a hole, and a hole reads as zeros.
            self._hwm = self._size
            self._finished = True
            self._cond.notify_all()
        await self._stream

    async def _growing(self) -> AsyncIterator[bytes]:
        """The spool as a stream that grows while the client keeps writing."""
        offset = 0
        while True:
            async with self._cond:
                await self._cond.wait_for(
                    lambda: self._hwm > offset or self._finished or self._aborted
                )
                if self._aborted:
                    raise RuntimeError(f"upload of {self._path} was aborted")
                limit = self._hwm
            if limit <= offset:
                return
            while offset < limit:
                size = min(SPOOL_READ_CHUNK, limit - offset)
                async with self._claim:
                    # From here on the bytes are spoken for.
                    self._yielded = offset + size
                chunk = await asyncio.to_thread(self._read_sync, offset, size)
                if not chunk:
                    raise RuntimeError(f"spool of {self._path} ended early")
                offset += len(chunk)
                await self._release_sent(offset)
                yield chunk

    async def _release_sent(self, sent: int) -> None:
        """Free the spool behind what has gone to the stores, and wake writers."""
        async with self._cond:
            self._cond.notify_all()
        upto = sent // _PUNCH_ALIGN * _PUNCH_ALIGN
        if not self._can_punch or upto <= self._punched:
            return
        start = self._punched
        self._can_punch = await asyncio.to_thread(self._punch_sync, start, upto - start)
        if self._can_punch:
            self._punched = upto

    def _punch_sync(self, start: int, length: int) -> bool:
        with self._io:
            return _punch_hole(self._spool.fileno(), start, length)

    async def _iter_spool(self) -> AsyncIterator[bytes]:
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
