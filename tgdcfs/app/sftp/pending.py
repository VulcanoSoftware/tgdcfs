"""Uploads that were acknowledged to the SFTP client but are still on their way
to the stores.

SFTP never announces a file's size and Telegram needs it before the first
byte, so an upload is spooled first and only sent on when the client closes
the handle. Left alone, that makes every upload take twice: once from the
client to the spool, then -- while the client sits at 100% waiting for the
answer to its CLOSE -- once more from the spool to Telegram. A client that
hears nothing for long enough (FileZilla gives up after 20 seconds) drops
the connection and starts over.

With ``sftp.write_behind`` the CLOSE is answered as soon as the spool is
complete, and the registry in this module finishes the job in the
background. Until the stores have the file, the registry is also what the
SFTP front-end asks first, so a ``stat``, a read or a listing straight after
the upload shows the file the client just wrote, not the empty placeholder.
"""

import asyncio
import logging
import tempfile
import threading
import time
from typing import AsyncIterator, Dict, Optional, Set, Tuple

from tgdcfs.core import Ops

logger = logging.getLogger(__name__)

# How much of the spool is handed to the uploader at a time.
SPOOL_READ_CHUNK = 1024 * 1024

# (client name, path inside that client's tree)
Key = Tuple[str, str]


def _within(path: str, other: str) -> bool:
    """True when ``path`` is ``other`` or lives somewhere below it."""
    return path == other or path.startswith(f"{other.rstrip('/')}/")


class PendingUpload:
    """One finished spool on its way to the stores.

    The spool is shared by the background upload and by any read handle
    opened on the file in the meantime, so it is reference counted: it is
    closed when the upload is over *and* the last reader let go.
    """

    def __init__(
        self,
        key: Key,
        spool: "tempfile.SpooledTemporaryFile[bytes]",
        size: int,
    ):
        self.key = key
        self.size = size
        self.modified_ms = int(time.time() * 1000)
        self.error: Optional[BaseException] = None
        self.done = asyncio.Event()

        self._spool = spool
        self._io_lock = threading.Lock()
        # The upload task owns the first reference.
        self._refs = 1
        self._spool_closed = False

    # -- reading ------------------------------------------------------------

    def read_at(self, offset: int, size: int) -> bytes:
        """Read from the spool at an absolute offset; blocking, run it in a thread."""
        with self._io_lock:
            if self._spool_closed:
                raise ValueError("read from a released upload")
            self._spool.seek(offset)
            return self._spool.read(size)

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        offset = 0
        while offset < self.size:
            chunk = await asyncio.to_thread(
                self.read_at, offset, min(SPOOL_READ_CHUNK, self.size - offset)
            )
            if not chunk:
                break
            offset += len(chunk)
            yield chunk

    # -- lifetime -----------------------------------------------------------

    def retain(self) -> None:
        self._refs += 1

    def release(self) -> None:
        self._refs -= 1
        if self._refs <= 0:
            self._close_spool()

    def _close_spool(self) -> None:
        with self._io_lock:
            if not self._spool_closed:
                self._spool_closed = True
                self._spool.close()


class PendingUploads:
    """Registry and runner of the background uploads of one SFTP listener.

    ``max_bytes`` caps the data held back at any one time (``0`` is no cap).
    A CLOSE that would exceed it is answered only once enough earlier
    uploads have finished, which is the backpressure that keeps a client
    uploading faster than Telegram can take from filling the scratch disk.
    A single file larger than the cap is always accepted when nothing else
    is pending, so the cap can delay a file but never block it for good.
    """

    def __init__(self, max_bytes: int = 0):
        self._max_bytes = max_bytes
        self._entries: Dict[Key, PendingUpload] = {}
        self._tasks: Set["asyncio.Task[None]"] = set()
        self._held = 0
        self._room = asyncio.Condition()

    # -- queries ------------------------------------------------------------

    @property
    def held_bytes(self) -> int:
        return self._held

    def get(self, client_name: str, path: str) -> Optional[PendingUpload]:
        return self._entries.get((client_name, path))

    async def wait(self, client_name: str, path: str) -> None:
        """Return once nothing is pending for exactly this path any more."""
        while (entry := self._entries.get((client_name, path))) is not None:
            await entry.done.wait()

    async def wait_under(self, client_name: str, path: str) -> None:
        """Return once nothing is pending for this path or anything below it."""
        while True:
            entries = [
                entry
                for (name, entry_path), entry in self._entries.items()
                if name == client_name and _within(entry_path, path)
            ]
            if not entries:
                return
            for entry in entries:
                await entry.done.wait()

    async def drain(self) -> None:
        """Wait for every background upload; used when the server shuts down."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # -- submitting ---------------------------------------------------------

    async def submit(
        self,
        ops: Ops,
        client_name: str,
        path: str,
        spool: "tempfile.SpooledTemporaryFile[bytes]",
        size: int,
    ) -> PendingUpload:
        """Take over a complete spool and upload it in the background.

        Returns as soon as the registry has accepted it, which may mean
        waiting for room. Ownership of ``spool`` passes to the registry only
        when this returns; if it raises, the caller still owns the spool.
        """
        await self._reserve(size)

        key = (client_name, path)
        # A newer upload of the same path goes after the one before it, so
        # the versions end up in the order the client wrote them.
        previous = self._entries.get(key)
        entry = PendingUpload(key, spool, size)
        self._entries[key] = entry

        task = asyncio.create_task(self._run(entry, ops, previous))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return entry

    async def _reserve(self, size: int) -> None:
        async with self._room:
            if self._max_bytes > 0:
                await self._room.wait_for(
                    lambda: self._held == 0 or self._held + size <= self._max_bytes
                )
            self._held += size

    async def _release(self, size: int) -> None:
        async with self._room:
            self._held -= size
            self._room.notify_all()

    async def _run(
        self, entry: PendingUpload, ops: Ops, previous: Optional[PendingUpload]
    ) -> None:
        _, path = entry.key
        try:
            if previous is not None:
                await previous.done.wait()
            await ops.upload_from_stream(entry.iter_chunks(), entry.size, path)
        except asyncio.CancelledError:
            entry.error = RuntimeError("upload cancelled")
            raise
        except Exception as ex:
            # The client has long been told "done". All that is left is to
            # say loudly that it was not; the upload task tracker shows it
            # too, and the placeholder created when the file was opened stays.
            entry.error = ex
            logger.error(
                "Background SFTP upload of %s/%s (%d bytes) failed: %s",
                entry.key[0],
                path,
                entry.size,
                ex,
            )
        finally:
            if self._entries.get(entry.key) is entry:
                del self._entries[entry.key]
            entry.release()
            entry.done.set()
            await asyncio.shield(self._release(entry.size))


__all__ = ["PendingUpload", "PendingUploads"]
