"""Tee mirroring: feed re-uploading mirrors from the upload stream itself.

A mirror that cannot take a server-side copy (a Discord store on either
side, another backend, ``mode: reupload``) has to receive every byte of
a version through this process. Today those bytes are read back from
the primary store after the upload, or from the local cache when one
is configured. With ``sync: tee`` the client's upload stream is split
instead: the primary store consumes it as it always did, and every
byte it takes is handed to one bounded buffer per such mirror, whose
upload runs at the same time. Nothing is read back and nothing touches
the disk; the price is that the client's upload runs at the pace of the
slowest store, since a full buffer makes the primary's next read wait.

The pieces:

* :class:`TeeBranch` is one buffer: the producer ``feed``\s chunks and
  waits while the branch holds ``capacity`` bytes; the consumer takes
  them off through ``stream``. ``finish`` marks the end of the bytes,
  ``detach`` drops the branch when its mirror failed so the producer is
  never held up by a consumer that is gone.
* :class:`TeeFileMessage` wraps the message the primary uploader reads
  and feeds every branch as a side effect of ``read``. The end of the
  stream is signalled by the byte count, not by ``close``: the Telegram
  uploader closes the message after every part.
* :class:`TeeUpload` bundles the wrapped message with the mirror upload
  tasks and collects their replicas once the primary is done.

Encryption sits above the repository, so the branches carry exactly
what the primary receives: ciphertext when encryption is on.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import AsyncGenerator, Awaitable, Callable, Deque, Dict, List, Optional

from tgdcfs.errors import TechnicalError
from tgdcfs.reqres import Replica, UploadableFileMessage

logger = logging.getLogger(__name__)


class TeeBranch:
    """A bounded byte queue between the primary's reads and one mirror."""

    def __init__(self, key: str, capacity: int):
        self.key = key
        self._capacity = max(1, capacity)
        self._chunks: Deque[bytes] = deque()
        self._buffered = 0
        self._eof = False
        self._detached = False
        self._changed = asyncio.Condition()

    @property
    def buffered(self) -> int:
        return self._buffered

    @property
    def detached(self) -> bool:
        return self._detached

    async def feed(self, data: bytes) -> None:
        """Queue ``data``; waits while the branch is full.

        A chunk larger than the whole capacity is admitted into an empty
        branch, so a producer with big reads never deadlocks.
        """
        if not data or self._detached:
            return
        async with self._changed:
            while (
                self._buffered
                and self._buffered + len(data) > self._capacity
                and not self._detached
            ):
                await self._changed.wait()
            if self._detached:
                return
            self._chunks.append(data)
            self._buffered += len(data)
            self._changed.notify_all()

    async def finish(self) -> None:
        """No more bytes will come; the consumer sees the end after the
        chunks still queued."""
        async with self._changed:
            self._eof = True
            self._changed.notify_all()

    async def detach(self) -> None:
        """Drop the branch: queued bytes are discarded, a waiting producer
        goes on, a waiting consumer gets an error."""
        async with self._changed:
            self._detached = True
            self._chunks.clear()
            self._buffered = 0
            self._changed.notify_all()

    async def stream(self) -> AsyncGenerator[bytes, None]:
        while True:
            async with self._changed:
                while not self._chunks and not self._eof and not self._detached:
                    await self._changed.wait()
                if self._detached:
                    raise TechnicalError(f"tee branch {self.key} was detached")
                if not self._chunks:
                    return
                data = self._chunks.popleft()
                self._buffered -= len(data)
                self._changed.notify_all()
            yield data


@dataclass
class TeeFileMessage(UploadableFileMessage):
    """The primary's upload message, feeding the branches as it is read."""

    inner: UploadableFileMessage = field(init=False)
    branches: List[TeeBranch] = field(init=False)
    _total: Optional[int] = field(init=False)
    _fed: int = field(init=False, default=0)

    @classmethod
    def wrap(
        cls, inner: UploadableFileMessage, branches: List[TeeBranch]
    ) -> "TeeFileMessage":
        # Preserve size=None so streaming uploads stay streaming through
        # the tee (get_size() returns 0 when unknown).
        obj = cls(
            name=inner.name,
            size=inner.size,
            caption=inner.caption,
            tags=inner.tags,
            _offset=0,
            _read_size=0,
            task_tracker=inner.task_tracker,
            version_id=inner.version_id,
        )
        obj.inner = inner
        obj.branches = list(branches)
        obj._total = inner.size  # None when streamed
        obj._fed = 0
        return obj

    @property
    def fed(self) -> int:
        return self._fed

    async def open(self) -> None:
        await self.inner.open()

    async def read(self, length: int) -> bytes:
        data = await self.inner.read(length)
        if data:
            self._fed += len(data)
            for branch in self.branches:
                await branch.feed(data)
            # Only auto-finish when total is known.
            if self._total is not None and self._fed >= self._total:
                await self.finish()
        return data

    async def finish(self) -> None:
        for branch in self.branches:
            await branch.finish()

    async def close(self) -> None:
        # Not the end of the stream: the Telegram uploader closes the
        # message after every part and reads on for the next one.
        # For size=None streaming, signal EOF to branches on final close
        # is handled by TeeUpload.collect calling finish().
        await self.inner.close()

    def file_name(self) -> str:
        return self.name or self.inner.file_name()

    def next_part(self, part_size: int) -> None:
        self.inner.next_part(part_size)
        self._offset += part_size


MirrorUpload = Callable[[TeeBranch], Awaitable[Replica]]


class TeeUpload:
    """One tee'd upload: the wrapped message and a task per mirror."""

    def __init__(
        self, message: TeeFileMessage, uploads: Dict[str, MirrorUpload]
    ) -> None:
        self.message = message
        self._branches = {branch.key: branch for branch in message.branches}
        self._tasks: Dict[str, asyncio.Task[Replica]] = {
            key: asyncio.create_task(
                self._run(key, upload), name=f"tee-{key}-{message.file_name()}"
            )
            for key, upload in uploads.items()
        }

    async def _run(self, key: str, upload: MirrorUpload) -> Replica:
        branch = self._branches[key]
        try:
            return await upload(branch)
        finally:
            # Whatever happened, the producer must never wait for this
            # branch again.
            await branch.detach()

    @property
    def store_keys(self) -> List[str]:
        return list(self._tasks)

    async def collect(self) -> Dict[str, Replica | Exception]:
        """Wait for every mirror; a failed one is returned as its error."""
        await self.message.finish()
        res: Dict[str, Replica | Exception] = {}
        for key, task in self._tasks.items():
            try:
                res[key] = await task
            except asyncio.CancelledError:
                res[key] = TechnicalError(f"tee upload into {key} was cancelled")
            except Exception as ex:
                res[key] = ex
        return res

    async def abort(self) -> None:
        """The primary upload failed: stop every mirror upload."""
        for branch in self._branches.values():
            await branch.detach()
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
