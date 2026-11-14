import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from telethon.helpers import generate_random_long

from tgdcfs.config import get_config
from tgdcfs.errors import TechnicalError
from tgdcfs.reqres import (
    SaveBigFilePartReq,
    SaveFilePartReq,
    SendFileReq,
    SendMessageResp,
    UploadableFileMessage,
    UploadedFile,
)
from tgdcfs.utils.others import flood_wait_seconds, is_big_file

from .interface import ITDLibClient

logger = logging.getLogger(__name__)

# A part that keeps failing has to give up eventually: the old unbounded,
# sleepless retry turned one broken part into a busy loop against Telegram,
# which is how a slow upload becomes a rate-limited one.
MAX_PART_ATTEMPTS = 5
RETRY_BASE_DELAY = 0.5
RETRY_MAX_DELAY = 30.0


@dataclass
class WorkersConfig:
    small: int = 3
    big: int = 8

    @classmethod
    def from_config(cls) -> "WorkersConfig":
        transfer = get_config().tgdcfs.transfer
        return cls(small=transfer.upload_workers_small, big=transfer.upload_workers_big)


@dataclass
class FileChunk:
    content: bytes
    file_part: int


class FileUploader:
    def __init__(
        self,
        client: ITDLibClient,
        file_msg: UploadableFileMessage,
        workers: Optional[WorkersConfig] = None,
    ):
        self.client = client
        self._file_msg = file_msg
        self._file_size = self._file_msg.get_size()
        self._file_name = self._file_msg.file_name()

        # One part size for every file. The library's heuristic drops to
        # 128 KiB below 100 MB, which quadruples the number of requests for
        # no gain -- 512 KiB is accepted for any size.
        self._chunk_size = get_config().tgdcfs.transfer.upload_part_size_bytes

        # A negative size means a live stream of unknown length; the total
        # only becomes known when the source runs dry. Those uploads follow
        # the streamed upload protocol (see ``_upload_streamed``).
        self._streamed = self._file_size < 0
        if self._streamed:
            self._total_parts = 0
        else:
            self._total_parts = (
                self._file_size + self._chunk_size - 1
            ) // self._chunk_size

        self._part_indexes: asyncio.Queue[int] = asyncio.Queue(
            maxsize=max(self._total_parts, 1)
        )

        self._workers = workers or WorkersConfig.from_config()

        self._file_id = generate_random_long()

        # Streamed uploads always go through saveBigFilePart, even when the
        # stream turns out to be smaller than 10 MB (core.telegram.org/api/files).
        self._is_big = self._streamed or is_big_file(self._file_size)
        self._read_size = 0
        self._uploaded_size = 0

        self._num_workers = self._workers.big if self._is_big else self._workers.small
        self._lock = asyncio.Lock()

    async def _close(self) -> None:
        await self._file_msg.close()

    async def _read(self, length: int) -> bytes:
        return await self._file_msg.read(length)

    async def _upload_chunk(
        self, chunk: FileChunk, file_total_parts: Optional[int] = None
    ) -> None:
        for attempt in range(1, MAX_PART_ATTEMPTS + 1):
            try:
                if self._is_big:
                    rsp = await self.client.save_big_file_part(
                        SaveBigFilePartReq(
                            file_id=self._file_id,
                            bytes=chunk.content,
                            file_part=chunk.file_part,
                            file_total_parts=(
                                self._total_parts
                                if file_total_parts is None
                                else file_total_parts
                            ),
                        )
                    )
                else:
                    rsp = await self.client.save_file_part(
                        SaveFilePartReq(
                            file_id=self._file_id,
                            bytes=chunk.content,
                            file_part=chunk.file_part,
                        )
                    )

                if not rsp.success:
                    raise TechnicalError(f"Unexpected response: {rsp}")

                self._uploaded_size += len(chunk.content)
                return

            except Exception as e:
                if attempt == MAX_PART_ATTEMPTS:
                    logger.error(
                        f"Giving up on part {chunk.file_part} for {self._file_name} "
                        f"after {attempt} attempts: {e}"
                    )
                    raise

                if (wait := flood_wait_seconds(e)) is not None:
                    delay = float(wait)
                else:
                    delay = min(RETRY_BASE_DELAY * 2 ** (attempt - 1), RETRY_MAX_DELAY)

                logger.warning(
                    f"Error uploading part {chunk.file_part} for {self._file_name}: "
                    f"{e}, attempt={attempt}/{MAX_PART_ATTEMPTS}, "
                    f"retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)

    def _done_reading(self) -> bool:
        return self._read_size >= self._file_size

    async def _upload_next_part(self, part: int) -> int:
        async with self._lock:
            if self._done_reading():
                return 0
            size_to_read = min(self._file_size - self._read_size, self._chunk_size)
            content = await self._read(size_to_read)
            self._read_size += size_to_read

        await self._upload_chunk(FileChunk(content=content, file_part=part))
        return size_to_read

    async def _cancelled(self) -> bool:
        if tt := self._file_msg.task_tracker:
            return await tt.cancelled()
        return False

    async def upload(self) -> int:
        await self._file_msg.open()

        if self._streamed:
            size = await self._upload_streamed()
            await self._close()
            return size

        for i in range(self._total_parts):
            await self._part_indexes.put(i)

        async def create_worker(worker_id: int) -> bool:
            while True:
                if await self._cancelled():
                    logger.warning(
                        f"Task uploading for {self._file_name} was cancelled. Worker {worker_id} exiting."
                    )
                    return False

                try:
                    part_size = await self._upload_next_part(
                        self._part_indexes.get_nowait()
                    )
                except asyncio.QueueEmpty:
                    logger.debug(
                        f"[Worker {worker_id}] No more parts to upload for {self._file_name}. Worker exiting."
                    )
                    return True
                logger.debug(
                    f"[Worker {worker_id}] {self._uploaded_size * 100 / self._file_size}% uploaded. file_id={self._file_id} file_name={self._file_name}"
                )
                if (tt := self._file_msg.task_tracker) and part_size > 0:
                    await tt.update_progress(size_delta=part_size)

        workers = [
            asyncio.create_task(create_worker(worker_id))
            for worker_id in range(self._num_workers)
        ]
        try:
            await asyncio.gather(*workers)
        except Exception:
            # One dead part makes the whole file_id unusable, so the other
            # workers should stop feeding it instead of uploading into a
            # file that can never be sent.
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise

        await self._close()
        return self._file_size

    async def _upload_streamed(self) -> int:
        """Upload a stream whose length is unknown until it ends.

        This follows the streamed upload protocol from the official API
        documentation (core.telegram.org/api/files, "Streamed uploads"):
        buffer ``part_size`` bytes -- or less when the stream ends -- and
        upload each part immediately, always through
        ``upload.saveBigFilePart``. Every part but the last is saved with
        ``file_total_parts = -1``; the last one carries
        ``file_total_parts = ceil(total_stream_size / part_size)`` and is
        saved only after every other part is in, so the server can
        finalize the file.

        One part is always held back until the next read proves that more
        data follows, so the stream ending exactly on a part boundary
        makes the held part the last one instead of sending an empty
        part: FILE_PART_EMPTY is on Telegram's error list, and the docs
        do not promise that an empty part finalizes a file. Only a
        completely empty stream saves nothing, exactly like a known-size
        empty file.
        """
        queue: asyncio.Queue[Optional[FileChunk]] = asyncio.Queue(
            maxsize=self._num_workers
        )
        worker_failed = asyncio.Event()

        async def worker() -> None:
            try:
                while True:
                    if await self._cancelled():
                        logger.warning(
                            f"Task uploading for {self._file_name} was cancelled. "
                            f"Streamed worker exiting."
                        )
                        return
                    chunk = await queue.get()
                    if chunk is None:
                        return
                    await self._upload_chunk(chunk, file_total_parts=-1)
                    if tt := self._file_msg.task_tracker:
                        await tt.update_progress(size_delta=len(chunk.content))
            except Exception:
                worker_failed.set()
                raise

        workers = [
            asyncio.create_task(worker(), name=f"streamed-upload-{self._file_name}")
            for _ in range(self._num_workers)
        ]

        async def feed(chunk: FileChunk) -> None:
            # A full queue waits for a worker to take from it; workers that
            # all stopped (cancelled or failed) never will, so bail out
            # instead of blocking on the queue forever.
            while True:
                try:
                    queue.put_nowait(chunk)
                    return
                except asyncio.QueueFull:
                    if worker_failed.is_set() or all(w.done() for w in workers):
                        raise TechnicalError(
                            f"Upload of {self._file_name} stopped before the "
                            f"stream was consumed"
                        )
                    await asyncio.sleep(0.05)

        total = 0
        index = 0
        held: Optional[bytes] = None
        tail: Optional[bytes] = None
        try:
            while True:
                if await self._cancelled():
                    # Nothing more goes to the server after a cancel, not
                    # even the part held back for the boundary check.
                    held = None
                    break
                content = await self._read(self._chunk_size)
                if not content:
                    break
                total += len(content)
                if held is not None:
                    # The read that just came back proves the held part
                    # has a successor, so it can go to the workers now.
                    await feed(FileChunk(content=held, file_part=index))
                    index += 1
                    held = None
                if len(content) < self._chunk_size:
                    # The stream ran dry mid-part: this short part is the
                    # last one and carries the real total.
                    tail = content
                    break
                held = content
        finally:
            if all(w.done() for w in workers):
                results = await asyncio.gather(*workers, return_exceptions=True)
            else:
                for _ in workers:
                    await queue.put(None)
                results = await asyncio.gather(*workers, return_exceptions=True)

        failed = next((r for r in results if isinstance(r, Exception)), None)
        if failed is not None:
            raise failed

        last = tail if tail is not None else held
        if last is not None:
            await self._upload_chunk(
                FileChunk(content=last, file_part=index), file_total_parts=index + 1
            )
            if tt := self._file_msg.task_tracker:
                await tt.update_progress(size_delta=len(last))
            index += 1

        self._file_size = total
        self._total_parts = index
        # Nothing was saved for an empty stream, so the send must use the
        # small-file constructor, exactly as a known-size empty file does.
        self._is_big = total > 0
        return total

    def get_uploaded_file(self) -> UploadedFile:
        return UploadedFile(
            id=self._file_id,
            parts=self._total_parts,
            name=self._file_name,
        )

    async def send(self, chat_id: int, caption: str = "") -> SendMessageResp:
        logger.debug(
            f"Sending file {self._file_name} ({self._file_id}) to chat {chat_id}"
        )

        req = SendFileReq(
            chat=chat_id,
            file=self.get_uploaded_file(),
            name=self._file_name,
            caption=caption,
        )

        if self._is_big:
            return await self.client.send_big_file(req)
        return await self.client.send_small_file(req)
