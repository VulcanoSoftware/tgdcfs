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


# What ``file_total_parts`` carries on every part of a streamed upload but
# the last one: the number of parts is not known yet.
STREAMED_TOTAL_PARTS = -1


@dataclass
class FileChunk:
    content: bytes
    file_part: int
    # ``None``: take the uploader's own total (a file of known size).
    total_parts: Optional[int] = None


class FileUploader:
    """Moves the bytes of one document to Telegram.

    A file of known size is cut into parts whose number is announced with
    every part (``file_total_parts``). One of unknown size is uploaded the
    way Telegram describes for streams (https://core.telegram.org/api/files,
    "Streamed uploads"): every part carries ``-1`` as the total, and only the
    last one the real number. That last part is sent once all earlier ones
    are confirmed, and parts are read one ahead so the last one is known to
    be the last without sending a part with nothing in it.

    ``max_bytes`` caps a streamed document (a message holds at most 2 GB);
    the stream is left unread beyond it so the caller can carry on with the
    next document. ``upload`` returns the number of bytes it took, which is
    ``0`` when the stream had nothing left. A caller that uploads several
    documents from one message opens and closes it itself
    (``manage_message=False``).
    """

    def __init__(
        self,
        client: ITDLibClient,
        file_msg: UploadableFileMessage,
        workers: Optional[WorkersConfig] = None,
        max_bytes: Optional[int] = None,
        manage_message: bool = True,
    ):
        self.client = client
        self._file_msg = file_msg
        self._streamed = file_msg.unsized
        self._max_bytes = max_bytes
        self._manage_message = manage_message
        self._file_size = 0 if self._streamed else self._file_msg.get_size()
        self._file_name = self._file_msg.file_name()

        # One part size for every file. The library's heuristic drops to
        # 128 KiB below 100 MB, which quadruples the number of requests for
        # no gain -- 512 KiB is accepted for any size.
        self._chunk_size = get_config().tgdcfs.transfer.upload_part_size_bytes
        self._total_parts = (self._file_size + self._chunk_size - 1) // self._chunk_size

        self._part_indexes: asyncio.Queue[int] = asyncio.Queue(
            maxsize=max(self._total_parts, 1)
        )

        self._workers = workers or WorkersConfig.from_config()

        self._file_id = generate_random_long()

        # A stream always goes through the big-file calls, even when it turns
        # out to be small: only those can say "the total is not known yet".
        self._is_big = self._streamed or is_big_file(self._file_size)
        self._read_size = 0
        self._uploaded_size = 0

        self._num_workers = self._workers.big if self._is_big else self._workers.small
        self._lock = asyncio.Lock()

    async def _close(self) -> None:
        if self._manage_message:
            await self._file_msg.close()

    async def _read(self, length: int) -> bytes:
        return await self._file_msg.read(length)

    async def _upload_chunk(self, chunk: FileChunk) -> None:
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
                                if chunk.total_parts is None
                                else chunk.total_parts
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

    async def _read_part(self) -> bytes:
        """The next part's worth of the stream; short only at its end."""
        part = bytearray()
        while len(part) < self._chunk_size:
            piece = await self._read(self._chunk_size - len(part))
            if not piece:
                break
            part.extend(piece)
        return bytes(part)

    async def _upload_stream(self) -> int:
        """Upload a stream of unknown length as one document."""
        part_limit = None
        if self._max_bytes is not None:
            part_limit = max(self._max_bytes // self._chunk_size, 1)

        slots = asyncio.Semaphore(self._num_workers)
        sending: set["asyncio.Task[None]"] = set()

        async def send(chunk: FileChunk) -> None:
            try:
                await self._upload_chunk(chunk)
            finally:
                slots.release()
            if tt := self._file_msg.task_tracker:
                await tt.update_progress(size_delta=len(chunk.content))

        async def stop() -> None:
            for task in sending:
                task.cancel()
            await asyncio.gather(*sending, return_exceptions=True)

        def raise_failures() -> None:
            for task in [t for t in sending if t.done()]:
                sending.discard(task)
                task.result()  # a failed part fails the document

        try:
            current = await self._read_part()
            if not current:
                return 0
            part = 0
            while True:
                if part_limit is not None and part + 1 >= part_limit:
                    following = b""  # the document is full, whatever comes next
                else:
                    following = await self._read_part()
                if not following:
                    break

                if await self._cancelled():
                    raise TechnicalError(f"Upload of {self._file_name} was cancelled")
                await slots.acquire()
                chunk = FileChunk(current, part, STREAMED_TOTAL_PARTS)
                self._read_size += len(current)
                task = asyncio.create_task(send(chunk))
                sending.add(task)
                raise_failures()
                part += 1
                current = following

            # The last part says how many there were, and goes out only when
            # every other part is in.
            await asyncio.gather(*sending)
            sending.clear()
            self._total_parts = part + 1
            self._read_size += len(current)
            await self._upload_chunk(FileChunk(current, part, self._total_parts))
            if tt := self._file_msg.task_tracker:
                await tt.update_progress(size_delta=len(current))
        except BaseException:
            await stop()
            raise

        self._file_size = self._read_size
        await self._close()
        return self._file_size

    async def upload(self) -> int:
        if self._manage_message:
            await self._file_msg.open()

        if self._streamed:
            return await self._upload_stream()

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
