import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Union

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
    # The count this part is uploaded with. -1 for every part of a
    # streamed upload but the last, which carries the true count.
    file_total_parts: int = -1


class FileUploader:
    def __init__(
        self,
        client: ITDLibClient,
        file_msg: UploadableFileMessage,
        workers: Optional[WorkersConfig] = None,
    ) -> None:
        self.client = client
        self._file_msg = file_msg
        self._file_size = self._file_msg.get_size()
        self._file_name = self._file_msg.file_name()

        # One part size for every file. The library's heuristic drops to
        # 128 KiB below 100 MB, which quadruples the number of requests for
        # no gain -- 512 KiB is accepted for any size.
        self._chunk_size = get_config().tgdcfs.transfer.upload_part_size_bytes

        # A size of -1 means the length is not known up front. Telegram
        # takes such uploads through its streamed-upload rules (see
        # core.telegram.org/api/files#streamed-uploads):
        # upload.saveBigFilePart is used for every part -- whatever the
        # stream turns out to weigh -- with file_total_parts -1 for all
        # parts but the last, and the part the stream ends on carries the
        # true count, as does one extra empty part when the stream ended
        # exactly on a part boundary.
        self._streamed = self._file_size < 0
        self._total_parts = (
            -1
            if self._streamed
            else (self._file_size + self._chunk_size - 1) // self._chunk_size
        )

        self._workers = workers or WorkersConfig.from_config()

        self._file_id = generate_random_long()

        self._is_big = True if self._streamed else is_big_file(self._file_size)
        self._read_size = 0
        self._uploaded_size = 0

        # Streamed uploads always take the big-file workers: they always
        # go through saveBigFilePart.
        self._num_workers = (
            self._workers.big
            if (self._is_big or self._streamed)
            else self._workers.small
        )
        self._lock = asyncio.Lock()

    async def _close(self) -> None:
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
                            file_total_parts=chunk.file_total_parts,
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

        await self._upload_chunk(
            FileChunk(
                content=content,
                file_part=part,
                file_total_parts=self._total_parts,
            )
        )
        return size_to_read

    async def _cancelled(self) -> bool:
        if tt := self._file_msg.task_tracker:
            return await tt.cancelled()
        return False

    async def upload(self) -> int:
        await self._file_msg.open()

        if self._streamed:
            uploaded = await self._upload_streamed()
        else:
            await self._upload_sized()
            uploaded = self._file_size

        await self._close()
        # Let the metadata see the length the stream turned out to have.
        self._file_msg.size = uploaded
        return uploaded

    async def _upload_sized(self) -> None:
        """The upload of a file whose length is known up front."""
        part_indexes: asyncio.Queue[int] = asyncio.Queue(maxsize=self._total_parts)
        for i in range(self._total_parts):
            await part_indexes.put(i)

        async def create_worker(worker_id: int) -> bool:
            while True:
                if await self._cancelled():
                    logger.warning(
                        f"Task uploading for {self._file_name} was cancelled. Worker {worker_id} exiting."
                    )
                    return False

                try:
                    part_size = await self._upload_next_part(part_indexes.get_nowait())
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

    async def _upload_streamed(self) -> int:
        """Upload without knowing the length, per the API's streamed rules.

        The parts are read front to back -- a stream cannot be re-read --
        and handed to the workers as they arrive, so reads keep running
        while earlier parts are still in flight. Every part goes out with
        ``file_total_parts`` -1 until the stream ends: the part it ends on
        is uploaded with the true count, or one extra empty part is when
        the stream ended exactly on a part boundary.
        """
        queue: asyncio.Queue[Union[FileChunk, BaseException, None]] = asyncio.Queue(
            maxsize=self._num_workers
        )

        async def read_parts() -> None:
            part = 0
            total = 0
            try:
                while True:
                    content = await self._read(self._chunk_size)
                    total += len(content)
                    if len(content) == self._chunk_size:
                        await queue.put(FileChunk(content=content, file_part=part))
                        part += 1
                        continue
                    # The stream ended: this is the last part. It carries
                    # the true count -- empty included, which is how the
                    # API finishes a stream that ended on a boundary. An
                    # empty stream from the very start sends nothing at
                    # all, exactly like a known-size empty upload.
                    self._total_parts = (total + self._chunk_size - 1) // (
                        self._chunk_size
                    )
                    if content or total > 0:
                        await queue.put(
                            FileChunk(
                                content=content,
                                file_part=part,
                                file_total_parts=self._total_parts,
                            )
                        )
                    await queue.put(None)
                    return
            except BaseException as ex:
                await queue.put(ex)

        async def create_worker(worker_id: int) -> bool:
            while True:
                if await self._cancelled():
                    logger.warning(
                        f"Task uploading for {self._file_name} was cancelled. Worker {worker_id} exiting."
                    )
                    return False

                item = await queue.get()
                if item is None:
                    return True
                if isinstance(item, BaseException):
                    raise item
                await self._upload_chunk(item)
                logger.debug(
                    f"[Worker {worker_id}] {self._uploaded_size} bytes of "
                    f"{self._file_name} uploaded. file_id={self._file_id}"
                )
                if (tt := self._file_msg.task_tracker) and item.content:
                    await tt.update_progress(size_delta=len(item.content))

        reader = asyncio.create_task(read_parts())
        workers = [
            asyncio.create_task(create_worker(worker_id))
            for worker_id in range(self._num_workers)
        ]
        try:
            await asyncio.gather(reader, *workers)
        except BaseException:
            # One dead part makes the whole file_id unusable, so the
            # other workers and the reader should stop feeding it.
            for task in [reader, *workers]:
                task.cancel()
            await asyncio.gather(reader, *workers, return_exceptions=True)
            raise

        # Every part's bytes are counted as they succeed, so this is the
        # length the stream turned out to have.
        return self._uploaded_size

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
