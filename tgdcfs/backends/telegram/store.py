"""The Telegram store: one channel, addressed through the bot pool.

Formerly ``MessageApi``. It batches ``get_messages`` calls (see
:class:`MessageBroker`), caches downloaded blocks, splits large
downloads into parallel pieces and owns the 2 GiB partitioning of
uploads -- everything Telegram-specific about moving bytes lives here.
"""

import asyncio
import logging
from typing import (
    AsyncIterator,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
)

from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate
from telethon.errors import MessageNotModifiedError, RPCError

from tgdcfs.backends.base import IStore, StoreCapabilities, make_store_key
from tgdcfs.config import TransferConfig, get_config
from tgdcfs.errors import (
    MessageNotFound,
    NoPinnedMessage,
    PinnedMessageNotSupported,
    TechnicalError,
)
from tgdcfs.reqres import (
    DeleteMessagesReq,
    DownloadFileReq,
    DownloadFileResp,
    EditMessageMediaReq,
    EditMessageTextReq,
    FileMessageFromBuffer,
    FileMessageFromStream,
    ForwardMessagesReq,
    GetPinnedMessageReq,
    MessageResp,
    MessageRespWithDocument,
    PinMessageReq,
    SearchMessageReq,
    SendTextReq,
    SentFileMessage,
    UploadableFileMessage,
)
from tgdcfs.utils.chunk_cache import (
    block_bounds,
    block_length,
    block_range,
    chunk_cache,
)
from tgdcfs.utils.message_cache import MessageCache
from tgdcfs.utils.others import exclude_none
from tgdcfs.utils.prefetching_chain import prefetching_chain

from .broker import MessageBroker
from .interface import TDLibApi
from .uploader import FileUploader

logger = logging.getLogger(__name__)

BACKEND_NAME = "telegram"
KEY_PREFIX = "tg"


def _transfer() -> TransferConfig:
    return get_config().tgdcfs.transfer


# Largest document a single message may carry.
PART_SIZE_DEFAULT = 512 * 1024 * 4000  # 2 GB
PART_SIZE_PREMIUM = 512 * 1024 * 8000  # 4 GB, premium accounts only

# Longest text message Telegram accepts.
MAX_TEXT_CHARS = 4096

# A longer text (a file descriptor listing a large replica: a 10 GB file
# mirrored to Discord carries over a thousand 10 MB part ids) is uploaded
# as a document, and the text message holds this marker plus the
# document's message id. The text message keeps its id, which the
# metadata refers to; reads resolve the marker transparently.
OVERFLOW_PREFIX = "TGDCFS_OVERFLOW:"
OVERFLOW_FILENAME = "overflow.json"

# A failed send is retried forever, this far apart: the bytes are
# already on Telegram's servers, only the message is missing.
SEND_RETRY_INTERVAL = 5  # seconds

# Telegram's messages.deleteMessages caps each request at 100 message ids.
DELETE_BATCH_SIZE = 100

# Read-ahead is speculative, so it must never crowd out the requests someone
# is actually waiting on.
MAX_READAHEAD_TASKS = 4

# Telegram addresses file content in 4 KiB units, so nothing smaller can
# arrive as a chunk. Used only to turn a byte budget into a queue length.
MIN_STREAM_CHUNK = 4096


rate = Rate(20, Duration.SECOND)
bucket = InMemoryBucket([rate])
limiter = Limiter(bucket, max_delay=60 * 1000)  # 60 seconds max delay


class TelegramStore(MessageBroker, IStore):
    def __init__(
        self,
        tdlib: TDLibApi,
        private_file_channel: int,
        key: Optional[str] = None,
        premium_upload: bool = False,
    ):
        """
        ``private_file_channel`` is the resolved (lib-specific) channel id
        used on the wire; ``key`` is the serialization key, built from the
        channel id as written in the config. ``premium_upload`` enables
        4 GiB parts through the premium user account.
        """
        MessageBroker.__init__(self, tdlib, private_file_channel)
        IStore.__init__(
            self, key or make_store_key(KEY_PREFIX, str(private_file_channel))
        )
        self._premium_upload = premium_upload and tdlib.account is not None
        self.__readahead: Set[tuple[int, int, int]] = set()
        # Text message id -> the overflow document it points to, as last
        # written or read here; lets an edit or a delete drop the document
        # it supersedes without reading the message first.
        self._overflow_of: Dict[int, int] = {}
        # Overflow document id -> its text. Documents are never edited, so
        # an entry cannot go stale.
        self._overflow_text: MessageCache[int, str] = MessageCache()

    @property
    def backend(self) -> str:
        return BACKEND_NAME

    @property
    def caps(self) -> StoreCapabilities:
        return StoreCapabilities(
            max_part_bytes=(
                PART_SIZE_PREMIUM if self._premium_upload else PART_SIZE_DEFAULT
            ),
            max_text_chars=MAX_TEXT_CHARS,
            supports_server_copy=True,
        )

    @staticmethod
    def __try_acquire(name: str):
        limiter.try_acquire(name)

    async def send_text(self, message: str) -> int:
        overflow = None
        if len(message) > MAX_TEXT_CHARS:
            overflow = await self._send_overflow(message)
            message = f"{OVERFLOW_PREFIX}{overflow}"
        self.__try_acquire("TelegramStore.send_text")
        message_id = (
            await self.tdlib.next_bot.send_text(
                SendTextReq(chat=self.private_file_channel, text=message)
            )
        ).message_id
        if overflow is not None:
            self._overflow_of[message_id] = overflow
        return message_id

    async def edit_message_text(self, message_id: int, message: str) -> int:
        superseded = self._overflow_of.get(message_id)
        overflow = None
        if len(message) > MAX_TEXT_CHARS:
            overflow = await self._send_overflow(message)
            message = f"{OVERFLOW_PREFIX}{overflow}"
        res = await self._edit_text(message_id, message)
        if overflow is not None:
            self._overflow_of[message_id] = overflow
        else:
            self._overflow_of.pop(message_id, None)
        if superseded is not None and superseded != overflow:
            await self.delete_messages([superseded], force=True)
        return res

    async def _edit_text(self, message_id: int, message: str) -> int:
        self.__try_acquire("TelegramStore.edit_message_text")
        try:
            return (
                await self.tdlib.next_bot.edit_message_text(
                    EditMessageTextReq(
                        chat=self.private_file_channel,
                        message_id=message_id,
                        text=message,
                    )
                )
            ).message_id
        except MessageNotModifiedError:
            return message_id
        except RPCError as e:
            if e.message == "Message to edit not found":
                raise MessageNotFound(message_id=message_id)
            if e.message == "Message is not modified":
                return message_id
            raise e

    async def _send_overflow(self, message: str) -> int:
        """Upload a text too long for a message as a document; its id."""
        sent = await self.upload(
            FileMessageFromBuffer.new(
                buffer=message.encode("utf-8"), name=OVERFLOW_FILENAME
            )
        )
        doc_id = sent[0].message_id
        self._overflow_text[doc_id] = message
        return doc_id

    async def _overflow_text_of(self, doc_id: int) -> Optional[str]:
        if (text := self._overflow_text.get(doc_id)) is not None:
            return text
        try:
            resp = await self.download_file(doc_id, 0, -1)
            data = bytearray()
            async for chunk in resp.chunks:
                data.extend(chunk)
        except Exception as ex:
            logger.warning(f"Could not read overflow document {doc_id}: {ex}")
            return None
        text = data.decode("utf-8")
        self._overflow_text[doc_id] = text
        return text

    async def _resolve_overflow(self, message: MessageResp) -> Optional[MessageResp]:
        """The message with the text its overflow marker stands for.

        ``None`` when the document is gone: the text is lost, as if the
        message itself were, and readers fall back to a mirror copy.
        """
        if message.document is not None or not message.text.startswith(OVERFLOW_PREFIX):
            return message
        try:
            doc_id = int(message.text[len(OVERFLOW_PREFIX) :])
        except ValueError:
            return message
        self._overflow_of[message.message_id] = doc_id
        text = await self._overflow_text_of(doc_id)
        if text is None:
            return None
        return MessageResp(message_id=message.message_id, text=text, document=None)

    async def get_messages(self, ids: list[int]) -> list[Optional[MessageResp]]:
        messages = await MessageBroker.get_messages(self, ids)
        if not any(
            m is not None and m.text and m.text.startswith(OVERFLOW_PREFIX)
            for m in messages
        ):
            return messages
        return [
            await self._resolve_overflow(m) if m is not None else None for m in messages
        ]

    async def get_pinned_message(self) -> MessageRespWithDocument:
        self.__try_acquire("TelegramStore.get_pinned_message")

        if not self.tdlib.account:
            raise PinnedMessageNotSupported()
        messages = await self.tdlib.account.get_pinned_messages(
            GetPinnedMessageReq(chat=self.private_file_channel)
        )

        if len(messages) == 0:
            raise NoPinnedMessage()

        if (message := messages[0]).document is None:
            raise TechnicalError("Pinned message does not contain a document")

        return MessageRespWithDocument(
            message_id=message.message_id,
            document=message.document,
            text="",
        )

    async def forward_messages_from(
        self, source_channel: int, message_ids: List[int]
    ) -> List[int]:
        """Server-side copy of messages from ``source_channel`` into this
        channel. Returns the new message ids, aligned with the input."""
        self.__try_acquire("TelegramStore.forward_messages_from")
        resp = await self.tdlib.next_bot.forward_messages(
            ForwardMessagesReq(
                from_chat=source_channel,
                to_chat=self.private_file_channel,
                message_ids=tuple(message_ids),
            )
        )
        return [m.message_id for m in resp]

    async def copy_from(
        self, source: IStore, message_ids: List[int]
    ) -> Optional[List[int]]:
        """Forward messages from another Telegram store into this one.

        Only Telegram-to-Telegram copies can happen server-side; for any
        other source the caller has to re-upload.
        """
        if not isinstance(source, TelegramStore):
            return None
        return await self.forward_messages_from(
            source.private_file_channel, message_ids
        )

    async def _reupload_within(self, message_id: int) -> int:
        """Bandwidth-bound copy inside this channel: stream a document
        down and up again.

        The fallback for channels where forwarding is impossible
        ("restrict saving content"). Bytes are copied verbatim -- this
        sits below the encryption decorator, so ciphertext stays
        ciphertext and is never encrypted twice. The document name is
        not preserved; names live in the metadata, so nothing
        user-visible depends on it.
        """
        message = (await self.get_messages([message_id]))[0]
        if not message or not message.document:
            raise MessageNotFound(message_id=message_id)
        size = message.document.size
        resp = await self.download_file(message_id, 0, size - 1)
        sent = await self.upload(
            FileMessageFromStream.new(
                stream=resp.chunks, size=size, name=f"part-{message_id}"
            )
        )
        if len(sent) != 1:
            raise TechnicalError(
                f"Re-uploading message {message_id} produced {len(sent)} parts"
            )
        return sent[0].message_id

    async def copy_within(self, message_ids: List[int]) -> List[int]:
        """Copy messages within this channel, without moving their bytes.

        Forwarding is a server-side operation: the new messages point at
        the documents that are already on Telegram, so a copy costs one
        API call no matter how large the file is. Channels with
        "restrict saving content" reject forwarding, so fall back to
        streaming each part down and up again.
        """
        if not message_ids:
            return []
        try:
            return await self.forward_messages_from(
                self.private_file_channel, message_ids
            )
        except Exception as ex:
            logger.warning(
                f"Could not forward {len(message_ids)} message(s) inside channel "
                f"{self.private_file_channel} ({ex}); falling back to re-upload"
            )
            return [
                await self._reupload_within(message_id) for message_id in message_ids
            ]

    # -- uploads -----------------------------------------------------------

    @staticmethod
    def _partition(size: int, part_size: int) -> Generator[int]:
        parts = (size + part_size - 1) // part_size
        for _ in range(parts - 1):
            yield part_size
        yield size - (parts - 1) * part_size

    async def _send_file(
        self, file_msg: UploadableFileMessage, use_account_api: bool
    ) -> SentFileMessage:
        if use_account_api and (account_api := self.tdlib.account):
            api = account_api
        else:
            api = self.tdlib.next_bot

        uploader = FileUploader(api, file_msg)
        logger.info(
            f"Uploading file {file_msg.name} of size {file_msg.size} bytes to "
            f"channel {self.private_file_channel} using {(await api.get_me()).name}."
        )
        size = await uploader.upload()

        while True:
            try:
                message = await uploader.send(self.private_file_channel)
                return SentFileMessage(message_id=message.message_id, size=size)
            except Exception as ex:
                logger.error(
                    f"Exception occurred when sending file {file_msg.name}: {ex}. "
                    f"Waiting {SEND_RETRY_INTERVAL} seconds before retrying."
                )
                await asyncio.sleep(SEND_RETRY_INTERVAL)

    @property
    def read_slots(self) -> int:
        # The same fan-out the store uses for its own split downloads.
        return _transfer().download_pieces_in_flight

    def _part_size_for(self, size: int) -> tuple[int, bool]:
        """The part size for ``size`` bytes and whether the account uploads."""
        premium_upload = size > PART_SIZE_DEFAULT and self._premium_upload
        return (
            PART_SIZE_PREMIUM if premium_upload else PART_SIZE_DEFAULT
        ), premium_upload

    def plan_parts(self, size: int) -> List[int]:
        part_size, _ = self._part_size_for(size)
        return list(self._partition(size, part_size))

    async def upload(self, file_msg: UploadableFileMessage) -> List[SentFileMessage]:
        """Upload ``file_msg`` in parts of at most one Telegram document.

        Files above the bot limit go through the premium account when the
        deployment allows it, in 4 GiB parts. A seekable source (a version
        in the local cache) can have ``transfer.upload_parts_in_flight``
        parts in the air at once, each through its own bot; a stream is
        read front to back and goes part by part.
        """
        size = file_msg.get_size()
        file_name = file_msg.name or "unnamed"
        part_size, premium_upload = self._part_size_for(size)
        parts = list(self._partition(size, part_size))

        in_flight = _transfer().upload_parts_in_flight
        part_of = getattr(file_msg, "part", None)
        if len(parts) > 1 and in_flight > 1 and callable(part_of):
            slots = asyncio.Semaphore(in_flight)
            offsets = [sum(parts[:i]) for i in range(len(parts))]

            async def send(index: int) -> SentFileMessage:
                async with slots:
                    part_msg = part_of(offsets[index], parts[index])
                    part_msg.name = f"[part{index + 1}]{file_name}"
                    part_msg.size = parts[index]
                    return await self._send_file(
                        part_msg, use_account_api=premium_upload
                    )

            tasks = [asyncio.create_task(send(i)) for i in range(len(parts))]
            try:
                return list(await asyncio.gather(*tasks))
            except BaseException:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

        res: List[SentFileMessage] = []
        for i, part in enumerate(parts):
            file_msg.name = f"[part{i + 1}]{file_name}"
            file_msg.size = part
            res.append(await self._send_file(file_msg, use_account_api=premium_upload))
            file_msg.next_part(part)
        return res

    async def replace_document(self, message_id: int, buffer: bytes, name: str) -> int:
        file_msg: FileMessageFromBuffer = FileMessageFromBuffer.new(
            buffer=buffer, name=name
        )
        uploader = FileUploader(self.tdlib.next_bot, file_msg)
        await uploader.upload()

        while True:
            try:
                message = await uploader.client.edit_message_media(
                    EditMessageMediaReq(
                        chat=self.private_file_channel,
                        message_id=message_id,
                        file=uploader.get_uploaded_file(),
                    )
                )
                return message.message_id
            except Exception as ex:
                logger.error(
                    f"Exception occurred when editing document of message "
                    f"{message_id}: {ex}. Waiting {SEND_RETRY_INTERVAL} seconds "
                    f"before retrying."
                )
                await asyncio.sleep(SEND_RETRY_INTERVAL)

    async def delete_messages(
        self, message_ids: Iterable[int], force: bool = False
    ) -> None:
        """Best-effort deletion of channel messages.

        Gated by ``telegram.delete_messages_on_remove`` so the default
        TGDCFS behavior (file removed from metadata, message kept on the
        channel) is unchanged. Failures are logged but never raised --
        the caller has already committed the metadata change and we do
        not want a delete error to roll that back.

        ``force=True`` bypasses the config gate; it is used for internal
        bookkeeping messages (e.g. a superseded mirrored metadata blob)
        that should never linger regardless of the user's preference for
        keeping removed *file* messages.
        """
        if not force and not get_config().telegram.delete_messages_on_remove:
            return
        message_ids = list(message_ids)
        # A text message that overflowed takes its document along.
        message_ids += [
            doc_id
            for mid in message_ids
            if (doc_id := self._overflow_of.pop(mid, None)) is not None
        ]
        unique_ids: List[int] = list({mid for mid in message_ids if mid > 0})
        if not unique_ids:
            return
        for start in range(0, len(unique_ids), DELETE_BATCH_SIZE):
            batch = tuple(unique_ids[start : start + DELETE_BATCH_SIZE])
            self.__try_acquire("TelegramStore.delete_messages")
            try:
                await self.tdlib.next_bot.delete_messages(
                    DeleteMessagesReq(
                        chat=self.private_file_channel,
                        message_ids=batch,
                    )
                )
            except Exception as ex:
                logger.warning(
                    f"Failed to delete telegram messages {batch} from channel "
                    f"{self.private_file_channel}: {ex}"
                )

    async def pin_message(self, message_id: int):
        self.__try_acquire("TelegramStore.pin_message")
        return await self.tdlib.next_bot.pin_message(
            PinMessageReq(chat=self.private_file_channel, message_id=message_id)
        )

    async def search_messages(self, search: str) -> list[MessageResp]:
        self.__try_acquire("TelegramStore.search_messages")
        if self.tdlib.account:
            return list(
                exclude_none(
                    await self.tdlib.account.search_messages(
                        SearchMessageReq(chat=self.private_file_channel, search=search)
                    )
                )
            )
        return []

    @staticmethod
    def split_download_pieces(
        begin: int, end: int, piece_size: int
    ) -> Iterator[tuple[int, int]]:
        """Cut the inclusive range ``[begin, end]`` into pieces."""
        for piece_begin in range(begin, end + 1, piece_size):
            yield piece_begin, min(piece_begin + piece_size - 1, end)

    @staticmethod
    def _size(begin: int, end: int) -> int:
        """Number of bytes in the inclusive range ``[begin, end]``."""
        return end - begin + 1

    def _download_piece(
        self, message_id: int, begin: int, end: int
    ) -> AsyncIterator[bytes]:
        """One piece of a split download.

        A generator, so the request is issued when the piece is actually
        started. Building them eagerly would fire one call per piece up
        front -- hundreds of them for a large file.
        """

        async def piece() -> AsyncIterator[bytes]:
            resp = await self.tdlib.next_bot.download_file(
                DownloadFileReq(
                    chat=self.private_file_channel,
                    message_id=message_id,
                    begin=begin,
                    end=end,
                )
            )
            async for chunk in resp.chunks:
                yield chunk

        return piece()

    async def download_file_parallel(
        self, message_id: int, begin: int, end: int
    ) -> DownloadFileResp:
        transfer = _transfer()
        piece_size = transfer.download_piece_size_bytes

        pieces = [
            self._download_piece(message_id, piece_begin, piece_end)
            for piece_begin, piece_end in self.split_download_pieces(
                begin, end, piece_size
            )
        ]

        # A piece has to fit in its queue, otherwise a piece that is ready
        # early blocks instead of freeing its bot for the next one -- which
        # is what would keep the download sequential in all but name. The
        # queue counts chunks, whose size the Telegram library picks, so the
        # bound is derived from the smallest one it could hand out. This does
        # not risk memory: a source here is a single piece, so it cannot
        # produce more than one piece worth of bytes however deep its queue.
        queue_depth = max(1, -(-piece_size // MIN_STREAM_CHUNK))

        return DownloadFileResp(
            chunks=prefetching_chain(
                pieces,
                concurrency=transfer.download_pieces_in_flight,
                queue_depth=queue_depth,
            ),
            size=self._size(begin, end),
        )

    async def _document_size(self, message_id: int) -> Optional[int]:
        """On-wire size of a file message, or ``None`` if it is not one."""
        messages = await self.get_messages([message_id])
        if not (message := messages[0]) or not message.document:
            return None
        return message.document.size

    async def _fetch_blocks(
        self, message_id: int, first: int, last: int, size: int, block_size: int
    ) -> AsyncIterator[tuple[int, bytes, bool]]:
        """Fetch blocks ``first..last`` and cut the stream back into blocks.

        Yields ``(index, data, complete)``. A block is complete when it has
        the length it should have for its position -- the final block of a
        document is legitimately short, a truncated stream is not, and only
        the former may be cached.
        """
        begin, _ = block_bounds(first, block_size, size)
        _, end = block_bounds(last, block_size, size)
        resp = await self._download_uncached(message_id, begin, end)

        buffer = bytearray()
        index = first
        async for chunk in resp.chunks:
            buffer.extend(chunk)
            while index <= last:
                expected = block_length(index, block_size, size)
                if len(buffer) < expected:
                    break
                yield index, bytes(buffer[:expected]), True
                del buffer[:expected]
                index += 1

        if index <= last and buffer:
            yield index, bytes(buffer), False

    async def _cached_download(
        self, message_id: int, begin: int, end: int, size: int, block_size: int
    ) -> AsyncIterator[bytes]:
        """Serve ``[begin, end]`` from cached blocks, fetching what is missing.

        Missing blocks are fetched in runs rather than one at a time: a
        request per block would trade the round trips this cache is meant to
        save for a larger number of smaller ones.
        """
        cache = chunk_cache()
        blocks = block_range(begin, min(end, size - 1), block_size)

        index = blocks.start
        while index < blocks.stop:
            key = (self.private_file_channel, message_id, index)
            if (cached := cache.get(key)) is not None:
                yield self._slice(cached, index, block_size, begin, end)
                index += 1
                continue

            # Everything up to the next cached block is fetched in one go.
            run_end = index
            while run_end + 1 < blocks.stop and not cache.contains(
                (self.private_file_channel, message_id, run_end + 1)
            ):
                run_end += 1

            async for position, block, complete in self._fetch_blocks(
                message_id, index, run_end, size, block_size
            ):
                if complete:
                    cache.put((self.private_file_channel, message_id, position), block)
                yield self._slice(block, position, block_size, begin, end)

            index = run_end + 1

    @staticmethod
    def _slice(
        block: bytes, index: int, block_size: int, begin: int, end: int
    ) -> bytes:
        """The part of one block that falls inside the requested range."""
        block_begin = index * block_size
        start = max(0, begin - block_begin)
        stop = min(len(block), end - block_begin + 1)
        return block[start:stop] if stop > start else b""

    async def _download_cached(
        self, message_id: int, begin: int, end: int
    ) -> Optional[DownloadFileResp]:
        """Wrap the download in the block cache, if it can be applied.

        Returns ``None`` when the cache cannot help -- disabled, or a
        message whose size is unknown -- so the caller falls through to the
        plain path.
        """
        cache = chunk_cache()
        if not cache.enabled or begin < 0:
            return None

        size = await self._document_size(message_id)
        if size is None or size <= 0 or begin >= size:
            return None

        if end < 0 or end > size - 1:
            end = size - 1
        if end < begin:
            return None

        block_size = _transfer().chunk_cache_block_bytes
        self._schedule_readahead(message_id, end // block_size, size, block_size)

        return DownloadFileResp(
            chunks=self._cached_download(message_id, begin, end, size, block_size),
            size=self._size(begin, end),
        )

    def _schedule_readahead(
        self, message_id: int, after_block: int, size: int, block_size: int
    ) -> None:
        """Pull the blocks just past this request into the cache.

        Readers rarely stop where they said they would: a player asks for a
        few kilobytes and comes back for the next few, and an SFTP client
        walks a whole file that way. Fetching the next blocks while the
        current ones are being served turns the following request into a
        cache hit.

        Deliberately best-effort -- capped, de-duplicated, and silent about
        failures, because nobody is waiting on it.
        """
        count = _transfer().chunk_cache_readahead
        if count < 1 or len(self.__readahead) >= MAX_READAHEAD_TASKS:
            return

        cache = chunk_cache()
        last_block = (size - 1) // block_size
        wanted = [
            index
            for index in range(
                after_block + 1, min(after_block + count, last_block) + 1
            )
            if not cache.contains((self.private_file_channel, message_id, index))
        ]
        if not wanted:
            return

        key = (message_id, wanted[0], wanted[-1])
        if key in self.__readahead:
            return
        self.__readahead.add(key)

        async def run() -> None:
            try:
                async for index, block, complete in self._fetch_blocks(
                    message_id, wanted[0], wanted[-1], size, block_size
                ):
                    if complete:
                        cache.put((self.private_file_channel, message_id, index), block)
            except Exception as ex:
                logger.debug(f"Read-ahead for message {message_id} failed: {ex}")
            finally:
                self.__readahead.discard(key)

        asyncio.create_task(run())

    async def download_file(
        self, message_id: int, begin: int, end: int
    ) -> DownloadFileResp:
        if (cached := await self._download_cached(message_id, begin, end)) is not None:
            return cached
        return await self._download_uncached(message_id, begin, end)

    async def _download_uncached(
        self, message_id: int, begin: int, end: int
    ) -> DownloadFileResp:
        # ``end == -1`` means the end of the document, but the clients below
        # take an inclusive range and refuse an end before the begin. Only
        # the block cache resolved it so far, and that is off by default:
        # an overflow descriptor, read with ``-1``, then failed to load.
        if end < 0:
            size = await self._document_size(message_id)
            if size is None:
                raise MessageNotFound(message_id=message_id)
            end = size - 1
        if (
            (account_config := get_config().telegram.account)
            and account_config.used_to_download
            and (account := self.tdlib.account)
        ):
            return await account.download_file(
                DownloadFileReq(
                    chat=self.private_file_channel,
                    message_id=message_id,
                    begin=begin,
                    end=end,
                )
            )

        if (
            end > 0
            and self._size(begin, end) > _transfer().parallel_download_threshold_bytes
        ):
            return await self.download_file_parallel(message_id, begin, end)

        return await self.tdlib.next_bot.download_file(
            DownloadFileReq(
                chat=self.private_file_channel,
                message_id=message_id,
                begin=begin,
                end=end,
            )
        )
