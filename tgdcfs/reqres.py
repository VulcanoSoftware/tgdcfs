import asyncio
import os
from dataclasses import dataclass, field
from io import IOBase
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from tgdcfs.errors import TechnicalError
from tgdcfs.tasks.integrations import TaskTracker


@dataclass
class Message:
    message_id: int


@dataclass
class Replica:
    """A copy of a whole file version in one store, with that store's own
    part layout. ``part_sizes`` may be empty until the copy is verified."""

    message_ids: List[int] = field(default_factory=list)
    part_sizes: List[int] = field(default_factory=list)

    @property
    def size(self) -> int:
        return sum(self.part_sizes)


@dataclass
class SentFileMessage(Message):
    size: int
    # Message ids of copies of this part in mirror stores, keyed by store
    # key. Empty when redundancy is off.
    mirrors: Dict[str, int] = field(default_factory=dict)
    # Copies of the whole version in stores that re-partitioned it, keyed
    # by store key. Carried by the first part's message only.
    replicas: Dict[str, "Replica"] = field(default_factory=dict)


@dataclass
class Chat:
    chat: int


@dataclass
class GetMessagesReq(Chat):
    message_ids: Tuple[int, ...]


@dataclass
class Document:
    size: int
    id: int
    access_hash: int
    file_reference: bytes
    mime_type: Optional[str]


@dataclass
class MessageResp(Message):
    text: str
    document: Optional[Document]


@dataclass
class MessageRespWithDocument(MessageResp):
    document: Document


GetMessagesResp = list[Optional[MessageResp]]
GetMessagesRespNoNone = list[MessageResp]


@dataclass
class SearchMessageReq(Chat):
    search: str


@dataclass
class ForwardMessagesReq:
    from_chat: int
    to_chat: int
    message_ids: Tuple[int, ...]


@dataclass
class DeleteMessagesReq(Chat):
    message_ids: Tuple[int, ...]


GetPinnedMessageReq = Chat
SendMessageResp = Message


@dataclass
class SendTextReq(Chat):
    text: str


@dataclass
class EditMessageTextReq(SendTextReq, Message):
    pass


@dataclass
class PinMessageReq(Chat, Message):
    pass


@dataclass
class SaveFilePartReq:
    file_id: int
    bytes: bytes
    file_part: int


@dataclass
class SaveBigFilePartReq(SaveFilePartReq):
    file_total_parts: int


@dataclass
class SaveFilePartResp:
    success: bool


@dataclass
class UploadedFile:
    id: int
    parts: int
    name: str


@dataclass
class FileAttr:
    name: str
    caption: str


@dataclass
class SendFileReq(Chat, FileAttr):
    file: UploadedFile


@dataclass
class EditMessageMediaReq(Chat, Message):
    file: UploadedFile


@dataclass
class DownloadFileReq(Chat, Message):
    begin: int
    end: int


FileContent = AsyncIterator[bytes]


async def aclose_content(stream: FileContent) -> None:
    """Close a stream a caller walks away from.

    Reads that stop early (a header probe, a client hanging up) leave an
    async generator suspended, and closing the generator that only iterates
    over it does not close it -- the interpreter then reports "coroutine
    method 'aclose' ... was never awaited" when it is collected. Iterators
    without ``aclose`` need nothing.
    """
    aclose = getattr(stream, "aclose", None)
    if aclose is not None:
        await aclose()


@dataclass
class DownloadFileResp:
    chunks: FileContent
    size: int


@dataclass
class GetMeResp:
    is_premium: bool
    name: str


@dataclass
class FileTags:
    pass


@dataclass
class FileMessage:
    name: str
    size: int


@dataclass
class UploadableFileMessage(FileMessage):
    caption: str
    tags: FileTags
    _offset: int
    _read_size: int

    task_tracker: Optional[TaskTracker]

    # Id of the version this upload becomes, set by the file-desc API
    # before the bytes move so the content repository can stage them in
    # the local cache under the same id the metadata will carry.
    version_id: Optional[str] = field(default=None, kw_only=True)

    # An upload whose total size is not known up front (SFTP never says).
    # It has no ``get_size``; the content repository cuts it into segments
    # of ``segment_size`` bytes, each of which has a known size as soon as
    # it is full, and uploads them as parts of one version while the rest
    # is still arriving. ``spool_dir`` is where a segment waits meanwhile.
    unsized: bool = field(default=False, kw_only=True)
    segment_size: int = field(default=0, kw_only=True)
    spool_dir: Optional[str] = field(default=None, kw_only=True)

    # Number of parts of this version that precede this message, so the
    # parts of later segments keep counting instead of starting at 1.
    part_offset: int = field(default=0, kw_only=True)

    def _get_size(self) -> int:
        return 0

    def get_size(self) -> int:
        if self.unsized:
            raise TechnicalError(f"The size of {self.name} is not known yet")
        return self.size or self._get_size()

    async def open(self) -> None:
        pass

    async def read(self, length: int) -> bytes:
        raise NotImplementedError("Subclasses must implement the read method")

    async def close(self) -> None:
        pass

    def file_name(self) -> str:
        return self.name or "unnamed"

    def next_part(self, part_size: int) -> None:
        self._offset += part_size
        self._read_size = 0


@dataclass
class FileMessageEmpty(FileMessage):
    @classmethod
    def new(cls, name: str = "unnamed") -> "FileMessageEmpty":
        return cls(name=name, size=0)


@dataclass
class FileMessageFromPath(UploadableFileMessage):
    path: str
    _fd: IOBase

    def _get_size(self) -> int:
        return os.path.getsize(self.path)

    @classmethod
    def new(cls, path: str, name: str = "unnamed") -> "FileMessageFromPath":
        return cls(
            name=name,
            caption="",
            tags=FileTags(),
            path=path,
            _offset=0,
            size=os.path.getsize(path),
            task_tracker=None,
            _read_size=0,
            _fd=open(path, "rb"),
        )

    async def read(self, length: int) -> bytes:
        # Off the event loop: a blocking disk read here stalls every other
        # transfer in the process, not just this one.
        return await asyncio.to_thread(self._fd.read, length)

    async def close(self) -> None:
        if self._fd:
            self._fd.close()

    def file_name(self) -> str:
        return self.name or os.path.basename(self.path)


@dataclass
class FileMessageFromBuffer(UploadableFileMessage):
    buffer: bytes
    __buffer: bytes = b""

    def _get_size(self) -> int:
        return len(self.buffer)

    @classmethod
    def new(cls, buffer: bytes, name: str = "unnamed") -> "FileMessageFromBuffer":
        return cls(
            name=name,
            caption="",
            tags=FileTags(),
            buffer=buffer,
            _offset=0,
            size=len(buffer),
            task_tracker=None,
            _read_size=0,
        )

    async def open(self) -> None:
        self.__buffer = self.buffer[self._offset :]

    async def read(self, length: int) -> bytes:
        chunk = self.__buffer[:length]
        self.__buffer = self.__buffer[length:]
        return chunk


@dataclass
class FileMessageFromStream(UploadableFileMessage):
    stream: FileContent
    buffer: bytearray = field(default_factory=bytearray)

    @classmethod
    def new(
        cls,
        stream: FileContent,
        size: Optional[int],
        name: str = "unnamed",
        segment_size: int = 0,
        spool_dir: Optional[str] = None,
    ) -> "FileMessageFromStream":
        """``size=None`` is a stream of unknown length, read until it ends."""
        return cls(
            name=name,
            caption="",
            tags=FileTags(),
            stream=stream,
            _offset=0,
            size=size or 0,
            task_tracker=None,
            _read_size=0,
            unsized=size is None,
            segment_size=segment_size,
            spool_dir=spool_dir,
        )

    async def read(self, length: int) -> bytes:
        """Take ``length`` bytes off the front of the stream.

        Kept in one buffer rather than a list of chunks: rejoining the list
        on every read copies everything still pending, which for a stream
        arriving in small chunks is quadratic in the size of the part.
        """
        if self.unsized:
            # Short only at the end of the stream.
            while len(self.buffer) < length:
                try:
                    self.buffer.extend(await anext(self.stream))
                except StopAsyncIteration:
                    break
            size_to_return = min(length, len(self.buffer))
        else:
            size_to_return = min(length, self.get_size() - self._read_size)
            while len(self.buffer) < size_to_return:
                self.buffer.extend(await anext(self.stream))

        res = bytes(self.buffer[:size_to_return])
        del self.buffer[:size_to_return]
        self._read_size += size_to_return
        return res


@dataclass
class FileMessageFromSpool(UploadableFileMessage):
    """One finished segment of an unsized upload, waiting in a spool file.

    Its size is known, which is the whole point: the stores can plan it
    like any other file. The spool is closed together with the message.
    """

    spool: Any = None

    @classmethod
    def new(
        cls,
        spool: Any,
        size: int,
        name: str,
        part_offset: int = 0,
        task_tracker: Optional[TaskTracker] = None,
    ) -> "FileMessageFromSpool":
        return cls(
            name=name,
            caption="",
            tags=FileTags(),
            spool=spool,
            _offset=0,
            size=size,
            task_tracker=task_tracker,
            _read_size=0,
            part_offset=part_offset,
        )

    def _get_size(self) -> int:
        return self.size

    async def open(self) -> None:
        await asyncio.to_thread(self.spool.seek, 0)

    async def read(self, length: int) -> bytes:
        return await asyncio.to_thread(self.spool.read, length)

    async def close(self) -> None:
        await asyncio.to_thread(self.spool.close)


@dataclass
class FileMessageImported(FileMessage):
    message_id: int

    @classmethod
    def new(
        cls, message_id: int, size: int, name: str = "unnamed"
    ) -> "FileMessageImported":
        return cls(name=name, size=size, message_id=message_id)
