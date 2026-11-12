"""An in-memory store for tests.

Implements the full :class:`IStore` contract on top of a dict, with the
knobs that make backend differences testable without a network:

* ``max_part_bytes`` -- how the store partitions uploads, so a
  "Discord-like" store with small parts can mirror a "Telegram-like" one;
* ``server_copy`` -- whether ``copy_from`` copies server-side (only
  between two stores of the same ``backend`` name) or reports ``None``;
* ``fail`` -- a set of method names that raise, to simulate outages;
* ``deleted_on_remove`` -- the ``delete_messages_on_remove`` gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

from tgdcfs.backends.base import IStore, StoreCapabilities
from tgdcfs.errors import MessageNotFound, NoPinnedMessage, TechnicalError
from tgdcfs.reqres import (
    Document,
    DownloadFileResp,
    MessageResp,
    MessageRespWithDocument,
    SentFileMessage,
    UploadableFileMessage,
)


@dataclass
class FakeMessage:
    message_id: int
    text: str = ""
    document: Optional[bytes] = None
    name: str = ""

    def to_resp(self) -> MessageResp:
        doc = None
        if self.document is not None:
            doc = Document(
                size=len(self.document),
                id=self.message_id,
                access_hash=0,
                file_reference=b"",
                mime_type=None,
            )
        return MessageResp(message_id=self.message_id, text=self.text, document=doc)


@dataclass
class FakeStore(IStore):
    key: str = "fake:1"
    backend_name: str = "fake"
    max_part_bytes: int = 1 << 30
    max_text_chars: int = 4096
    server_copy: bool = True
    deleted_on_remove: bool = True
    fail: Set[str] = field(default_factory=set)
    messages: Dict[int, FakeMessage] = field(default_factory=dict)
    pinned: Optional[int] = None
    next_id: int = 1
    calls: List[str] = field(default_factory=list)

    def __post_init__(self):
        IStore.__init__(self, self.key)

    # -- helpers -----------------------------------------------------------

    def _check(self, method: str) -> None:
        self.calls.append(method)
        if method in self.fail:
            raise TechnicalError(f"{self.key}: {method} failed (simulated)")

    def _add(
        self, text: str = "", document: Optional[bytes] = None, name: str = ""
    ) -> int:
        mid = self.next_id
        self.next_id += 1
        self.messages[mid] = FakeMessage(mid, text=text, document=document, name=name)
        return mid

    def document_of(self, message_id: int) -> bytes:
        message = self.messages[message_id]
        assert message.document is not None
        return message.document

    # -- IStore ------------------------------------------------------------

    @property
    def backend(self) -> str:
        return self.backend_name

    @property
    def caps(self) -> StoreCapabilities:
        return StoreCapabilities(
            max_part_bytes=self.max_part_bytes,
            max_text_chars=self.max_text_chars,
            supports_server_copy=self.server_copy,
        )

    async def send_text(self, message: str) -> int:
        self._check("send_text")
        return self._add(text=message)

    async def edit_message_text(self, message_id: int, message: str) -> int:
        self._check("edit_message_text")
        if message_id not in self.messages:
            raise MessageNotFound(message_id=message_id)
        self.messages[message_id].text = message
        return message_id

    async def get_messages(self, ids: list[int]) -> list[Optional[MessageResp]]:
        self._check("get_messages")
        return [m.to_resp() if (m := self.messages.get(i)) else None for i in ids]

    async def search_messages(self, search: str) -> list[MessageResp]:
        self._check("search_messages")
        return [m.to_resp() for m in self.messages.values() if search in m.text]

    async def upload(self, file_msg: UploadableFileMessage) -> List[SentFileMessage]:
        self._check("upload")
        await file_msg.open()
        size = file_msg.get_size()
        res: List[SentFileMessage] = []
        remaining = size
        part = 0
        while remaining > 0 or not res:
            part_size = min(self.max_part_bytes, remaining)
            data = bytearray()
            while len(data) < part_size:
                chunk = await file_msg.read(part_size - len(data))
                if not chunk:
                    break
                data.extend(chunk)
            part += 1
            mid = self._add(
                document=bytes(data),
                name=f"[part{file_msg.part_offset + part}]{file_msg.file_name()}",
            )
            res.append(SentFileMessage(message_id=mid, size=len(data)))
            remaining -= len(data)
            if not data:
                break
        await file_msg.close()
        return res

    async def download_file(
        self, message_id: int, begin: int, end: int
    ) -> DownloadFileResp:
        self._check("download_file")
        if (
            message := self.messages.get(message_id)
        ) is None or message.document is None:
            raise MessageNotFound(message_id=message_id)
        data = message.document
        if end < 0 or end >= len(data):
            end = len(data) - 1
        payload = data[begin : end + 1]

        async def chunks():
            # Two chunks so mid-stream behaviour gets exercised.
            half = len(payload) // 2
            if payload[:half]:
                yield payload[:half]
            if payload[half:]:
                yield payload[half:]

        return DownloadFileResp(chunks=chunks(), size=len(payload))

    async def replace_document(self, message_id: int, buffer: bytes, name: str) -> int:
        self._check("replace_document")
        if message_id not in self.messages:
            raise MessageNotFound(message_id=message_id)
        self.messages[message_id].document = buffer
        self.messages[message_id].name = name
        return message_id

    async def copy_within(self, message_ids: List[int]) -> List[int]:
        self._check("copy_within")
        return [
            self._add(document=self.document_of(mid), name=self.messages[mid].name)
            for mid in message_ids
        ]

    async def copy_from(
        self, source: IStore, message_ids: List[int]
    ) -> Optional[List[int]]:
        self._check("copy_from")
        if not (
            self.server_copy
            and isinstance(source, FakeStore)
            and source.backend_name == self.backend_name
        ):
            return None
        return [
            self._add(document=source.document_of(mid), name=source.messages[mid].name)
            for mid in message_ids
        ]

    async def delete_messages(
        self, message_ids: Iterable[int], force: bool = False
    ) -> None:
        self._check("delete_messages")
        if not force and not self.deleted_on_remove:
            return
        for mid in message_ids:
            self.messages.pop(mid, None)

    async def pin_message(self, message_id: int) -> None:
        self._check("pin_message")
        if message_id not in self.messages:
            raise MessageNotFound(message_id=message_id)
        self.pinned = message_id

    async def get_pinned_message(self) -> MessageRespWithDocument:
        self._check("get_pinned_message")
        if self.pinned is None or self.pinned not in self.messages:
            raise NoPinnedMessage()
        resp = self.messages[self.pinned].to_resp()
        assert resp.document is not None
        return MessageRespWithDocument(
            message_id=resp.message_id, text=resp.text, document=resp.document
        )
