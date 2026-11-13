"""The store interface every backend implements.

A store is one channel of one backend. Everything above this layer --
the repositories, the mirror engine, the file API -- addresses content
by ``(store key, message id)`` and never sees a backend library.

Store keys
----------

The key is the string that mirror maps in the metadata are keyed by, so
it has to be stable for the life of the data and unique across
backends. It is ``<prefix>:<channel id as written in the config>``,
for example ``tg:-1001234567890``. Metadata written by tgfs carries bare
channel ids; those are read as Telegram keys (``normalize_store_key``)
and written back with the prefix from the first write on.
"""

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

from tgdcfs.reqres import (
    DownloadFileResp,
    MessageResp,
    MessageRespWithDocument,
    SentFileMessage,
    UploadableFileMessage,
)

# A serialization key without a prefix predates the backend abstraction
# and always meant Telegram.
DEFAULT_PREFIX = "tg"
KEY_SEPARATOR = ":"


def make_store_key(prefix: str, channel: str) -> str:
    return f"{prefix}{KEY_SEPARATOR}{channel}"


def parse_store_key(key: str) -> Tuple[str, str]:
    """Split a key into ``(prefix, channel)``, defaulting to Telegram."""
    prefix, sep, channel = key.partition(KEY_SEPARATOR)
    if not sep or not prefix.isalpha():
        # Not a prefixed key: a bare (possibly negative) Telegram channel id.
        return DEFAULT_PREFIX, key
    return prefix, channel


def normalize_store_key(key: str) -> str:
    prefix, channel = parse_store_key(str(key))
    return make_store_key(prefix, channel)


@dataclass(frozen=True)
class StoreCapabilities:
    """What a store can do, so callers can pick a strategy up front."""

    # Largest part a single message may carry. ``upload`` partitions
    # bigger files itself; the value is informative for callers.
    max_part_bytes: int
    # Longest text message the backend accepts.
    max_text_chars: int
    # Whether ``copy_from``/``copy_within`` can copy server-side, without
    # moving the bytes through this process.
    supports_server_copy: bool
    # Whether ``replace_document`` can swap the document of a message.
    supports_edit_media: bool = True
    # Whether ``delete_messages`` may batch ids into one request.
    supports_bulk_delete: bool = True
    # Messages older than this cannot be bulk deleted (Discord: 14 days).
    delete_age_limit_days: Optional[int] = None


class IStore(metaclass=ABCMeta):
    """One channel of one backend.

    Message ids are ints scoped to this store. Every method that takes
    ids expects ids of *this* store, except ``copy_from`` which takes
    ids of ``source``.
    """

    def __init__(self, key: str):
        self._key = key

    @property
    def key(self) -> str:
        """Serialization key, ``<prefix>:<channel>``; see the module docstring."""
        return self._key

    @property
    @abstractmethod
    def caps(self) -> StoreCapabilities:
        pass

    @property
    @abstractmethod
    def backend(self) -> str:
        """Backend name as used in the config (``telegram``, ``discord``)."""
        pass

    # -- text messages -----------------------------------------------------

    @abstractmethod
    async def send_text(self, message: str) -> int:
        pass

    @abstractmethod
    async def edit_message_text(self, message_id: int, message: str) -> int:
        """Edit a text message; raises ``MessageNotFound`` when it is gone."""
        pass

    @abstractmethod
    async def get_messages(self, ids: list[int]) -> list[Optional[MessageResp]]:
        """Fetch messages by id, ``None`` for each one that does not exist."""
        pass

    @abstractmethod
    async def search_messages(self, search: str) -> list[MessageResp]:
        pass

    # -- documents ---------------------------------------------------------

    @abstractmethod
    async def upload(self, file_msg: UploadableFileMessage) -> List[SentFileMessage]:
        """Store the bytes of ``file_msg``, partitioned as the backend needs.

        Returns one entry per part message, in file order. The store owns
        the partitioning: a caller never learns the part size, which is
        what lets a mirror in another backend re-partition the same bytes.
        """
        pass

    @property
    def streams_unsized_uploads(self) -> bool:
        """Whether ``upload`` takes a stream of unknown length as it is.

        A store that does not (Discord needs every attachment's size before
        the first byte) is given such a stream in segments of known size
        instead.
        """
        return False

    @property
    def read_slots(self) -> int:
        """Pieces of one multi-source read this store keeps in flight."""
        return 2

    def plan_parts(self, size: int) -> List[int]:
        """The part sizes ``upload`` would cut ``size`` bytes into.

        Lets a caller decide, before anything is uploaded, whether a mirror
        can hold the primary's parts one to one. The default cuts at the
        store's largest part; a store with a smarter rule overrides it.
        """
        part = self.caps.max_part_bytes
        if size <= 0:
            return [0]
        parts = (size + part - 1) // part
        return [part] * (parts - 1) + [size - (parts - 1) * part]

    @abstractmethod
    async def download_file(
        self, message_id: int, begin: int, end: int
    ) -> DownloadFileResp:
        """Stream the inclusive byte range ``[begin, end]`` of a document.

        ``end == -1`` means the end of the document.
        """
        pass

    @abstractmethod
    async def replace_document(self, message_id: int, buffer: bytes, name: str) -> int:
        """Swap the document of an existing message in place.

        Returns the id of the message that now carries the document.
        """
        pass

    # -- copies ------------------------------------------------------------

    @abstractmethod
    async def copy_within(self, message_ids: List[int]) -> List[int]:
        """Duplicate messages inside this store, server-side when possible.

        Returns the new ids aligned with ``message_ids``. A backend without
        server-side copies streams the bytes down and up again.
        """
        pass

    @abstractmethod
    async def copy_from(
        self, source: "IStore", message_ids: List[int]
    ) -> Optional[List[int]]:
        """Server-side copy of ``source``'s messages into this store.

        Returns the new ids aligned with ``message_ids``, or ``None`` when
        this pair of stores cannot copy server-side (different backends, or
        a backend without the primitive). A ``None`` is not an error: the
        caller falls back to re-uploading the bytes.
        """
        pass

    # -- bookkeeping -------------------------------------------------------

    @abstractmethod
    async def delete_messages(
        self, message_ids: Iterable[int], force: bool = False
    ) -> None:
        """Best-effort deletion, gated by the backend's ``delete_messages_on_remove``.

        ``force=True`` bypasses the gate for internal bookkeeping messages.
        """
        pass

    @abstractmethod
    async def pin_message(self, message_id: int) -> None:
        pass

    @abstractmethod
    async def get_pinned_message(self) -> MessageRespWithDocument:
        """The pinned metadata document; raises ``NoPinnedMessage`` when unset."""
        pass

    async def close(self) -> None:
        """Release backend resources; the default has nothing to release."""
        return
