from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Generic, Iterable, List, Optional, Tuple, TypeVar

from lru import LRU  # type: ignore

from tgdcfs.reqres import MessageResp

K = TypeVar("K")
V = TypeVar("V")

DEFAULT_SIZE = 1024

# Entries per channel of the message-id caches; ``listing.message_cache_size``
# in the config. A listing reads the descriptor of every file in the folder,
# so a folder with more files than this evicts its own descriptors before
# the next listing could reuse them.
_id_cache_size = DEFAULT_SIZE


class MessageCache(Generic[K, V]):
    def __init__(self, size: int = DEFAULT_SIZE):
        self._lru = LRU(size)  # type: LRU[K, V]

    def set_size(self, size: int) -> None:
        self._lru.set_size(size)

    def get(self, key: K) -> Optional[V]:
        return self._lru.get(key)

    def __setitem__(self, key: K, value: V) -> None:
        self._lru[key] = value

    def __getitem__(self, key: K) -> V:
        return self._lru[key]

    def __contains__(self, key: K) -> bool:
        return key in self._lru

    def gets(self, keys: Iterable[K]) -> List[Optional[V]]:
        return [self.get(key) for key in keys]

    def find_nonexistent(self, keys: Iterable[K]) -> List[K]:
        return [key for key in keys if self.get(key) is None]


@dataclass
class ChannelMessageCache:
    id: MessageCache[int, Optional[MessageResp]]
    search: MessageCache[str, Tuple[MessageResp, ...]]


global_message_cache: DefaultDict[int, ChannelMessageCache] = defaultdict(
    lambda: ChannelMessageCache(
        id=MessageCache[int, Optional[MessageResp]](_id_cache_size),
        search=MessageCache[str, Tuple[MessageResp, ...]](),
    )
)


def channel_cache(channel_id: int) -> ChannelMessageCache:
    return global_message_cache[channel_id]


def set_message_cache_size(size: int) -> None:
    """Resize the message-id cache of every channel, present and future."""
    global _id_cache_size
    if size < 1:
        raise ValueError(f"message cache size must be at least 1, got {size}")
    _id_cache_size = size
    for cache in global_message_cache.values():
        cache.id.set_size(size)
