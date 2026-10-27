from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Generic, Iterable, List, Optional, Tuple, TypeVar

from lru import LRU  # type: ignore

from tgdcfs.reqres import MessageResp

K = TypeVar("K")
V = TypeVar("V")


class MessageCache(Generic[K, V]):
    def __init__(self, capacity: int = 1024):
        self._lru = LRU(capacity)  # type: LRU[K, V]

    def set_capacity(self, capacity: int) -> None:
        self._lru.set_size(capacity)

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


def _create_channel_cache() -> ChannelMessageCache:
    from tgdcfs.config import get_config

    capacity = 10000
    try:
        capacity = get_config().tgdcfs.transfer.message_cache_size
    except Exception:
        pass
    return ChannelMessageCache(
        id=MessageCache[int, Optional[MessageResp]](capacity=capacity),
        search=MessageCache[str, Tuple[MessageResp, ...]](capacity=capacity),
    )


global_message_cache: DefaultDict[int, ChannelMessageCache] = defaultdict(
    _create_channel_cache
)


def channel_cache(channel_id: int) -> ChannelMessageCache:
    cache = global_message_cache[channel_id]
    try:
        from tgdcfs.config import get_config

        capacity = get_config().tgdcfs.transfer.message_cache_size
        if cache.id._lru.get_size() != capacity:
            cache.id.set_capacity(capacity)
            cache.search.set_capacity(capacity)
    except Exception:
        pass
    return cache
