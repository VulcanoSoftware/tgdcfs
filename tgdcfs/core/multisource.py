"""Multi-source reads: several stores share one download (plan, 4.11).

A version that is complete in more than one store can be read from all
of them at once. The requested range is cut into pieces; every store
runs a few workers that take the next piece not yet claimed, so a fast
store simply takes more pieces than a slow one, without anyone
estimating throughput. Output stays in order through a bounded reorder
window, which also bounds the memory a read can hold.

A piece that fails on one store is re-queued for the others; a store
that fails several pieces in a row is benched for the rest of the read.
The read fails only when no store can serve a piece.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Dict, Generator, List, Optional, Set

from tgdcfs.backends.base import IStore
from tgdcfs.errors import TechnicalError

logger = logging.getLogger(__name__)

# Consecutive failures after which a store stops taking pieces of a read.
BENCH_AFTER = 2
# Workers per store when the store does not say how many it can keep busy.
DEFAULT_SLOTS = 2

# Maps ``[begin, end]`` of a version onto ``(part index, message id,
# begin within the part, end within the part)`` for one store's layout.
RangeMapper = Callable[
    [List[int], List[int], int, str, int, int], Generator[tuple[int, int, int, int], None, None]
]


@dataclass
class StoreView:
    """One store's complete copy of a version, with the store's layout."""

    store: IStore
    message_ids: List[int]
    part_sizes: List[int]
    slots: int = DEFAULT_SLOTS

    @property
    def key(self) -> str:
        return self.store.key


@dataclass
class _Piece:
    index: int
    begin: int
    end: int
    tried: Set[str] = field(default_factory=set)


def read_slots(store: IStore) -> int:
    """How many pieces a store keeps in flight; a store may say so itself."""
    slots = getattr(store, "read_slots", None)
    try:
        return max(1, int(slots)) if slots is not None else DEFAULT_SLOTS
    except (TypeError, ValueError):
        return DEFAULT_SLOTS


class MultiSourceRead:
    """Scheduler for one read of ``[begin, end]`` over several stores."""

    def __init__(
        self,
        views: List[StoreView],
        size: int,
        version_id: str,
        begin: int,
        end: int,
        piece_size: int,
        window: int,
        map_range: RangeMapper,
        name: str = "",
    ):
        if not views:
            raise TechnicalError(f"No store to read {name}@{version_id} from")
        self._views = views
        self._size = size
        self._version_id = version_id
        self._map_range = map_range
        self._name = name or version_id
        self._pieces = [
            _Piece(i, b, min(b + piece_size - 1, end))
            for i, b in enumerate(range(begin, end + 1, max(1, piece_size)))
        ]
        self._window = max(1, window)
        self._results: Dict[int, bytes] = {}
        self._next_to_emit = 0
        # Pieces not claimed yet, in order; a failed piece returns here.
        self._pending: List[_Piece] = list(self._pieces)
        self._failures: Dict[str, int] = {v.key: 0 for v in views}
        self._benched: Set[str] = set()
        self._fatal: Optional[BaseException] = None
        self._changed = asyncio.Condition()

    # -- worker side ---------------------------------------------------------

    def _claim(self, view: StoreView) -> Optional[_Piece]:
        """The next piece this store may take: inside the window and not
        yet tried on it."""
        limit = self._next_to_emit + self._window
        for i, piece in enumerate(self._pending):
            if piece.index >= limit:
                return None
            if view.key not in piece.tried:
                return self._pending.pop(i)
        return None

    async def _fetch(self, view: StoreView, piece: _Piece) -> bytes:
        out = bytearray()
        for _, mid, part_begin, part_end in self._map_range(
            view.message_ids,
            view.part_sizes,
            self._size,
            self._version_id,
            piece.begin,
            piece.end,
        ):
            resp = await view.store.download_file(mid, part_begin, part_end)
            async for chunk in resp.chunks:
                out.extend(chunk)
        expected = piece.end - piece.begin + 1
        if len(out) != expected:
            raise TechnicalError(
                f"{view.key} returned {len(out)} bytes for piece {piece.index} "
                f"({expected} expected)"
            )
        return bytes(out)

    async def _worker(self, view: StoreView) -> None:
        while True:
            async with self._changed:
                while True:
                    if self._fatal is not None or self._next_to_emit >= len(
                        self._pieces
                    ):
                        return
                    if view.key in self._benched:
                        return
                    piece = self._claim(view)
                    if piece is not None:
                        break
                    if not self._pending and not any(
                        p.index >= self._next_to_emit for p in self._pieces
                    ):
                        return
                    await self._changed.wait()
            try:
                data = await self._fetch(view, piece)
            except Exception as ex:
                async with self._changed:
                    piece.tried.add(view.key)
                    self._failures[view.key] += 1
                    logger.warning(
                        f"Piece {piece.index} of {self._name} failed on {view.key}: {ex}"
                    )
                    if self._failures[view.key] >= BENCH_AFTER:
                        self._benched.add(view.key)
                        logger.warning(
                            f"{view.key} is benched for the rest of the read of "
                            f"{self._name} after {self._failures[view.key]} failures"
                        )
                    remaining = [
                        v
                        for v in self._views
                        if v.key not in piece.tried and v.key not in self._benched
                    ]
                    if not remaining:
                        self._fatal = TechnicalError(
                            f"No store can serve bytes {piece.begin}-{piece.end} of "
                            f"{self._name}: {ex}"
                        )
                    else:
                        self._pending.append(piece)
                        self._pending.sort(key=lambda p: p.index)
                    self._changed.notify_all()
                if view.key in self._benched:
                    return
                continue
            async with self._changed:
                self._failures[view.key] = 0
                self._results[piece.index] = data
                self._changed.notify_all()

    # -- consumer side -------------------------------------------------------

    async def stream(self) -> AsyncIterator[bytes]:
        workers = [
            asyncio.create_task(self._worker(view))
            for view in self._views
            for _ in range(max(1, view.slots))
        ]
        try:
            while self._next_to_emit < len(self._pieces):
                async with self._changed:
                    while (
                        self._next_to_emit not in self._results and self._fatal is None
                    ):
                        if all(w.done() for w in workers) and not self._results:
                            raise TechnicalError(
                                f"Every store gave up on {self._name} at piece "
                                f"{self._next_to_emit}"
                            )
                        await self._changed.wait()
                    if self._fatal is not None:
                        raise self._fatal
                    data = self._results.pop(self._next_to_emit)
                    self._next_to_emit += 1
                    self._changed.notify_all()
                yield data
        finally:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    @property
    def benched(self) -> Set[str]:
        return set(self._benched)
