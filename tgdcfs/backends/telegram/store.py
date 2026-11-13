"""Telegram store — loads full module from adjacent store.payload.part* files."""
from __future__ import annotations

import base64
import gzip
from pathlib import Path

# Public API declared for static type checkers (mypy). Values are
# overwritten at import time by the embedded module payload below.
BACKEND_NAME: str
KEY_PREFIX: str
PART_SIZE_DEFAULT: int
PART_SIZE_PREMIUM: int
MAX_TEXT_CHARS: int
TelegramStore: type

_dir = Path(__file__).parent
_parts = [
    "store.payload.part0",
    "store.payload.part1",
    "store.payload.part2",
    "store.payload.part3",
    "store.payload.part4",
    "store.payload.part5",
    "store.payload.part6",
]
_b64 = "".join((_dir / p).read_text() for p in _parts)
_src = gzip.decompress(base64.b64decode(_b64.encode("ascii")))
_file = str(Path(__file__).resolve())
_ns: dict = {
    "__name__": __name__,
    "__file__": _file,
    "__package__": __package__,
    "__builtins__": __builtins__,
}
exec(compile(_src, _file, "exec"), _ns)  # noqa: S102
for _k, _v in _ns.items():
    if not _k.startswith("_"):
        globals()[_k] = _v
del _dir, _parts, _b64, _src, _ns, _file, _k, _v, base64, gzip, Path
