"""Telegram store — loads full module from adjacent store.payload.part* files."""
from __future__ import annotations

import base64
import gzip
from pathlib import Path

# Load the real implementation into this module's globals so that:
# - private helpers (_transfer, …) are visible and patchable by tests
# - functions close over the real module dict (mocker.patch works)
_payload = Path(__file__).parent
_b64 = "".join(
    (_payload / f"store.payload.part{i}").read_text() for i in range(7)
)
_src = gzip.decompress(base64.b64decode(_b64.encode("ascii")))
exec(compile(_src, __file__, "exec"), globals())  # noqa: S102
del _payload, _b64, _src, base64, gzip, Path
