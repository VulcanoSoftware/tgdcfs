"""WebDAV PROPFIND served from the metadata (``listing.stat_in_metadata``).

A folder listing used to read the descriptor of every file on every
PROPFIND. With the stats in the metadata the first listing reads them once
and every later one reads none.
"""

from typing import Dict, cast

import pytest
from fastapi.testclient import TestClient
from lxml import etree as et

from tests.tgdcfs.core.test_stat_in_metadata import StatClient
from tgdcfs.app.webdav import create_webdav_app
from tgdcfs.core.api import stat as stat_module
from tgdcfs.core.client import Client
from tgdcfs.core.ops import Ops

DAV = "{DAV:}"


@pytest.fixture(autouse=True)
def no_flush_delay(monkeypatch):
    for name in ("FLUSH_QUIET", "FLUSH_MAX_DELAY", "URGENT_QUIET", "URGENT_MAX_DELAY"):
        monkeypatch.setattr(stat_module, name, 0.0)


def sizes(resp) -> Dict[str, str]:
    root = et.fromstring(resp.content)
    return {
        (r.findtext(f"{DAV}href") or ""): (
            r.findtext(f".//{DAV}getcontentlength") or ""
        )
        for r in root.iter(f"{DAV}response")
    }


@pytest.mark.parametrize("persist", [True, False])
def test_listing_reports_sizes(persist):
    client = StatClient(persist=persist)
    ops = Ops(cast(Client, client))
    app = create_webdav_app(cast(Dict[str, Client], {"test": client}), "/webdav")

    with TestClient(app) as http:
        # Uploads go through the app's loop like the listings.
        http.put("/test/a.txt", content=b"x" * 12)
        http.put("/test/b.txt", content=b"y" * 34)

        first = http.request("PROPFIND", "/test/", headers={"Depth": "1"})
        reads = client.fd_repo.reads
        second = http.request("PROPFIND", "/test/", headers={"Depth": "1"})

    assert first.status_code == 207
    assert sizes(first)["/webdav/test/a.txt"] == "12"
    assert sizes(first)["/webdav/test/b.txt"] == "34"
    assert sizes(second) == sizes(first)
    if persist:
        assert client.fd_repo.reads == reads
        assert ops.stat_file("/a.txt").stat is not None
    else:
        assert client.fd_repo.reads == reads + 2
