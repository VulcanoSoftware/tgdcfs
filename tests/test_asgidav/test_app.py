import logging
from http import HTTPStatus

import pytest

from asgidav.app import create_app, extract_path_from_destination, split_path

from .common import MockResource


class TestAppHelpers:
    def test_split_path_root(self):
        parent, name = split_path("/")
        assert parent == "/"
        assert name == ""

    def test_split_path_single_level(self):
        parent, name = split_path("/test")
        assert parent == "/"
        assert name == "test"

    def test_split_path_multiple_levels(self):
        parent, name = split_path("/path/to/file.txt")
        assert parent == "path/to"
        assert name == "file.txt"

    def test_split_path_trailing_slash(self):
        parent, name = split_path("/path/to/folder/")
        assert parent == "path/to"
        assert name == "folder"

    def test_extract_path_from_destination_http(self):
        url = "http://example.com/webdav/path/to/file.txt"
        result = extract_path_from_destination(url)
        assert result == "/webdav/path/to/file.txt"

    def test_extract_path_from_destination_https(self):
        url = "https://example.com/webdav/path/to/file.txt"
        result = extract_path_from_destination(url)
        assert result == "/webdav/path/to/file.txt"

    def test_extract_path_from_destination_path_only(self):
        path = "/webdav/path/to/file.txt"
        result = extract_path_from_destination(path)
        assert result == "/webdav/path/to/file.txt"

    def test_extract_path_from_destination_encoded(self):
        path = "/webdav/path%20with%20spaces/file.txt"
        result = extract_path_from_destination(path)
        assert result == "/webdav/path with spaces/file.txt"


class TestClientDisconnect:
    @pytest.mark.asyncio
    async def test_hang_up_during_body_is_not_a_server_error(self, caplog):
        async def get_member(path):
            return None

        app = create_app(get_member)
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "PROPFIND",
            "scheme": "http",
            "path": "/folder/",
            "raw_path": b"/folder/",
            "root_path": "",
            "query_string": b"",
            "headers": [(b"depth", b"1"), (b"content-length", b"100")],
            "client": ("127.0.0.1", 1234),
            "server": ("127.0.0.1", 80),
        }

        async def receive():
            return {"type": "http.disconnect"}

        sent = []

        async def send(message):
            sent.append(message)

        with caplog.at_level(logging.DEBUG, logger="asgidav.app"):
            await app(scope, receive, send)

        assert sent[0]["status"] == HTTPStatus.BAD_REQUEST
        assert "Client disconnected during PROPFIND /folder/" in caplog.text
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    @pytest.mark.asyncio
    async def test_hang_up_during_put_is_logged_as_an_aborted_upload(self, caplog):
        class ReadingResource(MockResource):
            async def overwrite(self, content, size):
                async for _ in content:
                    pass

        async def get_member(path):
            return ReadingResource(path)

        app = create_app(get_member)
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "PUT",
            "scheme": "http",
            "path": "/big.bin",
            "raw_path": b"/big.bin",
            "root_path": "",
            "query_string": b"",
            "headers": [(b"content-length", b"100")],
            "client": ("127.0.0.1", 1234),
            "server": ("127.0.0.1", 80),
        }
        chunks = [
            {"type": "http.request", "body": b"x" * 40, "more_body": True},
            {"type": "http.disconnect"},
        ]

        async def receive():
            return chunks.pop(0) if len(chunks) > 1 else chunks[0]

        sent = []

        async def send(message):
            sent.append(message)

        with caplog.at_level(logging.DEBUG, logger="asgidav.app"):
            await app(scope, receive, send)

        assert sent[0]["status"] == HTTPStatus.BAD_REQUEST
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "Client disconnected during PUT /big.bin" in warnings[0].message
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
