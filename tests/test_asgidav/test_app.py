import logging
from http import HTTPStatus

import pytest

from asgidav.app import create_app, extract_path_from_destination, split_path


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


class TestPutOverwrite:
    def test_put_new_and_overwrite(self):
        from fastapi.testclient import TestClient
        from asgidav.folder import Folder
        from asgidav.resource import Resource

        class MemoryResource(Resource):
            def __init__(self, name):
                super().__init__(f"/{name}")
                self._data = b""

            async def overwrite(self, content, size):
                chunks = []
                async for chunk in content:
                    chunks.append(chunk)
                self._data = b"".join(chunks)

            async def content_length(self):
                return len(self._data)

            async def content_type(self):
                return "text/plain"

            async def last_modified(self):
                return 0

            async def creation_date(self):
                return 0

            async def display_name(self):
                return "file.txt"

            async def get_content(self, begin=0, end=-1):
                async def gen():
                    yield self._data
                return gen()

            async def remove(self):
                pass

            async def copy_to(self, dest):
                pass

            async def move_to(self, dest):
                pass

        class MemoryFolder(Folder):
            def __init__(self):
                super().__init__("/")
                self.members = {}

            async def display_name(self):
                return "root"

            async def member_names(self):
                return tuple(self.members.keys())

            async def member(self, path):
                clean = path.strip("/")
                if clean == "":
                    return self
                return self.members.get(clean)

            async def create_empty_resource(self, path):
                clean = path.strip("/")
                res = MemoryResource(clean)
                self.members[clean] = res
                return res

            async def create_folder(self, name):
                pass

            async def creation_date(self):
                return 0

            async def last_modified(self):
                return 0

            async def remove(self):
                pass

            async def copy_to(self, dest):
                pass

            async def move_to(self, dest):
                pass

        root_folder = MemoryFolder()

        async def get_member(path):
            clean = path.strip("/")
            if clean == "":
                return root_folder
            return await root_folder.member(clean)

        app = create_app(get_member)
        client = TestClient(app)

        # PUT new file -> 201 Created
        resp1 = client.put("/file.txt", content=b"hello", headers={"Content-Length": "5"})
        assert resp1.status_code == 201

        # PUT existing file -> 204 No Content
        resp2 = client.put("/file.txt", content=b"world!", headers={"Content-Length": "6"})
        assert resp2.status_code == 204
