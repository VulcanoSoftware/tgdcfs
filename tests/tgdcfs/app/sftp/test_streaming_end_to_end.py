"""A real SFTP client against the real stack on in-memory stores.

The segments are the whole point, so this goes through everything: the SSH
channel, the write handle, ``Ops``, the content repository and the store.
Nothing in between is a mock, apart from the task list.
"""

import asyncio
from typing import cast

import asyncssh
import pytest
from tests.fakes.store import FakeStore
from tests.tgdcfs.app.sftp.test_integration import _authenticate, connect
from tests.tgdcfs.core.test_store_integration import Stack
from tgdcfs.app.sftp.server import make_server_factory, make_sftp_factory
from tgdcfs.backends.base import IStore
from tgdcfs.config import SFTPConfig
from tgdcfs.core.client import Client

MB = 1024 * 1024
PAYLOAD = bytes((i * 31 + 7) % 251 for i in range(3 * MB + MB // 2))  # 3.5 MB


@pytest.fixture(params=["streaming_store", "segmenting_store"])
async def stack(request) -> Stack:
    # A store that streams (Telegram) keeps the file in one document; one
    # that does not (Discord) is given segments of ``stream_segment_mb``.
    streams = request.param == "streaming_store"
    return await Stack(
        FakeStore(key="tg:1", backend_name="telegram", streams=streams),
        FakeStore(key="tg:2", backend_name="telegram"),
    ).init()


def expected_parts(stack: Stack, size: int) -> list[int]:
    if stack.primary.streams:
        return [size]
    whole, rest = divmod(size, MB)
    return [MB] * whole + ([rest] if rest else [])


@pytest.fixture
def users(mocker):
    return {"admin": mocker.Mock(password="secret", readonly=False)}


@pytest.fixture
async def server(mocker, stack, users):
    mocker.patch("tgdcfs.core.ops.create_upload_task", new_callable=mocker.AsyncMock)
    mocker.patch(
        "tgdcfs.app.sftp.server.authenticate",
        side_effect=lambda username, password: _authenticate(users, username, password),
    )
    client = Client(
        name="notes",
        store=cast(IStore, stack.primary),
        file_api=stack.file_api,
        dir_api=stack.dir_api,
        fc_repo=stack.fc_repo,
        fd_repo=stack.fd_repo,
        metadata_api=stack.metadata_api,
        mirror_group=stack.group,
    )
    config = mocker.Mock()
    config.tgdcfs.users = users
    config.tgdcfs.sftp = SFTPConfig.from_dict(
        {"upload_buffer_size_mb": 0, "stream_segment_mb": 1}
    )
    acceptor = await asyncssh.listen(
        host="127.0.0.1",
        port=0,
        server_factory=make_server_factory(config),
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        sftp_factory=make_sftp_factory({"notes": client}, config),
    )
    try:
        yield acceptor
    finally:
        acceptor.close()
        await acceptor.wait_closed()


def stored_parts(stack: Stack, name: str) -> list[bytes]:
    parts = {
        m.name: m.document
        for m in stack.primary.messages.values()
        if m.document is not None and m.name.endswith(name)
    }
    return [parts[key] for key in sorted(parts)]


async def test_a_large_upload_arrives_intact_in_segments(server, stack, tmp_path):
    source = tmp_path / "big.bin"
    source.write_bytes(PAYLOAD)

    async with await connect(server) as conn, conn.start_sftp_client() as sftp:
        await sftp.put(str(source), "/notes/big.bin")

        back = tmp_path / "back.bin"
        await sftp.get("/notes/big.bin", str(back))
        assert (await sftp.stat("/notes/big.bin")).size == len(PAYLOAD)

    assert back.read_bytes() == PAYLOAD
    assert [len(p) for p in stored_parts(stack, "big.bin")] == expected_parts(
        stack, len(PAYLOAD)
    )
    assert await stack.read("big.bin") == PAYLOAD


async def test_the_stores_receive_segments_before_the_client_is_done(server, stack):
    async with await connect(server) as conn, conn.start_sftp_client() as sftp:
        async with sftp.open("/notes/live.bin", "wb") as remote:
            await remote.write(PAYLOAD[: 2 * MB + MB // 4])

            async def store_has_it() -> None:
                # A segmenting store has a message per finished segment; a
                # streaming one only makes the message when the document is
                # complete, but takes the bytes as they come.
                while not (
                    stack.primary.streamed >= MB
                    if stack.primary.streams
                    else stored_parts(stack, "live.bin")
                ):
                    await asyncio.sleep(0.01)

            # The client has not closed the file, yet the store has bytes.
            await asyncio.wait_for(store_has_it(), timeout=10)
            if not stack.primary.streams:
                assert stored_parts(stack, "live.bin")[0] == PAYLOAD[:MB]

            await remote.write(PAYLOAD[2 * MB + MB // 4 :], 2 * MB + MB // 4)

    assert await stack.read("live.bin") == PAYLOAD


async def test_a_small_file_is_stored_as_a_single_part(server, stack, tmp_path):
    source = tmp_path / "small.bin"
    source.write_bytes(PAYLOAD[:1000])

    async with await connect(server) as conn, conn.start_sftp_client() as sftp:
        await sftp.put(str(source), "/notes/small.bin")

    assert [len(p) for p in stored_parts(stack, "small.bin")] == [1000]
    assert await stack.read("small.bin") == PAYLOAD[:1000]


async def test_overwriting_a_large_file_replaces_it(server, stack, tmp_path):
    first, second = tmp_path / "first.bin", tmp_path / "second.bin"
    first.write_bytes(PAYLOAD)
    second.write_bytes(PAYLOAD[::-1][: 2 * MB + 5])

    async with await connect(server) as conn, conn.start_sftp_client() as sftp:
        await sftp.put(str(first), "/notes/f.bin")
        await sftp.put(str(second), "/notes/f.bin")
        assert (await sftp.stat("/notes/f.bin")).size == 2 * MB + 5

    assert await stack.read("f.bin") == PAYLOAD[::-1][: 2 * MB + 5]
