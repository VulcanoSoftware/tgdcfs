"""Write-behind uploads driven through a real SSH/SFTP client.

The point of ``sftp.write_behind`` is that the client's CLOSE is answered
before Telegram has the file, so these tests hold the store's upload back
and check what a client can and cannot see in the meantime.
"""

import asyncio
from typing import Any, cast

import asyncssh
import pytest
from tgdcfs.app.sftp.pending import PendingUploads
from tgdcfs.app.sftp.server import make_server_factory, make_sftp_factory

from tests.tgdcfs.app.sftp.test_integration import (
    FakeChannel,
    _authenticate,
    build_client,
    build_config,
    connect,
)

PAYLOAD = bytes(range(256)) * 400  # 100 KiB


@pytest.fixture
def channel():
    channel = FakeChannel()
    channel.root.create_dir("sub")
    return channel


@pytest.fixture
def users(mocker):
    return {"admin": mocker.Mock(password="secret", readonly=False)}


@pytest.fixture
def gate():
    """Closed until a test lets the stores finish their uploads."""
    return asyncio.Event()


@pytest.fixture
def pending():
    return PendingUploads(max_bytes=0)


@pytest.fixture
def store_fails():
    """Flip ``["on"]`` to make the stores refuse the held-back upload."""
    return {"on": False}


@pytest.fixture
async def server(mocker, channel, users, gate, pending, store_fails):
    mocker.patch("tgdcfs.core.ops.create_upload_task", new_callable=mocker.AsyncMock)
    mocker.patch(
        "tgdcfs.app.sftp.server.authenticate",
        side_effect=lambda username, password: _authenticate(users, username, password),
    )

    client = build_client(mocker, "notes", channel)

    # Hold back the real uploads, but let the placeholder (``touch``) through.
    original_upload = cast(Any, client.file_api.upload).side_effect

    async def slow_upload(directory, file_msg):
        if file_msg.size:
            await gate.wait()
            if store_fails["on"]:
                raise RuntimeError("telegram said no")
        return await original_upload(directory, file_msg)

    cast(Any, client.file_api.upload).side_effect = slow_upload

    clients = {"notes": client}
    config = build_config(mocker, users)

    acceptor = await asyncssh.listen(
        host="127.0.0.1",
        port=0,
        server_factory=make_server_factory(config),
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        sftp_factory=make_sftp_factory(clients, config, pending),
    )
    try:
        yield acceptor
    finally:
        gate.set()
        await pending.drain()
        acceptor.close()
        await acceptor.wait_closed()


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "source.bin"
    path.write_bytes(PAYLOAD)
    return path


class TestWriteBehind:
    async def test_the_upload_is_answered_before_the_stores_have_it(
        self, server, channel, pending, source
    ):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            # Would hang until the gate opens if close waited for the store.
            await asyncio.wait_for(sftp.put(str(source), "/notes/up.bin"), timeout=5)

            assert pending.get("notes", "/up.bin") is not None
            assert channel.contents.get("up.bin", b"") == b""

    async def test_stat_and_listing_already_show_the_new_size(
        self, server, pending, source
    ):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/up.bin")

            assert (await sftp.stat("/notes/up.bin")).size == len(PAYLOAD)
            listing = {e.filename: e for e in await sftp.readdir("/notes")}
            assert listing["up.bin"].attrs.size == len(PAYLOAD)

    async def test_the_file_can_be_read_back_straight_away(
        self, server, tmp_path, source
    ):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/up.bin")

            back = tmp_path / "back.bin"
            await asyncio.wait_for(sftp.get("/notes/up.bin", str(back)), timeout=5)

            assert back.read_bytes() == PAYLOAD

    async def test_the_stores_receive_the_payload_in_the_end(
        self, server, channel, pending, gate, source
    ):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/up.bin")

        gate.set()
        await pending.drain()

        assert channel.contents["up.bin"] == PAYLOAD
        assert pending.get("notes", "/up.bin") is None
        assert pending.held_bytes == 0

    async def test_the_session_can_end_while_the_upload_goes_on(
        self, server, channel, pending, gate, source
    ):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/up.bin")
        # The connection is gone; the data must not go with it.
        assert pending.get("notes", "/up.bin") is not None

        gate.set()
        await pending.drain()

        assert channel.contents["up.bin"] == PAYLOAD

    async def test_removing_the_file_waits_for_its_upload(
        self, server, channel, pending, gate, source
    ):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/up.bin")

            removal = asyncio.create_task(sftp.remove("/notes/up.bin"))
            await asyncio.sleep(0.1)
            assert not removal.done()

            gate.set()
            await asyncio.wait_for(removal, timeout=5)

        # The upload landed first and the removal came after it, so the
        # file does not come back.
        assert "up.bin" not in channel.contents

    async def test_renaming_the_file_waits_for_its_upload(
        self, server, channel, pending, gate, source, mocker
    ):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/up.bin")

            rename = asyncio.create_task(sftp.rename("/notes/up.bin", "/notes/new.bin"))
            await asyncio.sleep(0.1)
            assert not rename.done()

            gate.set()
            await asyncio.wait_for(rename, timeout=5)

        assert channel.contents["up.bin"] == PAYLOAD

    async def test_a_failing_background_upload_does_not_hurt_the_session(
        self, server, channel, pending, gate, store_fails, source, caplog
    ):
        store_fails["on"] = True
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/up.bin")

            gate.set()
            await pending.drain()

            assert pending.get("notes", "/up.bin") is None
            assert pending.held_bytes == 0
            assert "telegram said no" in caplog.text
            assert channel.contents.get("up.bin", b"") == b""
            # The session is still perfectly usable.
            assert await sftp.listdir("/notes")


class TestWriteBehindOff:
    async def test_without_a_registry_close_waits_for_the_store(
        self, mocker, channel, users, gate, source
    ):
        mocker.patch(
            "tgdcfs.core.ops.create_upload_task", new_callable=mocker.AsyncMock
        )
        mocker.patch(
            "tgdcfs.app.sftp.server.authenticate",
            side_effect=lambda u, p: _authenticate(users, u, p),
        )
        client = build_client(mocker, "notes", channel)
        original_upload = cast(Any, client.file_api.upload).side_effect

        async def slow_upload(directory, file_msg):
            if file_msg.size:
                await gate.wait()
            return await original_upload(directory, file_msg)

        cast(Any, client.file_api.upload).side_effect = slow_upload
        config = build_config(mocker, users)
        acceptor = await asyncssh.listen(
            host="127.0.0.1",
            port=0,
            server_factory=make_server_factory(config),
            server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
            sftp_factory=make_sftp_factory({"notes": client}, config),
        )
        try:
            async with await connect(acceptor) as conn:
                async with conn.start_sftp_client() as sftp:
                    put = asyncio.create_task(sftp.put(str(source), "/notes/up.bin"))
                    await asyncio.sleep(0.2)
                    assert not put.done()  # still waiting on the store, as before

                    gate.set()
                    await asyncio.wait_for(put, timeout=5)
                    assert channel.contents["up.bin"] == PAYLOAD
        finally:
            gate.set()
            acceptor.close()
            await acceptor.wait_closed()
