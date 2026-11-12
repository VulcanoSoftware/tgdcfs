"""Encryption of streams whose length is unknown until they end."""

import pytest

from tgdcfs.crypto.header import HEADER_SIZE, FileHeader
from tgdcfs.crypto.kdf import derive_file_key
from tgdcfs.crypto.stream import EncryptingFileMessage
from tgdcfs.reqres import FileMessageFromStream

MASTER_KEY = b"\x01" * 32
CHUNK_SIZE = 64 * 1024


async def stream_of(chunks):
    for chunk in chunks:
        yield chunk


async def read_all(msg, chunk_size=1024) -> bytes:
    await msg.open()
    out = bytearray()
    while chunk := await msg.read(chunk_size):
        out.extend(chunk)
    await msg.close()
    return bytes(out)


def wrap(plaintext_chunks, header=None, file_key=None):
    inner = FileMessageFromStream.new(
        stream=stream_of(plaintext_chunks), size=-1, name="live.bin"
    )
    if header is None:
        header = FileHeader.new(chunk_size=CHUNK_SIZE)
        file_key = derive_file_key(MASTER_KEY, header.file_salt)
    return EncryptingFileMessage.wrap(inner, file_key, header), inner


class TestEncryptingUnknownSize:
    @pytest.mark.asyncio
    async def test_unknown_size_stays_unknown_until_the_stream_ends(self):
        msg, inner = wrap([b"hello ", b"world"])
        assert msg.get_size() == -1

        ciphertext = await read_all(msg)

        assert msg.get_size() == len(ciphertext)
        assert inner.get_size() == 11

    @pytest.mark.asyncio
    async def test_ciphertext_matches_the_known_size_encryption(self):
        plaintext = b"suspect nothing" * 5000
        header = FileHeader.new(chunk_size=CHUNK_SIZE)
        file_key = derive_file_key(MASTER_KEY, header.file_salt)
        known = FileMessageFromStream.new(
            stream=stream_of([plaintext]), size=len(plaintext)
        )
        known_msg = EncryptingFileMessage.wrap(known, file_key, header)

        unknown_msg, _ = wrap(
            [plaintext[:1234], plaintext[1234:]], header=header, file_key=file_key
        )

        assert await read_all(known_msg) == await read_all(unknown_msg)

    @pytest.mark.asyncio
    async def test_ciphertext_starts_with_the_header(self):
        msg, _ = wrap([b"data"])

        ciphertext = await read_all(msg)

        assert len(ciphertext) > HEADER_SIZE
