"""Uploads whose size is not known up front (what SFTP sends).

The stores need a part's size before its first byte, so such an upload is
cut into segments that are uploaded as parts of one version while the rest
is still arriving. These tests run it through the whole stack on in-memory
stores: the file has to come back byte for byte, from any range, with the
parts numbered consecutively.
"""

import asyncio
from typing import AsyncIterator, List, Optional, cast

import pytest
from tests.fakes.store import FakeStore
from tests.tgdcfs.core.test_store_integration import (
    Stack,
    cross_backend_stack,
    same_backend_stack,
)

from tgdcfs.backends.base import IStore
from tgdcfs.core.api import FileApi, FileDescApi
from tgdcfs.crypto.repository import EncryptingFileContentRepository
from tgdcfs.errors import TechnicalError
from tgdcfs.reqres import FileMessageFromSpool, FileMessageFromStream

SEGMENT = 1000


def payload(size: int) -> bytes:
    return bytes((i * 7 + 3) % 251 for i in range(size))


async def pieces(data: bytes, piece: int = 333) -> AsyncIterator[bytes]:
    for i in range(0, len(data), piece):
        yield data[i : i + piece]


async def put_unsized(
    stack: Stack, name: str, stream: AsyncIterator[bytes], segment: int = SEGMENT
):
    return await stack.file_api.upload(
        stack.root,
        FileMessageFromStream.new(
            stream=stream, size=None, name=name, segment_size=segment
        ),
    )


async def part_sizes(stack: Stack, name: str) -> List[int]:
    fd = await stack.fd_repo.get(stack.root.find_file(name))
    return list(fd.get_latest_version().part_sizes)


def documents(store: FakeStore) -> dict:
    return {i: m for i, m in store.messages.items() if m.document is not None}


@pytest.fixture(params=["same_backend", "cross_backend"])
async def stack(request) -> Stack:
    build = (
        same_backend_stack if request.param == "same_backend" else cross_backend_stack
    )
    return await build().init()


class TestRoundTrip:
    @pytest.mark.parametrize(
        ("size", "expected_parts"),
        [
            (3500, [1000, 1000, 1000, 500]),
            (3000, [1000, 1000, 1000]),  # an exact multiple has no empty tail
            (1000, [1000]),
            (999, [999]),
            (1, [1]),
        ],
    )
    async def test_the_file_comes_back_in_segments(
        self, stack: Stack, size: int, expected_parts: List[int]
    ):
        data = payload(size)

        await put_unsized(stack, "a.bin", pieces(data))

        assert await stack.read("a.bin") == data
        assert await part_sizes(stack, "a.bin") == expected_parts

    async def test_every_range_reads_across_the_segments(self, stack: Stack):
        data = payload(3500)
        await put_unsized(stack, "a.bin", pieces(data))

        for begin, end in [(0, 0), (990, 1010), (999, 1000), (2500, 3499), (1, 3498)]:
            assert await stack.read("a.bin", begin, end) == data[begin : end + 1]

    async def test_the_parts_are_numbered_consecutively(self, stack: Stack):
        await put_unsized(stack, "a.bin", pieces(payload(3500)))

        names = sorted(
            m.name
            for m in stack.primary.messages.values()
            if m.document is not None and m.name.endswith("a.bin")
        )
        assert names == [f"[part{i}]a.bin" for i in range(1, 5)]

    async def test_the_recorded_size_is_the_real_one(self, stack: Stack):
        await put_unsized(stack, "a.bin", pieces(payload(3500)))

        fd = await stack.fd_repo.get(stack.root.find_file("a.bin"))
        assert fd.get_latest_version().size == 3500

    async def test_the_mirror_gets_the_parts_too(self, stack: Stack):
        data = payload(3500)
        await put_unsized(stack, "a.bin", pieces(data))

        fd = await stack.fd_repo.get(stack.root.find_file("a.bin"))
        version = fd.get_latest_version()
        assert stack.mirror.key in version.mirrors
        assert len(version.mirrors[stack.mirror.key]) == len(version.message_ids)

    async def test_the_stream_may_come_in_any_piece_size(self, stack: Stack):
        data = payload(2345)

        await put_unsized(stack, "a.bin", pieces(data, piece=1))
        await put_unsized(stack, "b.bin", pieces(data, piece=5000))

        assert await stack.read("a.bin") == data
        assert await stack.read("b.bin") == data

    async def test_a_new_version_replaces_the_old_one(self, stack: Stack):
        await put_unsized(stack, "a.bin", pieces(payload(2500)))
        newer = payload(1800)

        await put_unsized(stack, "a.bin", pieces(newer))

        assert await stack.read("a.bin") == newer


class TestFailure:
    async def test_an_empty_stream_stores_nothing(self, stack: Stack):
        before = len(documents(stack.primary))

        with pytest.raises(TechnicalError, match="empty"):
            await put_unsized(stack, "a.bin", pieces(b""))

        assert len(documents(stack.primary)) == before

    async def test_a_stream_that_breaks_leaves_no_parts_behind(self, stack: Stack):
        stack.primary.deleted_on_remove = False  # the gate for removed files
        before = set(documents(stack.primary))

        async def breaks() -> AsyncIterator[bytes]:
            yield payload(2500)
            raise RuntimeError("client went away")

        with pytest.raises(RuntimeError, match="client went away"):
            await put_unsized(stack, "a.bin", breaks())

        # Two full segments had already gone out; they are deleted again.
        assert set(documents(stack.primary)) == before

    async def test_a_segment_size_is_required(self, stack: Stack):
        with pytest.raises(TechnicalError, match="segment size"):
            await put_unsized(stack, "a.bin", pieces(payload(10)), segment=0)


class TestStreamingBehaviour:
    async def test_the_first_segment_goes_out_before_the_stream_ends(self):
        """The point of it all: the upload keeps pace with the client."""
        stack = await same_backend_stack().init()
        first_segment_stored = asyncio.Event()
        original_add = stack.primary._add

        def spy(*args, **kwargs):
            mid = original_add(*args, **kwargs)
            if kwargs.get("name", "").startswith("[part1]a.bin"):
                first_segment_stored.set()
            return mid

        stack.primary._add = spy  # type: ignore[method-assign]
        seen_before_end: Optional[bool] = None

        async def slow() -> AsyncIterator[bytes]:
            nonlocal seen_before_end
            yield payload(1500)  # more than one segment
            await asyncio.wait_for(first_segment_stored.wait(), timeout=5)
            seen_before_end = first_segment_stored.is_set()
            yield payload(500)

        await put_unsized(stack, "a.bin", slow())

        assert seen_before_end is True
        assert await stack.read("a.bin") == payload(1500) + payload(500)


class TestMessageTypes:
    async def test_an_unsized_stream_has_no_size_until_it_ends(self):
        msg = FileMessageFromStream.new(
            stream=pieces(b""), size=None, name="x", segment_size=10
        )

        assert msg.unsized
        with pytest.raises(TechnicalError, match="not known"):
            msg.get_size()

    async def test_reads_are_short_only_at_the_end(self):
        data = payload(25)
        msg = FileMessageFromStream.new(
            stream=pieces(data, piece=4), size=None, name="x", segment_size=10
        )

        got = [await msg.read(10), await msg.read(10), await msg.read(10)]

        assert [len(g) for g in got] == [10, 10, 5]
        assert b"".join(got) == data
        assert await msg.read(10) == b""

    async def test_a_sized_stream_is_not_affected(self):
        data = payload(25)
        msg = FileMessageFromStream.new(stream=pieces(data, piece=4), size=25, name="x")

        assert not msg.unsized
        assert msg.get_size() == 25
        assert await msg.read(100) == data

    async def test_a_spool_segment_reads_back_what_was_written(self):
        import tempfile

        spool = tempfile.SpooledTemporaryFile(max_size=4)
        spool.write(b"0123456789")
        msg = FileMessageFromSpool.new(spool, 10, name="x", part_offset=3)

        await msg.open()
        assert msg.get_size() == 10
        assert msg.part_offset == 3
        assert await msg.read(4) == b"0123"
        assert await msg.read(100) == b"456789"
        await msg.close()
        assert spool.closed


class TestEncrypted:
    async def test_an_encrypted_unsized_upload_round_trips(self):
        stack = await same_backend_stack().init()
        encrypting = EncryptingFileContentRepository(
            stack.fc_repo,
            master_key=b"\x42" * 32,
            chunk_size=256,
            encrypt_names=False,
        )
        stack.file_api = FileApi(
            stack.metadata_api,
            FileDescApi(stack.fd_repo, encrypting),
            cast(IStore, stack.primary),
            mirror_group=stack.group,
        )
        data = payload(3000)

        await put_unsized(stack, "secret.bin", pieces(data))

        assert await stack.read("secret.bin") == data
        assert await stack.read("secret.bin", 700, 1700) == data[700:1701]
        # What the store holds is ciphertext, cut into segments.
        stored = b"".join(
            m.document
            for m in sorted(documents(stack.primary).values(), key=lambda m: m.name)
            if m.name.endswith("secret.bin")
        )
        assert data[:64] not in stored
