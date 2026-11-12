import datetime

import pytest

from tgdcfs.backends.telegram.store import TelegramStore
from tgdcfs.core.model import TGFSFileVersion
from tgdcfs.core.repository.impl.file_content import StoreFileContentRepository
from tgdcfs.errors import TechnicalError
from tgdcfs.reqres import SentFileMessage, UploadableFileMessage


class MockFileMessage(UploadableFileMessage):
    """Mock file message for testing"""

    def __init__(self, name: str, size: int, caption: str = ""):
        self.name = name
        self.size = size
        self.caption = caption
        self._offset = 0
        self._task_tracker = None

    def get_size(self) -> int:
        return self.size if self.size is not None else 0

    def next_part(self, size: int):
        self._offset += size

    def file_name(self) -> str:
        return self.name

    async def open(self):
        pass

    async def close(self):
        pass

    async def read(self, size: int) -> bytes:
        return b"mock_content"[:size]


# Global fixtures for all test classes
@pytest.fixture
def mock_message_api(mocker):
    """Mock store with the methods the repository delegates to"""
    api = mocker.Mock(spec=TelegramStore)
    api.key = "tg:123456789"
    api.upload = mocker.AsyncMock(
        return_value=[SentFileMessage(message_id=12345, size=1000)]
    )
    api.replace_document = mocker.AsyncMock(return_value=54321)
    api.download_file = mocker.AsyncMock()
    return api


@pytest.fixture
def repository(mock_message_api):
    """Create repository instance with mocked API"""
    return StoreFileContentRepository(mock_message_api)


@pytest.fixture
def sample_file_version():
    """Create a sample TGFSFileVersion for testing"""
    return TGFSFileVersion(
        id="test_version_123",
        updated_at=datetime.datetime.now(),
        _size=2000,
        message_ids=[1001, 1002],
        part_sizes=[1000, 1000],
    )


class TestStaticMethods:
    """Test static/private methods of StoreFileContentRepository"""

    def test_get_file_part_to_download_full_file(self, sample_file_version):
        """Test getting parts for downloading entire file"""
        parts = list(
            StoreFileContentRepository._get_file_part_to_download(
                sample_file_version, 0, -1
            )
        )

        assert len(parts) == 2
        assert parts[0] == (1001, 0, 999)  # First part, full
        assert parts[1] == (1002, 0, 999)  # Second part, full

    def test_get_file_part_to_download_partial_range(self, sample_file_version):
        """Test getting parts for partial file download"""
        parts = list(
            StoreFileContentRepository._get_file_part_to_download(
                sample_file_version, 500, 1500
            )
        )

        assert len(parts) == 2
        assert parts[0] == (1001, 500, 999)  # First part, from byte 500 to end
        assert parts[1] == (1002, 0, 500)  # Second part, from start to byte 500

    def test_get_file_part_to_download_single_part_range(self, sample_file_version):
        """Test getting parts for range within single part"""
        parts = list(
            StoreFileContentRepository._get_file_part_to_download(
                sample_file_version, 100, 900
            )
        )

        assert len(parts) == 1
        assert parts[0] == (1001, 100, 900)

    def test_get_file_part_to_download_empty_file(self):
        """Test getting parts for empty file"""
        empty_version = TGFSFileVersion(
            id="empty",
            updated_at=datetime.datetime.now(),
            _size=0,
            message_ids=[],
            part_sizes=[],
        )
        parts = list(
            StoreFileContentRepository._get_file_part_to_download(empty_version, 0, -1)
        )
        assert len(parts) == 0

    def test_get_file_part_to_download_invalid_begin(self, sample_file_version):
        """Test error handling for invalid begin offset"""
        with pytest.raises(TechnicalError, match="Invalid begin value -5"):
            list(
                StoreFileContentRepository._get_file_part_to_download(
                    sample_file_version, -5, 100
                )
            )

    def test_get_file_part_to_download_begin_greater_than_end(
        self, sample_file_version
    ):
        """Test error handling for begin > end"""
        with pytest.raises(
            TechnicalError, match="Invalid range: begin 1500 is greater than end 500"
        ):
            list(
                StoreFileContentRepository._get_file_part_to_download(
                    sample_file_version, 1500, 500
                )
            )

    def test_get_file_part_to_download_end_exceeds_size(self, sample_file_version):
        """An end past the file is clamped to its last byte"""
        parts = list(
            StoreFileContentRepository._get_file_part_to_download(
                sample_file_version, 500, 3000
            )
        )

        assert parts == [(1001, 500, 999), (1002, 0, 999)]

    def test_get_file_part_to_download_begin_exceeds_size(self, sample_file_version):
        """Test error handling for begin exceeding file size"""
        with pytest.raises(
            TechnicalError, match="Invalid range: begin 2500 is greater than end 1999"
        ):
            list(
                StoreFileContentRepository._get_file_part_to_download(
                    sample_file_version, 2500, 3000
                )
            )

    def test_get_file_part_to_download_never_asks_past_a_part(
        self, sample_file_version
    ):
        """A part is asked for its own bytes only -- one more than it has
        could only be served by a download stopping short."""
        parts = list(
            StoreFileContentRepository._get_file_part_to_download(
                sample_file_version, 0, 1000
            )
        )

        assert parts == [(1001, 0, 999), (1002, 0, 0)]


class TestSaveAndUpdate:
    """The repository delegates the bytes to the store"""

    @pytest.mark.asyncio
    async def test_save_delegates_to_the_store(self, repository, mock_message_api):
        file_msg = MockFileMessage("test.txt", 100)

        result = await repository.save(file_msg)

        mock_message_api.upload.assert_awaited_once_with(file_msg)
        assert result == [SentFileMessage(message_id=12345, size=1000)]

    @pytest.mark.asyncio
    async def test_save_propagates_upload_errors(self, repository, mock_message_api):
        mock_message_api.upload.side_effect = Exception("Upload failed")

        with pytest.raises(Exception, match="Upload failed"):
            await repository.save(MockFileMessage("test.txt", 100))

    @pytest.mark.asyncio
    async def test_update_delegates_to_the_store(self, repository, mock_message_api):
        result = await repository.update(54321, b"updated content", "updated.txt")

        mock_message_api.replace_document.assert_awaited_once_with(
            54321, b"updated content", "updated.txt"
        )
        assert result == 54321


class TestGetMethod:
    """Test the get method for file content retrieval"""

    @pytest.mark.asyncio
    async def test_get_full_file(
        self, repository, mock_message_api, sample_file_version, mocker
    ):
        """Test getting entire file content"""

        # Each part download yields two chunks; downloads happen lazily
        # while the returned stream is consumed.
        async def fake_download(message_id, begin, end):
            async def chunks():
                yield b"chunk1"
                yield b"chunk2"

            return mocker.Mock(chunks=chunks())

        mock_message_api.download_file.side_effect = fake_download

        result = await repository.get(sample_file_version, 0, -1, "test.txt")
        chunks = [chunk async for chunk in result]

        assert mock_message_api.download_file.call_count == 2  # Two parts
        assert chunks == [b"chunk1", b"chunk2", b"chunk1", b"chunk2"]

    @pytest.mark.asyncio
    async def test_get_partial_range(
        self, repository, mock_message_api, sample_file_version, mocker
    ):
        """Test getting partial file range"""

        async def fake_download(message_id, begin, end):
            async def chunks():
                yield b"partial"

            return mocker.Mock(chunks=chunks())

        mock_message_api.download_file.side_effect = fake_download

        result = await repository.get(sample_file_version, 500, 1500, "test.txt")
        chunks = [chunk async for chunk in result]

        assert mock_message_api.download_file.call_count == 2  # Spans two parts
        assert chunks == [b"partial", b"partial"]

    @pytest.mark.asyncio
    async def test_get_empty_file(self, repository, mock_message_api, mocker):
        """Test getting content from empty file"""
        empty_version = TGFSFileVersion(
            id="empty",
            updated_at=datetime.datetime.now(),
            _size=0,
            message_ids=[],
            part_sizes=[],
        )

        result = await repository.get(empty_version, 0, -1, "empty.txt")

        assert [chunk async for chunk in result] == []
        mock_message_api.download_file.assert_not_called()


class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_file_part_download_edge_cases(self):
        """Test file part download calculations for edge cases"""
        # Single byte file
        tiny_version = TGFSFileVersion(
            id="tiny",
            updated_at=datetime.datetime.now(),
            _size=1,
            message_ids=[999],
            part_sizes=[1],
        )

        parts = list(
            StoreFileContentRepository._get_file_part_to_download(tiny_version, 0, -1)
        )
        assert len(parts) == 1
        assert parts[0] == (999, 0, 0)

        # Range at exact boundaries
        sample_version = TGFSFileVersion(
            id="boundary",
            updated_at=datetime.datetime.now(),
            _size=2000,
            message_ids=[1001, 1002],
            part_sizes=[1000, 1000],
        )

        # Range exactly at part boundary
        parts = list(
            StoreFileContentRepository._get_file_part_to_download(
                sample_version, 1000, 1999
            )
        )
        assert len(parts) == 1
        assert parts[0] == (1002, 0, 999)
