import os
import tempfile
from typing import AsyncIterator

import pytest

from tgdcfs.backends.telegram.uploader import (
    MAX_PART_ATTEMPTS,
    FileChunk,
    WorkersConfig,
    FileUploader,
)
from tgdcfs.reqres import (
    FileMessageFromBuffer,
    FileMessageFromPath,
    FileMessageFromStream,
    SaveFilePartResp,
    SendMessageResp,
)
from tgdcfs.errors import TechnicalError
from tgdcfs.tasks.integrations import TaskTracker
from tgdcfs.backends.telegram.interface import ITDLibClient, TDLibApi


@pytest.fixture
def no_backoff(mocker):
    """Skip the retry backoff so the retry tests do not sit in real sleeps."""
    return mocker.patch(
        "tgdcfs.backends.telegram.uploader.asyncio.sleep",
        new=mocker.AsyncMock(),
    )


class TestWorkersConfig:
    def test_default_values(self):
        config = WorkersConfig()
        assert config.small == 3
        assert config.big == 8

    def test_custom_values(self):
        config = WorkersConfig(small=2, big=8)
        assert config.small == 2
        assert config.big == 8


class TestFileChunk:
    def test_creation(self):
        content = b"test data"
        file_part = 5
        chunk = FileChunk(content=content, file_part=file_part)

        assert chunk.content == content
        assert chunk.file_part == file_part


class TestUploaderFromPath:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.save_big_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.send_small_file = mocker.AsyncMock(
            return_value=SendMessageResp(message_id=123)
        )
        client.send_big_file = mocker.AsyncMock(
            return_value=SendMessageResp(message_id=124)
        )
        return client

    @pytest.fixture
    def test_file(self):
        """Create a temporary test file"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            test_content = b"Hello, World!" * 100  # 1300 bytes
            f.write(test_content)
            f.flush()
            yield f.name, test_content
        os.unlink(f.name)

    @pytest.fixture
    def mock_task_tracker(self, mocker):
        tracker = mocker.AsyncMock(spec=TaskTracker)
        tracker.cancelled = mocker.AsyncMock(return_value=False)
        tracker.update_progress = mocker.AsyncMock()
        return tracker

    @pytest.mark.asyncio
    async def test_small_file_upload(self, mock_client, test_file, mock_task_tracker):
        file_path, expected_content = test_file
        file_size = len(expected_content)

        file_msg = FileMessageFromPath.new(path=file_path, name="test.txt")
        file_msg.task_tracker = mock_task_tracker

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == file_size
        assert uploader.get_uploaded_file().name == "test.txt"
        mock_client.save_file_part.assert_called()
        mock_task_tracker.update_progress.assert_called()

    @pytest.mark.asyncio
    async def test_big_file_upload(self, mock_client, mock_task_tracker):
        """Test big file upload (>10MB)"""
        big_content = b"x" * (11 * 1024 * 1024)  # 11MB

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(big_content)
            f.flush()

            try:
                file_msg = FileMessageFromPath.new(path=f.name, name="big_file.bin")
                file_msg.task_tracker = mock_task_tracker

                uploader = FileUploader(client=mock_client, file_msg=file_msg)

                uploaded_size = await uploader.upload()

                assert uploaded_size == len(big_content)
                mock_client.save_big_file_part.assert_called()
            finally:
                os.unlink(f.name)

    @pytest.mark.asyncio
    async def test_upload_with_offset(self, mock_client, test_file):
        file_path, original_content = test_file
        offset = 5
        expected_content = original_content[offset:]

        file_msg = FileMessageFromPath.new(path=file_path, name="test_offset.txt")
        file_msg._offset = offset
        file_msg.size = len(expected_content)

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(expected_content)

    @pytest.mark.asyncio
    async def test_default_file_name(self, mock_client, test_file):
        file_path, _ = test_file

        file_msg = FileMessageFromPath.new(path=file_path)
        file_msg.name = ""  # Clear the name to test default

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()

        expected_name = os.path.basename(file_path)
        assert uploader.get_uploaded_file().name == expected_name

    @pytest.mark.asyncio
    async def test_upload_failure_and_retry(self, mock_client, test_file):
        file_path, expected_content = test_file

        # First call fails, second succeeds
        mock_client.save_file_part.side_effect = [
            Exception("Network error"),
            SaveFilePartResp(success=True),
        ]

        file_msg = FileMessageFromPath.new(path=file_path, name="retry_test.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(expected_content)
        assert mock_client.save_file_part.call_count >= 1

    @pytest.mark.asyncio
    async def test_task_cancellation(
        self, mock_client, test_file, mock_task_tracker, mocker
    ):
        file_path, expected_content = test_file

        # Simulate cancellation
        mock_task_tracker.cancelled = mocker.AsyncMock(return_value=True)

        file_msg = FileMessageFromPath.new(path=file_path, name="cancelled.txt")
        file_msg.task_tracker = mock_task_tracker

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        # Note: Current implementation doesn't raise TaskCancelled, it just returns early
        uploaded_size = await uploader.upload()
        assert uploaded_size == len(
            expected_content
        )  # Should still complete the file size calculation

    @pytest.mark.asyncio
    async def test_send_file(self, mock_client, test_file):
        file_path, expected_content = test_file
        chat_id = 123
        caption = "Test caption"

        file_msg = FileMessageFromPath.new(path=file_path, name="send_test.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()
        response = await uploader.send(chat_id, caption)

        assert isinstance(response, SendMessageResp)
        mock_client.send_small_file.assert_called_once()


class TestUploaderFromBuffer:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.save_big_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        return client

    @pytest.mark.asyncio
    async def test_buffer_upload(self, mock_client):
        test_data = b"Buffer test data" * 50  # 800 bytes

        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="buffer_test.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(test_data)
        assert uploader.get_uploaded_file().name == "buffer_test.txt"
        mock_client.save_file_part.assert_called()

    @pytest.mark.asyncio
    async def test_buffer_with_offset(self, mock_client):
        test_data = b"0123456789" * 10  # 100 bytes
        offset = 10
        expected_data = test_data[offset:]

        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="offset_buffer.txt")
        file_msg._offset = offset
        file_msg.size = len(expected_data)

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(expected_data)

    @pytest.mark.asyncio
    async def test_default_buffer_name(self, mock_client):
        test_data = b"unnamed buffer data"

        file_msg = FileMessageFromBuffer.new(buffer=test_data)

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()

        assert uploader.get_uploaded_file().name == "unnamed"


class TestUploaderFromStream:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        return client

    @staticmethod
    async def create_test_stream(chunks: list[bytes]) -> AsyncIterator[bytes]:
        """Create a test async stream from chunks"""
        for chunk in chunks:
            yield chunk

    @pytest.mark.asyncio
    async def test_stream_upload(self, mock_client):
        chunks = [b"chunk1", b"chunk2", b"chunk3"]
        total_size = sum(len(chunk) for chunk in chunks)

        stream = self.create_test_stream(chunks)
        file_msg = FileMessageFromStream.new(
            stream=stream, size=total_size, name="stream_test.txt"
        )

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == total_size
        assert uploader.get_uploaded_file().name == "stream_test.txt"
        mock_client.save_file_part.assert_called()

    @pytest.mark.asyncio
    async def test_stream_default_name(self, mock_client):
        chunks = [b"data"]
        total_size = sum(len(chunk) for chunk in chunks)

        stream = self.create_test_stream(chunks)
        file_msg = FileMessageFromStream.new(stream=stream, size=total_size)

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()

        assert uploader.get_uploaded_file().name == "unnamed"

    @pytest.mark.asyncio
    async def test_stream_chunked_reading(self, mock_client):
        """Test that stream reading handles chunks correctly"""
        # Create chunks that don't align with read boundaries
        chunks = [b"abc", b"defghij", b"klm"]
        total_size = sum(len(chunk) for chunk in chunks)

        stream = self.create_test_stream(chunks)
        file_msg = FileMessageFromStream.new(
            stream=stream, size=total_size, name="chunked.txt"
        )

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == total_size


class TestErrorHandling:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        return client

    @pytest.mark.asyncio
    async def test_upload_response_failure(self, mock_client, mocker, no_backoff):
        """A part that fails within the retry budget still goes through."""
        failure_count = 0

        def side_effect(*_args, **_kwargs):
            nonlocal failure_count
            if failure_count < MAX_PART_ATTEMPTS - 1:
                failure_count += 1
                return SaveFilePartResp(success=False)
            return SaveFilePartResp(success=True)

        mock_client.save_file_part = mocker.AsyncMock(side_effect=side_effect)

        test_data = b"test data"
        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="fail_test.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()
        assert uploaded_size == len(test_data)
        assert mock_client.save_file_part.call_count == MAX_PART_ATTEMPTS

    @pytest.mark.asyncio
    async def test_upload_gives_up_after_the_retry_budget(
        self, mock_client, mocker, no_backoff
    ):
        """A part that never succeeds fails the upload instead of looping."""
        mock_client.save_file_part = mocker.AsyncMock(
            side_effect=Exception("Network error")
        )

        file_msg = FileMessageFromBuffer.new(buffer=b"test data", name="doomed.txt")
        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        with pytest.raises(Exception, match="Network error"):
            await uploader.upload()

        assert mock_client.save_file_part.call_count == MAX_PART_ATTEMPTS

    @pytest.mark.asyncio
    async def test_upload_waits_as_long_as_telegram_asks(self, mock_client, mocker):
        """A flood error is retried after the delay Telegram named."""

        class FloodWaitError(Exception):
            seconds = 17

        sleep = mocker.patch(
            "tgdcfs.backends.telegram.uploader.asyncio.sleep",
            new=mocker.AsyncMock(),
        )
        mock_client.save_file_part = mocker.AsyncMock(
            side_effect=[FloodWaitError(), SaveFilePartResp(success=True)]
        )

        file_msg = FileMessageFromBuffer.new(buffer=b"test data", name="flood.txt")
        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()

        sleep.assert_awaited_once_with(17.0)


class TestConcurrencyAndWorkers:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        return client

    @pytest.mark.asyncio
    async def test_custom_workers_config(self, mock_client):
        """Test upload with custom worker configuration"""
        test_data = b"concurrent test data" * 10
        workers_config = WorkersConfig(small=2, big=4)

        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="concurrent.txt")

        uploader = FileUploader(
            client=mock_client,
            file_msg=file_msg,
            workers=workers_config,
        )

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(test_data)

    @pytest.mark.asyncio
    async def test_concurrent_uploads(self, mock_client):
        """Test that multiple parts can be uploaded concurrently"""
        # Comfortably more than one 512 KiB part
        test_data = b"x" * (1024 * 1024 * 2)  # 2 MiB

        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="large.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(test_data)
        # Should have made multiple calls due to chunking
        assert mock_client.save_file_part.call_count > 1


class TestStreamedUpload:
    """The streamed-upload rules for a length that is not known up front.

    Per core.telegram.org/api/files#streamed-uploads: every part goes
    through upload.saveBigFilePart with ``file_total_parts`` -1 until the
    stream ends, the part it ends on carries the true count, and a stream
    that ended exactly on a part boundary sends one last empty part
    carrying it. The document is then sent as an inputFileBig.
    """

    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_big_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.send_big_file = mocker.AsyncMock(
            return_value=SendMessageResp(message_id=124)
        )
        client.send_small_file = mocker.AsyncMock(
            return_value=SendMessageResp(message_id=123)
        )
        return client

    @pytest.fixture
    def small_parts(self, mocker):
        """A 1 KiB part size and one worker, so order and counts are exact."""
        config = mocker.Mock()
        config.tgdcfs.transfer.upload_part_size_bytes = 1024
        config.tgdcfs.transfer.upload_workers_small = 1
        config.tgdcfs.transfer.upload_workers_big = 1
        return mocker.patch(
            "tgdcfs.backends.telegram.uploader.get_config", return_value=config
        )

    @pytest.fixture
    def mock_task_tracker(self, mocker):
        tracker = mocker.AsyncMock(spec=TaskTracker)
        tracker.cancelled = mocker.AsyncMock(return_value=False)
        tracker.update_progress = mocker.AsyncMock()
        return tracker

    @staticmethod
    def _stream(chunks: list[bytes]):
        async def gen():
            for chunk in chunks:
                yield chunk

        return gen()

    def _msg(self, chunks: list[bytes], name: str = "stream.bin"):
        return FileMessageFromStream.new(
            stream=self._stream(chunks), size=-1, name=name
        )

    @pytest.mark.asyncio
    async def test_a_stream_short_of_a_part_ends_on_the_true_count(
        self, mock_client, small_parts
    ):
        payload = b"x" * 2048 + b"y" * 512
        uploader = FileUploader(
            mock_client, self._msg([payload[:1000], payload[1000:]])
        )

        assert await uploader.upload() == len(payload)

        calls = sorted(
            mock_client.save_big_file_part.call_args_list,
            key=lambda c: c.args[0].file_part,
        )
        assert [c.args[0].file_part for c in calls] == [0, 1, 2]
        assert [c.args[0].file_total_parts for c in calls] == [-1, -1, 3]
        assert calls[-1].args[0].bytes == b"y" * 512
        mock_client.save_file_part.assert_not_called()

        await uploader.send(1)
        assert mock_client.send_big_file.call_args.args[0].file.parts == 3

    @pytest.mark.asyncio
    async def test_a_stream_on_a_part_boundary_sends_one_last_empty_part(
        self, mock_client, small_parts
    ):
        payload = b"x" * 2048
        uploader = FileUploader(mock_client, self._msg([payload]))

        assert await uploader.upload() == 2048

        calls = mock_client.save_big_file_part.call_args_list
        assert [c.args[0].file_part for c in calls] == [0, 1, 2]
        assert [c.args[0].file_total_parts for c in calls] == [-1, -1, 2]
        assert calls[-1].args[0].bytes == b""

        await uploader.send(1)
        assert mock_client.send_big_file.call_args.args[0].file.parts == 2

    @pytest.mark.asyncio
    async def test_one_small_chunk_is_a_single_part(self, mock_client, small_parts):
        uploader = FileUploader(mock_client, self._msg([b"z" * 100]))

        assert await uploader.upload() == 100

        calls = mock_client.save_big_file_part.call_args_list
        assert len(calls) == 1
        assert calls[0].args[0].file_part == 0
        assert calls[0].args[0].file_total_parts == 1
        assert calls[0].args[0].bytes == b"z" * 100

    @pytest.mark.asyncio
    async def test_an_empty_stream_uploads_no_part(self, mock_client, small_parts):
        uploader = FileUploader(mock_client, self._msg([b""]))

        assert await uploader.upload() == 0

        mock_client.save_big_file_part.assert_not_called()
        await uploader.send(1)
        assert mock_client.send_big_file.call_args.args[0].file.parts == 0

    @pytest.mark.asyncio
    async def test_the_task_tracker_sees_the_streamed_bytes(
        self, mock_client, small_parts, mock_task_tracker
    ):
        payload = b"x" * 2048
        file_msg = self._msg([payload])
        file_msg.task_tracker = mock_task_tracker

        uploader = FileUploader(mock_client, file_msg)
        await uploader.upload()

        deltas = [
            call.kwargs["size_delta"]
            for call in mock_task_tracker.update_progress.await_args_list
        ]
        assert sum(deltas) == len(payload)

    @pytest.mark.asyncio
    async def test_a_failed_part_fails_the_upload(
        self, mock_client, small_parts, mocker, no_backoff
    ):
        mocker.patch(
            "tgdcfs.backends.telegram.uploader.flood_wait_seconds",
            return_value=None,
        )
        mock_client.save_big_file_part.return_value = SaveFilePartResp(success=False)

        uploader = FileUploader(mock_client, self._msg([b"z" * 10]))

        with pytest.raises(TechnicalError):
            await uploader.upload()


class TestStreamMessageOfUnknownLength:
    """``FileMessageFromStream`` reads without a length."""

    @staticmethod
    def _stream(chunks: list[bytes]):
        async def gen():
            for chunk in chunks:
                yield chunk

        return gen()

    def _msg(self, chunks: list[bytes]):
        return FileMessageFromStream.new(stream=self._stream(chunks), size=-1)

    @pytest.mark.asyncio
    async def test_read_joins_chunks_up_to_the_request(self):
        file_msg = self._msg([b"ab", b"cde", b"f"])
        assert await file_msg.read(4) == b"abcd"
        assert await file_msg.read(4) == b"ef"

    @pytest.mark.asyncio
    async def test_a_short_answer_only_comes_at_the_end(self):
        file_msg = self._msg([b"abc"])
        assert await file_msg.read(10) == b"abc"
        assert await file_msg.read(10) == b""

    @pytest.mark.asyncio
    async def test_a_known_size_still_reads_exactly(self):
        file_msg = FileMessageFromStream.new(
            stream=self._stream([b"abc", b"de"]), size=5
        )
        assert await file_msg.read(10) == b"abcde"
