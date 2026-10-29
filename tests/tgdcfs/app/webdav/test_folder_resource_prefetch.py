from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from tgdcfs.app.fs_cache import FSCache, gfc
from tgdcfs.app.webdav.folder import Folder
from tgdcfs.app.webdav.resource import Resource
from tgdcfs.core.model import TGFSDirectory, TGFSFileVersion
from tgdcfs.core.model.serialized import TGFSFileVersionSerialized


class TestFolderResourcePrefetch:
    @pytest.mark.asyncio
    async def test_folder_prefetch_descriptors_and_member_with_fr(self):
        gfc["default"] = FSCache()
        mock_client = MagicMock()
        mock_client.name = "default"

        mock_dir = TGFSDirectory(name="testdir", parent=None)
        mock_dir.attach_file_ref("file1.txt", 101)
        mock_dir.attach_file_ref("file2.txt", 102)

        mock_client.dir_api.root = mock_dir
        mock_client.store = MagicMock()
        mock_client.store.get_messages = AsyncMock(return_value=[])

        folder = Folder("/", mock_client)

        mock_fd1 = MagicMock()
        mock_fv1 = MagicMock()
        mock_fv1.message_ids = [201]
        mock_fd1.get_latest_version.return_value = mock_fv1

        mock_fd2 = MagicMock()
        mock_fv2 = MagicMock()
        mock_fv2.message_ids = [202]
        mock_fd2.get_latest_version.return_value = mock_fv2

        mock_client.file_api.desc = AsyncMock(side_effect=[mock_fd1, mock_fd2])

        names = await folder.member_names()
        assert set(names) == {"file1.txt", "file2.txt"}
        assert mock_client.store.get_messages.call_count == 2
        mock_client.store.get_messages.assert_any_call([101, 102])
        mock_client.store.get_messages.assert_any_call([201, 202])

        resource = await folder.member("file1.txt")
        assert isinstance(resource, Resource)
        assert (await resource.display_name()) == "file1.txt"

    @pytest.mark.asyncio
    async def test_resource_uses_supplied_fr(self):
        gfc["default"] = FSCache()
        mock_client = MagicMock()
        mock_client.name = "default"
        mock_dir = TGFSDirectory(name="testdir", parent=None)
        fr = mock_dir.attach_file_ref("file1.txt", 101)

        with patch("tgdcfs.app.webdav.resource.Ops") as mock_ops_cls:
            mock_ops = MagicMock()
            mock_ops_cls.return_value = mock_ops

            res = Resource("/file1.txt", mock_client, fr=fr)

            assert await res.display_name() == "file1.txt"
            mock_ops.stat_file.assert_not_called()

    def test_file_version_from_dict_size(self):
        fv_dict: TGFSFileVersionSerialized = {
            "id": "v1",
            "updatedAt": 1609459200000,
            "messageIds": [201],
            "size": 123456,
        }
        fv = TGFSFileVersion.from_dict(cast(TGFSFileVersionSerialized, fv_dict))
        assert fv.size == 123456
