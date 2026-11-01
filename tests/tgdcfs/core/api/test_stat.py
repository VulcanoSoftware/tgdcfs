import datetime
from unittest.mock import AsyncMock, Mock

import pytest

from tgdcfs.core.api import FileStatApi
from tgdcfs.core.model import TGFSFileDesc, TGFSFileVersion
from tgdcfs.utils.time import ts

OLD = datetime.datetime(2024, 1, 1, 12, 0, 0)
NEW = datetime.datetime(2024, 6, 1, 12, 0, 0)


def file_desc_api(fd: TGFSFileDesc, size: int = 1234) -> Mock:
    api = Mock()
    api.get_file_desc = AsyncMock(return_value=fd)
    api.content_length = AsyncMock(return_value=size)
    return api


def two_versions() -> TGFSFileDesc:
    fd = TGFSFileDesc(name="a.txt")
    fd.add_version(TGFSFileVersion(id="old", updated_at=OLD, message_ids=[1]))
    fd.add_version(TGFSFileVersion(id="new", updated_at=NEW, message_ids=[2]))
    return fd


class TestFileStatApi:
    @pytest.mark.asyncio
    async def test_dates_come_from_the_versions(self):
        stat = await FileStatApi(file_desc_api(two_versions())).get(Mock())

        assert stat.size == 1234
        assert stat.created_at == ts(OLD)
        assert stat.modified_at == ts(NEW)

    @pytest.mark.asyncio
    async def test_the_validate_setting_reaches_the_descriptor_read(self):
        api = file_desc_api(two_versions())
        fr = Mock()

        await FileStatApi(api, validate=False).get(fr)

        api.get_file_desc.assert_awaited_once_with(fr, validate=False)

    @pytest.mark.asyncio
    async def test_an_unknown_size_is_reported_as_zero(self):
        stat = await FileStatApi(
            file_desc_api(TGFSFileDesc.empty("a.txt"), size=-1)
        ).get(Mock())

        assert stat.size == 0
