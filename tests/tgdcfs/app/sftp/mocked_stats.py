from tgdcfs.core.api import FileStatApi


class MockedDescApi:
    """Feeds a real ``FileStatApi`` from the mocked ``desc`` and
    ``content_length`` of a test client, so listings keep using them."""

    def __init__(self, client):
        self._client = client

    async def get_file_desc(self, fr, include_all_versions=False, validate=True):
        return await self._client.file_api.desc(fr)

    async def content_length(self, fv):
        return await self._client.fc_repo.content_length(fv)


def mocked_stats(client) -> FileStatApi:
    return FileStatApi(MockedDescApi(client))  # type: ignore[arg-type]
