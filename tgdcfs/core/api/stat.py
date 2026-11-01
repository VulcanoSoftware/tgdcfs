from tgdcfs.core.model import FileStat, TGFSFileDesc, TGFSFileRef
from tgdcfs.utils.time import ts

from .file_desc import FileDescApi


class FileStatApi:
    """Size and dates of files for listings (PROPFIND, SFTP readdir/stat).

    A download reads the descriptor itself; this is only what a listing
    shows. With ``validate=False`` (``listing.validate_on_list: false``)
    the descriptor is not checked against the content messages, which
    halves the store reads of a listing.
    """

    def __init__(self, file_desc_api: FileDescApi, validate: bool = True):
        self._file_desc_api = file_desc_api
        self._validate = validate

    async def get(self, fr: TGFSFileRef) -> FileStat:
        fd = await self._file_desc_api.get_file_desc(fr, validate=self._validate)
        return await self.stat_of(fd)

    async def stat_of(self, fd: TGFSFileDesc) -> FileStat:
        fv = fd.get_latest_version()
        size = await self._file_desc_api.content_length(fv)
        versions = fd.get_versions()
        # A descriptor read from the store does not carry a creation date;
        # the oldest version is when the file came into being.
        created = (
            min(v.updated_at_timestamp for v in versions)
            if versions
            else ts(fd.created_at)
        )
        return FileStat(
            size=max(0, size),
            created_at=created,
            modified_at=fv.updated_at_timestamp if versions else created,
        )
