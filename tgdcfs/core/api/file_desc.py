import datetime
from typing import List, Optional
from uuid import uuid4 as uuid

from tgdcfs.config import WriteAck
from tgdcfs.core.model import TGFSFileDesc, TGFSFileRef, TGFSFileVersion
from tgdcfs.core.repository.interface import (
    FDRepositoryResp,
    IFDRepository,
    IFileContentRepository,
)
from tgdcfs.reqres import (
    FileContent,
    FileMessage,
    FileMessageImported,
    SentFileMessage,
    UploadableFileMessage,
)


class FileDescApi:
    def __init__(
        self,
        fd_repo: IFDRepository,
        fc_repo: IFileContentRepository,
        write_ack: WriteAck = "primary",
    ):
        self.__fd_repo = fd_repo
        self.__fc_repo = fc_repo
        # ``cache``: a write is complete once the bytes are in the local
        # cache; the version is recorded as pending and a worker moves it
        # into the stores (design plan 4.12).
        self.__write_ack = write_ack

    async def _new_version(
        self, file_msg: FileMessage, version_id: str
    ) -> Optional[TGFSFileVersion]:
        """Store the bytes of an upload and describe the resulting version.

        With ``write_ack: cache`` the bytes go into the cache only and the
        version comes back pending; when the cache cannot take them, or for
        imports and empty files, the usual path runs. ``None`` for a
        message that carries no bytes at all.
        """
        if isinstance(file_msg, UploadableFileMessage):
            file_msg.version_id = version_id
            if self.__write_ack == "cache":
                staged = await self.__fc_repo.stage(file_msg, version_id)
                if staged is not None:
                    return TGFSFileVersion.pending_version(version_id, staged)
        if isinstance(file_msg, UploadableFileMessage | FileMessageImported):
            sent_file_msg = await self.get_sent_file_message(file_msg)
            return TGFSFileVersion.from_sent_file_message(
                *sent_file_msg, version_id=version_id
            )
        return None

    async def create_file_desc(self, file_msg: FileMessage) -> FDRepositoryResp:
        return await self.append_file_version(file_msg, fr=None)

    async def get_file_desc(
        self,
        fr: TGFSFileRef,
        include_all_versions: bool = False,
        validate: bool = True,
    ) -> TGFSFileDesc:
        return await self.__fd_repo.get(fr, include_all_versions, validate=validate)

    async def save_new_file_desc(self, fd: TGFSFileDesc) -> FDRepositoryResp:
        """Write ``fd`` to a brand new descriptor message.

        Unlike :meth:`create_file_desc` this uploads nothing -- the
        descriptor already knows which content messages it refers to.
        """
        return await self.__fd_repo.save(fd, fr=None)

    async def download_file_at_version(
        self, fv: TGFSFileVersion, begin: int, end: int, as_name: str
    ) -> FileContent:
        return await self.__fc_repo.get(
            fv=fv,
            begin=begin,
            end=end,
            name=as_name,
        )

    async def get_sent_file_message(
        self, file_msg: UploadableFileMessage | FileMessageImported
    ) -> List[SentFileMessage]:
        if isinstance(file_msg, FileMessageImported):
            return [SentFileMessage(file_msg.message_id, file_msg.size)]
        return await self.__fc_repo.save(file_msg)

    async def append_file_version(
        self, file_msg: FileMessage, fr: Optional[TGFSFileRef] = None
    ) -> FDRepositoryResp:
        fd = await self.get_file_desc(fr) if fr else TGFSFileDesc(name=file_msg.name)

        # The version id is chosen before the bytes move so the content
        # repository can stage them in the local cache under it.
        version = await self._new_version(file_msg, str(uuid()))
        if version is not None:
            fd.add_version(version)
        else:
            fd.add_empty_version()

        return await self.__fd_repo.save(fd, fr)

    async def update_file_version(
        self, fr: TGFSFileRef, file_msg: FileMessage, version_id: str
    ) -> FDRepositoryResp:
        fd = await self.get_file_desc(fr)
        if (fv := await self._new_version(file_msg, version_id)) is not None:
            fd.update_version(version_id, fv)
        else:
            fv = fd.get_version(version_id)
            fv.set_invalid()
            fd.update_version(version_id, fv)

        return await self.__fd_repo.save(fd, fr)

    async def set_last_modified(
        self, fr: TGFSFileRef, when: datetime.datetime
    ) -> FDRepositoryResp:
        """Re-date the latest version and write the descriptor back."""
        fd = await self.get_file_desc(fr)
        fd.set_last_modified(when)
        return await self.__fd_repo.save(fd, fr)

    async def delete_file_version(
        self, fr: TGFSFileRef, version_id: str
    ) -> FDRepositoryResp:
        fd = await self.get_file_desc(fr)
        fd.delete_version(version_id)
        resp = await self.__fd_repo.save(fd, fr)
        await self.__fc_repo.forget([version_id])
        return resp

    async def forget_versions(self, version_ids: List[str]) -> None:
        """Drop what the content repository kept locally for deleted versions."""
        await self.__fc_repo.forget(version_ids)
