from contextlib import asynccontextmanager
from typing import AsyncIterator, Iterable

from tgdcfs.core.model import TGFSDirectory, TGFSMetadata
from tgdcfs.core.repository.interface import IMetaDataRepository


class MetaDataApi:
    def __init__(self, metadata_repo: IMetaDataRepository):
        self.__metadata_repo = metadata_repo
        self.__batch_depth = 0
        self.__pending = False

    async def init(self) -> None:
        await self.__metadata_repo.init()

    def reset(self) -> None:
        self.__metadata_repo.metadata = TGFSMetadata(dir=TGFSDirectory.root_dir())

    @asynccontextmanager
    async def batch(self) -> AsyncIterator[None]:
        """Collapse the pushes of a bulk operation into a single write.

        The pinned-message backend rewrites the whole metadata blob on
        every push, so copying a folder file by file would rewrite it once
        per file. Nesting is allowed; only the outermost block writes.
        The write also happens when the block fails, because whatever part
        of the operation did succeed is real and has to be recorded.
        """
        self.__batch_depth += 1
        try:
            yield
        finally:
            self.__batch_depth -= 1
            if self.__batch_depth == 0 and self.__pending:
                self.__pending = False
                await self.__metadata_repo.push()

    async def push(self) -> None:
        if self.__batch_depth:
            self.__pending = True
            return
        await self.__metadata_repo.push()

    def get_root_directory(self) -> TGFSDirectory:
        return self.__metadata_repo.root()

    # -- file stats (``listing.stat_in_metadata``) -------------------------

    @property
    def push_writes_stats(self) -> bool:
        return self.__metadata_repo.push_writes_stats

    async def load_stats(self, directory: TGFSDirectory) -> bool:
        return await self.__metadata_repo.load_stats(directory)

    def stats_stored(self, directory: TGFSDirectory) -> bool:
        return self.__metadata_repo.stats_stored(directory)

    async def save_stats(
        self, directories: Iterable[TGFSDirectory], keep: bool
    ) -> None:
        await self.__metadata_repo.save_stats(directories, keep)
