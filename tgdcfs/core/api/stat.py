import asyncio
import logging
import time
from typing import Dict, Optional

from tgdcfs.core.model import FileStat, TGFSDirectory, TGFSFileDesc, TGFSFileRef
from tgdcfs.utils.time import ts

from .file_desc import FileDescApi
from .metadata import MetaDataApi

logger = logging.getLogger(__name__)

# Stats filled in by listings are written once nothing new came in for
# FLUSH_QUIET seconds, and at least every FLUSH_MAX_DELAY seconds while a
# long listing keeps filling them, so a listing the client abandons halfway
# still leaves everything read so far in the metadata.
FLUSH_QUIET = 5.0
FLUSH_MAX_DELAY = 60.0
# A stat dropped because its file changed is written sooner: until then
# the stored one is out of date, and a crash would keep it that way.
URGENT_QUIET = 1.0
URGENT_MAX_DELAY = 5.0
# Pause before a failed write is tried again.
FLUSH_RETRY_DELAY = 60.0
# How long shutdown waits for the last write.
CLOSE_TIMEOUT = 30.0


class FileStatApi:
    """Size and dates of files for listings (PROPFIND, SFTP readdir/stat).

    A download reads the descriptor itself; this is only what a listing
    shows. With ``validate=False`` (``listing.validate_on_list: false``)
    the descriptor is not checked against the content messages, which
    halves the store reads of a listing.

    With a ``metadata_api`` and ``persist=True``
    (``listing.stat_in_metadata``) the stats are kept on the file refs and
    written to the metadata. A listing then reads no descriptor at all; a
    stat that is not stored yet is read from the descriptor once and
    written back in the background. A write to a file drops its stat, the
    next listing reads it again.
    """

    def __init__(
        self,
        file_desc_api: FileDescApi,
        validate: bool = True,
        metadata_api: Optional[MetaDataApi] = None,
        persist: bool = False,
    ):
        self._file_desc_api = file_desc_api
        self._validate = validate
        self._metadata_api = metadata_api
        self._persist = persist and metadata_api is not None

        # Descriptor reads in flight, by ``id`` of the file ref: a folder
        # listed twice at once reads each descriptor once.
        self._reads: Dict[int, asyncio.Future[FileStat]] = {}
        # Folders whose stored stats have to be rewritten, by ``id``.
        self._dirty: Dict[int, TGFSDirectory] = {}
        self._dirty_since: Optional[float] = None
        self._last_mark = 0.0
        self._urgent = False
        self._flusher: Optional[asyncio.Task[None]] = None
        self._wake = asyncio.Event()
        self._closing = asyncio.Event()

    @property
    def persist(self) -> bool:
        return self._persist

    async def get(self, fr: TGFSFileRef) -> FileStat:
        if not self._persist:
            return await self._read(fr)

        if fr.stat is None and self._metadata_api is not None:
            await self._metadata_api.load_stats(fr.location)
        if fr.stat is not None:
            return fr.stat

        key = id(fr)
        read = self._reads.get(key)
        if read is None:
            read = asyncio.ensure_future(self._fill(fr))
            self._reads[key] = read
            read.add_done_callback(lambda done: self._read_done(key, done))
        # Shielded: when the client gives up on a long listing the request
        # is cancelled, but the descriptor reads it started run on and
        # their results are stored. The next attempt finds them there.
        return await asyncio.shield(read)

    def _read_done(self, key: int, read: "asyncio.Future[FileStat]") -> None:
        self._reads.pop(key, None)
        # Nobody may be waiting any more (the listing was abandoned); fetch
        # the error so it is not reported as never retrieved. The listings
        # that do wait get it from the shield.
        if not read.cancelled():
            read.exception()

    async def _read(self, fr: TGFSFileRef) -> FileStat:
        fd = await self._file_desc_api.get_file_desc(fr, validate=self._validate)
        return await self.stat_of(fd)

    async def _fill(self, fr: TGFSFileRef) -> FileStat:
        epoch = fr.stat_epoch
        fd = await self._file_desc_api.get_file_desc(fr, validate=self._validate)
        stat = await self.stat_of(fd)
        # A descriptor that could not be read is served but never stored,
        # and neither is one read before the file changed.
        if fd.complete and fr.stat_epoch == epoch and _in_tree(fr):
            fr.stat = stat
            self._mark(fr.location)
        return stat

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

    # -- changes ------------------------------------------------------------

    def invalidate(self, fr: TGFSFileRef, pushed: bool = False) -> None:
        """``fr``'s content or dates changed: drop its stored stat.

        ``pushed`` says the caller pushes the metadata itself after this,
        which already writes the change where the stats ride along with
        the tree.
        """
        fr.stat_epoch += 1
        if fr.stat is None and not self._stored(fr.location):
            return
        fr.stat = None
        self.changed(fr.location, pushed)

    def changed(self, directory: TGFSDirectory, pushed: bool = False) -> None:
        """The set of files in ``directory`` changed (a file was added,
        removed or moved), so its stored stats need rewriting."""
        metadata_api = self._metadata_api
        if metadata_api is None:
            return
        if pushed and metadata_api.push_writes_stats:
            return
        if self._persist or self._stored(directory):
            self._mark(directory, urgent=True)

    def _stored(self, directory: TGFSDirectory) -> bool:
        return self._metadata_api is not None and self._metadata_api.stats_stored(
            directory
        )

    # -- writing --------------------------------------------------------------

    def _mark(self, directory: TGFSDirectory, urgent: bool = False) -> None:
        now = time.monotonic()
        if not self._dirty:
            self._dirty_since = now
        self._dirty[id(directory)] = directory
        self._last_mark = now
        self._urgent = self._urgent or urgent
        if not self._closing.is_set() and (
            self._flusher is None or self._flusher.done()
        ):
            self._flusher = asyncio.ensure_future(self._run_flusher())
        self._wake.set()

    def _due_in(self) -> float:
        if self._closing.is_set():
            return 0.0
        quiet, max_delay = (
            (URGENT_QUIET, URGENT_MAX_DELAY)
            if self._urgent
            else (FLUSH_QUIET, FLUSH_MAX_DELAY)
        )
        now = time.monotonic()
        since = self._dirty_since if self._dirty_since is not None else now
        return max(0.0, min(self._last_mark + quiet, since + max_delay) - now)

    async def _run_flusher(self) -> None:
        """Write the marked folders whenever they are due, until none are
        left. Runs on its own, so no request has to wait for the write and
        no cancelled request can stop it."""
        while self._dirty:
            self._wake.clear()
            delay = self._due_in()
            if delay > 0:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=delay)
                    continue  # more marks came in; work out the time again
                except asyncio.TimeoutError:
                    pass
            if await self.flush():
                continue
            if self._closing.is_set():
                return  # one last attempt at shutdown, not a retry loop
            try:
                await asyncio.wait_for(self._closing.wait(), FLUSH_RETRY_DELAY)
            except asyncio.TimeoutError:
                pass

    async def flush(self) -> bool:
        """Write the marked folders now. Returns False when the write
        failed; the folders stay marked for the next attempt."""
        if not self._dirty or self._metadata_api is None:
            return True
        batch, self._dirty = self._dirty, {}
        self._dirty_since = None
        self._urgent = False
        try:
            await self._metadata_api.save_stats(batch.values(), keep=self._persist)
            return True
        except Exception as ex:
            logger.warning(
                f"Could not write the file stats of {len(batch)} folder(s): {ex}"
            )
            for key, directory in batch.items():
                self._dirty.setdefault(key, directory)
            if self._dirty_since is None:
                self._dirty_since = time.monotonic()
            return False

    async def close(self) -> None:
        """Write what is still marked, once; for shutdown."""
        self._closing.set()
        self._wake.set()
        flusher = self._flusher
        if flusher is not None and not flusher.done():
            try:
                await asyncio.wait_for(asyncio.shield(flusher), CLOSE_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning("Gave up waiting for the file stats to be written")
            except Exception as ex:
                logger.warning(f"Could not write the file stats: {ex}")
        else:
            await self.flush()


def _in_tree(fr: TGFSFileRef) -> bool:
    """Whether ``fr`` is still one of its folder's files (not removed or
    replaced by a move while its descriptor was read)."""
    return any(ref is fr for ref in fr.location.find_files([fr.name]))
