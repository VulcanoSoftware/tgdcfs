import asyncio
import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Optional, TypeVar

import requests
from github import Github, InputGitTreeElement
from github.Repository import Repository

from tgdcfs.core.model import TGFSDirectory, TGFSFileRef
from tgdcfs.crypto.path_names import encrypt_path_name
from tgdcfs.utils.time import FIRST_DAY_OF_EPOCH

from .dir_timestamps import DirTimestampStore
from .stats_file import STATS_FILE_NAME

logger = logging.getLogger(__name__)

T = TypeVar("T")

# A dropped connection (GitHub or the network hiccups for a few seconds) is
# tried again briefly; a WebDAV client gives up after about 20 s.
PUT_ATTEMPTS = 3
PUT_RETRY_DELAY = 1.5

# A deferred file reference write (an SFTP open does not wait for it) can
# take much more punishment: ten rounds over roughly ten minutes before the
# reference is given up on.
DEFERRED_PUT_ATTEMPTS = 10
DEFERRED_PUT_RETRY_DELAY = 5


def _is_transient(ex: Exception) -> bool:
    status = getattr(ex, "status", None)
    if status is not None:
        return status >= 500
    return isinstance(ex, (requests.exceptions.RequestException, OSError))


def _is_sha_missing(ex: Exception) -> bool:
    status = getattr(ex, "status", None)
    if status != 422:
        return False
    data = getattr(ex, "data", None)
    message = data.get("message", "") if isinstance(data, dict) else ""
    return '"sha" wasn\'t supplied' in str(message)


@dataclass
class GithubConfig:
    gh: Github
    repo_name: str
    repo: Repository
    commit: str
    # Deterministic AES-SIV key for path-name encryption. ``None`` keeps the
    # legacy behaviour of storing plaintext directory/file names in the repo.
    name_key: Optional[bytes] = None
    # Where the directory dates come from. ``None`` leaves every loaded
    # directory on the ``now()`` default, as it was before the cache existed.
    timestamps: Optional[DirTimestampStore] = None
    # PyGithub is synchronous: a write sleeps through GitHub's rate limits
    # and retries for as long as it takes, and on the event loop that froze
    # every other request (uploads timed out in FileZilla after 20 s of
    # silence while a 20k-file upload was throttled). The writes run on this
    # one thread instead. One thread, because every write is a commit on the
    # same branch: two at once only race each other into 409/422 conflicts.
    writer: ThreadPoolExecutor = field(
        default_factory=lambda: ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="github-writes"
        ),
        compare=False,
        repr=False,
    )
    # Serialises the file-reference blob writes of every directory that
    # shares this repo. A delete or rename that lands while a deferred
    # create is still retrying must not be undone by that retry -- the
    # deleted or renamed file would reappear after a restart. The lock is
    # FIFO-fair, so the write that asked first also runs first, and the
    # in-tree check a deferred task does under the lock always sees the
    # deletes that waited before it. The writer thread keeps the blocking
    # blob calls themselves from interleaving.
    lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        compare=False,
        repr=False,
    )

    async def write(self, fn: Callable[..., T], *args) -> T:
        """Run the blocking GitHub write ``fn`` off the event loop, after
        the writes queued before it."""
        return await asyncio.get_running_loop().run_in_executor(self.writer, fn, *args)


class GithubDirectory(TGFSDirectory):
    def __init__(
        self,
        ghc: GithubConfig,
        name: str,
        parent: Optional[TGFSDirectory],
        children: Optional[list[TGFSDirectory]] = None,
        files: Optional[list[TGFSFileRef]] = None,
        stored_encrypted: Optional[bool] = None,
        defer_timestamps: bool = False,
    ):
        # These have to exist before the dataclass initialiser runs: it assigns
        # created_at/modified_at, which goes through the setters below.
        self._created_at = self._modified_at = FIRST_DAY_OF_EPOCH
        self._created_pending = defer_timestamps
        self._modified_pending = defer_timestamps
        self._ts_initialised = False

        super().__init__(name, parent, children or [], files or [])
        self._ghc = ghc
        # Whether THIS directory is stored under an encrypted name in the
        # repo. New directories follow the configured key; directories loaded
        # from the repo carry whatever form they were found in, so encrypted
        # and legacy-plaintext folders can be read and written side by side.
        self._stored_encrypted = (
            stored_encrypted
            if stored_encrypted is not None
            else ghc.name_key is not None
        )
        self._ts_initialised = True
        # Blob sha of this folder's stats file in the repo, ``None`` when
        # there is none; whether its content has been put on the file refs.
        self.stats_sha: Optional[str] = None
        self.stats_loaded = False
        # Background blob writes started by ``create_file_ref_deferred``;
        # only kept here so they are not garbage collected mid-flight.
        self._deferred_refs: set[asyncio.Task] = set()

    # --- dates ---------------------------------------------------------------
    #
    # A directory loaded from the repo does not know its dates yet: reading them
    # costs two GitHub calls, and doing that for every directory while the tree
    # is built is what made the boot grow with the folder count. So the load
    # marks them pending and they are filled in from the cache on first read --
    # never by blocking the reader, because a PROPFIND of a folder with a
    # hundred children would otherwise pay for a hundred round trips at once.

    @property
    def created_at(self) -> datetime.datetime:
        self._resolve_timestamps()
        return self._created_at

    @created_at.setter
    def created_at(self, value: datetime.datetime) -> None:
        self._created_at = value
        if self._ts_initialised:
            # An explicit write knows better than the git history.
            self._created_pending = False

    @property
    def modified_at(self) -> datetime.datetime:
        self._resolve_timestamps()
        return self._modified_at

    @modified_at.setter
    def modified_at(self, value: datetime.datetime) -> None:
        self._modified_at = value
        if self._ts_initialised:
            self._modified_pending = False

    def _resolve_timestamps(self) -> None:
        if not (self._created_pending or self._modified_pending):
            return

        store = getattr(getattr(self, "_ghc", None), "timestamps", None)
        if store is None:
            return

        path = self._github_path
        if not path:
            # The root is dated from the repo's own metadata, which is free.
            self._created_pending = self._modified_pending = False
            return

        found = store.get(path)
        if found is None:
            # Queued for the background workers; report the default until then.
            return

        created, modified = found
        if self._created_pending:
            self._created_at = created
            self._created_pending = False
        if self._modified_pending:
            self._modified_at = modified
            self._modified_pending = False

    def is_attached(self) -> bool:
        """Whether this folder is still part of the tree (not deleted)."""
        node: TGFSDirectory = self
        while node.parent is not None:
            if not any(child is node for child in node.parent.children):
                return False
            node = node.parent
        return True

    @property
    def stats_path(self) -> str:
        """Repo path of this folder's stats file."""
        return self.join_path(self._github_path, STATS_FILE_NAME)

    @staticmethod
    def join_path(*args: str) -> str:
        """Join paths in a way that is compatible with GitHub"""
        return "/".join(part.strip("/") for part in args if part)

    @property
    def _storage_name(self) -> str:
        """The on-repo name of this directory (encrypted when applicable)."""
        key = self._ghc.name_key
        if key is not None and self._stored_encrypted:
            return encrypt_path_name(key, self.name)
        return self.name

    def _file_ref_storage_name(self, name: str, message_id: int) -> str:
        """On-repo filename ``<name>.<id>``, with ``<name>`` encrypted if keyed."""
        key = self._ghc.name_key
        if key is not None:
            return f"{encrypt_path_name(key, name)}.{message_id}"
        return f"{name}.{message_id}"

    def _message_name(self, name: str) -> str:
        """``name`` as it may appear in a commit message: encrypted when
        keyed, like the path it belongs to."""
        key = self._ghc.name_key
        return encrypt_path_name(key, name) if key is not None else name

    @property
    def _github_path(self) -> str:
        """Get the GitHub repository path for this directory"""
        if self.parent is None:
            return ""

        if isinstance(self.parent, GithubDirectory):
            parent_path = self.parent._github_path
        else:
            parent_path = ""

        return self.join_path(parent_path, self._storage_name)

    def create_dir_skip_github_ops(self, name: str) -> "GithubDirectory":
        res = GithubDirectory(self._ghc, name, self)
        self.children.append(res)
        return res

    def create_dir(self, name: str) -> "GithubDirectory":
        child = super().create_dir(name)

        # Create directory in GitHub by creating a placeholder file. The new
        # directory follows the configured key, so its on-repo segment is the
        # encrypted name when path-name encryption is enabled.
        key = self._ghc.name_key
        child_segment = encrypt_path_name(key, name) if key is not None else name
        dir_path = self.join_path(self._github_path, child_segment, ".gitkeep")
        try:
            self._ghc.repo.create_file(
                path=dir_path,
                # The on-repo segment, never the plaintext name: commit
                # messages are as public as the paths.
                message=f"Create directory {child_segment}",
                content="",
                branch=self._ghc.commit,
            )
            logger.info(f"Created directory {name} in GitHub repository at {dir_path}")
        except Exception as ex:
            logger.error(f"Failed to create directory {name} in GitHub: {ex}")
            self.children.remove(child)
            raise

        # Convert the child to GithubDirectory
        github_child = GithubDirectory(
            ghc=self._ghc,
            name=child.name,
            parent=self,
            children=child.children,
            files=child.files,
        )

        # Replace the child in the parent's children list
        child_index = self.children.index(child)
        self.children[child_index] = github_child

        return github_child

    def delete(self) -> None:
        if self.parent:
            # Remove all files and subdirectories from GitHub
            self._delete_github_directory()
        super().delete()

    def move_to(
        self, new_parent: TGFSDirectory, new_name: Optional[str] = None
    ) -> None:
        old_parent, old_name = self.parent, self.name
        old_prefix = self._github_path
        super().move_to(new_parent, new_name)
        new_prefix = self._github_path
        if not old_prefix or not new_prefix or old_prefix == new_prefix:
            return
        try:
            self._move_github_directory(old_prefix, new_prefix)
        except Exception:
            # The repo *is* the metadata here, so an in-memory tree that the
            # repo does not back would only survive until the next reload.
            # Put the directory back where it was instead.
            if old_parent is not None and self.parent is not None:
                self.parent.children.remove(self)
                self.name = old_name
                self.parent = old_parent
                old_parent.children.append(self)
            raise

    def _move_github_directory(self, old_prefix: str, new_prefix: str) -> None:
        """Re-path this directory's subtree in a single commit.

        The Contents API has no rename, and deleting plus re-creating every
        blob would lose the git history the timestamps are restored from, so
        rewrite the tree with the affected blobs moved to their new paths --
        the same Git Data API round-trip ``_delete_github_directory`` uses.
        """
        try:
            ref = self._ghc.repo.get_git_ref(f"heads/{self._ghc.commit}")
            base_commit = self._ghc.repo.get_git_commit(ref.object.sha)
            tree = self._ghc.repo.get_git_tree(base_commit.tree.sha, recursive=True)

            elements: list[InputGitTreeElement] = []
            moved = 0
            for entry in tree.tree:
                if entry.type != "blob":
                    continue
                path = entry.path
                if path == old_prefix or path.startswith(old_prefix + "/"):
                    path = new_prefix + path[len(old_prefix) :]
                    moved += 1
                elements.append(
                    InputGitTreeElement(
                        path=path,
                        mode=entry.mode,
                        type="blob",
                        sha=entry.sha,
                    )
                )

            if moved == 0:
                return

            new_tree = self._ghc.repo.create_git_tree(elements)
            new_commit = self._ghc.repo.create_git_commit(
                f"Move directory {old_prefix} to {new_prefix}", new_tree, [base_commit]
            )
            ref.edit(new_commit.sha)
            logger.info(
                f"Moved {moved} object(s) from {old_prefix} to {new_prefix} "
                f"in {self._ghc.repo_name}"
            )
        except Exception as ex:
            logger.error(
                f"Failed to move directory {old_prefix} to {new_prefix} "
                f"in {self._ghc.repo_name}: {ex}"
            )
            raise

    def create_file_ref(self, name: str, file_message_id: int) -> TGFSFileRef:
        file_ref = super().create_file_ref(name, file_message_id)
        try:
            self._put_file_ref(name, file_message_id)
        except Exception as ex:
            logger.error(
                f"Failed to create file reference {name} in "
                f"{self._ghc.repo_name}: {ex}"
            )
            self.files.remove(file_ref)
            raise
        return file_ref

    async def create_file_ref_async(
        self, name: str, file_message_id: int
    ) -> TGFSFileRef:
        # The in-memory tree is only ever touched on the event loop; just
        # the GitHub call goes to the writer thread.
        file_ref = super().create_file_ref(name, file_message_id)
        try:
            async with self._ghc.lock:
                await self._ghc.write(self._put_file_ref, name, file_message_id)
        except Exception as ex:
            logger.error(
                f"Failed to create file reference {name} in "
                f"{self._ghc.repo_name}: {ex}"
            )
            self.files.remove(file_ref)
            raise
        return file_ref

    async def create_file_ref_deferred(
        self, name: str, file_message_id: int
    ) -> TGFSFileRef:
        """Create the reference now, write the blob in the background.

        The in-memory part runs right away so the name shows up in a
        listing and a following upload finds it; only the GitHub blob
        write is deferred, so a GitHub outage cannot hold an SFTP open --
        and with it the client's transfer -- hostage. A write that keeps
        failing is treated like a failing ``create_file_ref_async``: the
        reference leaves the tree and the error is logged.
        """
        file_ref = super().create_file_ref(name, file_message_id)
        task = asyncio.create_task(
            self._put_file_ref_patiently(name, file_message_id, file_ref),
            name=f"github-file-ref-{name}",
        )
        self._deferred_refs.add(task)
        task.add_done_callback(self._deferred_refs.discard)
        return file_ref

    async def _put_file_ref_patiently(
        self, name: str, file_message_id: int, file_ref: TGFSFileRef
    ) -> None:
        for attempt in range(DEFERRED_PUT_ATTEMPTS):
            try:
                async with self._ghc.lock:
                    # A delete or rename that waited for the lock took the
                    # reference out of the tree; writing its blob anyway
                    # would resurrect the deleted file after a restart.
                    if not any(r is file_ref for r in self.files):
                        return
                    await self._ghc.write(self._put_file_ref, name, file_message_id)
                return
            except Exception as ex:
                if attempt == DEFERRED_PUT_ATTEMPTS - 1 or not _is_transient(ex):
                    logger.error(
                        f"Failed to create file reference {name} in "
                        f"{self._ghc.repo_name}: {ex}"
                    )
                    self.files.remove(file_ref)
                    return
                logger.warning(
                    f"Creating file reference {name} failed ({ex}); trying again"
                )
                await asyncio.sleep(DEFERRED_PUT_RETRY_DELAY * (attempt + 1))

    def _put_file_ref(self, name: str, file_message_id: int) -> None:
        """Create the file reference blob in the repo (filename encrypted
        when keyed). Blocking; raises when GitHub refuses."""
        file_path = self.join_path(
            self._github_path, self._file_ref_storage_name(name, file_message_id)
        )
        for attempt in range(PUT_ATTEMPTS):
            try:
                self._ghc.repo.create_file(
                    path=file_path,
                    message=f"Create file reference for {self._message_name(name)}",
                    content="",
                    branch=self._ghc.commit,
                )
                logger.info(
                    f"Created file reference {name} in {self._ghc.repo_name} "
                    f"at {file_path}"
                )
                return
            except Exception as ex:
                if _is_sha_missing(ex) or (
                    attempt > 0 and getattr(ex, "status", None) == 422
                ):
                    # GitHub only sends "sha wasn't supplied" when the blob
                    # is already there: an earlier attempt got through
                    # although its answer was lost. After an earlier
                    # attempt, any 422 means the same -- the first attempt
                    # was transient (otherwise it would have raised), so
                    # this one is not a fresh validation error.
                    logger.info(
                        f"Created file reference {name} in {self._ghc.repo_name} "
                        f"at {file_path} (an earlier attempt landed)"
                    )
                    return
                if attempt == PUT_ATTEMPTS - 1 or not _is_transient(ex):
                    raise
                logger.warning(
                    f"Creating file reference {name} failed ({ex}); trying again"
                )
                time.sleep(PUT_RETRY_DELAY * (attempt + 1))

    def delete_file_ref(self, fr: TGFSFileRef) -> None:
        self._drop_file_ref(fr)
        super().delete_file_ref(fr)

    async def delete_file_ref_async(self, fr: TGFSFileRef) -> None:
        # Under the lock, so a deferred create that is still retrying
        # cannot re-create this blob after the drop (its in-tree check
        # only helps if the removal happened before that check).
        async with self._ghc.lock:
            await self._ghc.write(self._drop_file_ref, fr)
            super().delete_file_ref(fr)

    def _drop_file_ref(self, fr: TGFSFileRef) -> None:
        """Remove the file reference blob from the repo. Blocking; a blob
        that cannot be found is logged, not raised."""
        # The ref may be stored under its encrypted or its legacy-plaintext
        # name (mixed repos), so try the encrypted form first, then plaintext.
        candidates = []
        if self._ghc.name_key is not None:
            candidates.append(self._file_ref_storage_name(fr.name, fr.message_id))
        candidates.append(f"{fr.name}.{fr.message_id}")

        for segment in dict.fromkeys(candidates):  # de-dup, keep order
            file_path = self.join_path(self._github_path, segment)
            try:
                file_content = self._ghc.repo.get_contents(
                    file_path, ref=self._ghc.commit
                )
                if isinstance(file_content, list):
                    file_content = file_content[0]
                # The name as this segment stores it: encrypted, or the
                # legacy plaintext the path already shows.
                stored_name = segment.rsplit(".", 1)[0]
                self._ghc.repo.delete_file(
                    path=file_path,
                    message=f"Delete file reference for {stored_name}",
                    sha=file_content.sha,
                    branch=self._ghc.commit,
                )
                logger.info(
                    f"Deleted file reference {fr.name} from {self._ghc.repo_name}"
                )
                break
            except Exception as ex:
                logger.debug(
                    f"Could not delete {file_path} from {self._ghc.repo_name}: {ex}"
                )
        else:
            logger.error(
                f"Failed to delete file reference {fr.name} "
                f"from {self._ghc.repo_name}: not found"
            )

    def _delete_github_directory(self) -> None:
        """Remove this directory and everything beneath it in one commit.

        ``delete_file`` can only remove a single file — it returns HTTP 422
        on a subtree — so a per-entry delete cannot recurse into
        subdirectories (their nested ``.gitkeep`` and files would survive).
        Instead we rewrite the git tree without any blob under this
        directory's path, dropping arbitrarily-nested content atomically in
        a single commit via the Git Data API.
        """
        prefix = self._github_path
        if not prefix:
            return  # guard: never wipe the whole repo via the root
        try:
            ref = self._ghc.repo.get_git_ref(f"heads/{self._ghc.commit}")
            base_commit = self._ghc.repo.get_git_commit(ref.object.sha)
            tree = self._ghc.repo.get_git_tree(base_commit.tree.sha, recursive=True)

            kept: list[InputGitTreeElement] = []
            removed = 0
            for entry in tree.tree:
                if entry.type != "blob":
                    continue
                if entry.path == prefix or entry.path.startswith(prefix + "/"):
                    removed += 1
                    continue
                kept.append(
                    InputGitTreeElement(
                        path=entry.path,
                        mode=entry.mode,
                        type="blob",
                        sha=entry.sha,
                    )
                )

            if removed == 0:
                return

            new_tree = self._ghc.repo.create_git_tree(kept)
            new_commit = self._ghc.repo.create_git_commit(
                f"Delete directory {prefix}", new_tree, [base_commit]
            )
            ref.edit(new_commit.sha)
            logger.info(
                f"Deleted {removed} object(s) under {prefix} "
                f"from {self._ghc.repo_name}"
            )
        except Exception as ex:
            logger.error(
                f"Failed to delete directory {prefix} "
                f"from {self._ghc.repo_name}: {ex}"
            )
