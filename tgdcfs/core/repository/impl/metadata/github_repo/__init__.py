import asyncio
import base64
import datetime
import hashlib
import json
import logging
from typing import Iterable, Optional

from github import Github, GithubException, InputGitTreeElement
from github.ContentFile import ContentFile
from github.GitTreeElement import GitTreeElement

from tgdcfs.config import GithubRepoConfig, expand_path
from tgdcfs.core.model import FileStat, TGFSDirectory, TGFSMetadata
from tgdcfs.core.repository.interface import IMetaDataRepository
from tgdcfs.crypto.path_names import (
    PathNameEncryptionError,
    decrypt_path_name,
    is_encrypted_path_name,
)
from tgdcfs.errors import TechnicalError

from .dir_timestamps import DirTimestampStore, cache_file_name
from .gh_directory import GithubConfig, GithubDirectory
from .stats_file import STATS_COMMIT_PREFIX, STATS_FILE_NAME

logger = logging.getLogger(__name__)


# How often a stats commit is retried when the branch moved under it.
STATS_COMMIT_ATTEMPTS = 3


def _blob_sha(content: str) -> str:
    """The git blob sha of ``content``, as GitHub will report it."""
    data = content.encode()
    # Git's object id, not a security measure.
    return hashlib.sha1(
        b"blob %d\0" % len(data) + data, usedforsecurity=False
    ).hexdigest()


class GithubRepoMetadataRepository(IMetaDataRepository):
    def __init__(self, config: GithubRepoConfig, name_key: Optional[bytes] = None):
        super().__init__()
        self._stats_loads: dict[int, asyncio.Future[bool]] = {}

        gh = Github(config.access_token)
        repo = gh.get_repo(config.repo)

        self._timestamps = DirTimestampStore(
            repo=repo,
            branch=config.commit,
            path=expand_path(cache_file_name(config.repo, config.commit)),
        )

        self._ghc = GithubConfig(
            gh=gh,
            repo_name=config.repo,
            repo=repo,
            commit=config.commit,
            name_key=name_key,
            timestamps=self._timestamps,
        )

    async def push(self) -> None:
        pass

    # -- file stats (``listing.stat_in_metadata``) -------------------------
    #
    # Every folder that has stats keeps them in its own ``.tgdcfsstats.json``
    # next to the file refs, keyed by descriptor message id, so a move or a
    # rename keeps them without a write. The tree load only notes the blob
    # sha; the content is read on the folder's first listing.

    push_writes_stats = False

    async def load_stats(self, directory: TGFSDirectory) -> bool:
        if not isinstance(directory, GithubDirectory) or directory.stats_loaded:
            return True
        if directory.stats_sha is None:
            directory.stats_loaded = True
            return True
        # Ten thousand concurrent listings of one folder read the blob once.
        key = id(directory)
        task = self._stats_loads.get(key)
        if task is None:
            task = asyncio.ensure_future(self._load_stats(directory))
            self._stats_loads[key] = task
            task.add_done_callback(lambda _: self._stats_loads.pop(key, None))
        # Shielded: a client that gives up does not cancel the read for the
        # listings that wait on it.
        return await asyncio.shield(task)

    async def _load_stats(self, directory: GithubDirectory) -> bool:
        sha = directory.stats_sha
        if sha is None:
            directory.stats_loaded = True
            return True
        try:
            blob = await asyncio.to_thread(self._ghc.repo.get_git_blob, sha)
            entries = json.loads(base64.b64decode(blob.content))
            if not isinstance(entries, dict):
                raise ValueError("not a JSON object")
        except Exception as ex:
            logger.warning(
                f"Could not read {directory.stats_path} from "
                f"{self._ghc.repo_name}: {ex}"
            )
            return False
        for fr in directory.find_files():
            if fr.stat is None:
                fr.stat = FileStat.from_dict(entries.get(str(fr.message_id)))
        directory.stats_loaded = True
        return True

    def stats_stored(self, directory: TGFSDirectory) -> bool:
        return isinstance(directory, GithubDirectory) and (
            directory.stats_sha is not None
        )

    async def save_stats(
        self, directories: Iterable[TGFSDirectory], keep: bool
    ) -> None:
        """Write the stats files of ``directories`` in one commit.

        Raises when a folder's stored stats could not be read first or the
        commit failed; the caller keeps the folders and tries again.
        """
        changes: list[tuple[GithubDirectory, Optional[str]]] = []
        unreadable: list[str] = []
        # Directories compare by content, so they are told apart by identity.
        for directory in {id(d): d for d in directories}.values():
            if not isinstance(directory, GithubDirectory):
                continue
            if not directory.is_attached():
                # Deleted meanwhile; its stats file went with it.
                continue
            if keep and not await self.load_stats(directory):
                unreadable.append(directory.stats_path)
                continue
            content = self._stats_content(directory) if keep else None
            if content is None:
                if directory.stats_sha is not None:
                    changes.append((directory, None))
            elif _blob_sha(content) != directory.stats_sha:
                changes.append((directory, content))

        if changes:
            # Paths and contents are fixed here, on the event loop; the
            # thread only talks to GitHub.
            writes = [(d.stats_path, content) for d, content in changes]
            await asyncio.to_thread(self._commit_stats, writes)
            for directory, content in changes:
                directory.stats_sha = None if content is None else _blob_sha(content)
                directory.stats_loaded = True

        if unreadable:
            raise TechnicalError(
                f"Could not read the stored stats of {', '.join(unreadable)}"
            )

    @staticmethod
    def _stats_content(directory: GithubDirectory) -> Optional[str]:
        entries = {
            str(fr.message_id): fr.stat.to_dict()
            for fr in directory.find_files()
            if fr.stat is not None
        }
        if not entries:
            return None
        return json.dumps(entries, sort_keys=True, separators=(",", ":"))

    def _commit_stats(self, writes: list[tuple[str, Optional[str]]]) -> None:
        """One commit on top of the branch head; retried when another
        commit landed in between (the ref update is not forced)."""
        repo = self._ghc.repo
        elements = [
            (
                InputGitTreeElement(path=path, mode="100644", type="blob", sha=None)
                if content is None
                else InputGitTreeElement(
                    path=path, mode="100644", type="blob", content=content
                )
            )
            for path, content in writes
        ]
        for attempt in range(STATS_COMMIT_ATTEMPTS):
            ref = repo.get_git_ref(f"heads/{self._ghc.commit}")
            base_commit = repo.get_git_commit(ref.object.sha)
            tree = repo.create_git_tree(elements, base_tree=base_commit.tree)
            commit = repo.create_git_commit(
                f"{STATS_COMMIT_PREFIX} of {len(writes)} folder(s)",
                tree,
                [base_commit],
            )
            try:
                ref.edit(commit.sha)
                logger.info(
                    f"Wrote the stats of {len(writes)} folder(s) "
                    f"to {self._ghc.repo_name}"
                )
                return
            except GithubException as ex:
                if attempt + 1 == STATS_COMMIT_ATTEMPTS:
                    raise
                logger.info(
                    f"Branch {self._ghc.commit} moved while writing file stats "
                    f"({ex.status}), retrying"
                )

    async def get(self) -> TGFSMetadata:
        # One call to read the repo head plus at most one to diff it against
        # the cached one. Off the event loop because PyGithub is synchronous.
        await asyncio.to_thread(self._timestamps.load)
        root_dir = self._build_directory_structure()
        return TGFSMetadata(dir=root_dir)

    def _build_directory_structure(self) -> GithubDirectory:
        root = GithubDirectory(
            self._ghc, name="root", parent=None, children=[], files=[]
        )
        self._restore_root_timestamps(root)

        try:
            entries = self._read_tree()
            if entries is not None:
                self._build_from_tree(entries, root)
            else:
                contents = self._ghc.repo.get_contents("", ref=self._ghc.commit)
                self._process_contents(contents, root)
        except Exception as ex:
            logger.error(ex)

        return root

    def _read_tree(self) -> Optional[list[GitTreeElement]]:
        """The whole repo tree in one request, or ``None`` if it is unusable.

        ``get_contents`` lists exactly one directory, so walking the tree with
        it costs one round trip per directory -- 306 of them here, all before
        a single file can be listed. The recursive tree API returns every blob
        and subtree at once instead, which makes the load independent of how
        many folders the repo holds.

        GitHub truncates the response for very large trees. That would silently
        hide whole folders, so a truncated tree is refused and the caller walks
        the old way rather than serving an incomplete filesystem.
        """
        try:
            tree = self._ghc.repo.get_git_tree(self._ghc.commit, recursive=True)
            if tree.truncated:
                logger.warning(
                    f"Tree of {self._ghc.repo_name}@{self._ghc.commit} came back "
                    "truncated; falling back to a directory-by-directory walk"
                )
                return None
            return list(tree.tree)
        except Exception as ex:
            logger.warning(
                f"Could not read the tree of {self._ghc.repo_name}@{self._ghc.commit}, "
                f"falling back to a directory-by-directory walk: {ex}"
            )
            return None

    def _build_from_tree(
        self, entries: list[GitTreeElement], root: GithubDirectory
    ) -> None:
        """Materialise the in-memory tree from one flat list of tree entries.

        Entries carry full storage paths and arrive in no guaranteed order, so
        directories are resolved on demand and every missing parent is created
        along the way.
        """
        dirs: dict[str, GithubDirectory] = {"": root}

        for entry in entries:
            if entry.type == "tree":
                self._dir_at(entry.path, dirs)
            elif entry.type == "blob":
                parent_path, _, segment = entry.path.rpartition("/")
                if segment == ".gitkeep":
                    continue
                if segment == STATS_FILE_NAME:
                    # Read on the folder's first listing, not here.
                    self._dir_at(parent_path, dirs).stats_sha = entry.sha
                    continue
                try:
                    stored_name, message_id = segment.rsplit(".", 1)
                    file_name, _ = self._decode_name(stored_name)
                    # attach, not create: the directory's date is the git
                    # history's, not the moment this tree was rebuilt.
                    self._dir_at(parent_path, dirs).attach_file_ref(
                        file_name, int(message_id)
                    )
                except ValueError:
                    logger.warning(
                        f"Invalid name format for {segment}, expected a format like 'name.message_id'"
                    )

    def _dir_at(
        self, storage_path: str, dirs: dict[str, GithubDirectory]
    ) -> GithubDirectory:
        """The directory at ``storage_path``, created with its parents if new."""
        found = dirs.get(storage_path)
        if found is not None:
            return found

        parent_path, _, segment = storage_path.rpartition("/")
        dir_name, was_encrypted = self._decode_name(segment)
        child = self._create_child_dir(
            dir_name,
            self._dir_at(parent_path, dirs),
            stored_encrypted=was_encrypted,
            defer_timestamps=True,
        )
        # Dates come from the cache on first read. Warming them here only
        # queues the path; the workers pay for the lookup in the background
        # instead of holding the load for two calls per directory.
        if self._ghc.timestamps is not None:
            self._ghc.timestamps.request(storage_path)
        dirs[storage_path] = child
        return child

    def _restore_root_timestamps(self, root: GithubDirectory) -> None:
        """Give the root its real dates from the repo's own metadata.

        The root has no ``.gitkeep`` to date it from, so fall back to the
        backing repository's creation and last-push timestamps instead of
        the ``now()`` dataclass default. Best-effort: any failure leaves the
        default rather than breaking the load.
        """
        try:
            created = self._ghc.repo.created_at
            modified = self._ghc.repo.pushed_at or self._ghc.repo.updated_at
            if isinstance(created, datetime.datetime):
                root.created_at = created
            if isinstance(modified, datetime.datetime):
                root.modified_at = modified
        except Exception as ex:
            logger.debug(f"Could not read repo timestamps for root: {ex}")

    def _create_child_dir(
        self,
        name: str,
        parent_dir: GithubDirectory,
        stored_encrypted: bool = False,
        defer_timestamps: bool = False,
    ) -> GithubDirectory:
        child_dir = GithubDirectory(
            self._ghc,
            name,
            parent_dir,
            stored_encrypted=stored_encrypted,
            defer_timestamps=defer_timestamps,
        )
        parent_dir.children.append(child_dir)
        return child_dir

    def _decode_name(self, raw: str) -> tuple[str, bool]:
        """Map an on-repo path segment to its (plaintext, was_encrypted) form.

        Legacy plaintext segments pass through unchanged; encrypted ones are
        decrypted with the configured key. This is what lets encrypted and
        plaintext entries coexist in the same repo during/after migration.
        """
        if not is_encrypted_path_name(raw):
            return raw, False
        key = self._ghc.name_key
        if key is None:
            return raw, True  # cannot decrypt without the key
        try:
            return decrypt_path_name(key, raw), True
        except PathNameEncryptionError as ex:
            logger.warning(f"Failed to decrypt path name {raw!r}: {ex}")
            return raw, True

    def _process_contents(
        self, contents: list[ContentFile] | ContentFile, parent_dir: GithubDirectory
    ) -> None:
        if not isinstance(contents, list):
            contents = [contents]

        for content in contents:
            if content.type == "dir":
                # content.name is the on-repo (possibly encrypted) segment;
                # decrypt it for the in-memory model but keep using
                # content.path (the storage path) for the git-history lookup.
                dir_name, was_encrypted = self._decode_name(content.name)
                child_dir = self._create_child_dir(
                    dir_name,
                    parent_dir,
                    stored_encrypted=was_encrypted,
                    defer_timestamps=True,
                )
                # Dates come from the cache on first read. Warming them here
                # only queues the path; the workers pay for the lookup in the
                # background instead of holding the boot for two calls each.
                if self._ghc.timestamps is not None:
                    self._ghc.timestamps.request(content.path)
                try:
                    child_contents = self._ghc.repo.get_contents(
                        content.path, ref=self._ghc.commit
                    )
                    self._process_contents(child_contents, child_dir)
                except Exception as ex:
                    logger.warning(
                        f"Failed to construct directory {content.name}: {ex}"
                    )
            elif content.type == "file":
                try:
                    if content.name == ".gitkeep":
                        continue
                    if content.name == STATS_FILE_NAME:
                        parent_dir.stats_sha = content.sha
                        continue
                    stored_name, message_id = content.name.rsplit(".", 1)
                    file_name, _ = self._decode_name(stored_name)
                    parent_dir.attach_file_ref(file_name, int(message_id))
                except ValueError:
                    logger.warning(
                        f"Invalid name format for {content.name}, expected a format like 'name.message_id'"
                    )
