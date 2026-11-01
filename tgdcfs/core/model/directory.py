import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Self

from tgdcfs.backends.base import normalize_store_key
from tgdcfs.errors import (
    FileOrDirectoryAlreadyExists,
    FileOrDirectoryDoesNotExist,
    InvalidPath,
    TechnicalError,
)
from tgdcfs.utils.time import FIRST_DAY_OF_EPOCH, ts

from .common import validate_name
from .serialized import TGFSDirectorySerialized, TGFSFileRefSerialized
from .stat import FileStat


@dataclass
class TGFSFileRef:
    message_id: int
    name: str
    location: "TGFSDirectory" = field(repr=False)

    # Mirror store key -> message id of the file descriptor copy in that
    # store. Empty when redundancy is off.
    mirrors: Dict[str, int] = field(default_factory=dict)

    # Key of the store ``message_id`` belongs to; ``None`` for refs written
    # by tgfs, which belong to the configured primary. See
    # ``TGFSFileVersion.store``.
    store: Optional[str] = None

    # Size and dates for listings, kept in the metadata with
    # ``listing.stat_in_metadata``; ``None`` when not known there. Readers
    # that do not know the field (tgfs) ignore it.
    stat: Optional[FileStat] = field(default=None, compare=False)

    # Bumped whenever ``stat`` is dropped because the file changed, so a
    # stat read from the descriptor before the change is not stored.
    stat_epoch: int = field(default=0, compare=False, repr=False)

    def to_dict(self) -> TGFSFileRefSerialized:
        res = TGFSFileRefSerialized(
            type="FR",
            messageId=self.message_id,
            name=self.name,
        )
        if self.mirrors:
            res["mirrors"] = self.mirrors
        if self.store:
            res["store"] = self.store
        if self.stat is not None:
            res["stat"] = self.stat.to_dict()
        return res

    def owned_by(self, store_key: str) -> bool:
        return self.store is None or self.store == store_key

    def relocate(self, primary_key: str) -> None:
        """Re-express the ref relative to a new primary store.

        Mirrors ``TGFSFileVersion.relocate``: the descriptor id of the old
        primary becomes that store's mirror entry, and the new primary's
        copy, if any, becomes ``message_id``.
        """
        if self.owned_by(primary_key) or self.store is None:
            return
        self.mirrors.setdefault(self.store, self.message_id)
        if (mid := self.mirrors.get(primary_key)) and mid > 0:
            del self.mirrors[primary_key]
            self.message_id = mid
            self.store = primary_key

    def delete(self) -> None:
        self.location.delete_file_ref(self)


class FileRefList(list[TGFSFileRef]):
    """The file refs of a directory, with an index by name.

    Every stat of a file looks its ref up by name, and a WebDAV listing
    stats each member of the folder: with a plain list that is one scan
    of the folder per member, which blocked the event loop for tens of
    seconds on a folder of ten thousand files. The index makes the
    lookup constant. Every way of changing the list goes through the
    overridden methods, so the index never drifts from the list; refs
    are indexed by identity because ``TGFSFileRef.__eq__`` compares the
    whole tree behind ``location``.
    """

    def __init__(self, refs: Iterable[TGFSFileRef] = ()):
        super().__init__(refs)
        self._by_name: Dict[str, List[TGFSFileRef]] = {}
        for ref in self:
            self._index(ref)

    def _index(self, ref: TGFSFileRef) -> None:
        self._by_name.setdefault(ref.name, []).append(ref)

    def _unindex(self, ref: TGFSFileRef) -> None:
        refs = self._by_name.get(ref.name)
        if refs is None:
            return
        refs[:] = [r for r in refs if r is not ref]
        if not refs:
            del self._by_name[ref.name]

    def _reindex(self) -> None:
        self._by_name = {}
        for ref in self:
            self._index(ref)

    def named(self, name: str) -> List[TGFSFileRef]:
        """The refs called ``name``, in list order."""
        return list(self._by_name.get(name, ()))

    # -- list mutators -----------------------------------------------------

    def append(self, ref: TGFSFileRef) -> None:
        super().append(ref)
        self._index(ref)

    def extend(self, refs: Iterable[TGFSFileRef]) -> None:
        for ref in refs:
            self.append(ref)

    def insert(self, index: Any, ref: TGFSFileRef) -> None:
        super().insert(index, ref)
        self._index(ref)

    def remove(self, ref: TGFSFileRef) -> None:
        super().remove(ref)
        self._unindex(ref)

    def pop(self, index: Any = -1) -> TGFSFileRef:
        ref = super().pop(index)
        self._unindex(ref)
        return ref

    def clear(self) -> None:
        super().clear()
        self._by_name = {}

    def __setitem__(self, index: Any, value: Any) -> None:
        super().__setitem__(index, value)
        self._reindex()

    def __delitem__(self, index: Any) -> None:
        super().__delitem__(index)
        self._reindex()

    def __iadd__(self, refs: Iterable[TGFSFileRef]) -> "FileRefList":  # type: ignore[override,misc]
        self.extend(refs)
        return self

    def __imul__(self, n: Any) -> "FileRefList":  # type: ignore[override,misc]
        super().__imul__(n)
        self._reindex()
        return self

    def __reduce__(self) -> Any:
        # Copies (``copy.deepcopy``, pickling) rebuild the index from the
        # items instead of carrying a stale one over.
        return (FileRefList, (list(self),))


@dataclass
class TGFSDirectory:
    name: str
    parent: Optional["TGFSDirectory"]
    children: list["TGFSDirectory"] = field(default_factory=list)
    files: list[TGFSFileRef] = field(default_factory=list)
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    modified_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def __post_init__(self):
        validate_name(self.name)

    def __setattr__(self, key: str, value: Any) -> None:
        # ``files`` is assigned by the dataclass initialiser and by
        # ``from_dict``; whatever list arrives is kept as the indexed one.
        if key == "files" and not isinstance(value, FileRefList):
            value = FileRefList(value)
        super().__setattr__(key, value)

    @property
    def created_at_timestamp(self) -> int:
        return ts(self.created_at)

    @property
    def modified_at_timestamp(self) -> int:
        return ts(self.modified_at)

    def _touch_modified(self) -> None:
        self.modified_at = datetime.datetime.now()

    def to_dict(self) -> TGFSDirectorySerialized:
        return TGFSDirectorySerialized(
            type="D",
            name=self.name,
            createdAt=self.created_at_timestamp,
            modifiedAt=self.modified_at_timestamp,
            children=[child.to_dict() for child in self.children],
            files=[file.to_dict() for file in self.files],
        )

    @staticmethod
    def from_dict(
        data: TGFSDirectorySerialized, parent: Optional["TGFSDirectory"] = None
    ) -> "TGFSDirectory":
        def _read_ts(value: int) -> datetime.datetime:
            if value > 0:
                return datetime.datetime.fromtimestamp(value / 1000)
            return FIRST_DAY_OF_EPOCH

        d = TGFSDirectory(
            name=data["name"],
            parent=parent,
            children=[],
            files=[],
            created_at=_read_ts(data.get("createdAt", 0) or 0),
            modified_at=_read_ts(data.get("modifiedAt", 0) or 0),
        )

        if data["files"]:
            d.files = [
                TGFSFileRef(
                    message_id=file["messageId"],
                    name=file["name"],
                    location=d,
                    # Bare keys come from tgfs metadata and mean Telegram.
                    mirrors={
                        normalize_store_key(channel): int(mid)
                        for channel, mid in (file.get("mirrors") or {}).items()
                    },
                    store=(
                        normalize_store_key(file["store"])
                        if file.get("store")
                        else None
                    ),
                    stat=FileStat.from_dict(file.get("stat")),
                )
                for file in data["files"]
                if file["name"] and file["messageId"]
            ]

        d.children = [TGFSDirectory.from_dict(child, d) for child in data["children"]]
        return d

    def create_dir(self, name: str) -> "TGFSDirectory":
        """Create an empty child directory.

        Deliberately does not take a directory to copy: seeding the child
        with another directory's ``children``/``files`` shares the very list
        objects and leaves every file ref pointing back at its old location,
        so the "copy" and the original were one and the same. Copying a
        directory is a recursive operation and lives in ``Ops.cp_dir``.
        """
        if len(self.find_dirs([name])) > 0:
            raise FileOrDirectoryAlreadyExists(name)

        child = TGFSDirectory(name=name, parent=self, children=[], files=[])

        self.children.append(child)
        self._touch_modified()
        return child

    @classmethod
    def root_dir(cls) -> Self:
        return cls(name="root", parent=None)

    def find_dirs(self, names: Iterable[str] = tuple()) -> List["TGFSDirectory"]:
        if not names:
            return self.children
        wanted = frozenset(names)
        return [child for child in self.children if child.name in wanted]

    def find_dir(self, name: str) -> "TGFSDirectory":
        dirs = self.find_dirs([name])
        if not dirs:
            raise FileOrDirectoryDoesNotExist(name)
        return dirs[0]

    def find_files(self, names: Iterable[str] = tuple()) -> List[TGFSFileRef]:
        if not names:
            return self.files
        wanted = tuple(names)
        if len(wanted) == 1:
            # The common case (stat of one file): served from the index.
            return self._indexed_files().named(wanted[0])
        wanted_set = frozenset(wanted)
        return [file for file in self.files if file.name in wanted_set]

    def _indexed_files(self) -> FileRefList:
        files = self.files
        if not isinstance(files, FileRefList):
            # A subclass or a caller could bypass ``__setattr__``; be safe.
            files = FileRefList(files)
            self.files = files
        return files

    def find_file(self, name: str) -> TGFSFileRef:
        files = self.find_files([name])
        if not files:
            raise FileOrDirectoryDoesNotExist(name)
        return files[0]

    def attach_file_ref(self, name: str, fd_message_id: int) -> TGFSFileRef:
        """Add a file ref without dating the directory.

        For rebuilding a tree from storage: the file already existed and the
        directory's dates come from the storage's own history. Dating the
        directory here would stamp it with the boot time instead -- which
        is what every folder with a file in it used to report.
        """
        if self.find_files([name]):
            raise FileOrDirectoryAlreadyExists(name)

        fr = TGFSFileRef(
            message_id=fd_message_id,
            name=name,
            location=self,
        )
        self.files.append(fr)
        return fr

    def create_file_ref(self, name: str, fd_message_id: int) -> TGFSFileRef:
        fr = self.attach_file_ref(name, fd_message_id)
        self._touch_modified()
        return fr

    def delete_file_ref(self, fr: TGFSFileRef) -> None:
        self.files.remove(fr)
        self._touch_modified()

    def relocate_file_ref(
        self, fr: TGFSFileRef, to: "TGFSDirectory", new_name: Optional[str] = None
    ) -> TGFSFileRef:
        """Move ``fr`` to ``to``, keeping the Telegram messages behind it.

        A move must never touch the channel: the descriptor message and every
        content message stay exactly where they are, only the reference to
        them changes place. The new ref is created before the old one is
        dropped so a failing backend leaves the file reachable.
        """
        name = new_name or fr.name
        if to is self and name == fr.name:
            return fr

        moved = to.create_file_ref(name, fr.message_id)
        moved.mirrors = dict(fr.mirrors)
        moved.store = fr.store
        # Same descriptor, same content: what a listing shows moves along,
        # and so does whether it changed since the start.
        moved.stat = fr.stat
        moved.stat_epoch = fr.stat_epoch
        try:
            self.delete_file_ref(fr)
        except Exception:
            to.delete_file_ref(moved)
            raise
        return moved

    def is_ancestor_of(self, other: "TGFSDirectory") -> bool:
        """True when ``other`` is ``self`` or lives somewhere below it."""
        node: Optional["TGFSDirectory"] = other
        while node is not None:
            if node is self:
                return True
            node = node.parent
        return False

    def move_to(
        self, new_parent: "TGFSDirectory", new_name: Optional[str] = None
    ) -> None:
        """Re-parent this directory (and everything below it) in place.

        Like :meth:`relocate_file_ref` this is a pure metadata operation --
        the subtree keeps pointing at the very same Telegram messages.
        """
        if self.parent is None:
            raise TechnicalError("The root directory cannot be moved")

        name = new_name or self.name
        validate_name(name)

        if new_parent is self.parent and name == self.name:
            return

        if self.is_ancestor_of(new_parent):
            raise InvalidPath(f"{self.absolute_path} cannot be moved into itself")

        if new_parent.find_dirs([name]):
            raise FileOrDirectoryAlreadyExists(name)

        old_parent = self.parent
        old_parent.children.remove(self)
        old_parent._touch_modified()

        self.name = name
        self.parent = new_parent
        new_parent.children.append(self)
        new_parent._touch_modified()

    def delete(self) -> None:
        if self.parent:
            self.parent.children.remove(self)
            self.parent._touch_modified()
        else:
            # root directory, just clear its contents
            self.children.clear()
            self.files.clear()
            self._touch_modified()

    @property
    def absolute_path(self) -> str:
        if self.parent is None:
            return ""
        return (
            f"{self.parent.absolute_path}/{self.name}"
            if self.name
            else self.parent.absolute_path
        )
