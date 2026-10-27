import base64
import datetime
import json
import struct
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional
from uuid import uuid4 as uuid

from tgdcfs.backends.base import normalize_store_key
from tgdcfs.reqres import Replica, SentFileMessage
from tgdcfs.utils.time import FIRST_DAY_OF_EPOCH, ts

from .common import validate_name
from .serialized import (
    ReplicaSerialized,
    TGFSFileDescSerialized,
    TGFSFileVersionSerialized,
)

EMPTY_FILE_MESSAGE = -1
INVALID_FILE_SIZE = -1
INVALID_VERSION_ID = ""

# Replica id lists longer than this are stored base64-packed: 19-digit
# Discord snowflakes as JSON cost 21 characters each, packed 11.
_COMPACT_IDS_THRESHOLD = 100


def _encode_ids(ids: List[int]) -> str:
    """Pack message ids as big-endian uint64, base64url without padding."""
    packed = b"".join(struct.pack(">Q", mid) for mid in ids)
    return base64.urlsafe_b64encode(packed).decode("ascii").rstrip("=")


def _decode_ids(encoded: str) -> List[int]:
    padded = encoded + "=" * (-len(encoded) % 4)
    packed = base64.urlsafe_b64decode(padded.encode("ascii"))
    return [struct.unpack(">Q", packed[i : i + 8])[0] for i in range(0, len(packed), 8)]


def replica_to_dict(replica: Replica) -> ReplicaSerialized:
    """Serialize a replica compactly.

    Part sizes collapse to ``[common, last]`` when every part but the last
    has the same size, which is what every store's partitioning produces.
    """
    res: ReplicaSerialized = {}
    if len(replica.message_ids) > _COMPACT_IDS_THRESHOLD:
        res["mb"] = _encode_ids(replica.message_ids)
    else:
        res["m"] = list(replica.message_ids)
    sizes = replica.part_sizes
    if len(sizes) > 2 and len(set(sizes[:-1])) == 1:
        res["p"] = [sizes[0], sizes[-1]]
        res["n"] = len(sizes)
    elif sizes:
        res["p"] = list(sizes)
    return res


def replica_from_dict(data: ReplicaSerialized) -> Replica:
    if (encoded := data.get("mb")) is not None:
        ids = _decode_ids(encoded)
    else:
        ids = list(data.get("m") or [])
    sizes = list(data.get("p") or [])
    if (n := data.get("n")) and len(sizes) == 2 and n > 2:
        sizes = [sizes[0]] * (n - 1) + [sizes[1]]
    return Replica(message_ids=ids, part_sizes=sizes)


@dataclass
class TGFSFileVersion:
    id: str
    updated_at: datetime.datetime
    _size: int = INVALID_FILE_SIZE  # total size

    # file can be split into multiple "file messages", each max 1GB
    message_ids: List[int] = field(default_factory=list)
    part_sizes: List[int] = field(default_factory=list)  # sizes of each part

    # Mirror store key -> message ids of the copies of each part in that
    # store, aligned with message_ids. A 0 entry means "this part has no
    # copy in that store (yet)".
    mirrors: Dict[str, List[int]] = field(default_factory=dict)

    # Store key -> copy of the whole version with the store's own part
    # layout, for stores whose messages are smaller than the primary's
    # parts (a Discord mirror of a Telegram primary). Unlike ``mirrors``
    # the ids are not aligned with ``message_ids``.
    replicas: Dict[str, Replica] = field(default_factory=dict)

    # Key of the store that ``message_ids`` belong to. ``None`` for
    # versions written before the field existed (tgfs), which always
    # belong to the configured primary. When the configured primary is a
    # different store (the mirror was promoted), ``relocate`` re-expresses
    # the version relative to the new primary.
    store: Optional[str] = None

    # True while the bytes live only in the local cache of the instance
    # that accepted the upload (``write_ack: cache``): no store has them
    # yet, so ``message_ids`` is empty. Every reader but that instance
    # treats the version as invalid, exactly like a version whose parts
    # are gone, and serves the previous one until distribution completes.
    pending: bool = False

    # Optional unencrypted (plaintext) logical size. Set when encryption is
    # enabled so PROPFIND / content_length() doesn't need to probe header.
    plaintext_size: Optional[int] = None

    # Whether reads of this version may go through the local read cache.
    # Not serialized: it is False only for the throwaway versions the
    # metadata repository builds around the pinned metadata blob, which
    # changes on every push and would only pollute the cache.
    cacheable: bool = field(default=True, compare=False, repr=False)

    @property
    def updated_at_timestamp(self) -> int:
        return ts(self.updated_at)

    @property
    def size(self) -> int:
        if self._size == INVALID_FILE_SIZE and self.part_sizes:
            self._size = sum(self.part_sizes)
        if self._size == INVALID_FILE_SIZE:
            # No primary layout (its parts are gone or unverified): a
            # verified replica knows the size too.
            for replica in self.replicas.values():
                if replica.part_sizes and len(replica.part_sizes) == len(
                    replica.message_ids
                ):
                    return replica.size
        return self._size

    def to_dict(self) -> dict:
        res = dict(
            type="FV",
            id=self.id,
            updatedAt=self.updated_at_timestamp,
            messageIds=self.message_ids,
            size=self.size,
        )
        # Only serialized when present so pre-redundancy consumers keep
        # seeing the exact format they always did.
        if self.mirrors:
            res["mirrors"] = self.mirrors
        if self.replicas:
            res["replicas"] = {
                key: replica_to_dict(replica) for key, replica in self.replicas.items()
            }
        if self.store:
            res["store"] = self.store
        if self.pending:
            res["pending"] = True
        if self.plaintext_size is not None:
            res["plaintextSize"] = self.plaintext_size
        return res

    @staticmethod
    def pending_version(version_id: str, size: int) -> "TGFSFileVersion":
        """A version whose bytes sit in the local cache, not yet in a store."""
        return TGFSFileVersion(
            id=version_id,
            updated_at=datetime.datetime.now(),
            _size=size,
            message_ids=[],
            pending=True,
        )

    def materialize(
        self, store: str, message_ids: List[int], part_sizes: List[int]
    ) -> None:
        """The primary store has the bytes: the version stops being pending."""
        self.message_ids = list(message_ids)
        self.part_sizes = list(part_sizes)
        self.store = store
        self.pending = False
        self._size = INVALID_FILE_SIZE

    @staticmethod
    def empty() -> "TGFSFileVersion":
        return TGFSFileVersion(
            id=str(uuid()),
            updated_at=datetime.datetime.now(),
            message_ids=[],
        )

    @staticmethod
    def from_sent_file_message(
        *messages: SentFileMessage,
        store: Optional[str] = None,
        version_id: Optional[str] = None,
    ) -> "TGFSFileVersion":
        mirrors: Dict[str, List[int]] = {}
        for channel in {ch for msg in messages for ch in msg.mirrors}:
            mirrors[channel] = [msg.mirrors.get(channel, 0) for msg in messages]
        replicas: Dict[str, Replica] = {}
        for msg in messages:
            replicas.update(msg.replicas)
        pt_size = messages[0].plaintext_size if messages and messages[0].plaintext_size is not None else None
        return TGFSFileVersion(
            id=version_id or str(uuid()),
            updated_at=datetime.datetime.now(),
            message_ids=[msg.message_id for msg in messages],
            part_sizes=[msg.size for msg in messages],
            mirrors=mirrors,
            replicas=replicas,
            store=store,
            plaintext_size=pt_size,
        )

    @staticmethod
    def from_dict(data: TGFSFileVersionSerialized) -> "TGFSFileVersion":
        if (updated_at_ts := data.get("updatedAt", 0)) > 0:
            updated_at = datetime.datetime.fromtimestamp(updated_at_ts / 1000)
        else:
            updated_at = FIRST_DAY_OF_EPOCH

        if (message_ids := data.get("messageIds")) is None:
            if (message_id := data["messageId"]) != EMPTY_FILE_MESSAGE:
                message_ids = [message_id]
            else:
                message_ids = []
        pending = bool(data.get("pending"))
        plaintext_size = data.get("plaintextSize")
        return TGFSFileVersion(
            id=data["id"],
            updated_at=updated_at,
            # A pending version's size is only known from the descriptor.
            _size=(
                int(data.get("size", INVALID_FILE_SIZE))
                if pending
                else INVALID_FILE_SIZE
            ),
            pending=pending,
            message_ids=message_ids,
            part_sizes=[],  # part sizes are not serialized
            # Keys written by tgfs are bare Telegram channel ids; they are
            # read as ``tg:`` keys and written back with the prefix.
            mirrors={
                normalize_store_key(channel): list(ids)
                for channel, ids in (data.get("mirrors") or {}).items()
            },
            replicas={
                normalize_store_key(key): replica_from_dict(replica)
                for key, replica in (data.get("replicas") or {}).items()
            },
            store=(normalize_store_key(data["store"]) if data.get("store") else None),
            plaintext_size=int(plaintext_size) if plaintext_size is not None else None,
        )

    def set_invalid(self):
        self.message_ids = []
        self.part_sizes = []
        self.mirrors = {}
        self.replicas = {}
        self._size = INVALID_FILE_SIZE

    def has_copy_in(self, store_key: str) -> bool:
        """Whether ``store_key`` holds a complete copy of this version.

        Ownership counts only when recorded: a version without a store
        belongs to the primary, which is never asked about here.
        """
        if self.store == store_key and self.is_valid():
            return True
        ids = self.mirrors.get(store_key)
        if ids and len(ids) == len(self.message_ids) and all(i > 0 for i in ids):
            return True
        replica = self.replicas.get(store_key)
        return bool(replica and replica.message_ids)

    def is_valid(self) -> bool:
        return bool(self.message_ids)

    def owned_by(self, store_key: str) -> bool:
        """Whether ``message_ids`` are ids of ``store_key``.

        A version without a recorded store belongs to whatever store is
        the primary; that is what tgfs always assumed.
        """
        return self.store is None or self.store == store_key

    def relocate(self, primary_key: str) -> None:
        """Re-express the version relative to a new primary store.

        After a mirror is promoted, ``message_ids`` still name messages of
        the old primary. They become that store's mirror entry, and the
        new primary's mirror entry, when it is complete, becomes
        ``message_ids``. Without a complete copy in the new primary the
        ids keep pointing at the old store, which readers then treat as a
        mirror: the version stays readable, and nothing is ever written
        to those ids in the wrong store.
        """
        if self.owned_by(primary_key) or self.store is None:
            return
        self.mirrors.setdefault(self.store, list(self.message_ids))
        ids = self.mirrors.get(primary_key)
        if ids and len(ids) == len(self.message_ids) and all(i > 0 for i in ids):
            del self.mirrors[primary_key]
            self.message_ids = list(ids)
            self.store = primary_key
            return
        # The new primary may hold the version as a replica with its own
        # part layout; that layout becomes the primary one and the old
        # primary's parts turn into a replica (their sizes are learned on
        # validation, when they are needed).
        if (replica := self.replicas.pop(primary_key, None)) and replica.message_ids:
            old_ids = self.mirrors.pop(self.store, list(self.message_ids))
            self.replicas[self.store] = Replica(
                message_ids=list(old_ids), part_sizes=list(self.part_sizes)
            )
            # Aligned mirrors were aligned with the old layout; they keep
            # that layout as replicas too.
            for key, aligned in list(self.mirrors.items()):
                if len(aligned) == len(old_ids) and all(i > 0 for i in aligned):
                    self.replicas[key] = Replica(
                        message_ids=list(aligned), part_sizes=list(self.part_sizes)
                    )
                del self.mirrors[key]
            self.message_ids = list(replica.message_ids)
            self.part_sizes = list(replica.part_sizes)
            self._size = INVALID_FILE_SIZE
            self.store = primary_key


@dataclass
class TGFSFileDesc:
    name: str
    latest_version_id: str = ""
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    versions: dict[str, TGFSFileVersion] = field(default_factory=dict)

    @property
    def updated_at_timestamp(self) -> int:
        if not self.versions or self.latest_version_id == INVALID_VERSION_ID:
            return ts(self.created_at)
        return self.get_latest_version().updated_at_timestamp

    def __post_init__(self):
        validate_name(self.name)

    def to_dict(self) -> dict:
        return dict(
            type="F",
            versions=[v.to_dict() for v in self.get_versions(sort=True)],
        )

    @staticmethod
    def from_dict(data: TGFSFileDescSerialized, name: str) -> "TGFSFileDesc":
        versions = {v["id"]: TGFSFileVersion.from_dict(v) for v in data["versions"]}
        if versions:
            latest_version_id = max(
                versions, key=lambda k: versions[k].updated_at_timestamp
            )
        else:
            latest_version_id = INVALID_VERSION_ID
        return TGFSFileDesc(
            name=name,
            latest_version_id=latest_version_id,
            versions=versions,
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @staticmethod
    def empty(name: str) -> "TGFSFileDesc":
        return TGFSFileDesc(
            name=name,
            latest_version_id="",
            versions={},
        )

    def get_latest_version(self) -> TGFSFileVersion:
        return (
            self.versions[self.latest_version_id]
            if self.latest_version_id
            else TGFSFileVersion.empty()
        )

    def get_version(self, version_id: str) -> TGFSFileVersion:
        return self.versions[version_id]

    def add_version(self, version: TGFSFileVersion) -> None:
        self.versions[version.id] = version
        if (
            self.latest_version_id == INVALID_VERSION_ID
            # ``>=`` and not ``>``: two versions can carry the same timestamp
            # (they are only millisecond-precise once serialized), and of two
            # equally-stamped versions the one added later is the newer one.
            or version.updated_at >= self.versions[self.latest_version_id].updated_at
        ):
            self.latest_version_id = version.id
        if not self.created_at or version.updated_at < self.created_at:
            self.created_at = version.updated_at

    def add_empty_version(self) -> None:
        version = TGFSFileVersion.empty()
        self.add_version(version)

    def set_last_modified(self, when: datetime.datetime) -> TGFSFileVersion:
        """Date the latest version ``when``, the way a client's mtime asks.

        Versions are ordered by their timestamp and the newest one is the
        content served, so the latest version cannot be dated at or before
        another version: that would silently promote the older content.
        Such a request raises ``ValueError`` and changes nothing.
        """
        if self.latest_version_id == INVALID_VERSION_ID:
            raise ValueError(f"{self.name} has no version to date")
        latest = self.versions[self.latest_version_id]
        # Compared as serialized (milliseconds), which is what decides the
        # order once the descriptor is read back.
        newest_other = max(
            (
                version.updated_at_timestamp
                for version_id, version in self.versions.items()
                if version_id != self.latest_version_id
            ),
            default=None,
        )
        if newest_other is not None and ts(when) <= newest_other:
            raise ValueError(
                f"{self.name}: a modification time of {when.isoformat()} would "
                f"date the latest version before an older version"
            )
        latest.updated_at = when
        if ts(when) < ts(self.created_at):
            self.created_at = when
        return latest

    def add_version_from_sent_file_message(
        self, *msg: SentFileMessage, version_id: Optional[str] = None
    ):
        version = TGFSFileVersion.from_sent_file_message(*msg, version_id=version_id)
        self.add_version(version)
        return self.versions[self.latest_version_id]

    def update_version(self, version_id: str, version: TGFSFileVersion):
        self.versions[version_id] = version

    def get_versions(
        self, sort: bool = False, exclude_invalid: bool = False
    ) -> List[TGFSFileVersion]:
        if not sort:
            res: Iterable[TGFSFileVersion] = self.versions.values()
        else:
            # Newest first. Timestamps are millisecond-precise, so two
            # versions can tie; insertion order decides then, because
            # ``from_dict`` reads the first entry as the latest version and
            # would otherwise resurrect the older one of the two.
            res = [
                version
                for _, version in sorted(
                    enumerate(self.versions.values()),
                    key=lambda item: (item[1].updated_at_timestamp, item[0]),
                    reverse=True,
                )
            ]

        if exclude_invalid:
            res = [v for v in res if v.is_valid()]
        return list(res)

    def delete_version(self, version_id: str) -> None:
        if version_id not in self.versions:
            raise ValueError(f"Version {version_id} not found in file {self.name}.")
        del self.versions[version_id]
        if version_id == self.latest_version_id:
            if self.versions:
                # Ties go to the version added last, as in ``add_version``.
                self.latest_version_id = max(
                    enumerate(self.versions),
                    key=lambda item: (self.versions[item[1]].updated_at, item[0]),
                )[1]
            else:
                self.latest_version_id = ""
