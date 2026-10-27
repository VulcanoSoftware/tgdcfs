from typing import Dict, List, Literal, TypedDict


class ReplicaSerialized(TypedDict, total=False):
    # Message ids as a JSON list, or packed (``mb``) when there are many.
    m: List[int]
    mb: str
    # Part sizes: the full list, or ``[common, last]`` with ``n`` parts.
    p: List[int]
    n: int


class TGFSFileVersionSerialized(TypedDict, total=False):
    type: Literal["FV"]
    id: str
    updatedAt: int
    messageId: int
    messageIds: List[int]
    size: int
    # Mirror store key (``tg:<channel>``; a bare channel id is a Telegram
    # key written by tgfs) -> message ids of the copies of each part, in
    # the same order as messageIds. Absent when redundancy is not in use,
    # so pre-redundancy metadata parses unchanged.
    mirrors: Dict[str, List[int]]
    # Store key -> copy of the version with that store's own part layout.
    # Absent when no such copy exists.
    replicas: Dict[str, ReplicaSerialized]
    # True while the bytes live only in the accepting instance's local
    # cache (write_ack: cache). Absent otherwise.
    pending: bool
    # Store key that messageIds belong to. Absent for versions written by
    # tgfs (they belong to the configured primary).
    store: str
    # Plaintext size (unencrypted size) when encryption is enabled.
    # Absent when not encrypted or for un-backfilled legacy metadata.
    plaintextSize: int


class TGFSFileDescSerialized(TypedDict, total=False):
    type: Literal["F"]
    name: str
    versions: List[TGFSFileVersionSerialized]


class TGFSFileRefSerialized(TypedDict, total=False):
    type: Literal["FR"]
    messageId: int
    name: str
    # Mirror store key -> message id of the file descriptor copy in that
    # store. Absent when redundancy is not in use.
    mirrors: Dict[str, int]
    # Store key that messageId belongs to; absent for refs written by tgfs.
    store: str


class TGFSDirectorySerialized(TypedDict, total=False):
    type: Literal["D"]
    name: str
    createdAt: int
    modifiedAt: int
    children: List["TGFSDirectorySerialized"]
    files: List[TGFSFileRefSerialized]
