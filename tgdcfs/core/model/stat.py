from dataclasses import dataclass
from typing import Optional, TypedDict


class FileStatSerialized(TypedDict):
    size: int
    createdAt: int
    modifiedAt: int


@dataclass(frozen=True)
class FileStat:
    """What a listing shows of a file: its size and dates.

    ``size`` is what a client receives (the plaintext size when encryption
    is on); the dates are Unix milliseconds, like every other timestamp
    in the serialized model.
    """

    size: int
    created_at: int
    modified_at: int

    def to_dict(self) -> FileStatSerialized:
        return FileStatSerialized(
            size=self.size, createdAt=self.created_at, modifiedAt=self.modified_at
        )

    @staticmethod
    def from_dict(data: object) -> Optional["FileStat"]:
        """The stat in ``data``, or ``None`` when it is not a complete one.

        The values come from storage another program may have written, so
        anything malformed is treated as missing rather than raised.
        """
        if not isinstance(data, dict):
            return None
        try:
            size, created, modified = (
                data["size"],
                data["createdAt"],
                data["modifiedAt"],
            )
        except KeyError:
            return None
        if not all(
            isinstance(v, int) and not isinstance(v, bool)
            for v in (size, created, modified)
        ):
            return None
        return FileStat(size=size, created_at=created, modified_at=modified)
