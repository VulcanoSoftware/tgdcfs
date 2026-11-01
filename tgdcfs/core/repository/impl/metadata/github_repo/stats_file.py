# Per-folder file holding the size and dates of the folder's files
# (``listing.stat_in_metadata``), keyed by descriptor message id.
STATS_FILE_NAME = ".tgdcfsstats.json"

# Every commit that only writes stats files starts with this. The folder
# dates come from the git history and must not move when one is written.
STATS_COMMIT_PREFIX = "Update file stats"


def is_stats_file(path: str) -> bool:
    return path.rpartition("/")[2] == STATS_FILE_NAME


def is_stats_commit(message: object) -> bool:
    return isinstance(message, str) and message.startswith(STATS_COMMIT_PREFIX)
