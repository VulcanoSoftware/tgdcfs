"""A repo too big for one recursive tree is read one directory at a time.

GitHub cuts the recursive tree off at 100,000 entries or 7 MB, which a few
tens of thousands of encrypted file names reach. The contents API is no way
out: it stops at 1000 entries per directory, so a folder of 20k files came
back with 1000 of them.
"""

from unittest.mock import Mock, patch

import pytest
from github import Github
from github.Repository import Repository

from tgdcfs.config import GithubRepoConfig
from tgdcfs.core.repository.impl.metadata.github_repo import (
    DirTimestampStore,
    GithubRepoMetadataRepository,
)


def entry(path: str, kind: str, sha: str) -> Mock:
    e = Mock()
    e.path, e.type, e.sha = path, kind, sha
    return e


def tree(entries, truncated=False) -> Mock:
    t = Mock()
    t.tree = entries
    t.truncated = truncated
    return t


@pytest.fixture
def repo():
    with (
        patch(
            "tgdcfs.core.repository.impl.metadata.github_repo.DirTimestampStore"
        ) as store_cls,
        patch("tgdcfs.core.repository.impl.metadata.github_repo.Github") as gh_cls,
    ):
        store_cls.return_value = Mock(spec=DirTimestampStore)
        repo = Mock(spec=Repository)
        gh = Mock(spec=Github)
        gh.get_repo.return_value = repo
        gh_cls.return_value = gh
        yield repo


def load(repo):
    import asyncio

    metadata = GithubRepoMetadataRepository(
        GithubRepoConfig(access_token="t", repo="o/r", commit="main")
    )
    return asyncio.run(metadata.get()).dir


def test_a_big_folder_is_read_whole(repo):
    files = [entry(f"f{i}.{i + 1}", "blob", f"s{i}") for i in range(1500)]
    subtree = tree(files)
    root = tree([entry("big", "tree", "sha-big")])
    repo.get_git_tree.side_effect = lambda ref, recursive=False: {
        ("main", True): tree([], truncated=True),
        ("main", False): root,
        ("sha-big", False): subtree,
    }[(ref, recursive)]
    repo.get_contents.side_effect = AssertionError("contents API must not be used")

    directory = load(repo)

    (big,) = directory.children
    assert big.name == "big"
    assert len(big.files) == 1500


def test_nested_folders_get_their_full_path(repo):
    repo.get_git_tree.side_effect = lambda ref, recursive=False: {
        ("main", True): tree([], truncated=True),
        ("main", False): tree([entry("a", "tree", "sha-a")]),
        ("sha-a", False): tree(
            [entry("b", "tree", "sha-b"), entry("one.7", "blob", "s1")]
        ),
        ("sha-b", False): tree([entry("two.8", "blob", "s2")]),
    }[(ref, recursive)]

    directory = load(repo)

    a = directory.children[0]
    assert [f.name for f in a.files] == ["one"]
    assert [f.name for f in a.children[0].files] == ["two"]
    assert a.children[0].name == "b"


def test_a_truncated_directory_falls_back_to_the_contents_api(repo):
    repo.get_git_tree.side_effect = lambda ref, recursive=False: tree(
        [], truncated=True
    )
    repo.get_contents.return_value = []

    directory = load(repo)

    repo.get_contents.assert_called_once()
    assert directory.children == [] and directory.files == []
