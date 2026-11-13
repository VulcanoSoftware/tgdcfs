"""GitHub writes must not block the event loop.

PyGithub is synchronous and sleeps through GitHub's rate limits. Called
straight from a coroutine, one throttled ``create_file`` froze the whole
server -- WebDAV clients timed out after 20 s of silence while a large upload
was running -- so the writes run on a worker thread of their own.
"""

import asyncio
import threading
import time
from unittest.mock import Mock

import pytest
from github import Github
from github.Repository import Repository

from tgdcfs.core.model import TGFSDirectory
from tgdcfs.core.repository.impl.metadata.github_repo.gh_directory import (
    GithubConfig,
    GithubDirectory,
)


@pytest.fixture
def ghc():
    repo = Mock(spec=Repository)
    repo.name = "test-repo"
    return GithubConfig(
        gh=Mock(spec=Github), repo_name="owner/test-repo", repo=repo, commit="main"
    )


async def _ticks_while(coro, interval: float = 0.01) -> int:
    """How many times the event loop got to run while ``coro`` was awaited."""
    ticks = 0
    task = asyncio.ensure_future(coro)
    while not task.done():
        await asyncio.sleep(interval)
        ticks += 1
    await task
    return ticks


class TestCreateFileRefAsync:
    async def test_event_loop_keeps_running_during_a_slow_write(self, ghc):
        ghc.repo.create_file.side_effect = lambda **_: time.sleep(0.3)
        directory = GithubDirectory(ghc, "d", None)

        ticks = await _ticks_while(directory.create_file_ref_async("f", 1))

        # A blocking call would have let the loop run once, at most.
        assert ticks >= 10
        assert [fr.name for fr in directory.files] == ["f"]

    async def test_write_runs_on_the_writer_thread(self, ghc):
        seen: list[str] = []
        ghc.repo.create_file.side_effect = lambda **_: seen.append(
            threading.current_thread().name
        )
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_async("f", 1)

        assert seen == [seen[0]]
        assert seen[0].startswith("github-writes")
        assert seen[0] != threading.current_thread().name

    async def test_writes_are_serialised(self, ghc):
        """Two commits on one branch at once only conflict with each other."""
        running = 0
        peak = 0
        lock = threading.Lock()

        def create_file(**_):
            nonlocal running, peak
            with lock:
                running += 1
                peak = max(peak, running)
            time.sleep(0.05)
            with lock:
                running -= 1

        ghc.repo.create_file.side_effect = create_file
        directory = GithubDirectory(ghc, "d", None)

        await asyncio.gather(
            *(directory.create_file_ref_async(f"f{i}", i) for i in range(6))
        )

        assert peak == 1
        assert len(directory.files) == 6

    async def test_failure_removes_the_ref_again(self, ghc):
        ghc.repo.create_file.side_effect = Exception("GitHub API error")
        directory = GithubDirectory(ghc, "d", None)

        with pytest.raises(Exception, match="GitHub API error"):
            await directory.create_file_ref_async("f", 1)

        assert len(directory.files) == 0

    async def test_same_blob_as_the_sync_version(self, ghc):
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_async("f", 12345)

        ghc.repo.create_file.assert_called_once_with(
            path="f.12345",
            message="Create file reference for f",
            content="",
            branch="main",
        )


class TestCreateFileRefDeferred:
    @pytest.fixture(autouse=True)
    def _no_delay(self, monkeypatch):
        from tgdcfs.core.repository.impl.metadata.github_repo import gh_directory

        monkeypatch.setattr(gh_directory, "DEFERRED_PUT_RETRY_DELAY", 0)

    @staticmethod
    async def _wait_for_background(directory: GithubDirectory) -> None:
        pending = [t for t in list(directory._deferred_refs) if not t.done()]
        if pending:
            await asyncio.gather(*pending)

    async def test_the_ref_is_there_before_the_blob_is(self, ghc):
        blob_done = threading.Event()

        def create_file(**_):
            time.sleep(0.3)
            blob_done.set()

        ghc.repo.create_file.side_effect = create_file
        directory = GithubDirectory(ghc, "d", None)

        fr = await directory.create_file_ref_deferred("f", 1)

        assert fr.name == "f"
        assert [r.name for r in directory.files] == ["f"]
        assert not blob_done.is_set()

        await self._wait_for_background(directory)
        assert blob_done.is_set()
        ghc.repo.create_file.assert_called_once_with(
            path="f.1",
            message="Create file reference for f",
            content="",
            branch="main",
        )

    async def test_the_caller_does_not_wait_for_github(self, ghc):
        ghc.repo.create_file.side_effect = lambda **_: time.sleep(0.3)
        directory = GithubDirectory(ghc, "d", None)

        start = time.monotonic()
        await directory.create_file_ref_deferred("f", 1)

        assert time.monotonic() - start < 0.1
        await self._wait_for_background(directory)

    async def test_transient_failures_are_retried_in_the_background(self, ghc):
        import requests

        ghc.repo.create_file.side_effect = [
            requests.exceptions.ConnectionError("Max retries"),
            None,
        ]
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_deferred("f", 1)
        await self._wait_for_background(directory)

        assert ghc.repo.create_file.call_count == 2
        assert [r.name for r in directory.files] == ["f"]

    async def test_a_permanent_failure_drops_the_ref(self, ghc):
        from github import GithubException

        ghc.repo.create_file.side_effect = GithubException(404, {}, {})
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_deferred("f", 1)
        assert [r.name for r in directory.files] == ["f"]

        await self._wait_for_background(directory)

        assert len(directory.files) == 0

    async def test_a_plain_directory_defers_to_the_sync_path(self):
        a = TGFSDirectory("a", None, [], [])

        fr = await a.create_file_ref_deferred("f", 1)

        assert [r.name for r in a.files] == ["f"]
        assert fr.message_id == 1


class TestDeleteFileRefAsync:
    async def test_removes_blob_and_ref_without_blocking(self, ghc):
        ghc.repo.get_contents.return_value = Mock(sha="abc")
        ghc.repo.delete_file.side_effect = lambda **_: time.sleep(0.3)
        directory = GithubDirectory(ghc, "d", None)
        fr = directory.create_file_ref("f", 7)
        ghc.repo.create_file.reset_mock()

        ticks = await _ticks_while(directory.delete_file_ref_async(fr))

        assert ticks >= 10
        ghc.repo.delete_file.assert_called_once()
        assert len(directory.files) == 0

    async def test_missing_blob_still_drops_the_ref(self, ghc):
        ghc.repo.get_contents.side_effect = Exception("404")
        directory = GithubDirectory(ghc, "d", None)
        fr = directory.create_file_ref("f", 7)

        await directory.delete_file_ref_async(fr)

        assert len(directory.files) == 0


class TestRelocateFileRefAsync:
    async def test_moves_the_ref_between_github_directories(self, ghc):
        ghc.repo.get_contents.return_value = Mock(sha="abc")
        src = GithubDirectory(ghc, "src", None)
        dst = GithubDirectory(ghc, "dst", None)
        fr = src.create_file_ref("f", 7)
        fr.mirrors = {"dc": 9}
        fr.store = "tg"

        moved = await src.relocate_file_ref_async(fr, dst, "g")

        assert [r.name for r in dst.files] == ["g"]
        assert moved.message_id == 7
        assert moved.mirrors == {"dc": 9}
        assert moved.store == "tg"
        assert len(src.files) == 0

    async def test_failed_delete_rolls_the_new_ref_back(self, ghc):
        src = GithubDirectory(ghc, "src", None)
        dst = GithubDirectory(ghc, "dst", None)
        fr = src.create_file_ref("f", 7)
        ghc.repo.get_contents.return_value = Mock(sha="abc")

        async def boom(_):
            raise RuntimeError("boom")

        src.delete_file_ref_async = boom  # type: ignore[method-assign]

        with pytest.raises(RuntimeError):
            await src.relocate_file_ref_async(fr, dst)

        assert len(dst.files) == 0
        assert len(src.files) == 1


class TestPlainDirectoryAsync:
    """A directory without a remote repository just runs the sync methods."""

    async def test_create_delete_relocate(self):
        a = TGFSDirectory("a", None, [], [])
        b = TGFSDirectory("b", None, [], [])

        fr = await a.create_file_ref_async("f", 1)
        assert [r.name for r in a.files] == ["f"]

        moved = await a.relocate_file_ref_async(fr, b)
        assert [r.name for r in b.files] == ["f"] and not a.files

        await b.delete_file_ref_async(moved)
        assert not b.files


class TestPutRetries:
    @pytest.fixture(autouse=True)
    def _no_delay(self, monkeypatch):
        from tgdcfs.core.repository.impl.metadata.github_repo import gh_directory

        monkeypatch.setattr(gh_directory, "PUT_RETRY_DELAY", 0)

    @staticmethod
    def _outcomes(ghc, names):
        import requests
        from github import GithubException

        table = {
            "conn": requests.exceptions.ConnectionError("Max retries"),
            "422": GithubException(422, {}, {}),
            "500": GithubException(500, {}, {}),
            "404": GithubException(404, {}, {}),
            "ok": None,
        }
        ghc.repo.create_file.side_effect = [table[n] for n in names]

    def test_a_dropped_connection_is_retried(self, ghc):
        self._outcomes(ghc, ["conn", "ok"])
        GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)
        assert ghc.repo.create_file.call_count == 2

    def test_an_attempt_that_landed_is_not_an_error(self, ghc):
        self._outcomes(ghc, ["conn", "422"])
        GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)

    def test_a_refusal_is_not_retried(self, ghc):
        from github import GithubException

        self._outcomes(ghc, ["404"])
        with pytest.raises(GithubException):
            GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)
        assert ghc.repo.create_file.call_count == 1

    def test_gives_up_after_the_attempts(self, ghc):
        from github import GithubException

        self._outcomes(ghc, ["500", "500", "500"])
        with pytest.raises(GithubException):
            GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)
        assert ghc.repo.create_file.call_count == 3
