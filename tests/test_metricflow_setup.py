"""Unit tests for the dbt MetricFlow demo bootstrap helpers.

These cover the transient-error classifier and the fetch retry loop added so a
transient GitHub hiccup (e.g. a 503 over a reachable socket) retries and, if it
persists, is skipped rather than hard-failing the notebook integration test.

The helpers live next to the demo notebook (``docs/examples/11_dbt_metricflow``),
which is not an importable package, so we put that directory on ``sys.path`` and
import by module name. ``importorskip`` keeps the suite green if the demo's own
dependencies (e.g. duckdb) are absent.
"""

import subprocess
import sys
from pathlib import Path

import pytest

_METRICFLOW_DIR = (
    Path(__file__).resolve().parent.parent / "docs" / "examples" / "11_dbt_metricflow"
)
if str(_METRICFLOW_DIR) not in sys.path:
    sys.path.insert(0, str(_METRICFLOW_DIR))

setup_metricflow = pytest.importorskip("setup_metricflow")


class TestIsTransientGitError:
    @pytest.mark.parametrize(
        "stderr",
        [
            "fatal: unable to access '...': The requested URL returned error: 503",
            "fatal: unable to access '...': The requested URL returned error: 500",
            "fatal: unable to access '...': The requested URL returned error: 502",
            "fatal: unable to access '...': The requested URL returned error: 504",
            "fatal: unable to access '...': The requested URL returned error: 429",
            "fatal: unable to access '...': Could not resolve host: github.com",
            "fatal: unable to access '...': Failed to connect to github.com port 443",
            "fatal: unable to access '...': Connection reset by peer",
            "ssh: connect to host github.com port 22: Connection timed out",
            "error: RPC failed; curl 92 HTTP/2 stream 5 was reset",
            "fatal: The remote end hung up unexpectedly",
            "fatal: early EOF",
        ],
    )
    def test_transient_messages_are_retryable(self, stderr):
        assert setup_metricflow._is_transient_git_error(stderr) is True

    def test_classification_is_case_insensitive(self):
        assert setup_metricflow._is_transient_git_error(
            "THE REQUESTED URL RETURNED ERROR: 503"
        )

    @pytest.mark.parametrize(
        "stderr",
        [
            "fatal: repository 'https://github.com/x/y.git/' not found",
            "remote: Repository not found.",
            "fatal: Authentication failed for 'https://github.com/x/y.git/'",
            "fatal: reference is not a tree: e4bdee5baeaa9b0ecb8345315c4adfffbeb2f0d1",
            "The requested URL returned error: 404",
            "The requested URL returned error: 403",
            "",
        ],
    )
    def test_deterministic_messages_are_not_retryable(self, stderr):
        assert setup_metricflow._is_transient_git_error(stderr) is False


def _called_process_error(stderr: str) -> subprocess.CalledProcessError:
    return subprocess.CalledProcessError(
        returncode=128, cmd=["git", "fetch"], stderr=stderr
    )


class TestFetchPinnedCommitRetry:
    def test_succeeds_on_first_attempt(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            setup_metricflow, "_git", lambda *a, **k: calls.append(a) or ""
        )
        # Fail loudly if the loop ever sleeps on a clean first attempt.
        monkeypatch.setattr(
            setup_metricflow.time,
            "sleep",
            lambda _: pytest.fail("should not sleep on success"),
        )
        setup_metricflow._fetch_pinned_commit(Path("/tmp/x"))
        assert len(calls) == 1

    def test_retries_transient_then_succeeds(self, monkeypatch):
        attempts = {"n": 0}

        def flaky_git(*args, **kwargs):
            if args and args[0] == "fetch":
                attempts["n"] += 1
                if attempts["n"] < 3:
                    raise _called_process_error("The requested URL returned error: 503")
            return ""

        sleeps = []
        monkeypatch.setattr(setup_metricflow, "_git", flaky_git)
        monkeypatch.setattr(setup_metricflow.time, "sleep", sleeps.append)

        setup_metricflow._fetch_pinned_commit(Path("/tmp/x"))

        assert attempts["n"] == 3  # two failures + one success
        assert len(sleeps) == 2  # slept once before each retry

    def test_exhausts_retries_and_raises(self, monkeypatch):
        attempts = {"n": 0}

        def always_503(*args, **kwargs):
            attempts["n"] += 1
            raise _called_process_error("The requested URL returned error: 503")

        monkeypatch.setattr(setup_metricflow, "_git", always_503)
        monkeypatch.setattr(setup_metricflow.time, "sleep", lambda _: None)

        with pytest.raises(subprocess.CalledProcessError):
            setup_metricflow._fetch_pinned_commit(Path("/tmp/x"))

        assert attempts["n"] == setup_metricflow._FETCH_RETRIES

    def test_deterministic_error_is_not_retried(self, monkeypatch):
        attempts = {"n": 0}

        def not_found(*args, **kwargs):
            attempts["n"] += 1
            raise _called_process_error("fatal: reference is not a tree: deadbeef")

        monkeypatch.setattr(setup_metricflow, "_git", not_found)
        monkeypatch.setattr(
            setup_metricflow.time,
            "sleep",
            lambda _: pytest.fail("should not retry a deterministic failure"),
        )

        with pytest.raises(subprocess.CalledProcessError):
            setup_metricflow._fetch_pinned_commit(Path("/tmp/x"))

        assert attempts["n"] == 1  # failed once, no retry
