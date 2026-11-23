"""The expensive-CI-job gate: first push, spec-archive push, or a failed last run."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from scripts import expensive_ci_gate as gate

GIT_ENV = {
    **{k: v for k, v in os.environ.items() if not k.startswith("GIT_")},
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.com",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.com",
}
CHANGE = "openspec/changes/dev-1-thing/proposal.md"
OWN_ARCHIVE = "openspec/changes/archive/2026-01-02-dev-1-thing/proposal.md"
MAIN_ARCHIVE = "openspec/changes/archive/2026-01-01-dev-0-other/proposal.md"
SYNC_EVENT: dict[str, Any] = {
    "action": "synchronize",
    "before": "b" * 40,
    "after": "a" * 40,
    "pull_request": {
        "base": {"ref": "main"},
        "head": {"ref": "feature", "repo": {"full_name": "org/repo"}},
    },
}


class GitRepo(BaseModel):
    path: Path

    def git(self, *args: str) -> str:
        done = subprocess.run(["git", *args], cwd=self.path, env=GIT_ENV, check=True, capture_output=True, text=True)
        return done.stdout.strip()

    def commit(self, message: str, *, write: Iterable[str] = (), move: tuple[str, str] | None = None) -> str:
        for rel in write:
            target = self.path / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(f"{rel}\n")
            self.git("add", rel)
        if move is not None:
            (self.path / move[1]).parent.mkdir(parents=True, exist_ok=True)
            self.git("mv", *move)
        self.git("commit", "-q", "-m", message)
        return self.git("rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GitRepo:
    repo = GitRepo(path=tmp_path)
    repo.git("init", "-q", "-b", "main")
    repo.commit("root", write=["README.md"])
    monkeypatch.chdir(tmp_path)
    return repo


def job(name: str, conclusion: str | None) -> dict[str, Any]:
    return {"name": name, "conclusion": conclusion}


def no_call() -> Any:
    raise AssertionError("not expected to be called")


# --- archive detection ---------------------------------------------------------------------------------------------


def test_archiving_own_change_is_detected(repo: GitRepo) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    before = repo.commit("change", write=[CHANGE])
    after = repo.commit("archive", move=(CHANGE, OWN_ARCHIVE))
    assert gate.archives_own_change(before=before, after=after, base_ref="main") is True


def test_ordinary_push_is_not_an_archive(repo: GitRepo) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    before = repo.commit("change", write=[CHANGE])
    after = repo.commit("code", write=["slayer/x.py"])
    assert gate.archives_own_change(before=before, after=after, base_ref="main") is False


def test_merging_main_with_other_archives_is_not_an_archive(repo: GitRepo) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    before = repo.commit("change", write=[CHANGE])
    repo.git("checkout", "-q", "main")
    repo.commit("other PR archived", write=[MAIN_ARCHIVE])
    repo.git("checkout", "-q", "feature")
    repo.git("merge", "-q", "--no-edit", "main")
    after = repo.git("rev-parse", "HEAD")
    assert gate.archives_own_change(before=before, after=after, base_ref="main") is False


def test_unknown_before_commit_is_undecided(repo: GitRepo) -> None:
    after = repo.git("rev-parse", "HEAD")
    assert gate.archives_own_change(before="0" * 40, after=after, base_ref="main") is None


# --- run history ---------------------------------------------------------------------------------------------------


def test_last_finished_conclusion_skips_unfinished_and_other_jobs() -> None:
    job_lists = [
        [job("bq", "skipped"), job("other", "failure")],
        [job("bq", "cancelled")],
        [job("bq", None)],
        [job("bq", "failure")],
        [job("bq", "success")],
    ]
    assert gate.last_finished_conclusion(job_lists, "bq") == "failure"


def test_last_finished_conclusion_is_none_without_a_finished_run() -> None:
    assert gate.last_finished_conclusion([[job("bq", "skipped")], [], [job("other", "success")]], "bq") is None


def test_previous_job_lists_skip_the_current_run_and_foreign_heads() -> None:
    runs = [
        {"id": 3, "jobs_url": "https://api/runs/3/jobs", "head_repository": {"full_name": "org/repo"}},
        {"id": 2, "jobs_url": "https://api/runs/2/jobs", "head_repository": {"full_name": "fork/repo"}},
        {"id": 1, "jobs_url": "https://api/runs/1/jobs", "head_repository": {"full_name": "org/repo"}},
    ]
    urls: list[str] = []

    def get(url: str) -> dict[str, Any]:
        urls.append(url)
        if "/workflows/" in url:
            return {"workflow_runs": runs}
        return {"jobs": [job("bq", url)]}

    lists = list(
        gate.previous_job_lists(
            api="https://api", repo="org/repo", workflow="ci.yml", event=SYNC_EVENT, run_id=3, get=get
        )
    )
    assert lists == [[job("bq", "https://api/runs/1/jobs?per_page=100")]]
    assert urls[0] == (
        "https://api/repos/org/repo/actions/workflows/ci.yml/runs?branch=feature&event=pull_request&per_page=30"
    )


# --- decision ------------------------------------------------------------------------------------------------------


def test_non_pull_request_events_always_run() -> None:
    assert gate.decide(event_name="push", event={}, job="bq", archive_check=no_call, previous_job_lists=no_call) == (
        True,
        "push event",
    )


@pytest.mark.parametrize("action", ["opened", "reopened"])
def test_a_new_pull_request_runs(action: str) -> None:
    run, _ = gate.decide(
        event_name="pull_request", event={"action": action}, job="bq", archive_check=no_call, previous_job_lists=no_call
    )
    assert run


@pytest.mark.parametrize(
    ("archived", "history", "expected"),
    [
        (True, [], True),
        (None, [], True),
        (False, [], True),
        (False, [[job("bq", "failure")]], True),
        (False, [[job("bq", "timed_out")]], True),
        (False, [[job("bq", "skipped")], [job("bq", "failure")]], True),
        (False, [[job("bq", "success")]], False),
        (False, [[job("bq", "skipped")], [job("bq", "success")], [job("bq", "failure")]], False),
    ],
    ids=[
        "archive-push",
        "undiffable-push",
        "never-finished",
        "last-failed",
        "last-timed-out",
        "failure-behind-a-skip",
        "last-succeeded",
        "success-behind-a-skip",
    ],
)
def test_a_later_push_runs_only_on_archive_or_after_failure(
    archived: bool | None, history: list[list[dict[str, Any]]], expected: bool
) -> None:
    run, _ = gate.decide(
        event_name="pull_request",
        event=SYNC_EVENT,
        job="bq",
        archive_check=lambda: archived,
        previous_job_lists=lambda: iter(history),
    )
    assert run is expected


def test_unreadable_history_runs() -> None:
    def broken() -> Iterable[list[dict[str, Any]]]:
        raise OSError("HTTP 502")

    assert gate.decide(
        event_name="pull_request", event=SYNC_EVENT, job="bq", archive_check=lambda: False, previous_job_lists=broken
    ) == (True, "could not read run history: HTTP 502")


# --- command line --------------------------------------------------------------------------------------------------


def run_main(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, event_name: str, event: dict[str, Any]) -> str:
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(event))
    output = tmp_path / "github_output"
    for name, value in {
        "GITHUB_EVENT_NAME": event_name,
        "GITHUB_EVENT_PATH": str(event_path),
        "GITHUB_OUTPUT": str(output),
        "GITHUB_REPOSITORY": "org/repo",
        "GITHUB_RUN_ID": "9",
        "GITHUB_API_URL": "https://api",
        "GH_TOKEN": "t",
    }.items():
        monkeypatch.setenv(name, value)
    assert gate.main(["--workflow", "ci.yml", "--job", "bq"]) == 0
    return output.read_text()


def test_main_writes_run_true_for_a_push(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert run_main(tmp_path, monkeypatch, event_name="push", event={}) == "run=true\n"


def test_main_skips_a_plain_push_after_a_success(repo: GitRepo, monkeypatch: pytest.MonkeyPatch) -> None:
    repo.git("update-ref", "refs/remotes/origin/main", "main")
    repo.git("checkout", "-q", "-b", "feature")
    before = repo.commit("change", write=[CHANGE])
    after = repo.commit("code", write=["slayer/x.py"])
    pages = {
        "https://api/repos/org/repo/actions/workflows/ci.yml/runs?branch=feature&event=pull_request&per_page=30": {
            "workflow_runs": [
                {"id": 8, "jobs_url": "https://api/runs/8/jobs", "head_repository": {"full_name": "org/repo"}}
            ]
        },
        "https://api/runs/8/jobs?per_page=100": {"jobs": [job("bq", "success")]},
    }
    monkeypatch.setattr(gate, "_api_get", lambda url, token: pages[url])
    event = {**SYNC_EVENT, "before": before, "after": after}
    output_dir = repo.path / ".out"
    output_dir.mkdir()
    assert run_main(output_dir, monkeypatch, event_name="pull_request", event=event) == "run=false\n"
