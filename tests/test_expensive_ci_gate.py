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
PY314 = [
    "unit-tests-py314 (1)",
    "unit-tests-py314 (2)",
    "unit-tests-py314 (3)",
    "unit-tests-py314 (4)",
    "integration-tests-py314",
]
PR_EVENT: dict[str, Any] = {
    "action": "synchronize",
    "number": 7,
    "before": "b" * 40,
    "after": "a" * 40,
    "pull_request": {
        "number": 7,
        "base": {"ref": "main"},
        "head": {"ref": "feature", "sha": "a" * 40, "repo": {"full_name": "org/repo"}},
    },
}

PriorRun = tuple[str, list[dict[str, Any]]]


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


def no_call(*_: Any) -> Any:
    raise AssertionError("not expected to be called")


def prior(sha: str, *jobs: dict[str, Any]) -> PriorRun:
    return sha, list(jobs)


def all_jobs(conclusion: str | None, names: Iterable[str] = PY314) -> list[dict[str, Any]]:
    return [job(name, conclusion) for name in names]


def decide_many(history: list[PriorRun], *, archived: dict[str, bool | None] | None = None) -> tuple[bool, str]:
    archived = archived or {}
    return gate.decide(
        event_name="pull_request",
        event=PR_EVENT,
        jobs=PY314,
        archived_since=lambda sha: archived.get(sha, False),
        previous_runs=lambda: iter(history),
    )


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


def test_last_finished_skips_unfinished_and_other_jobs() -> None:
    runs = [
        prior("s5", job("bq", "skipped"), job("other", "failure")),
        prior("s4", job("bq", "cancelled")),
        prior("s3", job("bq", None)),
        prior("s2", job("bq", "failure")),
        prior("s1", job("bq", "success")),
    ]
    assert gate.last_finished(runs, "bq") == ("failure", "s2")


def test_last_finished_is_none_without_a_finished_run() -> None:
    runs = [prior("s3", job("bq", "skipped")), prior("s2"), prior("s1", job("other", "success"))]
    assert gate.last_finished(runs, "bq") is None


def test_previous_runs_skip_the_current_run_and_foreign_heads() -> None:
    runs = [
        {"id": 3, "head_sha": "s3", "jobs_url": "https://api/runs/3/jobs", "head_repository": {"full_name": "org/repo"},
         "pull_requests": []},
        {"id": 2, "head_sha": "s2", "jobs_url": "https://api/runs/2/jobs", "head_repository": {"full_name": "fork/repo"},
         "pull_requests": []},
        {"id": 1, "head_sha": "s1", "jobs_url": "https://api/runs/1/jobs", "head_repository": {"full_name": "org/repo"},
         "pull_requests": []},
    ]
    urls: list[str] = []

    def get(url: str) -> dict[str, Any]:
        urls.append(url)
        if "/workflows/" in url:
            return {"workflow_runs": runs}
        return {"jobs": [job("bq", url)]}

    found = list(
        gate.previous_runs(api="https://api", repo="org/repo", workflow="ci.yml", event=PR_EVENT, run_id=3, get=get)
    )
    assert found == [("s1", [job("bq", "https://api/runs/1/jobs?per_page=100")])]
    assert urls[0] == (
        "https://api/repos/org/repo/actions/workflows/ci.yml/runs?branch=feature&event=pull_request&per_page=30"
    )


# --- decision ------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("archived", "history", "expected"),
    [
        (True, [prior("s1", job("bq", "success"))], True),
        (None, [prior("s1", job("bq", "success"))], True),
        (False, [], True),
        (False, [prior("s1", job("bq", "failure"))], True),
        (False, [prior("s1", job("bq", "timed_out"))], True),
        (False, [prior("s2", job("bq", "skipped")), prior("s1", job("bq", "failure"))], True),
        (False, [prior("s1", job("bq", "success"))], False),
        (
            False,
            [prior("s3", job("bq", "skipped")), prior("s2", job("bq", "success")), prior("s1", job("bq", "failure"))],
            False,
        ),
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
    archived: bool | None, history: list[PriorRun], expected: bool
) -> None:
    run, _ = gate.decide(
        event_name="pull_request",
        event=PR_EVENT,
        jobs=["bq"],
        archived_since=lambda sha: archived,
        previous_runs=lambda: iter(history),
    )
    assert run is expected


def test_unreadable_history_runs() -> None:
    def broken() -> Iterable[PriorRun]:
        raise OSError("HTTP 502")

    assert gate.decide(
        event_name="pull_request", event=PR_EVENT, jobs=["bq"], archived_since=lambda sha: False, previous_runs=broken
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


# --- several jobs --------------------------------------------------------------------------------------------------


def test_several_jobs_run_when_any_never_finished() -> None:
    history = [prior("s1", *all_jobs("success", PY314[:4]))]
    run, reason = decide_many(history)
    assert run
    assert "integration-tests-py314 never finished" in reason


def test_several_jobs_run_when_any_last_failed() -> None:
    history = [prior("s2", job(PY314[2], "failure")), prior("s1", *all_jobs("success"))]
    run, reason = decide_many(history)
    assert run
    assert f"{PY314[2]} last finished with failure" in reason


def test_the_first_listed_deciding_job_is_named() -> None:
    history = [prior("s1", job(PY314[0], "failure"), *all_jobs("success", PY314[1:4]))]
    run, reason = decide_many(history)
    assert run
    assert PY314[0] in reason
    assert "integration-tests-py314" not in reason


def test_several_jobs_skip_when_all_last_succeeded_and_name_each_conclusion() -> None:
    history = [prior("s1", *all_jobs("success", PY314[:4]), job(PY314[4], "neutral"))]
    run, reason = decide_many(history)
    assert not run
    for name in PY314[:4]:
        assert f"{name} last finished with success" in reason
    assert f"{PY314[4]} last finished with neutral" in reason


def test_cancelled_skipped_and_unfinished_runs_are_looked_past() -> None:
    history = [
        prior("s4", *all_jobs("cancelled")),
        prior("s3", *all_jobs("skipped")),
        prior("s2", *all_jobs(None)),
        prior("s1", *all_jobs("success")),
    ]
    assert decide_many(history)[0] is False


def test_jobs_finished_in_different_runs_are_each_found() -> None:
    history = [prior("s2", job(PY314[4], "success")), prior("s1", *all_jobs("success", PY314[:4]), job(PY314[4], "failure"))]
    assert decide_many(history)[0] is False


def test_the_history_is_read_once_even_as_a_one_shot_iterator() -> None:
    calls: list[int] = []

    def history() -> Iterable[tuple[str, list[dict[str, Any]]]]:
        calls.append(1)
        return (run for run in [prior("s1", *all_jobs("success"))])

    run, _ = gate.decide(
        event_name="pull_request", event=PR_EVENT, jobs=PY314, archived_since=lambda sha: False,
        previous_runs=history,
    )
    assert not run
    assert calls == [1]


def test_non_pull_request_events_run_without_history() -> None:
    assert gate.decide(
        event_name="schedule", event={}, jobs=PY314, archived_since=no_call, previous_runs=no_call
    ) == (True, "schedule event")


@pytest.mark.parametrize("action", ["opened", "reopened"])
def test_a_new_pull_request_runs_without_history(action: str) -> None:
    run, _ = gate.decide(
        event_name="pull_request", event={**PR_EVENT, "action": action}, jobs=PY314, archived_since=no_call,
        previous_runs=no_call,
    )
    assert run


# --- archive since the last finish ---------------------------------------------------------------------------------


def test_the_archive_check_starts_at_the_commit_each_job_last_finished_on() -> None:
    seen: list[str] = []

    def archived_since(sha: str) -> bool | None:
        seen.append(sha)
        return False

    history = [prior("s2", job(PY314[4], "success")), prior("s1", *all_jobs("success", PY314[:4]))]
    run, _ = gate.decide(
        event_name="pull_request", event=PR_EVENT, jobs=PY314, archived_since=archived_since,
        previous_runs=lambda: iter(history),
    )
    assert not run
    assert set(seen) == {"s1", "s2"}


def test_archive_added_since_a_jobs_last_finish_runs_and_names_it() -> None:
    history = [prior("s2", job(PY314[4], "success")), prior("s1", *all_jobs("success", PY314[:4]))]
    run, reason = decide_many(history, archived={"s2": True})
    assert run
    assert PY314[4] in reason
    assert "archive" in reason


def test_an_undeterminable_archive_check_runs() -> None:
    run, _ = decide_many([prior("s1", *all_jobs("success"))], archived={"s1": None})
    assert run


# --- history scoping -----------------------------------------------------------------------------------------------


def runs_page(*runs: dict[str, Any]) -> dict[str, Any]:
    return {"workflow_runs": list(runs)}


def api_run(run_id: int, *, sha: str, prs: list[int], head_repo: str = "org/repo") -> dict[str, Any]:
    return {
        "id": run_id,
        "head_sha": sha,
        "jobs_url": f"https://api/runs/{run_id}/jobs",
        "head_repository": {"full_name": head_repo},
        "pull_requests": [{"number": n} for n in prs],
    }


def test_history_is_matched_to_the_pull_request() -> None:
    runs = runs_page(
        api_run(9, sha="s9", prs=[7]),
        api_run(5, sha="s5", prs=[8]),
        api_run(4, sha="s4", prs=[7]),
        api_run(3, sha="s3", prs=[]),
        api_run(2, sha="s2", prs=[], head_repo="fork/repo"),
    )

    def get(url: str) -> dict[str, Any]:
        return runs if "/workflows/" in url else {"jobs": [job("bq", url)]}

    found = list(
        gate.previous_runs(api="https://api", repo="org/repo", workflow="ci.yml", event=PR_EVENT, run_id=9, get=get)
    )
    assert found == [
        ("s4", [job("bq", "https://api/runs/4/jobs?per_page=100")]),
        ("s3", [job("bq", "https://api/runs/3/jobs?per_page=100")]),
    ]


def test_a_fork_prs_history_matches_its_head_repository() -> None:
    head = {**PR_EVENT["pull_request"]["head"], "repo": {"full_name": "fork/repo"}}
    event = {**PR_EVENT, "pull_request": {**PR_EVENT["pull_request"], "head": head}}
    runs = runs_page(
        api_run(3, sha="s3", prs=[], head_repo="fork/repo"),
        api_run(2, sha="s2", prs=[], head_repo="other/repo"),
        api_run(1, sha="s1", prs=[], head_repo="org/repo"),
    )

    def get(url: str) -> dict[str, Any]:
        return runs if "/workflows/" in url else {"jobs": [job("bq", url)]}

    found = list(
        gate.previous_runs(api="https://api", repo="org/repo", workflow="ci.yml", event=event, run_id=9, get=get)
    )
    assert found == [("s3", [job("bq", "https://api/runs/3/jobs?per_page=100")])]


def test_a_success_older_than_the_lookback_counts_as_never_finished() -> None:
    all_runs = [api_run(100 - i, sha=f"s{i}", prs=[7]) for i in range(gate.RUNS_LOOKBACK + 1)]
    list_urls: list[str] = []

    def get(url: str) -> dict[str, Any]:
        if "/workflows/" in url:
            list_urls.append(url)
            return runs_page(*all_runs[: gate.RUNS_LOOKBACK])
        run_id = int(url.split("/runs/")[1].split("/")[0])
        conclusion = "success" if run_id == all_runs[-1]["id"] else "skipped"
        return {"jobs": all_jobs(conclusion)}

    run, reason = gate.decide(
        event_name="pull_request",
        event=PR_EVENT,
        jobs=PY314,
        archived_since=lambda sha: False,
        previous_runs=lambda: gate.previous_runs(
            api="https://api", repo="org/repo", workflow="ci.yml", event=PR_EVENT, run_id=1, get=get
        ),
    )
    assert run
    assert "never finished" in reason
    assert len(list_urls) == 1


# --- sequences through main() --------------------------------------------------------------------------------------


class Push(BaseModel):
    """One earlier CI run on the PR: its head commit and the 3.14 jobs' conclusion."""

    sha: str
    conclusion: str | None


def gate_main(
    repo: GitRepo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], *, head: str, pushes: list[Push]
) -> tuple[str, str]:
    """Run main() for PY314 on a push of ``head``, with ``pushes`` (newest first) as the PR's history."""
    repo.git("update-ref", "refs/remotes/origin/main", "main")
    runs = [api_run(100 - i, sha=p.sha, prs=[7]) for i, p in enumerate(pushes)]
    pages: dict[str, Any] = {
        "https://api/repos/org/repo/actions/workflows/ci.yml/runs?branch=feature&event=pull_request&per_page=30": (
            runs_page(*runs)
        ),
        **{f"https://api/runs/{r['id']}/jobs?per_page=100": {"jobs": all_jobs(p.conclusion)} for r, p in zip(runs, pushes)},
    }
    monkeypatch.setattr(gate, "_api_get", lambda url, token: pages[url])
    event = {
        **PR_EVENT,
        "before": pushes[0].sha if pushes else "0" * 40,
        "after": head,
        "pull_request": {**PR_EVENT["pull_request"], "head": {**PR_EVENT["pull_request"]["head"], "sha": head}},
    }
    out = repo.path / ".out"
    out.mkdir(exist_ok=True)
    event_path = out / "event.json"
    event_path.write_text(json.dumps(event))
    output = out / "github_output"
    output.unlink(missing_ok=True)
    for name, value in {
        "GITHUB_EVENT_NAME": "pull_request",
        "GITHUB_EVENT_PATH": str(event_path),
        "GITHUB_OUTPUT": str(output),
        "GITHUB_REPOSITORY": "org/repo",
        "GITHUB_RUN_ID": "1",
        "GITHUB_API_URL": "https://api",
        "GH_TOKEN": "t",
    }.items():
        monkeypatch.setenv(name, value)
    argv = ["--workflow", "ci.yml"] + [arg for name in PY314 for arg in ("--job", name)]
    assert gate.main(argv) == 0
    return output.read_text(), capsys.readouterr().out


def test_main_with_several_jobs_writes_one_line_and_logs_the_decision(
    repo: GitRepo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    s1 = repo.commit("change", write=[CHANGE])
    s2 = repo.commit("code", write=["slayer/x.py"])
    output, log = gate_main(repo, monkeypatch, capsys, head=s2, pushes=[Push(sha=s1, conclusion="failure")])
    assert output == "run=true\n"
    assert f"{PY314[0]} last finished with failure" in log


def test_success_then_gated_skip_then_ordinary_push_skips(
    repo: GitRepo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    s1 = repo.commit("change", write=[CHANGE])
    s2 = repo.commit("code", write=["slayer/x.py"])
    s3 = repo.commit("more code", write=["slayer/y.py"])
    pushes = [Push(sha=s2, conclusion="skipped"), Push(sha=s1, conclusion="success")]
    output, log = gate_main(repo, monkeypatch, capsys, head=s3, pushes=pushes)
    assert output == "run=false\n"
    assert f"{PY314[4]} last finished with success" in log


def test_success_then_run_cancelled_before_its_gate_then_ordinary_push_skips(
    repo: GitRepo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    s1 = repo.commit("change", write=[CHANGE])
    s2 = repo.commit("code", write=["slayer/x.py"])
    s3 = repo.commit("more code", write=["slayer/y.py"])
    pushes = [Push(sha=s2, conclusion="cancelled"), Push(sha=s1, conclusion="success")]
    assert gate_main(repo, monkeypatch, capsys, head=s3, pushes=pushes)[0] == "run=false\n"


def test_success_then_cancelled_archive_push_then_ordinary_push_runs(
    repo: GitRepo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    s1 = repo.commit("change", write=[CHANGE])
    s2 = repo.commit("archive", move=(CHANGE, OWN_ARCHIVE))
    s3 = repo.commit("code", write=["slayer/x.py"])
    pushes = [Push(sha=s2, conclusion="cancelled"), Push(sha=s1, conclusion="success")]
    output, log = gate_main(repo, monkeypatch, capsys, head=s3, pushes=pushes)
    assert output == "run=true\n"
    assert "archive" in log


def test_finished_archive_push_then_ordinary_push_skips(
    repo: GitRepo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    s1 = repo.commit("change", write=[CHANGE])
    s2 = repo.commit("archive", move=(CHANGE, OWN_ARCHIVE))
    s3 = repo.commit("code", write=["slayer/x.py"])
    pushes = [Push(sha=s2, conclusion="success"), Push(sha=s1, conclusion="success")]
    assert gate_main(repo, monkeypatch, capsys, head=s3, pushes=pushes)[0] == "run=false\n"


def test_archive_push_after_a_success_runs(
    repo: GitRepo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    s1 = repo.commit("change", write=[CHANGE])
    s2 = repo.commit("archive", move=(CHANGE, OWN_ARCHIVE))
    assert gate_main(repo, monkeypatch, capsys, head=s2, pushes=[Push(sha=s1, conclusion="success")])[0] == (
        "run=true\n"
    )


def test_an_unfetchable_last_finished_commit_runs(
    repo: GitRepo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    repo.git("checkout", "-q", "-b", "feature")
    s2 = repo.commit("code", write=["slayer/x.py"])
    pushes = [Push(sha="c" * 40, conclusion="success")]
    assert gate_main(repo, monkeypatch, capsys, head=s2, pushes=pushes)[0] == "run=true\n"


def test_main_skips_a_plain_push_after_a_success(repo: GitRepo, monkeypatch: pytest.MonkeyPatch) -> None:
    repo.git("update-ref", "refs/remotes/origin/main", "main")
    repo.git("checkout", "-q", "-b", "feature")
    before = repo.commit("change", write=[CHANGE])
    after = repo.commit("code", write=["slayer/x.py"])
    pages = {
        "https://api/repos/org/repo/actions/workflows/ci.yml/runs?branch=feature&event=pull_request&per_page=30": {
            "workflow_runs": [api_run(8, sha=before, prs=[7])]
        },
        "https://api/runs/8/jobs?per_page=100": {"jobs": [job("bq", "success")]},
    }
    monkeypatch.setattr(gate, "_api_get", lambda url, token: pages[url])
    event = {**PR_EVENT, "before": before, "after": after}
    output_dir = repo.path / ".out"
    output_dir.mkdir()
    assert run_main(output_dir, monkeypatch, event_name="pull_request", event=event) == "run=false\n"
