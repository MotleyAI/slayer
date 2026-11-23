"""Expensive-CI-job gate: writes ``run=true|false`` to ``$GITHUB_OUTPUT``.

On a pull-request push the job runs only if the PR was just opened, the job has never finished on the PR, the push
archives the PR's OpenSpec change, or the job's last finished run on the PR failed. Other events always run.
Anything the gate cannot determine counts as a reason to run.

Run from the repo root: ``python3 -m scripts.expensive_ci_gate --workflow <file> --job <name>``.
Stdlib only, so the gate job needs no Poetry environment.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Iterator
from typing import Any

ARCHIVE_DIR = "openspec/changes/archive/"
OPENED_ACTIONS = frozenset({"opened", "reopened"})
UNFINISHED = frozenset({None, "skipped", "cancelled"})
FAILED = frozenset({"failure", "timed_out", "startup_failure"})
RUNS_LOOKBACK = 30
HTTP_TIMEOUT_S = 30

Json = dict[str, Any]


def _git(*args: str) -> str | None:
    done = subprocess.run(["git", *args], check=False, capture_output=True, text=True)
    return done.stdout if done.returncode == 0 else None


def archives_own_change(*, before: str, after: str, base_ref: str) -> bool | None:
    """Whether ``before..after`` adds archive files absent from ``base_ref``; None if git can't tell."""
    added = _git("diff", "--no-renames", "--name-only", "--diff-filter=A", before, after, "--", ARCHIVE_DIR)
    on_base = _git("ls-tree", "-r", "--name-only", base_ref, "--", ARCHIVE_DIR)
    if added is None or on_base is None:
        return None
    return bool(set(added.split()) - set(on_base.split()))


def fetch_push_commits(*, before: str, after: str, base: str) -> None:
    """Fetch the push's endpoints and the base branch tip; failures surface later as a git error."""
    _git("fetch", "--no-tags", "--depth=1", "origin", before, after, f"+refs/heads/{base}:refs/remotes/origin/{base}")


def last_finished_conclusion(job_lists: Iterable[list[Json]], job: str) -> str | None:
    """The conclusion of ``job`` in the newest run that finished it, or None if no run did."""
    for jobs in job_lists:
        for entry in jobs:
            if entry.get("name") == job and entry.get("conclusion") not in UNFINISHED:
                return entry["conclusion"]
    return None


def decide(
    *,
    event_name: str,
    event: Json,
    job: str,
    archive_check: Callable[[], bool | None],
    previous_job_lists: Callable[[], Iterable[list[Json]]],
) -> tuple[bool, str]:
    """Whether the expensive job runs, and why."""
    if event_name != "pull_request":
        return True, f"{event_name} event"
    if event.get("action") in OPENED_ACTIONS:
        return True, f"pull request {event['action']}"
    archived = archive_check()
    if archived is None:
        return True, "could not diff the push"
    if archived:
        return True, "push archives the OpenSpec change"
    try:
        conclusion = last_finished_conclusion(previous_job_lists(), job)
    except (OSError, ValueError, KeyError) as exc:
        return True, f"could not read run history: {exc}"
    if conclusion is None:
        return True, f"{job} never finished on this pull request"
    if conclusion in FAILED:
        return True, f"{job} last finished with {conclusion}"
    return False, f"{job} last finished with {conclusion}"


def _api_get(url: str, token: str) -> Json:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_S) as response:
        return json.load(response)


def previous_job_lists(
    *, api: str, repo: str, workflow: str, event: Json, run_id: int, get: Callable[[str], Json]
) -> Iterator[list[Json]]:
    """Job lists of the workflow's earlier runs for this PR's head, newest first."""
    head = event["pull_request"]["head"]
    query = urllib.parse.urlencode({"branch": head["ref"], "event": "pull_request", "per_page": RUNS_LOOKBACK})
    runs = get(f"{api}/repos/{repo}/actions/workflows/{workflow}/runs?{query}")["workflow_runs"]
    for run in runs:
        if run["id"] == run_id or (run.get("head_repository") or {}).get("full_name") != head["repo"]["full_name"]:
            continue
        yield get(f"{run['jobs_url']}?per_page=100")["jobs"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Decide whether an expensive CI job runs on this event.")
    parser.add_argument("--workflow", required=True, help="workflow file name, e.g. ci.yml")
    parser.add_argument("--job", required=True, help="the expensive job's name as the Actions API reports it")
    args = parser.parse_args(argv)

    env = os.environ
    event_name = env["GITHUB_EVENT_NAME"]
    with open(env["GITHUB_EVENT_PATH"], encoding="utf-8") as fh:
        event: Json = json.load(fh)

    def archive_check() -> bool | None:
        base = event["pull_request"]["base"]["ref"]
        before, after = event["before"], event["after"]
        fetch_push_commits(before=before, after=after, base=base)
        return archives_own_change(before=before, after=after, base_ref=f"origin/{base}")

    def history() -> Iterator[list[Json]]:
        token = env["GH_TOKEN"]
        return previous_job_lists(
            api=env.get("GITHUB_API_URL", "https://api.github.com"),
            repo=env["GITHUB_REPOSITORY"],
            workflow=args.workflow,
            event=event,
            run_id=int(env["GITHUB_RUN_ID"]),
            get=lambda url: _api_get(url, token),
        )

    run, reason = decide(
        event_name=event_name, event=event, job=args.job, archive_check=archive_check, previous_job_lists=history
    )
    print(f"{'Running' if run else 'Skipping'} {args.job}: {reason}")
    with open(env["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
        fh.write(f"run={'true' if run else 'false'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
