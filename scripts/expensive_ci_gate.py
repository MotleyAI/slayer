"""Expensive-CI-job gate: writes ``run=true|false`` to ``$GITHUB_OUTPUT``.

On a pull-request push the gated jobs run only if the PR was just opened, or any of them has never finished on the PR,
last finished with a failure, or had the PR's OpenSpec change archived since the commit it last finished on. A
cancelled, skipped or unfinished run is looked past. Other events always run. Anything the gate cannot determine
counts as a reason to run.

Run from the repo root: ``python3 -m scripts.expensive_ci_gate --workflow <file> --job <name> [--job <name> ...]``.
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
from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Any

ARCHIVE_DIR = "openspec/changes/archive/"
OPENED_ACTIONS = frozenset({"opened", "reopened"})
UNFINISHED = frozenset({None, "skipped", "cancelled"})
FAILED = frozenset({"failure", "timed_out", "startup_failure"})
RUNS_LOOKBACK = 30
HTTP_TIMEOUT_S = 30

Json = dict[str, Any]
PriorRun = tuple[str, list[Json]]  # (head SHA, the run's jobs)


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


def fetch_commits(*shas_or_refspecs: str) -> None:
    """Shallow-fetch commits; failures surface later as a git error."""
    _git("fetch", "--no-tags", "--depth=1", "origin", *shas_or_refspecs)


def last_finished_each(runs: Iterable[PriorRun], jobs: Sequence[str]) -> dict[str, tuple[str, str]]:
    """Per job, ``(conclusion, head SHA)`` of the newest run that finished it; reads ``runs`` only as far as needed."""
    found: dict[str, tuple[str, str]] = {}
    for sha, entries in runs:
        for entry in entries:
            name = str(entry.get("name"))
            if name in jobs and name not in found and entry.get("conclusion") not in UNFINISHED:
                found[name] = (entry["conclusion"], sha)
        if len(found) == len(set(jobs)):
            break
    return found


def last_finished(runs: Iterable[PriorRun], job: str) -> tuple[str, str] | None:
    """``(conclusion, head SHA)`` of the newest run that finished ``job``, or None if no run did."""
    return last_finished_each(runs, [job]).get(job)


def decide(
    *,
    event_name: str,
    event: Json,
    jobs: Sequence[str],
    archived_since: Callable[[str], bool | None],
    previous_runs: Callable[[], Iterable[PriorRun]],
) -> tuple[bool, str]:
    """Whether the gated jobs run, and why."""
    if event_name != "pull_request":
        return True, f"{event_name} event"
    if event.get("action") in OPENED_ACTIONS:
        return True, f"pull request {event['action']}"
    try:
        finished = last_finished_each(previous_runs(), jobs)
    except (OSError, ValueError, KeyError) as exc:
        return True, f"could not read run history: {exc}"
    archived: dict[str, bool | None] = {}
    for job in jobs:
        if job not in finished:
            return True, f"{job} never finished on this pull request"
        conclusion, sha = finished[job]
        if conclusion in FAILED:
            return True, f"{job} last finished with {conclusion}"
        if sha not in archived:
            archived[sha] = archived_since(sha)
        if archived[sha] is None:
            return True, f"could not diff since {job} last finished ({sha[:12]})"
        if archived[sha]:
            return True, f"OpenSpec change archived since {job} last finished ({sha[:12]})"
    return False, "; ".join(f"{job} last finished with {finished[job][0]}" for job in jobs)


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


def _is_this_prs_run(run: Json, *, number: int, head_repo: str) -> bool:
    prs = run.get("pull_requests") or []
    if prs:
        return any(pr.get("number") == number for pr in prs)
    return (run.get("head_repository") or {}).get("full_name") == head_repo  # fork PRs list no pull_requests


def previous_runs(
    *, api: str, repo: str, workflow: str, event: Json, run_id: int, get: Callable[[str], Json]
) -> Iterator[PriorRun]:
    """``(head SHA, jobs)`` of the workflow's earlier runs for this PR, newest first."""
    pull_request = event["pull_request"]
    head = pull_request["head"]
    query = urllib.parse.urlencode({"branch": head["ref"], "event": "pull_request", "per_page": RUNS_LOOKBACK})
    runs = get(f"{api}/repos/{repo}/actions/workflows/{workflow}/runs?{query}")["workflow_runs"]
    for run in runs:
        if run["id"] == run_id or not _is_this_prs_run(
            run, number=pull_request["number"], head_repo=head["repo"]["full_name"]
        ):
            continue
        yield run["head_sha"], get(f"{run['jobs_url']}?per_page=100")["jobs"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Decide whether expensive CI jobs run on this event.")
    parser.add_argument("--workflow", required=True, help="workflow file name, e.g. ci.yml")
    parser.add_argument(
        "--job", required=True, action="append", dest="jobs", help="a gated job's name as the Actions API reports it"
    )
    args = parser.parse_args(argv)

    env = os.environ
    event_name = env["GITHUB_EVENT_NAME"]
    with open(env["GITHUB_EVENT_PATH"], encoding="utf-8") as fh:
        event: Json = json.load(fh)

    def archived_since(sha: str) -> bool | None:
        base = event["pull_request"]["base"]["ref"]
        head = event.get("after") or event["pull_request"]["head"]["sha"]
        fetch_commits(sha, head, f"+refs/heads/{base}:refs/remotes/origin/{base}")
        return archives_own_change(before=sha, after=head, base_ref=f"origin/{base}")

    def history() -> Iterator[PriorRun]:
        token = env["GH_TOKEN"]
        return previous_runs(
            api=env.get("GITHUB_API_URL", "https://api.github.com"),
            repo=env["GITHUB_REPOSITORY"],
            workflow=args.workflow,
            event=event,
            run_id=int(env["GITHUB_RUN_ID"]),
            get=lambda url: _api_get(url, token),
        )

    run, reason = decide(
        event_name=event_name, event=event, jobs=args.jobs, archived_since=archived_since, previous_runs=history
    )
    print(f"{'Running' if run else 'Skipping'} {', '.join(args.jobs)}: {reason}")
    with open(env["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
        fh.write(f"run={'true' if run else 'false'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
