"""Structure of the GitHub Actions workflows: cancellation, the 3.14 gate, unit-test distribution."""

from __future__ import annotations

import os
import re
import subprocess
from itertools import product
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
GATE_WORKFLOW = "./.github/workflows/expensive-job-gate.yml"
PR_CONCURRENCY = {
    "group": "${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.event.pull_request.number "
    "|| github.run_id }}",
    "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
}
EXPECTED_PR_WORKFLOWS = {
    "ci.yml",
    "examples.yml",
    "integration-clickhouse.yml",
    "integration-mysql.yml",
    "integration-snowflake.yml",
    "integration-sqlserver.yml",
    "integration-trino.yml",
    "latest-deps.yml",
    "pg-facade-e2e.yml",
}
PY314_JOBS = ("unit-tests-py314", "integration-tests-py314")
EXPRESSION = re.compile(r"^\$\{\{\s*(.*?)\s*\}\}$", re.DOTALL)
CONTEXT_REF = re.compile(r"\$\{\{\s*([\w-]+)\.([\w-]+)\s*\}\}")
POETRY_ENV = "./.github/actions/poetry-env"

Workflow = dict[Any, Any]


def load(name: str) -> Workflow:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def all_workflows() -> dict[str, Workflow]:
    return {path.name: load(path.name) for path in sorted(WORKFLOWS.glob("*.yml"))}


def triggers(workflow: Workflow) -> Any:
    return workflow[True]  # PyYAML (YAML 1.1) reads the `on:` key as True


def pr_workflows() -> list[str]:
    return [name for name, wf in all_workflows().items() if "pull_request" in triggers(wf)]


def bare(condition: str) -> str:
    match = EXPRESSION.match(condition.strip())
    return match.group(1) if match else condition.strip()


def needs(job: dict[str, Any]) -> list[str]:
    value = job.get("needs", [])
    return [value] if isinstance(value, str) else list(value)


def matrix_axes(job: dict[str, Any]) -> dict[str, list[Any]]:
    return {k: v for k, v in job.get("strategy", {}).get("matrix", {}).items() if k not in ("include", "exclude")}


def combos(job: dict[str, Any]) -> list[dict[str, Any]]:
    axes = matrix_axes(job)
    return [dict(zip(axes, values)) for values in product(*axes.values())]


def render(template: str, contexts: dict[str, dict[str, Any]]) -> str:
    """Substitute ``${{ ctx.key }}`` references; any unresolvable one fails the test."""

    def value(match: re.Match[str]) -> str:
        scope, key = match.groups()
        assert key in contexts.get(scope, {}), f"unresolvable {match.group(0)}"
        return str(contexts[scope][key])

    return CONTEXT_REF.sub(value, template)


def job_contexts(workflow: Workflow, job: dict[str, Any], combo: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {"matrix": combo, "env": {**workflow.get("env", {}), **job.get("env", {})}}


def api_names(job_id: str, job: dict[str, Any]) -> list[str]:
    """The job names the Actions API reports: ``id (v1, v2)`` per matrix combination."""
    assert "name" not in job, f"{job_id}: a custom name changes the API names"
    if not matrix_axes(job):
        return [job_id]
    return [f"{job_id} ({', '.join(str(v) for v in combo.values())})" for combo in combos(job)]


def merge_keys(text: str) -> int:
    tokens = list(yaml.scan(text))
    return sum(
        1
        for prev, token in zip(tokens, tokens[1:])
        if isinstance(prev, yaml.KeyToken) and isinstance(token, yaml.ScalarToken) and token.plain
        and token.value == "<<"
    )


def unit_pytest_commands(job: dict[str, Any]) -> list[str]:
    return [step["run"] for step in job["steps"] if "pytest" in step.get("run", "") and "--shard" in step["run"]]


# --- cancellation --------------------------------------------------------------------------------------------------


def test_the_pull_request_workflows_are_found() -> None:
    assert EXPECTED_PR_WORKFLOWS <= set(pr_workflows())


@pytest.mark.parametrize("name", pr_workflows())
def test_a_pull_request_workflow_cancels_only_the_same_prs_earlier_run(name: str) -> None:
    assert load(name).get("concurrency") == PR_CONCURRENCY


def test_bigquery_keeps_its_cross_pr_queue() -> None:
    assert load("ci.yml")["jobs"]["bigquery-example"]["concurrency"] == {
        "group": "bigquery-shared-gcp-project",
        "cancel-in-progress": False,
        "queue": "max",
    }


# --- 3.11 / 3.14 job layout ----------------------------------------------------------------------------------------


def test_the_311_jobs_run_without_the_gate() -> None:
    jobs = load("ci.yml")["jobs"]
    for job_id in ("unit-tests", "integration-tests"):
        assert not needs(jobs[job_id]), job_id
        assert "if" not in jobs[job_id], job_id
        assert jobs[job_id]["strategy"]["matrix"]["python-version"] == ["3.11"], job_id
    assert jobs["unit-tests"]["strategy"]["matrix"]["shard"] == [1, 2, 3, 4]


@pytest.mark.parametrize("job_id", PY314_JOBS)
def test_the_314_jobs_wait_for_the_gate(job_id: str) -> None:
    job = load("ci.yml")["jobs"][job_id]
    assert needs(job) == ["py314-gate"]
    assert bare(job["if"]) == "needs.py314-gate.outputs.run == 'true'"


def test_the_314_unit_job_keeps_four_shards() -> None:
    matrix = load("ci.yml")["jobs"]["unit-tests-py314"]["strategy"]["matrix"]
    assert matrix["shard"] == [1, 2, 3, 4]


def test_the_314_gate_watches_every_314_job_by_its_api_name() -> None:
    jobs = load("ci.yml")["jobs"]
    gate = jobs["py314-gate"]
    assert gate["uses"] == GATE_WORKFLOW
    assert gate["permissions"] == {"contents": "read", "actions": "read"}
    assert gate["with"]["workflow"] == "ci.yml"
    expected = [name for job_id in PY314_JOBS for name in api_names(job_id, jobs[job_id])]
    assert len(expected) == 5
    assert [line.strip() for line in gate["with"]["jobs"].splitlines() if line.strip()] == expected


def test_api_names_follow_the_matrix() -> None:
    job = {"strategy": {"matrix": {"python-version": ["3.14"], "shard": [1, 2]}}}
    assert api_names("unit", job) == ["unit (3.14, 1)", "unit (3.14, 2)"]
    assert api_names("integration", {}) == ["integration"]


@pytest.mark.parametrize(
    ("job_id", "python"),
    [("unit-tests", "3.11"), ("integration-tests", "3.11"), ("unit-tests-py314", "3.14"), ("integration-tests-py314", "3.14")],
)
def test_each_test_job_sets_up_its_python(job_id: str, python: str) -> None:
    workflow = load("ci.yml")
    job = workflow["jobs"][job_id]
    templates = [s["with"]["python-version"] for s in job["steps"] if s.get("uses") == POETRY_ENV]
    assert len(templates) == 1
    rendered = {render(templates[0], job_contexts(workflow, job, combo)) for combo in combos(job)}
    assert rendered == {python}


@pytest.mark.parametrize(("job_id", "python"), [("unit-tests", "3.11"), ("unit-tests-py314", "3.14")])
def test_unit_artifacts_keep_their_per_python_per_shard_names(job_id: str, python: str) -> None:
    workflow = load("ci.yml")
    job = workflow["jobs"][job_id]
    templates = [s["with"]["name"] for s in job["steps"] if "upload-artifact" in s.get("uses", "")]
    names = {render(t, job_contexts(workflow, job, combo)) for t in templates for combo in combos(job)}
    assert names == {f"{kind}-{python}-{shard}" for kind in ("hang-reports", "shard-durations") for shard in range(1, 5)}


# --- the expensive-job gate ----------------------------------------------------------------------------------------


def test_the_gate_takes_several_job_names() -> None:
    inputs = triggers(load("expensive-job-gate.yml"))["workflow_call"]["inputs"]
    assert "jobs" in inputs
    assert "job" not in inputs


def test_the_gate_passes_each_job_name_as_its_own_argument() -> None:
    step = next(s for s in load("expensive-job-gate.yml")["jobs"]["gate"]["steps"] if s.get("id") == "gate")
    contexts = {
        "inputs": {"workflow": "ci.yml", "jobs": "unit-tests-py314 (1)\nintegration-tests-py314\n"},
        "github": {"token": "t"},
    }
    env = {k: render(str(v), contexts) for k, v in step.get("env", {}).items()}
    script = 'python3() { printf "%s\\n" "$@"; }\n' + render(step["run"], contexts)
    done = subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
        env={**os.environ, **env}, check=True, capture_output=True, text=True,
    )
    args = done.stdout.splitlines()
    assert args[args.index("--workflow"):] == [
        "--workflow", "ci.yml", "--job", "unit-tests-py314 (1)", "--job", "integration-tests-py314",
    ]


def test_every_gate_caller_passes_jobs() -> None:
    callers = {
        (name, job_id): job["with"]
        for name, wf in all_workflows().items()
        for job_id, job in wf.get("jobs", {}).items()
        if job.get("uses") == GATE_WORKFLOW
    }
    assert {("ci.yml", "bigquery-gate"), ("ci.yml", "py314-gate"), ("integration-snowflake.yml", "snowflake-gate")} <= (
        set(callers)
    )
    for key, inputs in callers.items():
        assert "job" not in inputs, key
        assert inputs["jobs"].strip(), key
    assert callers[("ci.yml", "bigquery-gate")]["jobs"].strip() == "bigquery-example"
    assert callers[("integration-snowflake.yml", "snowflake-gate")]["jobs"].strip() == "snowflake-integration"


# --- pytest distribution -------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("workflow", "job_id"),
    [("ci.yml", "unit-tests"), ("ci.yml", "unit-tests-py314"), ("latest-deps.yml", "unit-test-latest-deps")],
)
def test_unit_shards_use_worksteal(workflow: str, job_id: str) -> None:
    commands = unit_pytest_commands(load(workflow)["jobs"][job_id])
    assert len(commands) == 1
    assert re.search(r"--dist[ =]worksteal\b", commands[0]), commands[0]


@pytest.mark.parametrize("job_id", ["integration-tests", "integration-tests-py314"])
def test_integration_jobs_keep_loadscope(job_id: str) -> None:
    commands = [s["run"] for s in load("ci.yml")["jobs"][job_id]["steps"] if "pytest" in s.get("run", "")]
    assert len(commands) == 1
    assert re.search(r"--dist[ =]loadscope\b", commands[0]), commands[0]


# --- YAML hygiene --------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("path", sorted(WORKFLOWS.glob("*.yml")), ids=lambda p: p.name)
def test_no_workflow_uses_a_yaml_merge_key(path: Path) -> None:
    assert merge_keys(path.read_text()) == 0


def test_merge_keys_are_found_in_every_style() -> None:
    assert merge_keys("base: &b {x: 1}\njobs:\n  a:\n    <<: *b\n") == 1
    assert merge_keys("s: &s {x: 1}\nsteps:\n  - <<: *s\n") == 1
    assert merge_keys("b: &b {x: 1}\njob: {<<: *b, runs-on: ubuntu-latest}\n") == 1
    assert merge_keys("run: \"echo '<<: not a key'\"\nother: |\n  cat <<EOF\n  <<: x\n  EOF\n") == 0
