"""Spec: compatibility/ossie-compliance › every job on the shared BigQuery project queues and is never cancelled."""

from __future__ import annotations

import re
import types
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"
GROUP = "bigquery-shared-gcp-project"
_EXPRESSION = re.compile(r"\$\{\{\s*(.*?)\s*\}\}", re.DOTALL)


def _workflows() -> list[Path]:
    return sorted([*WORKFLOWS.glob("*.yml"), *WORKFLOWS.glob("*.yaml")])


def _concurrencies(path: Path) -> list[tuple[str, Any]]:
    """(owner, concurrency) for the workflow and each of its jobs."""
    workflow = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    out = [(path.name, workflow["concurrency"])] if "concurrency" in workflow else []
    for name, job in (workflow.get("jobs") or {}).items():
        if isinstance(job, dict) and "concurrency" in job:
            out.append((f"{path.name}:{name}", job["concurrency"]))
    return out


def _group(concurrency: Any) -> str:
    return str(concurrency.get("group", "") if isinstance(concurrency, dict) else concurrency)


def _shared() -> list[tuple[str, Any]]:
    """Every concurrency whose group can resolve to the shared group, literally or inside an expression."""
    return [(owner, c) for path in _workflows() for owner, c in _concurrencies(path) if GROUP in _group(c).lower()]


def _resolve(group: str, **context: Any) -> str:
    """A group's ``${{ }}`` expression (``==``, ``&&``, ``||``, ``format``) evaluated with ``context`` bound."""
    match = _EXPRESSION.fullmatch(group.strip())
    if match is None:
        return group
    python = match[1].replace("&&", " and ").replace("||", " or ")
    names = {key: types.SimpleNamespace(**value) for key, value in context.items()}
    return eval(python, {"__builtins__": {}, "format": lambda text, *args: text.format(*args)}, names)  # noqa: S307


def test_workflows_are_found() -> None:
    assert (WORKFLOWS / "ci.yml") in _workflows()
    assert (WORKFLOWS / "ossie-compliance.yml") in _workflows()


def test_the_shared_group_is_used() -> None:
    owners = [owner for owner, _ in _shared()]
    assert "ci.yml:bigquery-example" in owners
    assert "ossie-compliance.yml:server" in owners


@pytest.mark.parametrize(("owner", "concurrency"), _shared(), ids=[owner for owner, _ in _shared()])
def test_every_shared_bigquery_job_queues_and_is_never_cancelled(owner: str, concurrency: Any) -> None:
    assert isinstance(concurrency, dict), f"{owner}: a bare group neither queues nor keeps a pending run"
    assert concurrency.get("cancel-in-progress") is False, owner
    assert concurrency.get("queue") == "max", owner


@pytest.mark.parametrize(("engine", "shared"), [("bigquery", True), ("postgres", False)])
def test_the_gate_puts_only_its_bigquery_leg_in_the_shared_group(engine: str, shared: bool) -> None:
    workflow = yaml.safe_load((WORKFLOWS / "ossie-compliance.yml").read_text(encoding="utf-8"))
    server = workflow["jobs"]["server"]
    assert engine in server["strategy"]["matrix"]["engine"]
    resolved = _resolve(_group(server["concurrency"]), matrix={"engine": engine}, github={"run_id": "1"})
    assert (resolved == GROUP) is shared, resolved


def test_a_group_spelled_in_another_case_is_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "other.yaml").write_text(
        "jobs:\n  bq:\n    concurrency: ${{ 'BigQuery-Shared-GCP-Project' }}\n", encoding="utf-8")
    monkeypatch.setattr(f"{__name__}.WORKFLOWS", tmp_path)
    assert [owner for owner, _ in _shared()] == ["other.yaml:bq"]
