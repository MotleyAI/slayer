"""CI runs the living-architecture cross-check from the project env; pyproject.toml holds its one pin."""

import tomllib
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (REPO_ROOT / rel).read_text(encoding="utf-8")


def test_repo_ci_runs_arch_check():
    workflow = yaml.safe_load(_read(".github/workflows/ci.yml"))
    runs = [step.get("run", "") for job in workflow["jobs"].values() for step in job.get("steps", [])]
    assert runs.count("poetry run la-arch-check") == 1


def test_living_architecture_pinned_only_in_pyproject():
    dev = tomllib.loads(_read("pyproject.toml"))["tool"]["poetry"]["group"]["dev"]["dependencies"]
    assert dev["living-architecture"].count(".") == 2
    for rel in ("CLAUDE.md", ".github/workflows/ci.yml"):
        assert "living-architecture==" not in _read(rel)
