"""CI keeps running the living-architecture cross-check, at the version CLAUDE.md documents."""

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PIN_RE = re.compile(r"living-architecture==([\w.]+) la-arch-")
STEP_RE = re.compile(r"^uvx --no-build --from living-architecture==([\w.]+) la-arch-check(?=\s|$)", re.MULTILINE)


def _read(rel: str) -> str:
    return (REPO_ROOT / rel).read_text(encoding="utf-8")


def _ci_arch_check_pins() -> list[str]:
    workflow = yaml.safe_load(_read(".github/workflows/ci.yml"))
    runs = [step.get("run", "") for job in workflow["jobs"].values() for step in job.get("steps", [])]
    return [pin for run in runs for pin in STEP_RE.findall(run)]


def test_repo_ci_runs_arch_check():
    assert len(_ci_arch_check_pins()) == 1


def test_documented_pin_matches_ci():
    assert set(PIN_RE.findall(_read("CLAUDE.md"))) == set(_ci_arch_check_pins())
