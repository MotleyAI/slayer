"""CI keeps running the living-architecture cross-check, at the version CLAUDE.md documents."""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PIN_RE = re.compile(r"living-architecture@(v[\w.]+) la-arch-")


def _read(rel: str) -> str:
    return (REPO_ROOT / rel).read_text(encoding="utf-8")


def test_repo_ci_runs_arch_check():
    assert "la-arch-check" in _read(".github/workflows/ci.yml")


def test_documented_pin_matches_ci():
    ci_pins = set(PIN_RE.findall(_read(".github/workflows/ci.yml")))
    assert len(ci_pins) == 1
    assert set(PIN_RE.findall(_read("CLAUDE.md"))) == ci_pins
