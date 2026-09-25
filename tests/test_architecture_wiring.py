"""CI keeps running the living-architecture cross-check."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_repo_ci_runs_arch_check():
    assert "la-arch-check" in (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
