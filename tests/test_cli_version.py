"""``slayer --version`` (specs/cli/commands)."""

from __future__ import annotations

from importlib.metadata import version

from tests._cli_inprocess import run_cli_in_process


def test_version_prints_package_version() -> None:
    result = run_cli_in_process(["--version"])
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"slayer {version('motley-slayer')}"
