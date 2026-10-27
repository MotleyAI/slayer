"""``slayer --version`` (specs/cli/commands)."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

import pytest

from slayer import cli
from tests._cli_inprocess import run_cli_in_process


def test_version_prints_package_version() -> None:
    result = run_cli_in_process(["--version"])
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"slayer {version('motley-slayer')}"


def _no_metadata(name: str) -> str:
    raise PackageNotFoundError(name)


def test_missing_package_metadata_breaks_no_other_command(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "version", _no_metadata)
    result = run_cli_in_process(["--help"], uncaught_exit=True)
    assert result.returncode == 0, result.stderr


def test_version_without_package_metadata_says_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "version", _no_metadata)
    result = run_cli_in_process(["--version"], uncaught_exit=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "slayer unknown"
