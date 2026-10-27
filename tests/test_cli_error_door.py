"""CLI errors are one line on stderr; ``--debug`` shows the traceback (specs/cli/commands)."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
import sqlalchemy.exc

from slayer import cli
from slayer.core.errors import MemoryNotFoundError, UnrelatedDatasetError
from slayer.engine.query_engine import SlayerQueryEngine
from tests._cli_inprocess import CliResult, run_cli_in_process
from tests._cli_shop import README_QUERY, TOTAL_QUERY, build_shop_storage


@pytest.fixture
def shop(tmp_path: Path) -> str:
    return build_shop_storage(tmp_path)


def _assert_one_error_line(result: CliResult) -> str:
    assert result.returncode == 1, result
    assert "Error" not in result.stdout
    assert "Traceback" not in result.stderr
    lines = result.stderr.splitlines()
    assert len(lines) == 1, result.stderr
    assert lines[0].startswith("Error: ")
    return lines[0]


def _raise(exc: BaseException):
    def _boom(self, **kwargs):  # noqa: ANN001, ANN202
        raise exc
    return _boom


class TestOneLineErrors:
    def test_failing_query_prints_one_line(self, shop: str) -> None:
        line = _assert_one_error_line(run_cli_in_process(["query", README_QUERY, "--storage", shop]))
        assert "stores.name reads stores, which no join path reaches" in line

    def test_driver_error_names_class_message_and_debug(self, shop: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(SlayerQueryEngine, "execute_sync", _raise(sqlite3.OperationalError("database is locked")))
        line = _assert_one_error_line(run_cli_in_process(["query", TOTAL_QUERY, "--storage", shop]))
        assert line == "Error: OperationalError: database is locked (rerun with --debug for the traceback)"

    def test_multiline_driver_error_stays_one_line(self, shop: str, monkeypatch: pytest.MonkeyPatch) -> None:
        exc = sqlalchemy.exc.OperationalError("SELECT 1", {}, sqlite3.OperationalError("disk I/O error"))
        monkeypatch.setattr(SlayerQueryEngine, "execute_sync", _raise(exc))
        line = _assert_one_error_line(run_cli_in_process(["query", TOTAL_QUERY, "--storage", shop]))
        assert "OperationalError" in line
        assert "disk I/O error" in line
        assert line.endswith("(rerun with --debug for the traceback)")

    def test_keyboard_interrupt_passes_through(self, shop: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(SlayerQueryEngine, "execute_sync", _raise(KeyboardInterrupt()))
        with pytest.raises(KeyboardInterrupt):
            run_cli_in_process(["query", TOTAL_QUERY, "--storage", shop])

    def test_memory_save_argument_error_on_stderr(self, shop: str) -> None:
        line = _assert_one_error_line(run_cli_in_process(["memory", "--storage", shop, "save", "--learning", "x"]))
        assert "--entities or --query" in line

    def test_memory_forget_missing_on_stderr(self, shop: str) -> None:
        line = _assert_one_error_line(run_cli_in_process(["memory", "--storage", shop, "forget", "999"]))
        assert "999" in line

    def test_bad_connection_string_on_stderr(self, shop: str) -> None:
        line = _assert_one_error_line(
            run_cli_in_process(["datasources", "--storage", shop, "create", "localhost/mydb"]),
        )
        assert "missing a scheme" in line

    def test_missing_query_file_on_stderr(self, shop: str, tmp_path: Path) -> None:
        missing = tmp_path / "nope.json"
        line = _assert_one_error_line(run_cli_in_process(["search", "--storage", shop, "--query", f"@{missing}"]))
        assert "Query file not found" in line


class TestDebugFlag:
    @pytest.mark.parametrize("position", ["root", "leaf"])
    def test_debug_reraises_with_traceback(self, shop: str, position: str) -> None:
        args = ["query", README_QUERY, "--storage", shop]
        args = ["--debug", *args] if position == "root" else [*args, "--debug"]
        with pytest.raises(UnrelatedDatasetError):
            run_cli_in_process(args)

    def test_debug_shows_traceback_text(self, shop: str) -> None:
        result = run_cli_in_process(["--debug", "query", README_QUERY, "--storage", shop], uncaught_exit=True)
        assert result.returncode == 1
        assert "Traceback (most recent call last)" in result.stderr

    def test_debug_on_nested_leaf(self, shop: str) -> None:
        with pytest.raises(MemoryNotFoundError):
            run_cli_in_process(["memory", "--storage", shop, "forget", "999", "--debug"])


class TestProcessBoundary:
    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "slayer", *args], capture_output=True, text=True, timeout=120,
        )

    def test_one_line_without_debug(self, shop: str) -> None:
        proc = self._run(["query", README_QUERY, "--storage", shop])
        assert proc.returncode == 1
        assert proc.stderr.splitlines() == [proc.stderr.strip()]
        assert proc.stderr.startswith("Error: ")

    def test_traceback_with_debug(self, shop: str) -> None:
        proc = self._run(["query", README_QUERY, "--storage", shop, "--debug"])
        assert proc.returncode == 1
        assert "Traceback (most recent call last)" in proc.stderr
        assert "UnrelatedDatasetError" in proc.stderr


def test_fail_prints_a_multiline_message_on_one_line(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        cli._fail("Failed to set up the Jaffle Shop demo: tweets: disk full\n  at line 3")
    assert capsys.readouterr().err == "Error: Failed to set up the Jaffle Shop demo: tweets: disk full at line 3\n"
