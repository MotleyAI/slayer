"""TEMPORARY diagnostic plugin: report where each sqlite3 connection garbage-collected unclosed was opened."""

from __future__ import annotations

import os
import sqlite3
import traceback
from pathlib import Path
from typing import Any

import pytest

_REPORT = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "sqlite-leaks.txt"
_connect = sqlite3.connect


class _Traced(sqlite3.Connection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._opened_at = "".join(traceback.format_stack(limit=25)[:-2])
        self._test = os.environ.get("PYTEST_CURRENT_TEST", "?")

    def __del__(self) -> None:
        try:
            self.total_changes
        except sqlite3.ProgrammingError:
            return
        with _REPORT.open("a", encoding="utf-8") as fh:
            fh.write(f"=== unclosed sqlite3 connection opened during {self._test}\n{self._opened_at}\n")


def _traced_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
    kwargs.setdefault("factory", _Traced)
    return _connect(*args, **kwargs)


sqlite3.connect = _traced_connect


def pytest_terminal_summary(terminalreporter: Any) -> None:
    if _REPORT.exists():
        terminalreporter.write_sep("=", "sqlite3 connections left unclosed")
        terminalreporter.write(_REPORT.read_text(encoding="utf-8")[:60000])


@pytest.hookimpl(trylast=True)
def pytest_sessionstart(session: Any) -> None:
    if not hasattr(session.config, "workerinput"):
        _REPORT.unlink(missing_ok=True)
