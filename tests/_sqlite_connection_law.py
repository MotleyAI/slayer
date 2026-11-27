"""Harness law (python.storage.arc42.md §3.1, python.database.arc42.md §3.3): every ``sqlite3`` connection is closed.

A pytest plugin. Both ``sqlite3`` connect doors open a traced connection recording its opening test, thread,
database and stack; its repr names the opener, so Python's own 3.13+ ``unclosed database`` warning does too.
A traced connection that dies unclosed fails the teardown that follows (or the session, if it dies after the last
teardown), naming its opener. One opened by an integration test is reported as a warning instead.
"""

from __future__ import annotations

import functools
import sqlite3
import sqlite3.dbapi2 as sqlite_dbapi2
import sys
import threading
import traceback
import warnings
import weakref
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

_REPO = Path(__file__).resolve().parents[1]
_HERE = str(Path(__file__).resolve())
_OUTSIDE = "<outside a test>"
_LAW = "sqlite connection law"
_INTEGRATION_DIR = "tests/integration/"
# ``factory`` is ``connect``'s sixth positional parameter.
_FACTORY_POSITION = 5


class SqliteConnectionLeakWarning(pytest.PytestWarning):
    """An integration test's ``sqlite3`` connection died unclosed."""


class _Frame(BaseModel):
    file: str
    line: int
    function: str

    def __str__(self) -> str:
        return f"{self.file}:{self.line} in {self.function}"


class _Record(BaseModel):
    """One traced connection's origin; it holds no reference to the connection."""

    database: str
    test: str
    integration: bool
    thread: str
    stack: tuple[_Frame, ...]
    closed: bool = False
    died_during: str = ""
    died_on: str = ""

    def opener(self) -> _Frame | None:
        """The innermost opening frame inside the repo (else outside this plugin)."""
        own = [f for f in reversed(self.stack) if f.file != _HERE]
        in_repo = [f for f in own if f.file.startswith(str(_REPO)) and "site-packages" not in f.file]
        return (in_repo or own or [None])[0]

    def report(self) -> str:
        stack = "\n".join(f"    {frame}" for frame in self.stack)
        return (
            f"sqlite3 connection to {self.database!r} was never closed\n"
            f"  opened by {self.test} on thread {self.thread!r}\n"
            f"  died during {self.died_during} on thread {self.died_on!r}\n"
            f"  opening stack (most recent call last):\n{stack}"
        )


class _TracedConnection(sqlite3.Connection):
    """A ``sqlite3.Connection`` that knows its opener. No ``__del__``: one would silence the built-in warning."""

    _law_record: _Record | None = None

    def close(self) -> None:
        super().close()
        if self._law_record is not None:
            self._law_record.closed = True

    def __repr__(self) -> str:
        record = self._law_record
        if record is None:
            return super().__repr__()
        frame = record.opener()
        where = f" at {frame}" if frame is not None else ""
        return f"<sqlite3.Connection to {record.database!r} opened by {record.test}{where}>"


_lock = threading.RLock()
_watched: dict[int, weakref.ref[_TracedConnection]] = {}
_pending: list[_Record] = []
_current: pytest.Item | None = None
_last: pytest.Item | None = None
_originals: dict[str, Callable[..., sqlite3.Connection]] = {}


def _current_test() -> str:
    item = _current
    return item.nodeid if item is not None else _OUTSIDE


def _died(key: int, record: _Record, _ref: object) -> None:
    with _lock:
        _watched.pop(key, None)
        if not record.closed and _originals:
            record.died_during = _current_test()
            record.died_on = threading.current_thread().name
            _pending.append(record)


def _opening_stack() -> tuple[_Frame, ...]:
    # Skip this function, ``_trace`` and the ``connect`` wrapper.
    summary = traceback.StackSummary.extract(traceback.walk_stack(sys._getframe(3)), lookup_lines=False)
    return tuple(_Frame(file=f.filename, line=f.lineno or 0, function=f.name) for f in reversed(summary))


def _trace(conn: sqlite3.Connection, database: object) -> None:
    if type(conn) is not _TracedConnection:
        return
    test = _current_test()
    record = _Record(
        database=str(database), test=test, integration=test.startswith(_INTEGRATION_DIR),
        thread=threading.current_thread().name, stack=_opening_stack(),
    )
    conn._law_record = record
    key = id(record)
    with _lock:
        _watched[key] = weakref.ref(conn, functools.partial(_died, key, record))


def _wrap(original: Callable[..., sqlite3.Connection]) -> Callable[..., sqlite3.Connection]:
    @functools.wraps(original)
    def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        if len(args) <= _FACTORY_POSITION and "factory" not in kwargs:
            kwargs["factory"] = _TracedConnection
        conn = original(*args, **kwargs)
        _trace(conn, args[0] if args else kwargs.get("database", ""))
        return conn
    return connect


def _install() -> None:
    _originals["connect"] = sqlite3.connect
    _originals["dbapi2"] = sqlite_dbapi2.connect
    wrapper = _wrap(_originals["connect"])
    sqlite3.connect = wrapper
    sqlite_dbapi2.connect = wrapper


def _uninstall() -> None:
    sqlite3.connect = _originals.pop("connect")
    sqlite_dbapi2.connect = _originals.pop("dbapi2")


def _take_pending() -> tuple[list[str], list[str]]:
    """(failures, warnings) among the connections that died unclosed since the last judgment."""
    with _lock:
        records = list(_pending)
        _pending.clear()
    failures = [r.report() for r in records if not r.integration]
    reports = [r.report() for r in records if r.integration]
    return failures, reports


def _warn(reports: list[str]) -> None:
    for text in reports:
        warnings.warn(SqliteConnectionLeakWarning(text), stacklevel=2)


def pytest_configure(config: pytest.Config) -> None:
    if not _originals:
        _install()
        config.add_cleanup(_uninstall)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None) -> Generator[None, object, object]:
    global _current, _last
    previous, _current, _last = _current, item, item
    try:
        return (yield)
    finally:
        _current = previous


@pytest.hookimpl(wrapper=True)
def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> Generator[None, None, None]:
    try:
        yield
    except BaseException as exc:
        failures, reports = _take_pending()
        _warn(reports)
        for failure in failures:
            exc.add_note(failure)
        raise
    failures, reports = _take_pending()
    _warn(reports)
    if failures:
        pytest.fail("\n".join(failures), pytrace=False)


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session) -> None:
    failures, reports = _take_pending()
    _warn(reports)
    if not failures:
        return
    # Filed against the last test run: an xdist worker forwards only reports of that test.
    last = _last
    nodeid, location = (last.nodeid, last.location) if last is not None else (_LAW, (__file__, None, _LAW))
    report = pytest.TestReport(
        nodeid=nodeid, location=location, keywords={}, outcome="failed",
        longrepr="\n".join(["after the last teardown:", *failures]), when="teardown",
    )
    session.config.hook.pytest_runtest_logreport(report=report)
    session.exitstatus = pytest.ExitCode.TESTS_FAILED
