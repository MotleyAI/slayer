"""Cancelling a SQL client's worker-dispatched blocking call returns at once."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, cast

import pytest
import sqlalchemy as sa
import sqlalchemy.exc

from slayer.core.models import DatasourceConfig
from slayer.sql import client as client_module
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient

# Seconds a blocked call waits for its release before giving up (the stall a broken fix shows).
_BOUND = 5.0
# Seconds within which a cancellation counts as prompt.
_PROMPT = 1.0
# Bound on a lock-blocked call; SQLite's busy timeout is 5 s.
_TIMEOUT = 0.2
_LOCKED_BOUND = 2.0
# The retry tests' stub ``run`` never touches the engine.
_NO_ENGINE = cast(sa.Engine, object())


class _Blocking:
    """A blocking call that waits for ``release``, then returns ``result`` or raises ``error``."""

    def __init__(self, *, result: Any = None, error: BaseException | None = None) -> None:
        self.result = result
        self.error = error
        self.entered = threading.Event()
        self.release = threading.Event()
        self.ended = threading.Event()
        self.calls = 0
        self.thread: threading.Thread | None = None

    def __call__(self) -> Any:
        self.calls += 1
        self.thread = threading.current_thread()
        self.entered.set()
        try:
            self.release.wait(_BOUND)
            if self.error is not None:
                raise self.error
            return self.result
        finally:
            self.ended.set()

    def finish(self) -> None:
        """Release the call and wait for its worker thread to end."""
        self.release.set()
        if self.thread is not None:
            self.thread.join(_BOUND)


class _Spy:
    """Wraps a module function, recording each call's thread and when it ends."""

    def __init__(self, func: Callable[..., Any]) -> None:
        self.func = func
        self.threads: list[threading.Thread] = []
        self.ended = threading.Event()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.threads.append(threading.current_thread())
        try:
            return self.func(*args, **kwargs)
        finally:
            self.ended.set()


class _Ticker:
    """Records the largest gap between event-loop ticks while running."""

    def __init__(self) -> None:
        self.max_gap = 0.0
        self._task: asyncio.Task[None] | None = None

    async def _run(self) -> None:
        last = time.monotonic()
        while True:
            await asyncio.sleep(0.01)
            now = time.monotonic()
            self.max_gap = max(self.max_gap, now - last)
            last = now

    async def __aenter__(self) -> _Ticker:
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, *_exc: object) -> None:
        assert self._task is not None
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


async def _until(predicate: Callable[[], bool]) -> None:
    deadline = time.monotonic() + _BOUND
    while not predicate():
        assert time.monotonic() < deadline, "condition never held"
        await asyncio.sleep(0.001)


async def _cancelled(task: asyncio.Task[Any]) -> float:
    """Cancel ``task``, assert it raises ``CancelledError``, and return how long that took."""
    started = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    return time.monotonic() - started


@pytest.fixture
def lite(tmp_path: Path) -> Iterator[DatasourceConfig]:
    path = tmp_path / "lite.db"
    with contextlib.closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
    ds = DatasourceConfig(name="lite", type="sqlite", connection_string=f"sqlite:///{path}")
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


@contextlib.contextmanager
def _exclusively_locked(ds: DatasourceConfig) -> Iterator[None]:
    """Hold the datasource's SQLite file under ``BEGIN EXCLUSIVE`` from another connection."""
    path = ds.get_connection_string().removeprefix("sqlite:///")
    conn = sqlite3.connect(path, isolation_level=None)
    try:
        conn.execute("BEGIN EXCLUSIVE")
        yield
    finally:
        conn.rollback()
        conn.close()


@pytest.fixture
def read_spy(monkeypatch: pytest.MonkeyPatch) -> _Spy:
    spy = _Spy(SlayerSQLClient._list_objects_sync)

    def _list_objects_sync(self: SlayerSQLClient, *args: Any, **kwargs: Any) -> Any:
        return spy(self, *args, **kwargs)

    monkeypatch.setattr(SlayerSQLClient, "_list_objects_sync", _list_objects_sync)
    return spy


@pytest.fixture
def ping_spy(monkeypatch: pytest.MonkeyPatch) -> _Spy:
    spy = _Spy(client_module._exec_verbatim)
    monkeypatch.setattr(client_module, "_exec_verbatim", spy)
    return spy


@pytest.fixture
def statement_spy(monkeypatch: pytest.MonkeyPatch) -> _Spy:
    spy = _Spy(client_module._execute_sql_sync)
    monkeypatch.setattr(client_module, "_execute_sql_sync", spy)
    return spy


def _joined(spy: _Spy) -> None:
    """Every call the spy saw has ended and its worker thread is gone."""
    assert spy.ended.wait(_BOUND + 1)
    for thread in spy.threads:
        thread.join(_BOUND)
        assert not thread.is_alive()


async def _bounded_while_locked(
    ds: DatasourceConfig, call: Callable[[SlayerSQLClient], Any], spy: _Spy,
) -> tuple[float, float]:
    """Run ``call`` under ``wait_for(_TIMEOUT)`` against the locked file; return (elapsed, max loop gap)."""
    client = SlayerSQLClient(ds)
    try:
        with _exclusively_locked(ds):
            async with _Ticker() as ticker:
                started = time.monotonic()
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(call(client), _TIMEOUT)
                elapsed = time.monotonic() - started
                await asyncio.sleep(0.05)
        _joined(spy)
        return elapsed, ticker.max_gap
    finally:
        client.close()


async def test_timeout_bounds_a_blocking_metadata_read(lite: DatasourceConfig, read_spy: _Spy) -> None:
    elapsed, max_gap = await _bounded_while_locked(lite, lambda c: c.list_objects(None), read_spy)
    assert elapsed < _LOCKED_BOUND
    assert max_gap < _PROMPT


async def test_timeout_bounds_a_blocking_statement(lite: DatasourceConfig, statement_spy: _Spy) -> None:
    elapsed, max_gap = await _bounded_while_locked(lite, lambda c: c.execute("SELECT * FROM t"), statement_spy)
    assert elapsed < _LOCKED_BOUND
    assert max_gap < _PROMPT


async def test_cancel_while_running_raises_at_once() -> None:
    call = _Blocking(result=1)
    task = asyncio.create_task(client_module._run_sync_in_thread(call))
    try:
        await _until(call.entered.is_set)
        assert await _cancelled(task) < _PROMPT
    finally:
        call.finish()
    assert call.thread is not None and not call.thread.is_alive()


async def test_cancel_before_start_never_runs_the_call() -> None:
    call = _Blocking(result=1)
    call.release.set()
    task = asyncio.create_task(client_module._run_sync_in_thread(call))
    await _cancelled(task)
    await asyncio.sleep(0.05)
    assert call.calls == 0


@pytest.mark.parametrize("outcome", [{"result": 1}, {"error": RuntimeError("late failure")}])
async def test_abandoned_outcome_is_dropped(outcome: dict[str, Any]) -> None:
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    reported: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: reported.append(context))
    call = _Blocking(**outcome)
    task = asyncio.create_task(client_module._run_sync_in_thread(call))
    try:
        await _until(call.entered.is_set)
        assert await _cancelled(task) < _PROMPT
        call.finish()
        await asyncio.sleep(0.05)
    finally:
        call.finish()
        loop.set_exception_handler(previous)
    assert call.thread is not None and not call.thread.is_alive()
    assert reported == []


@pytest.mark.parametrize("outcome", [{"result": 1}, {"error": RuntimeError("late failure")}])
def test_abandoned_outcome_is_dropped_after_loop_closed(
    outcome: dict[str, Any], caplog: pytest.LogCaptureFixture,
) -> None:
    call = _Blocking(**outcome)
    reported: list[dict[str, Any]] = []

    async def scenario() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: reported.append(context))
        task = asyncio.create_task(client_module._run_sync_in_thread(call))
        await _until(call.entered.is_set)
        await _cancelled(task)

    caplog.set_level(logging.ERROR)
    started = time.monotonic()
    try:
        asyncio.run(scenario())
        assert time.monotonic() - started < _PROMPT
    finally:
        call.finish()
    assert call.thread is not None and not call.thread.is_alive()
    assert reported == []
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []


async def test_close_right_after_a_cancellation(lite: DatasourceConfig, read_spy: _Spy) -> None:
    client = SlayerSQLClient(lite)
    with _exclusively_locked(lite):
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(client.list_objects(None), _TIMEOUT)
        started = time.monotonic()
        client.close()
        assert time.monotonic() - started < _PROMPT
        assert not read_spy.ended.is_set()
    _joined(read_spy)


def _locked_error() -> sqlalchemy.exc.OperationalError:
    return sqlalchemy.exc.OperationalError("SELECT 1", {}, sqlite3.OperationalError("database is locked"))


async def test_cancel_during_an_attempt_starts_no_further_attempt() -> None:
    call = _Blocking(error=_locked_error())
    attempts: list[int] = []

    async def run(_func: Callable[..., Any], **_kwargs: Any) -> Any:
        attempts.append(1)
        return await client_module._run_sync_in_thread(call)

    task = asyncio.create_task(client_module._execute_with_retry_threaded(
        sql="SELECT 1", db_type="sqlite", engine=_NO_ENGINE, initial_delay=0.01, run=run,
    ))
    try:
        await _until(call.entered.is_set)
        assert await _cancelled(task) < _PROMPT
    finally:
        call.finish()
    await asyncio.sleep(0.1)
    assert len(attempts) == 1


async def test_cancel_during_backoff_starts_no_further_attempt() -> None:
    attempts: list[int] = []

    async def run(_func: Callable[..., Any], **_kwargs: Any) -> Any:
        attempts.append(1)
        raise _locked_error()

    task = asyncio.create_task(client_module._execute_with_retry_threaded(
        sql="SELECT 1", db_type="sqlite", engine=_NO_ENGINE, initial_delay=0.05, run=run,
    ))
    await _until(lambda: len(attempts) == 1)
    assert await _cancelled(task) < _PROMPT
    await asyncio.sleep(0.2)
    assert len(attempts) == 1


async def test_uncancelled_call_returns_its_result_and_leaves_no_worker() -> None:
    call = _Blocking(result=42)
    call.release.set()
    assert await client_module._run_sync_in_thread(call) == 42
    assert call.thread is not None and not call.thread.is_alive()


async def test_uncancelled_call_raises_its_own_error_and_leaves_no_worker() -> None:
    error = RuntimeError("boom")
    call = _Blocking(error=error)
    call.release.set()
    with pytest.raises(RuntimeError) as raised:
        await client_module._run_sync_in_thread(call)
    assert raised.value is error
    assert call.thread is not None and not call.thread.is_alive()


async def test_uncancelled_ping_answers(lite: DatasourceConfig, ping_spy: _Spy) -> None:
    client = SlayerSQLClient(lite)
    try:
        assert await client.ping() is None
    finally:
        client.close()
    assert ping_spy.threads and not ping_spy.threads[0].is_alive()
