"""The statement timeout is set once per connection, on mocked engines (spec: sql/execution)."""

from __future__ import annotations

import asyncio
import functools
import warnings
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy.exc

from slayer.core.models import DatasourceConfig
from slayer.core.warnings import StatementTimeoutSkippedWarning
from slayer.database import client as sql_client
from slayer.database.client import ExecutionResult, SlayerSQLClient
from tests._ch_fake_http import route_engine

SQL = "SELECT 1 AS x"
ROLLBACK = "<rollback>"

_CONN_STRINGS = {
    "postgres": "postgresql://u@db.invalid/d",
    "mysql": "mysql+pymysql://u@db.invalid/d",
    "snowflake": "snowflake://u@acct/d",
    None: "postgresql://u@db.invalid/d",
    "totally_unknown": "postgresql://u@db.invalid/d",
}

_TIMEOUT_SQL = {
    "postgres": "SET statement_timeout = {ms}",
    None: "SET statement_timeout = {ms}",
    "totally_unknown": "SET statement_timeout = {ms}",
    "mysql": "SET max_execution_time = {ms}",
    "snowflake": "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {s}",
}

_TIMEOUT_PREFIXES = (
    "SET statement_timeout", "SET LOCAL statement_timeout", "SET max_execution_time",
    "ALTER SESSION SET STATEMENT_TIMEOUT",
)

_REJECTED = sqlalchemy.exc.ProgrammingError("SET", {}, Exception("unrecognized configuration parameter"))
_FAIL_CLOSED = sqlalchemy.exc.OperationalError("SET", {}, Exception("(1193, \"Unknown system variable\")"))


def _timeout_sql(ds_type: str | None, seconds: int) -> str:
    return _TIMEOUT_SQL[ds_type].format(ms=seconds * 1000, s=seconds)


class Link:
    """One physical connection: its pool-record ``info`` and whether it rejects timeout statements."""

    def __init__(self, *, reject: Exception | None = None) -> None:
        self.info: dict[Any, Any] = {}
        self.reject = reject
        self.invalidated = False


class Recorder:
    """One statement log over a fake pool holding one physical connection at a time."""

    def __init__(self) -> None:
        self.log: list[str] = []
        self.link = Link()

    def replace(self, link: Link) -> None:
        self.link = link

    def _result(self) -> MagicMock:
        result = MagicMock()
        result.keys.return_value = ["x"]
        result.fetchall.return_value = [(1,)]
        result.returns_rows = True
        result.cursor.description = [("x", None, None, None, None, None, None)]
        return result

    def _run(self, sql: str, link: Link) -> MagicMock:
        self.log.append(sql)
        if link.reject is not None and sql.startswith(_TIMEOUT_PREFIXES):
            raise link.reject
        return self._result()

    def _sync_conn(self, link: Link) -> MagicMock:
        conn = MagicMock()
        conn.info = link.info
        conn.connection.info = link.info
        conn.exec_driver_sql.side_effect = lambda sql, **_kw: self._run(sql, link)
        conn.rollback.side_effect = lambda: self.log.append(ROLLBACK)
        conn.in_transaction.return_value = False
        conn.invalidate.side_effect = lambda *_a, **_k: setattr(link, "invalidated", True)
        return conn

    def sync_engine(self) -> MagicMock:
        engine = MagicMock()

        def _connect() -> MagicMock:
            ctx = MagicMock()
            ctx.__enter__.return_value = self._sync_conn(self.link)
            ctx.__exit__.return_value = False
            return ctx

        engine.connect.side_effect = _connect
        return engine

    def async_engine(self) -> MagicMock:
        def _connect() -> MagicMock:
            link = self.link
            sync_conn = self._sync_conn(link)

            async def _exec(sql: str, **_kw: Any) -> MagicMock:
                return self._run(sql, link)

            async def _rollback() -> None:
                self.log.append(ROLLBACK)

            raw = MagicMock()
            raw.info = link.info
            conn = MagicMock()
            conn.info = link.info
            conn.sync_connection = sync_conn
            conn.exec_driver_sql = AsyncMock(side_effect=_exec)
            conn.run_sync = AsyncMock(side_effect=lambda fn, *a, **kw: fn(sync_conn, *a, **kw))
            conn.rollback = AsyncMock(side_effect=_rollback)
            conn.commit = AsyncMock()
            conn.in_transaction.return_value = False
            conn.get_raw_connection = AsyncMock(return_value=raw)
            ctx = MagicMock()
            ctx.__aenter__ = AsyncMock(return_value=conn)
            ctx.__aexit__ = AsyncMock(return_value=False)
            return ctx

        engine = MagicMock()
        engine.connect.side_effect = _connect
        return engine

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        route_engine(monkeypatch, self.sync_engine())  # pyright: ignore[reportArgumentType]
        async_engine = self.async_engine()
        monkeypatch.setattr(sql_client, "create_async_engine", lambda *_a, **_k: async_engine)

    def statements(self) -> list[str]:
        return [s for s in self.log if s != ROLLBACK]


def _client(ds_type: str | None, *, name: str = "ds", **extra: Any) -> SlayerSQLClient:
    return SlayerSQLClient(datasource=DatasourceConfig.model_validate(
        {"name": name, "type": ds_type, "connection_string": _CONN_STRINGS[ds_type], **extra},
    ))


Runner = Callable[[SlayerSQLClient, list[tuple[str, int]]], list[ExecutionResult | BaseException]]


def _run_sync(client: SlayerSQLClient, calls: list[tuple[str, int]]) -> list[ExecutionResult | BaseException]:
    out: list[ExecutionResult | BaseException] = []
    for sql, timeout in calls:
        try:
            out.append(client.execute_sync(sql, timeout_seconds=timeout))
        except Exception as exc:  # noqa: BLE001 — a fail-closed rejection is an outcome here
            out.append(exc)
    return out


def _run_async(client: SlayerSQLClient, calls: list[tuple[str, int]]) -> list[ExecutionResult | BaseException]:
    async def _go() -> list[ExecutionResult | BaseException]:
        out: list[ExecutionResult | BaseException] = []
        for sql, timeout in calls:
            try:
                out.append(await client.execute(sql, timeout_seconds=timeout))
            except Exception as exc:  # noqa: BLE001
                out.append(exc)
        return out

    return asyncio.run(_go())


@pytest.fixture(params=["sync", "async"])
def run(request: pytest.FixtureRequest) -> Runner:
    return _run_sync if request.param == "sync" else _run_async


def _quietly(fn: Callable[[], Any]) -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return fn()


# ---------------------------------------------------------------------------
# Set once, re-set on change
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ds_type", ["postgres", None, "totally_unknown", "mysql", "snowflake"])
def test_timeout_is_set_once_per_connection(monkeypatch, run: Runner, ds_type: str | None) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    results = run(_client(ds_type), [(SQL, 120)] * 3)
    assert rec.statements() == [_timeout_sql(ds_type, 120), SQL, SQL, SQL]
    assert all(isinstance(r, ExecutionResult) and r.rows == [{"x": 1}] for r in results)


@pytest.mark.parametrize("ds_type", ["postgres", "mysql", "snowflake"])
def test_changed_timeout_is_reset_once(monkeypatch, run: Runner, ds_type: str) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    run(_client(ds_type), [(SQL, 120), (SQL, 30), (SQL, 30), (SQL, 120)])
    assert rec.statements() == [
        _timeout_sql(ds_type, 120), SQL, _timeout_sql(ds_type, 30), SQL, SQL, _timeout_sql(ds_type, 120), SQL,
    ]


@pytest.mark.parametrize("ds_type", ["postgres", "mysql", "snowflake"])
def test_replaced_connection_resends_the_timeout(monkeypatch, run: Runner, ds_type: str) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    client = _client(ds_type)
    run(client, [(SQL, 120)])
    rec.replace(Link())
    run(client, [(SQL, 120)])
    assert rec.statements() == [_timeout_sql(ds_type, 120), SQL, _timeout_sql(ds_type, 120), SQL]


# ---------------------------------------------------------------------------
# A failed timeout statement clears the record
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ds_type", ["mysql", "snowflake"])
def test_fail_closed_rejection_is_resent_next_call(monkeypatch, run: Runner, ds_type: str) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    rec.link.reject = _FAIL_CLOSED
    client = _client(ds_type)
    first = run(client, [(SQL, 120)])
    rec.link.reject = None
    second = run(client, [(SQL, 120)])
    assert isinstance(first[0], sqlalchemy.exc.OperationalError)
    assert isinstance(second[0], ExecutionResult)
    assert rec.statements() == [_timeout_sql(ds_type, 120), _timeout_sql(ds_type, 120), SQL]


# ---------------------------------------------------------------------------
# A rejected best-effort timeout is remembered, reported, and never rolled back under autocommit
# ---------------------------------------------------------------------------

_SKIP = {"timeout_seconds": 120, "reason": "timeout_rejected"}


@pytest.mark.parametrize("ds_type", ["postgres", None, "totally_unknown"])
def test_rejected_best_effort_timeout_is_not_resent(monkeypatch, run: Runner, ds_type: str | None) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    rec.link.reject = _REJECTED
    results = _quietly(lambda: run(_client(ds_type, name="pg_ds"), [(SQL, 120)] * 3))
    assert rec.statements() == [_timeout_sql(ds_type, 120), SQL, SQL, SQL]
    expected = StatementTimeoutSkippedWarning.model_validate({"datasource": "pg_ds", **_SKIP})
    assert [r.warnings for r in results if isinstance(r, ExecutionResult)] == [[expected]] * 3


@pytest.mark.parametrize("ds_type", ["postgres", None])
def test_autocommit_rejection_sends_no_rollback(monkeypatch, run: Runner, ds_type: str | None) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    rec.link.reject = _REJECTED
    _quietly(lambda: run(_client(ds_type), [(SQL, 120)]))
    assert ROLLBACK not in rec.log


def test_rejection_record_follows_the_value(monkeypatch, run: Runner) -> None:
    """A rejection at 120 s says nothing about 30 s: that value is tried once."""
    rec = Recorder()
    rec.install(monkeypatch)
    rec.link.reject = _REJECTED
    _quietly(lambda: run(_client("postgres"), [(SQL, 120), (SQL, 120), (SQL, 30), (SQL, 30)]))
    assert rec.statements() == [_timeout_sql("postgres", 120), SQL, SQL, _timeout_sql("postgres", 30), SQL, SQL]


def test_replaced_connection_does_not_inherit_a_rejection(monkeypatch, run: Runner) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    rec.link.reject = _REJECTED
    client = _client("postgres")
    _quietly(lambda: run(client, [(SQL, 120)]))
    rec.replace(Link())
    results = run(client, [(SQL, 120)])
    assert rec.statements() == [_timeout_sql("postgres", 120), SQL, _timeout_sql("postgres", 120), SQL]
    assert isinstance(results[0], ExecutionResult)
    assert results[0].warnings == []


# ---------------------------------------------------------------------------
# transaction_pooler: transaction-local, every statement, rolled back on rejection
# ---------------------------------------------------------------------------


def test_pooler_sets_a_transaction_local_timeout_every_statement(monkeypatch, run: Runner) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    run(_client("postgres", transaction_pooler=True), [(SQL, 120)] * 2)
    assert rec.statements() == ["SET LOCAL statement_timeout = 120000", SQL] * 2


def test_pooler_rejection_rolls_back(monkeypatch, run: Runner) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    rec.link.reject = _REJECTED
    results = _quietly(lambda: run(_client("postgres", name="pg_ds", transaction_pooler=True), [(SQL, 120)]))
    assert rec.log == ["SET LOCAL statement_timeout = 120000", ROLLBACK, SQL]
    assert isinstance(results[0], ExecutionResult)
    assert results[0].warnings == [StatementTimeoutSkippedWarning.model_validate({"datasource": "pg_ds", **_SKIP})]


def test_a_lost_connection_during_the_timeout_is_not_a_rejection(monkeypatch, run: Runner) -> None:
    """Best-effort covers a rejected setting, never a dead connection: the error reaches the retry."""
    rec = Recorder()
    rec.install(monkeypatch)
    rec.link.reject = sqlalchemy.exc.OperationalError("SET", {}, Exception("server closed"), connection_invalidated=True)
    for name in ("_execute_with_retry_sync", "_execute_with_retry_threaded", "_execute_with_retry_async"):
        monkeypatch.setattr(sql_client, name, functools.partial(getattr(sql_client, name), max_attempts=1))
    results = run(_client("postgres"), [(SQL, 120)])
    assert isinstance(results[0], sqlalchemy.exc.OperationalError)
    assert rec.statements() == [_timeout_sql("postgres", 120)]
