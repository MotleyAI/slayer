"""Client statement-timeout orchestration on mocked engines (sync, async, type probe)."""

from __future__ import annotations

import asyncio
import warnings
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy.exc

from slayer.core.models import DatasourceConfig
from slayer.core.warnings import SlayerStatementTimeoutSkippedWarning, StatementTimeoutSkippedWarning
from slayer.sql import client as sql_client
from slayer.sql.client import ExecutionResult, SlayerSQLClient, get_column_types_sync
from tests._ch_fake_http import route_engine

SQL = "SELECT 1 AS x"
ROLLBACK = "<rollback>"

_CONN_STRINGS = {
    "postgres": "postgresql://u@db.invalid/d",
    "mysql": "mysql+pymysql://u@db.invalid/d",
    "mariadb": "mysql+pymysql://u@db.invalid/d",
    "snowflake": "snowflake://u@acct/d",
    "duckdb": "duckdb:///db.invalid.duckdb",
    None: "postgresql://u@db.invalid/d",
    "totally_unknown": "postgresql://u@db.invalid/d",
}


class Recorder:
    """One statement log shared by a fake sync engine and a fake async engine."""

    def __init__(self, *, reject_prefix: str | None = None, reject_exc: Exception | None = None) -> None:
        self.log: list[str] = []
        self.reject_prefix = reject_prefix
        self.reject_exc = reject_exc

    def _result(self) -> MagicMock:
        result = MagicMock()
        result.keys.return_value = ["x"]
        result.fetchall.return_value = [(1,)]
        result.cursor.description = [("x", None, None, None, None, None, None)]
        return result

    def _run(self, sql: str) -> MagicMock:
        self.log.append(sql)
        if self.reject_prefix is not None and sql.startswith(self.reject_prefix):
            assert self.reject_exc is not None
            raise self.reject_exc
        return self._result()

    def sync_engine(self) -> MagicMock:
        conn = MagicMock()
        conn.exec_driver_sql.side_effect = lambda sql, **_kw: self._run(sql)
        conn.rollback.side_effect = lambda: self.log.append(ROLLBACK)
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        return engine

    def async_engine(self) -> MagicMock:
        async def _exec(sql: str, **_kw: Any) -> MagicMock:
            return self._run(sql)

        async def _rollback() -> None:
            self.log.append(ROLLBACK)

        conn = MagicMock()
        conn.exec_driver_sql = AsyncMock(side_effect=_exec)
        conn.rollback = AsyncMock(side_effect=_rollback)
        conn.get_raw_connection = AsyncMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=conn)
        ctx.__aexit__ = AsyncMock(return_value=False)
        engine = MagicMock()
        engine.connect.return_value = ctx
        return engine

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        route_engine(monkeypatch, self.sync_engine())  # pyright: ignore[reportArgumentType]
        async_engine = self.async_engine()
        monkeypatch.setattr(sql_client, "_get_async_engine", lambda _cs: async_engine)

    def statements(self) -> list[str]:
        return [s for s in self.log if s != ROLLBACK]


def _client(ds_type: str | None, name: str = "ds") -> SlayerSQLClient:
    return SlayerSQLClient(datasource=DatasourceConfig(
        name=name, type=ds_type, connection_string=_CONN_STRINGS[ds_type],
    ))


def _skipped(record: list[warnings.WarningMessage]) -> list[StatementTimeoutSkippedWarning]:
    return [w.message.payload for w in record if isinstance(w.message, SlayerStatementTimeoutSkippedWarning)]


Runner = Callable[[SlayerSQLClient, str, int], ExecutionResult]


@pytest.fixture(params=["sync", "async"])
def run(request: pytest.FixtureRequest) -> Runner:
    def _sync(client: SlayerSQLClient, sql: str, timeout: int) -> ExecutionResult:
        return client.execute_sync(sql, timeout_seconds=timeout)

    def _async(client: SlayerSQLClient, sql: str, timeout: int) -> ExecutionResult:
        return asyncio.run(client.execute(sql, timeout_seconds=timeout))

    return _sync if request.param == "sync" else _async


def _run_recording(run: Runner, client: SlayerSQLClient, *, timeout: int = 120) -> tuple[ExecutionResult, list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        result = run(client, SQL, timeout)
    return result, record


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ds_type", "timeout_sql"),
    [
        ("mysql", "SET max_execution_time = 120000"),
        ("mariadb", "SET max_statement_time = 120"),
        ("postgres", "SET LOCAL statement_timeout = 120000"),
        (None, "SET LOCAL statement_timeout = 120000"),
        ("totally_unknown", "SET LOCAL statement_timeout = 120000"),
        ("snowflake", "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120"),
    ],
)
def test_timeout_statement_precedes_query(monkeypatch, run: Runner, ds_type: str | None, timeout_sql: str) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    result, record = _run_recording(run, _client(ds_type))
    assert rec.statements() == [timeout_sql, SQL]
    assert isinstance(result, ExecutionResult)
    assert result.rows == [{"x": 1}]
    assert result.warnings == []
    assert _skipped(record) == []


def test_mariadb_sends_no_max_execution_time(monkeypatch, run: Runner) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    run(_client("mariadb"), SQL, 120)
    assert not any("max_execution_time" in s for s in rec.log)


def test_duckdb_sends_no_timeout_and_warns_nothing(monkeypatch, run: Runner) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    result, record = _run_recording(run, _client("duckdb"))
    assert rec.statements() == [SQL]
    assert result.rows == [{"x": 1}]
    assert result.warnings == []
    assert _skipped(record) == []


@pytest.mark.parametrize("ds_type", ["postgres", None, "totally_unknown"])
def test_postgres_rejected_timeout_rolls_back_and_runs(monkeypatch, run: Runner, ds_type: str | None) -> None:
    rec = Recorder(
        reject_prefix="SET LOCAL statement_timeout",
        reject_exc=sqlalchemy.exc.ProgrammingError("SET LOCAL", {}, Exception("unrecognized configuration parameter")),
    )
    rec.install(monkeypatch)
    result, record = _run_recording(run, _client(ds_type, name="pg_ds"))
    assert rec.log == ["SET LOCAL statement_timeout = 120000", ROLLBACK, SQL]
    assert result.rows == [{"x": 1}]
    expected = StatementTimeoutSkippedWarning(datasource="pg_ds", timeout_seconds=120, reason="timeout_rejected")
    assert result.warnings == [expected]
    assert _skipped(record) == [expected]


@pytest.mark.parametrize(
    ("ds_type", "prefix"),
    [("mysql", "SET max_execution_time"), ("mariadb", "SET max_statement_time"), ("snowflake", "ALTER SESSION")],
)
def test_fail_closed_rejected_timeout_raises(monkeypatch, run: Runner, ds_type: str, prefix: str) -> None:
    rec = Recorder(
        reject_prefix=prefix,
        reject_exc=sqlalchemy.exc.OperationalError(prefix, {}, Exception("(1193, \"Unknown system variable\")")),
    )
    rec.install(monkeypatch)
    with pytest.raises(sqlalchemy.exc.OperationalError, match="Unknown system variable"):
        run(_client(ds_type), SQL, 120)
    assert SQL not in rec.log


# ---------------------------------------------------------------------------
# Column-type probe
# ---------------------------------------------------------------------------


def _probe(client: SlayerSQLClient) -> tuple[dict[str, str], list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        types = asyncio.run(client.get_column_types(SQL))
    return types, record


@pytest.mark.parametrize(
    ("ds_type", "prelude"),
    [
        ("postgres", ["SET LOCAL statement_timeout = 60000", "SET TRANSACTION READ ONLY"]),
        ("totally_unknown", ["SET LOCAL statement_timeout = 60000", "SET TRANSACTION READ ONLY"]),
        (None, ["SET LOCAL statement_timeout = 60000", "SET TRANSACTION READ ONLY"]),
        ("mysql", ["SET max_execution_time = 60000"]),
        ("mariadb", ["SET max_statement_time = 60"]),
        ("snowflake", ["ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 60"]),
        ("duckdb", []),
    ],
)
def test_probe_timeout_before_read_only_guard(monkeypatch, ds_type: str | None, prelude: list[str]) -> None:
    rec = Recorder()
    rec.install(monkeypatch)
    types, record = _probe(_client(ds_type))
    statements = rec.statements()
    assert statements[:-1] == prelude
    assert SQL in statements[-1]
    assert types == {"x": "number"}
    assert _skipped(record) == []


@pytest.mark.parametrize("ds_type", ["postgres", "totally_unknown", None])
def test_probe_rejected_timeout_keeps_guard_and_warns(monkeypatch, ds_type: str | None) -> None:
    rec = Recorder(
        reject_prefix="SET LOCAL statement_timeout",
        reject_exc=sqlalchemy.exc.ProgrammingError("SET LOCAL", {}, Exception("unrecognized configuration parameter")),
    )
    rec.install(monkeypatch)
    types, record = _probe(_client(ds_type, name="pg_ds"))
    assert rec.log[:3] == ["SET LOCAL statement_timeout = 60000", ROLLBACK, "SET TRANSACTION READ ONLY"]
    assert SQL in rec.log[3]
    assert types == {"x": "number"}
    assert _skipped(record) == [
        StatementTimeoutSkippedWarning(datasource="pg_ds", timeout_seconds=60, reason="timeout_rejected"),
    ]


def test_probe_fail_closed_rejected_timeout_raises(monkeypatch) -> None:
    rec = Recorder(
        reject_prefix="SET max_execution_time",
        reject_exc=sqlalchemy.exc.OperationalError("SET", {}, Exception("(1193, \"Unknown system variable\")")),
    )
    rec.install(monkeypatch)
    with pytest.raises(sqlalchemy.exc.OperationalError):
        asyncio.run(_client("mysql").get_column_types(SQL))
    assert not any(SQL in s for s in rec.log)


@pytest.mark.parametrize(
    ("ds_type", "prelude"),
    [
        ("postgres", ["SET LOCAL statement_timeout = 60000", "SET TRANSACTION READ ONLY"]),
        ("mysql", ["SET max_execution_time = 60000"]),
        ("mariadb", ["SET max_statement_time = 60"]),
        ("snowflake", ["ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 60"]),
    ],
)
def test_sync_probe_timeout_before_read_only_guard(ds_type: str, prelude: list[str]) -> None:
    rec = Recorder()
    types = get_column_types_sync(SQL, engine=rec.sync_engine(), db_type=ds_type)  # pyright: ignore[reportArgumentType]
    statements = rec.statements()
    assert statements[:-1] == prelude
    assert SQL in statements[-1]
    assert types == {"x": "number"}


def test_sync_probe_fail_closed_rejected_timeout_raises() -> None:
    rec = Recorder(
        reject_prefix="SET max_execution_time",
        reject_exc=sqlalchemy.exc.OperationalError("SET", {}, Exception("(1193, \"Unknown system variable\")")),
    )
    with pytest.raises(sqlalchemy.exc.OperationalError):
        get_column_types_sync(SQL, engine=rec.sync_engine(), db_type="mysql")  # pyright: ignore[reportArgumentType]
    assert not any(SQL in s for s in rec.log)
