"""Recording fake connections for ``SlayerSQLClient.explain``: a scripted database behind the client's async and threaded paths."""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.sql.dialects import TsqlDialect
from slayer.sql.dialects.base import ExplainStatements
from tests._engine_helpers import plant_async_engine

PLAN_SQL = "SELECT id FROM orders"
PLAN_ROWS = [{"StmtText": PLAN_SQL, "NodeId": 1}]
VERBATIM = {"no_parameters": True}
_WAIT = 5.0


class _Result:
    def __init__(self, *, rows: list[dict[str, Any]] | None, on_fetch: Any) -> None:
        self._rows = rows
        self._on_fetch = on_fetch
        self.returns_rows = rows is not None
        self.cursor = SimpleNamespace(description=None)

    def keys(self) -> list[str]:
        return list(self._rows[0]) if self._rows else []

    def fetchall(self) -> list[tuple[Any, ...]]:
        self._on_fetch()
        return [tuple(row.values()) for row in self._rows or []]


class FakeDb:
    """Scripted statements: rows per statement, per-statement failure queues, and gates that hold a statement open."""

    def __init__(self, *, plan_sql: str = PLAN_SQL) -> None:
        self.plan_sql = plan_sql
        self.events: list[tuple[int, str]] = []
        self.options: list[Any] = []
        self.failures: dict[str, list[BaseException]] = {}
        self.thread_gates: dict[str, threading.Event] = {}
        self.async_gates: dict[str, asyncio.Event] = {}
        self.entered: dict[str, threading.Event] = {}
        self.connections: list[Any] = []
        self.invalidate_gate: asyncio.Event | None = None
        self.invalidating = threading.Event()

    def fail(self, sql: str, *errors: BaseException) -> None:
        """Raise ``errors`` in turn on the next runs of ``sql``."""
        self.failures.setdefault(sql, []).extend(errors)

    def hold_threaded(self, sql: str) -> threading.Event:
        gate = self.thread_gates[sql] = threading.Event()
        self.entered[sql] = threading.Event()
        return gate

    def hold_async(self, sql: str) -> asyncio.Event:
        gate = self.async_gates[sql] = asyncio.Event()
        self.entered[sql] = threading.Event()
        return gate

    def hold_invalidate(self) -> asyncio.Event:
        """Hold an async connection's invalidation open until the gate is set."""
        gate = self.invalidate_gate = asyncio.Event()
        return gate

    def _begin(self, conn_id: int, sql: str, options: Any) -> None:
        self.events.append((conn_id, sql))
        self.options.append(options)
        if sql in self.entered:
            self.entered[sql].set()

    def _end(self, conn_id: int, sql: str) -> _Result:
        self.events.append((conn_id, f"END {sql}"))
        queue = self.failures.get(sql)
        if queue:
            raise queue.pop(0)
        rows = PLAN_ROWS if sql == self.plan_sql else None
        return _Result(rows=rows, on_fetch=lambda: self.events.append((conn_id, f"FETCH {sql}")))

    def run_sync(self, conn_id: int, sql: str, options: Any) -> _Result:
        self._begin(conn_id, sql, options)
        gate = self.thread_gates.get(sql)
        if gate is not None:
            gate.wait(_WAIT)
        return self._end(conn_id, sql)

    async def run_async(self, conn_id: int, sql: str, options: Any) -> _Result:
        self._begin(conn_id, sql, options)
        gate = self.async_gates.get(sql)
        if gate is not None:
            await asyncio.wait_for(gate.wait(), _WAIT)
        return self._end(conn_id, sql)

    # Reading the record.

    def started(self, conn_id: int | None = None) -> list[str]:
        """Statements started, in order (optionally on one connection)."""
        markers = {"COMMIT", "ROLLBACK", "INVALIDATE", "CLOSE"}
        return [
            sql for cid, sql in self.events
            if (conn_id is None or cid == conn_id) and sql not in markers and not sql.startswith(("END ", "FETCH "))
        ]

    def index(self, conn_id: int, event: str) -> int:
        return self.events.index((conn_id, event))

    def conn_ids(self) -> list[int]:
        return [c.id for c in self.connections]


class FakeSyncConn:
    def __init__(self, db: FakeDb, conn_id: int) -> None:
        self.db = db
        self.id = conn_id
        self.invalidated = False
        self.info: dict[str, Any] = {}
        self.connection = SimpleNamespace(dbapi_connection=SimpleNamespace())

    def exec_driver_sql(self, sql: str, execution_options: Any = None) -> _Result:
        return self.db.run_sync(self.id, sql, execution_options)

    def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("statements must go through the verbatim door")

    def commit(self) -> None:
        self.db.events.append((self.id, "COMMIT"))

    def rollback(self) -> None:
        self.db.events.append((self.id, "ROLLBACK"))

    def invalidate(self, exception: BaseException | None = None) -> None:  # noqa: ARG002 — SQLAlchemy's signature
        self.invalidated = True
        self.db.events.append((self.id, "INVALIDATE"))

    def __enter__(self) -> FakeSyncConn:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.db.events.append((self.id, "CLOSE"))


class FakeSyncEngine:
    url = "mssql+pyodbc://fake"

    def __init__(self, db: FakeDb) -> None:
        self.db = db

    def connect(self) -> FakeSyncConn:
        conn = FakeSyncConn(self.db, len(self.db.connections) + 1)
        self.db.connections.append(conn)
        return conn

    def dispose(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class FakeAsyncConn:
    def __init__(self, db: FakeDb, conn_id: int) -> None:
        self.db = db
        self.id = conn_id
        self.invalidated = False
        self.info: dict[str, Any] = {}

    async def exec_driver_sql(self, sql: str, execution_options: Any = None) -> _Result:
        return await self.db.run_async(self.id, sql, execution_options)

    async def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("statements must go through the verbatim door")

    async def get_raw_connection(self) -> Any:
        return SimpleNamespace(dbapi_connection=SimpleNamespace())

    async def commit(self) -> None:
        self.db.events.append((self.id, "COMMIT"))

    async def rollback(self) -> None:
        self.db.events.append((self.id, "ROLLBACK"))

    async def invalidate(self, exception: BaseException | None = None) -> None:  # noqa: ARG002 — SQLAlchemy's signature
        self.db.invalidating.set()
        if self.db.invalidate_gate is not None:
            await asyncio.wait_for(self.db.invalidate_gate.wait(), _WAIT)
        self.invalidated = True
        self.db.events.append((self.id, "INVALIDATE"))


class FakeAsyncEngine:
    url = "postgresql+psycopg://fake"

    def __init__(self, db: FakeDb) -> None:
        self.db = db
        self.disposed = False
        self.sync_engine = SimpleNamespace(dispose=lambda **_k: None)

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncGenerator[FakeAsyncConn]:
        conn = FakeAsyncConn(self.db, len(self.db.connections) + 1)
        self.db.connections.append(conn)
        try:
            yield conn
        finally:
            self.db.events.append((conn.id, "CLOSE"))

    async def dispose(self, *_args: Any, **_kwargs: Any) -> None:
        self.disposed = True


# A session-mode statement set: on/off are what the client must pair around the plan statement.
SESSION_ON = "SET SHOWPLAN_ALL ON"
SESSION_OFF = "SET SHOWPLAN_ALL OFF"
SESSION_STATEMENTS = ExplainStatements(statement=PLAN_SQL, session_on=SESSION_ON, session_off=SESSION_OFF)


def threaded_client(
    monkeypatch: pytest.MonkeyPatch, db: FakeDb, *, ds: DatasourceConfig | None = None,
) -> SlayerSQLClient:
    """A client whose factory engine is ``db``'s fake; its sync driver (default: SQL Server's pyodbc) runs on a worker thread."""
    ds = ds or DatasourceConfig(
        name="fake_mssql", type="mssql", host="h", port=1433, database="d", username="u", password="p",
    )
    fake = FakeSyncEngine(db)
    monkeypatch.setattr(engine_factory, "get_engine", lambda *_a, **_k: fake)
    client = SlayerSQLClient(datasource=ds)
    assert client._async_url() is None
    return client


def threaded_pg_client(monkeypatch: pytest.MonkeyPatch, db: FakeDb) -> SlayerSQLClient:
    """A Postgres client on the sync psycopg2 driver, so its statement timeout runs on the threaded path."""
    ds = DatasourceConfig(name="fake_pg2", type="postgres", connection_string="postgresql+psycopg2://u@h:5432/d")
    return threaded_client(monkeypatch, db, ds=ds)


def async_client(monkeypatch: pytest.MonkeyPatch, db: FakeDb) -> SlayerSQLClient:
    """A Postgres client whose running loop's async engine is ``db``'s fake."""
    ds = DatasourceConfig(name="fake_pg", type="postgres", host="example.invalid", port=5432, database="d")
    client = SlayerSQLClient(datasource=ds)
    plant_async_engine(monkeypatch, client, FakeAsyncEngine(db))
    return client


def tsql_statements(sql: str = PLAN_SQL) -> ExplainStatements:
    return TsqlDialect().explain_statements(sql)

