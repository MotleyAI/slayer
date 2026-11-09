"""``SlayerSQLClient.explain``: one connection, the session always restored (spec: sql/client-contract)."""

from __future__ import annotations

import asyncio
import gc
import time
from collections.abc import Callable
from pathlib import Path

import pytest
import sqlalchemy as sa
import sqlalchemy.exc

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.core.warnings import SlayerStatementTimeoutSkippedWarning
from slayer.database.client import AuthenticationRefused, ExecutionResult, SlayerSQLClient
from slayer.sql.dialects.base import ExplainStatements
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import disposable_engine
from tests._explain_fakes import (
    PLAN_ROWS,
    PLAN_SQL,
    SESSION_OFF,
    SESSION_ON,
    SESSION_STATEMENTS,
    VERBATIM,
    FakeDb,
    async_client,
    threaded_client,
    threaded_pg_client,
    tsql_statements,
)

_EXPLAIN = (SESSION_ON, PLAN_SQL, SESSION_OFF)
_BOUND = 5.0

PATHS = ["async", "threaded"]


def _client(path: str, monkeypatch: pytest.MonkeyPatch, db: FakeDb) -> SlayerSQLClient:
    return async_client(monkeypatch, db) if path == "async" else threaded_client(monkeypatch, db)


def _statements(path: str) -> ExplainStatements:
    return SESSION_STATEMENTS if path == "async" else tsql_statements()


def _explain_run(db: FakeDb, conn_id: int) -> list[str]:
    return [sql for sql in db.started(conn_id) if sql in _EXPLAIN]


def _plan_error() -> sqlalchemy.exc.ProgrammingError:
    return sqlalchemy.exc.ProgrammingError(PLAN_SQL, {}, Exception("Invalid object name 'orders'"))


def _off_error() -> sqlalchemy.exc.ProgrammingError:
    return sqlalchemy.exc.ProgrammingError(SESSION_OFF, {}, Exception("switch-off failed"))


def _transient() -> sqlalchemy.exc.OperationalError:
    return sqlalchemy.exc.OperationalError(PLAN_SQL, {}, Exception("server closed the connection unexpectedly"))


async def _until(predicate: Callable[[], bool]) -> None:
    deadline = time.monotonic() + _BOUND
    while not predicate():
        assert time.monotonic() < deadline, "condition never held"
        await asyncio.sleep(0.001)


def _discarded(db: FakeDb, conn_id: int) -> bool:
    """Invalidated before its checkout ended."""
    events = db.events
    return (conn_id, "INVALIDATE") in events and db.index(conn_id, "INVALIDATE") < db.index(conn_id, "CLOSE")


def _no_commit(db: FakeDb) -> bool:
    return not [event for _, event in db.events if event == "COMMIT"]


def _restored_or_discarded(db: FakeDb, conn_id: int) -> bool:
    """Its last statement switched plan mode off, or it never returned to the pool."""
    explain = _explain_run(db, conn_id)
    return _discarded(db, conn_id) or (bool(explain) and explain[-1] == SESSION_OFF)


@pytest.mark.parametrize("path", PATHS)
class TestTheSequence:
    async def test_runs_on_one_connection_in_order(self, path: str, monkeypatch: pytest.MonkeyPatch) -> None:
        db = FakeDb()
        result = await _client(path, monkeypatch, db).explain(_statements(path))
        assert result == ExecutionResult(rows=PLAN_ROWS)
        assert db.conn_ids() == [1]
        assert _explain_run(db, 1) == list(_EXPLAIN)

    async def test_each_statement_goes_through_the_verbatim_door(
        self, path: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db = FakeDb()
        await _client(path, monkeypatch, db).explain(_statements(path))
        assert db.options
        assert all(options == VERBATIM for options in db.options)

    async def test_plan_rows_are_fetched_before_the_switch_off(
        self, path: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db = FakeDb()
        await _client(path, monkeypatch, db).explain(_statements(path))
        assert db.index(1, f"FETCH {PLAN_SQL}") < db.index(1, SESSION_OFF)

    async def test_never_commits(self, path: str, monkeypatch: pytest.MonkeyPatch) -> None:
        db = FakeDb()
        await _client(path, monkeypatch, db).explain(_statements(path))
        assert _no_commit(db)

    async def test_a_single_statement_explain_runs_alone(self, path: str, monkeypatch: pytest.MonkeyPatch) -> None:
        db = FakeDb(plan_sql=f"EXPLAIN {PLAN_SQL}")
        result = await _client(path, monkeypatch, db).explain(ExplainStatements(statement=f"EXPLAIN {PLAN_SQL}"))
        assert result.rows == PLAN_ROWS
        assert [sql for sql in db.started(1) if "SHOWPLAN" in sql] == []
        assert f"EXPLAIN {PLAN_SQL}" in db.started(1)


@pytest.mark.parametrize("path", PATHS)
class TestFailures:
    async def test_a_failing_plan_statement_is_still_switched_off(
        self, path: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db = FakeDb()
        error = _plan_error()
        db.fail(PLAN_SQL, error)
        client, statements = _client(path, monkeypatch, db), _statements(path)
        with pytest.raises(sqlalchemy.exc.ProgrammingError) as excinfo:
            await client.explain(statements)
        assert excinfo.value is error
        assert _explain_run(db, 1) == list(_EXPLAIN)
        assert _no_commit(db)

    async def test_the_plan_error_wins_over_a_switch_off_error(
        self, path: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db = FakeDb()
        plan_error = _plan_error()
        db.fail(PLAN_SQL, plan_error)
        db.fail(SESSION_OFF, _off_error())
        client, statements = _client(path, monkeypatch, db), _statements(path)
        with pytest.raises(sqlalchemy.exc.ProgrammingError) as excinfo:
            await client.explain(statements)
        assert excinfo.value is plan_error
        assert _discarded(db, 1)
        assert _no_commit(db)

    async def test_a_failing_switch_off_discards_the_connection(
        self, path: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db = FakeDb()
        off_error = _off_error()
        db.fail(SESSION_OFF, off_error)
        client, statements = _client(path, monkeypatch, db), _statements(path)
        with pytest.raises(sqlalchemy.exc.ProgrammingError) as excinfo:
            await client.explain(statements)
        assert excinfo.value is off_error
        assert _discarded(db, 1)
        assert _no_commit(db)

    async def test_a_transient_failure_retries_on_a_fresh_connection(
        self, path: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db = FakeDb()
        db.fail(PLAN_SQL, _transient())
        db.fail(SESSION_OFF, _off_error())
        result = await _client(path, monkeypatch, db).explain(_statements(path))
        assert result.rows == PLAN_ROWS
        assert db.conn_ids() == [1, 2]
        assert _discarded(db, 1)
        retry_starts = next(i for i, (conn_id, _) in enumerate(db.events) if conn_id == 2)
        assert db.index(1, "INVALIDATE") < retry_starts
        assert _explain_run(db, 2) == list(_EXPLAIN)
        assert _no_commit(db)

    async def test_a_credential_rejection_is_typed_and_discards_the_engine(
        self, path: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db = FakeDb()
        db.fail(PLAN_SQL, sqlalchemy.exc.ProgrammingError(PLAN_SQL, {}, Exception("password authentication failed")))
        client = _client(path, monkeypatch, db)
        async_engine = client._async_engine_here() if path == "async" else None
        evicted: list[DatasourceConfig] = []
        monkeypatch.setattr(engine_factory, "invalidate_engine", lambda ds: evicted.append(ds) or True)
        statements = _statements(path)
        with pytest.raises(AuthenticationRefused):
            await client.explain(statements)
        if async_engine is not None:
            assert async_engine.disposed  # pyright: ignore[reportAttributeAccessIssue]
        else:
            assert [ds.name for ds in evicted] == [client.datasource.name]


class TestAsyncCancellation:
    async def test_cancelled_during_the_plan_statement(self, monkeypatch: pytest.MonkeyPatch) -> None:
        db = FakeDb()
        client = async_client(monkeypatch, db)
        db.hold_async(PLAN_SQL)
        task = asyncio.create_task(client.explain(SESSION_STATEMENTS))
        await _until(db.entered[PLAN_SQL].is_set)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _until(lambda: (1, "CLOSE") in db.events)
        assert _explain_run(db, 1) == list(_EXPLAIN)
        assert _discarded(db, 1)
        assert _no_commit(db)

    async def test_cancelled_during_the_switch_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        db = FakeDb()
        client = async_client(monkeypatch, db)
        gate = db.hold_async(SESSION_OFF)
        task = asyncio.create_task(client.explain(SESSION_STATEMENTS))
        await _until(db.entered[SESSION_OFF].is_set)
        task.cancel()
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _until(lambda: (1, f"END {SESSION_OFF}") in db.events and (1, "CLOSE") in db.events)
        assert _discarded(db, 1)

    async def test_a_second_cancellation_cannot_skip_the_switch_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        db = FakeDb()
        client = async_client(monkeypatch, db)
        db.hold_async(PLAN_SQL)
        off_gate = db.hold_async(SESSION_OFF)
        task = asyncio.create_task(client.explain(SESSION_STATEMENTS))
        await _until(db.entered[PLAN_SQL].is_set)
        task.cancel()
        await _until(db.entered[SESSION_OFF].is_set)
        task.cancel()
        off_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _until(lambda: (1, f"END {SESSION_OFF}") in db.events and (1, "CLOSE") in db.events)
        assert _discarded(db, 1)

    async def test_a_cancellation_cannot_skip_the_invalidation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        db = FakeDb()
        client = async_client(monkeypatch, db)
        db.hold_async(PLAN_SQL)
        gate = db.hold_invalidate()
        task = asyncio.create_task(client.explain(SESSION_STATEMENTS))
        await _until(db.entered[PLAN_SQL].is_set)
        task.cancel()
        await _until(db.invalidating.is_set)
        task.cancel()
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _until(lambda: (1, "CLOSE") in db.events)
        assert _discarded(db, 1)

    async def test_a_switch_off_failure_under_cancellation_is_retrieved(self, monkeypatch: pytest.MonkeyPatch) -> None:
        unhandled: list[dict[str, object]] = []
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: unhandled.append(context))
        db = FakeDb()
        client = async_client(monkeypatch, db)
        gate = db.hold_async(SESSION_OFF)
        db.fail(SESSION_OFF, _off_error())
        task = asyncio.create_task(client.explain(SESSION_STATEMENTS))
        await _until(db.entered[SESSION_OFF].is_set)
        task.cancel()
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        del task
        gc.collect()
        await asyncio.sleep(0)
        gc.collect()
        assert unhandled == []
        assert _discarded(db, 1)


class TestThreadedCancellation:
    @pytest.mark.parametrize("held", [PLAN_SQL, SESSION_OFF], ids=["plan", "switch-off"])
    async def test_a_cancelled_caller_leaves_no_connection_in_plan_mode(
        self, held: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db = FakeDb()
        client = threaded_client(monkeypatch, db)
        gate = db.hold_threaded(held)
        task = asyncio.create_task(client.explain(tsql_statements()))
        await _until(db.entered[held].is_set)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        gate.set()
        await _until(lambda: (1, "CLOSE") in db.events)
        assert SESSION_OFF in _explain_run(db, 1)
        assert _restored_or_discarded(db, 1)
        assert _no_commit(db)


_PG_CLIENTS = {"async": async_client, "threaded": threaded_pg_client}


@pytest.mark.parametrize("path", PATHS)
class TestTimeout:
    async def test_the_timeout_runs_before_the_switch_on(self, path: str, monkeypatch: pytest.MonkeyPatch) -> None:
        db = FakeDb()
        await _PG_CLIENTS[path](monkeypatch, db).explain(SESSION_STATEMENTS)
        started = db.started(1)
        timeout = next(i for i, sql in enumerate(started) if sql.startswith("SET statement_timeout"))
        assert timeout < started.index(SESSION_ON)

    async def test_a_skipped_timeout_is_reported(self, path: str, monkeypatch: pytest.MonkeyPatch) -> None:
        db = FakeDb()
        client = _PG_CLIENTS[path](monkeypatch, db)
        timeout_sql = "SET statement_timeout = 120000"
        db.fail(timeout_sql, sqlalchemy.exc.ProgrammingError(timeout_sql, {}, Exception("permission denied")))
        with pytest.warns(SlayerStatementTimeoutSkippedWarning):
            result = await client.explain(SESSION_STATEMENTS)
        assert result.rows == PLAN_ROWS
        assert [w.reason for w in result.warnings] == ["timeout_rejected"]


def _pragma_db(tmp_path: Path) -> str:
    path = tmp_path / "pragma.db"
    with transaction(path) as conn:
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
    return str(path)


# A connection-level SQLite setting standing in for a plan mode: ON / OFF set it, the plan statement reads it.
_PRAGMA_ON, _PRAGMA_OFF = "PRAGMA cache_size = 1234", "PRAGMA cache_size = 4321"


class TestOnARealConnection:
    """One pooled connection: what the next statement sees is what the explain left behind."""

    @pytest.fixture
    def client(self, tmp_path: Path):
        path = _pragma_db(tmp_path)
        with disposable_engine(f"sqlite:///{path}", poolclass=sa.QueuePool, pool_size=1, max_overflow=0) as engine:
            yield SlayerSQLClient(DatasourceConfig(name="pragma", type="sqlite", database=path), sa_engine=engine)

    async def test_the_session_is_restored_after_a_plan(self, client: SlayerSQLClient) -> None:
        result = await client.explain(ExplainStatements(
            statement="PRAGMA cache_size", session_on=_PRAGMA_ON, session_off=_PRAGMA_OFF,
        ))
        assert result.rows == [{"cache_size": 1234}]
        assert (await client.execute("PRAGMA cache_size")).rows == [{"cache_size": 4321}]

    async def test_the_session_is_restored_after_a_failing_plan_statement(self, client: SlayerSQLClient) -> None:
        statements = ExplainStatements(
            statement="SELECT * FROM no_such_table", session_on=_PRAGMA_ON, session_off=_PRAGMA_OFF,
        )
        with pytest.raises(sqlalchemy.exc.OperationalError, match="no such table"):
            await client.explain(statements)
        assert (await client.execute("PRAGMA cache_size")).rows == [{"cache_size": 4321}]
        assert (await client.execute("SELECT id FROM t")).rows == [{"id": 1}]
