"""A disposed synchronous engine closes every connection it opened (spec: database/execution)."""

from __future__ import annotations

import asyncio
import gc
import logging
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import psycopg
import pytest
import sqlalchemy as sa
import sqlalchemy.event as sa_event
from pytest_postgresql import factories
from sqlalchemy.engine import make_url

from slayer.core.models import DatasourceConfig
from slayer.database import client as sql_client
from slayer.database import engine_factory
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None, postgres_options="-c unix_socket_directories=")

# Seconds before a wait counts as hung.
_BOUND = 15


def _is_open(dbapi_conn: sqlite3.Connection) -> bool:
    try:
        dbapi_conn.total_changes  # noqa: B018 - raises once closed
    except sqlite3.ProgrammingError:
        return False
    return True


def _record_connections(engine: sa.Engine) -> list[Any]:
    """Every DBAPI connection ``engine`` opens from now on, across its pools."""
    opened: list[Any] = []
    sa_event.listen(engine, "connect", lambda dbapi_conn, _record: opened.append(dbapi_conn))
    return opened


def _still_open(opened: list[Any]) -> list[Any]:
    """The connections left open; each is closed here so a failing test leaks nothing."""
    left = [conn for conn in opened if _is_open(conn)]
    for conn in left:
        conn.close()
    return left


@pytest.fixture
def file_ds(tmp_path: Path) -> Iterator[DatasourceConfig]:
    path = tmp_path / "f.db"
    path.touch()
    engine_factory.reset_cache()
    try:
        yield DatasourceConfig(name="f", type="sqlite", database=str(path))
    finally:
        engine_factory.reset_cache()


def _late_return(engine: sa.Engine, dispose: Callable[[], object]) -> list[Any]:
    """Check out a connection, run ``dispose`` while it is in use, use it, return it; the connections left open."""
    opened = _record_connections(engine)
    conn = engine.connect()
    try:
        dispose()
        assert conn.exec_driver_sql("SELECT 1").scalar() == 1
    finally:
        conn.close()
    return _still_open(opened)


class TestLateReturn:
    """A connection in use across a disposal keeps working and is closed when returned."""

    def test_invalidation(self, file_ds: DatasourceConfig) -> None:
        engine = engine_factory.get_engine(file_ds)
        assert _late_return(engine, lambda: engine_factory.invalidate_engine(file_ds)) == []

    def test_eviction(self, file_ds: DatasourceConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, "1")
        other = tmp_path / "other.db"
        other.touch()
        engine = engine_factory.get_engine(file_ds)

        def evict() -> None:
            engine_factory.get_engine(DatasourceConfig(name="other", type="sqlite", database=str(other)))
            assert engine_factory._cache_key(file_ds, file_ds.get_connection_string()) not in engine_factory._engine_cache

        assert _late_return(engine, evict) == []

    def test_private_in_memory_engine_close(self) -> None:
        client = sql_client.SlayerSQLClient(DatasourceConfig(name="m", type="sqlite", database=":memory:"))
        engine = client._get_sync_engine_for_client()
        assert client._sync_engine_owner == "client"
        assert _late_return(engine, client.close) == []

    def test_private_in_memory_engine_collected(self) -> None:
        """The finalizer backstop of an unclosed client disposes the same way."""
        client = sql_client.SlayerSQLClient(DatasourceConfig(name="m", type="sqlite", database=":memory:"))
        engine = client._get_sync_engine_for_client()

        def collect() -> None:
            nonlocal client
            del client
            gc.collect()

        assert _late_return(engine, collect) == []


def test_a_blocking_call_abandoned_by_cancellation_leaves_no_open_connection(file_ds: DatasourceConfig) -> None:
    client = sql_client.SlayerSQLClient(file_ds)
    engine = client._get_sync_engine_for_client()
    opened = _record_connections(engine)
    holding, release, finished = threading.Event(), threading.Event(), threading.Event()

    def blocking_read(conn: sa.Connection) -> None:
        try:
            conn.exec_driver_sql("SELECT 1")
            holding.set()
            release.wait(_BOUND)
        finally:
            finished.set()

    async def scenario() -> None:
        call = asyncio.ensure_future(client._blocking(sql_client._read_on_connection, engine, blocking_read))
        assert await asyncio.to_thread(holding.wait, _BOUND)
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call

    try:
        asyncio.run(scenario())
        assert engine_factory.invalidate_engine(file_ds)
    finally:
        release.set()
    assert finished.wait(_BOUND)
    # ``finished`` is set inside the ``with engine.connect()``; the worker returns the connection right after.
    deadline = time.monotonic() + _BOUND
    while any(_is_open(conn) for conn in opened) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _still_open(opened) == []


def test_an_engine_used_after_its_disposal_leaves_no_open_connection(file_ds: DatasourceConfig) -> None:
    engine = engine_factory.get_engine(file_ds)
    opened = _record_connections(engine)
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT 1").scalar() == 1
    engine_factory.reset_cache()
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT 1").scalar() == 1
    assert _still_open(opened) == []


class TestQuietDisposal:
    """The disposal routine logs and never raises."""

    def test_a_listener_registration_failure_is_logged(
        self, file_ds: DatasourceConfig, caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine = engine_factory.get_engine(file_ds)
        boom = RuntimeError("listener refused")
        with patch("sqlalchemy.event.listen", side_effect=boom), \
                patch("sqlalchemy.event.api.listen", side_effect=boom), \
                caplog.at_level(logging.WARNING, logger=engine_factory.logger.name):
            engine_factory._dispose_quietly(engine=engine, reason="probe")
        assert any(r.exc_info is not None and r.exc_info[1] is boom for r in caplog.records), caplog.records

    def test_a_listener_registration_failure_still_disposes(self, file_ds: DatasourceConfig) -> None:
        engine = engine_factory.get_engine(file_ds)
        boom = RuntimeError("listener refused")
        with patch("sqlalchemy.event.listen", side_effect=boom), \
                patch("sqlalchemy.event.api.listen", side_effect=boom), \
                patch.object(engine, "dispose", wraps=engine.dispose) as dispose:
            engine_factory._dispose_quietly(engine=engine, reason="probe")
        dispose.assert_called_once_with()

    def test_a_listener_registration_failure_still_retires(self, file_ds: DatasourceConfig) -> None:
        engine = engine_factory.get_engine(file_ds)
        boom = RuntimeError("listener refused")
        with patch("sqlalchemy.event.listen", side_effect=boom), \
                patch("sqlalchemy.event.api.listen", side_effect=boom):
            engine_factory._dispose_quietly(engine=engine, reason="probe")
        assert engine_factory.is_retired(engine)

    def test_a_dispose_failure_is_logged(self, file_ds: DatasourceConfig, caplog: pytest.LogCaptureFixture) -> None:
        engine = engine_factory.get_engine(file_ds)
        boom = RuntimeError("pool stuck")
        with patch.object(engine, "dispose", side_effect=boom), \
                caplog.at_level(logging.WARNING, logger=engine_factory.logger.name):
            engine_factory._dispose_quietly(engine=engine, reason="probe")
        assert any(r.exc_info is not None and r.exc_info[1] is boom for r in caplog.records), caplog.records

    def test_a_target_never_disposed_is_not_retired(self) -> None:
        assert engine_factory.is_retired(object()) is False  # pyright: ignore[reportArgumentType]

    def test_disposing_twice_registers_one_listener(self, file_ds: DatasourceConfig) -> None:
        engine = engine_factory.get_engine(file_ds)
        before = len(engine.pool.dispatch.checkin)
        engine_factory._dispose_quietly(engine=engine, reason="first")
        engine_factory._dispose_quietly(engine=engine, reason="second")
        assert len(engine.pool.dispatch.checkin) == before + 1


def _sessions(info: Any, *, application_name: str) -> int:
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as admin:
        row = admin.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = %s", (application_name,),
        ).fetchone()
    assert row is not None
    return row[0]


def test_a_server_session_is_closed_on_a_late_return(postgresql_proc: Any) -> None:
    app = f"late_return_{uuid.uuid4().hex[:12]}"
    with pg_database(postgresql_proc, statements=[], name="late") as base:
        url = make_url(base.get_connection_string()).update_query_dict({"application_name": app})
        ds = base.model_copy(update={"connection_string": url.render_as_string(hide_password=False)})
        engine_factory.reset_cache()
        engine = engine_factory.get_engine(ds)
        conn = engine.connect()
        held = conn.connection.driver_connection
        try:
            engine_factory.reset_cache()
            assert conn.exec_driver_sql("SELECT 1").scalar() == 1
        finally:
            conn.close()
        try:
            deadline = time.monotonic() + _BOUND
            while _sessions(postgresql_proc, application_name=app) and time.monotonic() < deadline:
                time.sleep(0.1)
            assert _sessions(postgresql_proc, application_name=app) == 0
        finally:
            if held is not None and not held.closed:
                held.close()
            engine_factory.invalidate_engine(ds)


def test_a_client_re_resolves_a_retired_factory_engine(postgresql_proc: Any) -> None:
    # psycopg2 is sync-only and networked, so the client pools on a factory engine across calls.
    with pg_database(postgresql_proc, statements=[], name="reresolve") as base:
        url = make_url(base.get_connection_string()).set(drivername="postgresql+psycopg2")
        ds = base.model_copy(update={"connection_string": url.render_as_string(hide_password=False)})
        engine_factory.reset_cache()
        client = sql_client.SlayerSQLClient(ds)

        async def run_once() -> list[dict[str, Any]]:
            return (await client.execute("SELECT 1 AS one")).rows

        try:
            assert asyncio.run(run_once()) == [{"one": 1}]
            assert client._sync_engine_owner == "factory"
            engine_factory.reset_cache()
            current = engine_factory.get_engine(ds)
            opened = _record_connections(current)
            assert asyncio.run(run_once()) == [{"one": 1}]
            assert asyncio.run(run_once()) == [{"one": 1}]
            assert client._sync_engine is current
            assert len(opened) == 1
        finally:
            client.close()
            engine_factory.invalidate_engine(ds)
