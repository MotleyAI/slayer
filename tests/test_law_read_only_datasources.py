"""python.database.arc42.md §3.7: read-only datasources (spec: database/execution, database/client-contract)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import duckdb
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from pytest_postgresql import factories

from slayer.core.models import DatasourceConfig
from slayer.database import client as sql_client
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.storage.sqlite_conn import transaction
from tests._pg_helpers import pg_database

postgresql_ro = factories.postgresql_proc(port=None, postgres_options="-c unix_socket_directories=")

_WRITE = "INSERT INTO t VALUES (2)"

_READ_ONLY_ERRORS = r"(?i)read-only|readonly"


def _write_refused() -> type[Exception]:
    return sql_client.WriteStatementRefused


@contextlib.contextmanager
def _local(tmp_path: Path, ds_type: str) -> Generator[DatasourceConfig]:
    path = tmp_path / f"f.{ds_type}"
    if ds_type == "duckdb":
        with duckdb.connect(str(path)) as conn:
            conn.execute("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1)")
    else:
        with transaction(path) as conn:
            conn.executescript("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);")
    ds = DatasourceConfig(name="local", type=ds_type, database=str(path))
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


@contextlib.contextmanager
def _postgres(info: Any, scheme: str) -> Generator[DatasourceConfig]:
    with pg_database(info, statements=["CREATE TABLE t (x INTEGER)", "INSERT INTO t VALUES (1)"], name="law_ro") as plain:
        ds = DatasourceConfig(
            name="pg", type="postgres",
            connection_string=f"{scheme}://{plain.username}@{plain.host}:{plain.port}/{plain.database}",
        )
        try:
            yield ds
        finally:
            engine_factory.invalidate_engine(ds)


def _write_through(engine: sa.Engine) -> None:
    with engine.connect() as conn:
        conn.exec_driver_sql(_WRITE)
        conn.commit()


def _write_after_invalidate(engine: sa.Engine) -> None:
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
        conn.invalidate()
    _write_through(engine)


def _write_through_async(ds: DatasourceConfig) -> None:
    async def _go() -> None:
        client = SlayerSQLClient(ds)
        try:
            async with client._get_async_engine().connect() as conn:
                await conn.exec_driver_sql(_WRITE)
                await conn.commit()
        finally:
            await client.aclose()

    asyncio.run(_go())


_ENGINE_PATHS: dict[str, Callable[[sa.Engine], None]] = {
    "factory": _write_through,
    "replacement": _write_after_invalidate,
}


# ---------------------------------------------------------------------------
# A write through any SLayer-built engine fails at the database
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", sorted(_ENGINE_PATHS))
@pytest.mark.parametrize("ds_type", ["duckdb", "sqlite"])
def test_local_file_engines_refuse_writes(tmp_path: Path, ds_type: str, path: str) -> None:
    with _local(tmp_path, ds_type) as ds:
        write, engine = _ENGINE_PATHS[path], engine_factory.get_engine(ds)
        with pytest.raises(DBAPIError, match=_READ_ONLY_ERRORS):
            write(engine)


@pytest.mark.parametrize("path", sorted(_ENGINE_PATHS))
@pytest.mark.parametrize("scheme", ["postgresql+psycopg", "postgresql+psycopg2", "postgresql+pg8000"])
def test_postgres_sync_engines_refuse_writes(postgresql_ro: Any, scheme: str, path: str) -> None:
    with _postgres(postgresql_ro, scheme) as ds:
        write, engine = _ENGINE_PATHS[path], engine_factory.get_engine(ds)
        with pytest.raises(DBAPIError, match="read-only transaction"):
            write(engine)


@pytest.mark.parametrize("scheme", ["postgresql+psycopg", "postgresql+asyncpg"])
def test_postgres_async_engines_refuse_writes(postgresql_ro: Any, scheme: str) -> None:
    with _postgres(postgresql_ro, scheme) as ds:
        with pytest.raises(DBAPIError, match="read-only transaction"):
            _write_through_async(ds)


# ---------------------------------------------------------------------------
# The default client sends no statement it cannot classify as a read
# ---------------------------------------------------------------------------


@pytest.fixture
def nothing_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a refused text reached an engine")

    monkeypatch.setattr(engine_factory, "get_engine", _refuse)
    monkeypatch.setattr(sql_client, "create_async_engine", _refuse)


@pytest.mark.usefixtures("nothing_sent")
@pytest.mark.parametrize(
    "ds",
    [
        DatasourceConfig(name="d", type="duckdb", database="/nonexistent/f.duckdb"),
        DatasourceConfig(name="s", type="sqlite", database="/nonexistent/f.db"),
        DatasourceConfig(name="p", type="postgres", connection_string="postgresql+psycopg://u@db.invalid/d"),
        DatasourceConfig(name="q", type="sqlserver", connection_string="mssql+pyodbc://u@db.invalid/d"),
    ],
    ids=lambda ds: ds.type,
)
def test_the_client_refuses_a_write_unsent(ds: DatasourceConfig) -> None:
    client, refused = SlayerSQLClient(ds), _write_refused()
    with pytest.raises(refused):
        client.execute_sync(_WRITE)


# ---------------------------------------------------------------------------
# In-memory databases stay writable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ds_type", ["sqlite", "duckdb"])
def test_in_memory_databases_stay_writable(ds_type: str) -> None:
    ds = DatasourceConfig(name=f"mem_{ds_type}", type=ds_type, database=":memory:")
    client = SlayerSQLClient(ds)
    try:
        client.execute_sync("CREATE TABLE t (x INTEGER)")
        client.execute_sync("INSERT INTO t VALUES (1)")
        rows = client.execute_sync("SELECT x FROM t").rows
    finally:
        client.close()
        engine_factory.invalidate_engine(ds)
    assert rows == [{"x": 1}]
