"""Read-only DuckDB and SQLite files, the missing-file refusal and the local-file release (spec: database/execution)."""

from __future__ import annotations

import asyncio
import contextlib
import functools
from collections.abc import Generator
from pathlib import Path
from typing import Any

import duckdb
import pytest
from sqlalchemy.exc import DBAPIError

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.database.client import DatabaseUnavailable, ExecutionResult, SlayerSQLClient
from slayer.storage.sqlite_conn import open_connection, transaction
from tests._engine_helpers import disposable_engine


def _seed_duckdb(path: Path) -> None:
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1)")


def _seed_sqlite(path: Path) -> None:
    with transaction(path) as conn:
        conn.executescript("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);")


def _sqlite_rows(path: Path) -> list[tuple[int, ...]]:
    with open_connection(path) as conn:
        return conn.execute("SELECT x FROM t").fetchall()


@contextlib.contextmanager
def _datasource(**fields: str) -> Generator[DatasourceConfig]:
    ds = DatasourceConfig.model_validate({"name": "local", **fields})
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


# ---------------------------------------------------------------------------
# DuckDB file
# ---------------------------------------------------------------------------


def test_a_write_through_a_duckdb_engine_fails(tmp_path: Path) -> None:
    path = tmp_path / "f.duckdb"
    _seed_duckdb(path)
    with _datasource(type="duckdb", database=str(path)) as ds:
        with engine_factory.get_engine(ds).connect() as conn:
            with pytest.raises(DBAPIError, match="(?i)read-only"):
                conn.exec_driver_sql("INSERT INTO t VALUES (2)")
    with duckdb.connect(str(path), read_only=True) as conn:
        assert conn.execute("SELECT x FROM t").fetchall() == [(1,)]


# ---------------------------------------------------------------------------
# SQLite file: main, attached and temporary databases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "write",
    [
        "INSERT INTO t VALUES (2)",
        "INSERT INTO other.t VALUES (2)",
        "CREATE TEMP TABLE scratch (x INTEGER)",
    ],
)
def test_a_sqlite_connection_refuses_writes_to_every_database(tmp_path: Path, write: str) -> None:
    main, other = tmp_path / "main.db", tmp_path / "other.db"
    _seed_sqlite(main)
    _seed_sqlite(other)
    with _datasource(type="sqlite", database=str(main)) as ds:
        with engine_factory.get_engine(ds).connect() as conn:
            conn.exec_driver_sql(f"ATTACH DATABASE '{other}' AS other")
            with pytest.raises(DBAPIError, match="(?i)readonly"):
                conn.exec_driver_sql(write)
            median = conn.exec_driver_sql("SELECT median(x) FROM (SELECT 1 AS x UNION ALL SELECT 3)").scalar()
    assert median == 2
    assert _sqlite_rows(main) == _sqlite_rows(other) == [(1,)]


# ---------------------------------------------------------------------------
# In-memory databases stay writable; a caller's engine is not modified
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("ds_type", "path"), [("sqlite", "sync"), ("sqlite", "async"), ("duckdb", "sync")])
def test_in_memory_databases_stay_writable(ds_type: str, path: str) -> None:
    statements = ["CREATE TABLE t (x INTEGER)", "INSERT INTO t VALUES (1), (2)", "SELECT x FROM t ORDER BY x"]
    with _datasource(type=ds_type, database=":memory:") as ds:
        client = SlayerSQLClient(ds)

        async def _go() -> list[ExecutionResult]:
            try:
                return [await client.execute(sql) for sql in statements]
            finally:
                await client.aclose()

        try:
            results = [client.execute_sync(sql) for sql in statements] if path == "sync" else asyncio.run(_go())
        finally:
            client.close()
    assert results[-1].rows == [{"x": 1}, {"x": 2}]


def test_a_callers_engine_is_not_modified(tmp_path: Path) -> None:
    path = tmp_path / "caller.db"
    _seed_sqlite(path)
    ds = DatasourceConfig(name="caller", type="sqlite", database=str(path))
    with disposable_engine(f"sqlite:///{path}") as engine:
        client = SlayerSQLClient(ds, sa_engine=engine)
        assert client.execute_sync("SELECT x FROM t").rows == [{"x": 1}]
        with engine.begin() as conn:
            conn.exec_driver_sql("INSERT INTO t VALUES (2)")
    assert _sqlite_rows(path) == [(1,), (2,)]


# ---------------------------------------------------------------------------
# A missing local database file is refused, and none is created
# ---------------------------------------------------------------------------


def _assert_missing_refused(ds: DatasourceConfig, *, path: Path, mode: str) -> None:
    client = SlayerSQLClient(ds)
    run = (
        functools.partial(client.execute_sync, "SELECT 1 AS x") if mode == "sync"
        else functools.partial(asyncio.run, client.execute("SELECT 1 AS x"))
    )
    try:
        with pytest.raises(DatabaseUnavailable) as raised:
            run()
    finally:
        client.close()
    assert ds.name in str(raised.value)
    assert str(path.name) in str(raised.value)
    assert not path.exists()


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("form", ["relative", "absolute"])
def test_a_missing_duckdb_file_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, form: str, mode: str) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "missing.duckdb"
    with _datasource(type="duckdb", database="missing.duckdb" if form == "relative" else str(path)) as ds:
        _assert_missing_refused(ds, path=path, mode=mode)
    assert list(tmp_path.iterdir()) == []


def test_a_duckdb_file_removed_before_connect_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "vanishing.duckdb"
    _seed_duckdb(path)
    real_connect = duckdb.connect

    def _vanish_then_connect(*args: Any, **kwargs: Any) -> Any:
        path.unlink(missing_ok=True)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", _vanish_then_connect)
    with _datasource(type="duckdb", database=str(path)) as ds:
        _assert_missing_refused(ds, path=path, mode="sync")


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("form", ["relative", "absolute", "uri"])
def test_a_missing_sqlite_file_is_refused_in_each_url_form(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, form: str, mode: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "missing.db"
    fields = {
        "relative": {"database": "missing.db"},
        "absolute": {"database": str(path)},
        "uri": {"connection_string": f"sqlite:///file:{path}?uri=true"},
    }[form]
    with _datasource(type="sqlite", **fields) as ds:
        _assert_missing_refused(ds, path=path, mode=mode)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("ds_type", "url"),
    [
        ("sqlite", "sqlite://"),
        ("sqlite", "sqlite:///:memory:"),
        ("sqlite", "sqlite:///file:mem_ro?mode=memory&cache=shared&uri=true"),
        ("duckdb", "duckdb:///:memory:"),
    ],
)
def test_in_memory_forms_are_not_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ds_type: str, url: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    with _datasource(type=ds_type, connection_string=url) as ds:
        client = SlayerSQLClient(ds)
        try:
            client.execute_sync("CREATE TABLE m (x INTEGER)")
            rows = client.execute_sync("SELECT 1 AS x").rows
        finally:
            client.close()
    assert rows == [{"x": 1}]
    assert list(tmp_path.iterdir()) == []


# ---------------------------------------------------------------------------
# The default client's synchronous call releases a local database file
# ---------------------------------------------------------------------------


def test_execute_sync_releases_a_duckdb_file(tmp_path: Path) -> None:
    path = tmp_path / "held.duckdb"
    _seed_duckdb(path)
    with _datasource(type="duckdb", database=str(path)) as ds:
        client = SlayerSQLClient(ds)
        try:
            assert client.execute_sync("SELECT x FROM t").rows == [{"x": 1}]
            with duckdb.connect(str(path)) as writer:
                writer.execute("INSERT INTO t VALUES (2)")
            assert client.execute_sync("SELECT x FROM t ORDER BY x").rows == [{"x": 1}, {"x": 2}]
        finally:
            client.close()


# ---------------------------------------------------------------------------
# Where a URL keeps its database
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "kind", "path"),
    [
        ("sqlite:///file:///abs/dir/a%20b.db?uri=true", "file", "/abs/dir/a b.db"),
        ("sqlite:///file://localhost/abs/c.db?uri=true", "file", "/abs/c.db"),
        ("sqlite:///file:rel.db", "file", "file:rel.db"),
        ("sqlite:///file::memory:?uri=true", "memory", None),
        ("sqlite:///file:reports:memory:.db?uri=true", "file", "reports:memory:.db"),
        ("duckdb:///md:analytics", "remote", None),
        ("duckdb://", "memory", None),
        ("postgresql+psycopg://u@h/d", "remote", None),
    ],
)
def test_local_database_classification(url: str, kind: str, path: str | None) -> None:
    local = engine_factory.local_database(url)
    assert (local.kind, local.path) == (kind, path)
