"""Connection policy: per-dialect transaction mode, ping after idle, Snowflake overrides on connect,
and the ``transaction_pooler`` opt-in (spec: sql/execution, sql/client-contract)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import psycopg
import pytest
import sqlalchemy as sa
import yaml
from pytest_postgresql import factories
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import QueuePool

from slayer.core.models import DatasourceConfig
from slayer.sql import client as sql_client
from slayer.sql import engine_factory
from slayer.sql.client import DelegatingSqlClient, SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import disposable_engine
from tests._pg_helpers import pg_database
from tests._pg_statement_log import LOGGED_POSTGRES_OPTIONS, ServerLog

pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

postgresql_logged = factories.postgresql_proc(port=None, postgres_options=LOGGED_POSTGRES_OPTIONS)

_PINGS = {";", "SELECT 1"}


class EngineKwargs:
    """The keyword arguments of every engine built while installed (the engines themselves are real)."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.sync: list[dict[str, Any]] = []
        self.async_: list[dict[str, Any]] = []
        self._built: list[Any] = []
        real_sync, real_async = sa.create_engine, sql_client.create_async_engine

        def _sync(*args: Any, **kwargs: Any) -> Any:
            self.sync.append(kwargs)
            engine = real_sync(*args, **kwargs)
            self._built.append(engine)
            return engine

        def _async(*args: Any, **kwargs: Any) -> Any:
            self.async_.append(kwargs)
            engine = real_async(*args, **kwargs)
            self._built.append(engine.sync_engine)
            return engine

        monkeypatch.setattr(sa, "create_engine", _sync)
        monkeypatch.setattr(sql_client, "create_async_engine", _async)

    def dispose(self) -> None:
        for engine in self._built:
            engine.dispose()


@pytest.fixture
def engine_kwargs(monkeypatch: pytest.MonkeyPatch) -> Generator[EngineKwargs]:
    engine_factory.reset_cache()
    spy = EngineKwargs(monkeypatch)
    try:
        yield spy
    finally:
        engine_factory.reset_cache()
        spy.dispose()


def _ds(url: str, *, ds_type: str | None, **extra: Any) -> DatasourceConfig:
    return DatasourceConfig.model_validate({"name": "ds", "type": ds_type, "connection_string": url, **extra})


def _build_async(ds: DatasourceConfig) -> None:
    async def _go() -> None:
        client = SlayerSQLClient(ds)
        client._get_async_engine()
        await client.aclose()

    asyncio.run(_go())


_AUTOCOMMIT = {"isolation_level": "AUTOCOMMIT", "skip_autocommit_rollback": True}


def _mode(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {k: kwargs.get(k) for k in ("isolation_level", "skip_autocommit_rollback", "pool_pre_ping")}


# ---------------------------------------------------------------------------
# Per-dialect transaction mode
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "ds_type"),
    [
        ("postgresql+psycopg://u@db.invalid/d", "postgres"),
        ("postgresql+psycopg2://u@db.invalid/d", "postgres"),
        ("postgresql+psycopg2://u@db.invalid/d", None),
        ("mysql+pymysql://u@db.invalid/d", "mysql"),
        ("mysql+pymysql://u@db.invalid/d", "mariadb"),
    ],
)
def test_sync_engine_runs_in_driver_autocommit(engine_kwargs: EngineKwargs, url: str, ds_type: str | None) -> None:
    engine_factory.get_engine(_ds(url, ds_type=ds_type))
    assert [_mode(k) for k in engine_kwargs.sync] == [{**_AUTOCOMMIT, "pool_pre_ping": None}]


@pytest.mark.parametrize(
    ("url", "ds_type"),
    [
        ("postgresql+psycopg://u@db.invalid/d", "postgres"),
        ("postgresql+asyncpg://u@db.invalid/d", "postgres"),
        ("mysql+aiomysql://u@db.invalid/d", "mysql"),
    ],
)
def test_async_engine_runs_in_driver_autocommit(engine_kwargs: EngineKwargs, url: str, ds_type: str) -> None:
    _build_async(_ds(url, ds_type=ds_type))
    assert [_mode(k) for k in engine_kwargs.async_] == [{**_AUTOCOMMIT, "pool_pre_ping": None}]


@pytest.mark.parametrize("url", ["postgresql+psycopg2://u@db.invalid/d", "postgresql+asyncpg://u@db.invalid/d"])
def test_pooler_datasource_stays_transactional(engine_kwargs: EngineKwargs, url: str) -> None:
    ds = _ds(url, ds_type="postgres", transaction_pooler=True)
    if "asyncpg" in url:
        _build_async(ds)
    else:
        engine_factory.get_engine(ds)
    built = engine_kwargs.sync + engine_kwargs.async_
    assert [k.get("isolation_level") for k in built] == [None]


def test_sqlite_keeps_its_transaction_mode(engine_kwargs: EngineKwargs, tmp_path: Path) -> None:
    engine_factory.get_engine(_ds(f"sqlite:///{tmp_path / 'f.db'}", ds_type="sqlite"))
    assert [_mode(k) for k in engine_kwargs.sync] == [
        {"isolation_level": None, "skip_autocommit_rollback": None, "pool_pre_ping": None},
    ]


# ---------------------------------------------------------------------------
# Live Postgres: autocommit connections, and the after-idle ping replacing a dead connection
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _pg(info: Any, *, scheme: str, **extra: Any) -> Generator[tuple[DatasourceConfig, ServerLog]]:
    with pg_database(info, statements=["CREATE TABLE t (x INTEGER)"], name="policy") as plain:
        assert plain.database is not None
        ds = DatasourceConfig.model_validate({
            "name": plain.name, "type": "postgres",
            "connection_string": f"{scheme}://{plain.username}@{plain.host}:{plain.port}/{plain.database}",
            **extra,
        })
        try:
            yield ds, ServerLog(logfile=info.logfile, database=plain.database)
        finally:
            engine_factory.invalidate_engine(ds)


def _terminate(ds: DatasourceConfig, pid: int) -> None:
    url = make_url(ds.get_connection_string())
    with psycopg.connect(host=url.host, port=url.port, user=url.username, dbname=url.database, autocommit=True) as c:
        c.execute(f"SELECT pg_terminate_backend({int(pid)})".encode())


@pytest.mark.parametrize(("pooler", "autocommit"), [(False, True), (True, False)])
def test_postgres_connections_autocommit_unless_pooled(postgresql_logged: Any, pooler: bool, autocommit: bool) -> None:
    with _pg(postgresql_logged, scheme="postgresql+psycopg2", transaction_pooler=pooler) as (ds, _log):
        with engine_factory.get_engine(ds).connect() as conn:
            dbapi_connection = conn.connection.dbapi_connection
            assert dbapi_connection is not None
            assert dbapi_connection.autocommit is autocommit


@pytest.mark.parametrize("scheme", ["postgresql+psycopg", "postgresql+psycopg2"])
def test_a_dead_idle_connection_is_replaced_before_the_statement(
    postgresql_logged: Any, scheme: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    with _pg(postgresql_logged, scheme=scheme) as (ds, log):
        client = SlayerSQLClient(ds)

        async def _go() -> tuple[int, int]:
            try:
                first = (await client.execute("SELECT pg_backend_pid() AS p")).rows[0]["p"]
                _terminate(ds, first)
                real = time.monotonic
                monkeypatch.setattr("slayer.sql.connection_policy.monotonic", lambda: real() + 61)
                log.mark()
                second = (await client.execute("SELECT pg_backend_pid() AS p, 'after' AS m")).rows[0]["p"]
                return first, second
            finally:
                await client.aclose()

        try:
            with caplog.at_level(logging.WARNING, logger="slayer.sql.client"):
                first, second = asyncio.run(_go())
        finally:
            client.close()
        on_new = [e.sql for e in log.statements(until="'after'") if e.pid == second]
    assert second != first
    assert not any("Transient DB error" in r.getMessage() for r in caplog.records)
    assert [s for s in on_new if s in _PINGS] == []


# ---------------------------------------------------------------------------
# Snowflake session overrides run once per connection, on connect
# ---------------------------------------------------------------------------


def test_snowflake_overrides_run_once_per_connection(tmp_path: Path) -> None:
    engine_factory.reset_cache()
    ds = DatasourceConfig(name="sf", type="snowflake", connection_name="default", warehouse="WH")
    with disposable_engine(f"sqlite:///{tmp_path / 'sf.db'}") as standin:
        with (
            patch("slayer.sql.dialects.snowflake.SnowflakeDialect.build_engine", return_value=standin),
            patch("slayer.sql.dialects.snowflake.SnowflakeDialect.apply_session_overrides") as apply_mock,
        ):
            engine = engine_factory.get_engine(ds)
            for _ in range(3):
                with engine.connect() as conn:
                    conn.exec_driver_sql("SELECT 1")
    engine_factory.reset_cache()
    assert apply_mock.call_count == 1
    assert apply_mock.call_args.kwargs["datasource"].warehouse == "WH"


# ---------------------------------------------------------------------------
# transaction_pooler: default, save-time and build-time refusal, cache identity, stored form
# ---------------------------------------------------------------------------


def test_transaction_pooler_defaults_to_false() -> None:
    assert DatasourceConfig(name="pg", type="postgres").transaction_pooler is False


@pytest.mark.parametrize(("ds_type", "named"), [("mysql", "mysql"), (None, r"(?i)none|no type"), ("sqlite", "sqlite")])
async def test_saving_the_flag_on_another_type_is_refused(tmp_path: Path, ds_type: str | None, named: str) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    ds = DatasourceConfig(name="ds", type=ds_type, host="h", database="d", transaction_pooler=True)
    with pytest.raises(ValueError, match=named):
        await storage.save_datasource(ds)
    assert await storage.get_datasource("ds") is None


@pytest.mark.parametrize("ds_type", ["postgres", "postgresql"])
async def test_saving_the_flag_on_postgres_is_kept(tmp_path: Path, ds_type: str) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="ds", type=ds_type, host="h", transaction_pooler=True))
    loaded = await storage.get_datasource("ds")
    assert loaded is not None and loaded.transaction_pooler is True


def test_a_non_postgres_driver_with_the_flag_fails_to_connect() -> None:
    ds = _ds("mysql+pymysql://u@db.invalid/d", ds_type="postgres", transaction_pooler=True)
    with pytest.raises(ValueError, match="mysql"):
        engine_factory.get_engine(ds)
    with pytest.raises(ValueError, match="mysql"):
        SlayerSQLClient(ds).execute_sync("SELECT 1")


def test_the_flag_splits_the_engine_cache() -> None:
    url = "postgresql+psycopg2://u@db.invalid/d"
    plain, pooled = _ds(url, ds_type="postgres"), _ds(url, ds_type="postgres", transaction_pooler=True)
    assert engine_factory.client_cache_key(plain) != engine_factory.client_cache_key(pooled)


async def test_a_stored_datasource_without_the_flag_loads_unchanged(tmp_path: Path) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="pg", type="postgres", host="h"))
    path = Path(storage.datasources_dir) / "pg.yaml"
    stored = yaml.safe_load(path.read_text())
    stored.pop("transaction_pooler", None)
    path.write_text(yaml.safe_dump(stored))
    before = path.read_bytes()
    loaded = await storage.get_datasource("pg")
    assert loaded is not None and loaded.transaction_pooler is False
    assert path.read_bytes() == before


# ---------------------------------------------------------------------------
# End-of-workflow release: idle local-file connections only
# ---------------------------------------------------------------------------


def _release(client: SlayerSQLClient) -> None:
    asyncio.run(client.release_local_connections())


@pytest.mark.parametrize("ds_type", ["sqlite", "duckdb"])
def test_release_drains_a_file_backed_pool(tmp_path: Path, ds_type: str) -> None:
    ds = DatasourceConfig(name="f", type=ds_type, database=str(tmp_path / f"f.{ds_type}"))
    client = SlayerSQLClient(ds)
    try:
        client.execute_sync("SELECT 1 AS x")
        pool = cast(QueuePool, engine_factory.get_engine(ds).pool)
        assert pool.checkedin() == 1
        _release(client)
        assert pool.checkedin() == 0
        assert client.execute_sync("SELECT 1 AS x").rows == [{"x": 1}]
    finally:
        client.close()
        engine_factory.invalidate_engine(ds)


@pytest.mark.parametrize("ds_type", ["sqlite", "duckdb"])
def test_release_keeps_an_in_memory_database(ds_type: str) -> None:
    client = SlayerSQLClient(DatasourceConfig(name="m", type=ds_type, database=":memory:"))
    try:
        client.execute_sync("CREATE TABLE kept (v INTEGER)")
        client.execute_sync("INSERT INTO kept VALUES (1)")
        _release(client)
        assert client.execute_sync("SELECT count(*) AS n FROM kept").rows == [{"n": 1}]
    finally:
        client.close()
        engine_factory.reset_cache()


def test_release_keeps_a_networked_pool(postgresql_logged: Any) -> None:
    with _pg(postgresql_logged, scheme="postgresql+psycopg2") as (ds, _log):
        client = SlayerSQLClient(ds)
        try:
            pid = client.execute_sync("SELECT pg_backend_pid() AS p").rows[0]["p"]
            _release(client)
            assert cast(QueuePool, engine_factory.get_engine(ds).pool).checkedin() == 1
            assert client.execute_sync("SELECT pg_backend_pid() AS p").rows[0]["p"] == pid
        finally:
            client.close()


class _ReleaseRecorder:
    def __init__(self) -> None:
        self.releases = 0

    async def release_local_connections(self) -> None:
        self.releases += 1


async def test_a_delegating_client_forwards_the_release() -> None:
    inner = _ReleaseRecorder()
    await DelegatingSqlClient(inner).release_local_connections()  # pyright: ignore[reportArgumentType]
    assert inner.releases == 1
