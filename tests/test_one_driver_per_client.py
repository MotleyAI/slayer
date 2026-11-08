"""A client connects through one driver: its caller's engine's, else its URL's (spec: database/execution)."""

from __future__ import annotations

import asyncio
import contextlib
import threading
import warnings
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine.url import make_url

from slayer.core.errors import UnsupportedDriverError
from slayer.core.models import DatasourceConfig
from slayer.sql import client as client_module
from slayer.sql import engine_factory
from slayer.sql.client import ObjectNotFound, SlayerSQLClient
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.reflection import TableLocation

pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

_HOST = "one-driver.invalid"
_PG = f"u:p%40ss@{_HOST}:5432/d"
_MY = f"u:p%40ss@{_HOST}:3306/d"


class _Touched(Exception):
    """Raised where an instrumented engine would first reach the database."""


# A tolerant catalog read may turn ``_Touched`` into "not found"; only the engines built matter here.
_EXPECTED_FAILURES = (_Touched, ObjectNotFound)


class _Engines:
    """Records every async engine the client builds and every engine it asks the factory for."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.async_urls: list[str] = []
        self.factory_urls: list[str] = []
        self.connect_threads: list[threading.Thread] = []
        monkeypatch.setattr(client_module, "create_async_engine", self._async_engine)
        monkeypatch.setattr(engine_factory, "get_engine", self._factory_engine)

    def _async_engine(self, url: Any, **_kwargs: Any) -> Any:
        self.async_urls.append(make_url(url).render_as_string(hide_password=False))
        raise _Touched("async engine")

    def _factory_engine(self, datasource: DatasourceConfig) -> MagicMock:
        self.factory_urls.append(datasource.get_connection_string())

        def _connect(*_args: Any, **_kwargs: Any) -> Any:
            self.connect_threads.append(threading.current_thread())
            raise _Touched("sync engine")

        engine = MagicMock(name="factory_engine")
        engine.connect.side_effect = _connect
        engine.url = make_url(datasource.get_connection_string())
        return engine


@pytest.fixture
def engines(monkeypatch: pytest.MonkeyPatch) -> _Engines:
    return _Engines(monkeypatch)


_LOCATION = TableLocation(name="t")

_CALLS: dict[str, Callable[[SlayerSQLClient], Awaitable[Any]]] = {
    "execute": lambda c: c.execute("SELECT 1"),
    "get_result_column_types": lambda c: c.get_result_column_types("SELECT 1 AS x"),
    "server_profile": lambda c: c.server_profile(),
    "ping": lambda c: c.ping(),
    "list_schemas": lambda c: c.list_schemas(),
    "default_schema": lambda c: c.default_schema(),
    "current_catalog": lambda c: c.current_catalog(),
    "list_objects": lambda c: c.list_objects(None),
    "reflect_columns": lambda c: c.reflect_columns(_LOCATION),
    "reflect_table_keys": lambda c: c.reflect_table_keys(_LOCATION),
    "table_comment": lambda c: c.table_comment(_LOCATION),
    "schema_description": lambda c: c.schema_description(None, None),
}


async def _call_all(client: SlayerSQLClient) -> None:
    for call in _CALLS.values():
        with contextlib.suppress(*_EXPECTED_FAILURES):
            await call(client)


def _ds(ds_type: str, connection_string: str) -> DatasourceConfig:
    return DatasourceConfig(name="d", type=ds_type, connection_string=connection_string)


def _rendered(url: str) -> str:
    return make_url(url).render_as_string(hide_password=False)


# ---------------------------------------------------------------------------
# Async-capable drivers: statements and metadata on one async engine
# ---------------------------------------------------------------------------

_ASYNC_CASES = [
    pytest.param(
        _ds("postgres", f"postgresql://{_PG}?sslmode=disable&connect_timeout=5"),
        f"postgresql+psycopg://{_PG}?sslmode=disable&connect_timeout=5", id="bare-postgres",
    ),
    pytest.param(
        DatasourceConfig(
            name="d", type="postgres", host=_HOST, port=5432, database="d", username="u", password="p@ss",
        ),
        f"postgresql+psycopg://{_PG}", id="structured-postgres",
    ),
    pytest.param(
        _ds("postgresql", f"postgresql+psycopg://{_PG}?sslmode=require&sslrootcert=/ca.pem"),
        f"postgresql+psycopg://{_PG}?sslmode=require&sslrootcert=/ca.pem", id="psycopg",
    ),
    pytest.param(
        _ds("postgres", f"postgresql+asyncpg://{_PG}?ssl=require&prepared_statement_cache_size=0"),
        f"postgresql+asyncpg://{_PG}?ssl=require&prepared_statement_cache_size=0", id="asyncpg",
    ),
    pytest.param(
        _ds("mysql", f"mysql+aiomysql://{_MY}?charset=utf8mb4"),
        f"mysql+aiomysql://{_MY}?charset=utf8mb4", id="aiomysql",
    ),
    pytest.param(
        _ds("mariadb", f"mariadb+aiomysql://{_MY}"), f"mariadb+aiomysql://{_MY}", id="mariadb-aiomysql",
    ),
]


@pytest.mark.parametrize(("ds", "expected"), _ASYNC_CASES)
@pytest.mark.parametrize("method", sorted(_CALLS))
async def test_async_driver_serves_every_method_without_a_factory_engine(
    engines: _Engines, ds: DatasourceConfig, expected: str, method: str,
) -> None:
    client = SlayerSQLClient(ds)
    with contextlib.suppress(*_EXPECTED_FAILURES):
        await _CALLS[method](client)
    assert engines.factory_urls == []
    assert set(engines.async_urls) <= {_rendered(expected)}


@pytest.mark.parametrize(("ds", "expected"), _ASYNC_CASES)
async def test_async_driver_builds_its_engine_from_the_url(
    engines: _Engines, ds: DatasourceConfig, expected: str,
) -> None:
    await _call_all(SlayerSQLClient(ds))
    assert engines.async_urls
    assert set(engines.async_urls) == {_rendered(expected)}
    assert engines.factory_urls == []


@pytest.mark.parametrize(("ds", "expected"), _ASYNC_CASES)
async def test_async_driver_sees_the_query_parameters_unchanged(
    engines: _Engines, ds: DatasourceConfig, expected: str,
) -> None:
    with contextlib.suppress(_Touched):
        await SlayerSQLClient(ds).execute("SELECT 1")
    [built] = set(engines.async_urls)
    assert make_url(built).query == make_url(expected).query
    assert make_url(built).drivername == make_url(expected).drivername


@pytest.mark.parametrize(("ds", "expected"), _ASYNC_CASES)
def test_async_driver_execute_sync_builds_no_factory_engine(
    engines: _Engines, ds: DatasourceConfig, expected: str,
) -> None:
    client = SlayerSQLClient(ds)
    with pytest.raises(_Touched):
        client.execute_sync("SELECT 1")
    assert set(engines.async_urls) == {_rendered(expected)}
    assert engines.factory_urls == []


# ---------------------------------------------------------------------------
# Sync-only drivers: everything on the factory's engine, off the event loop
# ---------------------------------------------------------------------------

_SYNC_CASES = [
    pytest.param(
        _ds("postgres", f"postgresql+psycopg2://{_PG}?sslmode=disable&sslrootcert=/ca.pem"),
        f"postgresql+psycopg2://{_PG}?sslmode=disable&sslrootcert=/ca.pem", id="psycopg2",
    ),
    pytest.param(_ds("postgres", f"postgresql+pg8000://{_PG}"), f"postgresql+pg8000://{_PG}", id="pg8000"),
    pytest.param(
        _ds("mysql", f"mysql://{_MY}?ssl_ca=/ca.pem"), f"mysql+pymysql://{_MY}?ssl_ca=/ca.pem", id="bare-mysql",
    ),
    pytest.param(_ds("mysql", f"mysql+pymysql://{_MY}"), f"mysql+pymysql://{_MY}", id="pymysql"),
    pytest.param(_ds("mysql", f"mysql+mysqldb://{_MY}"), f"mysql+mysqldb://{_MY}", id="mysqldb"),
    pytest.param(_ds("mariadb", f"mariadb://{_MY}"), f"mariadb+pymysql://{_MY}", id="bare-mariadb"),
    pytest.param(
        DatasourceConfig(name="d", type="mysql", host=_HOST, port=3306, database="d", username="u", password="p@ss"),
        f"mysql+pymysql://{_MY}", id="structured-mysql",
    ),
]


@pytest.mark.parametrize(("ds", "expected"), _SYNC_CASES)
async def test_sync_only_driver_builds_no_async_engine(
    engines: _Engines, ds: DatasourceConfig, expected: str,
) -> None:
    await _call_all(SlayerSQLClient(ds))
    assert engines.async_urls == []
    assert engines.factory_urls
    assert {_rendered(u) for u in engines.factory_urls} == {_rendered(expected)}


@pytest.mark.parametrize("ds", [pytest.param(p.values[0], id=p.id) for p in _SYNC_CASES])
async def test_sync_only_driver_runs_off_the_event_loop(engines: _Engines, ds: DatasourceConfig) -> None:
    await _call_all(SlayerSQLClient(ds))
    assert engines.connect_threads
    assert threading.current_thread() not in engines.connect_threads


@pytest.mark.parametrize(
    ("ds_type", "connection_string"),
    [
        ("postgres", f"postgresql+psycopg2://{_PG}?sslmode=disable"),
        ("postgres", f"postgresql+psycopg://{_PG}?sslmode=disable"),
        ("mysql", f"mysql+pymysql://{_MY}?ssl_ca=/ca.pem"),
    ],
)
def test_factory_engine_keeps_the_datasource_url(monkeypatch: pytest.MonkeyPatch, ds_type, connection_string) -> None:
    monkeypatch.setattr(engine_factory, "_engine_cache", OrderedDict())
    engine = engine_factory.get_engine(_ds(ds_type, connection_string))
    try:
        assert engine.url.render_as_string(hide_password=False) == _rendered(connection_string)
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# A loop the driver's async mode refuses
# ---------------------------------------------------------------------------


class ProactorEventLoop(asyncio.SelectorEventLoop):
    """Stands in for Windows' proactor loop, the loop kind psycopg's async mode refuses."""


def _on_proactor(coro: Awaitable[Any]) -> Any:
    loop = ProactorEventLoop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_postgres_declares_the_loops_psycopg_refuses() -> None:
    assert dialect_for_ds_type("postgres").refused_loops == {"psycopg": frozenset({"ProactorEventLoop"})}
    assert dialect_for_ds_type("postgres").driver_facts("postgres").refused_loops == {
        "psycopg": frozenset({"ProactorEventLoop"}),
    }


@pytest.mark.parametrize(
    "ds",
    [
        _ds("postgres", f"postgresql+psycopg://{_PG}?sslmode=disable"),
        _ds("postgres", f"postgresql://{_PG}?sslmode=disable"),
    ],
    ids=["psycopg", "bare-postgres"],
)
def test_refused_loop_runs_the_same_driver_synchronously(engines: _Engines, ds: DatasourceConfig) -> None:
    _on_proactor(_call_all(SlayerSQLClient(ds)))
    assert engines.async_urls == []
    assert {_rendered(u) for u in engines.factory_urls} == {_rendered(f"postgresql+psycopg://{_PG}?sslmode=disable")}
    assert engines.connect_threads
    assert threading.current_thread() not in engines.connect_threads


def test_a_loop_another_driver_accepts_stays_async(engines: _Engines) -> None:
    url = f"postgresql+asyncpg://{_PG}?ssl=disable"
    _on_proactor(_call_all(SlayerSQLClient(_ds("postgres", url))))
    assert set(engines.async_urls) == {_rendered(url)}
    assert engines.factory_urls == []


# ---------------------------------------------------------------------------
# A caller's engine
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "connection_string", [f"postgresql+psycopg://{_PG}", f"postgresql+asyncpg://{_PG}", f"postgresql://{_PG}"],
)
async def test_a_callers_engine_is_the_only_engine(engines: _Engines, connection_string: str) -> None:
    caller = MagicMock(name="caller_engine")
    caller.connect.side_effect = _Touched("caller engine")
    client = SlayerSQLClient(_ds("postgres", connection_string), sa_engine=caller)
    await _call_all(client)
    with contextlib.suppress(_Touched):
        client.execute_sync("SELECT 1")
    assert caller.connect.called
    assert engines.async_urls == []
    assert engines.factory_urls == []


# ---------------------------------------------------------------------------
# A bundled non-default driver warns until 2.0
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ds", "driver", "extra"),
    [
        pytest.param(_ds("postgres", f"postgresql+psycopg2://{_PG}"), "psycopg2", "postgres", id="psycopg2"),
        pytest.param(_ds("postgres", f"postgresql+asyncpg://{_PG}"), "asyncpg", "postgres", id="asyncpg"),
        pytest.param(_ds("mysql", f"mysql+aiomysql://{_MY}"), "aiomysql", "mysql", id="aiomysql"),
        pytest.param(_ds("mariadb", f"mariadb+aiomysql://{_MY}"), "aiomysql", "mysql", id="mariadb-aiomysql"),
    ],
)
async def test_bundled_non_default_driver_warns(engines: _Engines, ds: DatasourceConfig, driver: str, extra: str) -> None:
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        with contextlib.suppress(*_EXPECTED_FAILURES):
            await SlayerSQLClient(ds).execute("SELECT 1")
    messages = [str(w.message) for w in record if issubclass(w.category, FutureWarning)]
    assert any(driver in m and f"motley-slayer[{extra}]" in m and "2.0" in m for m in messages), messages


@pytest.mark.parametrize(
    "ds",
    [
        pytest.param(_ds("postgres", f"postgresql://{_PG}"), id="bare-postgres"),
        pytest.param(_ds("postgres", f"postgresql+psycopg://{_PG}"), id="psycopg"),
        pytest.param(_ds("mysql", f"mysql+pymysql://{_MY}"), id="pymysql"),
        pytest.param(_ds("mysql", f"mysql://{_MY}"), id="bare-mysql"),
        pytest.param(_ds("postgres", f"postgresql+pg8000://{_PG}"), id="pg8000"),
    ],
)
async def test_default_and_unbundled_drivers_are_silent(engines: _Engines, ds: DatasourceConfig) -> None:
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        with contextlib.suppress(*_EXPECTED_FAILURES):
            await SlayerSQLClient(ds).execute("SELECT 1")
    assert [str(w.message) for w in record if issubclass(w.category, FutureWarning)] == []


# ---------------------------------------------------------------------------
# The URL-switching helpers are gone
# ---------------------------------------------------------------------------


def test_no_url_is_moved_to_another_driver() -> None:
    assert not hasattr(client_module, "_async_connection_string")
    assert not hasattr(engine_factory, "_sync_connection_string")


@pytest.mark.parametrize(
    "url,is_async",
    [
        ("postgresql+psycopg://", True),
        ("postgresql+asyncpg://", True),
        ("mysql+aiomysql://", True),
        ("mariadb+aiomysql://", True),
        ("postgresql+psycopg2://", False),
        ("postgresql+pg8000://", False),
        ("mysql+pymysql://", False),
        ("mysql+mysqldb://", False),
        ("mssql+pyodbc://", False),
    ],
)
def test_sqlalchemy_async_capability_pin(url, is_async) -> None:
    """Pins the underscore ``get_dialect(_is_async=...)`` keyword the one-driver rule relies on."""
    assert make_url(url).get_dialect(_is_async=True).is_async is is_async


# ---------------------------------------------------------------------------
# An async SQLite driver is refused
# ---------------------------------------------------------------------------


class _NoEngine:
    """Fails the test on any engine construction."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(client_module, "create_async_engine", self._built)
        monkeypatch.setattr(engine_factory.sa, "create_engine", self._built)

    def _built(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("an engine was built")


_AIOSQLITE_URLS = ["sqlite+aiosqlite:///{tmp}/x.db", "sqlite+aiosqlite://"]


@pytest.mark.parametrize("url", _AIOSQLITE_URLS)
async def test_an_async_sqlite_driver_is_refused(monkeypatch, tmp_path, url) -> None:
    _NoEngine(monkeypatch)
    ds = DatasourceConfig(name="lite", type="sqlite", connection_string=url.format(tmp=tmp_path))
    client = SlayerSQLClient(datasource=ds)
    with pytest.raises(UnsupportedDriverError, match=r"sqlite://") as exc:
        await client.execute("SELECT 1")
    assert exc.value.driver == "aiosqlite"
    with pytest.raises(UnsupportedDriverError):
        await client.list_schemas()
    client.close()


@pytest.mark.parametrize("url", _AIOSQLITE_URLS)
def test_an_async_sqlite_driver_is_refused_synchronously(monkeypatch, tmp_path, url) -> None:
    _NoEngine(monkeypatch)
    ds = DatasourceConfig(name="lite", type="sqlite", connection_string=url.format(tmp=tmp_path))
    client = SlayerSQLClient(datasource=ds)
    with pytest.raises(UnsupportedDriverError, match=r"sqlite://"):
        client.execute_sync("SELECT 1")
    with pytest.raises(UnsupportedDriverError, match=r"sqlite://"):
        engine_factory.get_engine(ds)


@pytest.mark.parametrize(("ds_type", "needs_sync"), [("sqlite", True), ("snowflake", True), ("postgres", False),
                                                       ("mysql", False), ("duckdb", False)])
def test_per_connection_hooks_need_sync_connections(ds_type, needs_sync) -> None:
    assert dialect_for_ds_type(ds_type).needs_sync_connections is needs_sync
