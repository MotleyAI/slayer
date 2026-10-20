"""A connection URL that names no driver connects with SLayer's default driver, not SQLAlchemy's."""

from __future__ import annotations

import sys
from collections import OrderedDict

import pytest
import sqlalchemy.dialects.postgresql.base as pg_base
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
from sqlalchemy.engine.url import make_url

from slayer.core.errors import MissingDriverError
from slayer.core.models import DatasourceConfig
from slayer.sql import dialects, engine_factory
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.base import DriverFacts

_HOST = "default-driver.invalid"

# ds type → (default driver, a backend it serves)
_DEFAULT_DRIVER = {
    "postgres": ("psycopg2", "postgresql"),
    "postgresql": ("psycopg2", "postgresql"),
    "mysql": ("pymysql", "mysql"),
    "mariadb": ("pymysql", "mariadb"),
    "clickhouse": ("http", "clickhouse"),
    "sqlserver": ("pyodbc", "mssql"),
    "mssql": ("pyodbc", "mssql"),
    "tsql": ("pyodbc", "mssql"),
}


@pytest.fixture(autouse=True)
def _isolated_engine_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(engine_factory, "_engine_cache", OrderedDict())


@pytest.fixture
def sa21_postgres_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """SQLAlchemy 2.1's default: bare ``postgresql://`` resolves to psycopg 3."""
    monkeypatch.setattr(pg_base, "dialect", PGDialect_psycopg)
    assert make_url("postgresql://").get_dialect().driver == "psycopg"


def _url(ds_type: str | None, connection_string: str) -> str:
    return DatasourceConfig(name="d", type=ds_type, connection_string=connection_string).get_connection_string()


# ---------------------------------------------------------------------------
# Pinned
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ds_type,connection_string,expected",
    [
        ("postgres", "postgresql://u:p%40ss@h:5432/d?sslmode=require",
         "postgresql+psycopg2://u:p%40ss@h:5432/d?sslmode=require"),
        ("postgresql", "postgresql://u:p%40ss@h:5432/d?sslmode=require",
         "postgresql+psycopg2://u:p%40ss@h:5432/d?sslmode=require"),
        ("mysql", "mysql://u:p%40ss@h/d?charset=utf8mb4", "mysql+pymysql://u:p%40ss@h/d?charset=utf8mb4"),
        ("mariadb", "mariadb://u@h/d", "mariadb+pymysql://u@h/d"),
        ("mysql", "mariadb://u@h/d", "mariadb+pymysql://u@h/d"),
        ("mariadb", "mysql://u@h/d", "mysql+pymysql://u@h/d"),
        ("clickhouse", "clickhouse://u@h/d", "clickhouse+http://u@h/d"),
        ("sqlserver", "mssql://u@h/d", "mssql+pyodbc://u@h/d"),
        ("mssql", "mssql://u@h/d", "mssql+pyodbc://u@h/d"),
        ("tsql", "mssql://u@h/d", "mssql+pyodbc://u@h/d"),
    ],
)
def test_bare_url_pinned_to_default_driver(ds_type, connection_string, expected) -> None:
    assert _url(ds_type, connection_string) == expected


def test_pin_preserves_every_url_part() -> None:
    cs = "postgresql://us%3Aer:p%40ss%2Fw@db.example:6543/warehouse?sslmode=require&application_name=x"
    out = make_url(_url("postgres", cs))
    assert out == make_url(cs).set(drivername="postgresql+psycopg2")
    assert out.password == "p@ss/w"


# ---------------------------------------------------------------------------
# Used exactly as given
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ds_type,connection_string",
    [
        ("postgres", "postgresql+psycopg://u@h/d?b=2&a=1"),
        ("postgres", "postgresql+asyncpg://u@h/d?b=2&a=1"),
        ("postgres", "postgresql+psycopg2://u@h/d?b=2&a=1"),
        ("mysql", "mysql+mysqldb://u@h/d?b=2&a=1"),
        ("mariadb", "mariadb+mysqldb://u@h/d?b=2&a=1"),
    ],
)
def test_named_driver_kept(ds_type, connection_string) -> None:
    assert _url(ds_type, connection_string) == connection_string


@pytest.mark.parametrize(
    "ds_type,connection_string",
    [
        ("sqlite", "sqlite:///tmp/x.db"),
        ("sqlite", "sqlite:///:memory:"),
        ("sqlite", ":memory:"),
        ("duckdb", "duckdb:///tmp/x.duckdb"),
        ("snowflake", "snowflake://u:p@acct/db?warehouse=w"),
        ("trino", "trino://u@h:8080/catalog"),
        ("redshift", "postgresql://u@h:5439/d?b=2&a=1"),
        ("foo", "postgresql://u@h/d?b=2&a=1"),
        (None, "postgresql://u@h/d?b=2&a=1"),
    ],
)
def test_type_without_default_driver_untouched(ds_type, connection_string) -> None:
    assert _url(ds_type, connection_string) == connection_string


def test_in_memory_sqlite_structured_untouched() -> None:
    ds = DatasourceConfig(name="d", type="sqlite", database=":memory:")
    assert ds.get_connection_string() == "sqlite:///:memory:"


@pytest.mark.parametrize(
    "ds_type,connection_string",
    [
        ("postgres", "mysql://u@h/d?b=2&a=1"),
        ("mysql", "postgresql://u@h/d?b=2&a=1"),
        ("clickhouse", "mysql://u@h/d"),
        ("postgres", "not a url"),
        ("mysql", "garbage::/"),
    ],
)
def test_foreign_backend_or_unparseable_untouched(ds_type, connection_string) -> None:
    assert _url(ds_type, connection_string) == connection_string


def test_stored_connection_string_not_rewritten() -> None:
    ds = DatasourceConfig(name="d", type="postgres", connection_string=f"postgresql://u@{_HOST}/d")
    engine = engine_factory.get_engine(ds)
    try:
        assert engine.url.drivername == "postgresql+psycopg2"
    finally:
        engine.dispose()
    assert ds.get_connection_string() == f"postgresql+psycopg2://u@{_HOST}/d"
    assert ds.connection_string == f"postgresql://u@{_HOST}/d"
    assert ds.model_dump()["connection_string"] == f"postgresql://u@{_HOST}/d"


# ---------------------------------------------------------------------------
# Resolution does not depend on SQLAlchemy's defaults
# ---------------------------------------------------------------------------


def test_default_driver_types_match_registry() -> None:
    declared = {t for t in dialects._BY_DS_TYPE if dialect_for_ds_type(t).driver_facts(t).sync_driver}
    assert declared == set(_DEFAULT_DRIVER)


@pytest.mark.parametrize("simulate_sa21", [False, True], ids=["installed", "sa21-postgres-default"])
@pytest.mark.parametrize("ds_type", sorted(_DEFAULT_DRIVER))
def test_sqlalchemy_resolves_declared_default_driver(request, ds_type, simulate_sa21) -> None:
    if simulate_sa21:
        request.getfixturevalue("sa21_postgres_default")
    driver, backend = _DEFAULT_DRIVER[ds_type]
    structured = DatasourceConfig(name="d", type=ds_type, host="h", database="db", username="u", password="p")
    for url in (structured.get_connection_string(), _url(ds_type, f"{backend}://u@h/d")):
        parsed = make_url(url)
        assert parsed.get_dialect().driver == driver
        assert parsed.drivername.partition("+")[2] == driver


# ---------------------------------------------------------------------------
# Engines see the pinned URL
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("sa21_postgres_default")
def test_sync_engine_uses_psycopg2_whatever_sqlalchemys_default() -> None:
    ds = DatasourceConfig(name="d", type="postgres", connection_string=f"postgresql://u@{_HOST}/d")
    engine = engine_factory.get_engine(ds)
    try:
        assert engine.url.drivername == "postgresql+psycopg2"
        assert engine.dialect.driver == "psycopg2"
    finally:
        engine.dispose()


@pytest.mark.usefixtures("sa21_postgres_default")
async def test_async_engine_uses_asyncpg_whatever_sqlalchemys_default() -> None:
    ds = DatasourceConfig(name="d", type="postgres", connection_string=f"postgresql://u@{_HOST}/d")
    client = SlayerSQLClient(ds)
    try:
        engine = client._get_async_engine()
        assert engine is not None
        assert engine.url.drivername == "postgresql+asyncpg"
    finally:
        await client.aclose()


@pytest.mark.usefixtures("sa21_postgres_default")
def test_missing_default_driver_names_extra(monkeypatch) -> None:
    ds = DatasourceConfig(name="pg_ds", type="postgres", connection_string=f"postgresql://u@{_HOST}/d")
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    assert info.value.missing == "psycopg2"
    assert "pip install 'motley-slayer[postgres]'" in str(info.value)


# ---------------------------------------------------------------------------
# The pin step itself
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "facts,url,expected",
    [
        (DriverFacts(url_scheme="postgresql+psycopg2"), "postgresql://u@h/d", "postgresql+psycopg2://u@h/d"),
        (DriverFacts(url_scheme="postgresql+psycopg2"), "postgresql+pg8000://u@h/d", "postgresql+pg8000://u@h/d"),
        (DriverFacts(url_scheme="postgresql"), "postgresql://u@h/d", "postgresql://u@h/d"),
        (DriverFacts(url_scheme="foo"), "foo://u@h/d", "foo://u@h/d"),
        (DriverFacts(), "postgresql://u@h/d", "postgresql://u@h/d"),
        (
            DriverFacts(url_scheme="mysql+pymysql", url_backend_aliases=frozenset({"mariadb"})),
            "mariadb://u@h/d", "mariadb+pymysql://u@h/d",
        ),
        (DriverFacts(url_scheme="mysql+pymysql"), "mariadb://u@h/d", "mariadb://u@h/d"),
    ],
)
def test_pin_default_driver(facts, url, expected) -> None:
    assert facts.pin_default_driver(url) == expected
