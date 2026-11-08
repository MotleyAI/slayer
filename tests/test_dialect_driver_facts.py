"""Driver facts (URL scheme, default driver, extra drivers, install extra) live on the dialect registry and nowhere else."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.engine.url import make_url

from slayer.core.models import DatasourceConfig
from slayer.sql import client, dialects, engine_factory
from slayer.sql.dialects import PostgresDialect, dialect_for_ds_type
from slayer.sql.dialects.base import DriverFacts, SqlDialect

_REPO_ROOT = Path(__file__).resolve().parents[1]

_PG_EXTRA_DRIVERS = {"psycopg": "psycopg", "psycopg2": "psycopg2-binary", "asyncpg": "asyncpg"}
_MYSQL_EXTRA_DRIVERS = {"pymysql": "pymysql", "aiomysql": "aiomysql"}

# ds type → (url_scheme, default_driver, install_extra)
_EXPECTED = {
    "postgres": ("postgresql+psycopg", "psycopg", "postgres"),
    "postgresql": ("postgresql+psycopg", "psycopg", "postgres"),
    "mysql": ("mysql+pymysql", "pymysql", "mysql"),
    "mariadb": ("mysql+pymysql", "pymysql", "mysql"),
    "clickhouse": ("clickhouse+http", "http", "clickhouse"),
    "mssql": ("mssql+pyodbc", "pyodbc", "sqlserver"),
    "sqlserver": ("mssql+pyodbc", "pyodbc", "sqlserver"),
    "tsql": ("mssql+pyodbc", "pyodbc", "sqlserver"),
    "snowflake": ("snowflake", None, "snowflake"),
    "bigquery": ("bigquery", None, "bigquery"),
    "sqlite": ("sqlite", None, None),
    "duckdb": ("duckdb", None, None),
    "redshift": (None, None, None),
    "trino": ("trino", None, "trino"),
    "presto": (None, None, None),
    "athena": (None, None, None),
    "databricks": (None, None, None),
    "spark": (None, None, None),
    "oracle": (None, None, None),
}

# ds type → extra drivers (driver → distribution); every other type has none.
_EXTRA_DRIVERS = {
    "postgres": _PG_EXTRA_DRIVERS,
    "postgresql": _PG_EXTRA_DRIVERS,
    "mysql": _MYSQL_EXTRA_DRIVERS,
    "mariadb": _MYSQL_EXTRA_DRIVERS,
}

_STRUCTURED: dict[str, Any] = dict(host="h.example", port=1234, database="db", username="u", password="p@ss")


def test_every_registered_type_is_covered() -> None:
    assert set(dialects._BY_DS_TYPE) == set(_EXPECTED)


@pytest.mark.parametrize("ds_type", sorted(_EXPECTED))
def test_dialect_declares_driver_facts(ds_type) -> None:
    d = dialect_for_ds_type(ds_type)
    declared = (d.url_scheme, d.default_driver, d.install_extra)
    assert declared == _EXPECTED[ds_type]
    assert d.extra_drivers == _EXTRA_DRIVERS.get(ds_type, {})


@pytest.mark.parametrize("ds_type", sorted(_EXPECTED))
def test_driver_facts_carry_the_dialects_facts(ds_type) -> None:
    facts = dialect_for_ds_type(ds_type).driver_facts(ds_type)
    assert facts.default_driver == _EXPECTED[ds_type][1]
    assert facts.install_extra == _EXPECTED[ds_type][2]
    assert facts.extra_drivers == _EXTRA_DRIVERS.get(ds_type, {})


def test_unregistered_type_gets_no_fallback_driver_facts() -> None:
    facts = dialect_for_ds_type("foo").driver_facts("foo")
    assert facts == DriverFacts(url_scheme="foo")
    assert facts.default_driver is None
    assert facts.extra_drivers == {}


@pytest.mark.parametrize(
    "url_scheme,default_driver",
    [("postgresql+psycopg", "psycopg"), ("mssql+pyodbc", "pyodbc"), ("sqlite", None), (None, None)],
)
def test_default_driver_is_the_scheme_driver(url_scheme, default_driver) -> None:
    assert DriverFacts(url_scheme=url_scheme).default_driver == default_driver


@pytest.mark.parametrize("model", [DriverFacts, SqlDialect])
def test_no_sync_or_async_driver_facts(model) -> None:
    removed = {"sync_driver", "async_driver", "type_probe_uses_sync_driver"}
    assert not removed & (set(model.model_fields) | {n for n in vars(model) if not n.startswith("__")})


@pytest.mark.parametrize(
    "ds_type,backends",
    [
        ("postgres", {"postgresql"}),
        ("mysql", {"mysql", "mariadb"}),
        ("mariadb", {"mysql", "mariadb"}),
        ("sqlserver", {"mssql"}),
        ("foo", {"foo"}),
    ],
)
def test_url_backends(ds_type, backends) -> None:
    assert dialect_for_ds_type(ds_type).driver_facts(ds_type).url_backends == backends


def test_no_scheme_serves_no_backend() -> None:
    assert DriverFacts(url_backend_aliases=frozenset({"mariadb"})).url_backends == frozenset()


def _pyproject_extras() -> dict[str, list[str]]:
    return tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())["tool"]["poetry"]["extras"]


def test_install_extras_exist_in_pyproject() -> None:
    declared = {facts[2] for facts in _EXPECTED.values()} - {None}
    assert declared <= set(_pyproject_extras())


@pytest.mark.parametrize(("extra", "ds_type"), [("postgres", "postgres"), ("mysql", "mysql")])
def test_extra_installs_exactly_its_extra_drivers(extra, ds_type) -> None:
    assert set(_pyproject_extras()[extra]) == set(dialect_for_ds_type(ds_type).extra_drivers.values())


def test_all_extra_installs_every_extra_driver() -> None:
    every = {dist for t in _EXPECTED for dist in dialect_for_ds_type(t).extra_drivers.values()}
    assert every <= set(_pyproject_extras()["all"])


@pytest.mark.parametrize("ds_type", sorted(_EXTRA_DRIVERS))
def test_default_driver_is_an_extra_driver(ds_type) -> None:
    d = dialect_for_ds_type(ds_type)
    assert d.default_driver in d.extra_drivers


_EXTRA_DRIVER_URLS = [
    *(f"postgresql+{driver}://" for driver in _PG_EXTRA_DRIVERS),
    *(f"mysql+{driver}://" for driver in _MYSQL_EXTRA_DRIVERS),
]


@pytest.mark.parametrize("url", _EXTRA_DRIVER_URLS)
def test_every_extra_driver_is_a_sqlalchemy_driver(url) -> None:
    assert make_url(url).get_dialect().driver == make_url(url).get_driver_name()


def test_no_parallel_async_driver_table() -> None:
    assert not hasattr(client, "_ASYNC_DRIVERS")
    assert not hasattr(client, "_async_connection_string")
    assert not hasattr(engine_factory, "_sync_connection_string")


def test_registry_is_the_one_driver_table(monkeypatch) -> None:
    swapped = PostgresDialect().model_copy(update={"url_scheme": "postgresql+pg8000"})
    monkeypatch.setitem(dialects._BY_DS_TYPE, "postgres", swapped)
    ds = DatasourceConfig(name="d", type="postgres", **_STRUCTURED)
    assert ds.get_connection_string().startswith("postgresql+pg8000://")
    bare = DatasourceConfig(name="d", type="postgres", connection_string="postgresql://h/d")
    assert bare.get_connection_string() == "postgresql+pg8000://h/d"


# ---------------------------------------------------------------------------
# Structured-config URLs stay byte-identical (regression guards)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ds_type,expected",
    [
        ("postgres", "postgresql+psycopg://u:p%40ss@h.example:1234/db"),
        ("postgresql", "postgresql+psycopg://u:p%40ss@h.example:1234/db"),
        ("mysql", "mysql+pymysql://u:p%40ss@h.example:1234/db"),
        ("mariadb", "mysql+pymysql://u:p%40ss@h.example:1234/db"),
        ("clickhouse", "clickhouse+http://u:p%40ss@h.example:1234/db"),
        ("bigquery", "bigquery://u:p%40ss@h.example:1234/db"),
        ("redshift", "redshift://u:p%40ss@h.example:1234/db"),
        ("trino", "trino://u:p%40ss@h.example:1234/db"),
        ("presto", "presto://u:p%40ss@h.example:1234/db"),
        ("athena", "athena://u:p%40ss@h.example:1234/db"),
        ("databricks", "databricks://u:p%40ss@h.example:1234/db"),
        ("spark", "spark://u:p%40ss@h.example:1234/db"),
        ("oracle", "oracle://u:p%40ss@h.example:1234/db"),
        ("foo", "foo://u:p%40ss@h.example:1234/db"),
        (
            "sqlserver",
            "mssql+pyodbc://u:p%40ss@h.example:1234/db"
            "?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server",
        ),
    ],
)
def test_structured_url_unchanged(ds_type, expected) -> None:
    ds = DatasourceConfig(name="d", type=ds_type, **_STRUCTURED)
    assert ds.get_connection_string() == expected


def test_structured_url_without_type_still_raises() -> None:
    ds = DatasourceConfig(name="d", type=None, host="h")
    with pytest.raises(TypeError, match="drivername must be a string"):
        ds.get_connection_string()


# ---------------------------------------------------------------------------
# Docs name each type's extra
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ds_type", sorted(t for t, f in _EXPECTED.items() if f[2]))
def test_datasource_docs_row_names_extra(ds_type) -> None:
    text = (_REPO_ROOT / "docs" / "configuration" / "datasources.md").read_text()
    extra = f"motley-slayer[{_EXPECTED[ds_type][2]}]"
    rows = [
        line for line in text.splitlines()
        if line.strip().startswith("|") and f"`{ds_type}`" in line
    ]
    assert rows, f"no datasource table row names `{ds_type}`"
    assert any(extra in row for row in rows), (
        f"no row naming `{ds_type}` mentions {extra}: {rows}"
    )
