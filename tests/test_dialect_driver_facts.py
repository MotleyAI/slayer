"""Driver facts (URL scheme, sync/async driver, install extra) live on the dialect registry and nowhere else."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.engine.url import make_url

from slayer.core.models import DatasourceConfig
from slayer.sql import client, dialects
from slayer.sql.dialects import PostgresDialect, dialect_for_ds_type

_REPO_ROOT = Path(__file__).resolve().parents[1]

# ds type → (url_scheme, sync_driver, async_driver, install_extra)
_EXPECTED = {
    "postgres": ("postgresql", "psycopg2", "asyncpg", "postgres"),
    "postgresql": ("postgresql", "psycopg2", "asyncpg", "postgres"),
    "mysql": ("mysql+pymysql", "pymysql", "aiomysql", "mysql"),
    "mariadb": ("mysql+pymysql", "pymysql", "aiomysql", "mysql"),
    "clickhouse": ("clickhouse+http", None, None, "clickhouse"),
    "mssql": ("mssql+pyodbc", None, None, "sqlserver"),
    "sqlserver": ("mssql+pyodbc", None, None, "sqlserver"),
    "tsql": ("mssql+pyodbc", None, None, "sqlserver"),
    "snowflake": ("snowflake", None, None, "snowflake"),
    "bigquery": ("bigquery", None, None, "bigquery"),
    "sqlite": ("sqlite", None, None, None),
    "duckdb": ("duckdb", None, None, None),
    "redshift": (None, None, None, None),
    "trino": (None, None, None, None),
    "presto": (None, None, None, None),
    "athena": (None, None, None, None),
    "databricks": (None, None, None, None),
    "spark": (None, None, None, None),
    "oracle": (None, None, None, None),
}

_STRUCTURED: dict[str, Any] = dict(host="h.example", port=1234, database="db", username="u", password="p@ss")


def test_every_registered_type_is_covered() -> None:
    assert set(dialects._BY_DS_TYPE) == set(_EXPECTED)


@pytest.mark.parametrize("ds_type", sorted(_EXPECTED))
def test_dialect_declares_driver_facts(ds_type) -> None:
    d = dialect_for_ds_type(ds_type)
    assert (d.url_scheme, d.sync_driver, d.async_driver, d.install_extra) == _EXPECTED[ds_type]


def test_install_extras_exist_in_pyproject() -> None:
    extras = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())["tool"]["poetry"]["extras"]
    declared = {facts[3] for facts in _EXPECTED.values()} - {None}
    assert declared <= set(extras)


@pytest.mark.parametrize("ds_type", sorted(t for t, f in _EXPECTED.items() if f[2]))
def test_async_driver_is_async_capable(ds_type) -> None:
    d = dialect_for_ds_type(ds_type)
    assert d.url_scheme is not None
    backend = d.url_scheme.split("+", 1)[0]
    assert make_url(f"{backend}+{d.async_driver}://").get_dialect(_is_async=True).is_async


def test_no_parallel_async_driver_table() -> None:
    assert not hasattr(client, "_ASYNC_DRIVERS")


def test_registry_is_the_one_driver_table(monkeypatch) -> None:
    swapped = PostgresDialect().model_copy(
        update={"url_scheme": "postgresql+psycopg", "async_driver": "psycopg"},
    )
    monkeypatch.setitem(dialects._BY_DS_TYPE, "postgres", swapped)
    ds = DatasourceConfig(name="d", type="postgres", **_STRUCTURED)
    assert ds.get_connection_string().startswith("postgresql+psycopg://")
    assert client._async_connection_string(
        connection_string="postgresql://h/d", db_type="postgres",
    ) == "postgresql+psycopg://h/d"


# ---------------------------------------------------------------------------
# Structured-config URLs stay byte-identical (regression guards)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ds_type,expected",
    [
        ("postgres", "postgresql://u:p%40ss@h.example:1234/db"),
        ("postgresql", "postgresql://u:p%40ss@h.example:1234/db"),
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


@pytest.mark.parametrize("ds_type", sorted(t for t, f in _EXPECTED.items() if f[3]))
def test_datasource_docs_row_names_extra(ds_type) -> None:
    text = (_REPO_ROOT / "docs" / "configuration" / "datasources.md").read_text()
    extra = f"motley-slayer[{_EXPECTED[ds_type][3]}]"
    rows = [
        line for line in text.splitlines()
        if line.strip().startswith("|") and f"`{ds_type}`" in line
    ]
    assert rows, f"no datasource table row names `{ds_type}`"
    assert any(extra in row for row in rows), (
        f"no row naming `{ds_type}` mentions {extra}: {rows}"
    )
