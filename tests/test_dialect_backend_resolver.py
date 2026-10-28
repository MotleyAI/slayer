"""The SQLAlchemy backend name → ``SqlDialect`` resolver used by structure reflection."""

from __future__ import annotations

import pytest

from slayer.sql.dialects import (
    _ALL_DIALECTS,
    BigqueryDialect,
    ClickhouseDialect,
    DatabricksDialect,
    DuckdbDialect,
    MariadbDialect,
    MysqlDialect,
    OracleDialect,
    PostgresDialect,
    RedshiftDialect,
    SnowflakeDialect,
    SqlDialect,
    SqliteDialect,
    TrinoDialect,
    TsqlDialect,
    build_sa_backend_registry,
    dialect_for_sa_backend,
)
from tests._engine_helpers import disposable_engine

_EXPECTED: list[tuple[str, type[SqlDialect]]] = [
    ("postgresql", PostgresDialect),
    ("mssql", TsqlDialect),
    ("duckdb", DuckdbDialect),
    ("sqlite", SqliteDialect),
    ("mysql", MysqlDialect),
    ("mariadb", MysqlDialect),
    ("snowflake", SnowflakeDialect),
    ("bigquery", BigqueryDialect),
    ("clickhouse", ClickhouseDialect),
    ("trino", TrinoDialect),
    ("redshift", RedshiftDialect),
    ("oracle", OracleDialect),
    ("databricks", DatabricksDialect),
]


@pytest.mark.parametrize(("backend", "expected"), _EXPECTED)
def test_backend_resolves_to_its_dialect(backend: str, expected: type[SqlDialect]) -> None:
    assert isinstance(dialect_for_sa_backend(backend), expected)


@pytest.mark.parametrize(
    "dialect", [*_ALL_DIALECTS, MariadbDialect()], ids=lambda d: type(d).__name__,
)
def test_every_declared_backend_resolves_to_its_declarer(dialect: SqlDialect) -> None:
    for backend in dialect.sa_backend_names:
        assert type(dialect_for_sa_backend(backend)) is type(dialect), backend


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg2://u@h/d",
        "sqlite://",
        "duckdb:///:memory:",
        "mysql+pymysql://u@h/d",
        "mariadb+pymysql://u@h/d",
        "snowflake://u@a/d",
        "clickhouse+http://u@h/d",
        "trino://u@h/c",
    ],
)
def test_live_engine_backend_name_is_declared(url: str) -> None:
    try:
        with disposable_engine(url) as engine:
            backend = engine.dialect.name
    except Exception as exc:  # noqa: BLE001 — driver not installed
        pytest.skip(f"driver unavailable: {exc}")
    assert type(dialect_for_sa_backend(backend)) is not SqlDialect


def test_undeclared_backend_gets_the_base() -> None:
    assert type(dialect_for_sa_backend("firebird")) is SqlDialect


def test_duplicate_declaration_fails_registry_build() -> None:
    clash = SqliteDialect(sa_backend_names=frozenset({"duckdb"}))
    dialects = [DuckdbDialect(), clash]
    with pytest.raises(ValueError, match="duckdb"):
        build_sa_backend_registry(dialects)


def test_registry_maps_each_declared_backend() -> None:
    registry = build_sa_backend_registry([DuckdbDialect(), SqliteDialect()])
    assert type(registry["duckdb"]) is DuckdbDialect
    assert type(registry["sqlite"]) is SqliteDialect


def test_only_sqlite_probes_integer_affinity() -> None:
    probing = {type(d) for d in (*_ALL_DIALECTS, MariadbDialect()) if d.probes_integer_affinity}
    assert probing == {SqliteDialect}
