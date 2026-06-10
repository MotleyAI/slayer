"""DEV-1542: registry tests for the SqlDialect strategy classes.

Verifies:
* ``get_dialect(sqlglot_name)`` is strict — raises ``KeyError`` for unknown
  names. This preserves today's ``_build_explain_sql("unknown", ...)``
  semantics, which raise ``ValueError`` via the ``_EXPLAIN_PREFIX.get(...)``
  miss at ``query_engine.py:132``.
* ``dialect_for_ds_type(ds_type)`` is lenient — unknown / ``None`` /
  empty string falls back to ``PostgresDialect``. Mirrors today's
  ``_DIALECT_MAP.get(ds_type or "", "postgres")`` semantics.
* Every ds-type alias from today's ``_DIALECT_MAP`` resolves to a dialect
  whose ``sqlglot_name`` matches the historical map value.
* ``ds_type_aliases`` sets are disjoint across dialects so registry
  construction is unambiguous.
"""

from __future__ import annotations

import pytest

from slayer.sql.dialects import (
    dialect_for_ds_type,
    get_dialect,
)
from slayer.sql.dialects._tier2 import (
    BigqueryDialect,
    DatabricksDialect,
    OracleDialect,
    PrestoDialect,
    RedshiftDialect,
    SparkDialect,
    TrinoDialect,
)
from slayer.sql.dialects.snowflake import SnowflakeDialect
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.clickhouse import ClickhouseDialect
from slayer.sql.dialects.duckdb import DuckdbDialect
from slayer.sql.dialects.mysql import MysqlDialect
from slayer.sql.dialects.postgres import PostgresDialect
from slayer.sql.dialects.sqlite import SqliteDialect
from slayer.sql.dialects.tsql import TsqlDialect


# Historical map from `slayer/engine/query_engine.py:_dialect_for_type._DIALECT_MAP`.
# This is the authoritative ground truth — every entry must keep resolving
# to the same sqlglot name after the refactor.
_LEGACY_DIALECT_MAP = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mariadb": "mysql",
    "clickhouse": "clickhouse",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "sqlite": "sqlite",
    "duckdb": "duckdb",
    "redshift": "redshift",
    "trino": "trino",
    "presto": "presto",
    "athena": "presto",
    "databricks": "databricks",
    "spark": "spark",
    "mssql": "tsql",
    "sqlserver": "tsql",
    "tsql": "tsql",
    "oracle": "oracle",
}


# ---------------------------------------------------------------------------
# get_dialect: strict lookup by sqlglot name
# ---------------------------------------------------------------------------


def test_get_dialect_returns_postgres_for_postgres() -> None:
    assert isinstance(get_dialect("postgres"), PostgresDialect)


def test_get_dialect_returns_sqlite_for_sqlite() -> None:
    assert isinstance(get_dialect("sqlite"), SqliteDialect)


def test_get_dialect_returns_duckdb_for_duckdb() -> None:
    assert isinstance(get_dialect("duckdb"), DuckdbDialect)


def test_get_dialect_returns_mysql_for_mysql() -> None:
    assert isinstance(get_dialect("mysql"), MysqlDialect)


def test_get_dialect_returns_clickhouse_for_clickhouse() -> None:
    assert isinstance(get_dialect("clickhouse"), ClickhouseDialect)


def test_get_dialect_returns_tsql_for_tsql() -> None:
    assert isinstance(get_dialect("tsql"), TsqlDialect)


def test_get_dialect_returns_snowflake_for_snowflake() -> None:
    d = get_dialect("snowflake")
    assert isinstance(d, SnowflakeDialect)
    assert d.sqlglot_name == "snowflake"


def test_get_dialect_returns_bigquery_for_bigquery() -> None:
    assert isinstance(get_dialect("bigquery"), BigqueryDialect)


def test_get_dialect_returns_redshift_for_redshift() -> None:
    assert isinstance(get_dialect("redshift"), RedshiftDialect)


def test_get_dialect_returns_trino_for_trino() -> None:
    assert isinstance(get_dialect("trino"), TrinoDialect)


def test_get_dialect_returns_presto_for_presto() -> None:
    assert isinstance(get_dialect("presto"), PrestoDialect)


def test_get_dialect_returns_databricks_for_databricks() -> None:
    assert isinstance(get_dialect("databricks"), DatabricksDialect)


def test_get_dialect_returns_spark_for_spark() -> None:
    assert isinstance(get_dialect("spark"), SparkDialect)


def test_get_dialect_returns_oracle_for_oracle() -> None:
    assert isinstance(get_dialect("oracle"), OracleDialect)


def test_get_dialect_strict_raises_on_unknown_sqlglot_name() -> None:
    """Strict lookup. Preserves today's ValueError from ``_build_explain_sql``
    at ``query_engine.py:132`` when an unrecognised dialect name is passed."""
    with pytest.raises(KeyError):
        get_dialect("not_a_dialect")


def test_get_dialect_strict_raises_on_ds_type_alias() -> None:
    """``get_dialect`` is sqlglot-name keyed, NOT ds-type keyed.

    Even though ``postgresql`` resolves to the Postgres dialect via
    ``dialect_for_ds_type``, calling ``get_dialect("postgresql")`` must
    raise — that's a ds-type alias, not a sqlglot name.
    """
    with pytest.raises(KeyError):
        get_dialect("postgresql")


# ---------------------------------------------------------------------------
# dialect_for_ds_type: lenient lookup by datasource-config type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ds_type,expected_sqlglot", sorted(_LEGACY_DIALECT_MAP.items()))
def test_dialect_for_ds_type_matches_legacy_map(
    ds_type: str, expected_sqlglot: str
) -> None:
    """Every key in today's ``_DIALECT_MAP`` resolves to a dialect whose
    ``sqlglot_name`` equals today's map value."""
    d = dialect_for_ds_type(ds_type)
    assert d.sqlglot_name == expected_sqlglot


def test_dialect_for_ds_type_none_falls_back_to_postgres() -> None:
    assert isinstance(dialect_for_ds_type(None), PostgresDialect)


def test_dialect_for_ds_type_empty_falls_back_to_postgres() -> None:
    assert isinstance(dialect_for_ds_type(""), PostgresDialect)


def test_dialect_for_ds_type_unknown_falls_back_to_postgres() -> None:
    """Unknown ds-type strings fall back to Postgres — preserves today's
    ``_DIALECT_MAP.get(ds_type or "", "postgres")`` lenient behaviour."""
    assert isinstance(dialect_for_ds_type("totally_unknown"), PostgresDialect)


def test_dialect_for_ds_type_mariadb_resolves_to_mysql() -> None:
    assert isinstance(dialect_for_ds_type("mariadb"), MysqlDialect)


def test_dialect_for_ds_type_athena_resolves_to_presto() -> None:
    """Athena uses Presto dialect — preserves a non-obvious legacy mapping."""
    assert isinstance(dialect_for_ds_type("athena"), PrestoDialect)


def test_dialect_for_ds_type_sqlserver_resolves_to_tsql() -> None:
    assert isinstance(dialect_for_ds_type("sqlserver"), TsqlDialect)


# ---------------------------------------------------------------------------
# ds_type_aliases sets are disjoint
# ---------------------------------------------------------------------------


def _all_dialect_classes() -> list[type[SqlDialect]]:
    return [
        SqliteDialect, PostgresDialect, DuckdbDialect,
        MysqlDialect, ClickhouseDialect, TsqlDialect,
        SnowflakeDialect, BigqueryDialect, RedshiftDialect,
        TrinoDialect, PrestoDialect, DatabricksDialect,
        SparkDialect, OracleDialect,
    ]


def test_ds_type_aliases_disjoint_across_all_dialects() -> None:
    seen: dict[str, type[SqlDialect]] = {}
    for cls in _all_dialect_classes():
        for alias in cls().ds_type_aliases:
            assert alias not in seen, (
                f"alias {alias!r} appears on both {seen[alias].__name__} and "
                f"{cls.__name__}; registry construction would be ambiguous."
            )
            seen[alias] = cls


def test_sqlglot_names_disjoint_across_all_dialects() -> None:
    """Each dialect class must produce a unique sqlglot name."""
    seen: dict[str, type[SqlDialect]] = {}
    for cls in _all_dialect_classes():
        name = cls().sqlglot_name
        assert name not in seen, (
            f"sqlglot name {name!r} appears on both {seen[name].__name__} and "
            f"{cls.__name__}."
        )
        seen[name] = cls


# ---------------------------------------------------------------------------
# Tier-1 dialect ds_type_aliases match the legacy map exactly
# ---------------------------------------------------------------------------


def test_postgres_dialect_aliases() -> None:
    assert PostgresDialect().ds_type_aliases == frozenset({"postgres", "postgresql"})


def test_mysql_dialect_aliases() -> None:
    assert MysqlDialect().ds_type_aliases == frozenset({"mysql", "mariadb"})


def test_tsql_dialect_aliases() -> None:
    assert TsqlDialect().ds_type_aliases == frozenset({"mssql", "sqlserver", "tsql"})


def test_presto_dialect_aliases_includes_athena() -> None:
    assert PrestoDialect().ds_type_aliases == frozenset({"presto", "athena"})


def test_sqlite_dialect_aliases() -> None:
    assert SqliteDialect().ds_type_aliases == frozenset({"sqlite"})


def test_duckdb_dialect_aliases() -> None:
    assert DuckdbDialect().ds_type_aliases == frozenset({"duckdb"})


def test_clickhouse_dialect_aliases() -> None:
    assert ClickhouseDialect().ds_type_aliases == frozenset({"clickhouse"})
