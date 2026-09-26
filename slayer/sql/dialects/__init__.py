"""SqlDialect registry: strict lookup by sqlglot name, lenient lookup by datasource type."""

from __future__ import annotations

from slayer.sql.dialects._tier2 import (
    DatabricksDialect,
    OracleDialect,
    PrestoDialect,
    RedshiftDialect,
    SparkDialect,
    TrinoDialect,
)
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.bigquery import BigqueryDialect
from slayer.sql.dialects.clickhouse import ClickhouseDialect
from slayer.sql.dialects.duckdb import DuckdbDialect
from slayer.sql.dialects.mysql import MariadbDialect, MysqlDialect
from slayer.sql.dialects.postgres import PostgresDialect
from slayer.sql.dialects.snowflake import SnowflakeDialect
from slayer.sql.dialects.sqlite import SqliteDialect
from slayer.sql.dialects.tsql import TsqlDialect
from slayer.sql.reserved_keywords import install_reserved_keywords


__all__ = [
    "SqlDialect",
    "SqliteDialect",
    "PostgresDialect",
    "DuckdbDialect",
    "MysqlDialect",
    "MariadbDialect",
    "ClickhouseDialect",
    "TsqlDialect",
    "SnowflakeDialect",
    "BigqueryDialect",
    "RedshiftDialect",
    "TrinoDialect",
    "PrestoDialect",
    "DatabricksDialect",
    "SparkDialect",
    "OracleDialect",
    "SQLGLOT_NAMES",
    "get_dialect",
    "dialect_for_ds_type",
]


_ALL_DIALECTS: tuple[SqlDialect, ...] = (
    SqliteDialect(),
    PostgresDialect(),
    DuckdbDialect(),
    MysqlDialect(),
    ClickhouseDialect(),
    TsqlDialect(),
    SnowflakeDialect(),
    BigqueryDialect(),
    RedshiftDialect(),
    TrinoDialect(),
    PrestoDialect(),
    DatabricksDialect(),
    SparkDialect(),
    OracleDialect(),
)


_BY_SQLGLOT_NAME: dict[str, SqlDialect] = {
    d.sqlglot_name: d for d in _ALL_DIALECTS
}

SQLGLOT_NAMES: tuple[str, ...] = tuple(_BY_SQLGLOT_NAME)


# Reached by ds-type only: they share a sqlglot name with a dialect above.
_DS_TYPE_ONLY_DIALECTS: tuple[SqlDialect, ...] = (MariadbDialect(),)

_BY_DS_TYPE: dict[str, SqlDialect] = {
    alias: d for d in (*_ALL_DIALECTS, *_DS_TYPE_ONLY_DIALECTS) for alias in d.ds_type_aliases
}


def get_dialect(sqlglot_name: str) -> SqlDialect:
    """Strict lookup by sqlglot name; raises ``KeyError`` on unknown."""
    return _BY_SQLGLOT_NAME[sqlglot_name]


def dialect_for_ds_type(ds_type: str | None) -> SqlDialect:
    """Lenient lookup by datasource type; ``None``, empty and unknown types get Postgres."""
    return _BY_DS_TYPE.get(ds_type or "", _BY_SQLGLOT_NAME["postgres"])


# Quote reserved-word identifiers in every dialect's emission; runs at import, idempotent.
install_reserved_keywords()
