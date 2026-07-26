"""DEV-1542: SLayer SQL dialect registry.

Strategy-pattern dispatch: every dialect-specific SQL-generation quirk
lives on a subclass of ``SqlDialect`` (one file per Tier-1 dialect under
this package, Tier-2 dialects together in ``_tier2.py``). The registry
exposes two lookup functions:

* ``get_dialect(sqlglot_name)`` — strict, raises ``KeyError`` on unknown
  (preserves today's ``_build_explain_sql`` ``ValueError`` semantics).
* ``dialect_for_ds_type(ds_type)`` — lenient, falls back to
  ``PostgresDialect`` (preserves today's
  ``_DIALECT_MAP.get(ds_type or "", "postgres")`` semantics).
"""

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
from slayer.sql.dialects.mysql import MysqlDialect
from slayer.sql.dialects.postgres import PostgresDialect
from slayer.sql.dialects.snowflake import SnowflakeDialect
from slayer.sql.dialects.sqlite import SqliteDialect
from slayer.sql.dialects.tsql import TsqlDialect


__all__ = [
    "SqlDialect",
    "SqliteDialect",
    "PostgresDialect",
    "DuckdbDialect",
    "MysqlDialect",
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


_BY_DS_TYPE: dict[str, SqlDialect] = {
    alias: d for d in _ALL_DIALECTS for alias in d.ds_type_aliases
}


def get_dialect(sqlglot_name: str) -> SqlDialect:
    """Strict lookup by sqlglot name. Raises ``KeyError`` on unknown.

    Preserves today's ``_build_explain_sql`` semantics at
    ``query_engine.py:132`` — an unrecognised dialect string raises
    rather than silently falling back. Internal callers resolve via
    ``dialect_for_ds_type`` first, so this branch is unreachable in
    normal flow but the strict error is preserved as defence in depth.
    """
    return _BY_SQLGLOT_NAME[sqlglot_name]


def dialect_for_ds_type(ds_type: str | None) -> SqlDialect:
    """Lenient lookup by datasource-config ``type`` string.

    Falls back to ``PostgresDialect`` for ``None``, empty, OR unknown
    ds-types. Matches today's
    ``_DIALECT_MAP.get(ds_type or "", "postgres")`` semantics exactly.
    """
    return _BY_DS_TYPE.get(ds_type or "", _BY_SQLGLOT_NAME["postgres"])


# DEV-1686: quote reserved-word identifiers. Union the curated reserved-word set
# into every dialect generator's RESERVED_KEYWORDS as soon as the registry is
# built, so any ``.sql(dialect=...)`` emission quotes reserved aliases /
# qualifiers / physical names. Imported here (after ``_ALL_DIALECTS`` is defined)
# so the installer runs before any SQL is generated; idempotent.
from slayer.sql.reserved_keywords import install_reserved_keywords  # noqa: E402

install_reserved_keywords()
