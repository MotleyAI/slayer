"""DEV-1542: Tier-2 dialect subclasses (no live integration tests).

Each Tier-2 dialect differs from ``SqlDialect``'s Postgres-shaped defaults
only in scalar config (sqlglot name, EXPLAIN prefix/postfix, log10/log2
native flags) — no SQL-shape logic. They live together in one file
because they're data-shaped, not logic-shaped.

Values codify today's behaviour from
``query_engine.py:_EXPLAIN_PREFIX`` / ``_EXPLAIN_POSTFIX`` and
``generator.py:_LOG10_NATIVE_DIALECTS`` / ``_LOG2_NATIVE_DIALECTS``.

DEV-1551 promoted ``SnowflakeDialect`` out of this file and into
``slayer/sql/dialects/snowflake.py`` — it gained runtime methods
(connection URL builder, ``creator=`` engine bridge, per-connection
session overrides, statement timeout, cursor type-code map) that are
Snowflake-specific and don't fit the data-shaped Tier-2 layout.
"""

from __future__ import annotations

from typing import Optional

from slayer.sql.dialects.base import SqlDialect


class BigqueryDialect(SqlDialect):
    sqlglot_name: str = "bigquery"
    ds_type_aliases: frozenset[str] = frozenset({"bigquery"})
    # BigQuery has no SQL-level EXPLAIN.
    explain_prefix: Optional[str] = None
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True


class RedshiftDialect(SqlDialect):
    sqlglot_name: str = "redshift"
    ds_type_aliases: frozenset[str] = frozenset({"redshift"})
    explain_prefix: Optional[str] = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = False


class TrinoDialect(SqlDialect):
    sqlglot_name: str = "trino"
    ds_type_aliases: frozenset[str] = frozenset({"trino"})
    explain_prefix: Optional[str] = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True


class PrestoDialect(SqlDialect):
    sqlglot_name: str = "presto"
    # Athena uses the Presto dialect via this alias.
    ds_type_aliases: frozenset[str] = frozenset({"presto", "athena"})
    explain_prefix: Optional[str] = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True


class DatabricksDialect(SqlDialect):
    sqlglot_name: str = "databricks"
    ds_type_aliases: frozenset[str] = frozenset({"databricks"})
    explain_prefix: Optional[str] = "EXPLAIN EXTENDED"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True


class SparkDialect(SqlDialect):
    sqlglot_name: str = "spark"
    ds_type_aliases: frozenset[str] = frozenset({"spark"})
    explain_prefix: Optional[str] = "EXPLAIN EXTENDED"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True


class OracleDialect(SqlDialect):
    sqlglot_name: str = "oracle"
    ds_type_aliases: frozenset[str] = frozenset({"oracle"})
    explain_prefix: Optional[str] = "EXPLAIN PLAN FOR"
    explain_postfix: str = ""
    # Oracle has neither LOG10 nor LOG2 as single-arg functions — keep
    # the canonical 2-arg LOG(base, x) form.
    log10_native: bool = False
    log2_native: bool = False
