"""DEV-1542: Tier-2 dialect subclasses (no live integration tests).

Each Tier-2 dialect differs from ``SqlDialect``'s Postgres-shaped defaults
only in scalar config (sqlglot name, EXPLAIN prefix/postfix, log10/log2
native flags) — no SQL-shape logic. They live together in one file
because they're data-shaped, not logic-shaped.

Values codify today's behaviour from
``query_engine.py:_EXPLAIN_PREFIX`` / ``_EXPLAIN_POSTFIX`` and
``generator.py:_LOG10_NATIVE_DIALECTS`` / ``_LOG2_NATIVE_DIALECTS``.

Two dialects were promoted out of this file to their own Tier 1 modules:

* ``BigqueryDialect`` — see ``slayer/sql/dialects/bigquery.py`` (alias
  mangling for joined-column references and per-statement quota tweaks).
* ``SnowflakeDialect`` (DEV-1551) — see ``slayer/sql/dialects/snowflake.py``
  (connection URL builder, ``creator=`` engine bridge, per-connection
  session overrides, statement timeout, cursor type-code map).
"""

from __future__ import annotations


from slayer.sql.dialects.base import SqlDialect


class RedshiftDialect(SqlDialect):
    sqlglot_name: str = "redshift"
    ds_type_aliases: frozenset[str] = frozenset({"redshift"})
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = False


class TrinoDialect(SqlDialect):
    sqlglot_name: str = "trino"
    ds_type_aliases: frozenset[str] = frozenset({"trino"})
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True


class PrestoDialect(SqlDialect):
    sqlglot_name: str = "presto"
    # Athena uses the Presto dialect via this alias.
    ds_type_aliases: frozenset[str] = frozenset({"presto", "athena"})
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True


class DatabricksDialect(SqlDialect):
    sqlglot_name: str = "databricks"
    ds_type_aliases: frozenset[str] = frozenset({"databricks"})
    explain_prefix: str | None = "EXPLAIN EXTENDED"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True


class SparkDialect(SqlDialect):
    sqlglot_name: str = "spark"
    ds_type_aliases: frozenset[str] = frozenset({"spark"})
    explain_prefix: str | None = "EXPLAIN EXTENDED"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True


class OracleDialect(SqlDialect):
    sqlglot_name: str = "oracle"
    ds_type_aliases: frozenset[str] = frozenset({"oracle"})
    explain_prefix: str | None = "EXPLAIN PLAN FOR"
    explain_postfix: str = ""
    # Oracle has neither LOG10 nor LOG2 as single-arg functions — keep
    # the canonical 2-arg LOG(base, x) form.
    log10_native: bool = False
    log2_native: bool = False
