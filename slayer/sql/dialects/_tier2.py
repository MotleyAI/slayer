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

from typing import Callable, Optional

from sqlglot import exp

from slayer.sql.dialects.base import SqlDialect


class RedshiftDialect(SqlDialect):
    sqlglot_name: str = "redshift"
    ds_type_aliases: frozenset[str] = frozenset({"redshift"})
    explain_prefix: Optional[str] = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = False

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Redshift: ``APPROXIMATE COUNT(DISTINCT x)`` (keyword prefix)."""
        return parse(f"APPROXIMATE COUNT(DISTINCT {col_sql})")


class TrinoDialect(SqlDialect):
    sqlglot_name: str = "trino"
    ds_type_aliases: frozenset[str] = frozenset({"trino"})
    explain_prefix: Optional[str] = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Trino: native ``approx_distinct(x)`` aggregate."""
        return parse(f"approx_distinct({col_sql})")


class PrestoDialect(SqlDialect):
    sqlglot_name: str = "presto"
    # Athena uses the Presto dialect via this alias.
    ds_type_aliases: frozenset[str] = frozenset({"presto", "athena"})
    explain_prefix: Optional[str] = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Presto: native ``approx_distinct(x)`` aggregate."""
        return parse(f"approx_distinct({col_sql})")


class DatabricksDialect(SqlDialect):
    sqlglot_name: str = "databricks"
    ds_type_aliases: frozenset[str] = frozenset({"databricks"})
    explain_prefix: Optional[str] = "EXPLAIN EXTENDED"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Databricks: native ``approx_count_distinct(x)`` aggregate."""
        return parse(f"approx_count_distinct({col_sql})")


class SparkDialect(SqlDialect):
    sqlglot_name: str = "spark"
    ds_type_aliases: frozenset[str] = frozenset({"spark"})
    explain_prefix: Optional[str] = "EXPLAIN EXTENDED"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Spark: native ``approx_count_distinct(x)`` aggregate."""
        return parse(f"approx_count_distinct({col_sql})")


class OracleDialect(SqlDialect):
    sqlglot_name: str = "oracle"
    ds_type_aliases: frozenset[str] = frozenset({"oracle"})
    explain_prefix: Optional[str] = "EXPLAIN PLAN FOR"
    explain_postfix: str = ""
    # Oracle has neither LOG10 nor LOG2 as single-arg functions — keep
    # the canonical 2-arg LOG(base, x) form.
    log10_native: bool = False
    log2_native: bool = False

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Oracle: native ``APPROX_COUNT_DISTINCT(x)`` aggregate.

        Built as an ``exp.Anonymous`` because sqlglot's Oracle dialect
        re-emits a parsed ``APPROX_COUNT_DISTINCT`` as ``APPROX_DISTINCT``
        (its Presto-family canonical form), which is not an Oracle function.
        """
        return exp.Anonymous(this="APPROX_COUNT_DISTINCT", expressions=[parse(col_sql)])
