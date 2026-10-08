"""Tier-2 dialect subclasses (no live integration tests).

Each Tier-2 dialect differs from its base only in scalar config (sqlglot
name, EXPLAIN prefix/postfix, log10/log2 native flags) — no SQL-shape
logic. They live together in one file because they're data-shaped, not
logic-shaped. ``PrestoDialect`` inherits the Presto-family grammar from
``slayer/sql/dialects/trino.py``.

BigQuery, Snowflake and Trino were promoted out of this file to their own
Tier 1 modules.
"""

from __future__ import annotations

from sqlglot import exp

from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.trino import PrestoFamilyDialect


class RedshiftDialect(SqlDialect):
    sqlglot_name: str = "redshift"
    ds_type_aliases: frozenset[str] = frozenset({"redshift"})
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = False
    max_identifier_bytes: int | None = 127
    approx_count_distinct_native: bool = True

    def build_null_safe_eq(
        self, left: exp.Expression, right: exp.Expression,
    ) -> exp.Expression:
        """Redshift (Postgres 8.0.2 fork) has no ``IS NOT DISTINCT
        FROM`` — emit the expanded ``a = b OR (a IS NULL AND b IS NULL)``."""
        return self._expanded_null_safe_eq(left, right)


class PrestoDialect(PrestoFamilyDialect):
    sqlglot_name: str = "presto"
    # Athena uses the Presto dialect via this alias.
    ds_type_aliases: frozenset[str] = frozenset({"presto", "athena"})


class DatabricksDialect(SqlDialect):
    sqlglot_name: str = "databricks"
    ds_type_aliases: frozenset[str] = frozenset({"databricks"})
    explain_prefix: str | None = "EXPLAIN EXTENDED"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = None  # unbounded
    approx_count_distinct_native: bool = True


class SparkDialect(SqlDialect):
    sqlglot_name: str = "spark"
    ds_type_aliases: frozenset[str] = frozenset({"spark"})
    explain_prefix: str | None = "EXPLAIN EXTENDED"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = None  # unbounded
    approx_count_distinct_native: bool = True


class OracleDialect(SqlDialect):
    sqlglot_name: str = "oracle"
    ds_type_aliases: frozenset[str] = frozenset({"oracle"})
    explain_prefix: str | None = "EXPLAIN PLAN FOR"
    explain_postfix: str = ""
    # Oracle has neither LOG10 nor LOG2 as single-arg functions — keep
    # the canonical 2-arg LOG(base, x) form.
    log10_native: bool = False
    log2_native: bool = False
    max_identifier_bytes: int | None = 128  # 12.2+; pre-12.2 (30) not modelled
    # Anonymous: sqlglot re-emits a parsed APPROX_COUNT_DISTINCT as its
    # Presto-family APPROX_DISTINCT canonical, which is not an Oracle function.
    approx_count_distinct_anonymous_name: str | None = "APPROX_COUNT_DISTINCT"

    def build_null_safe_eq(
        self, left: exp.Expression, right: exp.Expression,
    ) -> exp.Expression:
        """Oracle has no ``IS NOT DISTINCT FROM`` — emit the expanded
        ``a = b OR (a IS NULL AND b IS NULL)``."""
        return self._expanded_null_safe_eq(left, right)
