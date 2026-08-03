"""DEV-1542: DuckdbDialect.

DuckDB shape matches Postgres: native DATE_TRUNC, native PERCENTILE_CONT
(emitted via sqlglot's QUANTILE_CONT translation), native CORR / COVAR /
log10 / log2.
"""

from __future__ import annotations

from collections.abc import Callable

from sqlglot import exp

from slayer.sql.dialects.base import SqlDialect


class DuckdbDialect(SqlDialect):
    sqlglot_name: str = "duckdb"
    ds_type_aliases: frozenset[str] = frozenset({"duckdb"})
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """DuckDB: native ``approx_count_distinct(x)`` aggregate."""
        return parse(f"approx_count_distinct({col_sql})")
