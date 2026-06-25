"""DEV-1542: ClickhouseDialect.

ClickHouse uses native ``median(x)`` directly and the parametric
``quantile(p)(x)`` form for percentile. CORR / COVAR_SAMP / COVAR_POP
are native. log10 and log2 are native.
"""

from __future__ import annotations

from collections.abc import Callable

from sqlglot import exp

from slayer.sql.dialects.base import SqlDialect


class ClickhouseDialect(SqlDialect):
    sqlglot_name: str = "clickhouse"
    ds_type_aliases: frozenset[str] = frozenset({"clickhouse"})
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def build_median(
        self,
        inner: exp.Expression,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """ClickHouse: native ``median(x)`` aggregate."""
        inner_sql = inner.sql(dialect="clickhouse")
        return parse(f"median({inner_sql})")

    def build_percentile(
        self,
        p_str: str,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """ClickHouse: parametric ``quantile(p)(x)`` syntax."""
        return parse(f"quantile({p_str})({col_sql})")
