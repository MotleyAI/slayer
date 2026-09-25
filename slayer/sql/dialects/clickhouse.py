"""ClickhouseDialect.

ClickHouse uses native ``median(x)`` directly and the parametric
``quantile(p)(x)`` form for percentile. CORR / COVAR_SAMP / COVAR_POP
are native. log10 and log2 are native.
"""

from __future__ import annotations

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.sql.dialects.base import SqlDialect


class ClickhouseDialect(SqlDialect):
    sqlglot_name: str = "clickhouse"
    ds_type_aliases: frozenset[str] = frozenset({"clickhouse"})
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = None  # unbounded
    approx_count_distinct_native: bool = True

    def build_median(self, inner: Expression) -> Expression:
        """ClickHouse: ``quantile(0.5)(x)``."""
        return self.build_percentile(p=exp.Literal.number("0.5"), col_expr=inner)

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        """ClickHouse: parametric ``quantile(p)(x)`` syntax."""
        return exp.Quantile(this=col_expr.copy(), quantile=p.copy())
