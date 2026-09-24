"""MysqlDialect.

MySQL has no native ``PERCENTILE_CONT`` (``build_median`` / ``build_percentile``
raise ``NotImplementedError``) and no native ``CORR`` / ``COVAR_SAMP`` /
``COVAR_POP`` (uses the variance-decomposition formula).

``var_samp`` / ``var_pop`` need the ``exp.Anonymous`` workaround because
sqlglot's MySQL transpiler rewrites them to ``VARIANCE`` (which on MySQL
is actually ``VAR_POP`` — silently wrong sample variance).
"""

from __future__ import annotations

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.sql.dialects.base import (
    SqlDialect,
    StatAgg1Name,
    StatAgg2Name,
    _build_covar_decomposition,
)


class MysqlDialect(SqlDialect):
    sqlglot_name: str = "mysql"
    ds_type_aliases: frozenset[str] = frozenset({"mysql", "mariadb"})
    explain_prefix: str | None = "EXPLAIN FORMAT=JSON"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    # Conservative: MySQL allows 256 for column aliases but errors (not truncates).
    max_identifier_bytes: int | None = 64

    def rewrite_target_ast(self, tree: Expression) -> Expression:
        """MySQL's ``TRUNCATE`` has no single-argument form — ``TRUNCATE(x)`` is
        a syntax error. A 1-arg ``trunc(x)`` becomes ``TRUNCATE(x, 0)``."""
        def _fix(node: Expression) -> Expression:
            if isinstance(node, exp.Trunc) and node.args.get("decimals") is None:
                node.set("decimals", exp.Literal.number(0))
            return node
        return tree.transform(_fix)

    def build_median(self, inner: Expression) -> Expression:
        # ``mariadb`` resolves to this same dialect via ``ds_type_aliases``,
        # so the error must NOT suggest "use MariaDB" — that would loop the
        # user back here. Point them at a datasource with native percentile
        # support or client-side computation instead.
        raise NotImplementedError(
            "Aggregation 'median' is not supported on MySQL: MySQL has no native "
            "MEDIAN/PERCENTILE_CONT function and no Python UDF mechanism. "
            "Use a datasource with native percentile support (Postgres, DuckDB, "
            "ClickHouse, SQLite via UDF) or compute the value client-side."
        )

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        raise NotImplementedError(
            "Aggregation 'percentile' is not supported on MySQL: "
            "MySQL has no native PERCENTILE_CONT. "
            "Use a datasource with native percentile support (Postgres, DuckDB, "
            "ClickHouse, SQLite via UDF) or compute the value client-side."
        )

    def build_stat_agg_1arg(
        self, agg_name: StatAgg1Name, col_expr: Expression,
    ) -> Expression:
        """MySQL override for ``var_samp`` / ``var_pop``.

        sqlglot's MySQL transpiler rewrites ``VAR_SAMP`` → ``VARIANCE``
        (which on MySQL is ``VAR_POP``) — silently wrong. Emit the
        canonical MySQL names via ``exp.Anonymous`` to bypass sqlglot.
        """
        if agg_name in {"var_samp", "var_pop"}:
            return exp.Anonymous(
                this=agg_name.upper(),
                expressions=[col_expr.copy()],
            )
        return super().build_stat_agg_1arg(agg_name=agg_name, col_expr=col_expr)

    def build_covar_2arg(
        self,
        agg_name: StatAgg2Name,
        col_expr: Expression,
        other_expr: Expression,
    ) -> Expression:
        """MySQL has no native CORR / COVAR_* — use the
        variance-decomposition formula with MySQL-native VAR_SAMP /
        VAR_POP / STDDEV_SAMP names."""
        return _build_covar_decomposition(
            col_expr=col_expr,
            other_expr=other_expr,
            agg=agg_name,
            var_fn_samp="VAR_SAMP",
            var_fn_pop="VAR_POP",
            stddev_fn="STDDEV_SAMP",
        )
