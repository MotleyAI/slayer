"""DEV-1542: MysqlDialect.

MySQL has no native ``PERCENTILE_CONT`` (``build_median`` / ``build_percentile``
raise ``NotImplementedError``) and no native ``CORR`` / ``COVAR_SAMP`` /
``COVAR_POP`` (uses the variance-decomposition formula).

``var_samp`` / ``var_pop`` need the ``exp.Anonymous`` workaround because
sqlglot's MySQL transpiler rewrites them to ``VARIANCE`` (which on MySQL
is actually ``VAR_POP`` — silently wrong sample variance).
"""

from __future__ import annotations

from typing import Callable, Optional

from sqlglot import exp

from slayer.sql.dialects.base import SqlDialect, _build_covar_decomposition


class MysqlDialect(SqlDialect):
    sqlglot_name: str = "mysql"
    ds_type_aliases: frozenset[str] = frozenset({"mysql", "mariadb"})
    explain_prefix: Optional[str] = "EXPLAIN FORMAT=JSON"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def build_median(
        self,
        inner: exp.Expression,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        raise NotImplementedError(
            "Aggregation 'median' is not supported on MySQL: MySQL has no native "
            "MEDIAN/PERCENTILE_CONT function and no Python UDF mechanism. "
            "Use MariaDB (has MEDIAN()) or compute the value client-side."
        )

    def build_percentile(
        self,
        p_str: str,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        raise NotImplementedError(
            "Aggregation 'percentile' is not supported on MySQL: "
            "MySQL has no native PERCENTILE_CONT. "
            "Use MariaDB or compute the value client-side."
        )

    def build_stat_agg_1arg(
        self,
        agg_name: str,
        col_expr: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """MySQL override for ``var_samp`` / ``var_pop``.

        sqlglot's MySQL transpiler rewrites ``VAR_SAMP`` → ``VARIANCE``
        (which on MySQL is ``VAR_POP``) — silently wrong. Emit the
        canonical MySQL names via ``exp.Anonymous`` to bypass sqlglot.
        """
        if agg_name in {"var_samp", "var_pop"}:
            return exp.Anonymous(
                this=agg_name.upper(),
                expressions=[parse(col_expr)],
            )
        return super().build_stat_agg_1arg(agg_name, col_expr, parse=parse)

    def build_covar_2arg(
        self,
        agg_name: str,
        col_sql: str,
        other_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """MySQL has no native CORR / COVAR_* — use the
        variance-decomposition formula with MySQL-native VAR_SAMP /
        VAR_POP / STDDEV_SAMP names."""
        return _build_covar_decomposition(
            col_sql=col_sql,
            other_sql=other_sql,
            agg=agg_name,
            var_fn_samp="VAR_SAMP",
            var_fn_pop="VAR_POP",
            stddev_fn="STDDEV_SAMP",
            parse=parse,
        )
