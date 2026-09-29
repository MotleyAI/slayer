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

from slayer.core.enums import DataType, DatePart, TimeGranularity
from slayer.sql.dialects.base import (
    SqlDialect,
    StatAgg1Name,
    StatAgg2Name,
    _build_covar_decomposition,
)


_SUB_DAY_TRUNC_FORMATS = {
    TimeGranularity.HOUR: "%Y-%m-%d %H:00:00",
    TimeGranularity.MINUTE: "%Y-%m-%d %H:%i:00",
    TimeGranularity.SECOND: "%Y-%m-%d %H:%i:%s",
}
_MYSQL_PART_FUNCTIONS = {
    DatePart.YEAR: "YEAR", DatePart.QUARTER: "QUARTER", DatePart.MONTH: "MONTH",
    DatePart.DAY: "DAYOFMONTH", DatePart.DAY_OF_YEAR: "DAYOFYEAR",
    DatePart.HOUR: "HOUR", DatePart.MINUTE: "MINUTE", DatePart.SECOND: "SECOND",
}


class MysqlDialect(SqlDialect):
    sqlglot_name: str = "mysql"
    ds_type_aliases: frozenset[str] = frozenset({"mysql"})
    explain_prefix: str | None = "EXPLAIN FORMAT=JSON"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    # Conservative: MySQL allows 256 for column aliases but errors (not truncates).
    max_identifier_bytes: int | None = 64

    def statement_timeout_sql(self, timeout_seconds: int) -> str | None:
        return f"SET max_execution_time = {timeout_seconds * 1000}"

    def rewrite_target_ast(self, tree: Expression) -> Expression:
        """MySQL's ``TRUNCATE`` has no single-argument form — ``TRUNCATE(x)`` is
        a syntax error. A 1-arg ``trunc(x)`` becomes ``TRUNCATE(x, 0)``."""
        def _fix(node: Expression) -> Expression:
            if isinstance(node, exp.Trunc) and node.args.get("decimals") is None:
                node.set("decimals", exp.Literal.number(0))
            return node
        return tree.transform(_fix)

    def build_date_trunc(self, col_expr: Expression, granularity: TimeGranularity) -> Expression:
        """Sub-day units via ``DATE_FORMAT``: sqlglot renders them all as ``DATE(t)``."""
        fmt = _SUB_DAY_TRUNC_FORMATS.get(granularity)
        if fmt is None:
            return super().build_date_trunc(col_expr=col_expr, granularity=granularity)
        formatted = exp.Anonymous(this="DATE_FORMAT", expressions=[col_expr.copy(), exp.Literal.string(fmt)])
        return exp.Cast(this=formatted, to=exp.DataType.build("DATETIME"))

    def _date_part(self, part: DatePart, expr: Expression) -> Expression:
        """Named functions: ``WEEK(x, 3)`` is ISO, ``WEEKDAY`` counts from Monday=0."""
        if part is DatePart.WEEK:
            return exp.Anonymous(this="WEEK", expressions=[expr, exp.Literal.number(3)])
        if part is DatePart.ISO_YEAR:
            year_week = exp.Anonymous(this="YEARWEEK", expressions=[expr, exp.Literal.number(3)])
            return exp.Cast(
                this=exp.Floor(this=exp.Div(this=year_week, expression=exp.Literal.number(100))),
                to=exp.DataType.build("INT"),
            )
        if part is DatePart.DAY_OF_WEEK:
            return exp.Add(this=exp.Anonymous(this="WEEKDAY", expressions=[expr]), expression=exp.Literal.number(1))
        return exp.Anonymous(this=_MYSQL_PART_FUNCTIONS[part], expressions=[expr])

    def _day_gap(self, *, start: Expression, end: Expression) -> Expression:
        return exp.Anonymous(this="DATEDIFF", expressions=[end, start])

    def _second_gap(self, *, start: Expression, end: Expression) -> Expression:
        return exp.Anonymous(this="TIMESTAMPDIFF", expressions=[exp.var("SECOND"), start, end])

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """``DATE_ADD`` clamps at month-end and keeps a DATE a DATE for day-or-coarser units."""
        word = "WEEK" if unit is TimeGranularity.WEEK_SUNDAY else unit.value.upper()
        return exp.DateAdd(this=expr.copy(), expression=count.copy(), unit=exp.var(word))

    def build_median(self, inner: Expression) -> Expression:
        # ``MariadbDialect`` inherits this, so never suggest "use MariaDB".
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


class MariadbDialect(MysqlDialect):
    """MariaDB renders as MySQL; it names its statement timeout differently."""

    ds_type_aliases: frozenset[str] = frozenset({"mariadb"})

    def statement_timeout_sql(self, timeout_seconds: int) -> str | None:
        return f"SET max_statement_time = {timeout_seconds}"
