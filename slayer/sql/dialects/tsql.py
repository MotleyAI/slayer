"""TsqlDialect (SQL Server 2022+).

Dotted aliases are mangled to ``[a___b]``: T-SQL's ``ORDER BY`` resolves ``[a.b]``
against the FROM scope, not the SELECT list.
"""

from __future__ import annotations

import re
from typing import ClassVar, Literal

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.sql.dialects.base import (
    DottedAliasManglingMixin,
    SqlDialect,
    StatAgg1Name,
    StatAgg2Name,
    _build_covar_decomposition,
)


# sqlglot's tsql transpiler emits names (VAR_SAMP, VARIANCE_POP) T-SQL lacks.
_TSQL_STAT_NAMES: dict[str, str] = {
    "stddev_samp": "STDEV",
    "stddev_pop": "STDEVP",
    "var_samp": "VAR",
    "var_pop": "VARP",
}


# Caveat: a single-bracketed word-only path like ``[schema.tbl]`` false-positive mangles.
_TSQL_DOTTED_ALIAS_RE = re.compile(r"\[(\w+(?:\.\w+)+)\]", re.ASCII)


_DATEPART_UNITS = {
    DatePart.YEAR: "YEAR", DatePart.QUARTER: "QUARTER", DatePart.MONTH: "MONTH",
    DatePart.WEEK: "ISO_WEEK", DatePart.DAY: "DAY", DatePart.DAY_OF_YEAR: "DAYOFYEAR",
    DatePart.HOUR: "HOUR", DatePart.MINUTE: "MINUTE", DatePart.SECOND: "SECOND",
}
_DATEADD_UNITS = {
    TimeGranularity.SECOND: "SECOND", TimeGranularity.MINUTE: "MINUTE", TimeGranularity.HOUR: "HOUR",
    TimeGranularity.DAY: "DAY", TimeGranularity.WEEK: "WEEK", TimeGranularity.WEEK_SUNDAY: "WEEK",
    TimeGranularity.MONTH: "MONTH", TimeGranularity.QUARTER: "QUARTER", TimeGranularity.YEAR: "YEAR",
}


def _datepart(unit: str, expr: Expression) -> Expression:
    return exp.Anonymous(this="DATEPART", expressions=[exp.var(unit), expr])


def _dateadd(unit: str, count: Expression, expr: Expression) -> Expression:
    return exp.Anonymous(this="DATEADD", expressions=[exp.var(unit), count, expr])


def _days_since_monday(expr: Expression) -> Expression:
    """0 for Monday … 6 for Sunday, counted from 1900-01-01 (a Monday)."""
    days = exp.Anonymous(this="DATEDIFF", expressions=[
        exp.var("DAY"), exp.Cast(this=exp.Literal.string("1900-01-01"), to=exp.DataType.build("DATE")),
        expr.copy(),
    ])
    inner = exp.Mod(this=days, expression=exp.Literal.number(7))
    return exp.Mod(
        this=exp.Paren(this=exp.Add(this=exp.Paren(this=inner), expression=exp.Literal.number(7))),
        expression=exp.Literal.number(7),
    )


class TsqlDialect(DottedAliasManglingMixin, SqlDialect):
    sqlglot_name: str = "tsql"
    ds_type_aliases: frozenset[str] = frozenset({"mssql", "sqlserver", "tsql"})
    explain_prefix: str | None = "SET SHOWPLAN_ALL ON;"
    explain_postfix: str = "; SET SHOWPLAN_ALL OFF"
    log10_native: bool = True
    log2_native: bool = False
    max_identifier_bytes: int | None = 128  # sysname is nvarchar(128)
    # sqlglot would re-emit APPROX_DISTINCT, which T-SQL lacks.
    approx_count_distinct_anonymous_name: str | None = "APPROX_COUNT_DISTINCT"
    url_scheme: str | None = "mssql+pyodbc"
    install_extra: str | None = "sqlserver"
    dotted_alias_re: ClassVar[re.Pattern[str]] = _TSQL_DOTTED_ALIAS_RE
    alias_quote_open: ClassVar[str] = "["
    alias_quote_close: ClassVar[str] = "]"

    def build_null_safe_eq(
        self, left: Expression, right: Expression,
    ) -> Expression:
        """T-SQL has no ``IS NOT DISTINCT FROM`` / ``<=>``."""
        return self._expanded_null_safe_eq(left, right)

    def build_ordered(
        self,
        order_col: Expression,
        *,
        descending: bool,
        nulls: Literal["default", "first", "last"] = "default",
    ) -> exp.Ordered:
        """Pin the default to T-SQL's native null order; an explicit policy is honoured.

        sqlglot's nulls-last ``CASE WHEN <alias> IS NULL`` emulation mis-resolves the bracketed alias.
        """
        if nulls == "default":
            return exp.Ordered(
                this=order_col, desc=descending, nulls_first=not descending,
            )
        return super().build_ordered(
            order_col, descending=descending, nulls=nulls,
        )

    def build_date_trunc(
        self,
        col_expr: Expression,
        granularity: TimeGranularity,
    ) -> Expression:
        """``DATETRUNC`` (2022+); ``iso_week`` keeps weeks ``@@DATEFIRST``-independent."""
        if granularity == TimeGranularity.WEEK_SUNDAY:
            return super().build_date_trunc(
                col_expr=col_expr, granularity=granularity,
            )
        gran_str = granularity.value
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        tsql_gran = "iso_week" if gran_str == "week" else gran_str
        return exp.Anonymous(
            this="DATETRUNC",
            expressions=[exp.Var(this=tsql_gran), col_expr],
        )

    def rewrite_target_ast(self, tree: Expression) -> Expression:
        """``LEN`` drops trailing spaces: ``length`` counts an ``NVARCHAR(MAX)`` copy through a
        sentinel, and a 2-arg ``SUBSTRING`` (which requires a length) reads to the end via ``DATALENGTH``."""
        def _fix(node: Expression) -> Expression:
            if isinstance(node, exp.Substring) and node.args.get("length") is None:
                node.set("length", exp.Anonymous(this="DATALENGTH", expressions=[node.this.copy()]))
            if isinstance(node, exp.Length):
                text = exp.Cast(this=node.this.copy(), to=exp.DataType.build("NVARCHAR(MAX)", dialect="tsql"))
                padded = exp.Add(this=text, expression=exp.Literal.string("x"))
                return exp.Paren(this=exp.Sub(
                    this=exp.Anonymous(this="LEN", expressions=[padded]), expression=exp.Literal.number(1),
                ))
            return node
        return tree.transform(_fix)

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        return exp.select(exp.column("value").as_("i")).from_(exp.Table(
            this=exp.Anonymous(this="GENERATE_SERIES", expressions=[
                exp.Literal.number(0), exp.Literal.number(size - 1),
            ]),
            alias=exp.TableAlias(this=exp.to_identifier("_seq")),
        ))

    def build_current_date(self) -> Expression:
        return exp.Cast(this=exp.Anonymous(this="GETDATE"), to=exp.DataType.build("DATE"))

    def build_current_timestamp(self) -> Expression:
        return exp.Anonymous(this="SYSDATETIME")

    def _date_part(self, part: DatePart, expr: Expression) -> Expression:
        """``DATEPART``; weekday from a fixed Monday epoch so ``DATEFIRST`` never matters."""
        if part is DatePart.DAY_OF_WEEK:
            return exp.Add(this=exp.Paren(this=_days_since_monday(expr)), expression=exp.Literal.number(1))
        if part is DatePart.ISO_YEAR:
            thursday = _dateadd("DAY", exp.Sub(
                this=exp.Literal.number(3), expression=exp.Paren(this=_days_since_monday(expr)),
            ), expr.copy())
            return _datepart("YEAR", thursday)
        return _datepart(_DATEPART_UNITS[part], expr)

    def build_date_diff(
        self, *, unit: TimeGranularity, start: Expression, end: Expression, operand: DataType,
    ) -> Expression:
        """``DATEDIFF_BIG`` counts boundaries; its ``WEEK`` is Sunday-based regardless of ``DATEFIRST``."""
        if unit in SUB_DAY_GRANULARITIES and operand is DataType.DATE:
            start, end = self.promote_to_timestamp(start), self.promote_to_timestamp(end)
        else:
            start, end = start.copy(), end.copy()
        if unit is TimeGranularity.WEEK:
            # Monday boundaries = Sunday boundaries of the day before.
            start = _dateadd("DAY", exp.Literal.number(-1), start)
            end = _dateadd("DAY", exp.Literal.number(-1), end)
        return exp.Anonymous(this="DATEDIFF_BIG", expressions=[exp.var(_DATEADD_UNITS[unit]), start, end])

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """``DATEADD`` (clamps at month-end); a DATE is promoted to DATETIME2 for sub-day units."""
        base = (
            self.promote_to_timestamp(expr)
            if unit in SUB_DAY_GRANULARITIES and operand is DataType.DATE else expr.copy()
        )
        return _dateadd(_DATEADD_UNITS[unit], count.copy(), base)

    def build_median(self, inner: Expression) -> Expression:
        raise NotImplementedError(
            "Aggregation 'median' is not supported on T-SQL (SQL Server): "
            "PERCENTILE_CONT in T-SQL is a window function (requires OVER clause) "
            "and cannot be used as a GROUP BY aggregate. "
            "Use a window subquery or compute the value client-side."
        )

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        raise NotImplementedError(
            "Aggregation 'percentile' is not supported on T-SQL (SQL Server): "
            "PERCENTILE_CONT requires a window function OVER clause in T-SQL "
            "and is not valid as a GROUP BY aggregate. "
            "Compute the value client-side or restructure as a window query."
        )

    def build_stat_agg_1arg(
        self, agg_name: StatAgg1Name, col_expr: Expression,
    ) -> Expression:
        """Emit the T-SQL names (``STDEV`` / ``STDEVP`` / ``VAR`` / ``VARP``)."""
        if agg_name in _TSQL_STAT_NAMES:
            return exp.Anonymous(
                this=_TSQL_STAT_NAMES[agg_name],
                expressions=[col_expr.copy()],
            )
        return super().build_stat_agg_1arg(agg_name=agg_name, col_expr=col_expr)

    def build_covar_2arg(
        self,
        agg_name: StatAgg2Name,
        col_expr: Expression,
        other_expr: Expression,
    ) -> Expression:
        """T-SQL has no CORR / COVAR_*: variance decomposition with T-SQL names."""
        return _build_covar_decomposition(
            col_expr=col_expr,
            other_expr=other_expr,
            agg=agg_name,
            var_fn_samp="VAR",
            var_fn_pop="VARP",
            stddev_fn="STDEV",
        )

    def apply_pagination(
        self,
        select: exp.Select,
        *,
        limit: "int | None",
        offset: "int | None",
    ) -> exp.Select:
        """SQL Server rejects ``OFFSET`` without ``ORDER BY``: add ``ORDER BY (SELECT NULL)``."""
        if offset is not None and select.args.get("order") is None:
            select = select.order_by(
                exp.Subquery(this=exp.Select().select(exp.Null())),
            )
        return super().apply_pagination(select, limit=limit, offset=offset)
