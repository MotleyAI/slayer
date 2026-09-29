"""TsqlDialect (SQL Server / Microsoft T-SQL).

T-SQL is the most divergent Tier-1 dialect:

* ``DATETRUNC(unit, col)`` (SQL Server 2022+) instead of ``DATE_TRUNC``
* Week uses ``iso_week`` to be ``@@DATEFIRST``-independent (Monday-based)
* ``DATEADD`` / ``DATEDIFF_BIG`` for date arithmetic (no INTERVAL)
* ``build_median`` / ``build_percentile`` raise — PERCENTILE_CONT in T-SQL
  is a window function only
* Statistical aggregate names: STDEV / STDEVP / VAR / VARP via
  ``exp.Anonymous`` (sqlglot's tsql transpiler emits wrong names)
* Variance-decomposition formula for CORR / COVAR_* with the T-SQL names
* EXPLAIN is a session-toggle pair: ``SET SHOWPLAN_ALL ON; ... ; OFF``
* No native LOG2
* T-SQL's ``ORDER BY`` resolver does not treat
  ``[a.b]`` as a SELECT alias — it tries to resolve it as a column-name
  lookup against the FROM scope. ``rewrite_emitted_sql`` mangles dotted
  bracketed aliases to ``[a___b]``; ``decode_result_keys`` reverses on
  result rows. Same bijection as ``BigqueryDialect``, different regex
  anchor.
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


# sqlglot's tsql transpiler emits incorrect names (VAR_SAMP, VARIANCE_POP)
# that do not exist in T-SQL — these are the correct T-SQL canonical names.
_TSQL_STAT_NAMES: dict[str, str] = {
    "stddev_samp": "STDEV",
    "stddev_pop": "STDEVP",
    "var_samp": "VAR",
    "var_pop": "VARP",
}


# Bracket-quoted dotted alias. Same shape as BigQuery's
# backtick-anchored regex (``\w+(?:\.\w+)+``) with ``re.ASCII`` keeping
# ``\w`` ASCII-only so accented identifiers like ``[café.metric]`` do
# not mangle.
#
# Caveat (documented constraint, identical to BigQuery's): a fully
# bracketed dotted path of word-only segments (e.g. ``[my_schema.my_table]``)
# WOULD false-positive mangle. T-SQL users writing such paths in
# ``Column.sql`` must bracket each segment individually
# (``[my_schema].[my_table]``). T-SQL identifiers with spaces, hyphens,
# or other non-``\w`` characters (``[my table]``) are safe — the
# non-word character breaks the match.
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
    # Anonymous: sqlglot re-emits a parsed APPROX_COUNT_DISTINCT as its
    # Presto-family APPROX_DISTINCT canonical, which is not a T-SQL function.
    approx_count_distinct_anonymous_name: str | None = "APPROX_COUNT_DISTINCT"
    # Bracketed dotted-alias mangling (DottedAliasManglingMixin).
    dotted_alias_re: ClassVar[re.Pattern[str]] = _TSQL_DOTTED_ALIAS_RE
    alias_quote_open: ClassVar[str] = "["
    alias_quote_close: ClassVar[str] = "]"

    def build_null_safe_eq(
        self, left: Expression, right: Expression,
    ) -> Expression:
        """T-SQL has no ``IS NOT DISTINCT FROM`` / ``<=>`` — emit the
        portable expanded ``a = b OR (a IS NULL AND b IS NULL)``."""
        return self._expanded_null_safe_eq(left, right)

    def build_ordered(
        self,
        order_col: Expression,
        *,
        descending: bool,
        nulls: Literal["default", "first", "last"] = "default",
    ) -> exp.Ordered:
        """Pin ``nulls_first`` to T-SQL's native
        default for the direction (FIRST on ASC, LAST on DESC).

        Left unset, sqlglot emits ``CASE WHEN <alias> IS NULL THEN 1 ELSE 0
        END, <alias>`` to emulate the nulls-last ordering every other dialect
        gets; the bracketed alias INSIDE the CASE WHEN mis-resolves against the
        FROM scope (``Invalid column name``). So T-SQL trades null-ordering
        parity for a statement that runs — the one place SLayer's null ordering
        is dialect-specific, and only because the portable form is unavailable.

        An EXPLICIT ``first`` / ``last`` policy is honoured as asked — the pin
        exists to avoid the emulation, not to override a stated intent.
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
        """T-SQL: ``DATETRUNC(unit, col)``. Week uses ``iso_week``
        (Monday-start) to be ``@@DATEFIRST``-independent. ``DATETRUNC``
        requires a temporal type — wrap non-column/cast operands.

        ``DATETRUNC`` requires **SQL Server 2022+**. SLayer's T-SQL
        support is documented as 2022+ only (see ``CLAUDE.md`` under
        Tier-1 / SQL Server, and ``examples/sqlserver/``). Pre-2022
        SQL Server does not have a single-call truncation function;
        an equivalent ``DATEADD(unit, DATEDIFF(unit, 0, col), 0)``
        fallback exists but isn't a current target — track separately
        if anyone needs it.
        """
        if granularity == TimeGranularity.WEEK_SUNDAY:
            # Delegate to the base generic shift, which composes
            # T-SQL's DATEADD day-offset around the iso_week (Monday) DATETRUNC.
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
        """T-SQL ``SUBSTRING`` requires a length: a 2-arg call reads to the end via ``LEN``."""
        def _fix(node: Expression) -> Expression:
            if isinstance(node, exp.Substring) and node.args.get("length") is None:
                node.set("length", exp.Length(this=node.this.copy()))
            return node
        return tree.transform(_fix)

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
        """T-SQL: map ``stddev_samp``→``STDEV``, ``stddev_pop``→``STDEVP``,
        ``var_samp``→``VAR``, ``var_pop``→``VARP`` via ``exp.Anonymous``."""
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
        """T-SQL has no native CORR / COVAR_* — use the
        variance-decomposition formula with T-SQL names (VAR / VARP / STDEV)."""
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
        """T-SQL pagination, with the ``OFFSET`` ordering requirement made
        explicit.

        SQL Server rejects ``OFFSET`` without an ``ORDER BY``. When the query is
        genuinely unordered we supply ``ORDER BY (SELECT NULL)`` — the
        conventional no-op ordering, which adds no semantics because there were
        none to preserve, and only makes the statement legal.

        sqlglot happens to inject the same thing today, but that is its
        behaviour and not our contract: doing it here means the rule survives a
        sqlglot upgrade, and it puts the ordering in the AST where a caller (and
        our tests) can see it rather than only in the generated string. A user's
        own ORDER BY is never replaced.

        ``TOP`` versus ``FETCH`` needs no special handling — sqlglot picks
        ``TOP`` for a bare limit and ``OFFSET … FETCH`` once an offset is
        present, which is the correct T-SQL in both cases.
        """
        if offset is not None and select.args.get("order") is None:
            select = select.order_by(
                exp.Subquery(this=exp.Select().select(exp.Null())),
            )
        return super().apply_pagination(select, limit=limit, offset=offset)
