"""DEV-1542: TsqlDialect (SQL Server / Microsoft T-SQL).

T-SQL is the most divergent Tier-1 dialect:

* ``DATETRUNC(unit, col)`` (SQL Server 2022+) instead of ``DATE_TRUNC``
* Week uses ``iso_week`` to be ``@@DATEFIRST``-independent (Monday-based)
* ``DATEADD(unit, val, col)`` instead of ``col + INTERVAL N UNIT``
* ``add_intervals_expr`` chains ``DATEADD`` calls (no INTERVAL)
* ``build_median`` / ``build_percentile`` raise — PERCENTILE_CONT in T-SQL
  is a window function only
* Statistical aggregate names: STDEV / STDEVP / VAR / VARP via
  ``exp.Anonymous`` (sqlglot's tsql transpiler emits wrong names)
* Variance-decomposition formula for CORR / COVAR_* with the T-SQL names
* EXPLAIN is a session-toggle pair: ``SET SHOWPLAN_ALL ON; ... ; OFF``
* No native LOG2
"""

from __future__ import annotations

from typing import Callable, Optional

from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects.base import SqlDialect, _build_covar_decomposition


# sqlglot's tsql transpiler emits incorrect names (VAR_SAMP, VARIANCE_POP)
# that do not exist in T-SQL — these are the correct T-SQL canonical names.
_TSQL_STAT_NAMES: dict[str, str] = {
    "stddev_samp": "STDEV",
    "stddev_pop": "STDEVP",
    "var_samp": "VAR",
    "var_pop": "VARP",
}


class TsqlDialect(SqlDialect):
    sqlglot_name: str = "tsql"
    ds_type_aliases: frozenset[str] = frozenset({"mssql", "sqlserver", "tsql"})
    explain_prefix: Optional[str] = "SET SHOWPLAN_ALL ON;"
    explain_postfix: str = "; SET SHOWPLAN_ALL OFF"
    log10_native: bool = True
    log2_native: bool = False

    def build_date_trunc(
        self,
        col_expr: exp.Expression,
        granularity: TimeGranularity,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
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
        gran_str = granularity.value
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        tsql_gran = "iso_week" if gran_str == "week" else gran_str
        return exp.Anonymous(
            this="DATETRUNC",
            expressions=[exp.Var(this=tsql_gran), col_expr],
        )

    def build_time_offset_expr(
        self,
        col_expr: exp.Expression,
        offset: int,
        granularity: str,
    ) -> exp.Expression:
        """T-SQL: ``DATEADD(unit, val, col)``. INTERVAL is not valid T-SQL syntax.
        Quarter normalises to ``val * 3`` of MONTH."""
        unit_map = {
            "year": "YEAR", "month": "MONTH", "day": "DAY",
            "quarter": "MONTH", "week": "WEEK",
            "hour": "HOUR", "minute": "MINUTE", "second": "SECOND",
        }
        unit = unit_map.get(granularity, granularity.upper())
        val = offset * 3 if granularity == "quarter" else offset
        return exp.Anonymous(
            this="DATEADD",
            expressions=[exp.Var(this=unit), exp.Literal.number(val), col_expr],
        )

    def add_intervals_expr(
        self,
        expr: exp.Expression,
        intervals: list[exp.Expression],
        sign: int = 1,
    ) -> exp.Expression:
        """T-SQL: chain ``DATEADD(unit, ±amount, col)`` calls.

        Each interval in the list is an ``exp.Interval`` from
        ``duration_interval_exprs``; extract unit name and amount, negate
        when sign < 0.
        """
        result = expr
        for iv in intervals:
            if not isinstance(iv, exp.Interval):
                raise TypeError(
                    f"Expected exp.Interval in T-SQL DATEADD branch, got {type(iv)}"
                )
            unit_str = iv.unit.name.upper()
            amount = exp.Neg(this=iv.this) if sign < 0 else iv.this
            result = exp.Anonymous(
                this="DATEADD",
                expressions=[exp.Var(this=unit_str), amount, result],
            )
        return result

    def build_median(
        self,
        inner: exp.Expression,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        raise NotImplementedError(
            "Aggregation 'median' is not supported on T-SQL (SQL Server): "
            "PERCENTILE_CONT in T-SQL is a window function (requires OVER clause) "
            "and cannot be used as a GROUP BY aggregate. "
            "Use a window subquery or compute the value client-side."
        )

    def build_percentile(
        self,
        p_str: str,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        raise NotImplementedError(
            "Aggregation 'percentile' is not supported on T-SQL (SQL Server): "
            "PERCENTILE_CONT requires a window function OVER clause in T-SQL "
            "and is not valid as a GROUP BY aggregate. "
            "Compute the value client-side or restructure as a window query."
        )

    def build_stat_agg_1arg(
        self,
        agg_name: str,
        col_expr: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """T-SQL: map ``stddev_samp``→``STDEV``, ``stddev_pop``→``STDEVP``,
        ``var_samp``→``VAR``, ``var_pop``→``VARP`` via ``exp.Anonymous``."""
        if agg_name in _TSQL_STAT_NAMES:
            return exp.Anonymous(
                this=_TSQL_STAT_NAMES[agg_name],
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
        """T-SQL has no native CORR / COVAR_* — use the
        variance-decomposition formula with T-SQL names (VAR / VARP / STDEV)."""
        return _build_covar_decomposition(
            col_sql=col_sql,
            other_sql=other_sql,
            agg=agg_name,
            var_fn_samp="VAR",
            var_fn_pop="VARP",
            stddev_fn="STDEV",
            parse=parse,
        )
