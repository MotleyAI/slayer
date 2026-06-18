"""DEV-1542: tests for TsqlDialect (SQL Server / Microsoft T-SQL).

T-SQL has the most divergent shape of any Tier-1 dialect:
* ``DATETRUNC(unit, col)`` instead of ``DATE_TRUNC('unit', col)``
* Week uses ``iso_week`` for Monday-based truncation
* ``DATEADD(unit, val, col)`` instead of ``col + INTERVAL N UNIT``
* PERCENTILE_CONT is window-only — ``build_median`` / ``build_percentile``
  raise ``NotImplementedError``
* Statistical aggregate names: STDEV / STDEVP / VAR / VARP (not the
  Postgres canonical names)
* Variance-decomposition formula for CORR / COVAR_SAMP / COVAR_POP
* EXPLAIN is a session-toggle pair: ``SET SHOWPLAN_ALL ON; ... ; SET SHOWPLAN_ALL OFF``
* No native LOG2 (log2_native = False)
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

import pytest

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects.tsql import TsqlDialect


def _parse_tsql(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql, dialect="tsql")


def test_tsql_sqlglot_name() -> None:
    assert TsqlDialect().sqlglot_name == "tsql"


def test_tsql_explain_prefix_and_postfix() -> None:
    d = TsqlDialect()
    assert d.explain_prefix == "SET SHOWPLAN_ALL ON;"
    assert d.explain_postfix == "; SET SHOWPLAN_ALL OFF"


def test_tsql_log_native_flags() -> None:
    """SQL Server has LOG10 but no LOG2 (sqlglot has no LOG2 emit for tsql)."""
    d = TsqlDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is False


def test_tsql_ds_type_aliases() -> None:
    assert TsqlDialect().ds_type_aliases == frozenset({"mssql", "sqlserver", "tsql"})


# ---------------------------------------------------------------------------
# build_date_trunc — DATETRUNC(unit, col), iso_week for week
# ---------------------------------------------------------------------------


def test_tsql_build_date_trunc_month() -> None:
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_date_trunc(col, TimeGranularity.MONTH, parse=_parse_tsql)
    sql = out.sql(dialect="tsql").lower()
    assert "datetrunc" in sql
    assert "month" in sql


def test_tsql_build_date_trunc_week_uses_iso_week() -> None:
    """Week must use ISO_WEEK (Monday-start) to be @@DATEFIRST-independent."""
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_date_trunc(col, TimeGranularity.WEEK, parse=_parse_tsql)
    sql = out.sql(dialect="tsql").lower()
    assert "datetrunc" in sql
    assert "iso_week" in sql


def test_tsql_build_date_trunc_week_sunday_shift() -> None:
    """DEV-1572: WEEK_SUNDAY delegates to the generic shift, which composes
    T-SQL's DATEADD day-offset around the iso_week (Monday) DATETRUNC."""
    d = TsqlDialect()
    col = sqlglot.parse_one("ordered_at", dialect="tsql")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY, parse=_parse_tsql)
    sql = out.sql(dialect="tsql").lower()
    assert "datetrunc" in sql
    assert "iso_week" in sql          # inner Monday-week truncation
    assert "dateadd(day, 1," in sql   # inner +1 day
    assert "dateadd(day, -1," in sql  # outer -1 day


def test_tsql_build_date_trunc_casts_non_column_to_timestamp() -> None:
    """``DATETRUNC`` requires a temporal type — non-column operands are
    wrapped in ``CAST(... AS TIMESTAMP)``."""
    d = TsqlDialect()
    lit = sqlglot.parse_one("'2025-01-01'", dialect="tsql")
    out = d.build_date_trunc(lit, TimeGranularity.MONTH, parse=_parse_tsql)
    assert "CAST" in out.sql(dialect="tsql").upper()


# ---------------------------------------------------------------------------
# build_time_offset_expr — DATEADD, no INTERVAL
# ---------------------------------------------------------------------------


def test_tsql_build_time_offset_expr_day() -> None:
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_time_offset_expr(col, offset=3, granularity="day")
    sql = out.sql(dialect="tsql").upper()
    assert "DATEADD" in sql
    assert "DAY" in sql
    assert "3" in sql
    assert "INTERVAL" not in sql


def test_tsql_build_time_offset_expr_negative() -> None:
    """DATEADD takes a signed amount as its second arg — negative values
    propagate directly into the call."""
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_time_offset_expr(col, offset=-2, granularity="month")
    sql = out.sql(dialect="tsql").upper()
    assert "DATEADD" in sql
    assert "-2" in sql or "(-2)" in sql or "-(2)" in sql


def test_tsql_build_time_offset_expr_quarter_normalizes_to_3_month() -> None:
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_time_offset_expr(col, offset=1, granularity="quarter")
    sql = out.sql(dialect="tsql").upper()
    assert "DATEADD" in sql
    assert "MONTH" in sql
    assert "3" in sql


# ---------------------------------------------------------------------------
# add_intervals_expr — chains DATEADD calls (no INTERVAL)
# ---------------------------------------------------------------------------


def test_tsql_add_intervals_expr_uses_dateadd_chain() -> None:
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    intervals = [
        exp.Interval(
            this=exp.Literal.number(1),
            unit=exp.Var(this="DAY"),
        ),
    ]
    out = d.add_intervals_expr(col, intervals, sign=-1)
    sql = out.sql(dialect="tsql").upper()
    assert "DATEADD" in sql
    assert "INTERVAL" not in sql  # no INTERVAL keyword in T-SQL


# ---------------------------------------------------------------------------
# Median / percentile — not supported on T-SQL
# ---------------------------------------------------------------------------


def test_tsql_build_median_raises_not_implemented() -> None:
    d = TsqlDialect()
    inner = sqlglot.parse_one("amount", dialect="tsql")
    with pytest.raises(NotImplementedError, match="median.*T-SQL"):
        d.build_median(inner, parse=_parse_tsql)


def test_tsql_build_percentile_raises_not_implemented() -> None:
    d = TsqlDialect()
    with pytest.raises(NotImplementedError, match="percentile.*T-SQL"):
        d.build_percentile("0.5", "amount", parse=_parse_tsql)


# ---------------------------------------------------------------------------
# Stat aggs — T-SQL canonical names via exp.Anonymous
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "agg_name,tsql_fn",
    [
        ("stddev_samp", "STDEV"),
        ("stddev_pop", "STDEVP"),
        ("var_samp", "VAR"),
        ("var_pop", "VARP"),
    ],
)
def test_tsql_build_stat_agg_1arg_uses_tsql_names(
    agg_name: str, tsql_fn: str
) -> None:
    """sqlglot's tsql transpiler emits incorrect names (e.g. VAR_SAMP,
    VARIANCE_POP). The override emits the canonical T-SQL names via
    ``exp.Anonymous``."""
    d = TsqlDialect()
    out = d.build_stat_agg_1arg(agg_name, "amount", parse=_parse_tsql)
    sql = out.sql(dialect="tsql").upper()
    assert tsql_fn in sql
    # Sanity: NOT the Postgres-canonical name
    assert agg_name.upper() not in sql


# ---------------------------------------------------------------------------
# Covar — variance-decomposition formula with T-SQL names
# ---------------------------------------------------------------------------


def test_tsql_build_covar_2arg_corr_uses_decomposition() -> None:
    d = TsqlDialect()
    out = d.build_covar_2arg("corr", "amount", "quantity", parse=_parse_tsql)
    sql = out.sql(dialect="tsql").upper()
    # T-SQL covariance formula uses VAR / STDEV (sample form for corr/covar_samp)
    assert "VAR" in sql
    assert "STDEV" in sql
    assert "NULLIF" in sql  # zero-denominator guard for corr


def test_tsql_build_covar_2arg_covar_pop_uses_varp() -> None:
    d = TsqlDialect()
    out = d.build_covar_2arg("covar_pop", "amount", "quantity", parse=_parse_tsql)
    sql = out.sql(dialect="tsql").upper()
    assert "VARP" in sql


# ---------------------------------------------------------------------------
# build_explain_sql — wraps in SHOWPLAN session toggle pair
# ---------------------------------------------------------------------------


def test_tsql_build_explain_sql_wraps_in_showplan_pair() -> None:
    d = TsqlDialect()
    assert d.build_explain_sql("SELECT 1") == (
        "SET SHOWPLAN_ALL ON; SELECT 1; SET SHOWPLAN_ALL OFF"
    )
