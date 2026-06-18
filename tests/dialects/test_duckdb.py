"""DEV-1542: tests for DuckdbDialect.

DuckDB matches Postgres for every SQL-generation quirk we care about —
native DATE_TRUNC, native PERCENTILE_CONT (via sqlglot's QUANTILE_CONT
translation), native CORR / COVAR_SAMP / COVAR_POP, native log10 / log2.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects.duckdb import DuckdbDialect


def _parse_duckdb(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql, dialect="duckdb")


def test_duckdb_sqlglot_name() -> None:
    assert DuckdbDialect().sqlglot_name == "duckdb"


def test_duckdb_explain_prefix() -> None:
    assert DuckdbDialect().explain_prefix == "EXPLAIN ANALYZE"


def test_duckdb_log_native_flags() -> None:
    d = DuckdbDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


def test_duckdb_build_date_trunc_month() -> None:
    d = DuckdbDialect()
    col = sqlglot.parse_one("created_at", dialect="duckdb")
    out = d.build_date_trunc(col, TimeGranularity.MONTH, parse=_parse_duckdb)
    sql = out.sql(dialect="duckdb").upper()
    assert "DATE_TRUNC" in sql
    assert "MONTH" in sql


def test_duckdb_build_date_trunc_week_sunday_shift() -> None:
    """DEV-1572: WEEK_SUNDAY reuses DuckDB's native (Monday) DATE_TRUNC('week')
    with the +1d / -1d shift. DuckDB emits unquoted ``INTERVAL 1 DAY``."""
    d = DuckdbDialect()
    col = sqlglot.parse_one("ordered_at", dialect="duckdb")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY, parse=_parse_duckdb)
    up = out.sql(dialect="duckdb").upper()
    assert "DATE_TRUNC('WEEK'" in up
    assert "+ INTERVAL 1 DAY" in up
    assert "- INTERVAL 1 DAY" in up


def test_duckdb_build_median() -> None:
    """DuckDB uses sqlglot's QUANTILE_CONT translation for PERCENTILE_CONT —
    accept either spelling at the SQL-emit layer."""
    d = DuckdbDialect()
    inner = sqlglot.parse_one("amount", dialect="duckdb")
    out = d.build_median(inner, parse=_parse_duckdb)
    sql = out.sql(dialect="duckdb").upper()
    assert "QUANTILE_CONT" in sql or "PERCENTILE_CONT" in sql
    assert "0.5" in sql


def test_duckdb_build_percentile_preserves_literal() -> None:
    d = DuckdbDialect()
    out = d.build_percentile("0.50", "amount", parse=_parse_duckdb)
    assert "0.50" in out.sql(dialect="duckdb")


def test_duckdb_build_covar_2arg_corr_native() -> None:
    d = DuckdbDialect()
    out = d.build_covar_2arg("corr", "amount", "quantity", parse=_parse_duckdb)
    assert "CORR" in out.sql(dialect="duckdb").upper()


def test_duckdb_build_time_offset_expr_quarter_normalizes_to_3_month() -> None:
    d = DuckdbDialect()
    col = sqlglot.parse_one("created_at", dialect="duckdb")
    out = d.build_time_offset_expr(col, offset=1, granularity="quarter")
    sql = out.sql(dialect="duckdb").upper()
    assert "MONTH" in sql
    assert "3" in sql
