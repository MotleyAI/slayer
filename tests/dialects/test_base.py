"""Tests for the SqlDialect base class default impls."""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

import pytest

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects import (
    _ALL_DIALECTS,
    ClickhouseDialect,
    DuckdbDialect,
    MysqlDialect,
    PostgresDialect,
    SnowflakeDialect,
    SqliteDialect,
    TsqlDialect,
)
from slayer.sql.dialects import base as base_mod
from slayer.sql.dialects._tier2 import (
    DatabricksDialect,
    OracleDialect,
    PrestoDialect,
    RedshiftDialect,
    SparkDialect,
    TrinoDialect,
)
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.bigquery import BigqueryDialect


# build_date_trunc — default impl


def test_default_build_date_trunc_month() -> None:
    d = SqlDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_date_trunc(col, TimeGranularity.MONTH)
    sql = out.sql(dialect=d.sqlglot_name)
    assert "DATE_TRUNC" in sql.upper()
    assert "MONTH" in sql.upper()


def test_default_build_date_trunc_casts_non_column_to_timestamp() -> None:
    """Non-column operands get an explicit timestamp CAST."""
    d = SqlDialect()
    literal_expr = sqlglot.parse_one("'2025-01-01'", dialect="postgres")
    out = d.build_date_trunc(literal_expr, TimeGranularity.MONTH)
    assert "CAST" in out.sql(dialect=d.sqlglot_name).upper()


def test_default_build_date_trunc_idempotent_on_already_cast() -> None:
    """Already-cast expressions don't get double-wrapped."""
    d = SqlDialect()
    cast_expr = exp.Cast(
        this=sqlglot.parse_one("'2025-01-01'", dialect="postgres"),
        to=exp.DataType.build("TIMESTAMP"),
    )
    out = d.build_date_trunc(cast_expr, TimeGranularity.MONTH)
    sql = out.sql(dialect=d.sqlglot_name)
    # one CAST inside the DATE_TRUNC, not two nested
    assert sql.upper().count("CAST") == 1


# build_time_offset_expr — default impl uses INTERVAL


def test_default_build_time_offset_expr_uses_interval_add() -> None:
    d = SqlDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_time_offset_expr(col, offset=3, granularity="day")
    sql = out.sql(dialect=d.sqlglot_name)
    assert "INTERVAL" in sql.upper()
    assert "DAY" in sql.upper()


def test_default_build_time_offset_expr_negative_uses_subtract() -> None:
    d = SqlDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_time_offset_expr(col, offset=-2, granularity="month")
    sql = out.sql(dialect=d.sqlglot_name)
    assert "INTERVAL" in sql.upper()
    # the magnitude is positive, sign is in the operator
    assert "MONTH" in sql.upper()


def test_default_build_time_offset_expr_quarter_normalizes_to_3_month() -> None:
    """Quarter→3*month normalization is preserved across every dialect (today's ``generator.py:1037``)."""
    d = SqlDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_time_offset_expr(col, offset=1, granularity="quarter")
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "MONTH" in sql
    # The literal value should reflect quarter * 3
    assert "3" in sql


# duration_interval_exprs / add_intervals_expr — default impl uses INTERVAL nodes


def test_default_duration_interval_exprs_returns_interval_per_part() -> None:
    """The default impl yields one ``exp.Interval`` per parsed (amount, unit) pair."""
    d = SqlDialect()
    # parts = [(2, 'd'), (3, 'h')] — Postgres-shaped chained intervals
    out = d.duration_interval_exprs([(2, "d"), (3, "h")], sign=1)
    assert len(out) == 2
    assert all(isinstance(n, exp.Interval) for n in out)


def test_default_add_intervals_expr_chains_exp_add_for_positive_sign() -> None:
    """Positive sign folds with ``exp.Add`` (col + interval [+ interval ...])."""
    d = SqlDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    iv = exp.Interval(this=exp.Literal.number(1), unit=exp.Var(this="DAY"))
    out = d.add_intervals_expr(col, [iv], sign=1)
    assert isinstance(out, exp.Add)
    sql = out.sql(dialect="postgres").upper()
    assert "INTERVAL" in sql
    assert "DAY" in sql


def test_default_add_intervals_expr_uses_exp_sub_for_negative_sign() -> None:
    """Negative sign folds with ``exp.Sub`` (col - interval)."""
    d = SqlDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    iv = exp.Interval(this=exp.Literal.number(1), unit=exp.Var(this="DAY"))
    out = d.add_intervals_expr(col, [iv], sign=-1)
    assert isinstance(out, exp.Sub)


# build_median / build_percentile — default impl is PERCENTILE_CONT WITHIN GROUP


def test_default_build_median_uses_percentile_cont() -> None:
    d = SqlDialect()
    inner = sqlglot.parse_one("amount", dialect="postgres")
    out = d.build_median(inner)
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "PERCENTILE_CONT" in sql
    assert "WITHIN GROUP" in sql
    assert "0.5" in sql


def test_default_build_percentile_uses_percentile_cont() -> None:
    d = SqlDialect()
    out = d.build_percentile(p=exp.Literal.number("0.9"), col_expr=exp.column("amount"))
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "PERCENTILE_CONT" in sql
    assert "WITHIN GROUP" in sql
    assert "0.9" in sql


def test_default_build_percentile_preserves_literal_string() -> None:
    """The p_str spelling is preserved verbatim (``0.50`` stays ``0.50``)."""
    d = SqlDialect()
    out = d.build_percentile(p=exp.Literal.number("0.50"), col_expr=exp.column("amount"))
    sql = out.sql(dialect=d.sqlglot_name)
    assert "0.50" in sql


def test_default_build_percentile_preserves_integer_p() -> None:
    """``p=1`` stays ``1``, not ``1.0``."""
    d = SqlDialect()
    out = d.build_percentile(p=exp.Literal.number("1"), col_expr=exp.column("amount"))
    sql = out.sql(dialect=d.sqlglot_name)
    # Either "1" alone (parenthesised by sqlglot) or "1)" — but never "1.0"
    assert "1.0" not in sql


def test_default_build_percentile_preserves_scientific_notation() -> None:
    """``5e-2`` must remain ``5e-2`` end-to-end."""
    d = SqlDialect()
    out = d.build_percentile(p=exp.Literal.number("5e-2"), col_expr=exp.column("amount"))
    assert "5e-2" in out.sql(dialect=d.sqlglot_name)


# build_stat_agg_1arg / build_covar_2arg — default impl is native


def test_default_build_stat_agg_1arg_emits_canonical_name() -> None:
    d = SqlDialect()
    out = d.build_stat_agg_1arg(agg_name="stddev_samp", col_expr=exp.column("amount"))
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "STDDEV_SAMP" in sql or "STDDEV(" in sql  # Postgres native form


def test_default_build_covar_2arg_emits_native_corr() -> None:
    d = SqlDialect()
    out = d.build_covar_2arg(agg_name="corr", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "CORR" in sql


def test_default_build_covar_2arg_emits_native_covar_samp() -> None:
    d = SqlDialect()
    out = d.build_covar_2arg(agg_name="covar_samp", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "COVAR_SAMP" in sql


# should_use_native_log — default impl


def test_default_should_use_native_log_10_is_true() -> None:
    assert SqlDialect().should_use_native_log(10) is True


def test_default_should_use_native_log_2_is_true() -> None:
    """Defaults to True (Postgres-shaped)."""
    assert SqlDialect().should_use_native_log(2) is True


def test_default_should_use_native_log_other_bases_false() -> None:
    """Only 10 and 2 are special-cased; other bases fall through to the canonical 2-arg ``LOG(base, x)`` form."""
    assert SqlDialect().should_use_native_log(3) is False


# rewrite_parsed_ast — default impl is identity


def test_default_rewrite_parsed_ast_is_identity() -> None:
    d = SqlDialect()
    tree = sqlglot.parse_one("SELECT json_extract(j, '$.k') FROM t", dialect="postgres")
    out = d.rewrite_parsed_ast(tree)
    # Same string in, same string out — base class does nothing
    assert out.sql(dialect="postgres") == tree.sql(dialect="postgres")


# register_udfs — default impl is no-op


def test_default_register_udfs_is_noop_via_spy_connection() -> None:
    """Default ``register_udfs`` is a no-op."""

    class _SpyConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple, dict]] = []

        def __getattr__(self, name: str):
            def _record(*args, **kwargs):
                self.calls.append((name, args, kwargs))
                return None
            return _record

    spy = _SpyConn()
    SqlDialect().register_udfs(spy)
    assert spy.calls == [], (
        f"Default register_udfs must be a no-op; got calls: {spy.calls!r}"
    )


# build_explain_sql — default impl from prefix/postfix fields


def test_default_build_explain_sql_uses_prefix() -> None:
    """Base class defaults to ``"EXPLAIN"`` prefix."""
    assert SqlDialect().build_explain_sql("SELECT 1") == "EXPLAIN SELECT 1"


def test_bigquery_build_explain_sql_raises() -> None:
    """BigQuery has no EXPLAIN, so ``build_explain_sql`` raises ValueError."""
    d = BigqueryDialect()
    with pytest.raises(ValueError, match="EXPLAIN is not supported"):
        d.build_explain_sql("SELECT 1")


# Pydantic v2 frozen instance — defensive


def test_dialect_instances_are_frozen() -> None:
    """Dialect instances are frozen so cached singletons can't drift."""
    d = SqlDialect()
    with pytest.raises((TypeError, ValueError)):
        d.sqlglot_name = "mutated"  # type: ignore[misc]


# Oracle log-native flags (sanity check the base-class field overrides)


def test_oracle_overrides_log10_and_log2_to_false() -> None:
    d = OracleDialect()
    assert d.should_use_native_log(10) is False
    assert d.should_use_native_log(2) is False


# Base emit_outer_wrap (today's derived-table wrap shape)
#
# The base impl IS today's behaviour: wrap ``inner_sql`` in a derived
# table, project the public alias list, re-emit detached
# ORDER/LIMIT/OFFSET on the outer statement. The T-SQL override (in
# ``test_tsql.py``) lifts inner CTEs to the top so T-SQL's
# "WITH only as statement prefix" rule is satisfied; the base impl makes
# no such hoist and emits the existing shape verbatim.


def test_default_emit_outer_wrap_basic_shape() -> None:
    """Base impl: ``SELECT <quoted public> FROM (<inner>) AS _outer``."""
    out = SqlDialect().emit_outer_wrap(
        inner_sql="SELECT 1 AS x",
        public=["x"],
        projected=["x"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    normalised = " ".join(out.split())
    assert normalised.startswith('SELECT "x"'), (
        f"Default outer projection must use ANSI double quotes: {out}"
    )
    assert "AS _outer" in normalised
    # Inner SELECT survives inside the derived-table wrap.
    assert "SELECT 1 AS x" in normalised


def test_default_emit_outer_wrap_preserves_inner_cte_inside_derived_table() -> None:
    """Base impl does NOT hoist inner CTEs."""
    inner = "WITH base AS (SELECT 1 AS x) SELECT x AS y FROM base"
    out = SqlDialect().emit_outer_wrap(
        inner_sql=inner,
        public=["y"],
        projected=["y"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    normalised = " ".join(out.split())
    assert not normalised.startswith("WITH "), (
        f"Base impl must not hoist CTEs: {out}"
    )
    assert "WITH base" in out


def test_default_emit_outer_wrap_with_order() -> None:
    """ORDER BY rides on the outer statement and renders via sqlglot's Postgres dialect quoting."""
    order = sqlglot.parse_one('SELECT 1 ORDER BY "x" ASC', dialect="postgres").args.get("order")
    out = SqlDialect().emit_outer_wrap(
        inner_sql="SELECT 1 AS x",
        public=["x"],
        projected=["x"],
        order=order,
        limit=None,
        offset_arg=None,
    )
    assert "ORDER BY" in out.upper()


def test_default_emit_outer_wrap_strips_inner_qualifiers_in_order_by() -> None:
    """The detached ORDER BY may carry inner-CTE qualifiers (e.g. ``_base."col"``)."""
    order = sqlglot.parse_one(
        'SELECT 1 ORDER BY _base."orders.id" ASC', dialect="postgres"
    ).args.get("order")
    out = SqlDialect().emit_outer_wrap(
        inner_sql="SELECT 1 AS x",
        public=["orders.id"],
        projected=["orders.id"],
        order=order,
        limit=None,
        offset_arg=None,
    )
    assert "_base." not in out, (
        f"Inner-CTE qualifier _base. leaked into outer ORDER BY: {out}"
    )


def test_default_emit_outer_wrap_uses_sqlglot_name_not_dialect_attr() -> None:
    """The base impl renders with ``self.sqlglot_name``."""
    # Calling on bare SqlDialect must succeed — proving the impl reaches
    # for the right attribute name. AttributeError would surface here if
    # the impl referenced ``self.dialect``.
    SqlDialect().emit_outer_wrap(
        inner_sql="SELECT 1 AS x",
        public=["x"],
        projected=["x"],
        order=None,
        limit=None,
        offset_arg=None,
    )


# rewrite_target_ast default is identity (only Postgres overrides).


def test_base_rewrite_target_ast_is_identity_for_round() -> None:
    d = SqlDialect()
    tree = sqlglot.parse_one("ROUND(x, 2)", dialect="postgres")
    before = tree.sql(dialect="postgres")
    after = d.rewrite_target_ast(tree).sql(dialect="postgres")
    assert before == after


def test_duckdb_and_sqlite_rewrite_target_ast_leave_round_uncast() -> None:
    for d in (DuckdbDialect(), SqliteDialect()):
        tree = sqlglot.parse_one("ROUND(x, 2)", dialect="postgres")
        out = d.rewrite_target_ast(tree).sql(dialect=d.sqlglot_name).upper()
        assert "CAST(" not in out


# backslash_escapes_strings — dialect-aware Mode-A {var} escaping

# Pin the expected backslash-escaping regime for every dialect class. This is
# the single source of truth the {var} escaping keys off, DERIVED from
# sqlglot's tokenizer (SqlDialect.backslash_escapes_strings). Freezing the
# expected value here means a sqlglot upgrade that shifts a dialect's string
# escaping fails THIS test loudly for review rather than silently changing how
# {var} values are escaped in generated SQL.
#
# Standard (backslash is an ordinary literal char → only '' doubling):
#   sqlite, postgres, duckdb, tsql, trino, presto, oracle
# Backslash-escaping (a backslash escapes the next char in a string literal):
#   mysql, clickhouse, snowflake, redshift, bigquery, databricks, spark
_BACKSLASH_ESCAPES_PINS = [
    (SqliteDialect(), False),
    (PostgresDialect(), False),
    (DuckdbDialect(), False),
    (TsqlDialect(), False),
    (TrinoDialect(), False),
    (PrestoDialect(), False),
    (OracleDialect(), False),
    (MysqlDialect(), True),
    (ClickhouseDialect(), True),
    (SnowflakeDialect(), True),
    (RedshiftDialect(), True),
    (BigqueryDialect(), True),
    (DatabricksDialect(), True),
    (SparkDialect(), True),
]


@pytest.mark.parametrize(
    "dialect,expected",
    _BACKSLASH_ESCAPES_PINS,
    ids=[d.sqlglot_name for d, _ in _BACKSLASH_ESCAPES_PINS],
)
def test_backslash_escapes_strings_pin(dialect: SqlDialect, expected: bool) -> None:
    assert dialect.backslash_escapes_strings is expected


def test_backslash_escapes_strings_matches_sqlglot_for_all_dialects() -> None:
    """The property agrees with sqlglot's tokenizer table for every dialect."""
    for d in _ALL_DIALECTS:
        expected = "\\" in Dialect.get_or_raise(d.sqlglot_name).tokenizer_class.STRING_ESCAPES
        assert d.backslash_escapes_strings is expected, d.sqlglot_name


def test_backslash_escapes_strings_is_bool() -> None:
    # Contract: a plain bool (not a truthy set/other), so callers can pass it
    # straight into substitute_variables(backslash_escapes=...).
    assert isinstance(MysqlDialect().backslash_escapes_strings, bool)
    assert isinstance(SqliteDialect().backslash_escapes_strings, bool)


def test_backslash_escapes_derivation_guards_missing_string_escapes(monkeypatch) -> None:
    """A reshaped tokenizer STRING_ESCAPES raises a clear RuntimeError."""
    base_mod._sqlglot_backslash_escapes.cache_clear()
    tokenizer_cls = Dialect.get_or_raise("postgres").tokenizer_class
    # Simulate the attribute changing shape (a str is not a collection).
    monkeypatch.setattr(tokenizer_cls, "STRING_ESCAPES", "not-a-collection", raising=False)
    try:
        with pytest.raises(RuntimeError, match="STRING_ESCAPES"):
            base_mod._sqlglot_backslash_escapes("postgres")
    finally:
        base_mod._sqlglot_backslash_escapes.cache_clear()
