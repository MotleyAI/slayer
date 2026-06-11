"""DEV-1542: tests for the SqlDialect base class default impls.

The base class is fully concrete (instantiable) but the registry only
hands out concrete subclasses. Default impls are Postgres-shaped:

* ``DATE_TRUNC('unit', col)`` for ``build_date_trunc``
* ``PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY col)`` for percentile
* native ``CORR``, ``COVAR_SAMP``, ``COVAR_POP`` for the 2-arg stat aggs
* ``EXPLAIN <sql>`` for ``build_explain_sql`` (raises when prefix is None)
* identity for ``rewrite_parsed_ast`` and no-op for ``register_udfs``
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

import pytest

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects._tier2 import OracleDialect
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.bigquery import BigqueryDialect


# ---------------------------------------------------------------------------
# Shared parse helper — mirrors SQLGenerator._parse minus dialect-specific rewrites
# ---------------------------------------------------------------------------


def _parse_default(sql: str) -> exp.Expression:
    """Lookalike of SQLGenerator._parse for the default (postgres) dialect."""
    return sqlglot.parse_one(sql, dialect="postgres")


# ---------------------------------------------------------------------------
# build_date_trunc — default impl
# ---------------------------------------------------------------------------


def test_default_build_date_trunc_month() -> None:
    d = SqlDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_date_trunc(col, TimeGranularity.MONTH, parse=_parse_default)
    sql = out.sql(dialect=d.sqlglot_name)
    assert "DATE_TRUNC" in sql.upper()
    assert "MONTH" in sql.upper()


def test_default_build_date_trunc_casts_non_column_to_timestamp() -> None:
    """Non-column operands get an explicit CAST(... AS TIMESTAMP) so Postgres
    can pick the right ``date_trunc`` overload (preserves today's
    ``generator.py:_build_date_trunc`` behaviour)."""
    d = SqlDialect()
    literal_expr = sqlglot.parse_one("'2025-01-01'", dialect="postgres")
    out = d.build_date_trunc(literal_expr, TimeGranularity.MONTH, parse=_parse_default)
    assert "CAST" in out.sql(dialect=d.sqlglot_name).upper()


def test_default_build_date_trunc_idempotent_on_already_cast() -> None:
    """Already-cast expressions don't get double-wrapped."""
    d = SqlDialect()
    cast_expr = exp.Cast(
        this=sqlglot.parse_one("'2025-01-01'", dialect="postgres"),
        to=exp.DataType.build("TIMESTAMP"),
    )
    out = d.build_date_trunc(cast_expr, TimeGranularity.MONTH, parse=_parse_default)
    sql = out.sql(dialect=d.sqlglot_name)
    # one CAST inside the DATE_TRUNC, not two nested
    assert sql.upper().count("CAST") == 1


# ---------------------------------------------------------------------------
# build_time_offset_expr — default impl uses INTERVAL
# ---------------------------------------------------------------------------


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
    """Quarter→3*month normalization is preserved across every dialect
    (today's ``generator.py:1037``)."""
    d = SqlDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_time_offset_expr(col, offset=1, granularity="quarter")
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "MONTH" in sql
    # The literal value should reflect quarter * 3
    assert "3" in sql


# ---------------------------------------------------------------------------
# duration_interval_exprs / add_intervals_expr — default impl uses INTERVAL nodes
# ---------------------------------------------------------------------------


def test_default_duration_interval_exprs_returns_interval_per_part() -> None:
    """The default impl produces one ``exp.Interval`` per parsed (amount, unit)
    pair — used by ``_add_intervals_expr`` to chain ``col + INTERVAL N UNIT [+...]``."""
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


# ---------------------------------------------------------------------------
# build_median / build_percentile — default impl is PERCENTILE_CONT WITHIN GROUP
# ---------------------------------------------------------------------------


def test_default_build_median_uses_percentile_cont() -> None:
    d = SqlDialect()
    inner = sqlglot.parse_one("amount", dialect="postgres")
    out = d.build_median(inner, parse=_parse_default)
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "PERCENTILE_CONT" in sql
    assert "WITHIN GROUP" in sql
    assert "0.5" in sql


def test_default_build_percentile_uses_percentile_cont() -> None:
    d = SqlDialect()
    out = d.build_percentile("0.9", "amount", parse=_parse_default)
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "PERCENTILE_CONT" in sql
    assert "WITHIN GROUP" in sql
    assert "0.9" in sql


def test_default_build_percentile_preserves_literal_string() -> None:
    """The original p_str spelling must be preserved verbatim — passing it
    as a float would normalize ``0.50`` to ``0.5``."""
    d = SqlDialect()
    out = d.build_percentile("0.50", "amount", parse=_parse_default)
    sql = out.sql(dialect=d.sqlglot_name)
    assert "0.50" in sql


def test_default_build_percentile_preserves_integer_p() -> None:
    """``p=1`` stays ``1``, not ``1.0``."""
    d = SqlDialect()
    out = d.build_percentile("1", "amount", parse=_parse_default)
    sql = out.sql(dialect=d.sqlglot_name)
    # Either "1" alone (parenthesised by sqlglot) or "1)" — but never "1.0"
    assert "1.0" not in sql


def test_default_build_percentile_preserves_scientific_notation() -> None:
    """``5e-2`` must remain ``5e-2`` end-to-end. Float conversion would
    flatten this to ``0.05``."""
    d = SqlDialect()
    out = d.build_percentile("5e-2", "amount", parse=_parse_default)
    assert "5e-2" in out.sql(dialect=d.sqlglot_name)


# ---------------------------------------------------------------------------
# build_stat_agg_1arg / build_covar_2arg — default impl is native
# ---------------------------------------------------------------------------


def test_default_build_stat_agg_1arg_emits_canonical_name() -> None:
    d = SqlDialect()
    out = d.build_stat_agg_1arg("stddev_samp", "amount", parse=_parse_default)
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "STDDEV_SAMP" in sql or "STDDEV(" in sql  # Postgres native form


def test_default_build_covar_2arg_emits_native_corr() -> None:
    d = SqlDialect()
    out = d.build_covar_2arg("corr", "amount", "quantity", parse=_parse_default)
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "CORR" in sql


def test_default_build_covar_2arg_emits_native_covar_samp() -> None:
    d = SqlDialect()
    out = d.build_covar_2arg("covar_samp", "amount", "quantity", parse=_parse_default)
    sql = out.sql(dialect=d.sqlglot_name).upper()
    assert "COVAR_SAMP" in sql


# ---------------------------------------------------------------------------
# should_use_native_log — default impl
# ---------------------------------------------------------------------------


def test_default_should_use_native_log_10_is_true() -> None:
    assert SqlDialect().should_use_native_log(10) is True


def test_default_should_use_native_log_2_is_true() -> None:
    """Defaults to True (Postgres-shaped). Tier-1 dialects that lack native
    LOG2 (Oracle, T-SQL) override to False; Tier-2 dialects override via
    the ``log2_native`` field."""
    assert SqlDialect().should_use_native_log(2) is True


def test_default_should_use_native_log_other_bases_false() -> None:
    """Only 10 and 2 are special-cased; other bases fall through to the
    canonical 2-arg ``LOG(base, x)`` form."""
    assert SqlDialect().should_use_native_log(3) is False


# ---------------------------------------------------------------------------
# rewrite_parsed_ast — default impl is identity
# ---------------------------------------------------------------------------


def test_default_rewrite_parsed_ast_is_identity() -> None:
    d = SqlDialect()
    tree = sqlglot.parse_one("SELECT json_extract(j, '$.k') FROM t", dialect="postgres")
    out = d.rewrite_parsed_ast(tree)
    # Same string in, same string out — base class does nothing
    assert out.sql(dialect="postgres") == tree.sql(dialect="postgres")


# ---------------------------------------------------------------------------
# register_udfs — default impl is no-op
# ---------------------------------------------------------------------------


def test_default_register_udfs_is_noop_via_spy_connection() -> None:
    """Default impl is a no-op so ``SqlDialect.register_udfs(conn)`` can be
    called unconditionally from a SQLAlchemy connect hook without
    dispatching on dialect type.

    Use a spy object that records every method call — proves no
    create_function / create_aggregate (or anything else) is invoked on
    the connection. Robust against future SQLite builds that ship native
    median/percentile (Codex finding #4)."""

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


# ---------------------------------------------------------------------------
# build_explain_sql — default impl from prefix/postfix fields
# ---------------------------------------------------------------------------


def test_default_build_explain_sql_uses_prefix() -> None:
    """Base class defaults to ``"EXPLAIN"`` prefix."""
    assert SqlDialect().build_explain_sql("SELECT 1") == "EXPLAIN SELECT 1"


def test_bigquery_build_explain_sql_raises() -> None:
    """BigQuery has ``explain_prefix = None`` — call must raise ValueError
    so callers get a clean explain-unsupported error (preserves today's
    ``query_engine.py:132`` behaviour)."""
    with pytest.raises(ValueError, match="EXPLAIN is not supported"):
        BigqueryDialect().build_explain_sql("SELECT 1")


# ---------------------------------------------------------------------------
# Pydantic v2 frozen instance — defensive
# ---------------------------------------------------------------------------


def test_dialect_instances_are_frozen() -> None:
    """The base class declares ``frozen=True``; instances must reject
    field mutation so cached singletons can't drift."""
    d = SqlDialect()
    with pytest.raises((TypeError, ValueError)):
        d.sqlglot_name = "mutated"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Oracle log-native flags (sanity check the base-class field overrides)
# ---------------------------------------------------------------------------


def test_oracle_overrides_log10_and_log2_to_false() -> None:
    d = OracleDialect()
    assert d.should_use_native_log(10) is False
    assert d.should_use_native_log(2) is False
