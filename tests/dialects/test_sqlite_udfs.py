"""Unit tests for the Python aggregate UDFs registered on SQLite connections."""

from __future__ import annotations

import math
import random
import sqlite3
import statistics
import sys

import numpy as np
import pytest

from slayer.sql.dialects.sqlite import (
    _CorrAgg,
    _CovarPopAgg,
    _CovarSampAgg,
    _MedianAgg,
    _PercentileContAgg,
    _PercentileDiscAgg,
    _pow,
    _StddevPopAgg,
    _StddevSampAgg,
    _VarPopAgg,
    _VarSampAgg,
    register_sqlite_udfs,
)


# ---------------------------------------------------------------------------
# Median
# ---------------------------------------------------------------------------


def _run_agg(agg_cls, values, *, p=None):
    """Drive an aggregate class through step()/finalize() like SQLite would."""
    agg = agg_cls()
    for v in values:
        if p is None:
            agg.step(v)
        else:
            agg.step(v, p)
    return agg.finalize()


def test_median_agg_odd():
    assert _run_agg(_MedianAgg, [1, 2, 3, 4, 5]) == 3


def test_median_agg_even():
    assert _run_agg(_MedianAgg, [1, 2, 3, 4]) == 2.5


def test_median_agg_empty():
    assert _MedianAgg().finalize() is None


def test_median_agg_skips_nulls():
    # Nulls should be ignored, not counted toward N.
    assert _run_agg(_MedianAgg, [None, 1, None, 2, None, 3]) == 2


def test_median_agg_unsorted_input():
    # Input order must not matter.
    assert _run_agg(_MedianAgg, [5, 1, 4, 2, 3]) == 3


# ---------------------------------------------------------------------------
# percentile_cont
# ---------------------------------------------------------------------------


def test_percentile_cont_endpoints():
    assert _run_agg(_PercentileContAgg, [1, 2, 3, 4, 5], p=0.0) == 1
    assert _run_agg(_PercentileContAgg, [1, 2, 3, 4, 5], p=1.0) == 5


def test_percentile_cont_median_matches_statistics_median():
    vals = [10, 20, 30, 40, 50]
    assert _run_agg(_PercentileContAgg, vals, p=0.5) == statistics.median(vals)


def test_percentile_cont_interpolates():
    # Linear interpolation: with [1,2,3,4], p=0.25 -> rank=0.75 -> 1 + 0.75*(2-1) = 1.75
    assert _run_agg(_PercentileContAgg, [1, 2, 3, 4], p=0.25) == pytest.approx(1.75)


def test_percentile_cont_matches_numpy_linear():
    rng = np.random.default_rng(seed=42)
    vals = rng.uniform(0, 100, size=200).tolist()
    for p in (0.05, 0.25, 0.5, 0.75, 0.95):
        got = _run_agg(_PercentileContAgg, vals, p=p)
        # numpy "linear" method matches Postgres PERCENTILE_CONT semantics.
        assert got == pytest.approx(np.percentile(vals, p * 100, method="linear"))


def test_percentile_cont_empty():
    assert _run_agg(_PercentileContAgg, [], p=0.5) is None


def test_percentile_cont_skips_nulls():
    assert _run_agg(_PercentileContAgg, [None, 1, 2, None, 3], p=0.5) == 2


def test_percentile_cont_invalid_p():
    agg = _PercentileContAgg()
    with pytest.raises(ValueError, match=r"percentile p must be in \[0, 1\]"):
        agg.step(1, 1.5)


def test_percentile_cont_single_value():
    assert _run_agg(_PercentileContAgg, [42], p=0.5) == 42


# ---------------------------------------------------------------------------
# percentile_disc
# ---------------------------------------------------------------------------


def test_percentile_disc_quartiles():
    # PERCENTILE_DISC: smallest v with cume_dist(v) >= p.
    # For [1,2,3,4]: cume_dist values are 0.25, 0.5, 0.75, 1.0
    vals = [1, 2, 3, 4]
    assert _run_agg(_PercentileDiscAgg, vals, p=0.25) == 1
    assert _run_agg(_PercentileDiscAgg, vals, p=0.5) == 2
    assert _run_agg(_PercentileDiscAgg, vals, p=0.75) == 3
    assert _run_agg(_PercentileDiscAgg, vals, p=1.0) == 4


def test_percentile_disc_endpoints():
    assert _run_agg(_PercentileDiscAgg, [10, 20, 30], p=0.0) == 10
    assert _run_agg(_PercentileDiscAgg, [10, 20, 30], p=1.0) == 30


def test_percentile_disc_invalid_p():
    agg = _PercentileDiscAgg()
    with pytest.raises(ValueError):
        agg.step(1, -0.1)


def test_percentile_disc_empty():
    assert _run_agg(_PercentileDiscAgg, [], p=0.5) is None


# ---------------------------------------------------------------------------
# register_sqlite_udfs against a real sqlite3 connection
# ---------------------------------------------------------------------------


def test_register_sqlite_udfs_exposes_all_three():
    conn = sqlite3.connect(":memory:")
    register_sqlite_udfs(conn)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (x REAL)")
    cur.executemany("INSERT INTO t VALUES (?)", [(v,) for v in [1, 2, 3, 4, 5]])

    assert cur.execute("SELECT median(x) FROM t").fetchone()[0] == 3
    assert cur.execute("SELECT percentile_cont(x, 0.5) FROM t").fetchone()[0] == 3
    assert cur.execute("SELECT percentile_disc(x, 0.5) FROM t").fetchone()[0] == 3
    conn.close()


def test_register_sqlite_udfs_idempotent():
    # Calling register twice on the same connection must not error
    # (sqlite3 lets the second call replace the first).
    conn = sqlite3.connect(":memory:")
    register_sqlite_udfs(conn)
    register_sqlite_udfs(conn)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (x REAL)")
    cur.execute("INSERT INTO t VALUES (10)")
    assert cur.execute("SELECT median(x) FROM t").fetchone()[0] == 10
    conn.close()


def test_register_sqlite_udfs_per_group():
    # Catch UDF state-leak bugs: median per GROUP BY must restart per group.
    conn = sqlite3.connect(":memory:")
    register_sqlite_udfs(conn)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (g TEXT, x REAL)")
    cur.executemany(
        "INSERT INTO t VALUES (?, ?)",
        [("a", 1), ("a", 2), ("a", 3), ("b", 10), ("b", 20)],
    )
    rows = dict(cur.execute("SELECT g, median(x) FROM t GROUP BY g").fetchall())
    assert rows == {"a": 2, "b": 15}
    conn.close()


# ---------------------------------------------------------------------------
# Scalar math UDFs (DEV-1317)
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_conn():
    conn = sqlite3.connect(":memory:")
    register_sqlite_udfs(conn)
    yield conn
    conn.close()


def _scalar(conn, sql, *params):
    return conn.execute(f"SELECT {sql}", params).fetchone()[0]


# --- ln --------------------------------------------------------------------


def test_ln_known_value(sqlite_conn):
    assert _scalar(sqlite_conn, "ln(?)", math.e) == pytest.approx(1.0)


def test_ln_null_input_returns_null(sqlite_conn):
    assert _scalar(sqlite_conn, "ln(NULL)") is None


def test_ln_zero_raises(sqlite_conn):
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT ln(0)").fetchone()


def test_ln_negative_raises(sqlite_conn):
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT ln(-1)").fetchone()


# --- log10 -----------------------------------------------------------------


def test_log10_known_value(sqlite_conn):
    assert _scalar(sqlite_conn, "log10(?)", 1000) == pytest.approx(3.0)


def test_log10_null_input_returns_null(sqlite_conn):
    assert _scalar(sqlite_conn, "log10(NULL)") is None


def test_log10_zero_raises(sqlite_conn):
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT log10(0)").fetchone()


# --- log2 ------------------------------------------------------------------
# DEV-1337: registered alongside log10 so the SQL generator can render a
# user-written `log2(x)` formula verbatim on SQLite (the rewrite from
# `LOG(2, x)` → `log2(x)` would otherwise hit a missing-function error).


def test_log2_known_value(sqlite_conn):
    assert _scalar(sqlite_conn, "log2(?)", 8) == pytest.approx(3.0)


def test_log2_null_input_returns_null(sqlite_conn):
    assert _scalar(sqlite_conn, "log2(NULL)") is None


def test_log2_zero_raises(sqlite_conn):
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT log2(0)").fetchone()


def test_log2_negative_raises(sqlite_conn):
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT log2(-1)").fetchone()


# --- log(B, X) -------------------------------------------------------------
# Argument order: B first, X second. Returns log_B(X). Matches SQLite >=3.35
# built-in `log(B,X)` and Postgres `LOG(b, x)`.


def test_log_base_x_known_value_via_udf(sqlite_conn):
    # NOSONAR(S125) — mathematical equation in prose ("log base 10 of 1000
    # equals 3"), not commented-out Python. Sonar's python:S7632 rule
    # requires alphanumeric-only rule keys inside the suppression
    # parentheses, so the prefixed `python:S125` form is invalid; the
    # bare `S125` short name is the compliant one.
    # Post-C3 (PR #82): the UDF registers unconditionally, overriding any
    # built-in `log(B, X)` on SQLite >=3.35 with our strict-error semantics.
    assert _scalar(sqlite_conn, "log(10, 1000)") == pytest.approx(3.0)


def test_log_base_x_contract_holds(sqlite_conn):
    """Contract-level check: `log(B, X) == log_B(X)`. The UDF registers
    unconditionally now (post-C3 in commit `cce6d2e`), so this test
    no longer needs to disambiguate UDF-vs-builtin paths — it just
    pins the user-facing contract.
    """
    assert _scalar(sqlite_conn, "log(2, 8)") == pytest.approx(3.0)
    assert _scalar(sqlite_conn, "log(10, 1000)") == pytest.approx(3.0)


def test_log_b_x_null_propagation(sqlite_conn):
    # When the UDF is in play, NULL on either arg returns NULL.
    # When SQLite's built-in is used (>=3.35), it also returns NULL on NULL.
    assert _scalar(sqlite_conn, "log(NULL, 10)") is None
    assert _scalar(sqlite_conn, "log(10, NULL)") is None


# --- exp -------------------------------------------------------------------


def test_exp_known_value(sqlite_conn):
    assert _scalar(sqlite_conn, "exp(1)") == pytest.approx(math.e)


def test_exp_zero(sqlite_conn):
    assert _scalar(sqlite_conn, "exp(0)") == pytest.approx(1.0)


def test_exp_null_input_returns_null(sqlite_conn):
    assert _scalar(sqlite_conn, "exp(NULL)") is None


# --- sqrt ------------------------------------------------------------------


def test_sqrt_known_value(sqlite_conn):
    assert _scalar(sqlite_conn, "sqrt(4)") == pytest.approx(2.0)
    assert _scalar(sqlite_conn, "sqrt(2)") == pytest.approx(math.sqrt(2))


def test_sqrt_zero(sqlite_conn):
    assert _scalar(sqlite_conn, "sqrt(0)") == 0


def test_sqrt_null_input_returns_null(sqlite_conn):
    assert _scalar(sqlite_conn, "sqrt(NULL)") is None


def test_sqrt_negative_raises(sqlite_conn):
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT sqrt(-1)").fetchone()


# --- pow / power -----------------------------------------------------------


def test_pow_known_value(sqlite_conn):
    assert _scalar(sqlite_conn, "pow(2, 10)") == pytest.approx(1024.0)
    # 0**0 = 1 by Python convention, matches Postgres.
    assert _scalar(sqlite_conn, "pow(0, 0)") == 1


def test_pow_null_propagation(sqlite_conn):
    assert _scalar(sqlite_conn, "pow(NULL, 2)") is None
    assert _scalar(sqlite_conn, "pow(2, NULL)") is None


def test_pow_zero_to_negative_raises(sqlite_conn):
    # 0 ** -1 = ZeroDivisionError in Python; surfaces as OperationalError.
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT pow(0, -1)").fetchone()


def test_power_alias_known_value(sqlite_conn):
    # `power` is a registered alias for `pow` so cross-dialect SQL that
    # emits POWER(x, n) (e.g. originating from Postgres/MySQL) works.
    assert _scalar(sqlite_conn, "power(2, 10)") == pytest.approx(1024.0)


def test_power_alias_null_propagation(sqlite_conn):
    assert _scalar(sqlite_conn, "power(NULL, 2)") is None


def test_pow_negative_base_fractional_exponent_raises(sqlite_conn):
    """`pow(-2, 0.5)` is undefined in the reals. With Python's ``**`` it
    silently returns a complex number, which sqlite3 then errors on at
    marshalling time (clobbering the function's role boundary). With
    ``math.pow`` it raises ValueError up-front, surfacing as
    OperationalError — same shape as ln(0)/sqrt(-1) and the rest of the
    math-domain-error policy. CodeRabbit major on PR #82 round 3.
    """
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT pow(-2, 0.5)").fetchone()
    # `power` alias goes through the same wrapper.
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT power(-2, 0.5)").fetchone()


def test_pow_huge_exponent_overflows_cleanly():
    """`pow(2, 10000)` with Python's ``**`` builds a 3010-digit Python
    int — unbounded memory pressure on the SQLite-Python boundary.
    With ``math.pow`` it overflows IEEE-754 cleanly and raises
    ``OverflowError``. We test the wrapper directly here because the
    sqlite3 driver swallows the underlying exception and surfaces
    everything as ``DataError`` with a generic "string or blob too big"
    message, hiding the distinction between the bug and the fix at the
    SQL boundary. The Python-level invariant is the load-bearing one.
    """
    with pytest.raises(OverflowError):
        _pow(2, 10000)
    # `pow(2, 1000)` is large but does NOT overflow IEEE-754 (≈ 1.07e301).
    # math.pow returns a bounded float; ** would still build a 302-digit
    # int. Pin the float type to lock in the bounded-output guarantee.
    assert isinstance(_pow(2, 1000), float)


# ---------------------------------------------------------------------------
# `log` collision: register UDF only when SQLite < 3.35
# ---------------------------------------------------------------------------


def test_log_udf_registered_unconditionally():
    """`register_sqlite_udfs` must register the `log(B, X)` UDF on every
    SQLite version. Earlier draft skipped registration on >=3.35 to avoid
    clobbering SQLite's built-in `log(B, X)`, but that produced a
    version-dependent semantic split — built-in `log(0, 10)` returns
    NULL silently, our UDF raises `OperationalError`, contradicting the
    "match Postgres exactly — math errors propagate" promise from
    DEV-1317. Codex review #2 on PR #82.

    The UDF and the built-in have the same arg order (B first, X second)
    and the UDF's strict-error semantics is exactly what we want, so
    registering unconditionally just upgrades the built-in's behaviour
    to match.
    """
    # Spy on what gets registered. Production code no longer branches on
    # `sqlite_version_info`, so a single call against a spy connection
    # is sufficient — we just confirm `log` always appears.
    registered: list[tuple[str, int]] = []

    class _SpyConn:
        def create_function(self, name, narg, fn):  # noqa: ARG002
            registered.append((name, narg))

        def create_aggregate(self, name, narg, cls):  # noqa: ARG002
            pass

    register_sqlite_udfs(_SpyConn())
    names = {n for n, _ in registered}
    assert "log" in names, (
        f"Expected `log` UDF to register on every SQLite version (not "
        f"just <3.35) so strict-error semantics are uniform; got {names}"
    )
    assert "ln" in names
    assert "log10" in names
    # DEV-1337: log2 must register so the SQL generator can emit a
    # user-written `log2(x)` formula verbatim on SQLite.
    assert "log2" in names


def test_log_zero_raises_uniformly(sqlite_conn):
    """Whether SQLite's linked version is <3.35 or >=3.35, `log(B, 0)` must
    raise `OperationalError` — the strict-Postgres semantics promised by
    DEV-1317. The UDF override (registered unconditionally) is what makes
    this hold even when the built-in would silently return NULL.
    """
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT log(10, 0)").fetchone()
    with pytest.raises(sqlite3.OperationalError):
        sqlite_conn.execute("SELECT log(10, -1)").fetchone()


# ---------------------------------------------------------------------------
# Aggregate stat UDFs (DEV-1317)
# ---------------------------------------------------------------------------

# Fixed series for cross-checking against Python's `statistics` module.
_SERIES = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]


# --- stddev_samp / var_samp -----------------------------------------------


def test_stddev_samp_matches_statistics_stdev():
    got = _run_agg(_StddevSampAgg, _SERIES)
    assert got == pytest.approx(statistics.stdev(_SERIES), rel=1e-9)


def test_stddev_samp_n_zero_returns_null():
    assert _StddevSampAgg().finalize() is None


def test_stddev_samp_n_one_returns_null():
    # Bessel-corrected denominator (N-1) is undefined at N=1: must return NULL,
    # matching Postgres STDDEV_SAMP semantics.
    assert _run_agg(_StddevSampAgg, [42.0]) is None


def test_stddev_samp_skips_nulls():
    assert _run_agg(_StddevSampAgg, [None, *_SERIES, None]) == pytest.approx(
        statistics.stdev(_SERIES), rel=1e-9
    )


def test_var_samp_matches_statistics_variance():
    got = _run_agg(_VarSampAgg, _SERIES)
    assert got == pytest.approx(statistics.variance(_SERIES), rel=1e-9)


def test_var_samp_n_zero_returns_null():
    assert _VarSampAgg().finalize() is None


def test_var_samp_n_one_returns_null():
    assert _run_agg(_VarSampAgg, [42.0]) is None


# --- stddev_pop / var_pop --------------------------------------------------


def test_stddev_pop_matches_statistics_pstdev():
    got = _run_agg(_StddevPopAgg, _SERIES)
    assert got == pytest.approx(statistics.pstdev(_SERIES), rel=1e-9)


def test_stddev_pop_n_zero_returns_null():
    # Postgres: STDDEV_POP returns NULL on empty input (no rows).
    assert _StddevPopAgg().finalize() is None


def test_stddev_pop_n_one_returns_zero():
    # Postgres: STDDEV_POP at N=1 is 0 (population SD of a single sample is 0).
    assert _run_agg(_StddevPopAgg, [42.0]) == 0


def test_var_pop_matches_statistics_pvariance():
    got = _run_agg(_VarPopAgg, _SERIES)
    assert got == pytest.approx(statistics.pvariance(_SERIES), rel=1e-9)


def test_var_pop_n_zero_returns_null():
    assert _VarPopAgg().finalize() is None


def test_var_pop_n_one_returns_zero():
    assert _run_agg(_VarPopAgg, [42.0]) == 0


# --- corr (2-arg) ----------------------------------------------------------


def _run_corr(pairs):
    """Drive _CorrAgg with a list of (x, y) tuples (either may be None)."""
    agg = _CorrAgg()
    for x, y in pairs:
        agg.step(x, y)
    return agg.finalize()


def test_corr_matches_statistics_correlation():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    ys = [2.0, 4.1, 5.9, 8.0, 10.2, 11.8]
    got = _run_corr(list(zip(xs, ys)))
    assert got == pytest.approx(statistics.correlation(xs, ys), rel=1e-9)


def test_corr_perfect_positive_is_one():
    pairs = [(i, 2 * i + 3) for i in range(1, 6)]
    assert _run_corr(pairs) == pytest.approx(1.0, abs=1e-9)


def test_corr_perfect_negative_is_minus_one():
    pairs = [(i, -3 * i + 7) for i in range(1, 6)]
    assert _run_corr(pairs) == pytest.approx(-1.0, abs=1e-9)


def test_corr_constant_x_returns_null():
    # Var(x) = 0 → correlation undefined → NULL (matches Postgres).
    pairs = [(5.0, y) for y in [1.0, 2.0, 3.0, 4.0]]
    assert _run_corr(pairs) is None


def test_corr_constant_y_returns_null():
    pairs = [(x, 5.0) for x in [1.0, 2.0, 3.0, 4.0]]
    assert _run_corr(pairs) is None


def test_corr_fewer_than_two_pairs_returns_null():
    assert _CorrAgg().finalize() is None
    assert _run_corr([(1.0, 2.0)]) is None


def test_corr_skips_pair_with_either_null():
    # Pair where x or y is NULL must be dropped entirely.
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    ys = [2.0, 4.0, 6.0, 8.0, 10.0]
    expected = statistics.correlation(xs, ys)
    pairs = list(zip(xs, ys)) + [(None, 99.0), (99.0, None), (None, None)]
    assert _run_corr(pairs) == pytest.approx(expected, rel=1e-9)


def test_corr_all_null_returns_null():
    pairs = [(None, None), (None, 1.0), (2.0, None)]
    assert _run_corr(pairs) is None


# --- covar_samp / covar_pop (2-arg, same shape as corr) -------------------


def _run_covar(agg_cls, pairs):
    agg = agg_cls()
    for x, y in pairs:
        agg.step(x, y)
    return agg.finalize()


def _python_covar_samp(xs, ys):
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)


def _python_covar_pop(xs, ys):
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n


def test_covar_samp_known_value():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    ys = [2.0, 4.1, 5.9, 8.0, 10.2, 11.8]
    got = _run_covar(_CovarSampAgg, list(zip(xs, ys)))
    assert got == pytest.approx(_python_covar_samp(xs, ys), rel=1e-9)


def test_covar_samp_n_zero_returns_null():
    assert _CovarSampAgg().finalize() is None


def test_covar_samp_n_one_returns_null():
    # Bessel-corrected (N-1) is undefined at N=1: NULL, matching Postgres.
    assert _run_covar(_CovarSampAgg, [(1.0, 2.0)]) is None


def test_covar_samp_skips_pair_with_either_null():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    ys = [2.0, 4.0, 6.0, 8.0, 10.0]
    expected = _python_covar_samp(xs, ys)
    pairs = list(zip(xs, ys)) + [(None, 99.0), (99.0, None), (None, None)]
    assert _run_covar(_CovarSampAgg, pairs) == pytest.approx(expected, rel=1e-9)


def test_covar_samp_constant_columns_returns_zero():
    # Unlike corr, covariance is well-defined when one (or both) sides are
    # constant — it just returns 0.
    pairs = [(5.0, y) for y in [1.0, 2.0, 3.0, 4.0]]
    assert _run_covar(_CovarSampAgg, pairs) == pytest.approx(0.0, abs=1e-12)


def test_covar_pop_known_value():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    ys = [2.0, 4.1, 5.9, 8.0, 10.2, 11.8]
    got = _run_covar(_CovarPopAgg, list(zip(xs, ys)))
    assert got == pytest.approx(_python_covar_pop(xs, ys), rel=1e-9)


def test_covar_pop_n_zero_returns_null():
    assert _CovarPopAgg().finalize() is None


def test_covar_pop_n_one_returns_zero():
    # Population covariance at N=1 is 0 (not NULL): single-point spread is 0.
    assert _run_covar(_CovarPopAgg, [(42.0, 7.0)]) == 0


def test_covar_pop_constant_columns_returns_zero():
    pairs = [(5.0, y) for y in [1.0, 2.0, 3.0, 4.0]]
    assert _run_covar(_CovarPopAgg, pairs) == pytest.approx(0.0, abs=1e-12)


def _agg_internal_size(agg) -> int:
    """Approximate the in-memory size of an aggregator's *retained per-row*
    state — i.e. anything in __dict__ that scales with the number of rows
    fed through ``step``. With list-buffering this grows linearly in N;
    with Welford accumulators it stays constant. Walks one level into
    list/tuple values via ``sys.getsizeof`` to capture buffered rows.
    """
    total = sys.getsizeof(agg)
    for v in agg.__dict__.values():
        total += sys.getsizeof(v)
    return total


def _feed_random_floats(agg, n: int, *, seed: int = 0) -> None:
    """Stream `n` random floats through an aggregator's ``step``."""
    rng = random.Random(seed)
    for _ in range(n):
        agg.step(rng.random())


def _feed_random_pairs(agg, n: int, *, seed: int = 0) -> None:
    rng = random.Random(seed)
    for _ in range(n):
        agg.step(rng.random(), rng.random())


@pytest.mark.parametrize(
    "agg_cls",
    [_StddevSampAgg, _StddevPopAgg, _VarSampAgg, _VarPopAgg],
)
def test_one_arg_stat_agg_uses_constant_memory(agg_cls):
    """Each 1-arg stat aggregator must hold O(1) state — a count, a mean,
    and an M2 (sum of squared deviations) is enough for both stddev and
    variance variants. List-buffering would make memory grow with N and
    blow up SQLite analytics workloads on large groups (CodeRabbit major
    on PR #82 round 3). 100k rows shouldn't bloat the aggregator past a
    few hundred bytes.
    """
    agg = agg_cls()
    _feed_random_floats(agg, 100_000)
    size = _agg_internal_size(agg)
    assert size < 1024, (
        f"{agg_cls.__name__} retained {size} bytes after 100k rows — "
        f"expected constant-memory Welford state, not a buffered list. "
        f"State: {agg.__dict__!r}"
    )


@pytest.mark.parametrize(
    "agg_cls",
    [_CorrAgg, _CovarSampAgg, _CovarPopAgg],
)
def test_two_arg_stat_agg_uses_constant_memory(agg_cls):
    """Same constant-memory invariant for the paired aggregates. Welford
    keeps `(n, mean_x, mean_y, M2x, M2y, C)` — six scalars regardless of
    N. List-of-pairs buffering would explode at SQLite analytics scale.
    """
    agg = agg_cls()
    _feed_random_pairs(agg, 100_000)
    size = _agg_internal_size(agg)
    assert size < 1024, (
        f"{agg_cls.__name__} retained {size} bytes after 100k pairs — "
        f"expected constant-memory paired-Welford state. "
        f"State: {agg.__dict__!r}"
    )


def test_covar_skips_pair_with_either_null_pop():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    ys = [2.0, 4.0, 6.0, 8.0, 10.0]
    expected = _python_covar_pop(xs, ys)
    pairs = list(zip(xs, ys)) + [(None, 99.0), (99.0, None)]
    assert _run_covar(_CovarPopAgg, pairs) == pytest.approx(expected, rel=1e-9)


# ---------------------------------------------------------------------------
# Real-connection coverage for the new aggregates
# ---------------------------------------------------------------------------


def test_register_sqlite_udfs_exposes_stat_aggregates(sqlite_conn):
    """End-to-end: every new aggregate is callable via SELECT, including
    sqlglot-rewritten aliases (VARIANCE/VARIANCE_POP) so generator output
    that goes through sqlglot still finds a matching UDF on SQLite.
    """
    cur = sqlite_conn.cursor()
    cur.execute("CREATE TABLE t (x REAL, y REAL)")
    cur.executemany(
        "INSERT INTO t VALUES (?, ?)",
        [(1, 2), (2, 4), (3, 6), (4, 8), (5, 10)],
    )

    # Canonical Postgres-style names.
    assert cur.execute("SELECT stddev_samp(x) FROM t").fetchone()[0] == pytest.approx(
        statistics.stdev([1, 2, 3, 4, 5]), rel=1e-9
    )
    assert cur.execute("SELECT stddev_pop(x) FROM t").fetchone()[0] == pytest.approx(
        statistics.pstdev([1, 2, 3, 4, 5]), rel=1e-9
    )
    assert cur.execute("SELECT var_samp(x) FROM t").fetchone()[0] == pytest.approx(
        statistics.variance([1, 2, 3, 4, 5]), rel=1e-9
    )
    assert cur.execute("SELECT var_pop(x) FROM t").fetchone()[0] == pytest.approx(
        statistics.pvariance([1, 2, 3, 4, 5]), rel=1e-9
    )

    # sqlglot rewrites `var_samp(x)` → `VARIANCE(x)` on SQLite/DuckDB/MySQL,
    # and `var_pop(x)` → `VARIANCE_POP(x)` on SQLite/MySQL, so the SQLite
    # UDF registration must alias these names to keep generator output
    # working. SQLite UDF lookup is case-insensitive.
    assert cur.execute("SELECT VARIANCE(x) FROM t").fetchone()[0] == pytest.approx(
        statistics.variance([1, 2, 3, 4, 5]), rel=1e-9
    )
    assert cur.execute("SELECT VARIANCE_POP(x) FROM t").fetchone()[0] == pytest.approx(
        statistics.pvariance([1, 2, 3, 4, 5]), rel=1e-9
    )

    # corr.
    assert cur.execute("SELECT corr(x, y) FROM t").fetchone()[0] == pytest.approx(
        1.0, abs=1e-9
    )

    # covar_samp / covar_pop.
    xs_l = [1, 2, 3, 4, 5]
    ys_l = [2, 4, 6, 8, 10]
    assert cur.execute("SELECT covar_samp(x, y) FROM t").fetchone()[0] == pytest.approx(
        _python_covar_samp(xs_l, ys_l), rel=1e-9
    )
    assert cur.execute("SELECT covar_pop(x, y) FROM t").fetchone()[0] == pytest.approx(
        _python_covar_pop(xs_l, ys_l), rel=1e-9
    )


def test_register_sqlite_udfs_stat_per_group(sqlite_conn):
    """Catch state-leak bugs across GROUP BY for each new aggregate."""
    cur = sqlite_conn.cursor()
    cur.execute("CREATE TABLE t (g TEXT, x REAL)")
    cur.executemany(
        "INSERT INTO t VALUES (?, ?)",
        [("a", 1), ("a", 2), ("a", 3), ("b", 10), ("b", 20)],
    )
    rows = dict(
        cur.execute("SELECT g, stddev_samp(x) FROM t GROUP BY g").fetchall()
    )
    assert rows["a"] == pytest.approx(statistics.stdev([1, 2, 3]), rel=1e-9)
    assert rows["b"] == pytest.approx(statistics.stdev([10, 20]), rel=1e-9)


def test_register_sqlite_udfs_idempotent_for_new_aggregates(sqlite_conn):
    register_sqlite_udfs(sqlite_conn)  # second registration on same conn
    cur = sqlite_conn.cursor()
    cur.execute("CREATE TABLE t (x REAL)")
    cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
    assert cur.execute("SELECT var_pop(x) FROM t").fetchone()[0] == pytest.approx(
        statistics.pvariance([1, 2, 3]), rel=1e-9
    )
