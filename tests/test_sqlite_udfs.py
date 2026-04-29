"""Unit tests for the Python aggregate UDFs registered on SQLite connections."""

from __future__ import annotations

import sqlite3
import statistics

import numpy as np
import pytest

from slayer.sql.sqlite_udfs import (
    _MedianAgg,
    _PercentileContAgg,
    _PercentileDiscAgg,
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
