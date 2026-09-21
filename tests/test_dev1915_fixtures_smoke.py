"""Oracle smoke test — re-derive every DEV-1915 windowed expectation from the raw
rows in pure Python, so the executed-value tests assert against a checked oracle.

Runs green on the current tree (no SLayer execution); the interval membership
itself (which rows fall in each trailing window) is what the executed tests pin.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from statistics import mean, median, pstdev, pvariance, stdev

import pytest

from tests._dev1915_fixtures import (
    AVG_90D,
    CM_COUNT_1Y,
    CM_LAST_1Y,
    CORR_90D,
    COUNT_90D,
    COUNT_BY_REGION_90D,
    COUNT_DISTINCT_90D,
    COVAR_SAMP_90D,
    DISC_SPEND_1Y,
    FIRST_90D,
    LAST_90D,
    LAST_UPDATED_90D,
    MAX_90D,
    MEDIAN_90D,
    MEMBERS_90D,
    MEMBERS_CUST_1Y,
    MIN_90D,
    STDDEV_SAMP_90D,
    TRIMMED_MEAN_150_350_90D,
    VAR_POP_90D,
    WEIGHTED_AVG_90D,
    WEIGHTED_AVG_ATTACHED_90D,
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
)

_ORDERS = {r[0]: r for r in _ORDERS_ROWS}  # id -> (id, cust, amount, qty, created, updated, region)
_CUST = {r[0]: r for r in _CUSTOMERS_ROWS}  # id -> (id, spend, signup, tier, discount)
_REGION_QTY = {"US": 1.0 + 2.0 + 5.0, "EU": 3.0 + 4.0}


def _amounts(ids):
    return [_ORDERS[i][2] for i in ids]


def _qtys(ids):
    return [_ORDERS[i][3] for i in ids]


def _covar_samp(xs, ys):
    n = len(xs)
    mx, my = mean(xs), mean(ys)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)


def _corr(xs, ys):
    mx, my = mean(xs), mean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return cov / (sx * sy)


def _approx(d):
    return {k: pytest.approx(v, rel=1e-4) for k, v in d.items()}


def _bucket_end(month_key: str) -> date:
    y, mo = int(month_key[:4]), int(month_key[5:7])
    return date(y + (mo // 12), (mo % 12) + 1, 1)


def _members(rows, ts_index, lower_of):
    """Interval membership derived from the raw timestamps, independent of the
    MEMBERS_* constants: member iff lower_bound <= ts < bucket_end."""
    buckets = sorted({r[ts_index][:7] for r in rows.values()})
    out = {}
    for bkey in buckets:
        bend = _bucket_end(bkey)
        lower = lower_of(bend)
        out[bkey] = [i for i in sorted(rows)
                     if lower <= date.fromisoformat(rows[i][ts_index]) < bend]
    return out


def test_membership_90d_derived_from_raw_dates():
    got = _members(_ORDERS, 4, lambda bend: bend - timedelta(days=90))
    assert got == MEMBERS_90D


def test_membership_cust_1y_derived_from_raw_dates():
    got = _members(_CUST, 2, lambda bend: date(bend.year - 1, bend.month, bend.day))
    assert got == MEMBERS_CUST_1Y


def test_count_family():
    assert {m: len(ids) for m, ids in MEMBERS_90D.items()} == COUNT_90D
    assert {m: len(set(_amounts(ids))) for m, ids in MEMBERS_90D.items()} == COUNT_DISTINCT_90D


def test_min_max_avg_median():
    assert {m: min(_amounts(ids)) for m, ids in MEMBERS_90D.items()} == MIN_90D
    assert {m: max(_amounts(ids)) for m, ids in MEMBERS_90D.items()} == MAX_90D
    assert {m: mean(_amounts(ids)) for m, ids in MEMBERS_90D.items()} == _approx(AVG_90D)
    assert {m: median(_amounts(ids)) for m, ids in MEMBERS_90D.items()} == _approx(MEDIAN_90D)


def test_statistics_family():
    assert {m: stdev(_amounts(ids)) for m, ids in MEMBERS_90D.items()} == _approx(STDDEV_SAMP_90D)
    assert {m: pvariance(_amounts(ids)) for m, ids in MEMBERS_90D.items()} == _approx(VAR_POP_90D)
    assert {m: _corr(_amounts(ids), _qtys(ids)) for m, ids in MEMBERS_90D.items()} == _approx(CORR_90D)
    assert {m: _covar_samp(_amounts(ids), _qtys(ids)) for m, ids in MEMBERS_90D.items()} == _approx(COVAR_SAMP_90D)
    # pstdev unused elsewhere; assert it agrees with sqrt(var_pop) as a self-check.
    assert {m: pstdev(_amounts(ids)) for m, ids in MEMBERS_90D.items()} == _approx(
        {m: math.sqrt(v) for m, v in VAR_POP_90D.items()})


def test_weighted_and_trimmed():
    wavg = {m: sum(a * qy for a, qy in zip(_amounts(ids), _qtys(ids))) / sum(_qtys(ids))
            for m, ids in MEMBERS_90D.items()}
    assert wavg == _approx(WEIGHTED_AVG_90D)

    def _w(i):
        return _REGION_QTY[_ORDERS[i][6]]
    wavg_attached = {
        m: sum(_ORDERS[i][2] * _w(i) for i in ids) / sum(_w(i) for i in ids)
        for m, ids in MEMBERS_90D.items()
    }
    assert wavg_attached == _approx(WEIGHTED_AVG_ATTACHED_90D)

    trimmed = {m: mean([a for a in _amounts(ids) if 150 <= a <= 350])
               for m, ids in MEMBERS_90D.items()}
    assert trimmed == _approx(TRIMMED_MEAN_150_350_90D)


def test_first_last_by_created_at():
    first = {m: _ORDERS[min(ids, key=lambda i: _ORDERS[i][4])][2] for m, ids in MEMBERS_90D.items()}
    last = {m: _ORDERS[max(ids, key=lambda i: _ORDERS[i][4])][2] for m, ids in MEMBERS_90D.items()}
    assert first == FIRST_90D
    assert last == LAST_90D


def test_last_by_updated_at_skips_nulls():
    # Descending native ordering puts NULL keys last on both backends.
    last_upd = {}
    for m, ids in MEMBERS_90D.items():
        keyed = [(i, _ORDERS[i][5]) for i in ids if _ORDERS[i][5] is not None]
        last_upd[m] = _ORDERS[max(keyed, key=lambda p: p[1])[0]][2]
    assert last_upd == LAST_UPDATED_90D


def test_partitioned_count_by_region():
    got = {}
    for m, ids in MEMBERS_90D.items():
        for i in ids:
            region = _ORDERS[i][6]
            got[(region, m)] = got.get((region, m), 0) + 1
    # Keep only the cells with a populated (region, created-month) grain row.
    populated = {(_ORDERS[i][6], f"2024-{_ORDERS[i][4][5:7]}") for i in _ORDERS}
    got = {cell: n for cell, n in got.items() if cell in populated}
    assert got == COUNT_BY_REGION_90D


def test_cross_model_customers():
    assert {m: len(ids) for m, ids in MEMBERS_CUST_1Y.items()} == CM_COUNT_1Y
    last = {m: _CUST[max(ids, key=lambda i: _CUST[i][2])][1] for m, ids in MEMBERS_CUST_1Y.items()}
    assert last == CM_LAST_1Y
    disc = {m: sum(_CUST[i][1] * _CUST[i][4] for i in ids) for m, ids in MEMBERS_CUST_1Y.items()}
    assert disc == _approx(DISC_SPEND_1Y)
