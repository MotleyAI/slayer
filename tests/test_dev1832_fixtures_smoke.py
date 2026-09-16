"""DEV-1832 task 1.1 — the fixture smoke test.

Re-derives every DEV-1832 oracle constant from the raw rows in pure Python (the
double-entry guard against hand-arithmetic drift). Passes WITHOUT the feature —
it validates the dataset, not cross-model expression aggregation.
"""

from __future__ import annotations

from collections import defaultdict
from fractions import Fraction
from statistics import mean

import pytest

from tests._dev1832_fixtures import (
    _CUSTOMERS_ROWS,
    _MONTHLY_ROWS,
    _ORDERS_ROWS,
    _REGIONS_ROWS,
    _SALES_ROWS,
    COUNT_BY_QAMOUNT,
    COUNT_QAMT_MINUS_1,
    GRAINED_CUMSUM_BY_MONTH,
    HOST_DISCOUNT_BY_STATUS,
    JOINED_ROWLEAF_MIXED_BY_STATUS,
    MIXED_RANK_SUM_BY_REGION,
    NORTH_SPEND_EXPR_BY_STATUS,
    QAMT_SUM,
    SUM_QAMT_MINUS_1,
    UNGRAINED_CUMSUM_BY_MONTH,
    WAVG_AMOUNT_WEIGHT_QAMT,
    WAVG_QAMT_WEIGHT_QTY,
    WAVG_QAMT_WEIGHT_QTY_WRONG,
)

# Column indices.
_C_ID, _C_REGION, _C_SPEND, _C_DISCOUNT = 0, 1, 4, 6
_O_CUST, _O_STATUS, _O_AMOUNT = 1, 2, 4
_S_REGION, _S_PRODUCT, _S_AMOUNT, _S_QTY, _S_UP = 1, 3, 4, 5, 6
_M_REGION, _M_MONTH, _M_AMOUNT = 1, 2, 3

_CUST = {r[_C_ID]: r for r in _CUSTOMERS_ROWS}
_REGION_NAME = {r[0]: r[1] for r in _REGIONS_ROWS}


def _discount(cust):
    return None if cust is None else _CUST[cust][_C_DISCOUNT]


def _north_spend(cust):
    if cust is None:
        return None
    rid = _CUST[cust][_C_REGION]
    if rid is not None and _REGION_NAME.get(rid) == "North":
        return _CUST[cust][_C_SPEND]
    return None


def _qamt(row):
    return row[_S_AMOUNT] if row[_S_PRODUCT] == "Q" else None


def _ssum(vals):
    nn = [v for v in vals if v is not None]
    return sum(nn) if nn else None


class TestGraphAOracles:
    def test_host_homed_discount(self):
        by = defaultdict(list)
        for o in _ORDERS_ROWS:
            d = _discount(o[_O_CUST])
            by[o[_O_STATUS]].append(None if d is None else o[_O_AMOUNT] - d)
        assert {k: _ssum(v) for k, v in by.items()} == HOST_DISCOUNT_BY_STATUS

    def test_north_spend_expression(self):
        by = defaultdict(list)
        for o in _ORDERS_ROWS:
            ns = _north_spend(o[_O_CUST])
            by[o[_O_STATUS]].append(None if ns is None else o[_O_AMOUNT] - ns)
        assert {k: _ssum(v) for k, v in by.items()} == NORTH_SPEND_EXPR_BY_STATUS

    def test_joined_rowleaf_mixed(self):
        amt = defaultdict(list)
        for o in _ORDERS_ROWS:
            amt[o[_O_STATUS]].append(int(o[_O_AMOUNT]))
        avg_amt = {s: Fraction(sum(v), len(v)) for s, v in amt.items()}
        got: dict = defaultdict(lambda: Fraction(0))
        for o in _ORDERS_ROWS:
            d = _discount(o[_O_CUST])
            if d is not None:
                got[o[_O_STATUS]] += Fraction(int(d)) * avg_amt[o[_O_STATUS]]
        for status, expected in JOINED_ROWLEAF_MIXED_BY_STATUS.items():
            assert float(got[status]) == pytest.approx(expected)


class TestColumnFilterOracles:
    def test_qamt_sum(self):
        assert _ssum([_qamt(r) for r in _SALES_ROWS]) == QAMT_SUM

    def test_wavg_qamt_weight_qty_masks_only_value(self):
        num = sum(q * r[_S_QTY] for r in _SALES_ROWS if (q := _qamt(r)) is not None)
        den_all = sum(r[_S_QTY] for r in _SALES_ROWS)  # weight NOT masked
        den_q = sum(r[_S_QTY] for r in _SALES_ROWS if _qamt(r) is not None)
        assert num / den_all == pytest.approx(WAVG_QAMT_WEIGHT_QTY)
        assert num / den_q == pytest.approx(WAVG_QAMT_WEIGHT_QTY_WRONG)
        assert WAVG_QAMT_WEIGHT_QTY != pytest.approx(WAVG_QAMT_WEIGHT_QTY_WRONG)

    def test_wavg_amount_weight_qamt_masks_weight(self):
        num = sum(r[_S_AMOUNT] * q for r in _SALES_ROWS
                  if (q := _qamt(r)) is not None and r[_S_AMOUNT] is not None)
        den = sum(q for r in _SALES_ROWS if (q := _qamt(r)) is not None)
        assert num / den == pytest.approx(WAVG_AMOUNT_WEIGHT_QAMT)

    def test_group_by_qamount_null_group(self):
        counts = defaultdict(int)
        for r in _SALES_ROWS:
            counts[_qamt(r)] += 1
        assert dict(counts) == COUNT_BY_QAMOUNT

    def test_qamt_minus_one(self):
        vals = [q - 1 for r in _SALES_ROWS if (q := _qamt(r)) is not None]
        assert sum(vals) == SUM_QAMT_MINUS_1
        assert len(vals) == COUNT_QAMT_MINUS_1


class TestTransformOracles:
    def test_mixed_rank_sum_by_region(self):
        up = defaultdict(list)
        for r in _SALES_ROWS:
            if r[_S_UP] is not None:
                up[r[_S_PRODUCT]].append(r[_S_UP])
        avg_up = {p: mean(v) for p, v in up.items()}
        # RANK() DESC with SQL ties.
        rank = {p: 1 + sum(1 for q in avg_up if avg_up[q] > avg_up[p]) for p in avg_up}
        got = defaultdict(float)
        for r in _SALES_ROWS:
            got[r[_S_REGION]] += r[_S_QTY] * rank[r[_S_PRODUCT]]
        assert dict(got) == MIXED_RANK_SUM_BY_REGION

    def test_grained_cumsum_by_month(self):
        rm = defaultdict(float)
        for r in _MONTHLY_ROWS:
            rm[(r[_M_REGION], r[_M_MONTH][:7])] += r[_M_AMOUNT]
        running = {}
        for region in {k[0] for k in rm}:
            tot = 0.0
            for month in sorted(m for (rr, m) in rm if rr == region):
                tot += rm[(region, month)]
                running[(region, month)] = tot
        by_month = defaultdict(float)
        for (region, month), v in running.items():
            by_month[month] += v - 1
        assert dict(by_month) == GRAINED_CUMSUM_BY_MONTH

    def test_ungrained_cumsum_by_month(self):
        am = defaultdict(float)
        for r in _MONTHLY_ROWS:
            am[r[_M_MONTH][:7]] += r[_M_AMOUNT]
        running, tot = {}, 0.0
        for month in sorted(am):
            tot += am[month]
            running[month] = tot
        assert running == UNGRAINED_CUMSUM_BY_MONTH
