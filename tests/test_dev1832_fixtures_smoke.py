"""DEV-1832 task 1.1 — the fixture smoke test.

Re-derives every DEV-1832 oracle constant from the raw rows in pure Python (the
double-entry guard against hand-arithmetic drift). Passes WITHOUT the feature —
it validates the dataset, not cross-model expression aggregation.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from fractions import Fraction
from statistics import mean

import pytest

from tests._dev1832_fixtures import (
    _CUSTOMERS_ROWS,
    _MONTHLY_MULTI_ROWS,
    _MONTHLY_ROWS,
    _ORDERS_ROWS,
    _REGIONS_ROWS,
    _SALES_ROWS,
    CHANGE_PCT_SUM_BY_MONTH,
    CHANGE_SUM_BY_MONTH,
    COLLAPSE_MIXED_BY_MONTH,
    CONSEC_SUM_BY_MONTH,
    COUNT_BY_QAMOUNT,
    COUNT_QAMT_MINUS_1,
    CUMSUM_MINUS_LAST_BY_MONTH,
    EMPTY_GRAIN_MIXED_BY_MONTH,
    FIRST_SUM_BY_MONTH,
    GRAINED_CUMSUM_BY_MONTH,
    HANDWRITTEN_MIN_MIXED_BY_MONTH,
    HOST_DISCOUNT_BY_STATUS,
    JOINED_ROWLEAF_MIXED_BY_STATUS,
    LAG_SUM_BY_MONTH,
    LAST_SUM_BY_MONTH,
    LEAD_SUM_BY_MONTH,
    MIXED_COMBINED_BY_REGION_MONTH,
    MIXED_MULTI_BY_MONTH,
    MIXED_RANK_SUM_BY_REGION,
    NORTH_SPEND_EXPR_BY_STATUS,
    QAMT_SUM,
    SUM_QAMT_MINUS_1,
    TIME_SHIFT_BACK_SUM_BY_MONTH,
    UNGRAINED_CUMSUM_BY_MONTH,
    WAVG_AMOUNT_WEIGHT_QAMT,
    WAVG_MIN_MIXED_BY_MONTH,
    WAVG_QAMT_WEIGHT_QTY,
    WAVG_QAMT_WEIGHT_QTY_WRONG,
    WINDOWED_INNER_BY_MONTH,
    WINDOWED_MULTI_BY_MONTH,
    XMODEL_TIER_CUMSUM_BY_MONTH,
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
            if r[_M_AMOUNT] is None:  # NULL amount contributes nothing (SUM ignores it)
                continue
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
            if r[_M_AMOUNT] is None:  # NULL amount contributes nothing (SUM ignores it)
                continue
            am[r[_M_MONTH][:7]] += r[_M_AMOUNT]
        running, tot = {}, 0.0
        for month in sorted(am):
            tot += am[month]
            running[month] = tot
        assert running == UNGRAINED_CUMSUM_BY_MONTH


_MONTHS = ("2024-01", "2024-02", "2024-03")


def _x_series():
    """X = amount:sum per (region, month), as an ordered [(month, value)] list per
    region (a NULL amount cell yields a None value)."""
    by_rm: dict = {}
    for r in _MONTHLY_ROWS:
        key = (r[_M_REGION], r[_M_MONTH][:7])
        amt = r[_M_AMOUNT]
        by_rm[key] = None if amt is None else (by_rm.get(key) or 0.0) + amt
    series: dict = defaultdict(list)
    for (region, month) in sorted(by_rm):
        series[region].append((month, by_rm[(region, month)]))
    return series


def _sum_by_month(per_region_month: list) -> dict:
    """Sum values across regions per month, dropping None cells; a month with no
    non-None cell is a NULL measure row and drops out entirely."""
    out: dict = defaultdict(float)
    has_value: dict = defaultdict(bool)
    for (month, value) in per_region_month:
        if value is not None:
            out[month] += value
            has_value[month] = True
    return {m: out[m] for m in _MONTHS if has_value[m]}


class TestCollapseAndFamilyOracles:
    """D4c collapse + D4a family oracles, re-derived from the raw monthly rows."""

    def test_last_first_collapse(self):
        s = _x_series()
        last_total = sum(v for (_, v) in (ser[-1] for ser in s.values()) if v is not None)
        first_total = sum(v for (_, v) in (ser[0] for ser in s.values()) if v is not None)
        assert {m: last_total for m in _MONTHS} == LAST_SUM_BY_MONTH
        assert {m: first_total for m in _MONTHS} == FIRST_SUM_BY_MONTH

    def test_cumsum_minus_last(self):
        cells = []
        for ser in _x_series().values():
            last = ser[-1][1]
            tot = 0.0
            for (month, v) in ser:
                if v is None or last is None:
                    cells.append((month, None))
                    continue
                tot += v
                cells.append((month, tot - last))
        assert _sum_by_month(cells) == CUMSUM_MINUS_LAST_BY_MONTH

    def test_family_shifts_and_diffs(self):
        change, change_pct, shift_back, lead, consec = [], [], [], [], []
        for ser in _x_series().values():
            vals = [v for (_, v) in ser]
            streak = 0
            for i, (month, v) in enumerate(ser):
                prev = vals[i - 1] if i > 0 else None
                nxt = vals[i + 1] if i + 1 < len(vals) else None
                change.append((month, None if (v is None or prev is None) else v - prev))
                change_pct.append((month, None if (v is None or prev is None or prev == 0)
                                   else (v - prev) / prev))
                shift_back.append((month, prev))
                lead.append((month, nxt))
                streak = streak + 1 if (v is not None and v > 12) else 0
                consec.append((month, streak))
        assert _sum_by_month(change) == CHANGE_SUM_BY_MONTH
        assert _sum_by_month(change_pct) == CHANGE_PCT_SUM_BY_MONTH
        assert _sum_by_month(shift_back) == TIME_SHIFT_BACK_SUM_BY_MONTH
        assert _sum_by_month(shift_back) == LAG_SUM_BY_MONTH
        assert _sum_by_month(lead) == LEAD_SUM_BY_MONTH
        assert _sum_by_month(consec) == CONSEC_SUM_BY_MONTH


# --------------------------------------------------------------------------- #
# DEV-1928 — re-aggregation constituents in a mixed row source, windowed inner,
# and the to-one cross-model cumsum. Every oracle re-derived from the raw rows.
# --------------------------------------------------------------------------- #
_O_CUST, _O_AMOUNT, _O_ORDERED = 1, 4, 5
_C_ID2, _C_TIER = 0, 3
_TIER = {r[_C_ID2]: r[_C_TIER] for r in _CUSTOMERS_ROWS}


def _x_cells() -> dict:
    """X = amount:sum per (region, month) from the monthly rows (None if all-NULL)."""
    cells: dict = {}
    for r in _MONTHLY_ROWS:
        key, amt = (r[_M_REGION], r[_M_MONTH][:7]), r[_M_AMOUNT]
        cells[key] = amt if amt is None else amt + (cells.get(key) or 0.0)
    return cells


def _last_per_region() -> dict:
    out: dict = {}
    for (region, _m), v in sorted(_x_cells().items(), key=lambda kv: kv[0][1]):
        out[region] = v
    return out


def _min_per_region() -> dict:
    vals: dict = defaultdict(list)
    for (region, _m), v in _x_cells().items():
        vals[region].append(v)
    return {r: (min(nn) if (nn := [x for x in vs if x is not None]) else None)
            for r, vs in vals.items()}


def _region_total() -> dict:
    tot: dict = defaultdict(float)
    seen: dict = defaultdict(bool)
    for r in _MONTHLY_ROWS:
        if r[_M_AMOUNT] is not None:
            tot[r[_M_REGION]] += r[_M_AMOUNT]
            seen[r[_M_REGION]] = True
    return {region: tot[region] for region in seen}


def _mixed_by_month(region_value: dict) -> dict:
    """sum over rows of amount * (region's broadcast value), per month."""
    out: dict = defaultdict(float)
    has: dict = defaultdict(bool)
    for r in _MONTHLY_ROWS:
        rv, amt, month = region_value.get(r[_M_REGION]), r[_M_AMOUNT], r[_M_MONTH][:7]
        if amt is None or rv is None:
            continue
        out[month] += amt * rv
        has[month] = True
    return {m: out[m] for m in _MONTHS if has[m]}


class TestDev1928ReaggMixedOracles:
    def test_collapse_and_min_mixed(self):
        assert _mixed_by_month(_last_per_region()) == COLLAPSE_MIXED_BY_MONTH
        assert _mixed_by_month(_min_per_region()) == HANDWRITTEN_MIN_MIXED_BY_MONTH

    def test_empty_grain_mixed(self):
        by_month = _sum_by_month([(m, v) for (m, v) in
                                  ((r[_M_MONTH][:7], r[_M_AMOUNT]) for r in _MONTHLY_ROWS)])
        last_all = by_month[sorted(by_month)[-1]]  # empty grain → latest month total
        assert _mixed_by_month({r[_M_REGION]: last_all for r in _MONTHLY_ROWS}
                               ) == EMPTY_GRAIN_MIXED_BY_MONTH

    def test_wavg_min_mixed(self):
        mn = _min_per_region()
        num: dict = defaultdict(float)
        den: dict = defaultdict(float)
        for r in _MONTHLY_ROWS:
            w, amt, month = mn.get(r[_M_REGION]), r[_M_AMOUNT], r[_M_MONTH][:7]
            if w is None:
                continue
            den[month] += w
            if amt is not None:
                num[month] += amt * w
        got = {m: num[m] / den[m] for m in _MONTHS if den.get(m)}
        assert got == pytest.approx(WAVG_MIN_MIXED_BY_MONTH)

    def test_mixed_combined_by_region_month(self):
        mn, rt = _min_per_region(), _region_total()
        got = {}
        for r in _MONTHLY_ROWS:
            region, amt, month = r[_M_REGION], r[_M_AMOUNT], r[_M_MONTH][:7]
            if amt is None or mn.get(region) is None or region not in rt:
                continue
            got[(region, month)] = amt * mn[region] + rt[region]
        assert got == MIXED_COMBINED_BY_REGION_MONTH

    def test_windowed_inner_ranks(self):
        rows = [(r[_M_REGION], date.fromisoformat(r[_M_MONTH]), r[_M_AMOUNT])
                for r in _MONTHLY_ROWS]
        cells = {}  # (region, month) → trailing-90d per-region sum (None if all-NULL)
        for region, d, _amt in rows:
            window = [a for (rg, dd, a) in rows
                      if rg == region and dd <= d and (d - dd).days <= 90]
            nn = [a for a in window if a is not None]
            cells[(region, d.strftime("%Y-%m"))] = sum(nn) if nn else None
        nonnull = sorted((kv for kv in cells.items() if kv[1] is not None),
                         key=lambda kv: -kv[1])
        ranks, prev, rank = {}, object(), 0
        for i, (key, v) in enumerate(nonnull, start=1):
            if v != prev:
                rank, prev = i, v
            ranks[key] = rank
        for key, v in cells.items():
            if v is None:
                ranks[key] = len(nonnull) + 1
        got: dict = defaultdict(float)
        for (_region, month), rk in ranks.items():
            got[month] += rk
        assert dict(got) == WINDOWED_INNER_BY_MONTH

    def test_xmodel_tier_cumsum(self):
        cell: dict = defaultdict(float)  # (tier, month) → amount:sum
        for o in _ORDERS_ROWS:
            cell[(_TIER.get(o[_O_CUST]), o[_O_ORDERED][:7])] += o[_O_AMOUNT]
        running = {}
        for tier in {t for (t, _m) in cell}:
            tot = 0.0
            for month in sorted(m for (t, m) in cell if t == tier):
                tot += cell[(tier, month)]
                running[(tier, month)] = tot
        by_month: dict = defaultdict(float)
        for (_tier, month), v in running.items():
            by_month[month] += v
        assert dict(by_month) == XMODEL_TIER_CUMSUM_BY_MONTH


def _windowed_multi_cells() -> dict:
    """The windowed inner amount:sum(window='90d', partition_by=region), grained by
    month: one trailing-90d sum per (region, month) cell (months >90d apart, so a
    cell's window is exactly its own two rows)."""
    rows = [(r[_M_REGION], date.fromisoformat(r[_M_MONTH]), r[_M_AMOUNT])
            for r in _MONTHLY_MULTI_ROWS]
    cells: dict = {}
    for region, d, _amt in rows:
        window = [a for (rg, dd, a) in rows
                  if rg == region and dd <= d and (d - dd).days <= 90]
        cells[(region, d.strftime("%Y-%m"))] = sum(window)
    return cells


class TestDev1928AxiomComplianceOracles:
    """monthly_multi (two rows per cell): a pure re-aggregation counts operand CELLS
    (Axiom 2.4 — no row leaf → the home is the operand dataset), while a row-leaf
    mixed source counts base ROWS. The oracles differ, so the fixture tells them
    apart — the monthly (one-row-per-cell) fixture cannot."""

    def test_windowed_pure_reagg_counts_cells_not_rows(self):
        ranks, prev, rank = {}, object(), 0
        for i, (key, v) in enumerate(
                sorted(_windowed_multi_cells().items(), key=lambda kv: -kv[1]), start=1):
            if v != prev:
                rank, prev = i, v
            ranks[key] = rank
        rows_per_cell: dict = defaultdict(int)
        for r in _MONTHLY_MULTI_ROWS:
            rows_per_cell[(r[_M_REGION], r[_M_MONTH][:7])] += 1
        over_cells: dict = defaultdict(float)
        over_rows: dict = defaultdict(float)
        for (region, month), rk in ranks.items():
            over_cells[month] += rk
            over_rows[month] += rk * rows_per_cell[(region, month)]
        assert dict(over_cells) == WINDOWED_MULTI_BY_MONTH
        assert dict(over_rows) != WINDOWED_MULTI_BY_MONTH  # the fixture distinguishes them

    def test_mixed_reagg_counts_rows(self):
        cells: dict = defaultdict(float)
        for r in _MONTHLY_MULTI_ROWS:
            cells[(r[_M_REGION], r[_M_MONTH][:7])] += r[_M_AMOUNT]
        region_min: dict = {}
        for (region, _m), v in cells.items():
            region_min[region] = min(v, region_min.get(region, v))
        got: dict = defaultdict(float)
        for r in _MONTHLY_MULTI_ROWS:
            got[r[_M_MONTH][:7]] += r[_M_AMOUNT] * region_min[r[_M_REGION]]
        assert dict(got) == MIXED_MULTI_BY_MONTH
