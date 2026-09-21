"""Shared fixtures for DEV-1946 — a grained transform as an aggregation
parameter (``weight=rank(...)``, ``weight=cumsum(...)``).

Raw-row oracles over the DEV-1840 orders→customers→regions graph and the
DEV-1847 sales graph; every constant the delta scenarios cite is re-derived
here and pinned against the hand-typed literal by ``verify_oracles`` (the
double-entry guard). SQL-null weighted-average semantics: ``SUM(v*w)`` is NULL
when every term is NULL (an all-NULL cell like sales ``Void``), so the cell is
NULL even where the weights are present.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Optional, Tuple

import pytest

from tests._dev1840_fixtures import (
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
    _REGIONS_ROWS,
    month_key,
    rows_by,
)
from tests._dev1847_fixtures import _SALES_ROWS_WIDE
from tests._dev1859_fixtures import (
    distinct_customers_by_status,
    region_amount_totals,
    region_spend_totals,
)
from tests._dev1919_fixtures import RANKED, ranked_by_tier, ranked_global

# --------------------------------------------------------------------------- #
# Formula constants (one per delta scenario shape).
# --------------------------------------------------------------------------- #
RANKED_POSITIONAL = ("customers.spend:weighted_avg("
                     "rank(sum(amount, partition_by=customers.regions.name)))")
LAST_PARAM = ("customers.spend:weighted_avg(weight=last(sum(amount, "
              "partition_by=[customers.regions.name, ordered_at])))")
DET_CUMSUM = ("customers.spend:weighted_avg(weight=cumsum(sum(amount, "
              "partition_by=[customers.regions.name, customers.signup_at])))")
NOAXIS_CUMSUM = ("customers.spend:weighted_avg(weight=cumsum(sum(amount, "
                 "partition_by=customers.regions.name)))")
UNGRAINED_RANK = "customers.spend:weighted_avg(weight=rank(sum(amount)))"
WINDOWED_INNER = ("customers.spend:weighted_avg(weight=rank(sum(amount, "
                  "window='1y', partition_by=customers.regions.name)))")
WINDOWED_OUTER = ("customers.spend:weighted_avg(window='1y', "
                  "weight=rank(sum(amount, partition_by=customers.regions.name)))")
NESTED = ("customers.spend:weighted_avg(weight=weighted_avg(amount, "
          "weight=rank(sum(amount, partition_by=customers.regions.name)), "
          "partition_by=customers.regions.name))")
XMODEL_LOCAL = ("amount:weighted_avg(weight=rank(sum(customers.spend, "
                "partition_by=customers.regions.name)))")
OWN_PARTITION = ("customers.spend:weighted_avg(weight=rank("
                 "sum(amount, partition_by=[customers.regions.name, customers.tier]), "
                 "partition_by=customers.regions.name))")
#: refused at bind: a transform is not a valid first/last ranking key.
LAST_RANK_KEY = ("customers.spend:last(rank(sum(amount, "
                 "partition_by=customers.regions.name)))")
#: refused at bind (non-goal): an expression-valued parameter.
EXPR_ARG = "weighted_avg(amount, weight=quantity * 2)"

LOCAL_SALES = "weighted_avg(amount, weight=rank(sum(amount, partition_by=region)))"
LOCAL_SALES_CITY = "weighted_avg(amount, weight=rank(sum(amount, partition_by=city)))"
REAGG_RANK_COUNT = ("weighted_avg(sum(amount, partition_by=[city, region]), "
                    "weight=rank(count(id, partition_by=[city, region])))")
REAGG_RANK_PRODUCT = ("weighted_avg(sum(amount, partition_by=[city, region]), "
                      "weight=rank(count(id, partition_by=product)))")

# --------------------------------------------------------------------------- #
# Raw-row projections.
# --------------------------------------------------------------------------- #
_REGION_NAME: Dict[int, str] = {r[0]: r[1] for r in _REGIONS_ROWS}
#: id -> (region name, tier, spend, signup month).
_CUST = {c[0]: (_REGION_NAME.get(c[1]), c[3], c[4], c[5][:7]) for c in _CUSTOMERS_ROWS}
#: (id, customer_or_None, status, amount, ordered month).
_ORD = [(o[0], o[1], o[2], o[4], o[5][:7]) for o in _ORDERS_ROWS]


def _cust_region(cid: Optional[int]) -> Optional[str]:
    return _CUST[cid][0] if cid is not None else None


def _wavg(pairs) -> Optional[float]:
    """``SUM(v*w) / SUM(w)`` with SQL-null semantics: an all-NULL numerator (no
    pair with both parts present) is NULL, not zero, even when weights survive."""
    num = [v * w for v, w in pairs if v is not None and w is not None]
    if not num:
        return None
    den = sum(w for _v, w in pairs if w is not None)
    return None if den == 0 else sum(num) / den


def _rank_desc(totals: Dict) -> Dict:
    """Descending dense-competition rank; a NULL total ranks last."""
    nonnull = sum(1 for x in totals.values() if x is not None)

    def r(v):
        if v is None:
            return 1 + nonnull
        return 1 + sum(1 for x in totals.values() if x is not None and x > v)

    return {k: r(v) for k, v in totals.items()}


def _rank_amount() -> Dict[Optional[str], int]:
    """rank(sum(amount, partition_by=region name)) — North 1, NULL 2, South 3."""
    return _rank_desc(region_amount_totals())


def _rank_spend() -> Dict[Optional[str], int]:
    """rank(sum(customers.spend, partition_by=region name)) — North 1, South 2, NULL 3."""
    return _rank_desc(region_spend_totals())


# --------------------------------------------------------------------------- #
# Orders-graph oracles.
# --------------------------------------------------------------------------- #
def ranked_assoc_by_status() -> Dict[str, Optional[float]]:
    """RANKED under associate by status — each cell's distinct customers weighted
    by their region rank: ok 700/9, new 330/4."""
    rank = _rank_amount()
    return {
        status: _wavg([(_CUST[c][2], rank[_CUST[c][0]]) for c in custs])
        for status, custs in distinct_customers_by_status().items()
    }


def _last_region_weight() -> Dict[Optional[str], float]:
    """last(sum(amount, partition_by=[region, ordered month])) — each region's
    latest-month order total: North 12, South 15, NULL 47."""
    per: Dict[Tuple[Optional[str], str], float] = defaultdict(float)
    for _i, c, _s, a, m in _ORD:
        per[(_cust_region(c), m)] += a
    latest: Dict[Optional[str], Tuple[str, float]] = {}
    for (region, month), total in per.items():
        if region not in latest or month > latest[region][0]:
            latest[region] = (month, total)
    return {region: mv[1] for region, mv in latest.items()}


def last_default_value() -> Optional[float]:
    """LAST_PARAM broadcast over every customer — 8165/128."""
    w = _last_region_weight()
    return _wavg([(sp, w[rn]) for rn, _t, sp, _su in _CUST.values()])


def last_by_tier() -> Dict[str, Optional[float]]:
    """LAST_PARAM by tier: gold 3285/54, silver 3000/27, bronze 40."""
    w = _last_region_weight()
    return _by_tier(lambda rn, sp: (sp, w[rn]))


def _by_tier(pair_of) -> Dict[str, Optional[float]]:
    per: Dict[str, list] = defaultdict(list)
    for rn, tier, sp, _su in _CUST.values():
        per[tier].append(pair_of(rn, sp))
    return {tier: _wavg(ps) for tier, ps in per.items()}


def det_cumsum_by_signup_month() -> Dict[Optional[str], Optional[float]]:
    """DET_CUMSUM by signup month: Jan 100, Feb 150, Mar 1900/45, Apr 5700/140."""
    cell: Dict[Tuple[Optional[str], Optional[str]], float] = defaultdict(float)
    for _i, c, _s, a, _m in _ORD:
        signup = _CUST[c][3] if c is not None else None
        cell[(_cust_region(c), signup)] += a
    running: Dict[Tuple[Optional[str], Optional[str]], float] = {}
    per_region: Dict[Optional[str], list] = defaultdict(list)
    for (region, signup) in cell:
        per_region[region].append(signup)
    for region, signups in per_region.items():
        acc = 0.0
        for signup in sorted(s for s in signups if s is not None):
            acc += cell[(region, signup)]
            running[(region, signup)] = acc
        if (region, None) in cell:                       # a NULL signup keeps its own frame
            running[(region, None)] = cell[(region, None)]
    per_month: Dict[Optional[str], list] = defaultdict(list)
    for cid, (rn, _t, sp, su) in _CUST.items():
        per_month[su].append((sp, running.get((rn, su))))
    return {month: _wavg(ps) for month, ps in per_month.items()}


def ungrained_rank_by_tier() -> Dict[str, Optional[float]]:
    """UNGRAINED_RANK by tier — the ungrained inner types at [tier]; each tier's
    weight is constant so the value is the plain spend mean: gold 61.25, silver
    115, bronze 40."""
    amt: Dict[Optional[str], float] = defaultdict(float)
    for _i, c, _s, a, _m in _ORD:
        amt[_CUST[c][1] if c is not None else None] += a
    rank = _rank_desc(dict(amt))
    per: Dict[str, list] = defaultdict(list)
    for rn, tier, sp, _su in _CUST.values():
        per[tier].append((sp, rank.get(tier)))
    return {tier: _wavg(ps) for tier, ps in per.items()}


def _trailing_year(month: str, per: Dict[str, list]) -> list:
    y, m = int(month[:4]), int(month[5:7])
    upto, since = y * 12 + m, y * 12 + m - 12
    return [c for k, custs in per.items()
            if since < int(k[:4]) * 12 + int(k[5:7]) <= upto for c in custs]


def windowed_outer_by_signup_month() -> Dict[Optional[str], Optional[float]]:
    """WINDOWED_OUTER by signup month: trailing-1y ranked-weighted average —
    Jan 100, Feb 125, Mar 510/7, Apr 945/14."""
    rank = _rank_amount()
    per: Dict[str, list] = defaultdict(list)
    for cid, (rn, _t, sp, su) in _CUST.items():
        per[su].append(cid)
    return {month: _wavg([(_CUST[c][2], rank[_CUST[c][0]]) for c in _trailing_year(month, per)])
            for month in per}


def _nested_middle() -> Dict[Optional[str], Optional[float]]:
    """weighted_avg(amount, weight=rank(region), partition_by=region) per region —
    the rank is constant per region so this is the plain order-amount mean:
    North 50/3, South 10, NULL 23.5."""
    rank = _rank_amount()
    per: Dict[Optional[str], list] = defaultdict(list)
    for _i, c, _s, a, _m in _ORD:
        rn = _cust_region(c)
        per[rn].append((a, rank[rn]))
    return {rn: _wavg(ps) for rn, ps in per.items()}


def nested_global() -> Optional[float]:
    """NESTED broadcast globally — 45340/621."""
    middle = _nested_middle()
    return _wavg([(sp, middle.get(rn)) for rn, _t, sp, _su in _CUST.values()])


def nested_by_tier() -> Dict[str, Optional[float]]:
    """NESTED by tier: gold 62.1875, silver 123.75, bronze 40."""
    middle = _nested_middle()
    return _by_tier(lambda rn, sp: (sp, middle.get(rn)))


def xmodel_global() -> Optional[float]:
    """XMODEL_LOCAL globally — each order weighted by its customer's region spend
    rank: 281/16."""
    rank = _rank_spend()
    return _wavg([(a, rank.get(_cust_region(c))) for _i, c, _s, a, _m in _ORD])


def xmodel_by_status() -> Dict[str, Optional[float]]:
    """XMODEL_LOCAL by status: ok 116/11, new 33."""
    rank = _rank_spend()
    per: Dict[str, list] = defaultdict(list)
    for _i, c, s, a, _m in _ORD:
        per[s].append((a, rank.get(_cust_region(c))))
    return {s: _wavg(ps) for s, ps in per.items()}


def _own_rank() -> Dict[Tuple[Optional[str], Optional[str]], int]:
    """rank(sum(amount, partition_by=[region, tier]), partition_by=region) —
    descending within each region over its (region, tier) cells."""
    cell: Dict[Tuple[Optional[str], Optional[str]], float] = defaultdict(float)
    for _i, c, _s, a, _m in _ORD:
        rn = _cust_region(c)
        tier = _CUST[c][1] if c is not None else None
        cell[(rn, tier)] += a
    by_region: Dict[Optional[str], Dict[Optional[str], float]] = defaultdict(dict)
    for (rn, tier), v in cell.items():
        by_region[rn][tier] = v
    return {(rn, tier): 1 + sum(1 for x in cells.values() if x > v)
            for rn, cells in by_region.items() for tier, v in cells.items()}


def own_global() -> Optional[float]:
    """OWN_PARTITION globally — 760/11."""
    rank = _own_rank()
    return _wavg([(sp, rank.get((rn, tier))) for rn, tier, sp, _su in _CUST.values()])


def own_by_tier() -> Dict[str, Optional[float]]:
    """OWN_PARTITION by tier: gold 61.25, silver 115, bronze 40."""
    rank = _own_rank()
    per: Dict[str, list] = defaultdict(list)
    for rn, tier, sp, _su in _CUST.values():
        per[tier].append((sp, rank.get((rn, tier))))
    return {tier: _wavg(ps) for tier, ps in per.items()}


# --------------------------------------------------------------------------- #
# Sales-graph oracles.
# --------------------------------------------------------------------------- #
def _sales_totals(key_idx: int) -> Dict[Optional[str], Optional[float]]:
    """sum(amount) grouped by column ``key_idx``; an all-NULL cell is NULL."""
    acc: Dict[Optional[str], float] = defaultdict(float)
    has: Dict[Optional[str], bool] = defaultdict(bool)
    seen: Dict[Optional[str], bool] = {}
    for row in _SALES_ROWS_WIDE:
        k, a = row[key_idx], row[4]
        seen[k] = True
        if a is not None:
            acc[k] += a
            has[k] = True
    return {k: (acc[k] if has[k] else None) for k in seen}


def local_sales_by_region() -> Dict[str, Optional[float]]:
    """LOCAL_SALES by region: North 22.5, South 140/3, East 60, Gap 20/3, Void NULL."""
    rank = _rank_desc(_sales_totals(1))
    per: Dict[str, list] = defaultdict(list)
    for _i, r, _c, _p, a, *_x in _SALES_ROWS_WIDE:
        per[r].append((a, rank[r]))
    return {r: _wavg(ps) for r, ps in per.items()}


def local_sales_global() -> Optional[float]:
    """LOCAL_SALES with no dimensions — 810/43 (Void rows weight the denominator,
    contribute no numerator)."""
    rank = _rank_desc(_sales_totals(1))
    return _wavg([(a, rank[r]) for _i, r, _c, _p, a, *_x in _SALES_ROWS_WIDE])


def local_sales_by_region_city() -> Dict[Tuple[str, Optional[str]], Optional[float]]:
    """LOCAL_SALES_CITY by [region, city] — the per-cell plain mean (Xi/Void NULL)."""
    rank = _rank_desc(_sales_totals(2))
    per: Dict[Tuple[str, Optional[str]], list] = defaultdict(list)
    for _i, r, c, _p, a, *_x in _SALES_ROWS_WIDE:
        per[(r, c)].append((a, rank[c]))
    return {rc: _wavg(ps) for rc, ps in per.items()}


def reagg_rank_by_region() -> Dict[str, Optional[float]]:
    """REAGG_RANK_COUNT by region — city totals weighted by the rank of each
    cell's row count: North 55, South 580/7, East 60, Gap 64/7, Void NULL."""
    cell_sum: Dict[Tuple[Optional[str], str], float] = defaultdict(float)
    cell_has: Dict[Tuple[Optional[str], str], bool] = defaultdict(bool)
    cell_cnt: Dict[Tuple[Optional[str], str], int] = defaultdict(int)
    cells: set = set()
    for _i, r, c, _p, a, *_x in _SALES_ROWS_WIDE:
        cells.add((c, r))
        cell_cnt[(c, r)] += 1                            # count(id): ids never NULL
        if a is not None:
            cell_sum[(c, r)] += a
            cell_has[(c, r)] = True
    rank = _rank_desc({k: cell_cnt[k] for k in cells})
    per: Dict[str, list] = defaultdict(list)
    for (c, r) in cells:
        total = cell_sum[(c, r)] if cell_has[(c, r)] else None
        per[r].append((total, rank[(c, r)]))
    return {r: _wavg(ps) for r, ps in per.items()}


# --------------------------------------------------------------------------- #
# Result extractors.
# --------------------------------------------------------------------------- #
def region_vals(resp, name: str = "w") -> Dict[Optional[str], Any]:
    return {k[0]: v[f"sales.{name}"] for k, v in rows_by(resp, "sales.region").items()}


def region_city_vals(resp, name: str = "w") -> Dict[Tuple, Any]:
    return {k: v[f"sales.{name}"]
            for k, v in rows_by(resp, "sales.region", "sales.city").items()}


def global_val(resp, name: str = "w") -> Any:
    (row,) = resp.data
    return row[f"sales.{name}"]


def ordered_month_vals(resp, name: str = "w") -> Dict[Optional[str], Any]:
    return {(None if r["orders.ordered_at"] is None else month_key(r["orders.ordered_at"])):
            r[f"orders.{name}"] for r in resp.data}


def tier_month_vals(resp, name: str = "w") -> Dict[Tuple, Any]:
    out = {}
    for r in resp.data:
        month = (None if r["orders.ordered_at"] is None
                 else month_key(r["orders.ordered_at"]))
        out[(r["orders.customers.tier"], month)] = r[f"orders.{name}"]
    return out


def last_by_tier_month() -> Dict[Tuple[Optional[str], Optional[str]], Optional[float]]:
    """LAST_PARAM by [tier, ordered month] — the tier value broadcast to each
    (tier, month) cell present in the data."""
    tier_val = last_by_tier()
    out: Dict[Tuple[Optional[str], Optional[str]], Optional[float]] = {}
    for _i, c, _s, _a, m in _ORD:
        tier = _CUST[c][1] if c is not None else None
        out[(tier, m)] = tier_val.get(tier) if tier is not None else None
    return out


# --------------------------------------------------------------------------- #
# Double-entry guard: reductions vs the hand-typed spec constants.
# --------------------------------------------------------------------------- #
def _approx(got, want) -> None:
    if want is None:
        assert got is None, (got, want)
    else:
        assert got is not None, (got, want)
        assert float(got) == pytest.approx(want), (got, want)


def _approx_map(got: Dict, want: Dict) -> None:
    assert set(got) == set(want), (set(got), set(want))
    for key, value in want.items():
        _approx(got[key], value)


def verify_oracles() -> None:
    """Assert every reduction reproduces the literal constant its delta scenario
    cites — the double-entry guard, green without the feature."""
    _approx(ranked_global(), 945 / 14)
    _approx_map(ranked_by_tier(), {"gold": 475 / 8, "silver": 97.5, "bronze": 40.0})
    _approx_map(ranked_assoc_by_status(), {"ok": 700 / 9, "new": 330 / 4})
    _approx_map(_last_region_weight(), {"North": 12.0, "South": 15.0, None: 47.0})
    _approx(last_default_value(), 8165 / 128)
    _approx_map(last_by_tier(), {"gold": 3285 / 54, "silver": 3000 / 27, "bronze": 40.0})
    _approx_map(det_cumsum_by_signup_month(),
                {"2024-01": 100.0, "2024-02": 150.0,
                 "2024-03": 1900 / 45, "2024-04": 5700 / 140})
    _approx_map(ungrained_rank_by_tier(),
                {"gold": 61.25, "silver": 115.0, "bronze": 40.0})
    _approx_map(windowed_outer_by_signup_month(),
                {"2024-01": 100.0, "2024-02": 125.0,
                 "2024-03": 510 / 7, "2024-04": 945 / 14})
    _approx_map(_nested_middle(), {"North": 50 / 3, "South": 10.0, None: 23.5})
    _approx(nested_global(), 45340 / 621)
    _approx_map(nested_by_tier(),
                {"gold": 3316.6666666666665 / 53.333333333333336,
                 "silver": 123.75, "bronze": 40.0})
    _approx(xmodel_global(), 281 / 16)
    _approx_map(xmodel_by_status(), {"ok": 116 / 11, "new": 33.0})
    _approx(own_global(), 760 / 11)
    _approx_map(own_by_tier(), {"gold": 61.25, "silver": 115.0, "bronze": 40.0})
    _approx_map(local_sales_by_region(),
                {"North": 22.5, "South": 140 / 3, "East": 60.0,
                 "Gap": 20 / 3, "Void": None})
    _approx(local_sales_global(), 810 / 43)
    _approx_map(local_sales_by_region_city(), {
        ("North", "Alpha"): 10.0, ("North", "Beta"): 60.0,
        ("South", "Alpha"): 20.0, ("South", "Gamma"): 100.0,
        ("East", "Delta"): 50.0, ("East", "Epsilon"): 50.0, ("East", "Zeta"): 80.0,
        ("Gap", None): 6.0, ("Gap", "Kappa"): 8.0, ("Void", "Xi"): None})
    _approx_map(reagg_rank_by_region(),
                {"North": 55.0, "South": 580 / 7, "East": 60.0,
                 "Gap": 64 / 7, "Void": None})


__all__ = [
    "RANKED", "RANKED_POSITIONAL", "LAST_PARAM", "DET_CUMSUM", "NOAXIS_CUMSUM",
    "UNGRAINED_RANK", "WINDOWED_INNER", "WINDOWED_OUTER", "NESTED", "XMODEL_LOCAL",
    "OWN_PARTITION", "LAST_RANK_KEY", "EXPR_ARG", "LOCAL_SALES", "LOCAL_SALES_CITY",
    "REAGG_RANK_COUNT", "REAGG_RANK_PRODUCT",
    "ranked_global", "ranked_by_tier", "ranked_assoc_by_status",
    "last_default_value", "last_by_tier", "last_by_tier_month",
    "det_cumsum_by_signup_month", "ungrained_rank_by_tier",
    "windowed_outer_by_signup_month", "nested_global", "nested_by_tier",
    "xmodel_global", "xmodel_by_status", "own_global", "own_by_tier",
    "local_sales_by_region", "local_sales_global", "local_sales_by_region_city",
    "reagg_rank_by_region",
    "region_vals", "region_city_vals", "global_val", "ordered_month_vals",
    "tier_month_vals", "verify_oracles",
]
