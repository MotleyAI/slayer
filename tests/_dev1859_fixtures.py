"""Shared fixtures for DEV-1859 leg C — attached parameters on row-level sources.

Two graphs: the DEV-1840 associate graph (orders→customers→regions) for the
association shapes, and the DEV-1847 sales graph (with a ``wsum`` custom
aggregation appended) for the ordinary/ungrained/re-aggregation shapes. Every
oracle is a reduction over the raw seed rows — the double-entry reference the
executed suites assert against, runnable standalone.
"""

from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Dict, List, Optional

from slayer.core.models import Aggregation, AggregationParam, Column, SlayerModel

from tests._dev1840_fixtures import (
    _CUSTOMERS_ROWS as _DEV1840_CUSTOMERS,
    _ORDERS_ROWS,
    _REGIONS_ROWS as _DEV1840_REGIONS,
    dev1840_models,
)
from tests._dev1847_fixtures import (
    DataType,
    _SALES_ROWS_WIDE,
    dev1847_models,
)

# --------------------------------------------------------------------------- #
# Models — the sales graph with a ``wsum`` = SUM(value * weight) custom agg.
# --------------------------------------------------------------------------- #
_WSUM = Aggregation(
    name="wsum", formula="SUM({value} * {weight})",
    params=[AggregationParam(name="weight", sql="amount")],
)


def sales_wsum_models() -> List[SlayerModel]:
    """``dev1847_models()`` with ``wsum`` appended to the sales model (weight
    defaults to the ``amount`` column; every leg-C use passes it explicitly)."""
    models = dev1847_models()
    next(m for m in models if m.name == "sales").aggregations.append(_WSUM)
    return models


def customers_wsum_models() -> List[SlayerModel]:
    """The associate graph with ``wsum`` on customers (weight default ``spend``)."""
    models = dev1840_models()
    next(m for m in models if m.name == "customers").aggregations.append(
        Aggregation(name="wsum", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="spend")]),
    )
    return models


# --------------------------------------------------------------------------- #
# Raw-row projections.
# --------------------------------------------------------------------------- #
#: region_id -> region name (DEV-1840).
_REGION_NAME = {rid: name for (rid, name, _pop) in _DEV1840_REGIONS}
#: customer id -> (region_id, spend).
_CUST = {c[0]: (c[1], c[4]) for c in _DEV1840_CUSTOMERS}


def _cust_region_name(cid: Optional[int]) -> Optional[str]:
    """The region NAME of a customer (None for an unknown/orphan customer or a
    customer with no region) — the null-safe attach key."""
    info = _CUST.get(cid) if cid is not None else None
    return _REGION_NAME.get(info[0]) if info else None


def region_amount_totals() -> Dict[Optional[str], float]:
    """``sum(orders.amount, partition_by=customers.regions.name)`` — order
    amounts by the ordering customer's region name; the NULL cell holds c4's
    order and the orphan order (decision 18)."""
    tot: Dict[Optional[str], float] = defaultdict(float)
    for (_oid, cid, _st, _ch, amount, *_r) in _ORDERS_ROWS:
        tot[_cust_region_name(cid)] += amount
    return dict(tot)


def region_spend_totals() -> Dict[Optional[str], float]:
    """``sum(customers.spend, partition_by=customers.regions.name)`` — customer
    spend by region name over ALL customers (incl. orderless c7)."""
    tot: Dict[Optional[str], float] = defaultdict(float)
    for cid, (rid, spend) in _CUST.items():
        tot[_REGION_NAME.get(rid)] += spend
    return dict(tot)


def distinct_customers_by_status() -> Dict[str, set]:
    """Associate populations: the distinct customers of each status cell (an
    orphan order contributes no customer)."""
    pop: Dict[str, set] = defaultdict(set)
    for (_oid, cid, status, *_r) in _ORDERS_ROWS:
        if cid is not None:
            pop[status].add(cid)
    return dict(pop)


def amount_totals_by_status() -> Dict[str, float]:
    """``sum(orders.amount, partition_by=[])`` typed at ``[status]`` — order
    amounts by status over every order (orphan included)."""
    tot: Dict[str, float] = defaultdict(float)
    for (_oid, _cid, status, _ch, amount, *_r) in _ORDERS_ROWS:
        tot[status] += amount
    return dict(tot)


# sales rows: (id, region, city, product, amount, quantity, unit_price)
def _sales_by(idx: int) -> Dict[str, list]:
    d: Dict[str, list] = defaultdict(list)
    for r in _SALES_ROWS_WIDE:
        d[r[1]].append(r[idx])
    return d


def sales_region_amount_totals() -> Dict[str, Optional[float]]:
    """``sum(amount, partition_by=region)`` — Void's amounts are all NULL."""
    out: Dict[str, Optional[float]] = {}
    for region, amounts in _sales_by(4).items():
        nn = [a for a in amounts if a is not None]
        out[region] = sum(nn) if nn else None
    return out


def sales_region_row_counts() -> Dict[str, int]:
    return {region: len(rows) for region, rows in _sales_by(0).items()}


def _avg_up_by_product() -> Dict[str, float]:
    d: Dict[str, list] = defaultdict(list)
    for r in _SALES_ROWS_WIDE:
        if r[6] is not None:
            d[r[3]].append(r[6])
    return {p: mean(v) for p, v in d.items()}


# --------------------------------------------------------------------------- #
# Oracles.
# --------------------------------------------------------------------------- #
def _wavg(pairs) -> Optional[float]:
    """``SUM(v*w) / NULLIF(SUM(w), 0)`` over (value, weight) pairs, SQL-null
    semantics: ``v*w`` drops the numerator term when EITHER is NULL, but
    ``SUM(w)`` keeps every non-NULL weight even where its value is NULL."""
    num = sum(v * w for v, w in pairs if v is not None and w is not None)
    den = sum(w for _v, w in pairs if w is not None)
    return num / den if den else None


def assoc_wavg_region_weight() -> Dict[str, Optional[float]]:
    """Headline: ``customers.spend:weighted_avg(weight=sum(amount,
    partition_by=customers.regions.name))`` under associate mode, by status —
    each customer weighted by its region's order-amount total."""
    rt = region_amount_totals()
    return {
        status: _wavg([(_CUST[c][1], rt.get(_cust_region_name(c))) for c in custs])
        for status, custs in distinct_customers_by_status().items()
    }


def broadcast_wavg_global() -> Optional[float]:
    """Default/broadcast twin of the associate headline — the customers-rooted
    weighted_avg over EVERY customer (incl. orderless c7), region-order-amount
    weighted, broadcast identically to both status cells (decision 14 probe)."""
    rt = region_amount_totals()
    return _wavg([(spend, rt.get(_REGION_NAME.get(rid)))
                  for _cid, (rid, spend) in _CUST.items()])


def broadcast_wavg_target_side() -> Optional[float]:
    """``customers.spend:weighted_avg(weight=sum(customers.spend,
    partition_by=customers.regions.name))`` under broadcast — the
    customers-rooted global value, each customer weighted by its region's spend
    total (c4 takes the NULL-region cell)."""
    st = region_spend_totals()
    return _wavg([(spend, st.get(_REGION_NAME.get(rid)))
                  for _cid, (rid, spend) in _CUST.items()])


def ordinary_wavg_global() -> Optional[float]:
    """``weighted_avg(amount, weight=sum(amount, partition_by=region))`` with no
    dimensions — each row weighted by its region total, so weights vary."""
    wt = sales_region_amount_totals()
    return _wavg([(r[4], wt[r[1]]) for r in _SALES_ROWS_WIDE])


def ordinary_avg_amount_global() -> float:
    """Plain ``avg(amount)`` globally — the distinguishable unweighted value."""
    from statistics import mean
    return mean(r[4] for r in _SALES_ROWS_WIDE if r[4] is not None)


def mixed_plus_param_by_region() -> Dict[str, Optional[float]]:
    """``weighted_avg(quantity * avg(unit_price, partition_by=product),
    weight=sum(amount, partition_by=region))`` by region — the weight is
    constant per region cell, so it collapses to the row-mean of the mixed
    value (Void's NULL weight yields NULL)."""
    avg_p = _avg_up_by_product()
    wt = sales_region_amount_totals()
    per: Dict[str, list] = defaultdict(list)
    for r in _SALES_ROWS_WIDE:
        per[r[1]].append((r[5] * avg_p[r[3]], wt[r[1]]))
    return {region: _wavg(pairs) for region, pairs in per.items()}


def ungrained_wsum_row_by_region() -> Dict[str, Optional[float]]:
    """``wsum(amount, weight=sum(amount))`` — the ungrained weight types at the
    query's ``[region]`` grain, so each region equals its total squared."""
    tot = sales_region_amount_totals()
    return {r: (None if t is None else t * t) for r, t in tot.items()}


def ungrained_wsum_assoc_by_status() -> Dict[str, float]:
    """``customers.spend:wsum(weight=sum(amount))`` associate, by status — the
    ungrained weight types at ``[status]`` (the cell's order-amount total),
    broadcast onto each distinct customer."""
    amt = amount_totals_by_status()
    spend = distinct_customers_by_status()
    return {s: amt[s] * sum(_CUST[c][1] for c in custs)
            for s, custs in spend.items()}


def ungrained_wsum_reagg_by_region() -> Dict[str, Optional[float]]:
    """``wsum(sum(amount, partition_by=[city, region]), weight=count(id))`` by
    region — the ungrained ``count(id)`` types at ``[region]`` (the region row
    count), carried onto each city cell: N_region * region_total."""
    tot = sales_region_amount_totals()
    n = sales_region_row_counts()
    return {r: (None if t is None else n[r] * t) for r, t in tot.items()}


def literal_source_wsum_by_region() -> Dict[str, Optional[float]]:
    """``wsum(1, weight=sum(amount, partition_by=region))`` — a literal-only
    (row-grain) source: each region equals its row count times its total."""
    tot = sales_region_amount_totals()
    n = sales_region_row_counts()
    return {r: (None if t is None else n[r] * t) for r, t in tot.items()}


def ordinary_wavg_by_region() -> Dict[str, Optional[float]]:
    """``weighted_avg(amount, weight=sum(amount, partition_by=region))`` by
    region — the weight is constant per cell, so it is the plain row mean."""
    out: Dict[str, Optional[float]] = {}
    for region, amounts in _sales_by(4).items():
        nn = [a for a in amounts if a is not None]
        out[region] = sum(nn) / len(nn) if nn else None
    return out


def assoc_wavg_new_without_c4() -> Optional[float]:
    """The ``new`` cell if c4 were DROPPED (NULL weight) — the distinguishable
    wrong value the NULL-region cell (weight 47) must not produce."""
    rt = region_amount_totals()
    return _wavg([(_CUST[c][1], rt.get(_cust_region_name(c)))
                  for c in distinct_customers_by_status()["new"] if c != 4])


def cross_model_param_by_status() -> Dict[str, Optional[float]]:
    """``amount:weighted_avg(weight=sum(customers.spend,
    partition_by=customers.regions.name))`` rooted at orders, by status — each
    order weighted by its customer's region SPEND total."""
    sw = region_spend_totals()
    per: Dict[str, list] = defaultdict(list)
    for (_oid, cid, status, _ch, amount, *_r) in _ORDERS_ROWS:
        per[status].append((amount, sw.get(_cust_region_name(cid))))
    return {status: _wavg(pairs) for status, pairs in per.items()}


__all__ = [
    "Aggregation", "AggregationParam", "Column", "DataType", "SlayerModel",
    "sales_wsum_models", "customers_wsum_models",
    "region_amount_totals", "region_spend_totals",
    "distinct_customers_by_status", "amount_totals_by_status",
    "sales_region_amount_totals", "sales_region_row_counts",
    "assoc_wavg_region_weight", "assoc_wavg_new_without_c4",
    "broadcast_wavg_global", "broadcast_wavg_target_side",
    "ordinary_wavg_global", "ordinary_wavg_by_region",
    "ordinary_avg_amount_global", "mixed_plus_param_by_region",
    "ungrained_wsum_row_by_region", "ungrained_wsum_assoc_by_status",
    "ungrained_wsum_reagg_by_region", "literal_source_wsum_by_region",
    "cross_model_param_by_status",
]
