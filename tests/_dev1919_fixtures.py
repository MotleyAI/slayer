"""Shared fixtures for DEV-1919 — an attached input's producer is rooted at its
own home in every ``to_many_handling`` mode. Raw-row oracles over the DEV-1840
dataset (``tests/_dev1840_fixtures``), the double-entry reference the executed
suite asserts against.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

import pytest

from slayer.core.enums import JoinCardinality, TimeGranularity
from slayer.core.models import SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension

from tests._dev1840_fixtures import (
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
    _REGIONS_ROWS,
    dev1840_models,
    month_key,
    rows_by,
    signup_month_td,
)
from tests._dev1859_fixtures import _wavg, region_amount_totals

MODES = ["broadcast", "associate", "error"]

HEADLINE = ("customers.spend:weighted_avg("
            "weight=sum(amount, partition_by=customers.regions.name))")
MIXED = "sum(customers.spend * sum(amount, partition_by=customers.regions.name))"
RECURSIVE = ("customers.spend:weighted_avg(weight=weighted_avg(amount, "
             "weight=sum(customers.regions.pop, partition_by=customers.regions.name), "
             "partition_by=customers.regions.name))")
RANKED = ("customers.spend:weighted_avg("
          "weight=rank(sum(amount, partition_by=customers.regions.name)))")
WINDOWED_PARAM = ("customers.spend:weighted_avg(window='1y', "
                  "weight=sum(amount, partition_by=customers.regions.name))")
WINDOWED_CONSTITUENT = ("sum(customers.spend * sum(amount, "
                        "partition_by=customers.regions.name), window='1y')")
CUMSUM_PARAM = ("customers.spend:weighted_avg(weight=cumsum(sum(amount, "
                "partition_by=[customers.regions.name, ordered_at])))")
UNPARSE_PARAM = ("customers.spend:weighted_avg(weight=sum("
                 "customers.regions.unparseable, partition_by=customers.regions.name))")
#: the parameter's own key crosses the fanning regions → region_events hop (dev1900 graph).
OWN_FAN_BADPOP = ("amount:weighted_avg(weight=sum(customers.regions.pop, "
                  "partition_by=customers.regions.bad_pop))")
#: a host-safe key, unattributable only from the parameter's cross-model home.
OWN_FAN_STATUS = "amount:weighted_avg(weight=sum(customers.spend, partition_by=status))"
#: the outer home (customers) does not determine the parameter's grain member.
UNDETERMINED_PARAM = "customers.spend:weighted_avg(weight=sum(customers.spend, partition_by=status))"
LAST_HOST = "customers.spend:last(ordered_at)"
LAST_BADPOP = "customers.regions.bad_pop:last(ordered_at)"

# customers: (id, region_id, plan_code, tier, spend, signup_at)
_C_ID, _C_REGION, _C_TIER, _C_SPEND, _C_SIGNUP = 0, 1, 3, 4, 5
# orders: (id, customer_id, status, channel, amount, ordered_at, …)
_O_CUST, _O_STATUS, _O_AMOUNT = 1, 2, 4
_REGION_NAME: Dict[int, str] = {r[0]: r[1] for r in _REGIONS_ROWS}
_POP_BY_NAME: Dict[Optional[str], float] = {r[1]: r[2] for r in _REGIONS_ROWS}
_CUST_REGION: Dict[int, Optional[str]] = {
    c[_C_ID]: _REGION_NAME.get(c[_C_REGION]) for c in _CUSTOMERS_ROWS}


def keyless_declared_models() -> List[SlayerModel]:
    """customers without a unique key, the orders → customers hop declared
    many-to-one: association cannot dedup, the parameter's key stays provable."""
    models = dev1840_models()
    cust = next(m for m in models if m.name == "customers")
    cust.columns = [
        (c.model_copy(update={"primary_key": False}) if c.name == "id" else c)
        for c in cust.columns
    ]
    orders = models[0]
    orders.joins = [
        (j.model_copy(update={"cardinality": JoinCardinality.MANY_TO_ONE})
         if j.target_model == "customers" else j)
        for j in orders.joins
    ]
    return models


def mode_q(mode: Optional[str], **kw) -> SlayerQuery:
    """An orders-rooted query; ``mode=None`` leaves ``to_many_handling`` at its default."""
    kw.setdefault("source_model", "orders")
    if mode is not None:
        kw["to_many_handling"] = mode
    return SlayerQuery(**kw)


def ordered_month_td() -> List[TimeDimension]:
    return [TimeDimension(dimension=ColumnRef(name="ordered_at"),
                          granularity=TimeGranularity.MONTH)]


# --------------------------------------------------------------------------- #
# Oracles.
# --------------------------------------------------------------------------- #
Weight = Callable[[Optional[str]], Optional[float]]


def _wavg_over_customers(weight: Weight, custs=_CUSTOMERS_ROWS) -> Optional[float]:
    return _wavg([(c[_C_SPEND], weight(_CUST_REGION[c[_C_ID]])) for c in custs])


def _by_tier(reduce: Callable[[list], Optional[float]]) -> Dict[str, Optional[float]]:
    per: Dict[str, list] = defaultdict(list)
    for c in _CUSTOMERS_ROWS:
        per[c[_C_TIER]].append(c)
    return {tier: reduce(custs) for tier, custs in per.items()}


def _sum_product(custs, weight: Weight) -> Optional[float]:
    terms = [c[_C_SPEND] * w for c in custs
             if (w := weight(_CUST_REGION[c[_C_ID]])) is not None]
    return sum(terms) if terms else None


def _region_weight() -> Weight:
    totals = region_amount_totals()
    return lambda region: totals.get(region)


def wavg_by_tier() -> Dict[str, Optional[float]]:
    """The headline by tier: gold 63.75, silver 415/3, bronze 40."""
    w = _region_weight()
    return _by_tier(lambda custs: _wavg_over_customers(w, custs))


def mixed_global() -> Optional[float]:
    """``sum(customers.spend * sum(amount, partition_by=customers.regions.name))`` = 33780."""
    return _sum_product(_CUSTOMERS_ROWS, _region_weight())


def mixed_by_tier() -> Dict[str, Optional[float]]:
    w = _region_weight()
    return _by_tier(lambda custs: _sum_product(custs, w))


def _recursive_weight() -> Weight:
    """Per region: ``weighted_avg(amount, weight=sum(regions.pop, partition_by=name))``
    over orders — the region-less cell's innermost weight is NULL."""
    per: Dict[Optional[str], list] = defaultdict(list)
    for o in _ORDERS_ROWS:
        region = _CUST_REGION.get(o[_O_CUST])
        per[region].append((o[_O_AMOUNT], _POP_BY_NAME.get(region)))
    middle = {region: _wavg(pairs) for region, pairs in per.items()}
    return lambda region: middle.get(region)


def recursive_global() -> Optional[float]:
    return _wavg_over_customers(_recursive_weight())


def recursive_by_tier() -> Dict[str, Optional[float]]:
    w = _recursive_weight()
    return _by_tier(lambda custs: _wavg_over_customers(w, custs))


def _ranked_weight() -> Weight:
    """``rank`` (descending, ties share) of the region-amount cells, NULL name included."""
    totals = region_amount_totals()
    ranks = {region: 1 + sum(1 for other in totals.values() if other > v)
             for region, v in totals.items()}
    return lambda region: ranks.get(region)


def ranked_global() -> Optional[float]:
    return _wavg_over_customers(_ranked_weight())


def ranked_by_tier() -> Dict[str, Optional[float]]:
    w = _ranked_weight()
    return _by_tier(lambda custs: _wavg_over_customers(w, custs))


def _months() -> Dict[str, list]:
    per: Dict[str, list] = defaultdict(list)
    for c in _CUSTOMERS_ROWS:
        per[c[_C_SIGNUP][:7]].append(c)
    return per


def _trailing_year(month: str, per: Dict[str, list]) -> list:
    y, m = int(month[:4]), int(month[5:7])
    upto, since = y * 12 + m, y * 12 + m - 12
    return [c for k, custs in per.items()
            if since < int(k[:4]) * 12 + int(k[5:7]) <= upto for c in custs]


def windowed_constituent_by_month() -> Dict[str, Optional[float]]:
    """Trailing-1y ``sum(spend * region total)`` by signup month: 10000, 25000, 28080, 33780."""
    w, per = _region_weight(), _months()
    return {month: _sum_product(_trailing_year(month, per), w) for month in per}


def windowed_param_by_month() -> Dict[str, Optional[float]]:
    """Trailing-1y weighted average by signup month: 100, 125, 28080/267, 33780/407."""
    w, per = _region_weight(), _months()
    return {month: _wavg_over_customers(w, _trailing_year(month, per)) for month in per}


def local_wavg_status_assoc_by_status() -> Dict[str, Optional[float]]:
    """``amount:weighted_avg(weight=sum(customers.spend, partition_by=status))`` under
    associate — the weight is the cell's distinct-customer spend total, constant per
    cell, so each cell is its plain order-amount mean (82/7 ok, 85/3 new)."""
    per: Dict[str, list] = defaultdict(list)
    for o in _ORDERS_ROWS:
        per[o[_O_STATUS]].append(o[_O_AMOUNT])
    return {status: sum(v) / len(v) for status, v in per.items()}


# --------------------------------------------------------------------------- #
# Result helpers.
# --------------------------------------------------------------------------- #
def tier_vals(resp, name: str = "w") -> Dict[Optional[str], Any]:
    return {k[0]: v[f"orders.{name}"]
            for k, v in rows_by(resp, "orders.customers.tier").items()}


def status_vals(resp, name: str = "w") -> Dict[str, Any]:
    return {k[0]: v[f"orders.{name}"] for k, v in rows_by(resp, "orders.status").items()}


def month_vals(resp, name: str = "w") -> Dict[Optional[str], Any]:
    return {(None if r["orders.customers.signup_at"] is None
             else month_key(r["orders.customers.signup_at"])): r[f"orders.{name}"]
            for r in resp.data}


def assert_cells(got: Dict, expected: Dict) -> None:
    """Every expected cell present with its value (``None`` where NULL), no extras."""
    assert set(got) == set(expected), (set(got), set(expected))
    for key, value in expected.items():
        if value is None:
            assert got[key] is None, key
        else:
            assert float(got[key]) == pytest.approx(value), key


__all__ = [
    "MODES", "HEADLINE", "MIXED", "RECURSIVE", "RANKED", "WINDOWED_PARAM",
    "WINDOWED_CONSTITUENT", "CUMSUM_PARAM", "UNPARSE_PARAM", "OWN_FAN_BADPOP",
    "OWN_FAN_STATUS", "UNDETERMINED_PARAM", "LAST_HOST", "LAST_BADPOP",
    "keyless_declared_models", "mode_q", "ordered_month_td", "signup_month_td",
    "wavg_by_tier", "mixed_global", "mixed_by_tier", "recursive_global",
    "recursive_by_tier", "ranked_global", "ranked_by_tier",
    "windowed_constituent_by_month", "windowed_param_by_month",
    "local_wavg_status_assoc_by_status",
    "tier_vals", "status_vals", "month_vals", "assert_cells",
]
