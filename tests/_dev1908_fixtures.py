"""Shared fixtures for DEV-1908 — reverse-hop cancellation for definition defaults.

Extends the DEV-1900 ``orders → customers → regions → region_events`` graph with a
to-one ``regions → countries`` hop (PK-covered ``regions.country_id``; its inverse
fans), plus ``orders.ship_region_id`` (an inline ``ModelExtension`` edge
``ship_region`` → regions) and ``orders.cost`` (``amount * 2``). Dataset extensions
(DEV-1900 dataset reused verbatim): countries (id, name, gdp) = 1 North 1000 |
2 South 2000; regions.country_id North→1 South→2; orders.ship_region_id = each
order's customer's region (NULL where none).
"""

from __future__ import annotations

from collections import defaultdict
from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, ModelExtension, SlayerQuery, TimeDimension
from slayer.core.enums import TimeGranularity
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

from tests import _dev1900_fixtures
from tests._dev1900_fixtures import dev1900_models
from tests._dev1840_fixtures import (
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
    _PLANS_ROWS,
    _REGIONS_ROWS,
    month_key,
    rows_by,
)

# --------------------------------------------------------------------------- #
# Extension rows.
# --------------------------------------------------------------------------- #
#: countries (id, name, gdp).
_COUNTRIES_ROWS = [(1, "North", 1000.0), (2, "South", 2000.0)]
#: regions.country_id: each region's country (id == id here).
_REGION_COUNTRY = [(1, 1), (2, 2)]  # (region_id, country_id)
#: orders.ship_region_id: each order's customer's region (NULL where none).
_CUST_REGION = {c[0]: c[1] for c in _CUSTOMERS_ROWS}  # customer_id → region_id
_ORDER_SHIP_REGION = [
    (_CUST_REGION.get(o[1]), o[0])  # (ship_region_id, order_id)
    for o in _dev1900_fixtures._ORDERS_ROWS  # noqa: SLF001 — shared row constants
]


def _by_name(models: List[SlayerModel], name: str) -> SlayerModel:
    return next(m for m in models if m.name == name)


def countries_model() -> SlayerModel:
    """``countries`` + the aggregations whose defaults name a model on the path
    to them (``regions``, ``regions.customers``, …) — each cancels."""
    return SlayerModel(
        name="countries", data_source="test", sql_table="countries",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="gdp", type=DataType.DOUBLE),
        ],
        aggregations=[
            # Single cancel: regions is one hop back on the path.
            Aggregation(name="wsum_region_pop", formula="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight", sql="regions.pop")]),
            Aggregation(name="wsum_region_pop_expr", formula="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight", sql="regions.pop * 1")]),
            # Double cancel: regions.customers walks two back to customers.
            Aggregation(name="wsum_cust_spend2", formula="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight",
                                                 sql="regions.customers.spend")]),
            Aggregation(name="wsum_cust_spend2_expr", formula="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight",
                                                 sql="regions.customers.spend * 1")]),
            # Cancel then forward: regions.customers cancels, plans hops on.
            Aggregation(name="wsum_plan_fee", formula="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight",
                                                 sql="regions.customers.plans.fee")]),
            # Cancel then FANNING hop: regions cancels, region_events fans → closed.
            Aggregation(name="wsum_fan", formula="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight",
                                                 sql="regions.region_events.value")]),
            # Two defaults in two frames: regions.pop and regions.customers.plans.fee.
            Aggregation(name="wsum_two", formula="SUM({value} * {w1} * {w2})",
                        params=[AggregationParam(name="w1", sql="regions.pop"),
                                AggregationParam(name="w2",
                                                 sql="regions.customers.plans.fee")]),
            # D3: first token cancels (regions), then `plans` misses (not a hop from
            # regions) — fails closed, never re-anchored at the root.
            Aggregation(name="wsum_cancel_miss", formula="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight", sql="regions.plans.fee")]),
        ],
    )


def dev1908_models() -> List[SlayerModel]:
    """DEV-1900 graph + ``regions → countries`` and the cancellation
    aggregations. Host first (orders)."""
    models = dev1900_models()

    regions = _by_name(models, "regions")
    regions.columns.append(Column(name="country_id", type=DataType.INT))
    regions.joins.append(ModelJoin(
        target_model="countries", join_pairs=[["country_id", "id"]],
        cardinality=JoinCardinality.MANY_TO_ONE))
    # Cancel then forward from a regions-declared default: customers.plans.fee.
    regions.aggregations.append(Aggregation(
        name="wsum_cust_plan_fee", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="customers.plans.fee")]))
    # Reverse hop to a dataset NOT on the path (regions-rooted): stays refused.
    regions.aggregations.append(Aggregation(
        name="wsum_cust_spend", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="customers.spend")]))

    region_events = _by_name(models, "region_events")
    # Homes above the source over a to-one reverse hop: regions.pop.
    region_events.aggregations.append(Aggregation(
        name="wsum_rp", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.pop")]))

    orders = _by_name(models, "orders")
    orders.columns.append(Column(name="ship_region_id", type=DataType.INT))
    orders.columns.append(Column(name="cost", type=DataType.DOUBLE, sql="amount * 2"))
    orders.aggregations.extend([
        # Owner at the root consumes its own name once (degenerate cancel-to-self).
        Aggregation(name="wself", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="orders.cost")]),
        Aggregation(name="wself_expr", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="orders.cost * 1")]),
        # Second-order outer with a root-frame expression default (D8 kernel door).
        Aggregation(name="wavg_pop_expr", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight",
                                             sql="customers.regions.pop * 1")]),
        # A non-join qualifier default → fails closed at typing.
        Aggregation(name="wnowhere", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="nowhere.col")]),
    ])

    models.append(countries_model())
    return models


# --------------------------------------------------------------------------- #
# Dual-engine seed: reuse the DEV-1900 seed, then add countries + the columns.
# --------------------------------------------------------------------------- #
def _seed_sqlite(db_path: str) -> None:
    _dev1900_fixtures._seed_sqlite(db_path)  # noqa: SLF001 — shared base seed
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("ALTER TABLE regions ADD COLUMN country_id INTEGER")
        cur.executemany("UPDATE regions SET country_id=? WHERE id=?",
                        [(cid, rid) for rid, cid in _REGION_COUNTRY])
        cur.execute("CREATE TABLE countries (id INTEGER PRIMARY KEY, name TEXT, "
                    "gdp REAL)")
        cur.executemany("INSERT INTO countries VALUES (?,?,?)", _COUNTRIES_ROWS)
        cur.execute("ALTER TABLE orders ADD COLUMN ship_region_id INTEGER")
        cur.executemany("UPDATE orders SET ship_region_id=? WHERE id=?",
                        _ORDER_SHIP_REGION)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    _dev1900_fixtures._seed_duckdb(db_path)  # noqa: SLF001 — shared base seed
    con = duckdb.connect(db_path)
    con.execute("ALTER TABLE regions ADD COLUMN country_id INTEGER")
    for rid, cid in _REGION_COUNTRY:
        con.execute("UPDATE regions SET country_id=? WHERE id=?", [cid, rid])
    con.execute("CREATE TABLE countries (id INTEGER, name VARCHAR, gdp DOUBLE)")
    con.executemany("INSERT INTO countries VALUES (?,?,?)", _COUNTRIES_ROWS)
    con.execute("ALTER TABLE orders ADD COLUMN ship_region_id INTEGER")
    for ship_region_id, oid in _ORDER_SHIP_REGION:
        con.execute("UPDATE orders SET ship_region_id=? WHERE id=?",
                    [ship_region_id, oid])
    con.close()


async def make_exec_engine(
    request, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture (DEV-1943 door)."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed,
        models=models if models is not None else dev1908_models(),
    ) as (engine, _db):
        yield engine


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def bundle1908() -> ResolvedSourceBundle:
    models = dev1908_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def signup_month_td() -> List[TimeDimension]:
    return [TimeDimension(
        dimension=ColumnRef(name="customers.signup_at"),
        granularity=TimeGranularity.MONTH)]


def ship_region_extension() -> ModelExtension:
    """orders inline-extended with a named ``ship_region`` join to regions."""
    return ModelExtension(source_name="orders", joins=[
        ModelJoin(target_model="regions", join_pairs=[["ship_region_id", "id"]],
                  name="ship_region", cardinality=JoinCardinality.MANY_TO_ONE)])


# --------------------------------------------------------------------------- #
# Oracles — hand-derived, re-derived independently from the raw rows by
# ``derive_oracles`` (the smoke test asserts these literals equal it).
# --------------------------------------------------------------------------- #
#: SUM(country.gdp * region.pop) per region, each once.
SINGLE_CANCEL = 500000.0
#: The per-customer fan if the reverse hop multiplied.
SINGLE_CANCEL_FAN = 1500000.0
#: SUM(country.gdp * customer.spend) per customer (double cancel to customers).
DOUBLE_CANCEL = 670000.0
#: SUM(country.gdp * plan.fee) per customer (cancel then forward to plans).
CANCEL_FORWARD_PLAN = 100000.0
#: SUM(region.pop * plan.fee) per customer (regions-declared cancel-then-forward).
CANCEL_FORWARD_POP = 10000.0
#: SUM(country.gdp * region.pop * plan.fee) per customer (two frames).
TWO_FRAME = 17000000.0
#: SUM(event.value * region.pop) per event, each once (home region_events).
EVENTS_CANCEL = 16000.0
#: The per-customer fan of the same if it multiplied.
EVENTS_CANCEL_FAN = 48000.0
#: Association by status (wsum_region_pop_expr), home customers.regions.
ASSOC_EXPR = {"ok": 500000.0, "new": 100000.0}
#: Trailing-1y window by signup month (wsum_cust_spend2_expr), home customers.
WINDOW_EXPR = {"2024-01": 100000.0, "2024-02": 250000.0,
               "2024-03": 370000.0, "2024-04": 670000.0}
#: Second-order wavg_pop_expr over sum(amount, partition_by=region).
SECOND_ORDER = 14000.0
#: Owner-at-root self (wself): SUM(amount * cost), cost = amount * 2.
SELF = 8154.0


def _term(*factors: Optional[float]) -> float:
    """Product of factors, contributing 0 when any factor is absent (NULL)."""
    prod = 1.0
    for f in factors:
        if f is None:
            return 0.0
        prod *= f
    return prod


def derive_oracles() -> dict:
    """Recompute every numeric oracle straight from the raw row tuples."""
    pop = {r[0]: r[2] for r in _REGIONS_ROWS}            # region_id → pop
    country_of = dict(_REGION_COUNTRY)                   # region_id → country_id
    gdp = {c[0]: c[2] for c in _COUNTRIES_ROWS}          # country_id → gdp
    fee = {p[0]: p[2] for p in _PLANS_ROWS}              # plan_code → fee
    events = _dev1900_fixtures._REGION_EVENTS_ROWS       # noqa: SLF001

    def region_gdp(rid: Optional[int]) -> Optional[float]:
        cid = country_of.get(rid) if rid is not None else None
        return gdp.get(cid) if cid is not None else None

    def region_pop(rid: Optional[int]) -> Optional[float]:
        return pop.get(rid) if rid is not None else None

    def plan_fee(code: Optional[str]) -> Optional[float]:
        return fee.get(code) if code is not None else None

    # Per region (home = regions / ship_region), each region once.
    single = sum(_term(region_gdp(rid), pop[rid]) for rid in pop)
    # Per customer with a region (fan).
    single_fan = sum(_term(region_gdp(c[1]), region_pop(c[1]))
                     for c in _CUSTOMERS_ROWS if c[1] in pop)
    double = sum(_term(region_gdp(c[1]), c[4]) for c in _CUSTOMERS_ROWS)
    cancel_fwd_plan = sum(
        _term(region_gdp(c[1]), plan_fee(c[2])) for c in _CUSTOMERS_ROWS)
    cancel_fwd_pop = sum(
        _term(region_pop(c[1]), plan_fee(c[2])) for c in _CUSTOMERS_ROWS)
    two_frame = sum(
        _term(region_gdp(c[1]), region_pop(c[1]), plan_fee(c[2]))
        for c in _CUSTOMERS_ROWS)
    events_cancel = sum(_term(value, pop.get(rid)) for _eid, rid, value in events)
    # Per customer × their region's events (fan).
    events_fan = sum(
        _term(value, region_pop(c[1]))
        for c in _CUSTOMERS_ROWS if c[1] in pop
        for _eid, rid, value in events if rid == c[1])

    # --- kernel / self oracles (Codex: pin absolute values, not just the twin) ---
    cust_region = {c[0]: c[1] for c in _CUSTOMERS_ROWS}  # customer_id → region_id
    # Association by status, home customers.regions: SUM(gdp*pop) over the distinct
    # regions of the customers with an order of that status (region-less drops).
    status_regions: dict = defaultdict(set)
    for o in _ORDERS_ROWS:  # (id, customer_id, status, channel, amount, …)
        rid = cust_region.get(o[1])
        if rid in pop:
            status_regions[o[2]].add(rid)
    assoc_expr = {s: sum(_term(region_gdp(rid), pop[rid]) for rid in rids)
                  for s, rids in status_regions.items()}
    # Trailing-1y window by signup month, home customers: cumulative SUM(gdp*spend).
    month_term: dict = defaultdict(float)
    for c in _CUSTOMERS_ROWS:
        month_term[c[5][:7]] += _term(region_gdp(c[1]), c[4])
    window_expr, running = {}, 0.0
    for m in sorted(month_term):
        running += month_term[m]
        window_expr[m] = running
    # Second-order outer wavg_pop_expr over sum(amount, partition_by=region):
    # SUM(region-amount-sum * pop) per region.
    region_amt: dict = defaultdict(float)
    for o in _ORDERS_ROWS:
        rid = cust_region.get(o[1])
        if rid in pop:
            region_amt[rid] += o[4]
    second_order = sum(region_amt[rid] * pop[rid] for rid in region_amt)
    # Owner-at-root self: wself = SUM(amount * cost), cost = amount * 2.
    self_val = sum(o[4] * (o[4] * 2) for o in _ORDERS_ROWS)

    return {
        "SINGLE_CANCEL": single, "SINGLE_CANCEL_FAN": single_fan,
        "DOUBLE_CANCEL": double, "CANCEL_FORWARD_PLAN": cancel_fwd_plan,
        "CANCEL_FORWARD_POP": cancel_fwd_pop, "TWO_FRAME": two_frame,
        "EVENTS_CANCEL": events_cancel, "EVENTS_CANCEL_FAN": events_fan,
        "ASSOC_EXPR": assoc_expr, "WINDOW_EXPR": window_expr,
        "SECOND_ORDER": second_order, "SELF": self_val,
    }


ORACLE_LITERALS = {
    "SINGLE_CANCEL": SINGLE_CANCEL, "SINGLE_CANCEL_FAN": SINGLE_CANCEL_FAN,
    "DOUBLE_CANCEL": DOUBLE_CANCEL, "CANCEL_FORWARD_PLAN": CANCEL_FORWARD_PLAN,
    "CANCEL_FORWARD_POP": CANCEL_FORWARD_POP, "TWO_FRAME": TWO_FRAME,
    "EVENTS_CANCEL": EVENTS_CANCEL, "EVENTS_CANCEL_FAN": EVENTS_CANCEL_FAN,
    "ASSOC_EXPR": ASSOC_EXPR, "WINDOW_EXPR": WINDOW_EXPR,
    "SECOND_ORDER": SECOND_ORDER, "SELF": SELF,
}

__all__ = [
    "Aggregation", "AggregationParam", "Column", "ModelExtension", "ModelJoin",
    "ModelMeasure", "SlayerModel", "SlayerQuery", "JoinCardinality",
    "ColumnRef", "TimeDimension", "TimeGranularity",
    "dev1908_models", "countries_model", "make_exec_engine",
    "orders_q", "bundle1908", "signup_month_td", "ship_region_extension",
    "rows_by", "month_key",
    "derive_oracles", "ORACLE_LITERALS",
    "SINGLE_CANCEL", "SINGLE_CANCEL_FAN", "DOUBLE_CANCEL", "CANCEL_FORWARD_PLAN",
    "CANCEL_FORWARD_POP", "TWO_FRAME", "EVENTS_CANCEL", "EVENTS_CANCEL_FAN",
    "ASSOC_EXPR", "WINDOW_EXPR", "SECOND_ORDER", "SELF",
]
