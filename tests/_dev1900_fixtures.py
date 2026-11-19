"""Shared fixtures for fail-closed safety for path-bearing derived
refs crossing fanning hops.

Extends the ``_dev1840_fixtures`` ``orders → customers → regions`` graph with a 1:N
``regions → region_events`` hop and derived fields that cross it, so every
input role (positional arg, kwarg, definition default, dimension, metric-level
filter, re-aggregation parameter) can be exercised over a genuinely fanning
derived definition. One hand-computed dataset seeded into SQLite AND DuckDB;
every oracle below derives from it.

Graph extension
---------------
``regions → region_events``  1:N (fanning). ``regions.bad_pop = pop +
region_events.value`` crosses it; ``bad_pop2 = bad_pop * 2`` chains through it;
``derived_pop = pop * 2`` is local (crosses nothing). North has TWO
region_events of equal value, so ``bad_pop`` is single-valued per region (one
dimension cell) yet the hop still fans — the multiplying join is observable.

Dataset (region_events; the rest is the ``_dev1840_fixtures`` dataset verbatim)
-------------------------------------------------------------------------------
region_events (id, region_id, value): 1 r1 50 | 2 r1 50 | 3 r2 30
  → bad_pop:     North 100+50 = 150   South 200+30 = 230   (region-less → NULL)
  → derived_pop: North 200            South 400
"""

from __future__ import annotations

from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import FieldRef, QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

from tests._dev1840_fixtures import (
    broadcast_warnings,
    dev1840_datasets,
    month_key,
    rows_by,
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
    _PLANS_ROWS,
    _REGIONS_ROWS,
    _STORES_ROWS,
)
from tests._dev1841_fixtures import associated_warnings, pushed_filter_infos

# --------------------------------------------------------------------------- #
# Datasets — one rich graph; tests select the role via the metric / dimension.
# --------------------------------------------------------------------------- #
_REGION_EVENTS_ROWS = [(1, 1, 50.0), (2, 1, 50.0), (3, 2, 30.0)]


def region_events_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="region_events", data_source="test", sql_table="region_events",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
            DatasetField(name="value", datatype=DataType.FLOAT),
        ],
    )


def _regions(datasets: List[SlayerDataset]) -> SlayerDataset:
    return next(m for m in datasets if m.name == "regions")


def _customers(datasets: List[SlayerDataset]) -> SlayerDataset:
    return next(m for m in datasets if m.name == "customers")


def dev1900_datasets() -> List[SlayerDataset]:
    """``_dev1840_fixtures`` graph + region_events, the fanning derived fields, and the
    customer aggregations that name them. Host first (orders)."""
    models = dev1840_datasets()
    regions = _regions(models)
    regions.relationships.append(DatasetRelationship(
        to="region_events", from_columns=["id"], to_columns=["region_id"],
        cardinality=RelationshipCardinality.ONE_TO_MANY,
    ))
    regions.fields.append(
        DatasetField(name="bad_pop", datatype=DataType.FLOAT, sql="pop + region_events.value"))
    regions.fields.append(
        DatasetField(name="bad_pop2", datatype=DataType.FLOAT, sql="regions.bad_pop * 2"))
    regions.fields.append(
        DatasetField(name="derived_pop", datatype=DataType.FLOAT, sql="pop * 2"))
    # A region-owned aggregation whose weight defaults to the BARE owner-local
    # derived field bad_pop (which crosses the fanning hop) — the bare-default
    # counterpart to customers' dotted wsumx.
    regions.aggregations.append(Aggregation(
        name="wbadbare", expression="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="bad_pop")]))

    cust = _customers(models)
    cust.aggregations.append(Aggregation(
        name="wsumx", expression="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.bad_pop")]))
    cust.aggregations.append(Aggregation(
        name="wsumy", expression="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.bad_pop * 1")]))
    cust.aggregations.append(Aggregation(
        name="wgood", expression="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.derived_pop")]))
    cust.fields.append(DatasetField(
        name="bad_pop_spend", datatype=DataType.FLOAT, sql="spend",
        filter="regions.bad_pop > 0"))

    models.append(region_events_dataset())
    return models


def home_path_datasets() -> List[SlayerDataset]:
    """A region-owned aggregation (``wsum_cust_spend``) whose weight defaults to
    ``customers.spend``. With a source ``customers.regions.pop``, the shallower
    ``customers`` home determines both inputs (``pop`` to-one, ``spend`` local)
    — but only once definition-default paths join the home candidates (gap 4).
    An explicit ``weight=customers.spend`` already widens the home today, so it
    is the reference the default must match."""
    models = dev1900_datasets()
    _regions(models).aggregations.append(Aggregation(
        name="wsum_cust_spend", expression="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="customers.spend")]))
    return models


def unparseable_derived_datasets() -> List[SlayerDataset]:
    """Pathological derived fields on regions: ``unparseable`` (no dialect
    parses it → closure is tri-state None, never empty) and ``cyc`` (self-
    referential → the closure's cycle guard raises). ``wunparse`` names the
    unparseable one as a weight, so an aggregate over it must fail closed."""
    models = dev1900_datasets()
    regions = _regions(models)
    regions.fields.append(
        DatasetField(name="unparseable", datatype=DataType.FLOAT, sql=")((( bad"))
    regions.fields.append(
        DatasetField(name="cyc", datatype=DataType.FLOAT, sql="regions.cyc + 1"))
    customers = _customers(models)
    customers.aggregations.append(Aggregation(
        name="wunparse", expression="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.unparseable")]))
    # An unparseable EXPRESSION default: expr_refs is (None,), so no single
    # field can be named — the input closure is None but the diagnostic returns
    # None (the check_input_dependencies_analyzable unnameable arm).
    customers.aggregations.append(Aggregation(
        name="wexpr_unparse", expression="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql=")((( bad")]))
    customers.fields.append(DatasetField(
        name="flagged_spend", datatype=DataType.FLOAT, sql="spend",
        filter="regions.unparseable > 0"))
    return models


# --------------------------------------------------------------------------- #
# Dual-engine seed (``_dev1840_fixtures`` tables used by the graph + region_events).
# --------------------------------------------------------------------------- #
def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop REAL)")
        cur.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
        cur.execute("CREATE TABLE plans (code TEXT PRIMARY KEY, level TEXT, fee REAL)")
        cur.executemany("INSERT INTO plans VALUES (?,?,?)", _PLANS_ROWS)
        cur.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
            "plan_code TEXT, tier TEXT, spend REAL, signup_at TEXT)")
        cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE stores (co TEXT, no INTEGER, city TEXT, rent REAL, "
            "PRIMARY KEY (co, no))")
        cur.executemany("INSERT INTO stores VALUES (?,?,?,?)", _STORES_ROWS)
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
            "status TEXT, channel TEXT, amount REAL, ordered_at TEXT, "
            "store_co TEXT, store_no INTEGER)")
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_ROWS)
        cur.execute(
            "CREATE TABLE region_events (id INTEGER PRIMARY KEY, region_id INTEGER, "
            "value REAL)")
        cur.executemany("INSERT INTO region_events VALUES (?,?,?)", _REGION_EVENTS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR, pop DOUBLE)")
    con.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    con.execute("CREATE TABLE plans (code VARCHAR, level VARCHAR, fee DOUBLE)")
    con.executemany("INSERT INTO plans VALUES (?,?,?)", _PLANS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region_id INTEGER, "
        "plan_code VARCHAR, tier VARCHAR, spend DOUBLE, signup_at TIMESTAMP)")
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE stores (co VARCHAR, no INTEGER, city VARCHAR, rent DOUBLE)")
    con.executemany("INSERT INTO stores VALUES (?,?,?,?)", _STORES_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR, "
        "channel VARCHAR, amount DOUBLE, ordered_at TIMESTAMP, "
        "store_co VARCHAR, store_no INTEGER)")
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_ROWS)
    con.execute(
        "CREATE TABLE region_events (id INTEGER, region_id INTEGER, value DOUBLE)")
    con.executemany("INSERT INTO region_events VALUES (?,?,?)", _REGION_EVENTS_ROWS)
    con.close()


async def make_exec_engine(
    request, *, datasets: Optional[List[SlayerDataset]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; each test module
    wraps this in ``@pytest.fixture``."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, datasets=datasets if datasets is not None else dev1900_datasets(),
    ) as (engine, _db):
        yield engine


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(**kw)


def cust_q(**kw) -> SlayerQuery:
    kw.setdefault("population", "customers")
    return SlayerQuery(**kw)


# The fanning derived dimension, its cross-dataset / local metrics, and the two
# re-aggregation shapes, spelled once.
BAD_POP = "customers.regions.bad_pop"
AMOUNT_SUM = QueryMetric(metric="sum(amount)", alias="amt")
SPEND_SUM = QueryMetric(metric="sum(customers.spend)", alias="csp")
REAGG_GOOD = QueryMetric(
    metric="weighted_avg(sum(amount, partition_by=customers.regions.id), "
            "weight=customers.regions.derived_pop)", alias="ra")
REAGG_BAD = QueryMetric(
    metric="weighted_avg(sum(amount, partition_by=customers.regions.id), "
            "weight=customers.regions.bad_pop)", alias="ra")


def bad_pop_vals(resp, metric: str) -> dict:
    """``{bad_pop value: metric value}`` over ``resp.data`` (bad_pop cell key)."""
    return {k[0]: v[metric] for k, v in rows_by(resp, "orders." + BAD_POP).items()}


# --------------------------------------------------------------------------- #
# Hand-computed oracles (every value traced to the dataset above).
# --------------------------------------------------------------------------- #
#: North customers c1,c2,c6 orders = 10+20+3 + 30+25 + 12 = 100; South c3,c5 =
#: 5+15 = 20; region-less c4 (o6=40) + orphan o8 (7) = 47. Each order once.
ASSOC_AMOUNT_BY_BAD_POP = {150.0: 100.0, 230.0: 20.0, None: 47.0}
#: Cross-dataset spend, each customer once: North c1+c2+c6 = 280; South c3+c5+c7
#: = 195 (c7 has zero orders); region-less c4 = 40.
ASSOC_SPEND_BY_BAD_POP = {150.0: 280.0, 230.0: 195.0, None: 40.0}
#: The multiplying join doubles North's two same-value events (100→200, 280→560).
ASSOC_AMOUNT_NORTH_FAN_DEFECT = 200.0
ASSOC_SPEND_NORTH_FAN_DEFECT = 560.0
#: Broadcast repeats the ungrouped grand total per cell: local amount = all 10
#: orders = 167; cross-dataset spend = every customer once = 280+195+40 = 515.
BROADCAST_AMOUNT_TOTAL = 167.0
BROADCAST_SPEND_TOTAL = 515.0

#: weighted_avg of region-cell amount sums by derived_pop = 2·pop:
#: (100·200 + 20·400) / (200 + 400) = 28000 / 600.
REAGG_DERIVED_POP_WAVG = 28000.0 / 600.0

#: To-one metric-level population filter (customers.tier='gold') stays inline:
#: gold c1(33)+c3(5)+c6(12)+c7(0) = 50.
TO_ONE_FILTER_AMOUNT = 50.0

#: SUM(pop·spend) over customers at the shallower (customers) home for the
#: source customers.regions.pop with weight=customers.spend — the explicit
#: spelling produces this today; the definition default must match it (gap 4).
HOME_WIDEN_VALUE = 67000.0

#: Association targets: distinct customers with an 'ok'
#: order = c1+c2+c3+c5+c6 = 420 (the inline-fan defect double-counts c1's two ok
#: orders → 520); orders whose region passes bad_pop>0 = 167 − 47 = 120 (the
#: fan defect over North's two events → 220).
POP_FILTER_STRUCTURAL_ASSOC = 420.0
POP_FILTER_STRUCTURAL_FAN_DEFECT = 520.0
POP_FILTER_DERIVED_ASSOC = 120.0
POP_FILTER_DERIVED_FAN_DEFECT = 220.0

# --- Host-rooted regroup producers under the same 'ok' population ---
#: sum(spend, partition_by=tier) by tier; the fan defect double-counts c1's two
#: ok orders in gold (190 -> 290).
POP_FILTER_PARTITIONED_BY_TIER = {"gold": 190.0, "silver": 230.0}
POP_FILTER_PARTITIONED_GOLD_FAN_DEFECT = 290.0
#: *:count of the distinct population customers per tier (gold c1,c3,c6; silver c2,c5).
POP_FILTER_COUNT_BY_TIER = {"gold": 3, "silver": 2}
#: sum(spend, window='1y') bucketed by customers.signup_at month over the 'ok'
#: population, each customer once: trailing-1y cumulative 100/250/310/420 (April =
#: c1+c2+c3+c5+c6). The fanning orders.ordered_at axis fails closed (decision 12);
#: 520 was its pre-fix April fan defect (c1's two Jan ok orders double-counted).
POP_FILTER_WINDOWED_BY_MONTH = {
    "2024-01": 100.0, "2024-02": 250.0, "2024-03": 310.0, "2024-04": 420.0}
POP_FILTER_WINDOWED_APRIL = 420.0
POP_FILTER_WINDOWED_APRIL_FAN_DEFECT = 520.0
#: first/last(spend, customers.signup_at) over the 'ok' population per tier:
#: oldest-signup spend (gold c1=100, silver c2=150), newest (gold c6=30, silver
#: c5=80). Fan-immune picks, but a sibling spend:sum must not multiply.
POP_FILTER_FIRST_BY_TIER = {"gold": 100.0, "silver": 150.0}
POP_FILTER_LAST_BY_TIER = {"gold": 30.0, "silver": 80.0}
#: Nested avg(sum(spend, partition_by=tier)) with dims=[tier] is the degenerate
#: identity: per-tier the inner total, each customer once (gold 190 not 290).
POP_FILTER_NESTED_INNER_BY_TIER = {"gold": 190.0, "silver": 230.0}
#: Two independent branches: customers with an ok order AND a region event >= 50
#: (region North: c1,c2,c6) = 280.
POP_FILTER_TWO_BRANCH = 280.0
#: Producer-only orders.amount:sum over ok orders = 82; a predicate no order
#: passes yields zero rows (empty-base spine carries the EXISTS).
POP_FILTER_PRODUCER_ONLY = 82.0
#: Raw-row mode: one row per distinct population customer with an ok order (5),
#: never one per matching order (fan defect 6: c1's two ok orders).
POP_FILTER_RAW_ROWS = 5
#: Out-of-scope conjunct (tier='bronze' OR orders.status='ok') without an inline
#: aggregate still restricts the result rows: tiers bronze, gold, silver.
POP_FILTER_OUT_OF_SCOPE_TIERS = {"bronze", "gold", "silver"}
#: Association arm untouched: associate spend:sum by orders.status filtered
#: orders.amount in (20, 30) — new c1=100 / ok c2=150 (dev-1910, unchanged).
POP_FILTER_ASSOC_BY_STATUS = {"new": 100.0, "ok": 150.0}

__all__ = [
    "Aggregation", "AggregationParam", "DatasetField", "FieldRef", "DataType",
    "DatasetRelationship", "DatasetMetric", "SlayerDataset", "SlayerQuery", "RelationshipCardinality",
    "dev1840_datasets", "dev1900_datasets", "region_events_dataset",
    "home_path_datasets", "unparseable_derived_datasets",
    "make_exec_engine", "orders_q", "cust_q",
    "rows_by", "bad_pop_vals", "broadcast_warnings",
    "associated_warnings",
    "BAD_POP", "AMOUNT_SUM", "SPEND_SUM", "REAGG_GOOD", "REAGG_BAD",
    "ASSOC_AMOUNT_BY_BAD_POP", "ASSOC_SPEND_BY_BAD_POP",
    "ASSOC_AMOUNT_NORTH_FAN_DEFECT", "ASSOC_SPEND_NORTH_FAN_DEFECT",
    "BROADCAST_AMOUNT_TOTAL", "BROADCAST_SPEND_TOTAL", "REAGG_DERIVED_POP_WAVG",
    "TO_ONE_FILTER_AMOUNT", "HOME_WIDEN_VALUE", "POP_FILTER_STRUCTURAL_ASSOC",
    "POP_FILTER_STRUCTURAL_FAN_DEFECT", "POP_FILTER_DERIVED_ASSOC",
    "POP_FILTER_DERIVED_FAN_DEFECT",
    "POP_FILTER_PARTITIONED_BY_TIER", "POP_FILTER_PARTITIONED_GOLD_FAN_DEFECT",
    "POP_FILTER_COUNT_BY_TIER", "POP_FILTER_WINDOWED_BY_MONTH",
    "POP_FILTER_WINDOWED_APRIL", "POP_FILTER_WINDOWED_APRIL_FAN_DEFECT",
    "POP_FILTER_FIRST_BY_TIER", "POP_FILTER_LAST_BY_TIER",
    "POP_FILTER_NESTED_INNER_BY_TIER", "POP_FILTER_TWO_BRANCH",
    "POP_FILTER_PRODUCER_ONLY", "POP_FILTER_RAW_ROWS",
    "POP_FILTER_OUT_OF_SCOPE_TIERS", "POP_FILTER_ASSOC_BY_STATUS",
    "pushed_filter_infos", "month_key",
]
