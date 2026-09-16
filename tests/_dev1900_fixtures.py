"""Shared fixtures for DEV-1900 — fail-closed safety for path-bearing derived
refs crossing fanning hops.

Extends DEV-1840's ``orders → customers → regions`` with a 1:N
``regions → region_events`` hop and derived columns crossing it (``bad_pop``,
``bad_pop2``, local ``derived_pop``). North has TWO equal events, so ``bad_pop``
is single-valued per region yet the hop still fans. One dataset seeded into
SQLite AND DuckDB; every oracle below derives from it.

region_events (id, region_id, value): 1 r1 50 | 2 r1 50 | 3 r2 30
  bad_pop = pop + value: North 150, South 230, region-less NULL
  derived_pop = pop * 2: North 200, South 400
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1840_fixtures import (
    broadcast_warnings,
    dev1840_models,
    dropped_filter_warnings,
    rows_by,
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
    _PLANS_ROWS,
    _REGIONS_ROWS,
    _STORES_ROWS,
)
from tests._dev1841_fixtures import associated_warnings

# Models — one rich graph; tests select the role via the measure / dimension.
_REGION_EVENTS_ROWS = [(1, 1, 50.0), (2, 1, 50.0), (3, 2, 30.0)]


def region_events_model() -> SlayerModel:
    return SlayerModel(
        name="region_events", data_source="test", sql_table="region_events",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="value", type=DataType.DOUBLE),
        ],
    )


def _regions(models: List[SlayerModel]) -> SlayerModel:
    return next(m for m in models if m.name == "regions")


def _customers(models: List[SlayerModel]) -> SlayerModel:
    return next(m for m in models if m.name == "customers")


def dev1900_models() -> List[SlayerModel]:
    """DEV-1840 graph + region_events, the fanning derived columns and aggregations. Host first (orders)."""
    models = dev1840_models()
    regions = _regions(models)
    regions.joins.append(ModelJoin(
        target_model="region_events", join_pairs=[["id", "region_id"]],
        cardinality=JoinCardinality.ONE_TO_MANY,
    ))
    regions.columns.append(
        Column(name="bad_pop", type=DataType.DOUBLE, sql="pop + region_events.value"))
    regions.columns.append(
        Column(name="bad_pop2", type=DataType.DOUBLE, sql="bad_pop * 2"))
    regions.columns.append(
        Column(name="derived_pop", type=DataType.DOUBLE, sql="pop * 2"))
    # Weight defaults to the BARE owner-local bad_pop (fanning) — bare-default counterpart to wsumx.
    regions.aggregations.append(Aggregation(
        name="wbadbare", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="bad_pop")]))

    cust = _customers(models)
    cust.aggregations.append(Aggregation(
        name="wsumx", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.bad_pop")]))
    cust.aggregations.append(Aggregation(
        name="wsumy", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.bad_pop * 1")]))
    cust.aggregations.append(Aggregation(
        name="wgood", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.derived_pop")]))
    cust.columns.append(Column(
        name="bad_pop_spend", type=DataType.DOUBLE, sql="spend",
        filter="regions.bad_pop > 0"))

    models.append(region_events_model())
    return models


def home_path_models() -> List[SlayerModel]:
    """``wsum_cust_spend`` weight defaults to ``customers.spend``: the shallower ``customers`` home must
    determine both inputs once definition-default paths join the home candidates (gap 4)."""
    models = dev1900_models()
    _regions(models).aggregations.append(Aggregation(
        name="wsum_cust_spend", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="customers.spend")]))
    return models


def unparseable_derived_models() -> List[SlayerModel]:
    """Pathological derived columns on regions: ``unparseable`` (closure tri-state None) and self-referential
    ``cyc`` (cycle guard raises). ``wunparse`` weights the unparseable one, so its aggregate must fail closed."""
    models = dev1900_models()
    regions = _regions(models)
    regions.columns.append(
        Column(name="unparseable", type=DataType.DOUBLE, sql=")((( bad"))
    regions.columns.append(
        Column(name="cyc", type=DataType.DOUBLE, sql="cyc + 1"))
    customers = _customers(models)
    customers.aggregations.append(Aggregation(
        name="wunparse", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.unparseable")]))
    # An unparseable EXPRESSION default: expr_refs is (None,), so no column can be named (unnameable arm).
    customers.aggregations.append(Aggregation(
        name="wexpr_unparse", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql=")((( bad")]))
    customers.columns.append(Column(
        name="flagged_spend", type=DataType.DOUBLE, sql="spend",
        filter="regions.unparseable > 0"))
    return models


# Dual-engine seed (DEV-1840 tables used by the graph + region_events).
def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
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
    con.commit()
    con.close()


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


async def _engine_for(*, dialect: str, db_path: str,
                      models: List[SlayerModel]) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path))
    for model in models:
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


async def make_exec_engine(
    request, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; wrapped per module in ``@pytest.fixture``."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        if dialect == "sqlite":
            _seed_sqlite(db_path)
        else:
            _seed_duckdb(db_path)
        yield await _engine_for(
            dialect=dialect, db_path=db_path,
            models=models if models is not None else dev1900_models())


# Query shorthands.
def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def cust_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "customers")
    return SlayerQuery(**kw)


# The fanning derived dimension, its metrics, and the two re-aggregation shapes.
BAD_POP = "customers.regions.bad_pop"
AMOUNT_SUM = ModelMeasure(formula="amount:sum", name="amt")
SPEND_SUM = ModelMeasure(formula="customers.spend:sum", name="csp")
REAGG_GOOD = ModelMeasure(
    formula="weighted_avg(sum(amount, partition_by=customers.regions.id), "
            "weight=customers.regions.derived_pop)", name="ra")
REAGG_BAD = ModelMeasure(
    formula="weighted_avg(sum(amount, partition_by=customers.regions.id), "
            "weight=customers.regions.bad_pop)", name="ra")


def bad_pop_vals(resp, measure: str) -> dict:
    """``{bad_pop value: measure value}`` over ``resp.data`` (bad_pop cell key)."""
    return {k[0]: v[measure] for k, v in rows_by(resp, "orders." + BAD_POP).items()}


# Hand-computed oracles (every value traced to the dataset above).
#: amount by bad_pop, each order once: North c1,c2,c6=100; South c3,c5=20; region-less c4(40)+orphan o8(7)=47.
ASSOC_AMOUNT_BY_BAD_POP = {150.0: 100.0, 230.0: 20.0, None: 47.0}
#: spend by bad_pop, each customer once: North c1+c2+c6=280; South c3+c5+c7=195 (c7 no orders); c4=40.
ASSOC_SPEND_BY_BAD_POP = {150.0: 280.0, 230.0: 195.0, None: 40.0}
#: The multiplying join doubles North's two same-value events (100→200, 280→560).
ASSOC_AMOUNT_NORTH_FAN_DEFECT = 200.0
ASSOC_SPEND_NORTH_FAN_DEFECT = 560.0
#: Broadcast repeats the grand total per cell: amount all 10 orders=167; spend every customer once=515.
BROADCAST_AMOUNT_TOTAL = 167.0
BROADCAST_SPEND_TOTAL = 515.0

#: weighted_avg of region-cell amount sums by derived_pop=2·pop: (100·200 + 20·400) / (200+400).
REAGG_DERIVED_POP_WAVG = 28000.0 / 600.0

#: To-one filter (tier='gold') stays inline: gold c1(33)+c3(5)+c6(12)+c7(0) = 50.
TO_ONE_FILTER_AMOUNT = 50.0

#: SUM(pop·spend) at the shallower customers home (source regions.pop, weight=customers.spend); default must match (gap 4).
HOME_WIDEN_VALUE = 67000.0

#: DEV-1909 (DEV-1900 makes both RAISE): 'ok'-order customers c1+c2+c3+c5+c6=420 (inline-fan defect 520);
#: orders whose region passes bad_pop>0 = 167−47 = 120 (fan defect 220).
POP_FILTER_STRUCTURAL_ASSOC = 420.0
POP_FILTER_STRUCTURAL_FAN_DEFECT = 520.0
POP_FILTER_DERIVED_ASSOC = 120.0
POP_FILTER_DERIVED_FAN_DEFECT = 220.0

# Host-population pushdown oracles. Filter is ``orders.status = 'ok'`` unless
# noted; ok-order customers = c1,c2,c3,c5,c6 (c4 only 'new', c7 no orders).
#: sum(spend, partition_by=tier): gold c1+c3+c6=190, silver c2+c5=230 (fan defect gold 290).
POP_FILTER_PARTITIONED_BY_TIER = {"gold": 190.0, "silver": 230.0}
POP_FILTER_PARTITIONED_GOLD_FAN_DEFECT = 290.0
#: sum(spend, window='1y') by signup month, April cumulative bucket = five ok-order customers (defect 520).
POP_FILTER_WINDOWED_APRIL = 420.0
POP_FILTER_WINDOWED_APRIL_FAN_DEFECT = 520.0
#: *:count of the distinct ok-order customers per tier.
POP_FILTER_COUNT_BY_TIER = {"gold": 3, "silver": 2}
#: raw rows (distinct_dimension_values=False): one per ok-order customer (defect 6 from c1's second 'ok').
POP_FILTER_RAW_ROWS = 5
POP_FILTER_RAW_ROWS_FAN_DEFECT = 6
#: spend, two independent branches — 'ok' order AND a region_event value>=50 (North only): c1+c2+c6 = 280.
POP_FILTER_TWO_BRANCH_SPEND = 280.0
#: associate, dims=orders.status, amount in (20,30): amount-20 is c1's 'new', amount-30 c2's 'ok' (defect 250/250).
POP_FILTER_ASSOC_SAME_BRANCH = {"new": 100.0, "ok": 150.0}
POP_FILTER_ASSOC_SAME_BRANCH_FAN_DEFECT = 250.0
#: avg(sum(spend, partition_by=tier)) by tier equals the partitioned totals; mean over tiers 210 (defect 260).
POP_FILTER_NESTED_BY_TIER = {"gold": 190.0, "silver": 230.0}
POP_FILTER_NESTED_TIER_MEAN = 210.0
#: associate, dims=orders.status, amount=20: only c1's 'new' qualifies, so one cell binds that row.
POP_FILTER_SAME_ROW_ONE_CELL = {"new": 100.0}
#: one filter string, inline conjunct (tier='gold') + pushed (status='ok'): gold customers with an 'ok' order.
POP_FILTER_MIXED_CONJUNCT_SPEND = 190.0
#: out-of-scope OR (tier='bronze' OR status='ok') on dims-only rows stays applied: bronze c4 + ok-order tiers.
POP_FILTER_OUT_OF_SCOPE_TIERS = {"bronze", "gold", "silver"}
#: producer-only orders.amount:sum over all 'ok' orders, applied locally, incl. the customer-less orphan o8.
POP_FILTER_PRODUCER_ONLY_AMOUNT = 82.0

__all__ = [
    "Aggregation", "AggregationParam", "Column", "ColumnRef", "DataType",
    "ModelJoin", "ModelMeasure", "SlayerModel", "SlayerQuery", "JoinCardinality",
    "dev1840_models", "dev1900_models", "region_events_model",
    "home_path_models", "unparseable_derived_models",
    "make_exec_engine", "orders_q", "cust_q",
    "rows_by", "bad_pop_vals", "broadcast_warnings", "dropped_filter_warnings",
    "associated_warnings",
    "BAD_POP", "AMOUNT_SUM", "SPEND_SUM", "REAGG_GOOD", "REAGG_BAD",
    "ASSOC_AMOUNT_BY_BAD_POP", "ASSOC_SPEND_BY_BAD_POP",
    "ASSOC_AMOUNT_NORTH_FAN_DEFECT", "ASSOC_SPEND_NORTH_FAN_DEFECT",
    "BROADCAST_AMOUNT_TOTAL", "BROADCAST_SPEND_TOTAL", "REAGG_DERIVED_POP_WAVG",
    "TO_ONE_FILTER_AMOUNT", "HOME_WIDEN_VALUE", "POP_FILTER_STRUCTURAL_ASSOC",
    "POP_FILTER_STRUCTURAL_FAN_DEFECT", "POP_FILTER_DERIVED_ASSOC",
    "POP_FILTER_DERIVED_FAN_DEFECT",
    "POP_FILTER_PARTITIONED_BY_TIER", "POP_FILTER_PARTITIONED_GOLD_FAN_DEFECT",
    "POP_FILTER_WINDOWED_APRIL", "POP_FILTER_WINDOWED_APRIL_FAN_DEFECT",
    "POP_FILTER_COUNT_BY_TIER", "POP_FILTER_RAW_ROWS",
    "POP_FILTER_RAW_ROWS_FAN_DEFECT", "POP_FILTER_TWO_BRANCH_SPEND",
    "POP_FILTER_ASSOC_SAME_BRANCH", "POP_FILTER_ASSOC_SAME_BRANCH_FAN_DEFECT",
    "POP_FILTER_NESTED_BY_TIER", "POP_FILTER_NESTED_TIER_MEAN",
    "POP_FILTER_SAME_ROW_ONE_CELL", "POP_FILTER_MIXED_CONJUNCT_SPEND",
    "POP_FILTER_OUT_OF_SCOPE_TIERS", "POP_FILTER_PRODUCER_ONLY_AMOUNT",
]
