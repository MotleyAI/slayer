"""Shared fixtures for DEV-1900 — fail-closed safety for path-bearing derived
refs crossing fanning hops.

Extends the DEV-1840 ``orders → customers → regions`` graph with a 1:N
``regions → region_events`` hop and derived columns that cross it, so every
input role (positional arg, kwarg, definition default, dimension, measure-level
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

Dataset (region_events; the rest is the DEV-1840 dataset verbatim)
------------------------------------------------------------------
region_events (id, region_id, value): 1 r1 50 | 2 r1 50 | 3 r2 30
  → bad_pop:     North 100+50 = 150   South 200+30 = 230   (region-less → NULL)
  → derived_pop: North 200            South 400
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

# --------------------------------------------------------------------------- #
# Models — one rich graph; tests select the role via the measure / dimension.
# --------------------------------------------------------------------------- #
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
    """DEV-1840 graph + region_events, the fanning derived columns, and the
    customer aggregations that name them. Host first (orders)."""
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
    # A region-owned aggregation whose weight defaults to the BARE owner-local
    # derived column bad_pop (which crosses the fanning hop) — the bare-default
    # counterpart to customers' dotted wsumx.
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
    """A region-owned aggregation (``wsum_cust_spend``) whose weight defaults to
    ``customers.spend``. With a source ``customers.regions.pop``, the shallower
    ``customers`` home determines both inputs (``pop`` to-one, ``spend`` local)
    — but only once definition-default paths join the home candidates (gap 4).
    An explicit ``weight=customers.spend`` already widens the home today, so it
    is the reference the default must match."""
    models = dev1900_models()
    _regions(models).aggregations.append(Aggregation(
        name="wsum_cust_spend", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="customers.spend")]))
    return models


def unparseable_derived_models() -> List[SlayerModel]:
    """Pathological derived columns on regions: ``unparseable`` (no dialect
    parses it → closure is tri-state None, never empty) and ``cyc`` (self-
    referential → the closure's cycle guard raises). ``wunparse`` names the
    unparseable one as a weight, so an aggregate over it must fail closed."""
    models = dev1900_models()
    regions = _regions(models)
    regions.columns.append(
        Column(name="unparseable", type=DataType.DOUBLE, sql=")((( bad"))
    regions.columns.append(
        Column(name="cyc", type=DataType.DOUBLE, sql="cyc + 1"))
    _customers(models).aggregations.append(Aggregation(
        name="wunparse", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="regions.unparseable")]))
    return models


# --------------------------------------------------------------------------- #
# Dual-engine seed (DEV-1840 tables used by the graph + region_events).
# --------------------------------------------------------------------------- #
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
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; each test module
    wraps this in ``@pytest.fixture``."""
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


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def cust_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "customers")
    return SlayerQuery(**kw)


# The fanning derived dimension, its cross-model / local metrics, and the two
# re-aggregation shapes, spelled once.
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


# --------------------------------------------------------------------------- #
# Hand-computed oracles (every value traced to the dataset above).
# --------------------------------------------------------------------------- #
#: North customers c1,c2,c6 orders = 10+20+3 + 30+25 + 12 = 100; South c3,c5 =
#: 5+15 = 20; region-less c4 (o6=40) + orphan o8 (7) = 47. Each order once.
ASSOC_AMOUNT_BY_BAD_POP = {150.0: 100.0, 230.0: 20.0, None: 47.0}
#: Cross-model spend, each customer once: North c1+c2+c6 = 280; South c3+c5+c7
#: = 195 (c7 has zero orders); region-less c4 = 40.
ASSOC_SPEND_BY_BAD_POP = {150.0: 280.0, 230.0: 195.0, None: 40.0}
#: The multiplying join doubles North's two same-value events (100→200, 280→560).
ASSOC_AMOUNT_NORTH_FAN_DEFECT = 200.0
ASSOC_SPEND_NORTH_FAN_DEFECT = 560.0
#: Broadcast repeats the ungrouped grand total per cell: local amount = all 10
#: orders = 167; cross-model spend = every customer once = 280+195+40 = 515.
BROADCAST_AMOUNT_TOTAL = 167.0
BROADCAST_SPEND_TOTAL = 515.0

#: weighted_avg of region-cell amount sums by derived_pop = 2·pop:
#: (100·200 + 20·400) / (200 + 400) = 28000 / 600.
REAGG_DERIVED_POP_WAVG = 28000.0 / 600.0

#: To-one measure-level population filter (customers.tier='gold') stays inline:
#: gold c1(33)+c3(5)+c6(12)+c7(0) = 50.
TO_ONE_FILTER_AMOUNT = 50.0

#: SUM(pop·spend) over customers at the shallower (customers) home for the
#: source customers.regions.pop with weight=customers.spend — the explicit
#: spelling produces this today; the definition default must match it (gap 4).
HOME_WIDEN_VALUE = 67000.0

#: DEV-1909 targets (DEV-1900 makes both RAISE): distinct customers with an 'ok'
#: order = c1+c2+c3+c5+c6 = 420 (the inline-fan defect double-counts c1's two ok
#: orders → 520); orders whose region passes bad_pop>0 = 167 − 47 = 120 (the
#: fan defect over North's two events → 220).
POP_FILTER_STRUCTURAL_ASSOC = 420.0
POP_FILTER_STRUCTURAL_FAN_DEFECT = 520.0
POP_FILTER_DERIVED_ASSOC = 120.0
POP_FILTER_DERIVED_FAN_DEFECT = 220.0

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
]
