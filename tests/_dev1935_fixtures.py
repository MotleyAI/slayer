"""Shared fixtures for DEV-1935 — boolean-total semi-join pushdown: the DEV-1900
graph and dual-engine dataset (rows documented in ``tests/_dev1840_fixtures.py``)
plus three model variants (named reverse edge, unproven plans hop, event-less
region mini-graph)."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, JoinCardinality, JoinType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1840_fixtures import (
    ModelMeasure,
    dev1840_models,
    plans_model,
)
from tests._dev1841_fixtures import pushed_filter_infos
from tests._dev1900_fixtures import (
    cust_q,
    dev1900_models,
    make_exec_engine,
    orders_q,
    rows_by,
)

# --------------------------------------------------------------------------- #
# Filter predicates (spelled once).
# --------------------------------------------------------------------------- #
#: A root-local ref (tier) mixed with a cross-path ref (orders.status) under OR.
OR_MIX_LOCAL = "tier = 'bronze' or orders.status = 'ok'"
#: Same shape, but the local leg keeps a gold customer with no orders (c7) alive.
GOLD_OR_OK = "tier = 'gold' or orders.status = 'ok'"
#: Negation over a local-and-crosspath conjunction — existential reading.
NOT_GOLD_AND_OK = "not (tier = 'gold' and orders.status = 'ok')"
#: A null-test on a related column reads as absence (no orders).
NO_ORDERS = "orders.id is null"
#: Two independent fanning branches under OR (orders, regions.region_events).
NEW_OR_EVENT = "orders.status = 'new' or regions.region_events.value >= 50"
#: A single atom comparing columns of two branches -> judged on their product.
ATOM_TWO_BRANCH = "orders.amount < regions.region_events.value"
#: A materialised branch (orders.id dimension) OR a to-one branch (regions.name).
MATERIALISED_OR = "orders.status = 'ok' or regions.name = 'South'"
#: A materialised fanning branch (orders.id dimension) OR an unmaterialised one.
REDUCED_PUSH_OR = "orders.status = 'ok' or regions.region_events.value >= 50"
#: Two spellings of the customers->orders edge sharing one related row.
TWO_SPELLINGS = ["purchases.status = 'ok'", "orders.channel = 'app'"]
#: Producer-side (orders-rooted) mixed OR that stays dropped-and-warned today.
OR_MIX_PRODUCER = "customers.tier = 'bronze' OR channel = 'app'"
#: Association-arm mixed OR (orders-rooted, associate mode).
ASSOC_OR = "customers.tier = 'gold' OR channel = 'app'"
#: Association-arm mixed OR whose second leg crosses an unproven plans hop.
ASSOC_OR_ABSENT = "customers.tier = 'gold' OR customers.plans.level = 'basic'"

# Oracles — hand-computed from the DEV-1840/1900 dataset.
#: bronze {c4} OR >=1 ok order {c1,c2,c3,c5,c6} = {c1..c6} spend 460 (never 560).
OR_MIX_SPEND = 460.0
OR_MIX_SPEND_FAN_DEFECT = 560.0
#: gold OR ok = {c1,c2,c3,c5,c6,c7} = 475 (c7 kept null-extended); 420 = strict INNER; 675 = fan.
GOLD_OR_OK_SPEND = 475.0
GOLD_OR_OK_STRICT_INNER = 420.0
GOLD_OR_OK_FAN_DEFECT = 675.0
#: non-gold {c2,c4,c5} + gold with a non-ok order {c1} = 370 (c7 not counted); 520 = fan.
NOT_GOLD_AND_OK_SPEND = 370.0
NOT_GOLD_AND_OK_FAN_DEFECT = 520.0
#: customers with no orders: c7 -> 55.
NO_ORDERS_SPEND = 55.0
#: new order {c1,c2,c4} OR region event >=50 (North c1,c2,c6) = {c1,c2,c4,c6} = 320.
NEW_OR_EVENT_SPEND = 320.0
#: customers with some order.amount < some region-event value, each once = 420.
ATOM_TWO_BRANCH_SPEND = 420.0
#: dims=[orders.id], MATERIALISED_OR: orders that are ok or in South, plus c7's
#: null-order cell (its LEFT-extended row passes the region leg); o2 stays out.
MATERIALISED_ORDER_CELLS = {None, 1, 3, 5, 7, 9, 10}
#: dims=[orders.id], REDUCED_PUSH_OR: orders that are ok, or whose customer's
#: region (North only) has an event >= 50; no orderless customer qualifies.
REDUCED_PUSH_ORDER_CELLS = {1, 2, 3, 4, 5, 7, 9, 10}
#: sum(spend, partition_by=tier) by tier over the OR_MIX population.
OR_MIX_PARTITIONED = {"gold": 190.0, "silver": 230.0, "bronze": 40.0}
#: raw-row mode, OR_MIX: one row per population customer {c1..c6} = 6 (never 7).
OR_MIX_RAW_ROWS = 6
#: two spellings bind to ONE order (ok AND app): gold 60 / silver 80; 160/230 = split-node defect.
TWO_SPELLINGS_BY_TIER = {"gold": 60.0, "silver": 80.0}
TWO_SPELLINGS_SPLIT_DEFECT = {"gold": 160.0, "silver": 230.0}
#: producer-side OR_MIX_PRODUCER by tier: bronze OR an app order (never gold 245).
OR_MIX_PRODUCER_BY_TIER = {"gold": 160.0, "silver": 230.0, "bronze": 40.0}
#: associate ASSOC_OR by status: customers with an order of that status that is app or gold.
ASSOC_OR_BY_STATUS = {"ok": 270.0, "new": 250.0}
#: associate ASSOC_OR_ABSENT (unproven plans) by status: gold or basic-plan customers.
ASSOC_OR_ABSENT_BY_STATUS = {"ok": 270.0, "new": 100.0}

# Event-less-region mini-graph oracles (see event_less_models / _seed below).
#: gold OR event>=50 = {ec1,ec3} = 190 (ec3's event-less region null-extended);
#: under a declared-INNER hop ec3 has no product row -> 100.
EVENTLESS_GOLD_OR_EVENT_LEFT = 190.0
EVENTLESS_GOLD_OR_EVENT_INNER = 100.0
#: region_events.value>=50 alone rejects the null extension (UNKNOWN) -> INNER,
#: byte-identical LEFT/INNER: only ec1 -> 100.
EVENTLESS_EVENT_ONLY = 100.0


# --------------------------------------------------------------------------- #
# Model variants.
# --------------------------------------------------------------------------- #
def two_spellings_models() -> List[SlayerModel]:
    """DEV-1840 graph with the single ``customers -> orders`` edge NAMED
    ``purchases`` (1:N). ``purchases.status`` (edge name) and ``orders.channel``
    (target model) then name one physical edge two ways."""
    models = dev1840_models(declare_reverse=True)
    cust = next(m for m in models if m.name == "customers")
    for join in cust.joins:
        if join.target_model == "orders":
            join.name = "purchases"
    return models


def unproven_plans_models() -> List[SlayerModel]:
    """DEV-1900 graph with the ``customers -> plans`` hop unproven (plans.code not
    a PK), so a ``customers.plans.level`` filter must restrict by semi-join."""
    models = dev1900_models()
    return [plans_model(strong=False) if m.name == "plans" else m for m in models]


def disconnected_model_models() -> List[SlayerModel]:
    """DEV-1900 graph plus a standalone ``promos`` model with no join path from any
    root — a genuinely unreachable filter reference (``promos.discount``) stays
    dropped-and-warned, the one case DEV-1935 keeps excluded."""
    models = dev1900_models()
    models.append(SlayerModel(
        name="promos", data_source="test", sql_table="promos",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="discount", type=DataType.DOUBLE),
        ]))
    return models


def event_less_models(*, inner_events: bool = False) -> List[SlayerModel]:
    """A self-contained ``customers -> regions -> region_events`` graph. The
    fanning ``regions -> region_events`` hop is LEFT by default; ``inner_events``
    declares it INNER, so an event-less region contributes no product row."""
    events_join = ModelJoin(
        target_model="region_events", join_pairs=[["id", "region_id"]],
        cardinality=JoinCardinality.ONE_TO_MANY,
        join_type=JoinType.INNER if inner_events else JoinType.LEFT,
    )
    regions = SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="pop", type=DataType.DOUBLE),
        ],
        joins=[events_join],
    )
    region_events = SlayerModel(
        name="region_events", data_source="test", sql_table="region_events",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="value", type=DataType.DOUBLE),
        ],
    )
    customers = SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    return [customers, regions, region_events]


# --------------------------------------------------------------------------- #
# Event-less-region dual-engine seed: r3 (West) has NO region_events; ec3 lives
# there so its region_events branch is absent.
# --------------------------------------------------------------------------- #
_EL_REGIONS = [(1, "North", 100.0), (2, "South", 200.0), (3, "West", 300.0)]
_EL_CUSTOMERS = [
    (1, 1, "gold", 100.0), (2, 2, "silver", 80.0), (3, 3, "gold", 90.0)]
_EL_EVENTS = [(1, 1, 50.0), (2, 1, 50.0), (3, 2, 30.0)]  # none for region 3


def _seed_eventless_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop REAL)")
    cur.executemany("INSERT INTO regions VALUES (?,?,?)", _EL_REGIONS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "tier TEXT, spend REAL)")
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?)", _EL_CUSTOMERS)
    cur.execute(
        "CREATE TABLE region_events (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "value REAL)")
    cur.executemany("INSERT INTO region_events VALUES (?,?,?)", _EL_EVENTS)
    con.commit()
    con.close()


def _seed_eventless_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR, pop DOUBLE)")
    con.executemany("INSERT INTO regions VALUES (?,?,?)", _EL_REGIONS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region_id INTEGER, tier VARCHAR, "
        "spend DOUBLE)")
    con.executemany("INSERT INTO customers VALUES (?,?,?,?)", _EL_CUSTOMERS)
    con.execute(
        "CREATE TABLE region_events (id INTEGER, region_id INTEGER, value DOUBLE)")
    con.executemany("INSERT INTO region_events VALUES (?,?,?)", _EL_EVENTS)
    con.close()


async def _engine_for(*, dialect: str, db_path: str,
                      models: List[SlayerModel]) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path))
    for model in models:
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


async def make_eventless_engine(
    request, *, inner_events: bool = False,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture over the mini-graph."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        if dialect == "sqlite":
            _seed_eventless_sqlite(db_path)
        else:
            _seed_eventless_duckdb(db_path)
        yield await _engine_for(
            dialect=dialect, db_path=db_path,
            models=event_less_models(inner_events=inner_events))


__all__ = [
    "SlayerQuery", "ModelMeasure", "SlayerModel",
    "cust_q", "orders_q", "rows_by", "pushed_filter_infos",
    "dev1900_models", "make_exec_engine",
    "two_spellings_models", "unproven_plans_models", "event_less_models",
    "disconnected_model_models", "make_eventless_engine",
    "OR_MIX_LOCAL", "GOLD_OR_OK", "NOT_GOLD_AND_OK", "NO_ORDERS",
    "NEW_OR_EVENT", "ATOM_TWO_BRANCH", "MATERIALISED_OR", "REDUCED_PUSH_OR",
    "TWO_SPELLINGS",
    "OR_MIX_PRODUCER", "ASSOC_OR", "ASSOC_OR_ABSENT",
    "OR_MIX_SPEND", "OR_MIX_SPEND_FAN_DEFECT", "GOLD_OR_OK_SPEND",
    "GOLD_OR_OK_STRICT_INNER", "GOLD_OR_OK_FAN_DEFECT", "NOT_GOLD_AND_OK_SPEND",
    "NOT_GOLD_AND_OK_FAN_DEFECT", "NO_ORDERS_SPEND", "NEW_OR_EVENT_SPEND",
    "ATOM_TWO_BRANCH_SPEND", "MATERIALISED_ORDER_CELLS", "REDUCED_PUSH_ORDER_CELLS",
    "OR_MIX_PARTITIONED",
    "OR_MIX_RAW_ROWS", "TWO_SPELLINGS_BY_TIER", "TWO_SPELLINGS_SPLIT_DEFECT",
    "OR_MIX_PRODUCER_BY_TIER", "ASSOC_OR_BY_STATUS", "ASSOC_OR_ABSENT_BY_STATUS",
    "EVENTLESS_GOLD_OR_EVENT_LEFT", "EVENTLESS_GOLD_OR_EVENT_INNER",
    "EVENTLESS_EVENT_ONLY",
]
