"""Shared fixtures for DEV-1840 — semi-join (EXISTS) filter pushdown into
target-rooted producers.

Underscore-prefixed so pytest skips collection; mirrors the
``tests/_dev1836_fixtures.py`` dual-engine pattern (one hand-computed dataset
seeded into SQLite AND DuckDB).

Join design (the point of this graph)
-------------------------------------
``orders → customers``   safe: customers.id PK. NO reverse edge stored — the
                         canonical inversion shape (``declare_reverse=True``
                         adds the 1:N reverse edge, as DEV-1836 had).
``orders → stores``      safe: composite PK (co, no) — composite inline pin
                         AND, for a stores-rooted producer, a composite
                         inverted correlation.
``customers → regions``  safe: regions.id PK.
``customers → plans``    ``strong_plans=True``: plans.code PK (provable m:1);
                         ``False``: no claim (unproven forward hop → EXISTS).
``orders.cust_tier``     Mode-A derived (``customers.tier``): expanded-deps
                         classification probe.
``customers.last_status``  (reverse variant only) Mode-A derived over the 1:N
                         hop (``orders.status``): must never inline.
``tickets → agents`` ×2  (separate graph) two forward edges, no reverse:
                         ambiguous inversion → excluded.
``tickets → reviews``    safe m:1; from an agents-rooted producer the only
                         route to reviews runs through the ambiguous hop →
                         no resolvable path (genuinely unreachable).

Dataset (hand-computed; every executed expectation derives from here)
---------------------------------------------------------------------
regions: 1 North 100 | 2 South 200
plans:   p1 basic 10 | p2 pro 20 | p3 basic 5
customers (id, region_id, plan_code, tier, spend, signup_at):
   1 r1 p1   gold   100 2024-01-05
   2 r1 p2   silver 150 2024-02-10
   3 r2 p1   gold    60 2024-03-15
   4 -- p2   bronze  40 2024-03-20
   5 r2 p3   silver  80 2024-04-15
   6 r1 --   gold    30 2024-04-18
   7 r2 p2   gold    55 2024-04-20     (zero orders)
   (signup dates stay clear of month boundaries: the trailing-window bucket
    edge is inclusive)
stores (co, no, city, rent): A 1 NYC 500 | A 2 LA 300 | B 1 SF 200 | B 2 DAL 100
   (B,1) shares no with (A,1) and co with (B,2): correlating on only one pair
   column changes values. (B,2)'s only order is web — out of every app EXISTS.
orders (id, customer_id, status, channel, amount, ordered_at, store):
   1 c1 ok  web 10 2024-01-10 A1
   2 c1 new app 20 2024-01-20 A2
   3 c2 ok  web 30 2024-02-10 B1
   4 c2 new app 25 2024-02-20 A1
   5 c3 ok  app  5 2024-03-05 B1
   6 c4 new web 40 2024-03-10 A2
   7 c5 ok  app 15 2024-04-02 B1
   8 -- ok  web  7 2024-03-12 A1      (orphan order)
   9 c6 ok  web 12 2024-04-10 A2
  10 c1 ok  web  3 2024-01-25 B2      (c1's SECOND ok order: inlining a
                                       filter on last_status double-counts c1)
agents: 1 Ann 10 | 2 Bob 20 | 3 Cid 30
reviews: 1 stars 5 | 2 stars 2 | 3 stars 4
tickets (id, opened_by, closed_by, review_id, priority, effort):
   1 a1 a2 r1 high 5 | 2 a2 a3 r2 low 3 | 3 a1 a1 r3 low 2

Key hand-computed populations (spend sums by tier over the customers kept):
all customers:            gold 245 (c1,c3,c6,c7)  silver 230  bronze 40
≥1 app order:             gold 160 (c1,c3)        silver 230 (c2,c5)
≥1 web order:             gold 130 (c1,c6)        silver 150 (c2)  bronze 40
≥1 new order:             c1 (2024-01) c2 (2024-02) c4 (2024-03); none in 2024-04
one ok∧app order:         gold  60 (c3)           silver  80 (c5)
  (split-EXISTS defect: gold 160 / silver 230 — c1 and c2 have an ok order
   and an app order but no single ok app order)
basic plan:               gold 160 (c1,c3)        silver 80 (c5)
≥1 web order ∧ basic:     gold 100 (c1)
spend > amount on ≥1 order: gold 190 (c1,c3,c6)   silver 230 (c2,c5)
≥1 order with cust_tier='gold': 190 (c1,c3,c6; NOT c7 — no orders)
≥1 ok order (last_status probe): gold 190 (c1,c3,c6)  silver 230 (c2,c5)
  (inline-fan defect: gold 290 — c1's spend counted once per ok order)
stores rent, ≥1 app order: 1000 (A1+A2+B1); one app∧gold order: 500 (A2+B1)
  (split defect 1000; single-pair correlation defect 1100 — B2 leaks in)
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, JoinCardinality, TimeGranularity
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1836_fixtures import (
    broadcast_warnings,
    dropped_filter_warnings,
    rows_by,
)
from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Models.
# --------------------------------------------------------------------------- #
def regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="pop", type=DataType.DOUBLE),
        ],
    )


def plans_model(*, strong: bool = True) -> SlayerModel:
    # `strong` puts a PK on the join-target column: provably m:1 vs unproven.
    return SlayerModel(
        name="plans", data_source="test", sql_table="plans",
        columns=[
            Column(name="code", type=DataType.TEXT, primary_key=strong),
            Column(name="level", type=DataType.TEXT),
            Column(name="fee", type=DataType.DOUBLE),
        ],
    )


def stores_model() -> SlayerModel:
    return SlayerModel(
        name="stores", data_source="test", sql_table="stores",
        columns=[
            Column(name="co", type=DataType.TEXT, primary_key=True),
            Column(name="no", type=DataType.INT, primary_key=True),
            Column(name="city", type=DataType.TEXT),
            Column(name="rent", type=DataType.DOUBLE),
        ],
    )


def customers_model(*, declare_reverse: bool = False) -> SlayerModel:
    columns = [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="region_id", type=DataType.INT),
        Column(name="plan_code", type=DataType.TEXT),
        Column(name="tier", type=DataType.TEXT),
        Column(name="spend", type=DataType.DOUBLE),
        Column(name="signup_at", type=DataType.TIMESTAMP),
    ]
    joins = [
        ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
        ModelJoin(target_model="plans", join_pairs=[["plan_code", "code"]]),
    ]
    if declare_reverse:
        joins.append(ModelJoin(
            target_model="orders", join_pairs=[["id", "customer_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        ))
        # Derived over the declared 1:N hop — classification-by-deps probe.
        columns.append(Column(
            name="last_status", type=DataType.TEXT, sql="orders.status",
        ))
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=columns, joins=joins,
    )


def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="channel", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="store_co", type=DataType.TEXT),
            Column(name="store_no", type=DataType.INT),
            # Host-declared, target-reading: expanded-deps probe.
            Column(name="cust_tier", type=DataType.TEXT, sql="customers.tier"),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="stores",
                      join_pairs=[["store_co", "co"], ["store_no", "no"]]),
        ],
    )


def agents_model() -> SlayerModel:
    return SlayerModel(
        name="agents", data_source="test", sql_table="agents",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="score", type=DataType.DOUBLE),
        ],
    )


def reviews_model() -> SlayerModel:
    return SlayerModel(
        name="reviews", data_source="test", sql_table="reviews",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="stars", type=DataType.DOUBLE),
        ],
    )


def tickets_model() -> SlayerModel:
    # TWO forward edges to agents, no reverse: inverting is ambiguous.
    return SlayerModel(
        name="tickets", data_source="test", sql_table="tickets",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="opened_by", type=DataType.INT),
            Column(name="closed_by", type=DataType.INT),
            Column(name="review_id", type=DataType.INT),
            Column(name="priority", type=DataType.TEXT),
            Column(name="effort", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="agents", join_pairs=[["opened_by", "id"]]),
            ModelJoin(target_model="agents", join_pairs=[["closed_by", "id"]]),
            ModelJoin(target_model="reviews", join_pairs=[["review_id", "id"]]),
        ],
    )


def dev1840_models(*, declare_reverse: bool = False,
                   strong_plans: bool = True) -> List[SlayerModel]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [
        orders_model(),
        customers_model(declare_reverse=declare_reverse),
        regions_model(),
        stores_model(),
        plans_model(strong=strong_plans),
    ]


def ambiguity_models() -> List[SlayerModel]:
    return [tickets_model(), agents_model(), reviews_model()]


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def tq(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "tickets")
    return SlayerQuery(**kw)


def bundle(models: Optional[List[SlayerModel]] = None) -> ResolvedSourceBundle:
    models = models if models is not None else dev1840_models()
    return ResolvedSourceBundle(
        source_model=models[0], referenced_models=models[1:],
    )


def signup_month_td() -> List[TimeDimension]:
    return [TimeDimension(
        dimension=ColumnRef(name="customers.signup_at"),
        granularity=TimeGranularity.MONTH,
    )]


async def gen(query: SlayerQuery, *, dialect: str = "duckdb",
              models: Optional[List[SlayerModel]] = None) -> str:
    models = models if models is not None else dev1840_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
        dialect=dialect, validate=False,
    )


# --------------------------------------------------------------------------- #
# Oracles (see module docstring).
# --------------------------------------------------------------------------- #
SPEND_ALL_BY_TIER = {"gold": 245.0, "silver": 230.0, "bronze": 40.0}
SPEND_APP_BY_TIER = {"gold": 160.0, "silver": 230.0}
SPEND_APP_TOTAL = 390.0
SPEND_WEB_BY_TIER = {"gold": 130.0, "silver": 150.0, "bronze": 40.0}
SPEND_OK_APP_SAMEROW = {"gold": 60.0, "silver": 80.0}
SPEND_OK_APP_SPLIT_DEFECT = {"gold": 160.0, "silver": 230.0}
SPEND_BASIC_BY_TIER = {"gold": 160.0, "silver": 80.0}
SPEND_WEB_AND_BASIC = {"gold": 100.0}
SPEND_CORRELATED_GT = {"gold": 190.0, "silver": 230.0}
SPEND_CUST_TIER_GOLD = 190.0
SPEND_LAST_STATUS_OK = {"gold": 190.0, "silver": 230.0}
SPEND_LAST_STATUS_OK_INLINE_FAN_GOLD = 290.0
RENT_APP = 1000.0
RENT_APP_GOLD_SAMEROW = 500.0
RENT_APP_GOLD_SPLIT_DEFECT = 1000.0
RENT_SINGLE_PAIR_DEFECT = 1100.0
SPEND_APP_1Y_BY_SIGNUP_MONTH = {
    "2024-01": 100.0, "2024-02": 250.0, "2024-03": 310.0, "2024-04": 390.0,
}
SPEND_NEW_1Y_BY_SIGNUP_MONTH = {
    "2024-01": 100.0, "2024-02": 250.0, "2024-03": 290.0,
}
SPEND_APP_FIRST_BY_TIER = {"gold": 100.0, "silver": 150.0}
SPEND_APP_LAST_BY_TIER = {"gold": 60.0, "silver": 80.0}

#: Nested computed-dimension producer: per-tier spend band, threshold chosen so
#: the app pushdown flips gold (160 ≤ 170) while unfiltered gold (245) is 'hi'.
SPEND_BAND_170 = (
    "CASE WHEN customers.spend:sum(partition_by=customers.tier) > 170 "
    "THEN 'hi' ELSE 'lo' END"
)


# --------------------------------------------------------------------------- #
# Execution dataset.
# --------------------------------------------------------------------------- #
_REGIONS_ROWS = [(1, "North", 100.0), (2, "South", 200.0)]
_PLANS_ROWS = [("p1", "basic", 10.0), ("p2", "pro", 20.0), ("p3", "basic", 5.0)]
_CUSTOMERS_ROWS = [
    # (id, region_id, plan_code, tier, spend, signup_at)
    (1, 1, "p1", "gold", 100.0, "2024-01-05"),
    (2, 1, "p2", "silver", 150.0, "2024-02-10"),
    (3, 2, "p1", "gold", 60.0, "2024-03-15"),
    (4, None, "p2", "bronze", 40.0, "2024-03-20"),
    (5, 2, "p3", "silver", 80.0, "2024-04-15"),
    (6, 1, None, "gold", 30.0, "2024-04-18"),
    (7, 2, "p2", "gold", 55.0, "2024-04-20"),
]
_STORES_ROWS = [
    ("A", 1, "NYC", 500.0), ("A", 2, "LA", 300.0),
    ("B", 1, "SF", 200.0), ("B", 2, "DAL", 100.0),
]
_ORDERS_ROWS = [
    # (id, customer_id, status, channel, amount, ordered_at, store_co, store_no)
    (1, 1, "ok", "web", 10.0, "2024-01-10", "A", 1),
    (2, 1, "new", "app", 20.0, "2024-01-20", "A", 2),
    (3, 2, "ok", "web", 30.0, "2024-02-10", "B", 1),
    (4, 2, "new", "app", 25.0, "2024-02-20", "A", 1),
    (5, 3, "ok", "app", 5.0, "2024-03-05", "B", 1),
    (6, 4, "new", "web", 40.0, "2024-03-10", "A", 2),
    (7, 5, "ok", "app", 15.0, "2024-04-02", "B", 1),
    (8, None, "ok", "web", 7.0, "2024-03-12", "A", 1),
    (9, 6, "ok", "web", 12.0, "2024-04-10", "A", 2),
    (10, 1, "ok", "web", 3.0, "2024-01-25", "B", 2),
]
_AGENTS_ROWS = [(1, "Ann", 10.0), (2, "Bob", 20.0), (3, "Cid", 30.0)]
_REVIEWS_ROWS = [(1, 5.0), (2, 2.0), (3, 4.0)]
_TICKETS_ROWS = [
    (1, 1, 2, 1, "high", 5.0), (2, 2, 3, 2, "low", 3.0),
    (3, 1, 1, 3, "low", 2.0),
]


def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop REAL)")
    cur.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    cur.execute("CREATE TABLE plans (code TEXT PRIMARY KEY, level TEXT, fee REAL)")
    cur.executemany("INSERT INTO plans VALUES (?,?,?)", _PLANS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "plan_code TEXT, tier TEXT, spend REAL, signup_at TEXT)"
    )
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    cur.execute(
        "CREATE TABLE stores (co TEXT, no INTEGER, city TEXT, rent REAL, "
        "PRIMARY KEY (co, no))"
    )
    cur.executemany("INSERT INTO stores VALUES (?,?,?,?)", _STORES_ROWS)
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "status TEXT, channel TEXT, amount REAL, ordered_at TEXT, "
        "store_co TEXT, store_no INTEGER)"
    )
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_ROWS)
    cur.execute(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, name TEXT, score REAL)"
    )
    cur.executemany("INSERT INTO agents VALUES (?,?,?)", _AGENTS_ROWS)
    cur.execute("CREATE TABLE reviews (id INTEGER PRIMARY KEY, stars REAL)")
    cur.executemany("INSERT INTO reviews VALUES (?,?)", _REVIEWS_ROWS)
    cur.execute(
        "CREATE TABLE tickets (id INTEGER PRIMARY KEY, opened_by INTEGER, "
        "closed_by INTEGER, review_id INTEGER, priority TEXT, effort REAL)"
    )
    cur.executemany("INSERT INTO tickets VALUES (?,?,?,?,?,?)", _TICKETS_ROWS)
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
        "plan_code VARCHAR, tier VARCHAR, spend DOUBLE, signup_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE stores (co VARCHAR, no INTEGER, city VARCHAR, rent DOUBLE)"
    )
    con.executemany("INSERT INTO stores VALUES (?,?,?,?)", _STORES_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR, "
        "channel VARCHAR, amount DOUBLE, ordered_at TIMESTAMP, "
        "store_co VARCHAR, store_no INTEGER)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_ROWS)
    con.execute("CREATE TABLE agents (id INTEGER, name VARCHAR, score DOUBLE)")
    con.executemany("INSERT INTO agents VALUES (?,?,?)", _AGENTS_ROWS)
    con.execute("CREATE TABLE reviews (id INTEGER, stars DOUBLE)")
    con.executemany("INSERT INTO reviews VALUES (?,?)", _REVIEWS_ROWS)
    con.execute(
        "CREATE TABLE tickets (id INTEGER, opened_by INTEGER, "
        "closed_by INTEGER, review_id INTEGER, priority VARCHAR, effort DOUBLE)"
    )
    con.executemany("INSERT INTO tickets VALUES (?,?,?,?,?,?)", _TICKETS_ROWS)
    con.close()


async def _engine_for(*, dialect: str, db_path: str,
                      models: List[SlayerModel]) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path)
    )
    for model in models:
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


async def make_exec_engine(
    request, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; each test module
    wraps this in ``@pytest.fixture`` so the fixture name lives where used."""
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
            models=models if models is not None else dev1840_models(),
        )


def month_key(value) -> str:
    """Stable per-month key across SQLite text and DuckDB timestamp values."""
    return str(value)[:7]


__all__ = [
    "orders_model", "customers_model", "regions_model", "stores_model",
    "plans_model", "agents_model", "tickets_model", "reviews_model",
    "dev1840_models", "ambiguity_models",
    "q", "tq", "bundle", "gen", "signup_month_td", "month_key", "rows_by",
    "broadcast_warnings", "dropped_filter_warnings", "make_exec_engine",
    "SPEND_ALL_BY_TIER", "SPEND_APP_BY_TIER", "SPEND_APP_TOTAL",
    "SPEND_WEB_BY_TIER",
    "SPEND_OK_APP_SAMEROW", "SPEND_OK_APP_SPLIT_DEFECT",
    "SPEND_BASIC_BY_TIER", "SPEND_WEB_AND_BASIC", "SPEND_CORRELATED_GT",
    "SPEND_CUST_TIER_GOLD", "SPEND_LAST_STATUS_OK",
    "SPEND_LAST_STATUS_OK_INLINE_FAN_GOLD", "RENT_APP",
    "RENT_APP_GOLD_SAMEROW",
    "RENT_APP_GOLD_SPLIT_DEFECT", "RENT_SINGLE_PAIR_DEFECT",
    "SPEND_APP_1Y_BY_SIGNUP_MONTH", "SPEND_NEW_1Y_BY_SIGNUP_MONTH",
    "SPEND_APP_FIRST_BY_TIER",
    "SPEND_APP_LAST_BY_TIER", "SPEND_BAND_170",
    "SlayerQuery", "ModelMeasure", "ColumnRef", "TimeDimension",
    "TimeGranularity", "ModelJoin", "Column", "SlayerModel", "DataType",
    "JoinCardinality",
]
