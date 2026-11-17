"""Shared fixtures for DEV-1840 — semi-join (EXISTS) filter pushdown into
target-rooted producers.

Underscore-prefixed so pytest skips collection; mirrors the
``tests/_dev1836_fixtures.py`` dual-engine pattern (one hand-computed dataset
seeded into SQLite AND DuckDB).

Join design (the point of this graph)
-------------------------------------
``orders → customers``   safe: customers.id PK. One stored edge — the
                         canonical inversion shape. ``declare_reverse=True``
                         moves the declaration to the customers side
                         (``customers → orders``, 1:N) — mirror-parity
                         variant, same single edge (DEV-1853).
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
``tickets → agents`` ×2  (separate graph) two unnamed parallel edges: every
                         hop across the pair — measure, filter, correlation,
                         either direction — fails closed (DEV-1853).
``tickets → reviews``    safe m:1 — the graph's healthy leg.

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

from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, RelationshipCardinality, TimeGranularity
from slayer.core.models import (
    DatasetField,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import FieldRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

from tests._dev1836_fixtures import (
    broadcast_warnings,
    rows_by,
)
from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Models.
# --------------------------------------------------------------------------- #
def regions_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
            DatasetField(name="pop", datatype=DataType.DOUBLE),
        ],
    )


def plans_dataset(*, strong: bool = True) -> SlayerDataset:
    # `strong` puts a PK on the join-target column: provably m:1 vs unproven.
    return SlayerDataset(
        name="plans", data_source="test", sql_table="plans",
        fields=[
            DatasetField(name="code", datatype=DataType.TEXT, primary_key=strong),
            DatasetField(name="level", datatype=DataType.TEXT),
            DatasetField(name="fee", datatype=DataType.DOUBLE),
        ],
    )


def stores_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="stores", data_source="test", sql_table="stores",
        fields=[
            DatasetField(name="co", datatype=DataType.TEXT, primary_key=True),
            DatasetField(name="no", datatype=DataType.INT, primary_key=True),
            DatasetField(name="city", datatype=DataType.TEXT),
            DatasetField(name="rent", datatype=DataType.DOUBLE),
        ],
    )


def customers_dataset(*, declare_reverse: bool = False) -> SlayerDataset:
    """``declare_reverse=True``: customers OWNS the orders edge (1:N) and the
    ``last_status`` Mode-A probe; orders then declares no customers edge — one
    symmetric edge either way (DEV-1853)."""
    columns = [
        DatasetField(name="id", datatype=DataType.INT, primary_key=True),
        DatasetField(name="region_id", datatype=DataType.INT),
        DatasetField(name="plan_code", datatype=DataType.TEXT),
        DatasetField(name="tier", datatype=DataType.TEXT),
        DatasetField(name="spend", datatype=DataType.DOUBLE),
        DatasetField(name="signup_at", datatype=DataType.TIMESTAMP),
    ]
    joins = [
        DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]]),
        DatasetRelationship(to="plans", join_pairs=[["plan_code", "code"]]),
    ]
    if declare_reverse:
        joins.append(DatasetRelationship(
            to="orders", join_pairs=[["id", "customer_id"]],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        ))
        # Derived over the declared 1:N hop — classification-by-deps probe.
        columns.append(DatasetField(
            name="last_status", datatype=DataType.TEXT, sql="orders.status",
        ))
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=columns, relationships=joins,
    )


def orders_dataset(*, customers_edge: bool = True) -> SlayerDataset:
    joins = [
        DatasetRelationship(to="stores",
                  join_pairs=[["store_co", "co"], ["store_no", "no"]]),
    ]
    if customers_edge:
        joins.insert(0, DatasetRelationship(
            to="customers", join_pairs=[["customer_id", "id"]],
        ))
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INT),
            DatasetField(name="status", datatype=DataType.TEXT),
            DatasetField(name="channel", datatype=DataType.TEXT),
            DatasetField(name="amount", datatype=DataType.DOUBLE),
            DatasetField(name="ordered_at", datatype=DataType.TIMESTAMP),
            DatasetField(name="store_co", datatype=DataType.TEXT),
            DatasetField(name="store_no", datatype=DataType.INT),
            # Host-declared, target-reading: expanded-deps probe.
            DatasetField(name="cust_tier", datatype=DataType.TEXT, sql="customers.tier"),
        ],
        relationships=joins,
    )


def agents_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="agents", data_source="test", sql_table="agents",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
            DatasetField(name="score", datatype=DataType.DOUBLE),
        ],
    )


def reviews_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="reviews", data_source="test", sql_table="reviews",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="stars", datatype=DataType.DOUBLE),
        ],
    )


def tickets_dataset() -> SlayerDataset:
    # TWO unnamed parallel edges to agents: any hop across the pair fails closed.
    return SlayerDataset(
        name="tickets", data_source="test", sql_table="tickets",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="opened_by", datatype=DataType.INT),
            DatasetField(name="closed_by", datatype=DataType.INT),
            DatasetField(name="review_id", datatype=DataType.INT),
            DatasetField(name="priority", datatype=DataType.TEXT),
            DatasetField(name="effort", datatype=DataType.DOUBLE),
        ],
        relationships=[
            DatasetRelationship(to="agents", join_pairs=[["opened_by", "id"]]),
            DatasetRelationship(to="agents", join_pairs=[["closed_by", "id"]]),
            DatasetRelationship(to="reviews", join_pairs=[["review_id", "id"]]),
        ],
    )


def dev1840_datasets(*, declare_reverse: bool = False,
                   strong_plans: bool = True) -> List[SlayerDataset]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [
        orders_dataset(customers_edge=not declare_reverse),
        customers_dataset(declare_reverse=declare_reverse),
        regions_dataset(),
        stores_dataset(),
        plans_dataset(strong=strong_plans),
    ]


def ambiguity_datasets() -> List[SlayerDataset]:
    return [tickets_dataset(), agents_dataset(), reviews_dataset()]


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(**kw)


def tq(**kw) -> SlayerQuery:
    kw.setdefault("population", "tickets")
    return SlayerQuery(**kw)


def bundle(datasets: Optional[List[SlayerDataset]] = None) -> ResolvedSourceBundle:
    datasets = datasets if datasets is not None else dev1840_datasets()
    return ResolvedSourceBundle(
        dialect="postgres",
        population=datasets[0], referenced_datasets=datasets[1:],
    )


def signup_month_td() -> List[TimeDimension]:
    return [TimeDimension(
        field="customers.signup_at",
        granularity=TimeGranularity.MONTH,
    )]


async def gen(query: SlayerQuery, *, dialect: str = "duckdb",
              datasets: Optional[List[SlayerDataset]] = None) -> str:
    datasets = datasets if datasets is not None else dev1840_datasets()
    return await _engine_generate(
        query=query, dataset=datasets[0], extra_datasets=datasets[1:],
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
    "CASE WHEN sum(customers.spend, partition_by=customers.tier) > 170 "
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
    with transaction(db_path) as con:
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


async def make_exec_engine(
    request, *, datasets: Optional[List[SlayerDataset]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; each test module
    wraps this in ``@pytest.fixture`` so the fixture name lives where used."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, datasets=datasets if datasets is not None else dev1840_datasets(),
    ) as (engine, _db):
        yield engine


def month_key(value) -> str:
    """Stable per-month key across SQLite text and DuckDB timestamp values."""
    return str(value)[:7]


__all__ = [
    "orders_dataset", "customers_dataset", "regions_dataset", "stores_dataset",
    "plans_dataset", "agents_dataset", "tickets_dataset", "reviews_dataset",
    "dev1840_datasets", "ambiguity_datasets",
    "q", "tq", "bundle", "gen", "signup_month_td", "month_key", "rows_by",
    "broadcast_warnings", "make_exec_engine",
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
    "SlayerQuery", "DatasetMetric", "FieldRef", "TimeDimension",
    "TimeGranularity", "DatasetRelationship", "DatasetField", "SlayerDataset", "DataType",
    "RelationshipCardinality",
]
