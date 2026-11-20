"""Shared fixtures for cross-dataset unification: target-rooted
regroup producers, fan-out-safe grains, broadcast semantics, strict mode.

Underscore-prefixed so pytest skips collection (like ``tests/_dev1739_fixtures.py``,
whose dual-engine pattern this mirrors). One hand-computed dataset seeded into
SQLite AND DuckDB; ``make_exec_engine`` yields an engine per backend.

Relationship-arity design (the point of this chain)
---------------------------------------------------
``orders → customers``           safe: customers.id is PK (structural proof)
``customers → regions``          safe: regions.id is PK (structural proof)
``customers → segments``         UNPROVEN: segments.code has no PK/unique claim
``customers → orders``           UNSAFE: the inverted orders→customers edge is
                                 an unproven 1:N orientation (no
                                 declared reverse edge; traversal is automatic)

So, for an aggregate rooted at ``customers``: ``tier`` is zero-hop (exact),
``regions.name`` is provably safe (exact), ``segments.label`` is unproven
(broadcast), and every ``orders``-level dimension/filter crosses the inverted
1:N orientation (broadcast / dropped filter). ``sum(customers.regions.pop)``
roots at ``regions``: every hop away from it is an inverted fan-out
orientation, so every query dimension broadcasts. Unsafe-input probes:
``customers.seg_label`` (Mode-A derived over the unproven hop) and
``customers.vip_spend`` (``DatasetField.filter`` over the unproven hop).

Dataset (hand-computed; every executed expectation derives from here)
---------------------------------------------------------------------
regions (id, name, pop):   1 North 100 | 2 South 200          pop total = 300
segments (code, label, discount):  a Alpha 1.0 | b Beta 2.0
customers (id, region_id, segment_code, tier, spend, signup_at):
   1  r1    a     gold    100  2024-01-05
   2  r1    b     silver  150  2024-02-10
   3  r2    a     gold     60  2024-03-15
   4  NULL  NULL  bronze   40  2024-03-20      (NULL region/segment)
orders (id, customer_id, status, channel, amount, ordered_at):
   1  c1    ok   web  10  2024-01-10
   2  c1    new  app  20  2024-01-20
   3  c2    ok   web  30  2024-02-10
   4  c3    ok   app   5  2024-02-15
   5  c3    ok   app   5  2024-03-05
   6  c4    new  web  40  2024-03-10
   7  NULL  ok   web   7  2024-03-12          (orphan order, NULL customer)

spend: total=350; by tier gold=160 silver=150 bronze=40; by regions.name
North=250 South=60 NULL=40; hi/lo spend-band (per-tier>100) hi=310 lo=40.
Naive join-multiplied spend (100×2 + 150×1 + 60×2 + 40×1) = 510 — the fan-out
defect value that must never appear.
amount: total=117; by status ok=57 new=60; by channel web=87 app=30; by tier
gold=40 silver=30 bronze=40 NULL=7; by (tier,status) (gold,ok)=20 (gold,new)=20
(silver,ok)=30 (bronze,new)=40 (NULL,ok)=7; by spend-band hi=70 lo=47.
spend trailing-1y by signup month: Jan=100 Feb=250 Mar=350.
spend first/last by signup_at: first=100 (c1) last=40 (c4); by tier gold
first=100 last=60, silver 150/150, bronze 40/40.
"""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, RelationshipCardinality, TimeGranularity
from slayer.core.models import (
    DatasetField,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import FieldRef, SlayerQuery, TimeDimension
from slayer.core.warnings import BroadcastGrainWarningPayload
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._engine_helpers import seeded_exec_engine
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Datasets — orders (host) → customers → {regions, segments}; the reverse
# customers → orders direction is the inverted forward edge.
# --------------------------------------------------------------------------- #
def regions_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(name="pop", datatype=DataType.FLOAT),
        ],
    )


def segments_dataset() -> SlayerDataset:
    # `code` is the relationship target but carries NO primary_key/unique claim:
    # the customers → segments hop is deliberately unproven.
    return SlayerDataset(
        name="segments", data_source="test", sql_table="segments",
        fields=[
            DatasetField(name="code", datatype=DataType.STRING),
            DatasetField(name="label", datatype=DataType.STRING),
            DatasetField(name="discount", datatype=DataType.FLOAT),
        ],
    )


def customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
            DatasetField(name="segment_code", datatype=DataType.STRING),
            DatasetField(name="tier", datatype=DataType.STRING),
            DatasetField(name="spend", datatype=DataType.FLOAT),
            DatasetField(name="signup_at", datatype=DataType.DATETIME),
            # Target-local filtered field (safe; CASE-WHEN at aggregation time):
            DatasetField(name="gold_spend", datatype=DataType.FLOAT, expression="spend",
                   filter="tier = 'gold'"),
            # Unsafe-input probes (only planned when referenced):
            DatasetField(name="seg_label", datatype=DataType.STRING, expression="segments.label"),
            DatasetField(name="vip_spend", datatype=DataType.FLOAT, expression="spend",
                   filter="segments.label = 'Alpha'"),
        ],
        relationships=[
            DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"]),
            DatasetRelationship(to="segments",
                      from_columns=["segment_code"], to_columns=["code"]),
        ],
    )


def orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="channel", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="ordered_at", datatype=DataType.DATETIME),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
        ],
    )


def inventory_dataset() -> SlayerDataset:
    """Composite-PK target for the F6 predicate tests (never seeded)."""
    return SlayerDataset(
        name="inventory", data_source="test", sql_table="inventory",
        fields=[
            DatasetField(name="wh", datatype=DataType.STRING, primary_key=True),
            DatasetField(name="sku", datatype=DataType.STRING, primary_key=True),
            DatasetField(name="qty", datatype=DataType.FLOAT),
        ],
    )


def dev1836_datasets() -> List[SlayerDataset]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [orders_dataset(), customers_dataset(), regions_dataset(), segments_dataset()]


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(**kw)


def month_td(column: str = "orders.ordered_at") -> List[TimeDimension]:
    return [TimeDimension(
        field=column,
        granularity=TimeGranularity.MONTH,
    )]


#: Cross-dataset aggregate source inside a computed dimension (per-tier spend band).
SPEND_BAND = (
    "CASE WHEN sum(customers.spend, partition_by=customers.tier) > 100 "
    "THEN 'hi' ELSE 'lo' END"
)


async def gen(query: SlayerQuery, *, dialect: str = "duckdb",
              validate: bool = False) -> str:
    models = dev1836_datasets()
    return await _engine_generate(
        query=query, dataset=models[0], extra_datasets=models[1:],
        dialect=dialect, validate=validate,
    )


# --------------------------------------------------------------------------- #
# Oracles (see module docstring).
# --------------------------------------------------------------------------- #
SPEND_TOTAL = 350.0
SPEND_BY_TIER = {"gold": 160.0, "silver": 150.0, "bronze": 40.0}
SPEND_BY_REGION = {"North": 250.0, "South": 60.0, None: 40.0}
SPEND_BY_BAND = {"hi": 310.0, "lo": 40.0}
SPEND_FANNED = 510.0  # join-multiplied defect value — must never appear
POP_TOTAL = 300.0
AMOUNT_TOTAL = 117.0
AMOUNT_BY_STATUS = {"ok": 57.0, "new": 60.0}
AMOUNT_BY_CHANNEL = {"web": 87.0, "app": 30.0}
AMOUNT_BY_TIER = {"gold": 40.0, "silver": 30.0, "bronze": 40.0, None: 7.0}
AMOUNT_BY_TIER_STATUS = {
    ("gold", "ok"): 20.0, ("gold", "new"): 20.0, ("silver", "ok"): 30.0,
    ("bronze", "new"): 40.0, (None, "ok"): 7.0,
}
AMOUNT_BY_BAND = {"hi": 70.0, "lo": 47.0}
AMOUNT_BY_LABEL = {"Alpha": 40.0, "Beta": 30.0, None: 47.0}
GOLD_SPEND_BY_REGION = {"North": 100.0, "South": 60.0, None: None}
SPEND_1Y_BY_SIGNUP_MONTH = {"2024-01": 100.0, "2024-02": 250.0, "2024-03": 350.0}
SPEND_FIRST_BY_TIER = {"gold": 100.0, "silver": 150.0, "bronze": 40.0}
SPEND_LAST_BY_TIER = {"gold": 60.0, "silver": 150.0, "bronze": 40.0}


# --------------------------------------------------------------------------- #
# Execution dataset.
# --------------------------------------------------------------------------- #
_REGIONS_ROWS = [(1, "North", 100.0), (2, "South", 200.0)]
_SEGMENTS_ROWS = [("a", "Alpha", 1.0), ("b", "Beta", 2.0)]
_CUSTOMERS_ROWS = [
    # (id, region_id, segment_code, tier, spend, signup_at)
    (1, 1, "a", "gold", 100.0, "2024-01-05"),
    (2, 1, "b", "silver", 150.0, "2024-02-10"),
    (3, 2, "a", "gold", 60.0, "2024-03-15"),
    (4, None, None, "bronze", 40.0, "2024-03-20"),
]
_ORDERS_ROWS = [
    # (id, customer_id, status, channel, amount, ordered_at)
    (1, 1, "ok", "web", 10.0, "2024-01-10"),
    (2, 1, "new", "app", 20.0, "2024-01-20"),
    (3, 2, "ok", "web", 30.0, "2024-02-10"),
    (4, 3, "ok", "app", 5.0, "2024-02-15"),
    (5, 3, "ok", "app", 5.0, "2024-03-05"),
    (6, 4, "new", "web", 40.0, "2024-03-10"),
    (7, None, "ok", "web", 7.0, "2024-03-12"),
]


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop REAL)")
        cur.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
        cur.execute("CREATE TABLE segments (code TEXT, label TEXT, discount REAL)")
        cur.executemany("INSERT INTO segments VALUES (?,?,?)", _SEGMENTS_ROWS)
        cur.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
            "segment_code TEXT, tier TEXT, spend REAL, signup_at TEXT)"
        )
        cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
            "status TEXT, channel TEXT, amount REAL, ordered_at TEXT)"
        )
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", _ORDERS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR, pop DOUBLE)")
    con.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    con.execute("CREATE TABLE segments (code VARCHAR, label VARCHAR, discount DOUBLE)")
    con.executemany("INSERT INTO segments VALUES (?,?,?)", _SEGMENTS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region_id INTEGER, "
        "segment_code VARCHAR, tier VARCHAR, spend DOUBLE, signup_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR, "
        "channel VARCHAR, amount DOUBLE, ordered_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", _ORDERS_ROWS)
    con.close()


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; each test module
    wraps this in ``@pytest.fixture`` so the fixture name lives where used."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, datasets=dev1836_datasets()) as (engine, _db):
        yield engine


def month_key(value) -> str:
    """Stable per-month key across SQLite text and DuckDB timestamp values."""
    return str(value)[:7]


def rows_by(resp, *keys) -> dict:
    """Index ``resp.data`` rows by the given result-column key tuple."""
    out = {}
    for r in resp.data:
        out[tuple(r[k] for k in keys)] = r
    assert len(out) == len(resp.data), "duplicate result rows for one group key"
    return out


def broadcast_warnings(resp: SlayerResponse) -> list[BroadcastGrainWarningPayload]:
    """The response's ``kind == "broadcast"`` warnings."""
    return [w for w in (resp.warnings or [])
            if isinstance(w, BroadcastGrainWarningPayload)]


__all__ = [
    "orders_dataset", "customers_dataset", "regions_dataset", "segments_dataset",
    "inventory_dataset", "dev1836_datasets",
    "q", "gen", "month_td", "month_key", "rows_by",
    "broadcast_warnings",
    "SPEND_BAND",
    "SPEND_TOTAL", "SPEND_BY_TIER", "SPEND_BY_REGION", "SPEND_BY_BAND",
    "SPEND_FANNED", "POP_TOTAL", "AMOUNT_TOTAL", "AMOUNT_BY_STATUS",
    "AMOUNT_BY_CHANNEL", "AMOUNT_BY_TIER", "AMOUNT_BY_TIER_STATUS",
    "AMOUNT_BY_BAND", "AMOUNT_BY_LABEL", "GOLD_SPEND_BY_REGION",
    "SPEND_1Y_BY_SIGNUP_MONTH",
    "SPEND_FIRST_BY_TIER", "SPEND_LAST_BY_TIER",
    "SlayerQuery", "DatasetMetric", "FieldRef", "TimeDimension",
    "TimeGranularity", "DatasetRelationship", "DatasetField", "SlayerDataset", "DataType",
    "RelationshipCardinality",
]
