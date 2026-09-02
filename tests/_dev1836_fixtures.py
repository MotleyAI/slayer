"""Shared fixtures for DEV-1836 — cross-model unification: target-rooted
regroup producers, fan-out-safe grains, broadcast semantics, strict mode.

Underscore-prefixed so pytest skips collection (like ``tests/_dev1739_fixtures.py``,
whose dual-engine pattern this mirrors). One hand-computed dataset seeded into
SQLite AND DuckDB; ``make_exec_engine`` yields an engine per backend.

Join-arity design (the point of this chain)
-------------------------------------------
``orders → customers``           safe: customers.id is PK (structural proof)
``customers → regions``          safe: regions.id is PK (structural proof)
``customers → segments``         UNPROVEN: segments.code has no PK/unique claim
``customers → orders`` (reverse) UNSAFE: declared ``cardinality=one_to_many``

So, for an aggregate rooted at ``customers``: ``tier`` is zero-hop (exact),
``regions.name`` is provably safe (exact), ``segments.label`` is unproven
(broadcast), and every ``orders``-level dimension/filter crosses the declared
1:N edge (broadcast / dropped filter). ``customers.regions.pop:sum`` roots at
``regions``: nothing else is reachable from there (no stored reverse edges), so
every query dimension broadcasts. Unsafe-input probes: ``customers.seg_label``
(Mode-A derived over the unproven hop) and ``customers.vip_spend``
(``Column.filter`` over the unproven hop).

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

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List

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
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Models — orders (host) → customers → {regions, segments}; declared reverse
# customers → orders (one_to_many).
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


def segments_model() -> SlayerModel:
    # `code` is the join target but carries NO primary_key/unique claim:
    # the customers → segments hop is deliberately unproven.
    return SlayerModel(
        name="segments", data_source="test", sql_table="segments",
        columns=[
            Column(name="code", type=DataType.TEXT),
            Column(name="label", type=DataType.TEXT),
            Column(name="discount", type=DataType.DOUBLE),
        ],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="segment_code", type=DataType.TEXT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
            # Target-local filtered column (safe; CASE-WHEN at aggregation time):
            Column(name="gold_spend", type=DataType.DOUBLE, sql="spend",
                   filter="tier = 'gold'"),
            # Unsafe-input probes (only planned when referenced):
            Column(name="seg_label", type=DataType.TEXT, sql="segments.label"),
            Column(name="vip_spend", type=DataType.DOUBLE, sql="spend",
                   filter="segments.label = 'Alpha'"),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
            ModelJoin(target_model="segments",
                      join_pairs=[["segment_code", "code"]]),
            ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]],
                      cardinality=JoinCardinality.ONE_TO_MANY),
        ],
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
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def inventory_model() -> SlayerModel:
    """Composite-PK target for the F6 predicate tests (never seeded)."""
    return SlayerModel(
        name="inventory", data_source="test", sql_table="inventory",
        columns=[
            Column(name="wh", type=DataType.TEXT, primary_key=True),
            Column(name="sku", type=DataType.TEXT, primary_key=True),
            Column(name="qty", type=DataType.DOUBLE),
        ],
    )


def dev1836_models() -> List[SlayerModel]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [orders_model(), customers_model(), regions_model(), segments_model()]


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def month_td(column: str = "ordered_at") -> List[TimeDimension]:
    return [TimeDimension(
        dimension=ColumnRef(name=column),
        granularity=TimeGranularity.MONTH,
    )]


#: Cross-model aggregate source inside a computed dimension (per-tier spend band).
SPEND_BAND = (
    "CASE WHEN customers.spend:sum(partition_by=customers.tier) > 100 "
    "THEN 'hi' ELSE 'lo' END"
)


async def gen(query: SlayerQuery, *, dialect: str = "duckdb",
              validate: bool = False) -> str:
    models = dev1836_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
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
    con = sqlite3.connect(db_path)
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
    con.commit()
    con.close()


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


async def _engine_for(*, dialect: str, db_path: str) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path)
    )
    for model in dev1836_models():
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
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
        yield await _engine_for(dialect=dialect, db_path=db_path)


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


def broadcast_warnings(resp) -> list:
    """The response's ``kind == "broadcast"`` warnings (DEV-1836 D6)."""
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == "broadcast"]


def dropped_filter_warnings(resp) -> list:
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == "unreachable_filter_dropped"]


__all__ = [
    "orders_model", "customers_model", "regions_model", "segments_model",
    "inventory_model", "dev1836_models",
    "q", "gen", "month_td", "month_key", "rows_by",
    "broadcast_warnings", "dropped_filter_warnings",
    "SPEND_BAND",
    "SPEND_TOTAL", "SPEND_BY_TIER", "SPEND_BY_REGION", "SPEND_BY_BAND",
    "SPEND_FANNED", "POP_TOTAL", "AMOUNT_TOTAL", "AMOUNT_BY_STATUS",
    "AMOUNT_BY_CHANNEL", "AMOUNT_BY_TIER", "AMOUNT_BY_TIER_STATUS",
    "AMOUNT_BY_BAND", "AMOUNT_BY_LABEL", "GOLD_SPEND_BY_REGION",
    "SPEND_1Y_BY_SIGNUP_MONTH",
    "SPEND_FIRST_BY_TIER", "SPEND_LAST_BY_TIER",
    "SlayerQuery", "ModelMeasure", "ColumnRef", "TimeDimension",
    "TimeGranularity", "ModelJoin", "Column", "SlayerModel", "DataType",
    "JoinCardinality",
]
