"""Shared fixtures for DEV-1838 — node discipline: producer interning,
family unification under per-role crossing-input safety, CTE-body lifts.

Underscore-prefixed so pytest skips collection (like ``tests/_dev1739_fixtures.py``,
whose dual-engine pattern this mirrors). One hand-computed dataset seeded into
SQLite AND DuckDB; ``make_exec_engine`` yields an engine per backend.

Join-arity design (the point of this chain)
-------------------------------------------
``orders → customers``           safe: customers.id is PK (structural proof)
``customers → regions``          safe: regions.id is PK (structural proof)
``customers → segments``         UNPROVEN + genuinely fanning: segments.code has
                                 no PK/unique claim AND code 'a' has two rows
``orders → tags``                UNPROVEN + genuinely fanning: two tags on
                                 orders 1 and 3

Per-role probes on the host (all host-rooted producers):
``gold_amount``   Column.filter over the PROVEN hop (customers.tier)  → safe
``alpha_amount``  Column.filter over the UNPROVEN 2nd hop (segments)  → fans
``rush_amount``   Column.filter over the UNPROVEN 1:N hop (tags)      → fans
``wscaled_sum``   aggregation param over PROVEN hops (regions.weight) → safe
``tscaled_sum``   aggregation param over the UNPROVEN 1:N (tags)      → fans
host-grain wrap   ORDER BY an unprojected joined column               → legal

Dataset (hand-computed; every executed expectation derives from here)
---------------------------------------------------------------------
regions (id, name, weight):  1 North 2.0 | 2 South 3.0
segments (code, label, boost, updated_at):
   a Alpha 1.0 2024-01-01 | a Alpha 1.5 2024-06-01  (duplicate 'a' — real fan)
   b Beta  2.0 2024-02-01
customers (id, region_id, segment_code, tier, spend, signup_at):
   1  r1    a     gold    100  2024-01-05
   2  r1    b     silver  150  2024-02-10
   3  r2    a     silver   60  2024-03-15
   4  NULL  NULL  bronze   40  2024-03-20     (NULL region/segment)
orders (id, customer_id, region, city, amount, status, ordered_at):
   1  c1    North  CityA  10  ok   2024-01-10
   2  c1    North  CityA  20  new  2024-01-20
   3  c2    North  CityB  30  ok   2024-02-10
   4  c3    South  CityC   5  ok   2024-02-15
   5  c3    South  CityC   5  ok   2024-03-05
   6  c4    South  CityD  40  new  2024-03-10
   7  NULL  NULL   CityD   7  ok   2024-03-12  (orphan order, NULL customer)
tags (order_id, kind, factor):
   1 rush 2.0 | 1 gift 1.0 | 3 rush 3.0 | 3 rush 0.5 | 6 promo 0.2

amount: total=117; by status ok=57 new=60; by region North=60 South=50 NULL=7;
by city CityA=30 CityB=30 CityC=10 CityD=47.  BAND25 (city total > 25):
CityA=1 CityB=1 CityC=0 CityD=1.
gold_amount by status (c1 only): ok=10 new=20.
alpha_amount true intent (c1,c3): ok=20 new=20; FANNED (both 'a' rows pass the
label filter, doubling every c1/c3 order): ok=40 new=40.
rush_amount true intent (orders 1,3 once each): ok=40; FANNED (order 1 ×2 tags
keeps 1 match = 10, order 3 ×2 rush = 60): ok=70.
wscaled_sum = SUM(amount * regions.weight): ok = 10*2+30*2+5*3+5*3 = 110,
new = 20*2 = 40 (c4 NULL region and the orphan contribute NULL).
tscaled_sum = SUM(amount * tags.factor) over the fanning join:
ok = 10*2+10*1+30*3+30*0.5 = 135, new = 40*0.2 = 8.
tags.factor per status: MIN ok=0.5 new=0.2 (ASC → new first);
MAX ok=3.0 new=0.2 (DESC → ok first).
customers.spend: total=350; by tier gold=100 silver=210 bronze=40.
ok_amount (amount where status='ok'): windowed at (North, band 1, Jan) = 10
(drops the Jan 20 'new' row; amount's own w = 30).
window='45d' at (North, band 1, Feb): 45d back from Feb's end (~Jan 15)
drops the Jan 10 row → 50 (vs 60 for '90d'); no row sits within 4 days of
the boundary.

Multi-stage banded (stage-1 groups by region + BAND25, bt = amount:sum):
(North,1)=60  (South,0)=10  (South,1)=40  (NULL,1)=7.
Stage-2 filter band==1 by region: North=60 South=40 NULL=7; re-agg by band:
1→107, 0→10.  Grand-total attach gt = amount:sum(partition_by=[]) = 117.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1835_fixtures import cte_aliases  # noqa: F401 — re-exported
from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Models — orders (host) → customers → {regions, segments}; orders → tags.
# --------------------------------------------------------------------------- #
def regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="weight", type=DataType.DOUBLE),
        ],
    )


def segments_model() -> SlayerModel:
    # `code` carries NO primary_key/unique claim AND the data holds two 'a'
    # rows: the customers → segments hop is unproven and genuinely fans.
    return SlayerModel(
        name="segments", data_source="test", sql_table="segments",
        columns=[
            Column(name="code", type=DataType.TEXT),
            Column(name="label", type=DataType.TEXT),
            Column(name="boost", type=DataType.DOUBLE),
            Column(name="updated_at", type=DataType.TIMESTAMP),
        ],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        default_time_dimension="signup_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="segment_code", type=DataType.TEXT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
            ModelJoin(target_model="segments",
                      join_pairs=[["segment_code", "code"]]),
        ],
    )


def tags_model() -> SlayerModel:
    # `order_id` has no PK/unique claim; orders 1 and 3 carry two tags each.
    return SlayerModel(
        name="tags", data_source="test", sql_table="tags",
        columns=[
            Column(name="order_id", type=DataType.INT),
            Column(name="kind", type=DataType.TEXT),
            Column(name="factor", type=DataType.DOUBLE),
        ],
    )


def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="region", type=DataType.TEXT),
            Column(name="city", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            # Same-model filtered column (no isolation):
            Column(name="ok_amount", type=DataType.DOUBLE, sql="amount",
                   filter="status = 'ok'"),
            # Filtered-local probes (CASE-WHEN at aggregation time):
            Column(name="gold_amount", type=DataType.DOUBLE, sql="amount",
                   filter="customers.tier = 'gold'"),
            Column(name="alpha_amount", type=DataType.DOUBLE, sql="amount",
                   filter="customers.segments.label = 'Alpha'"),
            Column(name="rush_amount", type=DataType.DOUBLE, sql="amount",
                   filter="tags.kind = 'rush'"),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="tags", join_pairs=[["id", "order_id"]]),
        ],
        aggregations=[
            Aggregation(
                name="wscaled_sum", formula="SUM({value} * {w})",
                params=[AggregationParam(
                    name="w", sql="customers.regions.weight",
                )],
            ),
            Aggregation(
                name="tscaled_sum", formula="SUM({value} * {f})",
                params=[AggregationParam(name="f", sql="tags.factor")],
            ),
        ],
    )


def dev1838_models() -> List[SlayerModel]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [
        orders_model(), customers_model(), regions_model(), segments_model(),
        tags_model(),
    ]


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


#: Computed dimension banding the city totals at > 25 (row regroup attach).
BAND25 = "CASE WHEN amount:sum(partition_by=city) > 25 THEN 1 ELSE 0 END"
BAND = {"expression": BAND25, "name": "band"}

#: Cross-model banded dimension (customers-rooted producer, per-tier spend).
SPEND_BAND = (
    "CASE WHEN customers.spend:sum(partition_by=customers.tier) > 100 "
    "THEN 'hi' ELSE 'lo' END"
)


async def gen(query: SlayerQuery, *, dialect: str = "duckdb",
              validate: bool = False) -> str:
    models = dev1838_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
        dialect=dialect, validate=validate,
    )


# --------------------------------------------------------------------------- #
# Oracles (see module docstring).
# --------------------------------------------------------------------------- #
AMOUNT_TOTAL = 117.0
AMOUNT_BY_STATUS = {"ok": 57.0, "new": 60.0}
AMOUNT_BY_REGION = {"North": 60.0, "South": 50.0, None: 7.0}
CITY_TOTAL = {"CityA": 30.0, "CityB": 30.0, "CityC": 10.0, "CityD": 47.0}
BAND25_OF = {"CityA": 1, "CityB": 1, "CityC": 0, "CityD": 1}
GOLD_BY_STATUS = {"ok": 10.0, "new": 20.0}
ALPHA_TRUE_BY_STATUS = {"ok": 20.0, "new": 20.0}
ALPHA_FANNED_BY_STATUS = {"ok": 40.0, "new": 40.0}
RUSH_TRUE_BY_STATUS = {"ok": 40.0, "new": None}
RUSH_FANNED_BY_STATUS = {"ok": 70.0, "new": None}
WSCALED_BY_STATUS = {"ok": 110.0, "new": 40.0}
TSCALED_BY_STATUS = {"ok": 135.0, "new": 8.0}
FACTOR_MIN_BY_STATUS = {"ok": 0.5, "new": 0.2}
FACTOR_MAX_BY_STATUS = {"ok": 3.0, "new": 0.2}
SPEND_TOTAL = 350.0
SPEND_BY_TIER = {"gold": 100.0, "silver": 210.0, "bronze": 40.0}

#: Value-preservation pins for shapes that STAY legal under per-role safety.
GOLD_LAST_BY_STATUS = {"ok": 10.0, "new": 20.0}
LAST_BY_SIGNUP_BY_STATUS = {"ok": 5.0, "new": 40.0}

#: band × wm — (region, band, month) → (m, w). The interning flagship shape.
BAND_WM = {
    ("North", 1, "2024-01"): (30.0, 30.0),
    ("North", 1, "2024-02"): (30.0, 60.0),
    ("South", 0, "2024-02"): (5.0, 5.0),
    ("South", 0, "2024-03"): (5.0, 10.0),
    ("South", 1, "2024-03"): (40.0, 40.0),
    (None, 1, "2024-03"): (7.0, 7.0),
}

#: Nested-attach lift oracles (post-DEV-1838; these shapes are broken today).
#: band + gold_amount:sum — (status, band) → (m, g).
BAND_GOLD = {
    ("new", 1): (60.0, 20.0), ("ok", 0): (10.0, None), ("ok", 1): (47.0, 10.0),
}
#: band + ORDER BY customers.tier ASC (host-grain wrap): row order + m.
BAND_TIER_ORDER = [("new", 1, 60.0), ("ok", 1, 47.0), ("ok", 0, 10.0)]
#: cl = amount:last(partition_by=city) dim + gold_amount:sum — (status, cl) → g.
LASTDIM_GOLD = {
    ("new", 20.0): 20.0, ("new", 7.0): None, ("ok", 20.0): 10.0,
    ("ok", 30.0): None, ("ok", 5.0): None, ("ok", 7.0): None,
}
#: customers.spend:last + band — (status, band) → sl (broadcast last spend).
SPEND_LAST_BAND = {("new", 1): 40.0, ("ok", 0): 40.0, ("ok", 1): 40.0}

#: Stage-1 banded rows: (region, band) → amount:sum.
STAGE1_BANDED = {
    ("North", 1): 60.0, ("South", 0): 10.0, ("South", 1): 40.0, (None, 1): 7.0,
}
BAND1_BY_REGION = {"North": 60.0, "South": 40.0, None: 7.0}
BT_BY_BAND = {1: 107.0, 0: 10.0}
#: Stage-2 max over stage-1 windowed w at (region, month) / (region, band, month).
WMAX_BY_REGION = {"North": 60.0, "South": 50.0, None: 7.0}
BAND_WMAX_BY_REGION = {"North": 60.0, "South": 40.0, None: 7.0}


# --------------------------------------------------------------------------- #
# Execution dataset.
# --------------------------------------------------------------------------- #
_REGIONS_ROWS = [(1, "North", 2.0), (2, "South", 3.0)]
_SEGMENTS_ROWS = [
    ("a", "Alpha", 1.0, "2024-01-01"),
    ("a", "Alpha", 1.5, "2024-06-01"),
    ("b", "Beta", 2.0, "2024-02-01"),
]
_CUSTOMERS_ROWS = [
    # (id, region_id, segment_code, tier, spend, signup_at)
    (1, 1, "a", "gold", 100.0, "2024-01-05"),
    (2, 1, "b", "silver", 150.0, "2024-02-10"),
    (3, 2, "a", "silver", 60.0, "2024-03-15"),
    (4, None, None, "bronze", 40.0, "2024-03-20"),
]
_ORDERS_ROWS = [
    # (id, customer_id, region, city, amount, status, ordered_at)
    (1, 1, "North", "CityA", 10.0, "ok", "2024-01-10"),
    (2, 1, "North", "CityA", 20.0, "new", "2024-01-20"),
    (3, 2, "North", "CityB", 30.0, "ok", "2024-02-10"),
    (4, 3, "South", "CityC", 5.0, "ok", "2024-02-15"),
    (5, 3, "South", "CityC", 5.0, "ok", "2024-03-05"),
    (6, 4, "South", "CityD", 40.0, "new", "2024-03-10"),
    (7, None, None, "CityD", 7.0, "ok", "2024-03-12"),
]
_TAGS_ROWS = [
    (1, "rush", 2.0), (1, "gift", 1.0),
    (3, "rush", 3.0), (3, "rush", 0.5),
    (6, "promo", 0.2),
]


def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, weight REAL)")
    cur.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    cur.execute(
        "CREATE TABLE segments (code TEXT, label TEXT, boost REAL, updated_at TEXT)"
    )
    cur.executemany("INSERT INTO segments VALUES (?,?,?,?)", _SEGMENTS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "segment_code TEXT, tier TEXT, spend REAL, signup_at TEXT)"
    )
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "region TEXT, city TEXT, amount REAL, status TEXT, ordered_at TEXT)"
    )
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", _ORDERS_ROWS)
    cur.execute("CREATE TABLE tags (order_id INTEGER, kind TEXT, factor REAL)")
    cur.executemany("INSERT INTO tags VALUES (?,?,?)", _TAGS_ROWS)
    con.commit()
    con.close()


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR, weight DOUBLE)")
    con.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    con.execute(
        "CREATE TABLE segments (code VARCHAR, label VARCHAR, boost DOUBLE, "
        "updated_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO segments VALUES (?,?,?,?)", _SEGMENTS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region_id INTEGER, "
        "segment_code VARCHAR, tier VARCHAR, spend DOUBLE, signup_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, region VARCHAR, "
        "city VARCHAR, amount DOUBLE, status VARCHAR, ordered_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", _ORDERS_ROWS)
    con.execute("CREATE TABLE tags (order_id INTEGER, kind VARCHAR, factor DOUBLE)")
    con.executemany("INSERT INTO tags VALUES (?,?,?)", _TAGS_ROWS)
    con.close()


async def _engine_for(*, dialect: str, db_path: str) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path)
    )
    for model in dev1838_models():
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
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == "broadcast"]


def dropped_filter_warnings(resp) -> list:
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == "unreachable_filter_dropped"]


__all__ = [
    "orders_model", "customers_model", "regions_model", "segments_model",
    "tags_model", "dev1838_models",
    "q", "gen", "month_td", "month_key", "rows_by", "cte_aliases",
    "broadcast_warnings", "dropped_filter_warnings",
    "BAND25", "BAND", "BAND25_OF", "SPEND_BAND",
    "AMOUNT_TOTAL", "AMOUNT_BY_STATUS", "AMOUNT_BY_REGION", "CITY_TOTAL",
    "GOLD_BY_STATUS", "ALPHA_TRUE_BY_STATUS", "ALPHA_FANNED_BY_STATUS",
    "RUSH_TRUE_BY_STATUS", "RUSH_FANNED_BY_STATUS",
    "WSCALED_BY_STATUS", "TSCALED_BY_STATUS",
    "FACTOR_MIN_BY_STATUS", "FACTOR_MAX_BY_STATUS",
    "SPEND_TOTAL", "SPEND_BY_TIER",
    "GOLD_LAST_BY_STATUS", "LAST_BY_SIGNUP_BY_STATUS",
    "BAND_WM", "BAND_GOLD", "BAND_TIER_ORDER", "LASTDIM_GOLD",
    "SPEND_LAST_BAND",
    "STAGE1_BANDED", "BAND1_BY_REGION", "BT_BY_BAND",
    "WMAX_BY_REGION", "BAND_WMAX_BY_REGION",
    "SlayerQuery", "ModelMeasure", "ColumnRef", "OrderItem", "TimeDimension",
    "TimeGranularity",
]
