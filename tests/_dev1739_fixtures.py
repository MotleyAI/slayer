"""Shared fixtures for DEV-1739 — ``partition_by=`` on aggregations (aggregate at
a coarser grain than the query).

Underscore-prefixed so pytest skips collection here (like
``tests/_engine_helpers.py``). Mirrors the dual-engine execution pattern of
``tests/_dev1750_fixtures.py``: one hand-computed dataset seeded into SQLite AND
DuckDB, ``make_exec_engine`` yielding an engine per backend, plus ``gen`` for
SQL-shape assertions.

Model shape
-----------
``orders`` (host) carries the LOCAL partition dimensions ``region`` / ``city`` /
``channel`` directly, so ``revenue:sum(partition_by=region)`` is a purely-local
coarser aggregate. ``ok_amount`` / ``nomatch`` are filtered columns (CASE-WHEN at
aggregation time) for the filtered-share and zero-surviving-input cases.
``orders → customers → regions`` gives the cross-model + rerooted shapes:
``customers.spend`` is a target-grain measure, ``customers.tier`` and
``customers.regions.name`` are target-side dimensions.

Dataset (hand-computed; every executed expectation is derived here)
-------------------------------------------------------------------
orders (id, cust, region, city, channel, amount, status, ordered_at):
   1  c1  North  CityA  web   10  ok    2024-01-10
   2  c1  North  CityA  app   20  ok    2024-01-20
   3  c2  North  CityB  web   40  ok    2024-02-10
   4  c2  North  NULL   web   30  ok    2024-02-15   (NULL-city group in North)
   5  c3  South  CityC  web   25  ok    2024-01-25
   6  c3  South  CityC  app   25  hold  2024-03-05
   7  c1  NULL   CityD  web   60  ok    2024-03-10   (NULL-region group)

amount:sum by (region, city):
   (North, CityA)=30  (North, CityB)=40  (North, NULL)=30
   (South, CityC)=50  (NULL,  CityD)=60
amount:sum by region:   North=100  South=50  NULL=60   grand total=210
amount:sum by (region, channel):
   (North, web)=80  (North, app)=20  (South, web)=25  (South, app)=25  (NULL, web)=60
amount:sum by month:    Jan=55  Feb=70  Mar=85
amount:sum by (region, month):
   (North, Jan)=30 (South, Jan)=25 | (North, Feb)=70 | (South, Mar)=25 (NULL, Mar)=60

ok_amount (amount where status='ok') by region: North=100  South=25  NULL=60
nomatch  (amount where amount>100000): no row qualifies anywhere.

customers (id, tier, spend, region_id) → regions (id, name):
   c1 gold   100  r1(RegN)
   c2 silver 200  r1(RegN)
   c3 gold    50  r2(RegS)
customers.spend:sum by tier:  gold=150  silver=200   grand total=350
customers.spend:sum by regions.name:  RegN=300 (c1+c2)  RegS=50 (c3)  grand=350
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import ColumnRef, ModelMeasure, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate, _extract_cte_body


# --------------------------------------------------------------------------- #
# Models — orders (host) → customers → regions.
# --------------------------------------------------------------------------- #
def regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
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
            Column(name="channel", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            # Filtered columns (CASE-WHEN at aggregation time).
            Column(name="ok_amount", type=DataType.DOUBLE, sql="amount",
                   filter="status = 'ok'"),
            Column(name="nomatch", type=DataType.DOUBLE, sql="amount",
                   filter="amount > 100000"),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def dev1739_models() -> List[SlayerModel]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [orders_model(), customers_model(), regions_model()]


# --------------------------------------------------------------------------- #
# SQL-shape generation (dry-run) — for golden + scope-closed assertions.
# --------------------------------------------------------------------------- #
async def gen(query: SlayerQuery, *, dialect: str = "duckdb", validate: bool = False) -> str:
    models = dev1739_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
        dialect=dialect, validate=validate,
    )


def month_td(column: str = "ordered_at") -> List[TimeDimension]:
    return [TimeDimension(
        dimension=ColumnRef(name=column),
        granularity=TimeGranularity.MONTH,
    )]


def cm_cte_bodies(sql: str) -> str:
    """Concatenated body of every isolated aggregate (``_cm_*``) CTE — where a
    partitioned aggregate's GROUP BY lives."""
    return _extract_cte_body(sql, r"_cm_\w+")


# --------------------------------------------------------------------------- #
# Execution dataset — hand-computable (see module docstring).
# --------------------------------------------------------------------------- #
_REGIONS_ROWS = [(1, "RegN"), (2, "RegS")]
_CUSTOMERS_ROWS = [
    # (id, region_id, tier, spend)
    (1, 1, "gold", 100.0),
    (2, 1, "silver", 200.0),
    (3, 2, "gold", 50.0),
]
_ORDERS_ROWS = [
    # (id, customer_id, region, city, channel, amount, status, ordered_at)
    (1, 1, "North", "CityA", "web", 10.0, "ok", "2024-01-10"),
    (2, 1, "North", "CityA", "app", 20.0, "ok", "2024-01-20"),
    (3, 2, "North", "CityB", "web", 40.0, "ok", "2024-02-10"),
    (4, 2, "North", None, "web", 30.0, "ok", "2024-02-15"),
    (5, 3, "South", "CityC", "web", 25.0, "ok", "2024-01-25"),
    (6, 3, "South", "CityC", "app", 25.0, "hold", "2024-03-05"),
    (7, 1, None, "CityD", "web", 60.0, "ok", "2024-03-10"),
]


def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)")
    cur.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "tier TEXT, spend REAL)"
    )
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?)", _CUSTOMERS_ROWS)
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "region TEXT, city TEXT, channel TEXT, amount REAL, status TEXT, "
        "ordered_at TEXT)"
    )
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_ROWS)
    con.commit()
    con.close()


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR)")
    con.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region_id INTEGER, tier VARCHAR, "
        "spend DOUBLE)"
    )
    con.executemany("INSERT INTO customers VALUES (?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, region VARCHAR, "
        "city VARCHAR, channel VARCHAR, amount DOUBLE, status VARCHAR, "
        "ordered_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_ROWS)
    con.close()


async def _engine_for(*, dialect: str, db_path: str) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path)
    )
    for model in dev1739_models():
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture (the issue's required
    execution backends). A test module wraps this in ``@pytest.fixture`` so the
    fixture name lives where it is consumed."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        if dialect == "sqlite":
            _seed_sqlite(db_path)
        else:
            _seed_duckdb(db_path)
        engine = await _engine_for(dialect=dialect, db_path=db_path)
        yield engine


def month_key(value) -> str:
    """First 7 chars of a DATE_TRUNC'd month value — a stable per-month key
    across SQLite (``2024-01-01`` text) and DuckDB (``2024-01-01 00:00:00``)."""
    return str(value)[:7]


def rows_by(resp, *keys) -> dict:
    """Index ``resp.data`` rows by the given result-column key tuple."""
    out = {}
    for r in resp.data:
        out[tuple(r[k] for k in keys)] = r
    return out


def approx_sum(resp, key) -> float:
    """Sum of a numeric result column over every row (NULLs skipped)."""
    return sum(float(r[key]) for r in resp.data if r[key] is not None)


__all__ = [
    "orders_model", "customers_model", "regions_model", "dev1739_models",
    "gen", "month_td", "cm_cte_bodies",
    "make_exec_engine", "month_key", "rows_by", "approx_sum",
    "SlayerQuery", "ModelMeasure", "ColumnRef", "TimeDimension", "TimeGranularity",
]
