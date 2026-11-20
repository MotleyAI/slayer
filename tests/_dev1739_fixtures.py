"""Shared fixtures for ``partition_by=`` on aggregations (aggregate at
a coarser grain than the query).

Underscore-prefixed so pytest skips collection here (like
``tests/_engine_helpers.py``). Mirrors the dual-engine execution pattern of
``tests/_dev1750_fixtures.py``: one hand-computed dataset seeded into SQLite AND
DuckDB, ``make_exec_engine`` yielding an engine per backend, plus ``gen`` for
SQL-shape assertions.

Dataset shape
-------------
``orders`` (host) carries the LOCAL partition dimensions ``region`` / ``city`` /
``channel`` directly, so ``sum(revenue, partition_by=region)`` is a purely-local
coarser aggregate. ``ok_amount`` / ``nomatch`` are filtered fields (CASE-WHEN at
aggregation time) for the filtered-share and zero-surviving-input cases.
``orders → customers → regions`` gives the cross-dataset + rerooted shapes:
``customers.spend`` is a target-grain metric, ``customers.tier`` and
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

customers (id, tier, spend, signup_at, region_id) → regions (id, name):
   c1 gold   100  2024-01-05  r1(RegN)
   c2 silver 200  2024-02-10  r1(RegN)
   c3 gold    50  2024-03-15  r2(RegS)
customers.spend:sum by tier:  gold=150  silver=200   grand total=350
customers.spend:sum by regions.name:  RegN=300 (c1+c2)  RegS=50 (c3)  grand=350
customers.spend first/last by signup within regions.name (ranked
cross-dataset shapes): RegN first=100 last=200, RegS first=last=50.
"""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    DatasetField,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import FieldRef, DatasetMetric, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

from tests._engine_helpers import _engine_generate, _extract_cte_body


# --------------------------------------------------------------------------- #
# Datasets — orders (host) → customers → regions.
# --------------------------------------------------------------------------- #
def regions_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
        ],
    )


def customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        default_time_dimension="signup_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
            DatasetField(name="tier", datatype=DataType.STRING),
            DatasetField(name="spend", datatype=DataType.FLOAT),
            DatasetField(name="signup_at", datatype=DataType.DATETIME),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="region", datatype=DataType.STRING),
            DatasetField(name="city", datatype=DataType.STRING),
            DatasetField(name="channel", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="ordered_at", datatype=DataType.DATETIME),
            # Filtered fields (CASE-WHEN at aggregation time).
            DatasetField(name="ok_amount", datatype=DataType.FLOAT, sql="amount",
                   filter="status = 'ok'"),
            DatasetField(name="nomatch", datatype=DataType.FLOAT, sql="amount",
                   filter="amount > 100000"),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
        ],
    )


def dev1739_datasets() -> List[SlayerDataset]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [orders_dataset(), customers_dataset(), regions_dataset()]


# --------------------------------------------------------------------------- #
# SQL-shape generation (dry-run) — for golden + scope-closed assertions.
# --------------------------------------------------------------------------- #
async def gen(query: SlayerQuery, *, dialect: str = "duckdb", validate: bool = False) -> str:
    models = dev1739_datasets()
    return await _engine_generate(
        query=query, dataset=models[0], extra_datasets=models[1:],
        dialect=dialect, validate=validate,
    )


def month_td(field: str = "orders.ordered_at") -> List[TimeDimension]:
    return [TimeDimension(
        field=field,
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
    # (id, region_id, tier, spend, signup_at)
    (1, 1, "gold", 100.0, "2024-01-05"),
    (2, 1, "silver", 200.0, "2024-02-10"),
    (3, 2, "gold", 50.0, "2024-03-15"),
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
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)")
        cur.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
        cur.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
            "tier TEXT, spend REAL, signup_at TEXT)"
        )
        cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", _CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
            "region TEXT, city TEXT, channel TEXT, amount REAL, status TEXT, "
            "ordered_at TEXT)"
        )
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR)")
    con.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region_id INTEGER, tier VARCHAR, "
        "spend DOUBLE, signup_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, region VARCHAR, "
        "city VARCHAR, channel VARCHAR, amount DOUBLE, status VARCHAR, "
        "ordered_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_ROWS)
    con.close()


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture (the issue's required
    execution backends). A test module wraps this in ``@pytest.fixture`` so the
    fixture name lives where it is consumed."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, datasets=dev1739_datasets()) as (engine, _db):
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
    "orders_dataset", "customers_dataset", "regions_dataset", "dev1739_datasets",
    "gen", "month_td", "cm_cte_bodies",
    "make_exec_engine", "month_key", "rows_by", "approx_sum",
    "SlayerQuery", "DatasetMetric", "FieldRef", "TimeDimension", "TimeGranularity",
]
