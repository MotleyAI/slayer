"""Shared fixtures for DEV-1740 — SQL conditionals + expression dimensions
(including one over an aggregate).

Underscore-prefixed so pytest skips collection here. Mirrors
``tests/_dev1739_fixtures.py``: one hand-computed dataset seeded into SQLite AND
DuckDB, ``make_exec_engine`` yielding an engine per backend, ``gen`` for
SQL-shape assertions.

Model shape
-----------
``orders`` (host) carries the LOCAL dimensions ``region`` / ``city`` /
``channel`` plus ``amount`` / ``status`` / ``ordered_at``. The banding case
groups by ``region`` and a computed ``band`` derived from each city's total
(``sum(amount, partition_by=city)`` crossing 5000) — the aggregate lives at a
FINER grain (city) than the query (region), which is why Part B2 needs
DEV-1739's ``partition_by=`` and a post-aggregate regroup.
``orders → customers → regions`` gives the cross-model / extension-join shapes.

Dataset (hand-computed; every executed expectation is derived here)
-------------------------------------------------------------------
orders (id, cust, region, city, channel, amount, status, ordered_at):
   1  1  EU  Paris   web  3000  ok    2024-01-05
   2  1  EU  Paris   app  4000  ok    2024-02-05     Paris total 7000  -> band 1
   3  2  EU  Berlin  web  5000  ok    2024-01-15
   4  2  EU  Berlin  app  4000  hold  2024-02-15     Berlin total 9000 -> band 1
   5  3  EU  Lyon    web   400  ok    2024-01-25
   6  3  EU  Lyon    app   600  ok    2024-03-05     Lyon total 1000   -> band 0
   7  4  US  NYC     web   800  ok    2024-01-10
   8  4  US  NYC     app  1200  ok    2024-03-10     NYC total 2000    -> band 0
   9  4  US  NULL    web  6000  ok    2024-02-20     NULL-city 6000    -> band 1

city totals (amount:sum by city):
   Paris=7000  Berlin=9000  Lyon=1000  NYC=2000  NULL=6000
amount:sum by region:  EU=17000  US=8000   grand total=25000

Banding (band = 1 iff the city's total > 5000), then grouped by (region, band):
   (EU, 0) = 1000        (Lyon)
   (EU, 1) = 16000       (Paris 7000 + Berlin 9000)
   (US, 0) = 2000        (NYC)
   (US, 1) = 6000        (NULL-city — a NULL group key that must survive)

Per-(region, band) aggregates over the RAW rows (aggregate-once, no fan-out):
   count(*):        (EU,1)=4  (EU,0)=2  (US,0)=2  (US,1)=1     total=9
   amount:avg:      (EU,1)=4000  (EU,0)=500  (US,0)=1000  (US,1)=6000
   ok_amount:sum:   (EU,1)=12000 (EU,0)=1000 (US,0)=2000  (US,1)=6000   (row 4 'hold' excluded)

customers (id, region_id, tier, spend) → regions (id, name):
   1 r1 gold   100
   2 r1 silver 200
   3 r2 gold    50
   4 r2 silver 300
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
from slayer.core.query import DatasetMetric, FieldRef, QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

from tests._engine_helpers import _engine_generate
# The customer/region models and the generic result helpers are byte-identical
# to DEV-1739's; import them rather than restate them (DRY / Sonar duplication).
from tests._dev1739_fixtures import (
    approx_sum,
    cm_cte_bodies,
    customers_dataset,
    month_key,
    month_td,
    regions_dataset,
    rows_by,
)


# --------------------------------------------------------------------------- #
# Models — orders (host) → customers → regions.
# --------------------------------------------------------------------------- #
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
            # Filtered column (CASE-WHEN at aggregation time) — for a filtered
            # aggregate reconciliation under the computed dimension.
            DatasetField(name="ok_amount", datatype=DataType.FLOAT, sql="amount",
                   filter="status = 'ok'"),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
        ],
    )


def dev1740_datasets() -> List[SlayerDataset]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [orders_dataset(), customers_dataset(), regions_dataset()]


# --------------------------------------------------------------------------- #
# SQL-shape generation (dry-run) — for golden + scope-closed + no-second-pass.
# --------------------------------------------------------------------------- #
async def gen(query, *, dialect: str = "duckdb", validate: bool = False) -> str:
    models = dev1740_datasets()
    return await _engine_generate(
        query=query, dataset=models[0], extra_datasets=models[1:],
        dialect=dialect, validate=validate,
    )


# --------------------------------------------------------------------------- #
# Execution dataset — hand-computable (see module docstring).
# --------------------------------------------------------------------------- #
_REGIONS_ROWS = [(1, "RegN"), (2, "RegS")]
_CUSTOMERS_ROWS = [
    # (id, region_id, tier, spend)
    (1, 1, "gold", 100.0),
    (2, 1, "silver", 200.0),
    (3, 2, "gold", 50.0),
    (4, 2, "silver", 300.0),
]
_ORDERS_ROWS = [
    # (id, customer_id, region, city, channel, amount, status, ordered_at)
    (1, 1, "EU", "Paris", "web", 3000.0, "ok", "2024-01-05"),
    (2, 1, "EU", "Paris", "app", 4000.0, "ok", "2024-02-05"),
    (3, 2, "EU", "Berlin", "web", 5000.0, "ok", "2024-01-15"),
    (4, 2, "EU", "Berlin", "app", 4000.0, "hold", "2024-02-15"),
    (5, 3, "EU", "Lyon", "web", 400.0, "ok", "2024-01-25"),
    (6, 3, "EU", "Lyon", "app", 600.0, "ok", "2024-03-05"),
    (7, 4, "US", "NYC", "web", 800.0, "ok", "2024-01-10"),
    (8, 4, "US", "NYC", "app", 1200.0, "ok", "2024-03-10"),
    (9, 4, "US", None, "web", 6000.0, "ok", "2024-02-20"),
]

# Hand-computed oracles (see docstring). Keyed (region, band).
CITY_TOTAL = {"Paris": 7000.0, "Berlin": 9000.0, "Lyon": 1000.0, "NYC": 2000.0,
              None: 6000.0}
BAND_SUM = {("EU", 0): 1000.0, ("EU", 1): 16000.0,
            ("US", 0): 2000.0, ("US", 1): 6000.0}
BAND_COUNT = {("EU", 1): 4, ("EU", 0): 2, ("US", 0): 2, ("US", 1): 1}
BAND_AVG = {("EU", 1): 4000.0, ("EU", 0): 500.0, ("US", 0): 1000.0, ("US", 1): 6000.0}
BAND_OK_SUM = {("EU", 1): 12000.0, ("EU", 0): 1000.0, ("US", 0): 2000.0, ("US", 1): 6000.0}
# count(distinct customer_id) per (region, band): Paris(c1)+Berlin(c2)=2; Lyon(c3)=1;
# NYC(c4)=1; NULL-city(c4)=1.
BAND_DISTINCT_CUST = {("EU", 1): 2, ("EU", 0): 1, ("US", 0): 1, ("US", 1): 1}
REGION_SUM = {"EU": 17000.0, "US": 8000.0}
REGION_COUNT = {"EU": 6, "US": 3}
GRAND_TOTAL = 25000.0


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
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


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture (the issue's required
    execution backends). A test module wraps this in ``@pytest.fixture``."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, datasets=dev1740_datasets()) as (engine, _db):
        yield engine


# --------------------------------------------------------------------------- #
# The two-stage workaround — the issue's verified oracle, as executable stages.
# The single-stage expression-dimension form MUST reproduce these value tuples.
# --------------------------------------------------------------------------- #
def two_stage_banding() -> list:
    """The ``[stage1, stage2]`` list from the issue: per-city totals, then a
    row-level CASE band over that total, regrouped by (region, band)."""
    stage1 = SlayerQuery(
        name="per_city", population="orders",
        dimensions=["orders.city", "orders.region"],
        metrics=[QueryMetric(metric="sum(orders.amount)", alias="city_total")],
    )
    stage2 = SlayerQuery(
        population="per_city",
        dimensions=[QueryDimension(field="per_city.region"),
                    QueryDimension(field="CASE WHEN per_city.city_total > 5000 THEN 1 ELSE 0 END", alias="band")],
        metrics=[QueryMetric(metric="sum(per_city.city_total)", alias="band_total")],
    )
    return [stage1, stage2]


def band_value_tuples(resp, *, region_key: str, band_key: str, total_key: str) -> set:
    """The ``(region, int(band), round(total))`` set of a banding response —
    prefix-independent so the single-stage and two-stage forms compare."""
    out = set()
    for r in resp.data:
        out.add((r[region_key], int(r[band_key]), round(float(r[total_key]), 3)))
    return out


__all__ = [
    "orders_dataset", "customers_dataset", "regions_dataset", "dev1740_datasets",
    "gen", "month_td", "month_key", "cm_cte_bodies",
    "make_exec_engine", "two_stage_banding", "band_value_tuples",
    "rows_by", "approx_sum",
    "CITY_TOTAL", "BAND_SUM", "BAND_COUNT", "BAND_AVG", "BAND_OK_SUM",
    "BAND_DISTINCT_CUST", "REGION_SUM", "REGION_COUNT", "GRAND_TOTAL",
    "SlayerQuery", "DatasetMetric", "FieldRef", "TimeDimension", "TimeGranularity",
]
