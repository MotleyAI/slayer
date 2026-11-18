"""Shared fixtures for DEV-1846 composite-input ``time_shift`` /
``consecutive_periods``. Underscore-prefixed so pytest skips collection.

sales(id, region_id, store, status, revenue, qty, cost, weight, sku, ordered_at)
joined to regions(id → factor: r1=2.0, r2=3.0); store A in region 1, B in 2.
``hi_rev`` = ``revenue`` filtered >15; ``wrevenue_sum`` = SUM(revenue * factor).

  id store status month revenue qty cost region(factor)
  1  A     a      Jan   10      2   4    1(2)
  2  A     b      Jan   20      3   6    1(2)
  3  B     a      Jan   30      5   10   2(3)
  4  A     a      Feb   40      5   8    1(2)
  5  B     c      Feb   60      6   30   2(3)
  6  A     a      Mar   50      5   25   1(2)
  7  B     b      Mar   10      2   5    2(3)

Monthly revenue:sum 60/100/60, qty:sum 10/11/7, cost:sum 20/38/30, *:count 3/2/2,
wrevenue_sum 150/260/130, hi_rev:sum 50/100/50. Per-(store,month) revenue/qty/
count: A Jan 30/5/2 Feb 40/5/1 Mar 50/5/1; B Jan 30/5/1 Feb 60/6/1 Mar 10/2/1.
"""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import FieldRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

from tests._engine_helpers import _engine_generate


def regions_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="factor", datatype=DataType.DOUBLE),
        ],
    )


def sales_dataset() -> SlayerDataset:
    """Host: ``wrevenue_sum`` crosses ``sales → regions`` (join-crossing leaf);
    ``hi_rev`` is a column-filtered projection of ``revenue``."""
    return SlayerDataset(
        name="sales", data_source="test", sql_table="sales",
        default_time_dimension="ordered_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INT),
            DatasetField(name="store", datatype=DataType.TEXT),
            DatasetField(name="status", datatype=DataType.TEXT),
            DatasetField(name="revenue", datatype=DataType.DOUBLE),
            DatasetField(name="qty", datatype=DataType.DOUBLE),
            DatasetField(name="cost", datatype=DataType.DOUBLE),
            DatasetField(name="weight", datatype=DataType.DOUBLE),
            DatasetField(name="sku", datatype=DataType.TEXT),
            DatasetField(name="hi_rev", datatype=DataType.DOUBLE, sql="revenue",
                   filter="revenue > 15"),
            DatasetField(name="ordered_at", datatype=DataType.TIMESTAMP),
        ],
        relationships=[
            DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]]),
        ],
        aggregations=[
            Aggregation(
                name="wrevenue_sum", expression="SUM({value} * {f})",
                params=[AggregationParam(name="f", sql="regions.factor")],
            ),
        ],
    )


def dev1846_datasets() -> List[SlayerDataset]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [sales_dataset(), regions_dataset()]


async def gen(query: SlayerQuery, *, dialect: str = "duckdb") -> str:
    models = dev1846_datasets()
    return await _engine_generate(
        query=query, dataset=models[0], extra_datasets=models[1:],
        dialect=dialect, validate=False,
    )


def month_td(column: str = "ordered_at") -> List[TimeDimension]:
    return [TimeDimension(
        field=column,
        granularity=TimeGranularity.MONTH,
    )]


_REGIONS_ROWS = [(1, 2.0), (2, 3.0)]
_SALES_ROWS = [
    # (id, region_id, store, status, revenue, qty, cost, weight, sku, ordered_at)
    (1, 1, "A", "a", 10.0, 2.0, 4.0, 1.0, "k1", "2024-01-10"),
    (2, 1, "A", "b", 20.0, 3.0, 6.0, 2.0, "k2", "2024-01-20"),
    (3, 2, "B", "a", 30.0, 5.0, 10.0, 1.0, "k3", "2024-01-15"),
    (4, 1, "A", "a", 40.0, 5.0, 8.0, 1.0, "k4", "2024-02-10"),
    (5, 2, "B", "c", 60.0, 6.0, 30.0, 2.0, "k5", "2024-02-12"),
    (6, 1, "A", "a", 50.0, 5.0, 25.0, 1.0, "k6", "2024-03-08"),
    (7, 2, "B", "b", 10.0, 2.0, 5.0, 1.0, "k7", "2024-03-20"),
]


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, factor REAL)")
        cur.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
        cur.execute(
            "CREATE TABLE sales (id INTEGER PRIMARY KEY, region_id INTEGER, "
            "store TEXT, status TEXT, revenue REAL, qty REAL, cost REAL, "
            "weight REAL, sku TEXT, ordered_at TEXT)"
        )
        cur.executemany("INSERT INTO sales VALUES (?,?,?,?,?,?,?,?,?,?)", _SALES_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, factor DOUBLE)")
    con.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
    con.execute(
        "CREATE TABLE sales (id INTEGER, region_id INTEGER, store VARCHAR, "
        "status VARCHAR, revenue DOUBLE, qty DOUBLE, cost DOUBLE, "
        "weight DOUBLE, sku VARCHAR, ordered_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO sales VALUES (?,?,?,?,?,?,?,?,?,?)", _SALES_ROWS)
    con.close()


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; a module wraps it in
    ``@pytest.fixture`` so the name lives where consumed (no F811 shadow)."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, datasets=dev1846_datasets()) as (engine, _db):
        yield engine


def month_key(value) -> str:
    """First 7 chars of a DATE_TRUNC'd month value — a stable per-month key
    across SQLite (``2024-01-01`` text) and DuckDB (timestamp)."""
    return str(value)[:7]


def rows_by(resp, *keys) -> dict:
    """Index ``resp.data`` rows by the given result-column key tuple."""
    out = {}
    for r in resp.data:
        out[tuple(r[k] for k in keys)] = r
    return out


__all__ = [
    "sales_dataset", "regions_dataset", "dev1846_datasets",
    "gen", "month_td", "make_exec_engine", "month_key", "rows_by",
    "SlayerQuery", "DatasetMetric", "FieldRef", "TimeDimension",
    "TimeGranularity",
]
