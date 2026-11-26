"""The ``tests/_dev1739_fixtures.py`` datasets (``orders → customers → regions``)
plus an ``items`` child of ``orders``, seeded into SQLite and DuckDB.

items (id, order_id, qty):
   1  o1  1     2  o1  2     3  o3  4     4  o5  3     5  o6  5     6  o7  9
items.qty:sum by orders.region:  North=7 (o1, o3)  South=8 (o5, o6)  NULL=9 (o7)

Hand values on the ``orders`` data (see ``tests/_dev1739_fixtures.py``):
   amount:sum by (region, city) max per region:  North=40  South=50  NULL=60
   customers.spend:sum by region:  North=300 (c1+c2)  South=50 (c3)  NULL=100 (c1)
   customers.spend:sum by (region, city):
      (North, CityA)=100  (North, CityB)=200  (North, NULL)=200
      (South, CityC)=50   (NULL, CityD)=100
   customers.spend:sum by month:  Jan=150 (c1+c3)  Feb=200 (c2)  Mar=150 (c3+c1)
"""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev1739_fixtures import _seed_duckdb, _seed_sqlite, dev1739_datasets
from tests._engine_helpers import seed_tables, seeded_exec_engine

ITEMS_ROWS = [(1, 1, 1.0), (2, 1, 2.0), (3, 3, 4.0), (4, 5, 3.0), (5, 6, 5.0), (6, 7, 9.0)]


def items_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="items", data_source="test", sql_table="items",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="order_id", datatype=DataType.INTEGER),
            DatasetField(name="qty", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="orders", from_columns=["order_id"], to_columns=["id"])],
    )


def reagg_filter_datasets() -> List[SlayerDataset]:
    """``[orders, customers, regions, items]``."""
    return [*dev1739_datasets(), items_dataset()]


def reagg_filter_bundle() -> ResolvedSourceBundle:
    datasets = reagg_filter_datasets()
    return ResolvedSourceBundle(dialect="sqlite", population=datasets[0], referenced_datasets=datasets[1:])


def _seed_items(db_path: str) -> None:
    seed_tables(db_path, {"items": ("id INTEGER, order_id INTEGER, qty DOUBLE", ITEMS_ROWS)})


def _seed_all_sqlite(db_path: str) -> None:
    _seed_sqlite(db_path)
    _seed_items(db_path)


def _seed_all_duckdb(db_path: str) -> None:
    _seed_duckdb(db_path)
    _seed_items(db_path)


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_all_duckdb if dialect == "duckdb" else _seed_all_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, datasets=reagg_filter_datasets()) as (engine, _db):
        yield engine
