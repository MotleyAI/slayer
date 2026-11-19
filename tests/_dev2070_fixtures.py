"""Shared fixtures for the expression-aggregate auto-name tests.

Underscore-prefixed so pytest skips collection. ``orders.customer_id →
customers.id`` and ``customers.region_id → regions.id`` are many-to-one.

orders (id, customer_id, amount, status, ordered_at = order_date):
   1  10  10  paid     2024-01-10
   2  10  20  paid     2024-02-05
   3  20  30  pending  2024-02-20
   4  30   5  paid     2024-03-15
   5  30  50  pending  2025-01-20
   6  20  60  paid     2025-02-01
customers (id, region_id, spend, signup_at; spend_x2 = spend * 2):
  10 1 100 2024-01-15 | 20 1 50 2024-03-10 | 30 2 200 2024-02-05
regions (id, name, weight):  1 north 2 | 2 south 3
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncIterator, Callable, List, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import seeded_exec_engine


def orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="ordered_at", datatype=DataType.DATETIME),
            DatasetField(name="order_date", datatype=DataType.DATE),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )


def customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
            DatasetField(name="spend", datatype=DataType.INTEGER),
            DatasetField(name="spend_x2", sql="spend * 2", datatype=DataType.INTEGER),
            DatasetField(name="signup_at", datatype=DataType.DATETIME),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def regions_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="regions", sql_table="regions", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(name="weight", datatype=DataType.INTEGER),
        ],
    )


def dev2070_datasets() -> List[SlayerDataset]:
    return [orders_dataset(), customers_dataset(), regions_dataset()]


_ORDER_ROWS = [
    (1, 10, 10, "paid", "2024-01-10"),
    (2, 10, 20, "paid", "2024-02-05"),
    (3, 20, 30, "pending", "2024-02-20"),
    (4, 30, 5, "paid", "2024-03-15"),
    (5, 30, 50, "pending", "2025-01-20"),
    (6, 20, 60, "paid", "2025-02-01"),
]
_CUSTOMER_ROWS = [(10, 1, 100, "2024-01-15"), (20, 1, 50, "2024-03-10"), (30, 2, 200, "2024-02-05")]
_REGION_ROWS = [(1, "north", 2), (2, "south", 3)]


def _lit(v) -> str:
    return f"'{v}'" if isinstance(v, str) else str(v)


def _values(rows: list) -> str:
    return ", ".join("(" + ", ".join(_lit(v) for v in row) + ")" for row in rows)


def seed_statements(*, text_type: str) -> List[str]:
    orders = [(*r, r[-1]) for r in _ORDER_ROWS]  # order_date = ordered_at's day
    return [
        f"CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER, "
        f"status {text_type}, ordered_at TIMESTAMP, order_date DATE)",
        f"INSERT INTO orders VALUES {_values(orders)}",
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, spend INTEGER, "
        "signup_at TIMESTAMP)",
        f"INSERT INTO customers VALUES {_values(_CUSTOMER_ROWS)}",
        f"CREATE TABLE regions (id INTEGER PRIMARY KEY, name {text_type}, weight INTEGER)",
        f"INSERT INTO regions VALUES {_values(_REGION_ROWS)}",
    ]


def seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        for stmt in seed_statements(text_type="TEXT"):
            con.execute(stmt)


def seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    for stmt in seed_statements(text_type="VARCHAR"):
        con.execute(stmt)
    con.close()


async def make_exec_engine(
    dialect: str, *, clock: Optional[Callable[[], datetime]] = None,
    datasets: Optional[List[SlayerDataset]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture (or a pinned-clock engine)."""
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, datasets=datasets or dev2070_datasets(), clock=clock,
    ) as (engine, _):
        yield engine
