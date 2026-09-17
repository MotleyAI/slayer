"""Shared fixtures for the DEV-1883 functional time-granularity tests."""
import sqlite3
from typing import Any

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def q(**kw: Any) -> SlayerQuery:
    """Build a ``SlayerQuery``; dict/string field values are coerced by validators (keeps type-checkers off the shorthand)."""
    return SlayerQuery(**kw)


def td(**kw: Any) -> TimeDimension:
    """Build a ``TimeDimension`` from shorthand (string ``dimension``/``granularity`` coerced by validators)."""
    return TimeDimension(**kw)


def model(**kw: Any) -> SlayerModel:
    """Build a ``SlayerModel`` from shorthand kwargs (dict columns/aggregations coerced by validators)."""
    return SlayerModel(**kw)

# Orders span three month buckets across two years so month vs year bucketing
# is observable: 2024-01 -> 10.0, 2024-02 -> 60.0, 2025-03 -> 5.0.
ORDER_ROWS = [
    (1, 100, "paid", "2024-01-15", 10.0),
    (2, 100, "paid", "2024-02-02", 40.0),
    (3, 101, "open", "2024-02-20", 20.0),
    (4, 101, "open", "2025-03-01", 5.0),
]
MONTH_SUMS = {"2024-01-01": 10.0, "2024-02-01": 60.0, "2025-03-01": 5.0}


async def save_models(storage: YAMLStorage) -> None:
    await storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))


def seed_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, created_at TEXT);
        CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER,
                             status TEXT, created_at TEXT, amount REAL);
        INSERT INTO customers VALUES (100,'West','2023-05-10'),(101,'East','2024-07-20');
        """
    )
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", ORDER_ROWS)
    conn.commit()
    conn.close()


async def build_engine(tmp_path) -> SlayerQueryEngine:
    """Dry-run engine over an in-memory datasource."""
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")
    )
    await save_models(storage)
    return SlayerQueryEngine(storage=storage)


async def build_exec_engine(tmp_path) -> SlayerQueryEngine:
    """Engine over a seeded on-disk SQLite database."""
    db_path = str(tmp_path / "t.db")
    seed_db(db_path)
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=db_path)
    )
    await save_models(storage)
    return SlayerQueryEngine(storage=storage)
