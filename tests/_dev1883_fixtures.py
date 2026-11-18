"""Shared fixtures for the DEV-1883 functional time-granularity tests."""
from slayer.storage.sqlite_conn import transaction
from typing import Any

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def q(**kw: Any) -> SlayerQuery:
    """Build a ``SlayerQuery``; dict/string field values are coerced by validators (keeps type-checkers off the shorthand)."""
    return SlayerQuery(**kw)


def td(**kw: Any) -> TimeDimension:
    """Build a ``TimeDimension`` from shorthand (string ``field``/``granularity`` coerced by validators)."""
    return TimeDimension(**kw)


def dataset(**kw: Any) -> SlayerDataset:
    """Build a ``SlayerModel`` from shorthand kwargs (dict columns/aggregations coerced by validators)."""
    return SlayerDataset(**kw)

# Orders span three month buckets across two years so month vs year bucketing
# is observable: 2024-01 -> 10.0, 2024-02 -> 60.0, 2025-03 -> 5.0.
ORDER_ROWS = [
    (1, 100, "paid", "2024-01-15", 10.0),
    (2, 100, "paid", "2024-02-02", 40.0),
    (3, 101, "open", "2024-02-20", 20.0),
    (4, 101, "open", "2025-03-01", 5.0),
]
MONTH_SUMS = {"2024-01-01": 10.0, "2024-02-01": 60.0, "2025-03-01": 5.0}


async def save_datasets(storage: YAMLStorage) -> None:
    await storage.save_dataset(SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="region", sql="region", datatype=DataType.TEXT),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.TIMESTAMP),
        ],
    ))
    await storage.save_dataset(SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.INT),
            DatasetField(name="status", sql="status", datatype=DataType.TEXT),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.TIMESTAMP),
            DatasetField(name="amount", sql="amount", datatype=DataType.DOUBLE),
        ],
        relationships=[DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]])],
    ))


def seed_db(db_path: str) -> None:
    with transaction(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, created_at TEXT);
            CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER,
                                 status TEXT, created_at TEXT, amount REAL);
            INSERT INTO customers VALUES (100,'West','2023-05-10'),(101,'East','2024-07-20');
            """
        )
        conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", ORDER_ROWS)


async def build_engine(tmp_path) -> SlayerQueryEngine:
    """Dry-run engine over an in-memory datasource."""
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")
    )
    await save_datasets(storage)
    return SlayerQueryEngine(storage=storage)


async def build_exec_engine(tmp_path) -> SlayerQueryEngine:
    """Engine over a seeded on-disk SQLite database."""
    db_path = str(tmp_path / "t.db")
    seed_db(db_path)
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=db_path)
    )
    await save_datasets(storage)
    return SlayerQueryEngine(storage=storage)
