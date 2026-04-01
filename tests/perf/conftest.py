"""Fixtures for performance benchmarks.

Provides seeded SQLite databases at various scales with SLayer models configured.
"""

import tempfile

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from .seed import Dataset, generate_dataset, seed_database


# ---------------------------------------------------------------------------
# SLayer model definitions for the benchmark schema
# ---------------------------------------------------------------------------

def _build_orders_model(ds_name: str) -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source=ds_name,
        default_time_dimension="created_at",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Dimension(name="shop_id", sql="shop_id", type=DataType.NUMBER),
            Dimension(name="category", sql="category", type=DataType.STRING),
            Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Dimension(name="completed_at", sql="completed_at", type=DataType.TIMESTAMP),
            Dimension(name="cancelled_at", sql="cancelled_at", type=DataType.TIMESTAMP),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
            Measure(name="total_cost", sql="cost", type=DataType.SUM),
            Measure(name="avg_cost", sql="cost", type=DataType.AVERAGE),
            Measure(name="min_cost", sql="cost", type=DataType.MIN),
            Measure(name="max_cost", sql="cost", type=DataType.MAX),
            Measure(name="latest_cost", sql="cost", type=DataType.LAST),
        ],
    )


def _build_shops_model(ds_name: str) -> SlayerModel:
    return SlayerModel(
        name="shops",
        sql_table="shops",
        data_source=ds_name,
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="name", sql="name", type=DataType.STRING),
            Dimension(name="region_id", sql="region_id", type=DataType.NUMBER),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
        ],
    )


def _build_customers_model(ds_name: str) -> SlayerModel:
    return SlayerModel(
        name="customers",
        sql_table="customers",
        data_source=ds_name,
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="name", sql="name", type=DataType.STRING),
            Dimension(name="segment", sql="segment", type=DataType.STRING),
            Dimension(name="primary_shop_id", sql="primary_shop_id", type=DataType.NUMBER),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
        ],
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_env(order_count: int) -> tuple[SlayerQueryEngine, Dataset]:
    """Create a seeded SQLite database + SLayer engine at a given scale."""
    tmpdir = tempfile.mkdtemp()
    db_path = f"{tmpdir}/bench.db"

    # Generate and seed
    dataset = generate_dataset(order_count=order_count)
    engine = sa.create_engine(f"sqlite:///{db_path}")
    seed_database(engine=engine, dataset=dataset)

    # Configure SLayer
    storage = YAMLStorage(base_dir=tmpdir)
    ds = DatasourceConfig(name="bench", type="sqlite", database=db_path)
    storage.save_datasource(ds)

    storage.save_model(_build_orders_model("bench"))
    storage.save_model(_build_shops_model("bench"))
    storage.save_model(_build_customers_model("bench"))

    slayer_engine = SlayerQueryEngine(storage=storage)
    return slayer_engine, dataset


BenchEnv = tuple[SlayerQueryEngine, Dataset]


@pytest.fixture(scope="session")
def env_1k() -> BenchEnv:
    """1,000 orders — fast baseline."""
    return _create_env(1_000)


@pytest.fixture(scope="session")
def env_10k() -> BenchEnv:
    """10,000 orders — moderate scale."""
    return _create_env(10_000)


@pytest.fixture(scope="session")
def env_100k() -> BenchEnv:
    """100,000 orders — stress test."""
    return _create_env(100_000)
