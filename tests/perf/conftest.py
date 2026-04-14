"""Fixtures for performance benchmarks.

Provides seeded databases at various scales with SLayer models configured.
"""

import tempfile

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from slayer.async_utils import run_sync

from .params import (
    DATA_END_DATE, DATA_START_DATE, DB_BACKEND, DB_TYPE, DB_URL,
    INDEXES, SCALES, SEED,
)
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
            Measure(name="total_cost", sql="cost"),
            Measure(name="avg_cost", sql="cost"),
            Measure(name="min_cost", sql="cost"),
            Measure(name="max_cost", sql="cost"),
            Measure(name="latest_cost", sql="cost"),
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
        measures=[],
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
        measures=[],
    )


# ---------------------------------------------------------------------------
# Environment creation
# ---------------------------------------------------------------------------

BenchEnv = tuple[SlayerQueryEngine, Dataset]


def _create_env(order_count: int) -> BenchEnv:
    """Create a seeded database + SLayer engine at a given scale.

    Uses DB_BACKEND from params.py:
      - "sqlite": auto-created temp file (default)
      - "url": connect to external DB via DB_URL (Postgres, MySQL, etc.)
    """
    tmpdir = tempfile.mkdtemp()

    # Generate dataset
    dataset = generate_dataset(
        order_count=order_count,
        start_date=DATA_START_DATE,
        end_date=DATA_END_DATE,
        seed=SEED,
    )

    # Create DB engine and seed
    if DB_BACKEND == "url":
        if not DB_URL or not DB_TYPE:
            raise ValueError("DB_BACKEND='url' requires DB_URL and DB_TYPE in params.py")
        if "bench" not in DB_URL.lower():
            raise ValueError(
                f"DB_URL must contain 'bench' in the database name as a safety check "
                f"(e.g., 'slayer_bench'). Got: {DB_URL}"
            )
        db_engine = sa.create_engine(DB_URL)
        seed_database(engine=db_engine, dataset=dataset, clean=True)
        ds = DatasourceConfig(name="bench", type=DB_TYPE, connection_string=DB_URL)
    else:
        # Default: SQLite
        db_path = f"{tmpdir}/bench.db"
        db_engine = sa.create_engine(f"sqlite:///{db_path}")
        seed_database(engine=db_engine, dataset=dataset)
        ds = DatasourceConfig(name="bench", type="sqlite", database=db_path)

    # Create indexes for realistic query performance
    import warnings
    dialect = db_engine.dialect.name
    with db_engine.connect() as conn:
        for idx_sql in INDEXES:
            try:
                conn.execute(sa.text(idx_sql))
            except Exception as e:
                warnings.warn(
                    f"[{dialect}] Index creation failed: {idx_sql!r} — {e}",
                    stacklevel=2,
                )
        conn.commit()

    # Configure SLayer
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(ds))

    run_sync(storage.save_model(_build_orders_model("bench")))
    run_sync(storage.save_model(_build_shops_model("bench")))
    run_sync(storage.save_model(_build_customers_model("bench")))

    slayer_engine = SlayerQueryEngine(storage=storage)

    # Warmup: run a simple query to prime DB caches and connection pool
    slayer_engine.execute_sync(query=SlayerQuery(
        source_model="orders", fields=[{"formula": "*:count"}],
    ))

    return slayer_engine, dataset


# ---------------------------------------------------------------------------
# Dynamically generate session-scoped fixtures from SCALES
# ---------------------------------------------------------------------------

for _name, _count in SCALES.items():
    def _make_fixture(n: int, fixture_name: str) -> BenchEnv:
        @pytest.fixture(scope="session", name=fixture_name)
        def _fixture() -> BenchEnv:
            return _create_env(n)
        return _fixture
    globals()[f"env_{_name}"] = _make_fixture(_count, f"env_{_name}")
