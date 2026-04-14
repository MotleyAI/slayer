"""Helpers for the joins notebook — diamond join schema and setup."""

import os
import tempfile

import duckdb

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def create_diamond_db(db_path: str) -> None:
    """Create a DuckDB with a diamond-join schema and sample data.

    Schema:
        regions (id PK, name)
        customers (id PK, name, region_id FK -> regions)
        warehouses (id PK, name, region_id FK -> regions)
        orders (id PK, customer_id FK -> customers, warehouse_id FK -> warehouses, amount)

    The diamond: orders -> customers -> regions
                 orders -> warehouses -> regions
    """
    conn = duckdb.connect(db_path)

    conn.execute("""
        CREATE TABLE regions (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            region_id INTEGER NOT NULL REFERENCES regions(id)
        )
    """)
    conn.execute("""
        CREATE TABLE warehouses (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            region_id INTEGER NOT NULL REFERENCES regions(id)
        )
    """)
    conn.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            warehouse_id INTEGER NOT NULL REFERENCES warehouses(id),
            amount DOUBLE NOT NULL
        )
    """)

    # Sample data: 3 regions, customers and warehouses in different regions
    conn.execute("INSERT INTO regions VALUES (1, 'West'), (2, 'East'), (3, 'Central')")
    conn.execute("""
        INSERT INTO customers VALUES
            (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Carol', 3),
            (4, 'Dave', 1), (5, 'Eve', 2)
    """)
    conn.execute("""
        INSERT INTO warehouses VALUES
            (1, 'WH-West', 1), (2, 'WH-East', 2), (3, 'WH-Central', 3)
    """)
    conn.execute("""
        INSERT INTO orders VALUES
            (1, 1, 2, 100.0), (2, 2, 1, 200.0), (3, 3, 3, 150.0),
            (4, 4, 2, 300.0), (5, 5, 3, 250.0), (6, 1, 1, 175.0),
            (7, 2, 3, 125.0), (8, 3, 1, 225.0), (9, 4, 2, 350.0),
            (10, 5, 1, 400.0)
    """)

    conn.close()


def setup_diamond_example() -> tuple:
    """Create a diamond-join DB, ingest it, and return (engine, storage, models, db_path, work_dir).

    Uses a temporary directory that the caller can clean up.
    """
    work_dir = tempfile.mkdtemp(prefix="diamond_joins_")
    db_path = os.path.join(work_dir, "diamond.duckdb")
    models_dir = os.path.join(work_dir, "slayer_models")

    create_diamond_db(db_path)

    storage = YAMLStorage(base_dir=models_dir)
    ds = DatasourceConfig(name="diamond", type="duckdb", database=db_path)
    run_sync(storage.save_datasource(ds))

    models = ingest_datasource(datasource=ds)
    for model in models:
        run_sync(storage.save_model(model))

    engine = SlayerQueryEngine(storage=storage)
    return engine, storage, models, db_path, work_dir
