#!/usr/bin/env python3
"""Generate Jaffle Shop data, load into DuckDB, and auto-ingest with SLayer.

Dependencies: pip install motley-slayer[examples]
Usage: python ingest.py
"""

import os
import sys

import duckdb

# Add project root to path for local development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from jaffle_shop_duckdb import SCHEMA_FILE, create_schema, generate_data, load_data

from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource
from slayer.storage.yaml_storage import YAMLStorage

WORK_DIR = os.path.join(os.path.dirname(__file__), "jaffle_data")
DB_PATH = os.path.join(WORK_DIR, "jaffle_shop.duckdb")

# Models with a natural time dimension
TIME_DIMENSIONS = {
    "orders": "ordered_at",
    "tweets": "tweeted_at",
}


def ensure_database(years: int = 3) -> str:
    """Create the Jaffle Shop DuckDB database if it doesn't exist. Returns the db path."""
    if os.path.exists(DB_PATH):
        print(f"Database already exists at {DB_PATH}")
        return DB_PATH

    os.makedirs(WORK_DIR, exist_ok=True)

    print(f"=== Generating {years} years of Jaffle Shop data ===")
    data_dir = generate_data(output_dir=WORK_DIR, years=years)

    print("\n=== Creating DuckDB schema and loading data ===")
    conn = duckdb.connect(DB_PATH)
    try:
        create_schema(conn=conn, schema_path=SCHEMA_FILE)
        load_data(conn=conn, data_dir=data_dir)
    finally:
        conn.close()

    print(f"\nDatabase created at {DB_PATH}")
    return DB_PATH


def run_ingestion(db_path: str) -> None:
    """Ingest the DuckDB database with SLayer and display results."""
    storage_dir = os.path.join(WORK_DIR, "slayer_models")
    storage = YAMLStorage(base_dir=storage_dir)

    ds = DatasourceConfig(name="jaffle_shop", type="duckdb", database=db_path)
    storage.save_datasource(ds)

    print("\n=== Auto-ingesting with SLayer ===")
    models = ingest_datasource(datasource=ds)

    for model in models:
        if model.name in TIME_DIMENSIONS:
            model.default_time_dimension = TIME_DIMENSIONS[model.name]
        storage.save_model(model)

    print(f"\nDiscovered {len(models)} models:\n")
    for model in sorted(models, key=lambda m: m.name):
        has_rollup = " (with rollup joins)" if model.sql else ""
        print(f"--- {model.name}{has_rollup} ---")

        if model.default_time_dimension:
            print(f"  Default time dimension: {model.default_time_dimension}")

        print(f"  Dimensions ({len(model.dimensions)}):")
        for dim in model.dimensions:
            pk = " [PK]" if dim.primary_key else ""
            print(f"    {dim.name} ({dim.type}){pk}")

        print(f"  Measures ({len(model.measures)}):")
        for meas in model.measures:
            sql_info = f" sql={meas.sql}" if meas.sql else ""
            print(f"    {meas.name} ({meas.type}){sql_info}")

        if model.sql:
            print(f"  SQL:\n    {model.sql.replace(chr(10), chr(10) + '    ')}")

        print()

    print(f"Models saved to {storage_dir}")


def main() -> None:
    db_path = ensure_database()
    run_ingestion(db_path)


if __name__ == "__main__":
    main()
