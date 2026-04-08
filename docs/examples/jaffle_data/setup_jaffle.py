"""Shared setup for Jaffle Shop example notebooks.

Ensures the DuckDB database and SLayer models exist, creating them if needed.
Each notebook calls ensure_jaffle_shop() to get a ready-to-use query engine.
"""

import os
import sys
from typing import List, Tuple

import duckdb

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

# Add jaffle_data dir so we can import the data generation utils
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _THIS_DIR)

from ingest_jaffle_shop import SCHEMA_FILE, create_schema, generate_data, load_data

JAFFLE_DATA_DIR = _THIS_DIR
DB_PATH = os.path.join(JAFFLE_DATA_DIR, "jaffle_shop.duckdb")
MODELS_DIR = os.path.join(JAFFLE_DATA_DIR, "slayer_models")

_DEFAULT_TIME_DIMENSIONS = {
    "orders": "ordered_at",
    "tweets": "tweeted_at",
}


def ensure_jaffle_shop(
    years: int = 3,
) -> Tuple[SlayerQueryEngine, YAMLStorage, List[SlayerModel]]:
    """Ensure the Jaffle Shop DuckDB and SLayer models exist, then return a query engine.

    On first run, generates ~3 years of synthetic data with jafgen and ingests it.
    Subsequent runs reuse the existing database and models.

    Returns:
        (engine, storage, models) tuple ready for querying.
    """
    storage = YAMLStorage(base_dir=MODELS_DIR)

    # Generate DB if missing
    if not os.path.exists(DB_PATH):
        print("Generating Jaffle Shop data (this takes ~1-2 minutes)...")
        data_dir = generate_data(output_dir=JAFFLE_DATA_DIR, years=years)
        conn = duckdb.connect(DB_PATH)
        create_schema(conn, SCHEMA_FILE)
        load_data(conn, data_dir)
        conn.close()
        print(f"Database created at {DB_PATH}")

    # Ingest models if missing
    ds = DatasourceConfig(name="jaffle_shop", type="duckdb", database=DB_PATH)
    existing_models = storage.list_models()
    if not existing_models:
        print("Auto-ingesting models...")
        storage.save_datasource(ds)
        models = ingest_datasource(datasource=ds)
        for model in models:
            if model.name in _DEFAULT_TIME_DIMENSIONS:
                model.default_time_dimension = _DEFAULT_TIME_DIMENSIONS[model.name]
            storage.save_model(model)
        print(f"Ingested {len(models)} models")
    else:
        models = [storage.get_model(name) for name in existing_models]

    engine = SlayerQueryEngine(storage=storage)
    return engine, storage, models
