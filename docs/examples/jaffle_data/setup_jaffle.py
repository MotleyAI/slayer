"""Shared setup for Jaffle Shop example notebooks.

Thin wrapper over ``slayer.demo.jaffle_shop.ensure_demo_datasource`` — pins the
DuckDB + SLayer-models location next to this file so notebooks can reuse a
single on-disk dataset across runs.
"""

import os
from typing import List, Tuple

from slayer.core.models import SlayerModel
from slayer.demo.jaffle_shop import ensure_demo_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.path.join(_THIS_DIR, "demo", "jaffle_shop.duckdb")
MODELS_DIR = os.path.join(_THIS_DIR, "slayer_models")


def ensure_jaffle_shop(
    years: int = 3,
) -> Tuple[SlayerQueryEngine, YAMLStorage, List[SlayerModel]]:
    """Ensure the Jaffle Shop DuckDB and SLayer models exist; return an engine.

    On first run, generates ~``years`` of synthetic data with jafgen and ingests
    the models. Subsequent runs reuse the existing database and models.
    """
    storage = YAMLStorage(base_dir=MODELS_DIR)

    _ds, models, db_built = ensure_demo_datasource(
        storage,
        storage_path=_THIS_DIR,
        years=years,
        ingest_models=True,
        assume_yes=True,
    )

    if db_built:
        print(f"Database created at {DB_PATH}")

    engine = SlayerQueryEngine(storage=storage)
    return engine, storage, models
