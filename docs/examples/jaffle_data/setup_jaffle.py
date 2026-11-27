"""Shared setup for Jaffle Shop example notebooks.

Thin wrapper over ``slayer.demo.jaffle_shop.ensure_demo_datasource`` — pins the
DuckDB + SLayer-datasets location next to this file so notebooks can reuse a
single on-disk dataset across runs.
"""

import os
import warnings
from typing import List, Tuple

from slayer.core.models import SlayerDataset
from slayer.demo.jaffle_shop import ensure_demo_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.path.join(_THIS_DIR, "demo", "jaffle_shop.duckdb")
DATASETS_DIR = os.path.join(_THIS_DIR, "slayer_datasets")


def ensure_jaffle_shop(
    years: int = 3,
) -> Tuple[SlayerQueryEngine, YAMLStorage, List[SlayerDataset]]:
    """Ensure the Jaffle Shop DuckDB and SLayer datasets exist; return an engine.

    On first run, generates ~``years`` of synthetic data with jafgen and ingests
    the datasets. Subsequent runs reuse the existing database and datasets.
    """
    # Machine-independent notebook outputs: no paths in warnings (kernel-wide by design).
    warnings.formatwarning = lambda msg, cat, *a, **k: f"{cat.__name__}: {msg}\n"

    storage = YAMLStorage(base_dir=DATASETS_DIR)

    _ds, datasets, db_built = ensure_demo_datasource(
        storage,
        storage_path=_THIS_DIR,
        years=years,
        ingest_datasets=True,
        assume_yes=True,
    )

    if db_built:
        print(f"Database created at {os.path.relpath(DB_PATH, start=_THIS_DIR)}")

    engine = SlayerQueryEngine(storage=storage)
    return engine, storage, datasets
