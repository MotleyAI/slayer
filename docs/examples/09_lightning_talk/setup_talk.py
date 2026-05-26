"""Setup helper for the DEV-1473 lightning-talk notebook.

Like ``docs/examples/jaffle_data/setup_jaffle.py``, but with an isolated
``YAMLStorage`` rooted next to the notebook so ``save_memory`` calls in
the talk don't bleed into the other example notebooks. The Jaffle Shop
DuckDB itself is shared across notebooks — only the SLayer model /
memory store is local.

Returns ``(engine, storage, client, models)``. Idempotent: re-runs reuse
the existing DuckDB file and YAML store. Pre-deletes the two stable
memory ids the notebook saves so partial prior runs cannot leave stale
state behind.
"""

import os
from typing import List, Tuple

from slayer.async_utils import run_sync
from slayer.client.slayer_client import SlayerClient
from slayer.core.errors import MemoryNotFoundError
from slayer.core.models import SlayerModel
from slayer.demo.jaffle_shop import ensure_demo_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_JAFFLE_DATA_DIR = os.path.normpath(os.path.join(_THIS_DIR, "..", "jaffle_data"))

LIGHTNING_MODELS_DIR = os.path.join(_THIS_DIR, "slayer_models")
BROOKLYN_MEMORY_ID = "lightning.brooklyn_pos"
TOP_CUSTOMERS_MEMORY_ID = "lightning.top_customers"


def _pre_clean_memories(client: SlayerClient) -> None:
    """Best-effort: drop the two stable memory ids so a partial prior run
    can't taint the search ranks of the next run."""
    try:
        run_sync(client.forget_memory("lightning.brooklyn_pos"))
    except MemoryNotFoundError:
        pass
    try:
        run_sync(client.forget_memory("lightning.top_customers"))
    except MemoryNotFoundError:
        pass


def ensure_lightning_talk_demo(
    years: int = 3,
) -> Tuple[SlayerQueryEngine, YAMLStorage, SlayerClient, List[SlayerModel]]:
    """Ensure the isolated lightning-talk YAML store is populated and return
    a ready-to-use ``(engine, storage, client, models)`` 4-tuple.

    - DuckDB lives at ``docs/examples/jaffle_data/demo/jaffle_shop.duckdb``
      (shared with the other example notebooks).
    - Models live at ``docs/examples/09_lightning_talk/slayer_models/``
      (isolated; ``save_memory`` calls only affect this dir).
    - Both stable memory ids are pre-deleted (best-effort) so re-running
      the notebook is idempotent after partial prior runs.
    """
    storage = YAMLStorage(base_dir=LIGHTNING_MODELS_DIR)

    _ds, models, _db_built = ensure_demo_datasource(
        storage=storage,
        storage_path=_JAFFLE_DATA_DIR,
        years=years,
        ingest_models=True,
        assume_yes=True,
    )

    engine = SlayerQueryEngine(storage=storage)
    client = SlayerClient(storage=storage)
    _pre_clean_memories(client)

    return engine, storage, client, models
