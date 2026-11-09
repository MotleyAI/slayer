"""Shared Jaffle Shop demo setup for integration tests.

The demo DuckDB is built once per run (``shared_demo_duckdb``); each server fixture copies that
prebuilt file into its own fresh storage dir. ``ensure_demo_datasource``'s
reuse fast-path then skips the expensive jafgen build, while per-dir ingest
still runs so each copy keeps correct in-dir datasource paths and full
isolation between concurrently-running servers.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from slayer.cli import _prepare_demo, _resolve_storage
from slayer.demo.jaffle_shop import build_jaffle_shop, resolve_demo_db_path

NOTEBOOK_DEMO_DIR = Path(__file__).resolve().parents[2] / "docs" / "examples" / "jaffle_data"
NOTEBOOK_DEMO_DB = NOTEBOOK_DEMO_DIR / "demo" / "jaffle_shop.duckdb"
NOTEBOOK_DEMO_YEARS = 3


def build_notebook_demo_db() -> None:
    """Build the example notebooks' demo DuckDB, or reuse a current one with dates shifted to today."""
    NOTEBOOK_DEMO_DB.parent.mkdir(parents=True, exist_ok=True)
    build_jaffle_shop(db_path=str(NOTEBOOK_DEMO_DB), years=NOTEBOOK_DEMO_YEARS)


def prepare_demo_storage(*, base_dir: str, prebuilt_duckdb: str | None = None):
    """Return ``(args, storage)`` for the demo storage dir at ``base_dir``.

    The caller owns ``base_dir`` (e.g. a pytest ``tmp_path``) and its cleanup.
    With ``prebuilt_duckdb`` the file is copied in first, so the demo build
    reuse-path skips jafgen; ingest still runs per dir with the correct in-dir
    database path.
    """
    args = argparse.Namespace(
        storage=base_dir, datasource=None, force=False
    )
    storage = _resolve_storage(args)
    if prebuilt_duckdb is not None:
        shutil.copy2(prebuilt_duckdb, resolve_demo_db_path(base_dir))
    _prepare_demo(args, storage)
    return args, storage
