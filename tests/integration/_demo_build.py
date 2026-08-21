"""Shared Jaffle Shop demo build helpers for integration tests (DEV-1815).

The demo DuckDB is built once per session; each server fixture copies that
prebuilt file into its own fresh storage dir. ``ensure_demo_datasource``'s
reuse fast-path then skips the expensive jafgen build, while per-dir ingest
still runs so each copy keeps correct in-dir datasource paths and full
isolation between concurrently-running servers.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile

from slayer.cli import _prepare_demo, _resolve_storage
from slayer.demo.jaffle_shop import build_jaffle_shop, resolve_demo_db_path

DEMO_YEARS = 2


def build_shared_demo_duckdb(dest_dir: str, *, years: int = DEMO_YEARS) -> str:
    """Build the Jaffle Shop DuckDB once at ``<dest_dir>/demo/jaffle_shop.duckdb``."""
    db_path = resolve_demo_db_path(dest_dir)
    build_jaffle_shop(db_path=db_path, years=years)
    return db_path


def prepare_demo_storage(*, prebuilt_duckdb: str | None = None):
    """Return ``(args, storage)`` for a fresh demo storage dir.

    With ``prebuilt_duckdb`` the file is copied into the fresh dir first, so the
    demo build reuse-path skips jafgen; ingest still runs per dir with the
    correct in-dir database path.
    """
    base = tempfile.mkdtemp(prefix="slayer-it-demo-")
    args = argparse.Namespace(
        storage=base, models_dir=None, datasource=None, force=False
    )
    storage = _resolve_storage(args)
    if prebuilt_duckdb is not None:
        shutil.copy2(prebuilt_duckdb, resolve_demo_db_path(base))
    _prepare_demo(args, storage)
    return args, storage
