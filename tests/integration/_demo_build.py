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

from slayer.cli import _prepare_demo, _resolve_storage
from slayer.demo.jaffle_shop import resolve_demo_db_path


def prepare_demo_storage(*, base_dir: str, prebuilt_duckdb: str | None = None):
    """Return ``(args, storage)`` for the demo storage dir at ``base_dir``.

    The caller owns ``base_dir`` (e.g. a pytest ``tmp_path``) and its cleanup.
    With ``prebuilt_duckdb`` the file is copied in first, so the demo build
    reuse-path skips jafgen; ingest still runs per dir with the correct in-dir
    database path.
    """
    args = argparse.Namespace(
        storage=base_dir, models_dir=None, datasource=None, force=False
    )
    storage = _resolve_storage(args)
    if prebuilt_duckdb is not None:
        shutil.copy2(prebuilt_duckdb, resolve_demo_db_path(base_dir))
    _prepare_demo(args, storage)
    return args, storage
