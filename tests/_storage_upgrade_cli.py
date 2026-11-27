"""Run ``slayer storage upgrade`` in process, and build the stores the upgrade tests read."""

from __future__ import annotations

import asyncio
import contextlib
import io
import os
import sys
from collections.abc import Sequence
from typing import Any

import duckdb
from pydantic import BaseModel

from slayer.cli import main as cli_main
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests import _granularity_rename_fixtures as gran
from tests._integrator_1x import DATA_SOURCE, DATA_SQL, raw_documents

#: A memory as the 1.x line stored it, bundling a query.
OLD_MEMORY = {
    "version": 4, "id": "m1", "learning": "revenue by status", "entities": [],
    "query": {"version": 6, "source_model": "orders", "measures": [{"formula": "sum(amount)", "name": "rev"}]},
}
OLD_DATASOURCE_VERSION = 2
REVENUE_BY_STATUS = {"population": "orders", "dimensions": ["orders.status"], "metrics": ["sum(orders.amount)"]}


class UpgradeRun(BaseModel):
    code: int
    out: str
    err: str


def upgrade(storage_path: str, *args: str) -> UpgradeRun:
    """``slayer storage upgrade --storage <storage_path> <args>``: exit code, stdout, stderr."""
    out, err = io.StringIO(), io.StringIO()
    argv = sys.argv
    sys.argv = ["slayer", "storage", "upgrade", "--storage", storage_path, *args]
    code = 0
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            cli_main()
    except SystemExit as exc:  # NOSONAR(S5754) — the exit code is the result under test
        code = int(exc.code or 0)
    finally:
        sys.argv = argv
    return UpgradeRun(code=code, out=out.getvalue(), err=err.getvalue())


def store_path(storage: StorageBackend) -> str:
    if isinstance(storage, YAMLStorage):
        return storage.base_dir
    assert isinstance(storage, SQLiteStorage)
    return storage.db_path


def warehouse_db(base: str) -> str:
    """A DuckDB file holding the integrator fixture data."""
    os.makedirs(base, exist_ok=True)
    path = os.path.join(base, "warehouse.duckdb")
    con = duckdb.connect(path)
    try:
        for statement in (s.strip() for s in DATA_SQL.read_text().split(";")):
            if statement:
                con.execute(statement)
    finally:
        con.close()
    return path


def warehouse_datasource(db_path: str, *, name: str = DATA_SOURCE) -> dict[str, Any]:
    return {"version": OLD_DATASOURCE_VERSION, "name": name, "type": "duckdb", "database": db_path}


def warehouse_store(
    *, backend: str, base: str, datasource: bool = True, datasets: list[dict[str, Any]] | None = None,
    extra_datasources: Sequence[dict[str, Any]] = (), memories: list[dict[str, Any]] | None = None,
) -> StorageBackend:
    """A store written verbatim: the integrator's v15 datasets, an old memory and an old datasource config."""
    db_path = warehouse_db(base)
    return asyncio.run(gran.write_raw_store(
        backend=backend, base=base,
        datasources=[*([warehouse_datasource(db_path)] if datasource else []), *extra_datasources],
        datasets=list(raw_documents().values()) if datasets is None else datasets,
        memories=[OLD_MEMORY] if memories is None else memories,
    ))


def raw(*, backend: str, base: str) -> dict[str, str]:
    """Every stored document exactly as stored, keyed ``<kind>/<name>``."""
    return gran.raw_documents(backend=backend, base=base)
