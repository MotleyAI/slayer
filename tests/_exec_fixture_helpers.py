"""Shared dual-backend (SQLite + DuckDB) exec-engine helpers for the per-DEV
windowed fixture modules; consolidates the byte-identical copies they carried."""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator, Callable, List

import pytest

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


async def engine_for(
    *, dialect: str, db_path: str, models: List[SlayerModel]
) -> SlayerQueryEngine:
    """Storage + engine over ``models`` bound to a seeded ``dialect`` DB file."""
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path)
    )
    for model in models:
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


async def make_exec_engine(
    request,
    *,
    seed_sqlite: Callable[[str], None],
    seed_duckdb: Callable[[str], None],
    models: List[SlayerModel],
) -> AsyncIterator[SlayerQueryEngine]:
    """Fixture body: seed the backend file and yield the engine (each module
    wraps this so the fixture name lives where used)."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        (seed_sqlite if dialect == "sqlite" else seed_duckdb)(db_path)
        yield await engine_for(dialect=dialect, db_path=db_path, models=models)


def month_key(value) -> str:
    """Stable per-month key across SQLite text and DuckDB timestamp values."""
    return str(value)[:7]


def rows_by(resp, *keys) -> dict:
    """Index ``resp.data`` rows by the given result-column key tuple."""
    out = {}
    for r in resp.data:
        out[tuple(r[k] for k in keys)] = r
    assert len(out) == len(resp.data), "duplicate result rows for one group key"
    return out
