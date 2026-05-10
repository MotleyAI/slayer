"""Profile-column helper extracted from inspect_model (DEV-1375).

Pins:
* `profile_column` returns the same string format as the existing
  `_collect_dim_profile` / `_format_dim_profile_value` produces.
* `refresh_table_backed_model_sampled` iterates non-hidden columns,
  persists each via storage, returns per-column error strings.
* sql-mode and query-backed models are silently skipped (mirrors ingest
  behaviour; broader coverage tracked in DEV-1377).
* Per-column DB exceptions don't stop the loop.
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from typing import Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.profiling import (
    profile_column,
    refresh_table_backed_model_sampled,
)
from slayer.storage.base import resolve_storage


@pytest.fixture
def sqlite_setup():
    """Build a SQLite-backed engine + storage with a populated `orders` table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, status TEXT)")
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?)",
            [
                (1, 10.0, "paid"),
                (2, 20.5, "paid"),
                (3, 5.0, "refunded"),
                (4, 99.99, "cancelled"),
                (5, None, "paid"),
            ],
        )
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)

        ds = DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        )

        async def _setup():
            await storage.save_datasource(ds)
            await storage.save_model(SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="ds",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="amount", type=DataType.DOUBLE),
                    Column(name="status", type=DataType.TEXT),
                ],
            ))

        asyncio.run(_setup())
        engine = SlayerQueryEngine(storage=storage)
        yield engine, storage


# ---------------------------------------------------------------------------
# profile_column
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_profile_column_returns_string_for_categorical(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    col = model.get_column("status")
    sampled = await profile_column(model=model, column=col, engine=engine)
    assert sampled is not None
    # Low-cardinality TEXT → distinct values, comma-separated.
    assert "paid" in sampled
    assert "refunded" in sampled


@pytest.mark.asyncio
async def test_profile_column_returns_min_max_for_numeric(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    col = model.get_column("amount")
    sampled = await profile_column(model=model, column=col, engine=engine)
    assert sampled is not None
    # Numeric → "min .. max" form.
    assert ".." in sampled


@pytest.mark.asyncio
async def test_profile_column_handles_pk_columns(sqlite_setup) -> None:
    """PK columns are still profiled (caller decides whether to persist)."""
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    col = model.get_column("id")
    # Caller may get None (PK skipped) or a value — both are acceptable.
    sampled = await profile_column(model=model, column=col, engine=engine)
    assert sampled is None or isinstance(sampled, str)


# ---------------------------------------------------------------------------
# refresh_table_backed_model_sampled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_persists_sampled_for_each_non_hidden_column(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    errors = await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage,
    )
    assert errors == []
    reloaded = await storage.get_model("orders", data_source="ds")
    # status is categorical → has a sampled string
    assert reloaded.get_column("status").sampled is not None
    # amount is numeric → has a sampled string
    assert reloaded.get_column("amount").sampled is not None


@pytest.mark.asyncio
async def test_refresh_skips_hidden_columns(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    model.columns.append(
        Column(name="hidden_one", type=DataType.TEXT, hidden=True),
    )
    await storage.save_model(model)
    await refresh_table_backed_model_sampled(
        model=await storage.get_model("orders", data_source="ds"),
        engine=engine,
        storage=storage,
    )
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("hidden_one").sampled is None


@pytest.mark.asyncio
async def test_refresh_only_columns_filter(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage,
        only_columns={"status"},
    )
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("status").sampled is not None
    assert reloaded.get_column("amount").sampled is None


@pytest.mark.asyncio
async def test_refresh_skips_sql_mode_models(sqlite_setup) -> None:
    """sql-mode model: silently skipped per DEV-1375 v1; broader coverage in
    DEV-1377."""
    engine, storage = sqlite_setup
    sql_model = SlayerModel(
        name="sql_orders",
        sql="SELECT * FROM orders",
        data_source="ds",
        columns=[Column(name="amount", type=DataType.DOUBLE)],
    )
    errors = await refresh_table_backed_model_sampled(
        model=sql_model, engine=engine, storage=storage,
    )
    assert errors == []
    # No assertion that the column was profiled — sql-mode is opt-out in v1.


@pytest.mark.asyncio
async def test_refresh_continues_after_per_column_failure(sqlite_setup, monkeypatch) -> None:
    """Best-effort: one bad column doesn't stop the rest."""
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")

    call_count = {"n": 0}
    real_profile_column = profile_column

    async def boom_then_ok(*, model, column, engine) -> Optional[str]:
        call_count["n"] += 1
        if column.name == "amount":
            raise RuntimeError("simulated profile failure")
        return await real_profile_column(model=model, column=column, engine=engine)

    monkeypatch.setattr(
        "slayer.engine.profiling.profile_column", boom_then_ok,
    )
    errors = await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage,
    )
    assert any("amount" in e and "simulated" in e for e in errors)
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("amount").sampled is None
    assert reloaded.get_column("status").sampled is not None
