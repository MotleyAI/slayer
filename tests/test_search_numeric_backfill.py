"""DEV-1615: the search post-fusion column-hit hook now back-fills
genuinely-unsampled NUMERIC/temporal columns too (not just categorical).

Removing ``ensure_column_sample_fresh``'s categorical-only early-return is a
deliberate, shared behavior change: both ``inspect`` and ``search`` fill
ranges on read. These tests pin the search side against a REAL sqlite table
(no monkeypatch of the helper) so the end-to-end profile + persist is proven.

The hits are surfaced deterministically via ``entities=[<canonical>]`` (the
BM25 implicit self-reference, DEV-1513) so the numeric column is reliably a
column hit regardless of ranking.
"""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.search.service import SearchService
from slayer.storage.base import resolve_storage


@pytest_asyncio.fixture
async def search_setup(tmp_path) -> AsyncIterator[tuple[SearchService, object]]:
    """Real sqlite ``orders`` table; the numeric ``amount`` column is saved
    uncached so the post-fusion hook has something to back-fill."""
    db_file = str(tmp_path / "data.db")
    conn = sqlite3.connect(db_file)
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, status TEXT)"
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [(1, 10.0, "paid"), (2, 5.5, "refunded"), (3, 99.9, "paid")],
    )
    conn.commit()
    conn.close()

    storage = resolve_storage(str(tmp_path / "storage"))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_file)
    )
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="ds",
        description="Checkout orders.",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE,
                   description="Order total in USD."),
            Column(name="status", type=DataType.TEXT,
                   description="Order lifecycle status."),
        ],
    ))
    engine = SlayerQueryEngine(storage=storage)
    yield SearchService(storage=storage, engine=engine), storage


@pytest.mark.asyncio
async def test_search_backfills_numeric_column_hit_verbose(search_setup) -> None:
    service, storage = search_setup
    # Sanity: the numeric column starts uncached.
    pre = await storage.get_model("orders", data_source="ds")
    assert pre.get_column("amount").sampled is None

    resp = await service.search(
        entities=["ds.orders.amount"], compact=False,
    )
    amount_hits = [
        h for h in resp.results
        if h.kind == "column" and h.id == "ds.orders.amount"
    ]
    assert amount_hits, "expected the amount column as a hit"
    # Persisted min/max range — proof the numeric back-fill fired in search.
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("amount").sampled is not None
    assert ".." in reloaded.get_column("amount").sampled
    # Verbose hit text carries the refreshed min/max RANGE (not just any
    # "Sample values:" label) — proves the numeric back-fill re-rendered text.
    assert any(
        "Sample values:" in h.text and ".." in h.text for h in amount_hits
    )


@pytest.mark.asyncio
async def test_search_backfills_numeric_column_hit_compact(search_setup) -> None:
    """compact=True keeps ``text=""`` but still persists the back-filled
    range (the persist side-effect mirrors the categorical path)."""
    service, storage = search_setup
    resp = await service.search(
        entities=["ds.orders.amount"], compact=True,
    )
    amount_hits = [
        h for h in resp.results
        if h.kind == "column" and h.id == "ds.orders.amount"
    ]
    assert amount_hits, "expected the amount column as a hit"
    for h in amount_hits:
        assert h.text == ""
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("amount").sampled is not None
