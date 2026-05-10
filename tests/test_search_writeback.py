"""Sample-value refresh hooks (DEV-1375).

Pins the four refresh trigger paths from §7 of the spec:

1. ``ingest`` populates ``Column.sampled`` for every column on every
   table-backed model in the touched datasource.
2. ``slayer search refresh-samples`` does the same on demand.
3. ``edit_model`` invalidates and immediately recomputes:
   - column sql/filter/type/name change → just that column;
   - SlayerModel.filters / sql / source_queries change → every column.
4. ``inspect_model`` lazily fills the cache on a miss (read cached when
   present; if None, profile live and persist via
   ``update_column_sampled`` before returning).
"""

from __future__ import annotations

import sqlite3

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import resolve_storage


@pytest.fixture
def sqlite_table_setup(tmp_path):
    """A real SQLite DB with a populated `orders` table + a storage backend."""
    db_file = str(tmp_path / "data.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, status TEXT)")
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [(1, 10.0, "paid"), (2, 5.5, "refunded"), (3, 99.9, "paid")],
    )
    conn.commit()
    conn.close()
    storage_dir = str(tmp_path / "storage")
    storage = resolve_storage(storage_dir)
    yield storage, db_file


# ---------------------------------------------------------------------------
# (1) ingest populates Column.sampled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_populates_column_sampled(sqlite_table_setup) -> None:
    storage, db_file = sqlite_table_setup
    ds = DatasourceConfig(name="ds", type="sqlite", database=db_file)
    await storage.save_datasource(ds)
    from slayer.engine.ingestion import ingest_datasource_idempotent
    await ingest_datasource_idempotent(datasource=ds, storage=storage)
    loaded = await storage.get_model("orders", data_source="ds")
    # Both non-PK columns should have sampled values populated.
    assert loaded.get_column("amount").sampled is not None
    assert loaded.get_column("status").sampled is not None


# ---------------------------------------------------------------------------
# (2) slayer search refresh-samples
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_samples_via_engine_helper(sqlite_table_setup) -> None:
    """The CLI subcommand wires through to a single engine helper that can
    be exercised directly."""
    storage, db_file = sqlite_table_setup
    await storage.save_datasource(DatasourceConfig(
        name="ds", type="sqlite", database=db_file,
    ))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
    ))
    engine = SlayerQueryEngine(storage=storage)
    from slayer.engine.profiling import refresh_all_table_backed_sampled
    errors = await refresh_all_table_backed_sampled(
        engine=engine, storage=storage, data_source="ds",
    )
    assert errors == [] or all(isinstance(e, str) for e in errors)
    loaded = await storage.get_model("orders", data_source="ds")
    assert loaded.get_column("amount").sampled is not None


# ---------------------------------------------------------------------------
# (3) edit_model invalidates + recomputes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_edit_model_column_change_recomputes_only_that_column(
    sqlite_table_setup,
) -> None:
    storage, db_file = sqlite_table_setup
    await storage.save_datasource(DatasourceConfig(
        name="ds", type="sqlite", database=db_file,
    ))
    model = SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="amount", type=DataType.DOUBLE, sampled="OLD AMOUNT"),
            Column(name="status", type=DataType.TEXT, sampled="OLD STATUS"),
        ],
    )
    await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    from slayer.engine.profiling import handle_edit_refresh
    await handle_edit_refresh(
        engine=engine,
        storage=storage,
        data_source="ds",
        model_name="orders",
        changed_columns={"amount"},
        model_level_change=False,
    )
    loaded = await storage.get_model("orders", data_source="ds")
    # amount was recomputed (no longer "OLD AMOUNT")
    assert loaded.get_column("amount").sampled != "OLD AMOUNT"
    assert loaded.get_column("amount").sampled is not None
    # status was untouched
    assert loaded.get_column("status").sampled == "OLD STATUS"


@pytest.mark.asyncio
async def test_edit_model_model_level_change_recomputes_all_columns(
    sqlite_table_setup,
) -> None:
    storage, db_file = sqlite_table_setup
    await storage.save_datasource(DatasourceConfig(
        name="ds", type="sqlite", database=db_file,
    ))
    model = SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="amount", type=DataType.DOUBLE, sampled="OLD AMOUNT"),
            Column(name="status", type=DataType.TEXT, sampled="OLD STATUS"),
        ],
    )
    await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    from slayer.engine.profiling import handle_edit_refresh
    await handle_edit_refresh(
        engine=engine,
        storage=storage,
        data_source="ds",
        model_name="orders",
        changed_columns=set(),
        model_level_change=True,
    )
    loaded = await storage.get_model("orders", data_source="ds")
    # Both columns recomputed.
    assert loaded.get_column("amount").sampled != "OLD AMOUNT"
    assert loaded.get_column("status").sampled != "OLD STATUS"


# ---------------------------------------------------------------------------
# (4) inspect_model lazy fill on miss
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inspect_model_writes_back_on_sampled_miss(
    sqlite_table_setup, monkeypatch,
) -> None:
    storage, db_file = sqlite_table_setup
    await storage.save_datasource(DatasourceConfig(
        name="ds", type="sqlite", database=db_file,
    ))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),  # sampled=None
            Column(name="status", type=DataType.TEXT),    # sampled=None
        ],
    ))
    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage)
    # First call: sampled is None → live profile + writeback
    result = await mcp.call_tool("inspect_model", {
        "model_name": "orders", "data_source": "ds",
        "sections": ["columns"],
    })
    assert result is not None
    loaded = await storage.get_model("orders", data_source="ds")
    # After the call, sampled should be populated for non-PK columns.
    assert loaded.get_column("amount").sampled is not None
    assert loaded.get_column("status").sampled is not None


@pytest.mark.asyncio
async def test_inspect_model_reads_cached_sampled_without_recompute(
    sqlite_table_setup, monkeypatch,
) -> None:
    """When Column.sampled is set, inspect_model uses it and does not call
    profile_column."""
    storage, db_file = sqlite_table_setup
    await storage.save_datasource(DatasourceConfig(
        name="ds", type="sqlite", database=db_file,
    ))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="amount", type=DataType.DOUBLE, sampled="cached value"),
        ],
    ))
    profile_call_count = {"n": 0}
    from slayer.engine import profiling
    original = profiling.profile_column

    async def counting_profile(*, model, column, engine):
        profile_call_count["n"] += 1
        return await original(model=model, column=column, engine=engine)

    monkeypatch.setattr(profiling, "profile_column", counting_profile)

    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage)
    await mcp.call_tool("inspect_model", {
        "model_name": "orders", "data_source": "ds",
        "sections": ["columns"],
    })
    assert profile_call_count["n"] == 0
