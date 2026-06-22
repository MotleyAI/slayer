"""Regression tests for the async-engine leak in ``execute_sync``
(STORYLINE-BG-API-37). See ``SlayerQueryEngine.aclose`` for the fix."""

from __future__ import annotations

import asyncio
import math
import tempfile
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace() -> Iterator[Path]:
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _build_engine_with_fake_pg_client(
    workspace: Path,
) -> tuple[SlayerQueryEngine, MagicMock]:
    """Engine with one cached postgres-typed client whose ``_async_engine``
    is a tracking mock (so the leak path is exercised; sqlite/duckdb skip it)."""
    storage = YAMLStorage(base_dir=str(workspace))
    engine = SlayerQueryEngine(storage=storage)
    ds = DatasourceConfig(
        name="leaky", type="postgres",
        host="example.invalid", port=5432, database="x",
    )
    client = SlayerSQLClient(datasource=ds)
    fake_engine = MagicMock(name="fake_async_engine")
    fake_engine.dispose = AsyncMock(name="dispose")
    client._async_engine = fake_engine
    engine._sql_clients[("leaky", "postgres")] = client
    return engine, fake_engine


class TestAsyncEngineDisposal:

    async def test_aclose_disposes_cached_async_engine(
        self, workspace: Path,
    ) -> None:
        engine, fake_engine = _build_engine_with_fake_pg_client(workspace)
        client = next(iter(engine._sql_clients.values()))

        await engine.aclose()

        fake_engine.dispose.assert_awaited_once()
        # Client stays cached (sync engine survives); only async engine nulled.
        assert engine._sql_clients
        assert client._async_engine is None

    async def test_aclose_is_idempotent(self, workspace: Path) -> None:
        engine, fake_engine = _build_engine_with_fake_pg_client(workspace)
        await engine.aclose()
        await engine.aclose()
        fake_engine.dispose.assert_awaited_once()

    async def test_client_aclose_nulls_engine_before_dispose(
        self, workspace: Path,
    ) -> None:
        """``_async_engine`` is None'd BEFORE dispose so a failure can't
        leave a half-torn engine cached."""
        engine, fake_engine = _build_engine_with_fake_pg_client(workspace)
        client = next(iter(engine._sql_clients.values()))

        def dispose_and_check() -> None:
            assert client._async_engine is None
        fake_engine.dispose = AsyncMock(side_effect=dispose_and_check)

        await engine.aclose()
        fake_engine.dispose.assert_awaited_once()

    async def test_dispose_failure_logged_not_raised(
        self, workspace: Path, caplog,
    ) -> None:
        """Dispose errors must not mask the real query error in ``finally``."""
        engine, fake_engine = _build_engine_with_fake_pg_client(workspace)
        fake_engine.dispose = AsyncMock(
            side_effect=RuntimeError("simulated dispose failure"),
        )
        await engine.aclose()  # does not raise

    def test_execute_sync_disposes_on_success(self, workspace: Path) -> None:
        """``execute_sync`` triggers aclose in its ``finally`` before the loop closes."""
        import sqlite3
        db = workspace / "live.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val REAL)")
        conn.execute("INSERT INTO t (val) VALUES (1.0), (2.0), (3.0)")
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(workspace / "store"))
        asyncio.run(storage.save_datasource(
            DatasourceConfig(name="lite", type="sqlite", database=str(db)),
        ))
        asyncio.run(storage.save_model(SlayerModel(
            name="t", data_source="lite", sql_table="t",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="val", sql="val", type=DataType.DOUBLE),
            ],
        )))
        engine = SlayerQueryEngine(storage=storage)

        # Plant a tracked engine on an unrelated client; ``finally`` must still dispose it.
        ds_fake = DatasourceConfig(
            name="leaky-extra", type="postgres",
            host="example.invalid", port=5432, database="x",
        )
        client = SlayerSQLClient(datasource=ds_fake)
        fake_engine = MagicMock()
        fake_engine.dispose = AsyncMock()
        client._async_engine = fake_engine
        engine._sql_clients[("leaky-extra", "postgres")] = client

        result = engine.execute_sync(SlayerQuery(
            source_model="t", measures=["val:sum"],
        ))
        assert math.isclose(result.data[0]["t.val_sum"], 6.0)
        fake_engine.dispose.assert_awaited_once()
        assert client._async_engine is None
        assert engine._sql_clients

    def test_execute_sync_disposes_even_on_error(
        self, workspace: Path,
    ) -> None:
        """Disposal runs in ``finally`` even when the query raises."""
        storage = YAMLStorage(base_dir=str(workspace))
        engine = SlayerQueryEngine(storage=storage)
        ds = DatasourceConfig(
            name="leaky", type="postgres",
            host="example.invalid", port=5432, database="x",
        )
        client = SlayerSQLClient(datasource=ds)
        fake_engine = MagicMock()
        fake_engine.dispose = AsyncMock()
        client._async_engine = fake_engine
        engine._sql_clients[("leaky", "postgres")] = client

        with pytest.raises(Exception):
            engine.execute_sync(SlayerQuery(
                source_model="missing", measures=["*:count"],
            ))
        fake_engine.dispose.assert_awaited_once()

    def test_no_leak_across_repeated_execute_sync_calls(
        self, workspace: Path,
    ) -> None:
        """Re-attach a fresh fake engine to one cached client across 20
        ``execute_sync`` calls; every previous engine must be disposed
        exactly once (the Storyline N-block pattern)."""
        import sqlite3
        db = workspace / "live.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(workspace / "store"))
        asyncio.run(storage.save_datasource(
            DatasourceConfig(name="lite", type="sqlite", database=str(db)),
        ))
        asyncio.run(storage.save_model(SlayerModel(
            name="t", data_source="lite", sql_table="t",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        )))
        engine = SlayerQueryEngine(storage=storage)

        ds_fake = DatasourceConfig(
            name="fake", type="postgres",
            host="x.invalid", port=5432, database="x",
        )
        client_i = SlayerSQLClient(datasource=ds_fake)
        engine._sql_clients[("fake", "postgres")] = client_i

        fakes: list[Any] = []
        for i in range(20):
            fake = MagicMock()
            fake.dispose = AsyncMock()
            client_i._async_engine = fake
            fakes.append(fake)

            engine.execute_sync(SlayerQuery(
                source_model="t", measures=["*:count"],
            ))
            assert client_i in engine._sql_clients.values()
            assert client_i._async_engine is None, f"iter {i}"

        for i, fake in enumerate(fakes):
            fake.dispose.assert_awaited_once_with()
            assert fake.dispose.await_count == 1, f"iter {i}"

    def test_sync_engine_survives_across_execute_sync_calls(
        self, workspace: Path,
    ) -> None:
        """``client._sync_engine`` must be the SAME object across calls — the
        identity that lets ``:memory:`` SQLite's StaticPool keep its data.
        File-backed DB stands in for ``:memory:`` since the cache logic is
        identical and ``:memory:`` can't be probed past DEV-1538's introspect."""
        import sqlite3
        db = workspace / "live.db"
        c = sqlite3.connect(db)
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        c.execute("INSERT INTO t (v) VALUES (10), (20), (30)")
        c.commit()
        c.close()

        storage = YAMLStorage(base_dir=str(workspace / "store"))
        asyncio.run(storage.save_datasource(
            DatasourceConfig(name="lite", type="sqlite", database=str(db)),
        ))
        asyncio.run(storage.save_model(SlayerModel(
            name="t", data_source="lite", sql_table="t",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="v", sql="v", type=DataType.INT),
            ],
        )))
        engine = SlayerQueryEngine(storage=storage)

        r1 = engine.execute_sync(SlayerQuery(
            source_model="t", measures=["*:count", "v:sum"],
        ))
        assert r1.data[0]["t._count"] == 3
        assert r1.data[0]["t.v_sum"] == 60
        assert len(engine._sql_clients) == 1
        client = next(iter(engine._sql_clients.values()))
        sync_engine_first = client._sync_engine

        r2 = engine.execute_sync(SlayerQuery(
            source_model="t", measures=["*:count", "v:sum"],
        ))
        assert r2.data[0]["t._count"] == 3
        assert client in engine._sql_clients.values()
        assert client._sync_engine is sync_engine_first, (
            "_sync_engine rebuilt — :memory: data would be lost"
        )
