"""Regression tests for the async-engine connection leak in ``execute_sync``.

Background: ``SlayerQueryEngine`` caches one ``SlayerSQLClient`` per
datasource, and each client lazily creates an async SQLAlchemy engine bound
to whichever event loop first opens a connection. ``execute_sync`` ran
``asyncio.run(coro)`` — a fresh loop per call — and never disposed the
engine before the loop closed. asyncpg's pool can't free its connections
without a live loop, so each call leaked at least one server-side
connection until TCP keepalive expired. Multi-block deck resolves in
Storyline exhausted the warehouse's ``role`` connection limit
(STORYLINE-BG-API-37).

The fix: ``SlayerQueryEngine.aclose()`` disposes every cached client's
``_async_engine``; ``execute_sync`` calls it in a ``finally`` so disposal
runs inside the same loop that owned the connections.
"""

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
    """Engine with a single cached SlayerSQLClient whose ``_async_engine``
    is a tracking mock. The datasource is typed ``postgres`` so the leak
    path is the one being exercised (sqlite/duckdb skip the async engine
    entirely via ``_INLINE_SYNC_DB_TYPES``).
    """
    storage = YAMLStorage(base_dir=str(workspace))
    engine = SlayerQueryEngine(storage=storage)

    ds = DatasourceConfig(
        name="leaky", type="postgres",
        host="example.invalid", port=5432, database="x",
        # No username/password — these clients only host MagicMock async engines,
        # the real connection string is never used. Avoids Sonar S2068.
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
        # The SlayerSQLClient instance is KEPT in the cache (so :memory:
        # SQLite's StaticPool-pinned _sync_engine survives across calls);
        # only the async engine reference is nulled.
        assert engine._sql_clients, "client cache must persist for sync-engine reuse"
        assert client._async_engine is None

    async def test_aclose_is_idempotent(self, workspace: Path) -> None:
        engine, fake_engine = _build_engine_with_fake_pg_client(workspace)

        await engine.aclose()
        await engine.aclose()

        # Dispose ran exactly once — the second call finds ``_async_engine``
        # already None on every cached client and is a no-op rather than
        # re-disposing the now-stale reference.
        fake_engine.dispose.assert_awaited_once()

    async def test_client_aclose_nulls_engine_before_dispose(
        self, workspace: Path,
    ) -> None:
        """A failed dispose mustn't leave a half-torn engine cached. The
        client nulls its ``_async_engine`` reference BEFORE awaiting
        dispose; subsequent calls see ``None`` and treat the client as
        already closed.
        """
        engine, fake_engine = _build_engine_with_fake_pg_client(workspace)
        client = next(iter(engine._sql_clients.values()))

        # Sync side_effect (no awaits needed) — AsyncMock invokes it the
        # same way and Sonar S7503 stays clean.
        def dispose_and_check() -> None:
            assert client._async_engine is None
        fake_engine.dispose = AsyncMock(side_effect=dispose_and_check)

        await engine.aclose()
        fake_engine.dispose.assert_awaited_once()

    async def test_dispose_failure_logged_not_raised(
        self, workspace: Path, caplog,
    ) -> None:
        engine, fake_engine = _build_engine_with_fake_pg_client(workspace)
        fake_engine.dispose = AsyncMock(
            side_effect=RuntimeError("simulated dispose failure"),
        )

        # aclose() must not raise — a dispose failure can't be allowed
        # to mask the real query error in execute_sync's finally block.
        await engine.aclose()

    def test_execute_sync_disposes_on_success(self, workspace: Path) -> None:
        """End-to-end leak check using an in-process SQLite datasource:
        ``execute_sync`` returns a result AND every async engine it
        created has been disposed before the run_sync loop closes.

        SQLite itself doesn't use the async-engine path
        (``_INLINE_SYNC_DB_TYPES``), so we instead instrument the cache
        directly: install a tracking mock before the call and confirm
        it was disposed afterwards.
        """
        # Build a real sqlite datasource so engine.execute() actually
        # runs end-to-end.
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

        # Pre-populate a fake postgres-typed client whose async engine is
        # tracked. The query itself won't go through this client (it's
        # against the sqlite "lite" datasource), but the finally-dispose
        # has to clean it up regardless.
        ds_fake = DatasourceConfig(
            name="leaky-extra", type="postgres",
            host="example.invalid", port=5432, database="x",
            # No username/password — fake mock-only clients (Sonar S2068).
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
        # Disposal ran inside the same loop that owned the engine,
        # BEFORE asyncio.run closed it.
        fake_engine.dispose.assert_awaited_once()
        # Client is KEPT in the cache; only its async engine was nulled
        # so the next call lazily rebuilds it on the new loop.
        assert client._async_engine is None
        assert engine._sql_clients, "client cache must persist for sync-engine reuse"

    def test_execute_sync_disposes_even_on_error(
        self, workspace: Path,
    ) -> None:
        """A query error must NOT prevent disposal — that's the original
        leak: every Storyline block was failing or succeeding, and
        either way the engine was orphaned.
        """
        storage = YAMLStorage(base_dir=str(workspace))
        engine = SlayerQueryEngine(storage=storage)

        ds = DatasourceConfig(
            name="leaky", type="postgres",
            host="example.invalid", port=5432, database="x",
            # No username/password — fake mock-only clients (Sonar S2068).
        )
        client = SlayerSQLClient(datasource=ds)
        fake_engine = MagicMock()
        fake_engine.dispose = AsyncMock()
        client._async_engine = fake_engine
        engine._sql_clients[("leaky", "postgres")] = client

        # No model named "missing" → query() raises during enrichment.
        with pytest.raises(Exception):
            engine.execute_sync(SlayerQuery(
                source_model="missing", measures=["*:count"],
            ))

        # Disposal still happened.
        fake_engine.dispose.assert_awaited_once()

    def test_no_leak_across_repeated_execute_sync_calls(
        self, workspace: Path,
    ) -> None:
        """The reported failure mode: a loop of ``execute_sync`` calls
        steadily accumulating server-side connections. With the fix,
        the cache should be empty between calls and every fake engine
        we install should be disposed exactly once.
        """
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

        # Install ONE fake postgres-typed client up front; across N
        # execute_sync calls we re-attach a fresh async engine to it
        # each time and confirm every previous one was disposed exactly
        # once. This mirrors the real Storyline pattern: one engine /
        # one client, called repeatedly across asyncio.run boundaries.
        ds_fake = DatasourceConfig(
            name="fake", type="postgres",
            host="x.invalid", port=5432, database="x",
            # No username/password — fake mock-only clients (Sonar S2068).
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
            # Client persists across calls so its _sync_engine (and any
            # pinned ``:memory:`` SQLite StaticPool connection) survives.
            assert client_i in engine._sql_clients.values()
            # But the per-call async engine is gone — disposed inside
            # the live loop before asyncio.run closed it.
            assert client_i._async_engine is None, (
                f"iter {i}: async engine should be nulled after execute_sync"
            )

        # Every fake we installed got disposed exactly once.
        for i, fake in enumerate(fakes):
            fake.dispose.assert_awaited_once_with()
            assert fake.dispose.await_count == 1, f"iter {i}: disposed {fake.dispose.await_count} times"

    def test_sync_engine_survives_across_execute_sync_calls(
        self, workspace: Path,
    ) -> None:
        """The reviewer's concrete concern: ``:memory:`` SQLite pins its
        data inside ``SlayerSQLClient._sync_engine``'s ``StaticPool``. If
        ``aclose`` discarded the client (or rebuilt its sync engine)
        between calls, the next ``execute_sync`` would see a fresh empty
        in-memory DB.

        Use a file-backed SQLite for the test (``:memory:`` itself can't
        be probed end-to-end — the DEV-1538 affinity-widening introspect
        path opens its own connection that doesn't see the StaticPool's
        pinned in-memory state). The reviewer's concern reduces to:
        ``client._sync_engine`` must be the SAME object across calls. If
        that identity holds for a file-backed DS, it holds for
        ``:memory:`` too, since the cache logic is identical.
        """
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
        # Same client + same sync engine object — the engine the
        # reviewer is worried about losing for :memory: SQLite.
        assert client in engine._sql_clients.values()
        assert client._sync_engine is sync_engine_first, (
            "client._sync_engine was rebuilt — :memory: data would be lost "
            "in the analogous in-memory case"
        )
