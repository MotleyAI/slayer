"""Regression tests for the async-engine leak in the sync bridge. See ``SlayerQueryEngine.aclose`` for the fix."""

from __future__ import annotations

import asyncio
import math
import tempfile
from pathlib import Path
from typing import Any, TypeVar, cast
from collections.abc import Awaitable, Coroutine, Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import client as sql_client
from slayer.database.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import plant_async_engine

_T = TypeVar("_T")


@pytest.fixture
def workspace() -> Iterator[Path]:
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _fake_async_engine(**dispose_kwargs: Any) -> MagicMock:
    fake = MagicMock(name="fake_async_engine")
    fake.dispose = AsyncMock(name="dispose", **dispose_kwargs)
    return fake


def _pg_client() -> SlayerSQLClient:
    return SlayerSQLClient(datasource=DatasourceConfig(
        name="leaky", type="postgres", host="example.invalid", port=5432, database="x",
    ))


def _engine_with_pg_client(workspace: Path) -> tuple[SlayerQueryEngine, SlayerSQLClient]:
    """Engine with one cached postgres-typed client (sqlite/duckdb build no async engine)."""
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(workspace)))
    client = _pg_client()
    engine._sql_clients[("leaky", "postgres")] = client
    return engine, client


def _sqlite_engine(workspace: Path, *, rows: str) -> SlayerQueryEngine:
    db = workspace / "live.db"
    with transaction(db) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val REAL)")
        conn.execute(f"INSERT INTO t (val) VALUES {rows}")
    storage = YAMLStorage(base_dir=str(workspace / "store"))
    asyncio.run(storage.save_datasource(DatasourceConfig(name="lite", type="sqlite", database=str(db))))
    asyncio.run(storage.save_model(SlayerModel(
        name="t", data_source="lite", sql_table="t",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="val", sql="val", type=DataType.DOUBLE),
        ],
    )))
    return SlayerQueryEngine(storage=storage)


async def _planted_then(
    monkeypatch: pytest.MonkeyPatch, client: SlayerSQLClient, fake: MagicMock, work: Awaitable[_T],
) -> _T:
    """Build ``fake`` as ``client``'s engine on the bridged loop, then run ``work`` there."""
    plant_async_engine(monkeypatch, client, fake)
    return await work


def _no_second_build(*_a: Any, **_k: Any) -> Any:
    raise AssertionError("a second async engine was built")


async def _reused_then(client: SlayerSQLClient, fake: MagicMock, work: Coroutine[Any, Any, _T]) -> _T:
    """Check ``client`` still holds ``fake`` on the bridged loop, then run ``work`` there."""
    try:
        assert client._get_async_engine() is fake
    except BaseException:
        work.close()
        raise
    return await work


class TestAsyncEngineDisposal:

    async def test_aclose_disposes_cached_async_engine(self, workspace: Path, monkeypatch) -> None:
        engine, client = _engine_with_pg_client(workspace)
        fake = _fake_async_engine()
        plant_async_engine(monkeypatch, client, fake)

        await engine.aclose()

        fake.dispose.assert_awaited_once()
        # Client stays cached (sync engine survives); the next use builds a fresh async engine.
        assert engine._sql_clients
        plant_async_engine(monkeypatch, client, _fake_async_engine())

    async def test_aclose_is_idempotent(self, workspace: Path, monkeypatch) -> None:
        engine, client = _engine_with_pg_client(workspace)
        fake = _fake_async_engine()
        plant_async_engine(monkeypatch, client, fake)
        await engine.aclose()
        await engine.aclose()
        fake.dispose.assert_awaited_once()

    async def test_failed_dispose_leaves_no_stale_engine(self, workspace: Path, monkeypatch) -> None:
        """A failed dispose can't leave a half-torn engine cached."""
        engine, client = _engine_with_pg_client(workspace)
        plant_async_engine(monkeypatch, client, _fake_async_engine(side_effect=RuntimeError("dispose failed")))

        await engine.aclose()

        plant_async_engine(monkeypatch, client, _fake_async_engine())

    async def test_dispose_failure_logged_not_raised(self, workspace: Path, monkeypatch) -> None:
        """Dispose errors must not mask the real query error in ``finally``."""
        engine, client = _engine_with_pg_client(workspace)
        plant_async_engine(
            monkeypatch, client, _fake_async_engine(side_effect=RuntimeError("simulated dispose failure")),
        )
        await engine.aclose()  # does not raise

    def test_bridge_keeps_its_loops_engine_until_close(self, workspace: Path, monkeypatch) -> None:
        """The bridge loop outlives ``run_sync``: its engine is disposed by ``close()``, not per call."""
        engine = _sqlite_engine(workspace, rows="(1.0), (2.0), (3.0)")
        client = _pg_client()
        engine._sql_clients[("leaky-extra", "postgres")] = client
        fake = _fake_async_engine()

        result = engine.run_sync(_planted_then(
            monkeypatch, client, fake, engine.execute(SlayerQuery.model_validate({
                "source_model": "t",
                "measures": ["sum(val)"],
            })),
        ))
        assert math.isclose(result.data[0]["t.val_sum"], 6.0)
        fake.dispose.assert_not_awaited()
        engine.close()
        fake.dispose.assert_awaited_once()

    def test_bridge_keeps_its_loops_engine_until_close_even_on_error(self, workspace: Path, monkeypatch) -> None:
        engine, client = _engine_with_pg_client(workspace)
        fake = _fake_async_engine()
        q = SlayerQuery(
            source_model="missing", measures=["count(*)"],  # type: ignore[arg-type]
        )
        workflow = _planted_then(monkeypatch, client, fake, engine.execute(q))
        with pytest.raises(Exception):
            engine.run_sync(workflow)
        fake.dispose.assert_not_awaited()
        engine.close()
        fake.dispose.assert_awaited_once()

    def test_repeated_bridges_share_one_engine(self, workspace: Path, monkeypatch) -> None:
        """20 bridges reuse the one engine built on the bridge loop; ``close()`` disposes it once
        (the Storyline N-block pattern)."""
        engine = _sqlite_engine(workspace, rows="(1.0)")
        client_i = SlayerSQLClient(datasource=DatasourceConfig(
            name="fake", type="postgres", host="x.invalid", port=5432, database="x",
        ))
        engine._sql_clients[("fake", "postgres")] = client_i
        fake = _fake_async_engine()
        count = SlayerQuery.model_validate({"source_model": "t", "measures": ["count(*)"]})

        engine.run_sync(_planted_then(monkeypatch, client_i, fake, engine.execute(count)))
        monkeypatch.setattr(sql_client, "create_async_engine", _no_second_build)
        for i in range(19):
            engine.run_sync(_reused_then(client_i, fake, engine.execute(count)))
            assert client_i in engine._sql_clients.values(), f"iter {i}"
        fake.dispose.assert_not_awaited()
        engine.close()
        fake.dispose.assert_awaited_once_with()

    def test_sync_engine_survives_across_execute_sync_calls(
        self, workspace: Path,
    ) -> None:
        """``client._sync_engine`` must be the SAME object across calls — the
        identity that lets ``:memory:`` SQLite's StaticPool keep its data.
        File-backed DB stands in for ``:memory:`` since the cache logic is
        identical and ``:memory:`` can't be probed past DEV-1538's introspect."""
        db = workspace / "live.db"
        with transaction(db) as c:
            c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            c.execute("INSERT INTO t (v) VALUES (10), (20), (30)")

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

        r1 = engine.execute_sync(SlayerQuery.model_validate({"source_model": "t", "measures": ["count(*)", "sum(v)"]}))
        assert r1.data[0]["t._count"] == 3
        assert r1.data[0]["t.v_sum"] == 60
        assert len(engine._sql_clients) == 1
        client = cast(SlayerSQLClient, next(iter(engine._sql_clients.values())))
        sync_engine_first = client._sync_engine

        r2 = engine.execute_sync(SlayerQuery.model_validate({"source_model": "t", "measures": ["count(*)", "sum(v)"]}))
        assert r2.data[0]["t._count"] == 3
        assert client in engine._sql_clients.values()
        assert client._sync_engine is sync_engine_first, (
            "_sync_engine rebuilt — :memory: data would be lost"
        )


async def test_a_concurrent_first_use_converges_on_one_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _pg_client()
    built: list[MagicMock] = []
    inner: list[Any] = []

    def _create(*_a: Any, **_k: Any) -> MagicMock:
        engine = _fake_async_engine()
        built.append(engine)
        if len(built) == 1:  # another first use on this loop finishes while this one builds
            inner.append(client._get_async_engine())
        return engine

    monkeypatch.setattr(sql_client, "create_async_engine", _create)
    outer = client._get_async_engine()
    assert outer is inner[0] is built[1]
    assert client._get_async_engine() is outer
    built[0].sync_engine.dispose.assert_called_once_with(close=False)
