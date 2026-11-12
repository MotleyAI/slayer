"""A client holds one async engine per event loop (spec: database/execution)."""

from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import logging
import threading
from collections.abc import Coroutine, Iterator
from pathlib import Path
from typing import Any

import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import client as client_module
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from tests._pg_helpers import pg_database

# Clean paths: an unclosed connection or transport fails the test.
_NO_LEAKS = pytest.mark.filterwarnings(
    "error::ResourceWarning", "error::pytest.PytestUnraisableExceptionWarning",
)

postgresql_proc = factories.postgresql_proc(port=None)

SQL = "SELECT 1 AS x"
ROWS = [{"x": 1}]

# Seconds before a cross-loop wait counts as hung.
_BOUND = 20

# Async drivers: a URL naming none (psycopg) and asyncpg.
_SCHEMES = {"psycopg": "postgresql", "asyncpg": "postgresql+asyncpg"}


@pytest.fixture(params=sorted(_SCHEMES))
def pg_ds(request: pytest.FixtureRequest, postgresql_proc: Any) -> Iterator[DatasourceConfig]:
    statements = ["CREATE TABLE orders (id INTEGER PRIMARY KEY)", "INSERT INTO orders VALUES (1), (2), (3)"]
    with pg_database(postgresql_proc, statements=statements, name=f"loops_{request.param}") as plain:
        ds = DatasourceConfig(
            name=plain.name, type="postgres",
            connection_string=(
                f"{_SCHEMES[request.param]}://{plain.username}@{plain.host}:{plain.port}/{plain.database}"
            ),
        )
        try:
            yield ds
        finally:
            engine_factory.invalidate_engine(ds)


@pytest.fixture
def builds(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Every async engine a client builds, in order."""
    built: list[Any] = []
    real = client_module.create_async_engine

    def _build(url: Any, **kwargs: Any) -> Any:
        engine = real(url, **kwargs)
        built.append(engine)
        return engine

    monkeypatch.setattr(client_module, "create_async_engine", _build)
    return built


def _refuse(what: str) -> Any:
    def _refused(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError(f"{what} called")

    return _refused


class _LoopThread:
    """A second event loop running on its own thread."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self._thread.start()

    def run(self, coro: Coroutine[Any, Any, Any]) -> Any:
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout=_BOUND)

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=_BOUND)
        self.loop.close()


# ---------------------------------------------------------------------------
# A synchronous bridge after asynchronous use
# ---------------------------------------------------------------------------


@_NO_LEAKS
async def test_sync_bridge_after_async_use(pg_ds: DatasourceConfig, tmp_path: Path) -> None:
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "store")))
    client = engine.client_for(pg_ds)
    try:
        first = await client.execute(SQL)
        bridged = engine.run_sync(client.execute(SQL))
        after = await client.execute(SQL)
    finally:
        await engine.aclose()
        engine.close()
    gc.collect()
    assert first.rows == bridged.rows == after.rows == ROWS


@_NO_LEAKS
async def test_query_engine_execute_sync_inside_a_running_loop(pg_ds: DatasourceConfig, tmp_path: Path) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(pg_ds)
    await storage.save_model(SlayerModel(
        name="orders", data_source=pg_ds.name, sql_table="orders",
        columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
    ), _validate=False)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery.model_validate({"source_model": "orders", "measures": ["count(*)"]})
    try:
        awaited = await engine.execute(query)
        bridged = engine.execute_sync(query)
        again = await engine.execute(query)
    finally:
        await engine.aclose()
        engine.close()
    gc.collect()
    assert awaited.data == bridged.data == again.data == [{"orders._count": 3}]


# ---------------------------------------------------------------------------
# One engine per loop
# ---------------------------------------------------------------------------


@_NO_LEAKS
async def test_releasing_one_loop_keeps_the_other(pg_ds: DatasourceConfig, builds: list[Any]) -> None:
    client = SlayerSQLClient(pg_ds)
    other = _LoopThread()
    try:
        assert other.run(client.execute(SQL)).rows == ROWS
        assert (await client.execute(SQL)).rows == ROWS
        assert len(builds) == 2
        await client.aclose()
        assert other.run(client.execute(SQL)).rows == ROWS
        assert len(builds) == 2
        assert (await client.execute(SQL)).rows == ROWS
        assert len(builds) == 3
    finally:
        other.run(client.aclose())
        await client.aclose()
        other.stop()
        client.close()
    gc.collect()


@pytest.mark.abandons_async_engines
def test_a_closed_loops_engine_is_dropped(
    pg_ds: DatasourceConfig, builds: list[Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = SlayerSQLClient(pg_ds)
    try:
        assert asyncio.run(client.execute(SQL)).rows == ROWS
        closed_pool = builds[0].sync_engine.pool
        for name in ("connect", "dispose"):
            monkeypatch.setattr(closed_pool, name, _refuse(f"closed loop's pool {name}()"))
        assert asyncio.run(client.execute(SQL)).rows == ROWS
        assert asyncio.run(client.execute(SQL)).rows == ROWS
    finally:
        client.close()
    assert len({id(engine) for engine in builds}) == 3
    # The abandoned connections warn in this test, not a later leak-gated one.
    monkeypatch.undo()
    del closed_pool
    gc.collect()


@_NO_LEAKS
def test_two_loops_on_two_threads_at_once(pg_ds: DatasourceConfig, builds: list[Any]) -> None:
    client = SlayerSQLClient(pg_ds)
    both_running = threading.Barrier(2)

    async def _serve() -> list[list[dict[str, Any]]]:
        await asyncio.to_thread(both_running.wait, 30)
        try:
            return [(await asyncio.wait_for(client.execute(SQL), _BOUND)).rows for _ in range(5)]
        finally:
            await asyncio.wait_for(client.aclose(), _BOUND)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: asyncio.run(_serve()), range(2)))
    finally:
        client.close()
    gc.collect()
    assert results == [[ROWS] * 5] * 2
    assert len(builds) == 2


# ---------------------------------------------------------------------------
# close() and live loops
# ---------------------------------------------------------------------------


async def test_close_warns_while_a_live_loops_engine_remains(
    pg_ds: DatasourceConfig, caplog: pytest.LogCaptureFixture,
) -> None:
    client = SlayerSQLClient(pg_ds)
    try:
        await client.execute(SQL)
        with caplog.at_level(logging.WARNING, logger="slayer.database.client"):
            client.close()
        assert any("aclose" in r.getMessage() for r in caplog.records)
        assert (await client.execute(SQL)).rows == ROWS
    finally:
        await client.aclose()
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="slayer.database.client"):
        client.close()
    assert not any("aclose" in r.getMessage() for r in caplog.records)
