"""A search ``cypher_filter`` runs off the event loop under a fixed time limit."""

from __future__ import annotations

import asyncio
import tempfile
import threading
import time
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.search import graph
from slayer.search.graph import clear_cache, get_filtered_ids, is_available
from slayer.storage.yaml_storage import YAMLStorage

pytestmark = pytest.mark.skipif(not is_available(), reason="ladybug not installed")

# Walks every JOINS path of up to 30 hops: several seconds over the cycles below.
RUNAWAY = "MATCH (a)-[:JOINS*]->(b) RETURN DISTINCT b.id AS id"
MODELS = "MATCH (m:Model) RETURN m.id AS id"
DATASOURCES = "MATCH (d:Datasource) RETURN d.id AS id"

#: m0 ⇄ m1 ⇄ m2 plus m2 → m0: dense enough that RUNAWAY takes seconds untimed.
_EDGES = {0: [1], 1: [2, 0], 2: [0, 1]}


def _model(i: int) -> SlayerModel:
    targets = _EDGES[i]
    return SlayerModel(
        name=f"m{i}", sql_table=f"m{i}", data_source="shop",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 *(Column(name=f"f{j}", type=DataType.INT) for j in range(len(targets)))],
        joins=[ModelJoin(target_model=f"m{t}", join_pairs=[[f"f{j}", "id"]], name=f"e{i}_{j}")
               for j, t in enumerate(targets)],
    )


@pytest_asyncio.fixture
async def cyclic_storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(name="shop", type="sqlite", database=":memory:"))
        for i in _EDGES:
            await storage.save_model(_model(i), _validate=False)
        clear_cache()
        yield storage
        clear_cache()


def _limit(monkeypatch: pytest.MonkeyPatch, ms: int) -> None:
    monkeypatch.setattr(graph, "CYPHER_FILTER_TIMEOUT_MS", ms)
    clear_cache()


async def test_runaway_traversal_times_out(cyclic_storage, monkeypatch) -> None:
    _limit(monkeypatch, 300)
    await get_filtered_ids(MODELS, cyclic_storage)  # warm the cache
    start = time.monotonic()
    with pytest.raises(ValueError) as ei:
        await get_filtered_ids(RUNAWAY, cyclic_storage)
    assert time.monotonic() - start < 5.0
    msg = str(ei.value)
    assert "cypher_filter" in msg
    assert "timed out" in msg.lower()
    assert "300" in msg


class _BlockingConn:
    """Delegates to the real connection; ``DATASOURCES`` blocks in ``execute`` until released."""

    def __init__(self, conn) -> None:
        self._conn = conn
        self.entered = threading.Event()
        self.release = threading.Event()
        self.timed_out = False

    def execute(self, cypher: str, *args, **kwargs):
        if cypher == DATASOURCES:
            self.entered.set()
            self.timed_out = not self.release.wait(10)
        return self._conn.execute(cypher, *args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self._conn, name)


@pytest.fixture
def blocking_conns(monkeypatch) -> list[_BlockingConn]:
    built: list[_BlockingConn] = []
    real_build = graph.build_graph

    async def build(*args, **kwargs):
        db, conn = await real_build(*args, **kwargs)
        built.append(_BlockingConn(conn))
        return db, built[-1]

    monkeypatch.setattr(graph, "build_graph", build)
    clear_cache()
    return built


async def test_other_requests_proceed_during_a_slow_filter(cyclic_storage, blocking_conns) -> None:
    await get_filtered_ids(MODELS, cyclic_storage)
    conn = blocking_conns[-1]
    slow = asyncio.create_task(get_filtered_ids(DATASOURCES, cyclic_storage))
    try:
        assert await asyncio.to_thread(conn.entered.wait, 5)
        assert await asyncio.wait_for(asyncio.sleep(0.01, result="served"), timeout=2) == "served"
        assert not slow.done()
    finally:
        conn.release.set()
    assert await slow == frozenset({"shop"})
    assert not conn.timed_out


async def test_concurrent_filters_return_their_own_ids(cyclic_storage) -> None:
    clear_cache()
    queries = [MODELS, DATASOURCES] * 10
    results = await asyncio.gather(*(get_filtered_ids(q, cyclic_storage) for q in queries))
    for query, ids in zip(queries, results):
        expected = {"shop.m0", "shop.m1", "shop.m2"} if query == MODELS else {"shop"}
        assert ids == frozenset(expected)


async def test_rebuild_waits_for_an_in_flight_filter(cyclic_storage, blocking_conns) -> None:
    await get_filtered_ids(MODELS, cyclic_storage)
    conn = blocking_conns[-1]
    slow = asyncio.create_task(get_filtered_ids(DATASOURCES, cyclic_storage))
    try:
        assert await asyncio.to_thread(conn.entered.wait, 5)
        assert not slow.done()
        await cyclic_storage.save_memory(learning="Changes the graph fingerprint.", entities=[])
        rebuild = asyncio.create_task(get_filtered_ids(MODELS, cyclic_storage))
        await asyncio.sleep(0.3)
        assert not rebuild.done()
    finally:
        conn.release.set()
    assert await slow == frozenset({"shop"})
    assert await rebuild == frozenset({"shop.m0", "shop.m1", "shop.m2"})
    assert len(blocking_conns) == 2
    assert not conn.timed_out
