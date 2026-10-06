"""Stored rank orders persisted as ``raw_formula`` get ``direction='desc'`` (spec: models/stored-upgrade)."""

from __future__ import annotations

import pytest

from slayer.core.models import DatasourceConfig
from slayer.memories.models import Memory
from slayer.sql import engine_factory
from slayer.storage import migrations as mig
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    TOP_STATUS_DESC,
    by_dimension,
    customers_v10,
    funcstyle_rank_order,
    memory_v2,
    orders_v10,
    query_backed_v10,
    raw_doc,
    raw_store,
    run,
    seed_shop,
)

STAGE_RANK = "rank(rev:sum)"
MEMORY_RANK = "rank(amount:sum)"
TOP_QUERY = {"source_model": "top_status", "dimensions": ["status"], "measures": ["sum(total)"]}


def _stages(*, query_version: int | None = None) -> list[dict]:
    stages = [
        {"name": "x", "source_model": "orders", "dimensions": [{"name": "status"}],
         "measures": [{"formula": "sum(amount)", "name": "rev"}]},
        {"source_model": "x", "dimensions": [{"name": "status"}],
         "measures": [{"formula": "rev:sum", "name": "total"}],
         "order": [funcstyle_rank_order(STAGE_RANK)], "limit": 1},
    ]
    if query_version is not None:
        for stage in stages:
            stage["version"] = query_version
    return stages


def _memory_query(*, version: int | None = 4) -> dict:
    q: dict = {"source_model": "orders", "dimensions": [{"name": "status"}],
               "measures": [{"formula": "sum(amount)", "name": "rev"}],
               "order": [funcstyle_rank_order(MEMORY_RANK)], "limit": 1}
    if version is not None:
        q["version"] = version
    return q


def _filled(raw: str) -> str:
    return raw[:-1] + ", direction='desc')"


@pytest.fixture
def shop_ds(tmp_path):
    db = str(tmp_path / "shop.duckdb")
    seed_shop(db, dialect="duckdb")
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


def test_current_versions():
    assert mig.CURRENT_VERSIONS["SlayerModel"] == 14
    assert mig.CURRENT_VERSIONS["SlayerQuery"] == 6
    assert mig.CURRENT_VERSIONS["Memory"] == 4
    for key in (("SlayerModel", 13), ("SlayerQuery", 5), ("Memory", 3)):
        assert key in mig._REGISTRY
        assert key in mig._STORED_ONLY


@pytest.mark.parametrize("backend", BACKENDS)
async def test_0_10_2_stage_ordering_by_rank_executes_descending(tmp_path, shop_ds, backend):
    storage = await raw_store(
        backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
        models=[orders_v10(), customers_v10(), query_backed_v10(name="top_status", stages=_stages())])
    model = await storage.get_model("top_status", data_source=DS)
    assert model is not None
    [order] = (model.source_queries or [])[-1].order or []
    assert order.raw_formula == _filled(STAGE_RANK)
    resp = await run(storage, TOP_QUERY)
    assert list(by_dimension(resp, dim_suffix="status")) == [TOP_STATUS_DESC]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_0_10_2_memory_ordering_by_rank_executes_descending(tmp_path, shop_ds, backend):
    storage = await raw_store(
        backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
        models=[orders_v10(), customers_v10()],
        memories=[memory_v2(memory_id="m1", query=_memory_query())])
    mem = await storage.get_memory("m1")
    assert mem.query is not None
    [order] = mem.query.order or []
    assert order.raw_formula == _filled(MEMORY_RANK)
    resp = await run(storage, mem.query)
    assert list(by_dimension(resp, dim_suffix="status")) == [TOP_STATUS_DESC]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_v13_model_with_unrewritten_raw_formula_is_repaired(tmp_path, shop_ds, backend):
    qb = query_backed_v10(name="top_status", stages=_stages(query_version=5), version=13)
    storage = await raw_store(
        backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
        models=[orders_v10(version=13), customers_v10(version=13), qb])
    model = await storage.get_model("top_status", data_source=DS)
    assert model is not None
    assert model.version == mig.CURRENT_VERSIONS["SlayerModel"]
    final = (model.source_queries or [])[-1]
    assert final.version == mig.CURRENT_VERSIONS["SlayerQuery"]
    [order] = final.order or []
    assert order.raw_formula == _filled(STAGE_RANK)
    doc = await raw_doc(storage, "top_status")
    assert doc["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
    resp = await run(storage, TOP_QUERY)
    assert list(by_dimension(resp, dim_suffix="status")) == [TOP_STATUS_DESC]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_v3_memory_with_unrewritten_raw_formula_is_repaired(tmp_path, shop_ds, backend):
    storage = await raw_store(
        backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
        models=[orders_v10(version=13), customers_v10(version=13)],
        memories=[memory_v2(memory_id="m3", version=3, query=_memory_query(version=5))])
    mem = await storage.get_memory("m3")
    assert mem.version == mig.CURRENT_VERSIONS["Memory"]
    assert mem.query is not None
    assert mem.query.version == mig.CURRENT_VERSIONS["SlayerQuery"]
    [order] = mem.query.order or []
    assert order.raw_formula == _filled(MEMORY_RANK)


def test_fresh_query_order_is_not_rewritten():
    mem = Memory.model_validate({"id": "f", "learning": "x", "query": _memory_query(version=None)})
    assert mem.query is not None
    [order] = mem.query.order or []
    assert order.raw_formula == MEMORY_RANK
