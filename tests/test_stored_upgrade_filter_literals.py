"""Legacy time literals in stored filters are repaired on load against temporal columns only (spec: models/stored-upgrade)."""

from __future__ import annotations

import pytest

from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    customers_v10,
    measure_total,
    memory_v2,
    orders_v10,
    query_backed_v10,
    raw_store,
    rev_query,
    run,
    seed_shop,
)

REPAIRED_CASES = [
    pytest.param("ordered_at >= '2024-01-01T00:00:00Z'", 200.0, id="zulu"),
    pytest.param("ordered_at >= '2024-01-01T02:00:00+02:00'", 150.0, id="plus-two"),
    pytest.param("'2024/01/01' <= ordered_at", 200.0, id="reversed-slashed"),
    pytest.param("ordered_at >= '2024-01-01T00:00:00Z' and ordered_at <= '2024-06-30T00:00:00+02:00'",
                 90.0, id="two-bound-range"),
    pytest.param("ordered_at IN ('2024/01/01', '2024/02/01')", 20.0, id="all-literal-in"),
    pytest.param("customers.signed_up_at >= '2024-01-01T00:00:00Z'", 180.0, id="joined-timestamp"),
    pytest.param("ordered_at >= '2024-01-01T00:00:00Z' and like(status, 'n%')", 70.0, id="mixed-with-like"),
]
LEGACY_FRAGMENTS = ("Z'", "+02:00", "/")

VERBATIM_CASES = [
    pytest.param("status = '2024/01/01'", id="text-column"),
    pytest.param("ordered_at IN ('2024/01/01', status)", id="mixed-in"),
    pytest.param("missing_col >= '2024-01-01T00:00:00Z'", id="unresolvable"),
]


@pytest.fixture
def shop_ds(tmp_path):
    db = str(tmp_path / "shop.duckdb")
    seed_shop(db, dialect="duckdb")
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def _store(*, tmp_path, ds: DatasourceConfig, backend: str, filters: list[str]):
    qb = query_backed_v10(name="rev_f", stages=[rev_query(filters=filters)])
    return await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds,
                           models=[orders_v10(), customers_v10(), qb])


async def _loaded_filters(storage) -> list[str]:
    model = await storage.get_model("rev_f", data_source=DS)
    assert model is not None
    return list((model.source_queries or [])[0].filters or [])


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("stored_filter", "expected"), REPAIRED_CASES)
async def test_legacy_literal_against_temporal_column_runs(tmp_path, shop_ds, backend, stored_filter, expected):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend=backend, filters=[stored_filter])
    [loaded] = await _loaded_filters(storage)
    assert not [frag for frag in LEGACY_FRAGMENTS if frag in loaded], loaded
    resp = await run(storage, {"source_model": "rev_f", "measures": ["sum(rev)"]})
    assert measure_total(resp, measure="rev_sum") == expected


async def test_repair_keeps_the_rest_of_the_filter_verbatim(tmp_path, shop_ds):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml",
                           filters=["ordered_at >= '2024-01-01T00:00:00Z' and like(status, 'n%')"])
    assert await _loaded_filters(storage) == ["ordered_at >= '2024-01-01T00:00:00' and like(status, 'n%')"]


@pytest.mark.parametrize("stored_filter", VERBATIM_CASES)
async def test_non_temporal_or_unresolvable_comparison_stays_verbatim(tmp_path, shop_ds, stored_filter):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml", filters=[stored_filter])
    assert await _loaded_filters(storage) == [stored_filter]


async def test_text_column_comparison_keeps_its_rows(tmp_path, shop_ds):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml", filters=["status = '2024/01/01'"])
    resp = await run(storage, {"source_model": "rev_f", "measures": ["sum(rev)"]})
    assert measure_total(resp, measure="rev_sum") == 60.0


@pytest.mark.parametrize("backend", BACKENDS)
async def test_memory_query_filter_literal_is_repaired(tmp_path, shop_ds, backend):
    query = rev_query(version=4, filters=["ordered_at >= '2024-01-01T02:00:00+02:00'"])
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                              models=[orders_v10(), customers_v10()],
                              memories=[memory_v2(memory_id="m_filter", query=query)])
    mem = await storage.get_memory("m_filter")
    assert mem.query is not None
    [loaded] = mem.query.filters or []
    assert "+02:00" not in loaded
    resp = await run(storage, mem.query)
    assert measure_total(resp, measure="rev") == 150.0
