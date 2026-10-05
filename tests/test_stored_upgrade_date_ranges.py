"""Stored legacy ``date_range`` values are repaired on load; client queries keep the strict check (spec: models/stored-upgrade)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.sql import engine_factory
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    TOTAL,
    customers_v10,
    measure_total,
    memory_v2,
    orders_v10,
    query_backed_v10,
    raw_store,
    rev_query,
    run,
    seed_shop,
    year_td,
)

#: Rows 2, 3, 4, 6 (2024, offsets stripped to wall-clock time).
IN_2024 = 150.0
#: Rows 4, 6: a ``+02:00`` lower bound keeps 02:00 wall-clock time, excluding 00:00 and 01:30.
FROM_0200 = 100.0

STORED_CASES = [
    pytest.param(["2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z"], IN_2024, id="zulu"),
    pytest.param(["2024-01-01T02:00:00+02:00", "2024-12-31T23:59:59+02:00"], FROM_0200, id="plus-two"),
    pytest.param(["2024-01-01T00:00:00+00:00", "2024-12-31T23:59:59-05:00"], IN_2024, id="zero-and-negative"),
    pytest.param(["2024/01/01", "2024/12/31"], IN_2024, id="slashed"),
    pytest.param(["2024/01/01 00:00:00", "2024/12/31 23:59"], IN_2024, id="slashed-with-time"),
    pytest.param([], TOTAL, id="empty"),
    pytest.param(["2024-01-01"], TOTAL, id="one-element"),
    pytest.param(["2024-01-01", "2024-06-30", "2024-12-31"], TOTAL, id="three-elements"),
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


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("date_range", "expected"), STORED_CASES)
async def test_query_backed_model_returns_the_0_10_2_rows(tmp_path, shop_ds, backend, date_range, expected):
    qb = query_backed_v10(name="rev_range", stages=[rev_query(time_dimensions=[year_td(date_range)])])
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                              models=[orders_v10(), customers_v10(), qb])
    resp = await run(storage, {"source_model": "rev_range", "measures": ["sum(rev)"]})
    assert measure_total(resp, measure="rev_sum") == expected


@pytest.mark.parametrize("date_range", [[], ["2024-01-01"], ["2024-01-01", "2024-06-30", "2024-12-31"]])
async def test_wrong_length_range_is_dropped(tmp_path, shop_ds, date_range):
    qb = query_backed_v10(name="rev_range", stages=[rev_query(time_dimensions=[year_td(date_range)])])
    storage = await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=shop_ds,
                              models=[orders_v10(), customers_v10(), qb])
    model = await storage.get_model("rev_range", data_source=DS)
    assert model is not None
    [td] = (model.source_queries or [])[0].time_dimensions or []
    assert td.date_range is None


@pytest.mark.parametrize("bound", ["2024-13-45", "soonish"])
async def test_unrepairable_bound_still_fails_to_load(tmp_path, shop_ds, bound):
    qb = query_backed_v10(name="rev_range", stages=[rev_query(time_dimensions=[year_td([bound, "2024-12-31"])])])
    storage = await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=shop_ds,
                              models=[orders_v10(), customers_v10(), qb])
    with pytest.raises(ValueError, match="date_range"):
        await storage.get_model("rev_range", data_source=DS)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_memory_query_with_offset_bound_runs(tmp_path, shop_ds, backend):
    query = rev_query(version=4, time_dimensions=[year_td(["2024-01-01T02:00:00+02:00", "2024-12-31T23:59:59+02:00"])])
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                              models=[orders_v10(), customers_v10()],
                              memories=[memory_v2(memory_id="m_offset", query=query)])
    mem = await storage.get_memory("m_offset")
    assert mem.query is not None
    resp = await run(storage, mem.query)
    assert measure_total(resp, measure="rev") == FROM_0200


@pytest.mark.parametrize("date_range", [[], ["2024-01-01T00:00:00Z", "2024-12-31"], ["2024/01/01", "2024/12/31"]])
def test_client_query_keeps_the_strict_check(date_range):
    raw = rev_query(time_dimensions=[year_td(date_range)])
    with pytest.raises(ValidationError, match="date_range"):
        SlayerQuery.model_validate(raw)
