"""A stored bare name keeps its pre-2.0 positional reading when an alias of its stage shares it.

Spec: models/stored-upgrade › Stored queries are upgraded to qualified references.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.database import engine_factory
from slayer.engine.query_engine import SlayerResponse
from slayer.storage.base import StorageBackend
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    customers_v10,
    metric_total,
    orders_v10,
    query_backed_v10,
    raw_store,
    rev_query,
    run,
    seed_shop,
)

#: The versions just before qualified references.
PRE_MODEL_V, PRE_QUERY_V = 27, 16


@pytest.fixture
def shop_ds(tmp_path) -> Iterator[DatasourceConfig]:
    db = str(tmp_path / "shop.duckdb")
    seed_shop(db, dialect="duckdb")
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


def _orders_with_revenue() -> dict:
    return {**orders_v10(), "measures": [{"name": "revenue", "formula": "sum(amount)"}]}


def _stage(**body: Any) -> dict:
    return {"version": PRE_QUERY_V, **body}


def _saved(name: str, *stages: dict) -> dict:
    return query_backed_v10(name=name, stages=list(stages), version=PRE_MODEL_V)


async def _store(tmp_path, ds: DatasourceConfig, backend: str, *docs: dict, orders: dict | None = None
                 ) -> StorageBackend:
    return await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds,
                           models=[orders or orders_v10(), customers_v10(), *docs])


async def _stages(storage: StorageBackend, name: str) -> list[SlayerQuery]:
    dataset = await storage.get_dataset(name, data_source=DS)
    assert isinstance(dataset, SlayerDataset)
    out = [q for q in dataset.source_queries or [] if isinstance(q, SlayerQuery)]
    assert len(out) == len(dataset.source_queries or [])
    return out


def _texts(query: SlayerQuery) -> dict[str, list[Any]]:
    return {
        "metrics": [(m.metric, m.alias) for m in query.metrics or []],
        "filter": list(query.filter or []),
    }


def _table(resp: SlayerResponse, *keys: str) -> set[tuple]:
    def norm(v: Any) -> Any:
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
    return {tuple(norm(r[k]) for k in keys) for r in resp.data}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("threshold", "expected"), [(0, 210.0), (65, 150.0)])
async def test_a_metric_alias_equal_to_a_population_column_reads_the_column(
    tmp_path, shop_ds, backend: str, threshold: int, expected: float,
) -> None:
    rev_f = query_backed_v10(name="rev_f", stages=[rev_query(dimensions=[{"name": "status"}])])
    rev_g = query_backed_v10(name="rev_g", stages=[{
        "source_model": "rev_f", "measures": [{"formula": "sum(rev)", "name": "rev"}],
        "filters": [f"rev >= {threshold}"]}])
    storage = await _store(tmp_path, shop_ds, backend, rev_f, rev_g)
    [stage] = await _stages(storage, "rev_g")
    assert _texts(stage) == {"metrics": [("sum(rev_f.rev)", "rev")], "filter": [f"rev_f.rev >= {threshold}"]}
    resp = await run(storage, {"population": "rev_g", "metrics": ["sum(rev_g.rev)"]})
    assert metric_total(resp, metric="rev_sum") == expected


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_metric_reads_the_population_metric_a_sibling_alias_shares(tmp_path, shop_ds, backend: str) -> None:
    sq = _saved("sq", _stage(
        population="orders", dimensions=["status"],
        metrics=[{"metric": "count(*)", "alias": "revenue"}, {"metric": "revenue * 10", "alias": "r10"}],
        filter=["revenue > 1"], order=[{"field": "revenue", "direction": "desc"}]))
    storage = await _store(tmp_path, shop_ds, backend, sq, orders=_orders_with_revenue())
    [stage] = await _stages(storage, "sq")
    assert _texts(stage) == {
        "metrics": [("count(orders.*)", "revenue"), ("orders.revenue * 10", "r10")], "filter": ["revenue > 1"]}
    assert [o.field for o in stage.order or []] == ["revenue"]
    resp = await run(storage, {"population": "sq", "dimensions": ["sq.status", "sq.r10"]})
    assert _table(resp, "sq.status", "sq.r10") == {("ok", 800.0), ("new", 700.0)}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_stage_over_an_earlier_stage_reads_the_earlier_stages_column(tmp_path, shop_ds, backend: str) -> None:
    chain = _saved(
        "chain",
        _stage(name="s1", population="orders", dimensions=["order_id"],
               metrics=[{"metric": "sum(amount)", "alias": "rev"}]),
        _stage(population="s1", metrics=[{"metric": "sum(rev)", "alias": "rev"}], filter=["rev > 25"]),
    )
    storage = await _store(tmp_path, shop_ds, backend, chain)
    stages = await _stages(storage, "chain")
    assert _texts(stages[-1]) == {"metrics": [("sum(s1.rev)", "rev")], "filter": ["s1.rev > 25"]}
    resp = await run(storage, {"population": "chain", "metrics": ["sum(chain.rev)"]})
    assert metric_total(resp, metric="rev_sum") == 180.0


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_partition_key_naming_a_computed_dimension_stays_bare(tmp_path, shop_ds, backend: str) -> None:
    sq = _saved("sq", _stage(
        population="orders", dimensions=["status", {"field": "upper(status)", "alias": "su"}],
        metrics=[{"metric": "sum(amount, partition_by=su)", "alias": "p"}]))
    storage = await _store(tmp_path, shop_ds, backend, sq)
    [stage] = await _stages(storage, "sq")
    assert _texts(stage)["metrics"] == [("sum(orders.amount, partition_by=su)", "p")]
    resp = await run(storage, {"population": "sq", "dimensions": ["sq.status", "sq.su", "sq.p"]})
    assert _table(resp, "sq.status", "sq.su", "sq.p") == {
        ("ok", "OK", 80.0), ("new", "NEW", 70.0), ("2024/01/01", "2024/01/01", 60.0)}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_entry_keeps_a_sibling_alias_the_population_does_not_declare(tmp_path, shop_ds, backend: str) -> None:
    sq = _saved("sq", _stage(
        population="orders", dimensions=["status"],
        metrics=[{"metric": "count(*)", "alias": "n"}, {"metric": "n * 10", "alias": "n10"}]))
    storage = await _store(tmp_path, shop_ds, backend, sq)
    [stage] = await _stages(storage, "sq")
    assert _texts(stage)["metrics"] == [("count(orders.*)", "n"), ("n * 10", "n10")]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_main_time_dimension_reads_a_population_field_before_an_alias(tmp_path, shop_ds, backend: str) -> None:
    sq = _saved("sq", _stage(
        population="orders", time_dimensions=[{"field": "ordered_at", "granularity": "year", "alias": "ordered_at"}],
        metrics=[{"metric": "change(sum(amount))", "alias": "c"}], main_time_dimension="ordered_at"))
    storage = await _store(tmp_path, shop_ds, backend, sq)
    [stage] = await _stages(storage, "sq")
    assert stage.main_time_dimension == "orders.ordered_at"
    resp = await run(storage, {"population": "sq", "metrics": ["sum(sq.c)"]})
    assert metric_total(resp, metric="c_sum") == 40.0


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_quoted_alias_matches_only_its_exact_spelling(tmp_path, shop_ds, backend: str) -> None:
    sq = _saved("sq", _stage(
        population="orders", dimensions=["status"], metrics=[{"metric": "count(*)", "alias": "Amount"}],
        filter=['"Amount" > 1']))
    storage = await _store(tmp_path, shop_ds, backend, sq)
    [stage] = await _stages(storage, "sq")
    assert _texts(stage) == {"metrics": [("count(orders.*)", "Amount")], "filter": ['"Amount" > 1']}
    resp = await run(storage, {"population": "sq", "dimensions": ["sq.status"]})
    assert {r["sq.status"] for r in resp.data} == {"ok", "new"}
