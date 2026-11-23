"""An unpinned stored memory whose population names no stored dataset reads one datasource (models/stored-upgrade)."""

from __future__ import annotations

import warnings
from collections.abc import Callable, Generator, Sequence
from pathlib import Path
from typing import Any

import pytest

from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.database import engine_factory
from slayer.engine.query_engine import SlayerResponse
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures
from tests._engine_helpers import owned_query_engine
from tests._granularity_rename_fixtures import (
    DS as GDS,
    EXPECTED_BUCKETS,
    SUM,
    datasource_doc,
    events_doc,
    granularity_definition,
    live_db,
    memory_doc,
    raw_documents,
    write_raw_store,
)
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    by_dimension,
    customers_v10,
    in_datasource,
    memory_v2,
    metric_total,
    orders_v10,
    raw_store,
    run,
    seed_shop,
)

DS2 = "shop2"
REV = {"formula": "sum(amount)", "name": "rev"}
#: ``sum(amount)`` per status over every order, and over North customers' orders.
REV_BY_STATUS = {"ok": 80.0, "new": 70.0, "2024/01/01": 60.0}
UNDETERMINED = "datasource cannot be determined"

DatasourceFactory = Callable[[str], DatasourceConfig]


@pytest.fixture
def shop_datasource(tmp_path: Path) -> Generator[DatasourceFactory]:
    """A factory of DuckDB datasources, each over its own seeded shop database."""
    made: list[DatasourceConfig] = []

    def make(name: str) -> DatasourceConfig:
        db = str(tmp_path / f"{name}.duckdb")
        seed_shop(db, dialect="duckdb")
        ds = DatasourceConfig(name=name, type="duckdb", database=db)
        made.append(ds)
        return ds

    try:
        yield make
    finally:
        for ds in made:
            engine_factory.invalidate_engine(ds)


def rootless(*filters: str, **extra: Any) -> dict:
    """The stored v4 root-less query: ``status`` and ``sum(amount)`` as ``rev``."""
    return {"version": 4, "dimensions": [{"name": "status"}], "measures": [REV], "filters": list(filters), **extra}


def rootless_memory(*filters: str, entities: Sequence[str] = ()) -> dict:
    return memory_v2(memory_id="m", query=rootless(*filters), entities=entities)


def shop(data_source: str) -> list[dict]:
    """``orders`` and ``customers`` stored in ``data_source``."""
    return [in_datasource(orders_v10(), data_source), in_datasource(customers_v10(), data_source)]


def swapped(data_source: str) -> list[dict]:
    """``customers`` reading the orders table (``status``, ``amount``) and ``orders`` reading the customers table."""
    customers = {**orders_v10(joins=[{"target_model": "orders", "join_pairs": [["customer_id", "id"]],
                                      "cardinality": "many_to_one"}]), "name": "customers"}
    return [in_datasource(customers, data_source), in_datasource(customers_v10(name="orders"), data_source)]


def typed(doc: dict, *, column: str, type_: str) -> dict:
    return {**doc, "columns": [{**c, "type": type_} if c["name"] == column else c for c in doc["columns"]]}


async def store(*, tmp_path: Path, backend: str, datasources: Sequence[DatasourceConfig], models: Sequence[dict],
                memories: Sequence[dict], priority: Sequence[str] = ()) -> StorageBackend:
    first, *rest = datasources
    storage = await raw_store(backend=backend, base=str(tmp_path / f"store-{backend}"), datasource=first,
                              extra_datasources=rest, models=models, memories=memories)
    if priority:
        await storage.set_datasource_priority(list(priority))
    return storage


async def loaded(storage: StorageBackend, memory_id: str = "m") -> SlayerQuery:
    mem = await storage.get_memory(memory_id)
    assert mem.query is not None
    return mem.query


async def run_in(storage: StorageBackend, query: SlayerQuery, *, data_source: str) -> SlayerResponse:
    async with owned_query_engine(storage=storage) as engine:
        return await engine.execute(query, data_source=data_source)


def fields(query: SlayerQuery) -> tuple[list[str], list[str], list[str]]:
    return ([d.field for d in query.dimensions or []], [m.metric for m in query.metrics or []],
            list(query.filter or []))


@pytest.mark.parametrize("backend", BACKENDS)
async def test_rootless_memory_loads_on_a_single_datasource_store(
    tmp_path: Path, shop_datasource: DatasourceFactory, backend: str,
) -> None:
    storage = await store(tmp_path=tmp_path, backend=backend, datasources=[shop_datasource(DS)], models=shop(DS),
                          memories=[rootless_memory("status = 'ok'")])
    base = str(tmp_path / f"store-{backend}")
    before = raw_documents(backend=backend, base=base)["memories/m"]
    query = await loaded(storage)
    assert fields(query) == (["orders.status"], ["sum(orders.amount)"], ["orders.status = 'ok'"])
    assert by_dimension(await run(storage, query), dim_suffix="status") == {"ok": 80.0}
    assert raw_documents(backend=backend, base=base)["memories/m"] == before


@pytest.mark.parametrize("backend", BACKENDS)
async def test_entities_pin_a_rootless_memory_over_the_priority(
    tmp_path: Path, shop_datasource: DatasourceFactory, backend: str,
) -> None:
    entities = [f"{DS2}.customers", f"{DS2}.customers.status", f"{DS2}.customers.amount"]
    storage = await store(tmp_path=tmp_path, backend=backend, datasources=[shop_datasource(DS), shop_datasource(DS2)],
                          models=[*shop(DS), *swapped(DS2)], memories=[rootless_memory(entities=entities)],
                          priority=[DS, DS2])
    query = await loaded(storage)
    assert fields(query)[:2] == (["customers.status"], ["sum(customers.amount)"])
    resp = await run_in(storage, query, data_source=DS2)
    assert by_dimension(resp, dim_suffix="status") == REV_BY_STATUS


@pytest.mark.parametrize("backend", BACKENDS)
async def test_rootless_memory_legacy_literal_is_repaired(
    tmp_path: Path, shop_datasource: DatasourceFactory, backend: str,
) -> None:
    storage = await store(tmp_path=tmp_path, backend=backend, datasources=[shop_datasource(DS)], models=shop(DS),
                          memories=[rootless_memory("ordered_at >= '2024/01/01'")])
    base = str(tmp_path / f"store-{backend}")
    before = raw_documents(backend=backend, base=base)["memories/m"]
    query = await loaded(storage)
    assert query.filter == ["orders.ordered_at >= '2024-01-01'"]
    assert metric_total(await run(storage, query), metric="rev") == 200.0
    assert raw_documents(backend=backend, base=base)["memories/m"] == before


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("pinned", "literal"), [
    pytest.param(DS, "'2024-01-01'", id="timestamp-datasource"),
    pytest.param(DS2, "'2024/01/01'", id="text-datasource"),
])
async def test_rootless_memory_literal_is_typed_in_the_datasource_it_is_read_in(
    tmp_path: Path, shop_datasource: DatasourceFactory, backend: str, pinned: str, literal: str,
) -> None:
    text_orders = in_datasource(typed(orders_v10(), column="ordered_at", type_="TEXT"), DS2)
    memory = rootless_memory("ordered_at >= '2024/01/01'", entities=[f"{pinned}.orders"])
    storage = await store(tmp_path=tmp_path, backend=backend, datasources=[shop_datasource(DS), shop_datasource(DS2)],
                          models=[*shop(DS), text_orders, in_datasource(customers_v10(), DS2)], memories=[memory])
    assert (await loaded(storage)).filter == [f"orders.ordered_at >= {literal}"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_entity_pinned_memory_is_read_only_in_its_datasource(
    tmp_path: Path, shop_datasource: DatasourceFactory, backend: str,
) -> None:
    # Guard: already holds on HEAD.
    models = [in_datasource(customers_v10(), DS), *shop(DS2)]
    storage = await store(tmp_path=tmp_path, backend=backend, datasources=[shop_datasource(DS), shop_datasource(DS2)],
                          models=models, memories=[rootless_memory(entities=[f"{DS}.customers"])])
    await assert_fails_to_load(storage, match="status")


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("entities", [
    pytest.param([], id="no-entities"),
    pytest.param([f"{DS}.gone", "memory:1"], id="no-stored-dataset"),
    pytest.param([f"{DS}.orders", f"{DS2}.orders.status"], id="both-datasources"),
])
async def test_entityless_memory_on_a_multi_datasource_store_fails_to_load(
    tmp_path: Path, shop_datasource: DatasourceFactory, backend: str, entities: list[str],
) -> None:
    storage = await store(tmp_path=tmp_path, backend=backend, datasources=[shop_datasource(DS), shop_datasource(DS2)],
                          models=[*shop(DS), *shop(DS2)], memories=[rootless_memory(entities=entities)])
    await assert_fails_to_load(storage, match=UNDETERMINED)


async def assert_fails_to_load(storage: StorageBackend, *, match: str) -> None:
    """Memory ``m`` is skipped by a listing with one warning naming it, and refused by ``get_memory``."""
    failures = DocumentLoadFailures()
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        assert await storage.list_memories(failures=failures) == []
    [warning] = failures.warnings
    assert warning.name == "m"
    assert match in warning.cause
    with pytest.raises(StoredDocumentLoadError, match=match):
        await storage.get_memory("m")


# --------------------------------------------------------------------------- #
# Custom granularities (a v1.4.1 store whose datasource defines them).
# --------------------------------------------------------------------------- #
MID_MONTH_FILTER = "date_trunc('mid_month', events.created_at) >= '2025-02-01'"
SPINE_RANGE = ["2025-01-15", "2025-03-14"]


async def granularity_store(*, tmp_path: Path, backend: str, granularity: str, query: dict) -> StorageBackend:
    base = str(tmp_path / f"gstore-{backend}")
    db_path = live_db(base)
    return await write_raw_store(
        backend=backend, base=base,
        datasources=[{**datasource_doc(db_path=db_path), "granularities": [granularity_definition(granularity)]}],
        datasets=[events_doc()], memories=[memory_doc(memory_id="m", query=query)],
    )


def rootless_bucket(granularity: str, *filters: str) -> dict:
    return {"version": 6, "time_dimensions": [{"dimension": "created_at", "granularity": granularity}],
            "measures": [SUM], "filters": list(filters)}


def buckets(resp: SlayerResponse) -> dict[str, float]:
    row = resp.data[0]
    time_key = next(k for k in row if k.endswith("created_at"))
    value_key = next(k for k in row if k != time_key)
    return {str(r[time_key])[:10]: float(r[value_key]) for r in resp.data}


def granularity_of(query: SlayerQuery) -> str:
    [td] = query.time_dimensions or []
    return str(td.granularity)


def rows(resp: SlayerResponse) -> list[tuple]:
    return [tuple(r.values()) for r in resp.data]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_rootless_memory_custom_granularity_bucket_is_rewritten(tmp_path: Path, backend: str) -> None:
    storage = await granularity_store(tmp_path=tmp_path, backend=backend, granularity="mid_month",
                                      query=rootless_bucket("mid_month", "created_at:mid_month >= '2025-02-01'"))
    base = str(tmp_path / f"gstore-{backend}")
    before = raw_documents(backend=backend, base=base)["memories/m"]
    query = await loaded(storage)
    assert query.filter == [MID_MONTH_FILTER]
    assert buckets(await run(storage, query)) == {"2025-02-15": EXPECTED_BUCKETS["2025-02-15"]}
    assert raw_documents(backend=backend, base=base)["memories/m"] == before


@pytest.mark.parametrize("backend", BACKENDS)
async def test_rootless_memory_library_colliding_granularity_is_renamed(tmp_path: Path, backend: str) -> None:
    storage = await granularity_store(tmp_path=tmp_path, backend=backend, granularity="left",
                                      query=rootless_bucket("left"))
    query = await loaded(storage)
    assert granularity_of(query) == "left_granularity"
    assert buckets(await run(storage, query)) == EXPECTED_BUCKETS


@pytest.mark.parametrize("backend", BACKENDS)
async def test_entity_pinned_memory_custom_granularities_follow_its_datasource(tmp_path: Path, backend: str) -> None:
    base = str(tmp_path / f"gstore-{backend}")
    db_path = live_db(base)
    query = rootless_bucket("left", "created_at:mid_month >= '2025-02-01'")
    storage = await write_raw_store(
        backend=backend, base=base,
        datasources=[datasource_doc(db_path=db_path, name="g1"),
                     datasource_doc(db_path=db_path, name="g2", granularities=["left", "mid_month"])],
        datasets=[events_doc(data_source="g1"), events_doc(data_source="g2")],
        memories=[memory_doc(memory_id="m", query=query, entities=["g2.events"])],
    )
    loaded_query = await loaded(storage)
    assert granularity_of(loaded_query) == "left_granularity"
    assert loaded_query.filter == [MID_MONTH_FILTER]
    resp = await run_in(storage, loaded_query, data_source="g2")
    assert buckets(resp) == {"2025-02-15": EXPECTED_BUCKETS["2025-02-15"]}


def spine_bucket(granularity: str, *filters: str) -> dict:
    return {"version": 6, "source_model": "time_spine", "measures": [{"formula": "sum(events.amount)", "name": "s"}],
            "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": granularity,
                                 "date_range": SPINE_RANGE}], "filters": list(filters)}


def fresh_spine(granularity: str, *filters: str) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "population": "time_spine", "metrics": [{"metric": "sum(events.amount)", "alias": "s"}],
        "time_dimensions": [{"field": "time_spine.timestamp", "granularity": granularity, "date_range": SPINE_RANGE}],
        "filter": list(filters)})


@pytest.mark.parametrize("backend", BACKENDS)
async def test_spine_memory_library_colliding_granularity_is_renamed(tmp_path: Path, backend: str) -> None:
    storage = await granularity_store(tmp_path=tmp_path, backend=backend, granularity="left",
                                      query=spine_bucket("left"))
    query = await loaded(storage)
    assert granularity_of(query) == "left_granularity"
    assert rows(await run(storage, query)) == rows(await run(storage, fresh_spine("left_granularity")))


@pytest.mark.parametrize("backend", BACKENDS)
async def test_spine_memory_custom_granularity_bucket_is_rewritten(tmp_path: Path, backend: str) -> None:
    storage = await granularity_store(tmp_path=tmp_path, backend=backend, granularity="mid_month",
                                      query=spine_bucket("month", "events.created_at:mid_month >= '2025-02-01'"))
    query = await loaded(storage)
    assert query.filter == [MID_MONTH_FILTER]
    assert rows(await run(storage, query)) == rows(await run(storage, fresh_spine("month", MID_MONTH_FILTER)))


@pytest.mark.parametrize("backend", BACKENDS)
async def test_missing_explicit_population_gets_no_fallback(tmp_path: Path, backend: str) -> None:
    # Guard: already holds on HEAD.
    query = {**rootless_bucket("left"), "source_model": "ordrs"}
    storage = await granularity_store(tmp_path=tmp_path, backend=backend, granularity="left", query=query)
    with pytest.raises(StoredDocumentLoadError, match="ordrs"):
        await storage.get_memory("m")
    stored = raw_documents(backend=backend, base=str(tmp_path / f"gstore-{backend}"))
    assert "left_granularity" not in stored["memories/m"]
    assert "left_granularity" in stored[f"datasources/{GDS}"]
