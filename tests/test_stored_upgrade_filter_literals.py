"""Legacy time literals in stored filters are repaired on load against temporal columns only (spec: models/stored-upgrade)."""

from __future__ import annotations

import copy

import pytest

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from tests._stored_upgrade_fixtures import (
    AMOUNT_OPENED_SINCE_2024,
    BACKENDS,
    CUSTOMERS_TO_ORDERS,
    DS,
    customers_v10,
    in_datasource,
    measure_total,
    memory_v2,
    orders_v10,
    query_backed_v10,
    raw_store,
    region_join,
    regions_v10,
    rev_query,
    run,
    seed_regions,
    seed_shop,
    stored_bytes,
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


@pytest.mark.parametrize(("stored_filter", "repaired"), [
    ("ordered_at = '2024/01/01'", "ordered_at = '2024-01-01'"),
    ("ordered_at == '2024/01/01'", "ordered_at = '2024-01-01'"),
    ("ordered_at != '2024/01/01'", "ordered_at != '2024-01-01'"),
    ("ordered_at <> '2024/01/01'", "ordered_at <> '2024-01-01'"),
    ("ordered_at < '2024/01/01'", "ordered_at < '2024-01-01'"),
    ("ordered_at <= '2024/01/01'", "ordered_at <= '2024-01-01'"),
    ("ordered_at > '2024/01/01'", "ordered_at > '2024-01-01'"),
    ("'2024/01/01' > ordered_at", "'2024-01-01' > ordered_at"),
    ("(ordered_at >= '2024/01/01') and (status = 'ok')", "(ordered_at >= '2024-01-01') and (status = 'ok')"),
    ("ordered_at NOT IN ('2024/01/01', '2024/02/01')", "ordered_at NOT IN ('2024-01-01', '2024-02-01')"),
    ("customers.signed_up_at IN ('2024/01/01')", "customers.signed_up_at IN ('2024-01-01')"),
])
async def test_every_operator_layout_is_repaired_exactly(tmp_path, shop_ds, stored_filter, repaired):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml", filters=[stored_filter])
    assert await _loaded_filters(storage) == [repaired]


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


# --------------------------------------------------------------------------- #
# The compared column resolves as the binder resolves it.
# --------------------------------------------------------------------------- #
DS2 = "shop2"


def _regions_db(tmp_path, name: str) -> str:
    db = str(tmp_path / f"{name}.duckdb")
    seed_shop(db, dialect="duckdb")
    seed_regions(db)
    return db


@pytest.fixture
def regions_ds(tmp_path):
    ds = DatasourceConfig(name=DS, type="duckdb", database=_regions_db(tmp_path, "shop"))
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


def _routed_models() -> list[dict]:
    """orders → customers → regions, each join declared forward."""
    return [orders_v10(), customers_v10(joins=[region_join()]), regions_v10()]


def _reverse_models() -> list[dict]:
    """The only ``orders ↔ customers`` join is declared on ``customers``."""
    return [orders_v10(joins=[]), customers_v10(joins=[CUSTOMERS_TO_ORDERS]), regions_v10()]


EXTENDED_REGIONS = {
    "source_name": "regions",
    "columns": [{"name": "founded_on", "sql": "opened_on", "type": "DATE"}],
}

ROUTED_REPAIRS = [
    pytest.param(_routed_models, rev_query(filters=["regions.opened_on >= '2024/01/01'"]),
                 "regions.opened_on >= '2024-01-01'", AMOUNT_OPENED_SINCE_2024, id="short-form-two-hops"),
    pytest.param(_reverse_models, rev_query(filters=["customers.signed_up_at >= '2024-01-01T00:00:00Z'"]),
                 "customers.signed_up_at >= '2024-01-01T00:00:00'", 180.0, id="reverse-edge"),
    pytest.param(_routed_models,
                 {"version": 12, **rev_query(filters=["regions.founded_on >= '2024/01/01'"]),
                  "extensions": [EXTENDED_REGIONS]},
                 "regions.founded_on >= '2024-01-01'", AMOUNT_OPENED_SINCE_2024, id="extension-on-routed-model"),
    pytest.param(_routed_models, rev_query(filters=["'2024/01/01' <= regions.opened_on"]),
                 "'2024-01-01' <= regions.opened_on", AMOUNT_OPENED_SINCE_2024, id="short-form-reversed-operands"),
    pytest.param(_routed_models, rev_query(filters=["orders.regions.opened_on >= '2024/01/01'"]),
                 "orders.regions.opened_on >= '2024-01-01'", AMOUNT_OPENED_SINCE_2024, id="own-qualifier-then-route"),
]


async def _routed_store(*, tmp_path, ds: DatasourceConfig, backend: str, models: list[dict], stage: dict,
                        **extra) -> StorageBackend:
    qb = query_backed_v10(name="rev_f", stages=[stage])
    return await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds, models=[*models, qb], **extra)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("models", "stage", "repaired", "expected"), ROUTED_REPAIRS)
async def test_routed_reference_literal_is_repaired_and_runs(
    tmp_path, regions_ds, backend, models, stage, repaired, expected,
):
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend=backend, models=models(), stage=stage)
    assert await _loaded_filters(storage) == [repaired]
    resp = await run(storage, {"source_model": "rev_f", "measures": ["sum(rev)"]})
    assert measure_total(resp, measure="rev_sum") == expected


async def test_routed_text_column_stays_verbatim_and_keeps_its_rows(tmp_path, regions_ds):
    stage = rev_query(filters=["regions.label = '2024/01/01'"])
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend="yaml", models=_routed_models(),
                                  stage=stage)
    assert await _loaded_filters(storage) == ["regions.label = '2024/01/01'"]
    resp = await run(storage, {"source_model": "rev_f", "measures": ["sum(rev)"]})
    assert measure_total(resp, measure="rev_sum") == 120.0


async def test_ambiguous_short_form_route_and_routed_text_column_stay_verbatim(tmp_path, regions_ds):
    """``regions`` is reached through ``customers`` and through ``buyers``, both fan-out-free."""
    orders = orders_v10(joins=[
        {"target_model": "customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"},
        {"target_model": "buyers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"},
    ])
    models = [orders, customers_v10(joins=[region_join()]), customers_v10(name="buyers", joins=[region_join()]),
              regions_v10()]
    filters = ["regions.opened_on >= '2024/01/01'", "customers.regions.label = '2024/01/01'"]
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend="yaml", models=models,
                                  stage=rev_query(filters=filters))
    assert await _loaded_filters(storage) == filters


@pytest.mark.parametrize("backend", BACKENDS)
async def test_unloadable_model_does_not_block_an_unrelated_repair(tmp_path, regions_ds, backend):
    broken = {"version": 10, "name": "broken", "sql_table": "broken", "data_source": DS,
              "columns": [{"type": "TEXT"}]}
    stage = rev_query(filters=["regions.opened_on >= '2024/01/01'"])
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend=backend,
                                  models=[*_routed_models(), broken], stage=stage)
    assert await _loaded_filters(storage) == ["regions.opened_on >= '2024-01-01'"]
    resp = await run(storage, {"source_model": "rev_f", "measures": ["sum(rev)"]})
    assert measure_total(resp, measure="rev_sum") == AMOUNT_OPENED_SINCE_2024


def _case_sensitive_models() -> list[dict]:
    return [orders_v10(), customers_v10(joins=[region_join("Regions")]),
            regions_v10(name="Regions", date_column="Opened_On")]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_double_quoted_hop_and_leaf_are_repaired(tmp_path, regions_ds, backend):
    stage = rev_query(filters=['"Regions"."Opened_On" >= \'2024/01/01\''])
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend=backend,
                                  models=_case_sensitive_models(), stage=stage)
    assert await _loaded_filters(storage) == ['"Regions"."Opened_On" >= \'2024-01-01\'']
    resp = await run(storage, {"source_model": "rev_f", "measures": ["sum(rev)"]})
    assert measure_total(resp, measure="rev_sum") == AMOUNT_OPENED_SINCE_2024


@pytest.mark.parametrize("stored", [
    pytest.param('"REGIONS"."Opened_On" >= \'2024/01/01\'', id="hop"),
    pytest.param('"Regions"."OPENED_ON" >= \'2024/01/01\'', id="leaf"),
])
async def test_double_quoted_segment_in_the_wrong_case_stays_verbatim(tmp_path, regions_ds, stored):
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend="yaml",
                                  models=_case_sensitive_models(), stage=rev_query(filters=[stored]))
    assert await _loaded_filters(storage) == [stored]


async def test_sibling_stage_host_bare_reference_stays_verbatim_and_extension_hop_is_repaired(tmp_path, regions_ds):
    base = {"version": 12, "name": "base", "source_model": "orders",
            "dimensions": [{"name": "customer_id"}, {"name": "ordered_at"}],
            "measures": [{"formula": "sum(amount)", "name": "rev"}]}
    outer = {
        "version": 12, "source_model": "base", "measures": [{"formula": "sum(rev)", "name": "rev"}],
        "filters": ["ordered_at >= '2024/01/01'", "customers.signed_up_at >= '2024-01-01T00:00:00Z'"],
        "extensions": [{"source_name": "base", "joins": [
            {"target_model": "customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"}]}],
    }
    qb = query_backed_v10(name="rev_s", stages=[base, outer])
    storage = await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=regions_ds,
                              models=[*_routed_models(), qb])
    model = await storage.get_model("rev_s", data_source=DS)
    assert model is not None
    assert (model.source_queries or [])[-1].filters == [
        "ordered_at >= '2024/01/01'", "customers.signed_up_at >= '2024-01-01T00:00:00'"]


# --------------------------------------------------------------------------- #
# Memories.
# --------------------------------------------------------------------------- #
ROUTED_MEMORY_FILTER = "regions.opened_on >= '2024/01/01'"


def _routed_memory() -> dict:
    return memory_v2(memory_id="m_routed", query=rev_query(version=4, filters=[ROUTED_MEMORY_FILTER]))


@pytest.mark.parametrize("backend", BACKENDS)
async def test_memory_short_form_routed_literal_is_repaired(tmp_path, regions_ds, backend):
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=regions_ds,
                              models=_routed_models(), memories=[_routed_memory()])
    mem = await storage.get_memory("m_routed")
    assert mem.query is not None
    assert mem.query.filters == ["regions.opened_on >= '2024-01-01'"]
    assert measure_total(await run(storage, mem.query), measure="rev") == AMOUNT_OPENED_SINCE_2024


@pytest.fixture
def two_regions_ds(tmp_path, regions_ds):
    """``shop`` routes orders to regions; ``shop2`` holds the same models without the customers → regions join."""
    ds2 = DatasourceConfig(name=DS2, type="duckdb", database=_regions_db(tmp_path, "shop2"))
    models = [*_routed_models(), *(in_datasource(d, DS2) for d in (orders_v10(), customers_v10(), regions_v10()))]
    try:
        yield regions_ds, ds2, models
    finally:
        engine_factory.invalidate_engine(ds2)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("priority", "repaired"), [
    pytest.param([DS, DS2], True, id="routed-datasource-first"),
    pytest.param([DS2, DS], False, id="unrouted-datasource-first"),
    pytest.param([], False, id="no-priority-ambiguous"),
])
async def test_memory_repair_follows_the_datasource_priority(tmp_path, two_regions_ds, backend, priority, repaired):
    ds, ds2, models = two_regions_ds
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds, extra_datasources=[ds2],
                              models=models, memories=[_routed_memory()])
    await storage.set_datasource_priority(priority)
    mem = await storage.get_memory("m_routed")
    assert mem.query is not None
    if not repaired:
        assert mem.query.filters == [ROUTED_MEMORY_FILTER]
        return
    assert mem.query.filters == ["regions.opened_on >= '2024-01-01'"]
    assert measure_total(await run(storage, mem.query), measure="rev") == AMOUNT_OPENED_SINCE_2024


# --------------------------------------------------------------------------- #
# Building the repair's views never mutates the pass's raw reads.
# --------------------------------------------------------------------------- #
def _hygiene_models() -> list[dict]:
    return [
        *_routed_models(),
        query_backed_v10(name="rev_f", stages=[rev_query(filters=["regions.opened_on >= '2024/01/01'"])]),
        query_backed_v10(name="rev_g", stages=[rev_query(filters=["customers.signed_up_at >= '2024/01/01'"])]),
    ]


async def _load_recording_raw_reads(storage: StorageBackend, *, reverse: bool) -> list[tuple[dict, dict]]:
    """Load every model concurrently (identities reversed when ``reverse``); each raw read and a snapshot of it."""
    reads: list[tuple[dict, dict]] = []
    load_raw, identities = storage._load_raw_model_dict, storage._list_all_model_identities

    async def recording_load_raw(*, name: str, data_source: str) -> dict | None:
        doc = await load_raw(name=name, data_source=data_source)
        if isinstance(doc, dict):
            reads.append((doc, copy.deepcopy(doc)))
        return doc

    async def ordered_identities() -> list[tuple[str, str]]:
        found = sorted(await identities())
        return found[::-1] if reverse else found

    storage._load_raw_model_dict = recording_load_raw  # type: ignore[method-assign]
    storage._list_all_model_identities = ordered_identities  # type: ignore[method-assign]
    _, failures = await storage.load_models(data_source=DS)
    assert not failures
    return reads


@pytest.mark.parametrize("backend", BACKENDS)
async def test_concurrent_loads_leave_raw_reads_unmutated_and_bytes_order_independent(tmp_path, regions_ds, backend):
    stores = [
        await raw_store(backend=backend, base=str(tmp_path / f"store_{i}"), datasource=regions_ds,
                        models=_hygiene_models())
        for i in range(2)
    ]
    for storage, reverse in zip(stores, (False, True)):
        reads = await _load_recording_raw_reads(storage, reverse=reverse)
        assert reads
        assert all(doc == snapshot for doc, snapshot in reads)
    names = [d["name"] for d in _hygiene_models()]
    first, second = ({n: stored_bytes(backend=backend, base=str(tmp_path / f"store_{i}"), name=n) for n in names}
                     for i in range(2))
    assert first == second
    assert "'2024-01-01'" in first["rev_f"]
