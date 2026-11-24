"""Legacy time literals in stored filters are repaired on load against temporal columns only (spec: models/stored-upgrade)."""

from __future__ import annotations

import copy

import pytest

from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from tests._stored_upgrade_fixtures import (
    AMOUNT_OPENED_BEFORE_2024,
    AMOUNT_OPENED_ON_JAN_1,
    AMOUNT_OPENED_SINCE_2024,
    BACKENDS,
    CUSTOMERS_TO_ORDERS,
    DS,
    TOTAL,
    customers_v10,
    in_datasource,
    metric_total,
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
    pytest.param("status = '2024/01/01'", "orders.status = '2024/01/01'", id="text-column"),
    pytest.param("missing_col >= '2024-01-01T00:00:00Z'", "orders.missing_col >= '2024-01-01T00:00:00Z'",
                 id="unresolvable"),
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


async def _loaded_filters(storage: StorageBackend, name: str = "rev_f", *, index: int = 0) -> list[str]:
    model = await storage.get_dataset(name, data_source=DS)
    assert model is not None
    return list((model.source_queries or [])[index].filter or [])


async def _total(storage: StorageBackend, name: str = "rev_f") -> float:
    return metric_total(await run(storage, {"population": name, "metrics": [f"sum({name}.rev)"]}), metric="rev_sum")


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("stored_filter", "expected"), REPAIRED_CASES)
async def test_legacy_literal_against_temporal_column_runs(tmp_path, shop_ds, backend, stored_filter, expected):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend=backend, filters=[stored_filter])
    [loaded] = await _loaded_filters(storage)
    assert not [frag for frag in LEGACY_FRAGMENTS if frag in loaded], loaded
    assert await _total(storage) == expected


async def test_repair_keeps_the_rest_of_the_filter_verbatim(tmp_path, shop_ds):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml",
                           filters=["ordered_at >= '2024-01-01T00:00:00Z' and like(status, 'n%')"])
    assert await _loaded_filters(storage) == [
        "orders.ordered_at >= '2024-01-01T00:00:00' and like(orders.status, 'n%')"]


@pytest.mark.parametrize(("stored_filter", "loaded"), VERBATIM_CASES)
async def test_non_temporal_or_unresolvable_comparison_stays_verbatim(tmp_path, shop_ds, stored_filter, loaded):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml", filters=[stored_filter])
    assert await _loaded_filters(storage) == [loaded]


async def test_mixed_in_list_is_left_to_the_grammar(tmp_path, shop_ds):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml",
                           filters=["ordered_at IN ('2024/01/01', status)"])
    with pytest.raises(StoredDocumentLoadError, match=r"ordered_at IN \('2024/01/01', status\).*must be a literal"):
        await _loaded_filters(storage)


async def test_text_column_comparison_keeps_its_rows(tmp_path, shop_ds):
    storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml", filters=["status = '2024/01/01'"])
    assert await _total(storage) == 60.0


@pytest.mark.parametrize("backend", BACKENDS)
async def test_memory_query_filter_literal_is_repaired(tmp_path, shop_ds, backend):
    query = rev_query(version=4, filters=["ordered_at >= '2024-01-01T02:00:00+02:00'"])
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                              models=[orders_v10(), customers_v10()],
                              memories=[memory_v2(memory_id="m_filter", query=query)])
    mem = await storage.get_memory("m_filter")
    assert mem.query is not None
    [loaded] = mem.query.filter or []
    assert "+02:00" not in loaded
    assert metric_total(await run(storage, mem.query), metric="rev") == 150.0


# --------------------------------------------------------------------------- #
# The compared column resolves as the binder resolves it.
# --------------------------------------------------------------------------- #
DS2 = "shop2"
ROUTE = "orders.customers.regions"


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
                 f"{ROUTE}.opened_on >= '2024-01-01'", AMOUNT_OPENED_SINCE_2024, id="short-form-two-hops"),
    pytest.param(_reverse_models, rev_query(filters=["customers.signed_up_at >= '2024-01-01T00:00:00Z'"]),
                 "customers.signed_up_at >= '2024-01-01T00:00:00'", 180.0, id="reverse-edge"),
    pytest.param(_routed_models, rev_query(filters=["'2024/01/01' <= regions.opened_on"]),
                 f"'2024-01-01' <= {ROUTE}.opened_on", AMOUNT_OPENED_SINCE_2024, id="short-form-reversed-operands"),
    pytest.param(_routed_models, rev_query(filters=["orders.regions.opened_on >= '2024/01/01'"]),
                 f"{ROUTE}.opened_on >= '2024-01-01'", AMOUNT_OPENED_SINCE_2024, id="own-qualifier-then-route"),
    pytest.param(_routed_models,
                 {"version": 12, **rev_query(filters=["regions.founded_on >= '2024/01/01'"]),
                  "extensions": [EXTENDED_REGIONS]},
                 "CAST(customers.regions.opened_on AS DATE) >= '2024-01-01'", AMOUNT_OPENED_SINCE_2024,
                 id="extension-on-routed-model"),
]


async def _routed_store(*, tmp_path, ds: DatasourceConfig, backend: str, models: list[dict], stage: dict,
                        name: str = "rev_f", **extra) -> StorageBackend:
    qb = query_backed_v10(name=name, stages=[stage])
    return await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds, models=[*models, qb], **extra)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("models", "stage", "repaired", "expected"), ROUTED_REPAIRS)
async def test_routed_reference_literal_is_repaired_and_runs(
    tmp_path, regions_ds, backend, models, stage, repaired, expected,
):
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend=backend, models=models(), stage=stage)
    assert await _loaded_filters(storage) == [repaired]
    assert await _total(storage) == expected


#: Each layout on the routed TIMESTAMP ``regions.opened_at``: repaired, then upgraded to its whole-day meaning.
OPENED_AT = f"{ROUTE}.opened_at"
ROUTED_LAYOUTS = [
    pytest.param("regions.opened_at >= '2024/01/01'", f"{OPENED_AT} >= '2024-01-01'", AMOUNT_OPENED_ON_JAN_1, id="gte"),
    pytest.param("regions.opened_at = '2024/01/01'", f"{OPENED_AT} in '2024-01-01'", AMOUNT_OPENED_ON_JAN_1, id="eq"),
    pytest.param("regions.opened_at == '2024/01/01'", f"{OPENED_AT} in '2024-01-01'", AMOUNT_OPENED_ON_JAN_1,
                 id="double-eq"),
    pytest.param("regions.opened_at != '2024/01/01'", f"{OPENED_AT} not in '2024-01-01'", AMOUNT_OPENED_BEFORE_2024,
                 id="neq"),
    pytest.param("regions.opened_at <> '2024/01/01'", f"{OPENED_AT} not in '2024-01-01'", AMOUNT_OPENED_BEFORE_2024,
                 id="ltgt"),
    pytest.param("regions.opened_at < '2024/01/01'", f"{OPENED_AT} < '2024-01-01'", AMOUNT_OPENED_BEFORE_2024, id="lt"),
    pytest.param("regions.opened_at <= '2024/01/01'", f"{OPENED_AT} < '2024-01-02'", TOTAL, id="lte"),
    pytest.param("regions.opened_at > '2024/01/01'", f"{OPENED_AT} >= '2024-01-02'", 0.0, id="gt"),
    pytest.param("'2024/01/01' <= regions.opened_at", f"'2024-01-01' <= {OPENED_AT}", AMOUNT_OPENED_ON_JAN_1,
                 id="lte-reversed"),
    pytest.param("'2024/01/01' > regions.opened_at", f"'2024-01-01' > {OPENED_AT}", AMOUNT_OPENED_BEFORE_2024,
                 id="gt-reversed"),
    pytest.param("'2024/01/01' >= regions.opened_at", f"'2024-01-02' > {OPENED_AT}", TOTAL, id="gte-reversed"),
    pytest.param("(regions.opened_at >= '2024/01/01') and (status = 'ok')",
                 f"({OPENED_AT} >= '2024-01-01') and (orders.status = 'ok')", 30.0, id="parenthesised"),
    pytest.param("regions.opened_at IN ('2024/01/01 10:00:00', '2024/02/01')",
                 f"{OPENED_AT} IN ('2024-01-01 10:00:00', '2024-02-01')", AMOUNT_OPENED_ON_JAN_1, id="in"),
    pytest.param("regions.opened_at NOT IN ('2024/01/01 10:00:00')",
                 f"{OPENED_AT} NOT IN ('2024-01-01 10:00:00')", AMOUNT_OPENED_BEFORE_2024, id="not-in"),
]


@pytest.mark.parametrize(("stored_filter", "loaded", "expected"), ROUTED_LAYOUTS)
async def test_every_operator_layout_on_a_routed_column(tmp_path, regions_ds, stored_filter, loaded, expected):
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend="yaml", models=_routed_models(),
                                  stage=rev_query(filters=[stored_filter]))
    assert await _loaded_filters(storage) == [loaded]
    assert await _total(storage) == expected


async def test_routed_text_column_stays_verbatim_and_keeps_its_rows(tmp_path, regions_ds):
    stage = rev_query(filters=["regions.label = '2024/01/01'"])
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend="yaml", models=_routed_models(),
                                  stage=stage)
    assert await _loaded_filters(storage) == [f"{ROUTE}.label = '2024/01/01'"]
    assert await _total(storage) == AMOUNT_OPENED_BEFORE_2024


async def test_ambiguous_short_form_route_is_refused_by_qualification(tmp_path, regions_ds):
    """``regions`` is reached through ``customers`` and through ``buyers``, both fan-out-free."""
    orders = orders_v10(joins=[
        {"target_model": "customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"},
        {"target_model": "buyers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"},
    ])
    models = [orders, customers_v10(joins=[region_join()]), customers_v10(name="buyers", joins=[region_join()]),
              regions_v10()]
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend="yaml", models=models,
                                  stage=rev_query(filters=["regions.opened_on >= '2024/01/01'"]))
    with pytest.raises(StoredDocumentLoadError, match="regions.opened_on"):
        await _loaded_filters(storage)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_unloadable_model_does_not_block_an_unrelated_repair(tmp_path, regions_ds, backend):
    broken = {"version": 10, "name": "broken", "sql_table": "broken", "data_source": DS,
              "columns": [{"type": "TEXT"}]}
    stage = rev_query(filters=["regions.opened_on >= '2024/01/01'"])
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend=backend,
                                  models=[*_routed_models(), broken], stage=stage)
    assert await _loaded_filters(storage) == [f"{ROUTE}.opened_on >= '2024-01-01'"]
    assert await _total(storage) == AMOUNT_OPENED_SINCE_2024


# --------------------------------------------------------------------------- #
# Quoting.
# --------------------------------------------------------------------------- #
QUOTED_ROUTE = "orders.customers.Regions"


def _case_sensitive_models() -> list[dict]:
    return [orders_v10(), customers_v10(joins=[region_join("Regions")]),
            regions_v10(name="Regions", date_column="Opened_On", timestamp_column="Opened_At")]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_double_quoted_hop_and_leaf_are_repaired(tmp_path, regions_ds, backend):
    stage = rev_query(filters=['"Regions"."Opened_On" >= \'2024/01/01\''])
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend=backend,
                                  models=_case_sensitive_models(), stage=stage)
    assert await _loaded_filters(storage) == [f'{QUOTED_ROUTE}."Opened_On" >= \'2024-01-01\'']
    assert await _total(storage) == AMOUNT_OPENED_SINCE_2024


async def test_double_quoted_leaf_in_the_wrong_case_stays_verbatim(tmp_path, regions_ds):
    stage = rev_query(filters=['"Regions"."OPENED_ON" >= \'2024/01/01\''])
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend="yaml",
                                  models=_case_sensitive_models(), stage=stage)
    assert await _loaded_filters(storage) == [f'{QUOTED_ROUTE}."OPENED_ON" >= \'2024/01/01\'']


async def test_double_quoted_hop_in_the_wrong_case_is_never_repaired(tmp_path, regions_ds):
    stage = rev_query(filters=['"REGIONS"."Opened_On" >= \'2024/01/01\''])
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend="yaml",
                                  models=_case_sensitive_models(), stage=stage)
    with pytest.raises(StoredDocumentLoadError, match="REGIONS"):
        await _loaded_filters(storage)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_qualified_quoted_leaf_keeps_its_exact_spelling(tmp_path, regions_ds, backend):
    stage = {**rev_query(filters=['"Regions"."Opened_At" >= \'2024-01-01\'']), "version": 16}
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend=backend,
                                  models=_case_sensitive_models(), stage=stage)
    assert await _loaded_filters(storage) == [f'{QUOTED_ROUTE}."Opened_At" >= \'2024-01-01\'']
    assert await _total(storage) == AMOUNT_OPENED_ON_JAN_1


# --------------------------------------------------------------------------- #
# Populations: an earlier stage, and one inferred for a root-less query.
# --------------------------------------------------------------------------- #
async def test_stage_population_types_only_its_extension_joins(tmp_path, regions_ds):
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
    assert await _loaded_filters(storage, "rev_s", index=-1) == [
        "base.ordered_at >= '2024/01/01'", "base.customers.signed_up_at >= '2024-01-01T00:00:00'"]


def _rootless(*filters: str) -> dict:
    return {"version": 12, "dimensions": [{"name": "status"}], "measures": [{"formula": "sum(amount)", "name": "rev"}],
            "filters": list(filters)}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_inferred_population_types_a_routed_reference(tmp_path, regions_ds, backend):
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend=backend, models=_routed_models(),
                                  stage=_rootless("regions.opened_on >= '2024/01/01'"))
    assert await _loaded_filters(storage) == [f"{ROUTE}.opened_on >= '2024-01-01'"]


async def test_ambiguous_inferred_population_is_refused(tmp_path, regions_ds):
    stage = {**_rootless("signed_up_at >= '2024-01-01T00:00:00Z'"), "dimensions": []}
    models = [*_routed_models(), customers_v10(name="buyers")]
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend="yaml", models=models, stage=stage)
    with pytest.raises(StoredDocumentLoadError, match="names no population"):
        await _loaded_filters(storage)


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
    assert mem.query.filter == [f"{ROUTE}.opened_on >= '2024-01-01'"]
    assert metric_total(await run(storage, mem.query), metric="rev") == AMOUNT_OPENED_SINCE_2024


@pytest.fixture
def two_regions_ds(tmp_path, regions_ds):
    """``shop`` routes orders to a DATE ``regions.opened_on``; in ``shop2`` the same route reaches a TEXT one."""
    ds2 = DatasourceConfig(name=DS2, type="duckdb", database=_regions_db(tmp_path, "shop2"))
    shop2 = (orders_v10(), customers_v10(joins=[region_join()]), regions_v10(date_type="TEXT"))
    models = [*_routed_models(), *(in_datasource(d, DS2) for d in shop2)]
    try:
        yield regions_ds, ds2, models
    finally:
        engine_factory.invalidate_engine(ds2)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("priority", "loaded"), [
    pytest.param([DS, DS2], f"{ROUTE}.opened_on >= '2024-01-01'", id="date-datasource-first"),
    pytest.param([DS2, DS], f"{ROUTE}.opened_on >= '2024/01/01'", id="text-datasource-first"),
])
async def test_memory_repair_follows_the_datasource_priority(tmp_path, two_regions_ds, backend, priority, loaded):
    ds, ds2, models = two_regions_ds
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds, extra_datasources=[ds2],
                              models=models, memories=[_routed_memory()])
    await storage.set_datasource_priority(priority)
    mem = await storage.get_memory("m_routed")
    assert mem.query is not None
    assert mem.query.filter == [loaded]
    if priority[0] == DS:
        assert metric_total(await run(storage, mem.query), metric="rev") == AMOUNT_OPENED_SINCE_2024


# --------------------------------------------------------------------------- #
# The whole-day upgrade types its operand as the binder resolves it.
# --------------------------------------------------------------------------- #
PRE_WHOLE_DAY = {"SlayerModel": 23, "SlayerQuery": 14}


def _whole_day_stage(*filters: str) -> dict:
    return {**rev_query(filters=list(filters)), "version": PRE_WHOLE_DAY["SlayerQuery"]}


def _models_at(version: int, models: list[dict]) -> list[dict]:
    return [{**m, "version": version} for m in models]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("stage", [
    pytest.param(_whole_day_stage("regions.opened_at = '2024-01-01'"), id="date-only"),
    pytest.param({**rev_query(filters=["regions.opened_at = '2024/01/01'"]), "version": 10}, id="slashed"),
])
async def test_routed_operand_keeps_its_whole_day_meaning(tmp_path, regions_ds, backend, stage):
    models = _models_at(PRE_WHOLE_DAY["SlayerModel"], _routed_models())
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=regions_ds, models=[
        *models, query_backed_v10(name="rev_f", stages=[stage], version=PRE_WHOLE_DAY["SlayerModel"])])
    assert await _loaded_filters(storage) == [f"{OPENED_AT} in '2024-01-01'"]
    assert await _total(storage) == AMOUNT_OPENED_ON_JAN_1


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("stored_filter", "loaded"), [
    pytest.param('"Regions"."Opened_At" <= \'2024-01-31\'', f'{QUOTED_ROUTE}."Opened_At" < \'2024-02-01\'',
                 id="exact"),
    pytest.param('"Regions"."OPENED_AT" <= \'2024-01-31\'', f'{QUOTED_ROUTE}."OPENED_AT" <= \'2024-01-31\'',
                 id="wrong-case"),
])
async def test_quoted_operand_is_typed_by_its_exact_spelling(tmp_path, regions_ds, backend, stored_filter, loaded):
    models = _models_at(PRE_WHOLE_DAY["SlayerModel"], _case_sensitive_models())
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=regions_ds, models=[
        *models, query_backed_v10(name="rev_f", stages=[_whole_day_stage(stored_filter)],
                                  version=PRE_WHOLE_DAY["SlayerModel"])])
    assert await _loaded_filters(storage) == [loaded]


EXTENDED_REGIONS_AT = {
    "source_name": "regions",
    "columns": [{"name": "founded_at", "sql": "opened_at", "type": "TIMESTAMP"}],
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("model_version", [pytest.param(10, id="older-model"), pytest.param(28, id="current-model")])
async def test_older_stage_with_extension_columns_is_typed_inlined_and_qualified(
    tmp_path, regions_ds, backend, model_version,
):
    stage = {**rev_query(filters=["regions.founded_at = '2024-01-01'"]), "version": 9,
             "extensions": [EXTENDED_REGIONS_AT]}
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=regions_ds, models=[
        *_routed_models(), query_backed_v10(name="rev_f", stages=[stage], version=model_version)])
    assert await _loaded_filters(storage) == ["CAST(customers.regions.opened_at AS TIMESTAMP) in '2024-01-01'"]
    assert await _total(storage) == AMOUNT_OPENED_ON_JAN_1


#: A current-version stage still holding extension columns (hand-edited): its own form, not its version, needs inlining.
CURRENT_STAGE_WITH_COLUMNS = {
    "version": 17, "population": "orders", "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
    "filter": ["orders.customers.regions.founded_at >= '2024-01-01'"],
    "extensions": [{"dataset": "regions", "columns": EXTENDED_REGIONS_AT["columns"]}],
}
INLINED_FOUNDED_AT = "CAST(customers.regions.opened_at AS TIMESTAMP) >= '2024-01-01'"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_current_stage_holding_extension_columns_is_inlined(tmp_path, regions_ds, backend):
    stage = CURRENT_STAGE_WITH_COLUMNS
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=regions_ds, models=[
        *_routed_models(), query_backed_v10(name="rev_f", stages=[stage], version=28)])
    assert await _loaded_filters(storage) == [INLINED_FOUNDED_AT]
    assert await _total(storage) == AMOUNT_OPENED_ON_JAN_1


@pytest.mark.parametrize("backend", BACKENDS)
async def test_current_memory_query_holding_extension_columns_is_inlined(tmp_path, regions_ds, backend):
    memory = memory_v2(memory_id="m_columns", query=CURRENT_STAGE_WITH_COLUMNS, version=13)
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=regions_ds,
                              models=_routed_models(), memories=[memory])
    mem = await storage.get_memory("m_columns")
    assert mem.query is not None
    assert mem.query.filter == [INLINED_FOUNDED_AT]


# --------------------------------------------------------------------------- #
# The conversion scope: no recursion, no double write, raw reads unmutated.
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
    load_raw, identities = storage._load_raw_dataset_dict, storage._list_all_dataset_identities

    async def recording_load_raw(*, name: str, data_source: str) -> dict | None:
        doc = await load_raw(name=name, data_source=data_source)
        if isinstance(doc, dict):
            reads.append((doc, copy.deepcopy(doc)))
        return doc

    async def ordered_identities() -> list[tuple[str, str]]:
        found = sorted(await identities())
        return found[::-1] if reverse else found

    storage._load_raw_dataset_dict = recording_load_raw  # type: ignore[method-assign]
    storage._list_all_dataset_identities = ordered_identities  # type: ignore[method-assign]
    _, failures = await storage.load_datasets(data_source=DS)
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


def _counting_saves(storage: StorageBackend) -> list[str]:
    saved: list[str] = []
    save = storage.save_dataset

    async def counting_save(dataset, *args, **kwargs):
        saved.append(dataset.name)
        return await save(dataset, *args, **kwargs)

    storage.save_dataset = counting_save  # type: ignore[method-assign]
    return saved


@pytest.mark.parametrize("backend", BACKENDS)
async def test_loading_the_converting_document_neither_recurses_nor_writes_twice(tmp_path, regions_ds, backend):
    storage = await _routed_store(tmp_path=tmp_path, ds=regions_ds, backend=backend, models=_routed_models(),
                                  stage=rev_query(filters=["regions.opened_on >= '2024/01/01'"]))
    saved = _counting_saves(storage)
    assert await _loaded_filters(storage) == [f"{ROUTE}.opened_on >= '2024-01-01'"]
    assert saved.count("rev_f") == 0


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("first", ["rev_f", "rev_g"])
async def test_two_model_cycle_neither_recurses_nor_writes_a_peer_twice(tmp_path, regions_ds, backend, first):
    """``rev_g`` reads ``rev_f``, and ``rev_f``'s conversion reads every dataset of its datasource, ``rev_g`` included."""
    rev_g = query_backed_v10(name="rev_g", stages=[{
        "source_model": "rev_f", "measures": [{"formula": "sum(rev)", "name": "total"}],
        "filters": ["rev >= 0"]}])
    rev_f = query_backed_v10(name="rev_f", stages=[rev_query(filters=["regions.opened_on >= '2024/01/01'"])])
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=regions_ds,
                              models=[*_routed_models(), rev_f, rev_g])
    saved = _counting_saves(storage)
    second = "rev_g" if first == "rev_f" else "rev_f"
    loaded = {n: await _loaded_filters(storage, n) for n in (first, second)}
    assert loaded == {"rev_f": [f"{ROUTE}.opened_on >= '2024-01-01'"], "rev_g": ["rev_f.rev >= 0"]}
    assert (saved.count("rev_f"), saved.count("rev_g")) == (0, 0), saved
    total = await run(storage, {"population": "rev_g", "metrics": ["sum(rev_g.total)"]})
    assert metric_total(total, metric="total_sum") == AMOUNT_OPENED_SINCE_2024
