"""Stored root extensions move into ``extensions`` on load, silently (spec: models/stored-upgrade)."""

from __future__ import annotations

import warnings
from collections.abc import Iterator

import pytest

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.database import engine_factory
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS, stamp_stored
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    by_dimension,
    customers_v10,
    measure_total,
    memory_v2,
    orders_v10,
    query_backed_v10,
    raw_doc,
    raw_store,
    rev_query,
    run,
    seed_shop,
)

LEGACY_EXTENSION = {
    "source_name": "orders",
    "columns": [{"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"}],
    "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"}],
}
#: The rows the stage returned before the upgrade: sum(amount * 2) by customers.region.
RECORDED = {"North": 240.0, "South": 180.0}


def _stage(version: int | None = 9) -> dict:
    stage = {"source_model": LEGACY_EXTENSION, "dimensions": ["customers.region"],
             "measures": [{"formula": "sum(double_amount)", "name": "rev2"}]}
    return stage if version is None else {"version": version, **stage}


def _assert_upgraded(query: SlayerQuery | None) -> None:
    assert query is not None
    assert query.version == CURRENT_VERSIONS["SlayerQuery"]
    assert query.source_model == "orders"
    (extension,) = query.extensions or []
    assert isinstance(extension, ModelExtension)
    assert extension.source_name == "orders"
    assert [c.name for c in extension.columns or []] == ["double_amount"]
    assert [(j.target_model, j.join_pairs) for j in extension.joins or []] == [("customers", [["customer_id", "id"]])]


@pytest.fixture(autouse=True)
def quiet() -> Iterator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught
    assert [str(w.message) for w in caught if issubclass(w.category, FutureWarning)] == []


@pytest.fixture
def shop_ds(tmp_path) -> Iterator[DatasourceConfig]:
    db = str(tmp_path / "shop.duckdb")
    seed_shop(db, dialect="duckdb")
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def _qb_store(tmp_path, ds: DatasourceConfig, backend: str) -> StorageBackend:
    qb = query_backed_v10(name="rev2_q", stages=[_stage()], version=CURRENT_VERSIONS["SlayerModel"])
    return await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds,
                           models=[orders_v10(), customers_v10(), qb])


class TestVersion:
    def test_query_version_bumped(self) -> None:
        assert CURRENT_VERSIONS["SlayerQuery"] == 14


class TestQueryDocument:
    @pytest.mark.parametrize("version", [9, 10, 11, 12])
    def test_a_stored_query_converts_silently(self, version: int) -> None:
        _assert_upgraded(SlayerQuery.model_validate(_stage(version=version)))

    def test_a_version_1_extension_spelled_with_dimensions(self) -> None:
        legacy = {"source_name": "orders", "dimensions": [{"name": "region", "type": "string"}]}
        direct = SlayerQuery.model_validate({"version": 1, "source_model": legacy, "fields": [{"formula": "count(*)"}]})
        nested = SlayerModel.model_validate({"version": 1, "name": "qb", "data_source": DS, "source_queries": [
            {"version": 1, "source_model": legacy, "fields": [{"formula": "count(*)"}]}]})
        for query in (direct, (nested.source_queries or [None])[0]):
            assert query is not None
            assert query.source_model == "orders"
            (extension,) = query.extensions or []
            assert [c.name for c in extension.columns or []] == ["region"]

    def test_upgrade_is_idempotent(self) -> None:
        once = SlayerQuery.model_validate(_stage())
        dumped = once.model_dump(mode="json", exclude_none=True)
        twice = SlayerQuery.model_validate(dumped)
        assert twice == once
        assert twice.model_dump(mode="json", exclude_none=True) == dumped
        assert dumped["source_model"] == "orders"


class TestStoredModels:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_query_backed_model_stage_loads_upgraded_and_returns_its_rows(
        self, tmp_path, shop_ds, backend: str,
    ) -> None:
        storage = await _qb_store(tmp_path, shop_ds, backend)
        model = await storage.get_model("rev2_q", data_source=DS)
        assert model is not None
        _assert_upgraded((model.source_queries or [None])[0])
        resp = await run(storage, {"source_model": "rev2_q", "dimensions": ["customers__region"],
                                   "measures": [{"formula": "sum(rev2)", "name": "t"}]})
        assert by_dimension(resp, dim_suffix="customers__region") == RECORDED

    async def test_a_saved_query_run_by_name_returns_its_rows(self, tmp_path, shop_ds) -> None:
        storage = await _qb_store(tmp_path, shop_ds, "yaml")
        engine = SlayerQueryEngine(storage=storage)
        try:
            resp = await engine.execute("rev2_q")
        finally:
            engine.close()
        assert by_dimension(resp, dim_suffix="region") == RECORDED

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_save_writes_the_upgraded_form_and_reloads_unchanged(
        self, tmp_path, shop_ds, backend: str,
    ) -> None:
        storage = await _qb_store(tmp_path, shop_ds, backend)
        model = await storage.get_model("rev2_q", data_source=DS)
        assert model is not None
        await storage.save_model(model)
        [stage] = (await raw_doc(storage, "rev2_q"))["source_queries"]
        assert stage["version"] == CURRENT_VERSIONS["SlayerQuery"]
        assert stage["source_model"] == "orders"
        assert [e["source_name"] for e in stage["extensions"]] == ["orders"]
        assert await storage.get_model("rev2_q", data_source=DS) == model

    def test_a_current_version_model_holding_a_stored_stage(self) -> None:
        doc = query_backed_v10(name="rev2_q", stages=[_stage()], version=CURRENT_VERSIONS["SlayerModel"])
        _assert_upgraded((SlayerModel.model_validate(doc).source_queries or [None])[0])


class TestMemories:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_current_version_memory_holding_a_stored_query(
        self, tmp_path, shop_ds, backend: str,
    ) -> None:
        memory = memory_v2(memory_id="m_ext", query=_stage(), version=CURRENT_VERSIONS["Memory"])
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(), customers_v10()], memories=[memory])
        loaded = await storage.get_memory("m_ext")
        _assert_upgraded(loaded.query)
        assert loaded.query is not None
        assert by_dimension(await run(storage, loaded.query), dim_suffix="region") == RECORDED

    def test_an_unversioned_stored_memory(self) -> None:
        doc = {"id": "m_old", "learning": "x", "entities": [], "query": _stage(version=None)}
        _assert_upgraded(Memory.model_validate(stamp_stored(doc)).query)

    def test_a_memory_dump_reloads_unchanged(self) -> None:
        memory = Memory.model_validate(memory_v2(memory_id="m_ext", query=_stage(), version=CURRENT_VERSIONS["Memory"]))
        again = Memory.model_validate(memory.model_dump(mode="json", exclude_none=True))
        assert again == memory


EXTENDED_ORDERS = {
    "source_name": "orders",
    "columns": [{"name": "extended_date", "sql": "CAST(ordered_at AS DATE)", "type": "DATE"},
                {"name": "status_code", "sql": "status", "type": "TEXT"}],
}
EXTENDED_CUSTOMERS = {
    "source_name": "customers",
    "columns": [{"name": "signup_day", "sql": "CAST(signed_up_at AS DATE)", "type": "DATE"}],
}


def _legacy_root_stage(stored_filter: str) -> dict:
    return {"version": 9, **rev_query(filters=[stored_filter]), "source_model": EXTENDED_ORDERS}


def _extensions_stage(stored_filter: str) -> dict:
    return {"version": 12, **rev_query(filters=[stored_filter]), "extensions": [EXTENDED_CUSTOMERS]}


async def _loaded_stage(storage: StorageBackend) -> SlayerQuery:
    model = await storage.get_model("rev_x", data_source=DS)
    assert model is not None
    (stage,) = model.source_queries or []
    return stage


class TestExtensionColumnFilterLiterals:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize(("stage", "repaired", "expected"), [
        pytest.param(_legacy_root_stage("extended_date >= '2024/01/01'"), "extended_date >= '2024-01-01'", 200.0,
                     id="legacy-root-extension-column"),
        pytest.param(_legacy_root_stage("ordered_at >= '2024/01/01'"), "ordered_at >= '2024-01-01'", 200.0,
                     id="legacy-root-base-column"),
        pytest.param(_extensions_stage("customers.signup_day >= '2024/01/01'"),
                     "customers.signup_day >= '2024-01-01'", 180.0, id="extension-of-joined-model"),
    ])
    async def test_a_stored_stage_filter_on_an_extension_column_is_repaired_and_runs(
        self, tmp_path, shop_ds, backend: str, stage: dict, repaired: str, expected: float,
    ) -> None:
        qb = query_backed_v10(name="rev_x", stages=[stage])
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(), customers_v10(), qb])
        assert (await _loaded_stage(storage)).filters == [repaired]
        resp = await run(storage, {"source_model": "rev_x", "measures": ["sum(rev)"]})
        assert measure_total(resp, measure="rev_sum") == expected

    async def test_a_non_temporal_extension_column_literal_stays_verbatim(self, tmp_path, shop_ds) -> None:
        qb = query_backed_v10(name="rev_x", stages=[_legacy_root_stage("status_code = '2024/01/01'")])
        storage = await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(), customers_v10(), qb])
        assert (await _loaded_stage(storage)).filters == ["status_code = '2024/01/01'"]
        resp = await run(storage, {"source_model": "rev_x", "measures": ["sum(rev)"]})
        assert measure_total(resp, measure="rev_sum") == 60.0

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_stored_memory_filter_on_a_legacy_extension_column_is_repaired(
        self, tmp_path, shop_ds, backend: str,
    ) -> None:
        memory = memory_v2(memory_id="m_ext_f", query=_legacy_root_stage("extended_date >= '2024/01/01'"))
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(), customers_v10()], memories=[memory])
        loaded = await storage.get_memory("m_ext_f")
        assert loaded.query is not None
        assert loaded.query.filters == ["extended_date >= '2024-01-01'"]
        assert measure_total(await run(storage, loaded.query), measure="rev") == 200.0
