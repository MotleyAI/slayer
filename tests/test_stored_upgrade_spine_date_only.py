"""The stored date-only upgrade types operands on the built-in time spine (spec: models/stored-upgrade)."""

from __future__ import annotations

import json
import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.models import DatasourceConfig
from slayer.core.time_spine import TIME_SPINE_COLUMN, TIME_SPINE_MODEL, spine_model
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from slayer.storage.date_only_migration import StoredModels
from slayer.storage.sqlite_conn import transaction
from tests._stored_upgrade_fixtures import (
    BACKENDS, DS, customers_v10, memory_v2, orders_v10, query_backed_v10, raw_store, run, seed_shop,
)

PRE = {"SlayerModel": 23, "SlayerQuery": 14, "Memory": 10}
OTHER = "warehouse"
JAN_31_ORDER = (7, 1, "ok", "2024-01-31 10:00:00", 70.0)
SPINE_LE = "time_spine.timestamp <= '2024-01-31'"
SPINE_LT = "time_spine.timestamp < '2024-02-01'"
TS_LE = "timestamp <= '2024-01-31'"
TS_LT = "timestamp < '2024-02-01'"
ORDERS_LE = "orders.ordered_at <= '2024-01-31'"
ORDERS_LT = "orders.ordered_at < '2024-02-01'"
SPINE_TIME_DIMENSION = {"dimension": "time_spine.timestamp", "granularity": "day"}


def stage(*filters: str, source: str = "orders", version: int = PRE["SlayerQuery"], **extra: Any) -> dict:
    return {"version": version, "source_model": source, "measures": [{"formula": "sum(amount)", "name": "rev"}],
            "filters": list(filters), **extra}


def spine_stage(*filters: str, source: str = TIME_SPINE_MODEL) -> dict:
    return {"version": PRE["SlayerQuery"], "source_model": source, "time_dimensions": [
        {"dimension": "timestamp", "granularity": "day"}], "filters": list(filters)}


def saved(name: str, *stages: dict, version: int = PRE["SlayerModel"]) -> dict:
    return query_backed_v10(name=name, stages=list(stages), version=version)


def model_doc(name: str, columns: list[dict], *, joins: list[dict] | None = None, **extra: Any) -> dict:
    return {"version": PRE["SlayerModel"], "name": name, "sql_table": "orders", "data_source": DS,
            "columns": columns, "joins": joins or [], **extra}


def memory(memory_id: str, body: dict, *, version: int = PRE["Memory"]) -> dict:
    return memory_v2(memory_id=memory_id, version=version, query=body)


def to_customers() -> dict:
    return {"target_model": "customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"}


def axisless_customers() -> dict:
    doc = customers_v10(version=PRE["SlayerModel"])
    return {**doc, "columns": [c for c in doc["columns"] if c["name"] != "signed_up_at"]}


def dumped(obj: Any) -> str:
    return json.dumps(obj.model_dump(mode="json"), default=str)


@pytest.fixture
def shop_ds(tmp_path: Path) -> Generator[DatasourceConfig]:
    db = str(tmp_path / "shop.db")
    seed_shop(db, dialect="sqlite")
    with transaction(db) as cur:
        cur.execute("INSERT INTO orders VALUES (?,?,?,?,?)", JAN_31_ORDER)
    ds = DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "database": db})
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def store(tmp_path: Path, ds: DatasourceConfig, *, backend: str, models: tuple[dict, ...] = (),
                memories: tuple[dict, ...] = (), orders: dict | None = None,
                customers: dict | None = None) -> StorageBackend:
    base = [orders or orders_v10(version=PRE["SlayerModel"]), customers or customers_v10(version=PRE["SlayerModel"])]
    return await raw_store(backend=backend, base=str(tmp_path / f"store-{backend}"), datasource=ds,
                           models=[*base, *models], memories=memories)


async def add_other_datasource(storage: StorageBackend, tmp_path: Path, *, backend: str,
                               models: tuple[dict, ...] = ()) -> None:
    """A second datasource, ``OTHER``, holding ``models`` written verbatim."""
    await storage.save_datasource(DatasourceConfig.model_validate(
        {"name": OTHER, "type": "sqlite", "database": str(tmp_path / "other.db")}))
    seed_other(tmp_path / f"store-{backend}", backend=backend, models=models)


def seed_other(base: Path, *, backend: str, models: tuple[dict, ...]) -> None:
    for doc in models:
        if backend == "yaml":
            path = base / "models" / OTHER / f"{doc['name']}.yaml"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
        else:
            with transaction(str(base / "store.db")) as conn:
                conn.execute("INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                             (OTHER, doc["name"], json.dumps(doc)))


async def loaded_filters(storage: StorageBackend, name: str, *, index: int = 0) -> list[str]:
    model = await storage.get_model(name, data_source=DS)
    assert model is not None
    return list((model.source_queries or [])[index].filters or [])


@contextmanager
def logged(caplog: pytest.LogCaptureFixture) -> Generator[list[str]]:
    texts: list[str] = []
    with caplog.at_level(logging.WARNING):
        yield texts
    texts.extend(r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)


@pytest.mark.parametrize("backend", BACKENDS)
class TestSpineOperands:
    async def test_spine_operand_in_a_query_on_a_stored_model(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                              backend: str) -> None:
        body = stage("time_spine.timestamp >= '2024-01-25'", ORDERS_LE, SPINE_LE,
                     time_dimensions=[SPINE_TIME_DIMENSION])
        storage = await store(tmp_path, shop_ds, backend=backend, models=(saved("jan", body),))
        model = await storage.get_model("jan", data_source=DS)
        assert model is not None
        q = (model.source_queries or [])[0]
        assert q.filters == ["time_spine.timestamp >= '2024-01-25'", ORDERS_LT, SPINE_LT]
        resp = await run(storage, q)
        by_day = {r["time_spine.timestamp"]: r["orders.rev"] for r in resp.data}
        assert by_day.get("2024-01-31") == pytest.approx(JAN_31_ORDER[-1]), resp.data

    async def test_spine_spelt_in_another_case(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        body = stage("TIME_SPINE.timestamp <= '2024-01-31'", "Time_Spine.TIMESTAMP > '2024-01-31'")
        storage = await store(tmp_path, shop_ds, backend=backend, models=(saved("cased", body),))
        assert await loaded_filters(storage, "cased") == [
            "TIME_SPINE.timestamp < '2024-02-01'", "Time_Spine.TIMESTAMP >= '2024-02-01'"]

    @pytest.mark.parametrize("source", [TIME_SPINE_MODEL, "TIME_SPINE"])
    async def test_query_rooted_at_the_spine(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                             source: str) -> None:
        body = spine_stage(TS_LE, SPINE_LE, ORDERS_LE, source=source)
        storage = await store(tmp_path, shop_ds, backend=backend, models=(saved("rooted", body),))
        assert await loaded_filters(storage, "rooted") == [TS_LT, SPINE_LT, ORDERS_LT]

    async def test_saved_measure_and_column_filter(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                   backend: str) -> None:
        orders = orders_v10(version=PRE["SlayerModel"])
        orders["columns"] = [*orders["columns"],
                             {"name": "jan_amount", "sql": "amount", "type": "DOUBLE", "filter": SPINE_LE}]
        orders["measures"] = [{"name": "jan_n", "formula": f"count(*) FILTER (WHERE {SPINE_LE})"}]
        storage = await store(tmp_path, shop_ds, backend=backend, orders=orders)
        model = await storage.get_model("orders", data_source=DS)
        assert model is not None
        assert [m.formula for m in model.measures] == [f"count(*) FILTER (WHERE {SPINE_LT})"]
        assert next(c for c in model.columns if c.name == "jan_amount").filter == SPINE_LT

    async def test_current_model_with_an_older_nested_query(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                            backend: str) -> None:
        doc = saved("nested", stage(SPINE_LE), version=PRE["SlayerModel"] + 1)
        storage = await store(tmp_path, shop_ds, backend=backend, models=(doc,))
        assert await loaded_filters(storage, "nested") == [SPINE_LT]

    async def test_memory_query_naming_its_datasource(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                      backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_orders", stage(SPINE_LE)),))
        mem = await storage.get_memory("m_orders")
        assert mem.query is not None
        assert mem.query.filters == [SPINE_LT]

    async def test_memory_query_naming_no_datasource(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                     backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend,
                              memories=(memory("m_spine", spine_stage(TS_LE, SPINE_LE)),))
        mem = await storage.get_memory("m_spine")
        assert mem.query is not None
        assert mem.query.filters == [TS_LT, SPINE_LT]

    async def test_memory_on_the_spine_in_a_store_with_one_datasource(
        self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
    ) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend,
                              memories=(memory("m_sole", spine_stage(ORDERS_LE)),))
        mem = await storage.get_memory("m_sole")
        assert mem.query is not None
        assert mem.query.filters == [ORDERS_LT]

    async def test_memory_on_the_spine_with_a_variant_in_two_datasources(
        self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
    ) -> None:
        variant = model_doc("Time_Spine", [{"name": "timestamp", "type": "TEXT", "primary_key": True}])
        storage = await store(tmp_path, shop_ds, backend=backend, models=(variant,),
                              memories=(memory("m_variants", spine_stage(TS_LE)),))
        await add_other_datasource(storage, tmp_path, backend=backend, models=({**variant, "data_source": OTHER},))
        mem = await storage.get_memory("m_variants")
        assert mem.query is not None
        assert mem.query.filters == [TS_LT]

    async def test_extension_column_filter(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        # Stored before extension columns were removed: the load-time conversion inlines it after this upgrade.
        body = {**stage(version=13), "measures": [{"formula": "sum(jan_amount)", "name": "jan"}],
                "extensions": [{"source_name": "orders", "columns": [
                    {"name": "jan_amount", "sql": "amount", "type": "DOUBLE", "filter": SPINE_LE}]}]}
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_ext", body, version=9),))
        mem_query = (await storage.get_memory("m_ext")).query
        assert mem_query is not None
        text = dumped(mem_query)
        assert SPINE_LT in text, text
        assert SPINE_LE not in text, text

    async def test_extension_on_the_spine(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        body = {**stage(), "extensions": [{"source_name": "TIME_SPINE", "measures": [
            {"name": "jan_days", "formula": f"count(*) FILTER (WHERE {TS_LE})"}]}]}
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_spine_ext", body),))
        mem = await storage.get_memory("m_spine_ext")
        assert mem.query is not None
        assert [m.formula for e in mem.query.extensions or [] for m in e.measures or []] == [
            f"count(*) FILTER (WHERE {TS_LT})"]

    async def test_spine_reached_through_an_extensions_join(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                            backend: str) -> None:
        body = {**stage(f"customers.{SPINE_LE}"), "extensions": [{"source_name": "orders", "joins": [to_customers()]}]}
        storage = await store(tmp_path, shop_ds, backend=backend, orders=orders_v10(version=PRE["SlayerModel"], joins=[]),
                              models=(saved("ext_join", body),))
        assert await loaded_filters(storage, "ext_join") == [f"customers.{SPINE_LT}"]

    @pytest.mark.parametrize("via", ["axis", "axisless_to_one", "reverse_axisless"])
    async def test_spine_reached_from_a_joined_model(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                                     via: str) -> None:
        customers = customers_v10(version=PRE["SlayerModel"]) if via == "axis" else axisless_customers()
        orders = orders_v10(version=PRE["SlayerModel"])
        if via == "reverse_axisless":
            orders = orders_v10(version=PRE["SlayerModel"], joins=[])
            customers = {**customers, "joins": [{"target_model": "orders", "join_pairs": [["id", "customer_id"]],
                                                 "cardinality": "one_to_many"}]}
        storage = await store(tmp_path, shop_ds, backend=backend, orders=orders, customers=customers,
                              models=(saved("joined", stage(f"customers.{SPINE_LE}")),))
        assert await loaded_filters(storage, "joined") == [f"customers.{SPINE_LT}"]

    async def test_spine_reached_from_a_detached_axisless_model(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                                backend: str) -> None:
        regions = model_doc("regions", [{"name": "region", "type": "TEXT", "primary_key": True}])
        body = stage(SPINE_LE, source="regions")
        storage = await store(tmp_path, shop_ds, backend=backend, models=(regions, saved("detached", body)))
        assert await loaded_filters(storage, "detached") == [SPINE_LT]

    async def test_legacy_time_literal_on_the_spine(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                    backend: str) -> None:
        body = {**spine_stage("timestamp <= '2024/01/31'"), "version": 1}
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_legacy", body, version=3),))
        mem = await storage.get_memory("m_legacy")
        assert mem.query is not None
        assert mem.query.filters == [TS_LT]

    async def test_legacy_time_literal_on_a_case_variant_source(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                                backend: str) -> None:
        body = {**stage("ordered_at <= '2024/01/31'", source="ORDERS"), "version": 1}
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_legacy_case", body, version=3),))
        mem = await storage.get_memory("m_legacy_case")
        assert mem.query is not None
        assert mem.query.filters == ["ordered_at < '2024-02-01'"]


@pytest.mark.parametrize("backend", BACKENDS)
class TestSpineLeftAlone:
    @pytest.mark.parametrize("body", [
        stage("TIME_SPINE.timestamp <= '2024-01-31'"),
        spine_stage(TS_LE, source="TIME_SPINE"),
    ], ids=["path", "root"])
    async def test_spine_spelling_shared_with_a_stored_variant(
        self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str, body: dict, caplog: pytest.LogCaptureFixture,
    ) -> None:
        variant = model_doc("Time_Spine", [{"name": "timestamp", "type": "TIMESTAMP", "primary_key": True}])
        storage = await store(tmp_path, shop_ds, backend=backend, models=(variant,),
                              memories=(memory("m_ambiguous", body),))
        with logged(caplog) as texts:
            mem = await storage.get_memory("m_ambiguous")
        assert mem.query is not None
        assert mem.query.filters == body["filters"]
        assert any("m_ambiguous" in t and body["filters"][0] in t for t in texts), texts

    async def test_memory_on_the_spine_in_a_store_with_two_datasources(
        self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str, caplog: pytest.LogCaptureFixture,
    ) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend,
                              memories=(memory("m_two_ds", spine_stage(ORDERS_LE)),))
        await add_other_datasource(storage, tmp_path, backend=backend)
        with logged(caplog) as texts:
            mem = await storage.get_memory("m_two_ds")
        assert mem.query is not None
        assert mem.query.filters == [ORDERS_LE]
        assert any("m_two_ds" in t and ORDERS_LE in t for t in texts), texts

    async def test_source_held_by_two_datasources(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                                  caplog: pytest.LogCaptureFixture) -> None:
        stored_spine = model_doc(TIME_SPINE_MODEL, [{"name": "timestamp", "type": "TEXT", "primary_key": True}])
        storage = await store(tmp_path, shop_ds, backend=backend, models=(stored_spine,),
                              memories=(memory("m_held_twice", spine_stage(TS_LE)),))
        await add_other_datasource(storage, tmp_path, backend=backend, models=({**stored_spine, "data_source": OTHER},))
        with logged(caplog) as texts:
            mem = await storage.get_memory("m_held_twice")
        assert mem.query is not None
        assert mem.query.filters == [TS_LE]
        assert any("m_held_twice" in t and TS_LE in t for t in texts), texts

    async def test_path_continuing_past_the_spine(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                                  caplog: pytest.LogCaptureFixture) -> None:
        crossing = "orders.time_spine.customers.created_at <= '2024-01-31'"
        customers = customers_v10(version=PRE["SlayerModel"])
        customers["columns"] = [*customers["columns"], {"name": "created_at", "sql": "signed_up_at", "type": "TIMESTAMP"}]
        storage = await store(tmp_path, shop_ds, backend=backend, customers=customers,
                              memories=(memory("m_crossing", stage(crossing)),))
        with logged(caplog) as texts:
            mem = await storage.get_memory("m_crossing")
        assert mem.query is not None
        assert mem.query.filters == [crossing]
        assert any("m_crossing" in t and crossing in t for t in texts), texts

    async def test_stored_join_named_like_the_spine(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                                    caplog: pytest.LogCaptureFixture) -> None:
        codes = model_doc("codes", [{"name": "id", "type": "INT", "primary_key": True}, {"name": "code", "type": "TEXT"}])
        orders = orders_v10(version=PRE["SlayerModel"], joins=[
            to_customers(), {"name": TIME_SPINE_MODEL, "target_model": "codes", "join_pairs": [["status", "code"]]}])
        text = "time_spine.code <= '2024-01-31'"
        storage = await store(tmp_path, shop_ds, backend=backend, orders=orders,
                              models=(codes, saved("join_named", stage(text))))
        with logged(caplog) as texts:
            filters = await loaded_filters(storage, "join_named")
        assert filters == [text]
        assert not any(text in t for t in texts), texts

    async def test_sibling_stage_named_like_the_spine(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                                      caplog: pytest.LogCaptureFixture) -> None:
        sibling = {**stage(), "name": TIME_SPINE_MODEL}
        doc = saved("staged", sibling, stage(SPINE_LE))
        storage = await store(tmp_path, shop_ds, backend=backend, models=(doc,))
        with logged(caplog) as texts:
            filters = await loaded_filters(storage, "staged", index=1)
        assert filters == [SPINE_LE]
        assert any("staged" in t and SPINE_LE in t for t in texts), texts

    async def test_grandfathered_stored_spine(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                              caplog: pytest.LogCaptureFixture) -> None:
        stored_spine = model_doc(TIME_SPINE_MODEL, [{"name": "timestamp", "type": "TEXT", "primary_key": True}])
        storage = await store(tmp_path, shop_ds, backend=backend,
                              models=(stored_spine, saved("grandfathered", spine_stage(SPINE_LE))))
        with logged(caplog) as texts:
            filters = await loaded_filters(storage, "grandfathered")
        assert filters == [SPINE_LE]
        assert not any(SPINE_LE in t for t in texts), texts


def stored(*extra: dict, stages: frozenset[str] = frozenset()) -> StoredModels:
    docs = [orders_v10(version=PRE["SlayerModel"]), customers_v10(version=PRE["SlayerModel"]), *extra]
    return StoredModels(data_source=DS, models={d["name"]: d for d in docs}, stages=stages)


SPINE_TYPE = spine_model(data_source=DS).columns[0].type.value


class TestStoredModels:
    @pytest.mark.parametrize("spelling", [TIME_SPINE_MODEL, "TIME_SPINE", "Time_Spine"])
    def test_host_resolves_the_spine(self, spelling: str) -> None:
        models = stored()
        host = models.host(spelling)
        assert host is not None
        assert host["name"] == TIME_SPINE_MODEL
        assert models.column_type(host=host, parts=[TIME_SPINE_COLUMN], extensions=[]) == SPINE_TYPE

    def test_spine_without_a_datasource(self) -> None:
        host = StoredModels().host("TIME_SPINE")
        assert host is not None
        assert StoredModels().column_type(host=host, parts=["TIMESTAMP"], extensions=[]) == SPINE_TYPE

    def test_spine_columns_come_from_the_core_definition(self) -> None:
        host = stored().host(TIME_SPINE_MODEL)
        assert host is not None
        expected = spine_model(data_source=DS).columns
        assert [(c["name"], c["type"]) for c in host["columns"]] == [(c.name, c.type.value) for c in expected]

    def test_exact_stored_spelling_wins(self) -> None:
        variant = model_doc("Time_Spine", [{"name": "timestamp", "type": "TEXT"}])
        models = stored(variant)
        assert models.host("Time_Spine") == variant
        spine = models.host(TIME_SPINE_MODEL)
        assert spine is not None
        assert spine["name"] == TIME_SPINE_MODEL
        assert models.column_type(host=spine, parts=[TIME_SPINE_COLUMN], extensions=[]) == SPINE_TYPE
        assert models.host("TIME_SPINE") is None

    def test_grandfathered_stored_spine_is_the_spine(self) -> None:
        grandfathered = model_doc(TIME_SPINE_MODEL, [{"name": "timestamp", "type": "TEXT"}])
        models = stored(grandfathered)
        assert models.host(TIME_SPINE_MODEL) == grandfathered
        assert models.host("TIME_SPINE") == grandfathered

    def test_stage_wins_over_the_spine(self) -> None:
        models = stored(stages=frozenset({TIME_SPINE_MODEL}))
        assert models.host(TIME_SPINE_MODEL) is None
        orders = models.host("orders")
        assert orders is not None
        assert models.column_type(host=orders, parts=[TIME_SPINE_MODEL, TIME_SPINE_COLUMN], extensions=[]) is None

    @pytest.mark.parametrize(("source", "parts", "expected"), [
        ("orders", ["time_spine", "timestamp"], SPINE_TYPE),
        ("orders", ["orders", "TIME_SPINE", "timestamp"], SPINE_TYPE),
        ("orders", ["customers", "time_spine", "timestamp"], SPINE_TYPE),
        ("time_spine", ["orders", "ordered_at"], "TIMESTAMP"),
        ("time_spine", ["time_spine", "orders", "customers", "signed_up_at"], "TIMESTAMP"),
        ("orders", ["time_spine", "customers", "signed_up_at"], None),
        ("orders", ["customers", "time_spine", "orders", "ordered_at"], None),
        ("time_spine", ["orders", "time_spine", "timestamp"], SPINE_TYPE),
    ])
    def test_column_type_walks_to_but_not_past_the_spine(self, source: str, parts: list[str],
                                                         expected: str | None) -> None:
        models = stored()
        host = models.host(source)
        assert host is not None
        assert models.column_type(host=host, parts=parts, extensions=[]) == expected

    def test_join_to_a_stored_variant_is_not_a_join_to_the_spine(self) -> None:
        variant = model_doc("Time_Spine", [{"name": "timestamp", "type": "TEXT"}])
        orders = orders_v10(version=PRE["SlayerModel"], joins=[
            {"name": "variant_link", "target_model": "Time_Spine", "join_pairs": [["ordered_at", "timestamp"]]}])
        models = StoredModels(data_source=DS, models={"orders": orders, "Time_Spine": variant})
        spine = models.host(TIME_SPINE_MODEL)
        assert spine is not None
        assert models.column_type(host=orders, parts=["time_spine", "timestamp"], extensions=[]) == SPINE_TYPE
        assert models.column_type(host=orders, parts=["Time_Spine", "timestamp"], extensions=[]) == "TEXT"
        assert models.column_type(host=spine, parts=["variant_link", "ordered_at"], extensions=[]) is None

    @pytest.mark.parametrize(("head", "expected"), [("Time_Spine", SPINE_TYPE), ("TIME_SPINE", None)])
    def test_head_naming_the_spine_host_beside_a_stored_variant(self, head: str, expected: str | None) -> None:
        # The engine drops a head naming the root in any case; a respelled spine head beside a variant is ambiguous.
        models = stored(model_doc("Time_Spine", [{"name": "timestamp", "type": "TEXT"}]))
        spine = models.host(TIME_SPINE_MODEL)
        assert spine is not None
        assert models.column_type(host=spine, parts=[head, "timestamp"], extensions=[]) == expected

    @pytest.mark.parametrize("stage_name", ["ORDERS", "Orders"])
    def test_head_naming_a_stage_beats_the_host(self, stage_name: str) -> None:
        models = stored(stages=frozenset({stage_name}))
        orders = models.models["orders"]
        assert models.column_type(host=orders, parts=["ORDERS", "ordered_at"], extensions=[]) is None

    @pytest.mark.parametrize(("parts", "expected"), [
        (["TIME_SPINE", "timestamp"], SPINE_TYPE),
        (["time_spine", "timestamp"], SPINE_TYPE),
        (["orders", "time_spine", "timestamp"], SPINE_TYPE),
        (["orders", "TIME_SPINE", "timestamp"], "TEXT"),
        (["orders", "Time_spine", "timestamp"], "TEXT"),
    ])
    def test_join_named_like_the_spine_in_another_case(self, parts: list[str], expected: str) -> None:
        # A head is respelled to the spine first; a later hop tries exact spellings before case-insensitive ones.
        codes = model_doc("codes", [{"name": "timestamp", "type": "TEXT"}])
        orders = orders_v10(version=PRE["SlayerModel"], joins=[
            {"name": "TIME_SPINE", "target_model": "codes", "join_pairs": [["status", "timestamp"]]}])
        models = StoredModels(data_source=DS, models={"orders": orders, "codes": codes})
        assert models.column_type(host=orders, parts=parts, extensions=[]) == expected

    def test_stored_join_named_like_the_spine_wins(self) -> None:
        codes = model_doc("codes", [{"name": "code", "type": "TEXT"}])
        orders = orders_v10(version=PRE["SlayerModel"], joins=[
            {"name": TIME_SPINE_MODEL, "target_model": "codes", "join_pairs": [["status", "code"]]}])
        models = StoredModels(data_source=DS, models={"orders": orders, "codes": codes})
        assert models.column_type(host=orders, parts=["time_spine", "code"], extensions=[]) == "TEXT"
        assert models.column_type(host=orders, parts=["time_spine", "timestamp"], extensions=[]) is None
