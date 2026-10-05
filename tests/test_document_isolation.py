"""One stored model or memory that cannot be loaded: typed load error, skip-and-warn enumeration, fail-closed answer picking (spec: models/document-isolation)."""

from __future__ import annotations

import json
import os
import tempfile
import warnings
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

import pydantic
import pytest
import sqlalchemy.exc
import yaml
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.memories.resolver import resolve_entity
from slayer.search.retrievers.bm25 import BM25Retriever
from slayer.search.service import SearchService
from slayer.sql import engine_factory
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

DS = "ds"
BAD = "orders"
CAUSE = "cid_expr"
GOOD_MODELS = ("customers", "products")
BAD_MEMORY = "broken-note"
GOOD_MEMORIES = ("good-a", "good-b")


def _bad_orders() -> dict:
    """Join keyed on an expression column: fails construction, not repaired on load."""
    return {
        "version": 10, "name": BAD, "sql_table": "orders", "data_source": DS,
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "customer_id", "type": "INT"},
            {"name": "amount", "type": "INT"},
            {"name": CAUSE, "type": "INT", "sql": "customer_id + 0"},
        ],
        "joins": [{"target_model": "customers", "join_pairs": [[CAUSE, "id"]]}],
    }


def _customers() -> dict:
    """Lacks the live ``tier`` column, so re-ingest merges and saves it."""
    return {
        "version": 10, "name": "customers", "sql_table": "customers", "data_source": DS,
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "name", "type": "TEXT"},
        ],
    }


def _products(*, drifted: bool = False) -> dict:
    """A named edge, so a save loads every peer twice; ``drifted`` adds a column the live table lacks."""
    columns = [
        {"name": "id", "type": "INT", "primary_key": True},
        {"name": "customer_id", "type": "INT"},
        {"name": "label", "type": "TEXT"},
    ]
    if drifted:
        columns.append({"name": "gone", "type": "TEXT"})
    return {
        "version": 10, "name": "products", "sql_table": "products", "data_source": DS,
        "columns": columns,
        "joins": [{"name": "buyer", "target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
    }


def _orders_v1_colliding() -> dict:
    """The v1→v2 migration rejects a dimension and a measure sharing a name."""
    return {
        "version": 1, "name": BAD, "sql_table": "orders", "data_source": DS,
        "dimensions": [{"name": "amount", "sql": "amount", "type": "number"}],
        "measures": [{"name": "amount", "sql": "amount", "type": "sum"}],
    }


def _orders_v5_double() -> dict:
    return {
        "version": 5, "name": BAD, "sql_table": "orders", "data_source": DS,
        "columns": [{"name": "id", "type": "INT", "primary_key": True}, {"name": "amount", "type": "DOUBLE"}],
    }


def _orders_bad_version() -> dict:
    return {
        "version": "abc", "name": BAD, "sql_table": "orders", "data_source": DS,
        "columns": [{"name": "id", "type": "INT", "primary_key": True}],
    }


_UNREACHABLE_PG = DatasourceConfig(
    name=DS, type="postgres", host="127.0.0.1", port=1, database="x", username="u", password="p",
)


def _is_value_error(text: str) -> Callable[[BaseException], bool]:
    return lambda cause: type(cause) is ValueError and text in str(cause)


# (doc, datasource override or None to drop the entry or "live", cause predicate)
_LOAD_FAILURES: dict[str, tuple[Callable[[], dict], Any, Callable[[BaseException], bool]]] = {
    "migration": (_orders_v1_colliding, "live", _is_value_error("collision")),
    "refinement-missing-datasource": (
        _orders_v5_double, None, _is_value_error("unavailable for type refinement")),
    "refinement-driver-error": (
        _orders_v5_double, _UNREACHABLE_PG,
        lambda cause: isinstance(cause, sqlalchemy.exc.OperationalError)),
    "malformed-version": (_orders_bad_version, "live", _is_value_error("abc")),
}


def _memory(memory_id: str, **extra) -> dict:
    return {"version": 2, "id": memory_id, "learning": f"note {memory_id}", **extra}


def _seed_data(db_path: str) -> None:
    with transaction(db_path) as cur:
        cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, tier TEXT)")
        cur.execute("INSERT INTO customers VALUES (1, 'Ann', 'gold'), (2, 'Bob', 'silver')")
        cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)")
        cur.execute("INSERT INTO orders VALUES (1, 1, 10), (2, 2, 20)")
        cur.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, customer_id INTEGER, label TEXT, sku TEXT)")
        cur.execute("INSERT INTO products VALUES (1, 1, 'pen', 'P1')")


class Store:
    """A raw-seeded YAML or SQLite store over a live SQLite datasource."""

    def __init__(self, *, kind: str, tmp: str) -> None:
        self.kind = kind
        self.tmp = tmp
        data_db = os.path.join(tmp, "data.db")
        _seed_data(data_db)
        self.datasource = DatasourceConfig(name=DS, type="sqlite", database=data_db)
        self.store_db = os.path.join(tmp, "store.db")
        self.storage: StorageBackend = (
            YAMLStorage(base_dir=os.path.join(tmp, "store")) if kind == "yaml"
            else SQLiteStorage(db_path=self.store_db)
        )

    async def seed(
        self, *, models: list[dict] | None = None, memories: list[dict] | None = None,
        register_datasource: bool = True,
    ) -> StorageBackend:
        if register_datasource:
            await self.storage.save_datasource(self.datasource)
        for m in models or []:
            self.write_model(name=m["name"], text=yaml.dump(m, sort_keys=False) if self.kind == "yaml" else json.dumps(m))
        for mem in memories or []:
            self._write_memory(mem)
        return self.storage

    def write_model(self, *, name: str, text: str) -> None:
        if self.kind == "yaml":
            path = os.path.join(self.tmp, "store", "models", DS, f"{name}.yaml")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:  # NOSONAR(S7493) — test seed
                f.write(text)
            return
        with transaction(self.store_db) as conn:
            conn.execute("INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)", (DS, name, text))

    def corrupt_text(self) -> str:
        return "columns: [unclosed\n" if self.kind == "yaml" else "{not json"

    def _write_memory(self, mem: dict) -> None:
        if self.kind == "yaml":
            fm = {k: v for k, v in mem.items() if k not in ("id", "learning")}
            path = self.storage._memory_md_path(mem["id"])  # type: ignore[attr-defined]
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:  # NOSONAR(S7493) — test seed
                f.write(f"---\n{yaml.safe_dump(fm)}---\n{mem['learning']}")
            return
        with transaction(self.store_db) as conn:
            conn.execute("INSERT INTO memories (id, data) VALUES (?, ?)", (mem["id"], json.dumps(mem)))


@pytest.fixture(params=["yaml", "sqlite"])
async def store(request) -> AsyncIterator[Store]:
    with tempfile.TemporaryDirectory() as tmp:
        s = Store(kind=request.param, tmp=tmp)
        try:
            yield s
        finally:
            engine_factory.invalidate_engine(s.datasource)


@pytest.fixture
async def bad_store(store: Store) -> StorageBackend:
    return await store.seed(models=[_bad_orders(), _customers(), _products(drifted=True)])


@pytest.fixture
def recorded() -> Iterator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter("always")
        yield records


def _unloadable(records: list[warnings.WarningMessage], name: str) -> list[warnings.WarningMessage]:
    return [
        r for r in records
        if getattr(getattr(r.message, "payload", None), "kind", None) == "unloadable_document"
        and name in str(r.message)
    ]


def _assert_one_warning(records: list[warnings.WarningMessage], *, name: str, cause: str) -> None:
    [w] = _unloadable(records, name)
    assert isinstance(w.message, UserWarning)
    assert cause in str(w.message)


class TestTypedLoadError:
    async def test_validation_failure_names_the_model(self, bad_store: StorageBackend) -> None:
        with pytest.raises(StoredDocumentLoadError) as ei:
            await bad_store.get_model(BAD, data_source=DS)
        assert isinstance(ei.value, ValueError)
        assert f"{DS}.{BAD}" in str(ei.value)
        assert isinstance(ei.value.__cause__, pydantic.ValidationError)
        assert CAUSE in str(ei.value.__cause__)

    async def test_corrupt_document_names_the_model(self, store: Store) -> None:
        storage = await store.seed(models=[_customers()])
        store.write_model(name=BAD, text=store.corrupt_text())
        with pytest.raises(StoredDocumentLoadError) as ei:
            await storage.get_model(BAD, data_source=DS)
        assert isinstance(ei.value, ValueError)
        assert f"{DS}.{BAD}" in str(ei.value)
        assert ei.value.__cause__ is not None
        assert await storage.get_model("customers", data_source=DS) is not None

    @pytest.mark.parametrize("case", sorted(_LOAD_FAILURES))
    async def test_every_load_stage_failure_is_typed(self, store: Store, case: str) -> None:
        make_doc, datasource, cause_ok = _LOAD_FAILURES[case]
        if datasource not in (None, "live"):
            store.datasource = datasource
        storage = await store.seed(models=[make_doc()], register_datasource=datasource is not None)
        with pytest.raises(StoredDocumentLoadError) as ei:
            await storage.get_model(BAD, data_source=DS)
        assert isinstance(ei.value, ValueError)
        assert f"{DS}.{BAD}" in str(ei.value)
        assert ei.value.__cause__ is not None
        assert cause_ok(ei.value.__cause__), repr(ei.value.__cause__)


class TestEnumerationSkipsAndWarns:
    @pytest.mark.parametrize("name", GOOD_MODELS)
    async def test_every_valid_model_resaves_with_one_warning(
        self, bad_store: StorageBackend, recorded: list, name: str,
    ) -> None:
        model = await bad_store.get_model(name, data_source=DS)
        assert model is not None
        recorded.clear()
        await bad_store.save_model(model)
        _assert_one_warning(recorded, name=BAD, cause=CAUSE)

    async def test_reingest_succeeds_and_reports_the_bad_model_once(
        self, store: Store, bad_store: StorageBackend, recorded: list,
    ) -> None:
        result = await ingest_datasource_idempotent(datasource=store.datasource, storage=bad_store)
        bad_errors = [e for e in result.errors if e.model_name == BAD]
        assert len(bad_errors) == 1
        assert CAUSE in bad_errors[0].error
        assert [e for e in result.errors if e.model_name != BAD] == []
        customers = await bad_store.get_model("customers", data_source=DS)
        assert customers is not None
        assert customers.get_column("tier") is not None
        products = await bad_store.get_model("products", data_source=DS)
        assert products is not None
        assert products.get_column("sku") is not None
        _assert_one_warning(recorded, name=BAD, cause=CAUSE)

    async def test_direct_load_still_raises_after_a_skip(self, bad_store: StorageBackend) -> None:
        customers = await bad_store.get_model("customers", data_source=DS)
        assert customers is not None
        await bad_store.save_model(customers)
        with pytest.raises(StoredDocumentLoadError):
            await bad_store.get_model(BAD, data_source=DS)


async def _mcp_text(storage: StorageBackend, *, tool: str, arguments: dict[str, Any]) -> str:
    result: Any = await create_mcp_server(storage=storage).call_tool(name=tool, arguments=arguments)
    return result[0][0].text


class TestEnumerationSurfaces:
    async def test_builtin_models_detailed(self, bad_store: StorageBackend, recorded: list) -> None:
        [spine] = await bad_store.builtin_models(DS, detailed=True)
        assert spine.name == "time_spine"
        _assert_one_warning(recorded, name=BAD, cause=CAUSE)

    async def test_mcp_models_summary(self, bad_store: StorageBackend, recorded: list) -> None:
        text = await _mcp_text(bad_store, tool="models_summary", arguments={"datasource_name": DS})
        assert all(name in text for name in GOOD_MODELS)
        _assert_one_warning(recorded, name=BAD, cause=CAUSE)

    async def test_mcp_inspect_model_unknown_lists_the_rest(self, bad_store: StorageBackend, recorded: list) -> None:
        text = await _mcp_text(bad_store, tool="inspect_model", arguments={"model_name": "nope"})
        assert all(f"{DS}.{name}" in text for name in GOOD_MODELS)
        _assert_one_warning(recorded, name=BAD, cause=CAUSE)

    async def test_search_carries_the_warning(self, bad_store: StorageBackend, recorded: list) -> None:
        service = SearchService(storage=bad_store, retrievers=[BM25Retriever()])
        response = await service.search(question="customers label", datasource=DS)
        [w] = _unloadable(recorded, BAD)
        assert str(w.message) in response.warnings

    def test_rest_models_listing(self, bad_store: StorageBackend, recorded: list) -> None:
        resp = TestClient(create_app(storage=bad_store)).get("/models", params={"data_source": DS})
        assert resp.status_code == 200
        names = {m["name"] for m in resp.json()}
        assert set(GOOD_MODELS) <= names
        assert BAD not in names
        _assert_one_warning(recorded, name=BAD, cause=CAUSE)

    async def test_query_peer_loading_warns_on_the_response(
        self, bad_store: StorageBackend, recorded: list,
    ) -> None:
        engine = SlayerQueryEngine(storage=bad_store)
        try:
            resp = await engine.execute(
                SlayerQuery.model_validate({"source_model": "customers", "dimensions": ["name"]}))
        finally:
            engine.close()
        assert {r["customers.name"] for r in resp.data} == {"Ann", "Bob"}
        [payload] = [w for w in resp.warnings or [] if w.kind == "unloadable_document"]
        assert BAD in payload.human_message()
        _assert_one_warning(recorded, name=BAD, cause=CAUSE)


class TestMemoryListing:
    @pytest.fixture
    async def memory_store(self, store: Store) -> StorageBackend:
        return await store.seed(memories=[
            _memory(GOOD_MEMORIES[0]),
            _memory(BAD_MEMORY, created_at="not-a-date"),
            _memory(GOOD_MEMORIES[1]),
        ])

    async def test_listing_returns_the_valid_memories_and_warns_once(
        self, memory_store: StorageBackend, recorded: list,
    ) -> None:
        listed = await memory_store.list_memories()
        assert sorted(m.id for m in listed) == sorted(GOOD_MEMORIES)
        _assert_one_warning(recorded, name=BAD_MEMORY, cause="created_at")

    async def test_fetching_the_invalid_memory_raises(self, memory_store: StorageBackend) -> None:
        with pytest.raises(StoredDocumentLoadError) as ei:
            await memory_store.get_memory(BAD_MEMORY)
        assert BAD_MEMORY in str(ei.value)
        assert isinstance(ei.value.__cause__, pydantic.ValidationError)


class TestValidateModels:
    async def test_reports_the_unloadable_model_and_the_others(self, bad_store: StorageBackend) -> None:
        engine = SlayerQueryEngine(storage=bad_store)
        try:
            report = await engine.validate_models(data_source=DS)
        finally:
            engine.close()
        [bad] = [e for e in report if e.model_name == BAD]
        assert CAUSE in bad.model_dump_json()
        [drift] = [e for e in report if e.model_name == "products"]
        assert "gone" in drift.model_dump_json()


class TestAnswerPickingFailsClosed:
    async def test_population_inference(self, bad_store: StorageBackend) -> None:
        engine = SlayerQueryEngine(storage=bad_store)
        query = SlayerQuery.model_validate({"dimensions": ["customers.name"]})
        try:
            with pytest.raises(StoredDocumentLoadError) as ei:
                await engine.execute(query)
        finally:
            engine.close()
        assert f"{DS}.{BAD}" in str(ei.value)

    async def test_memory_entity_resolution(self, bad_store: StorageBackend) -> None:
        with pytest.raises(StoredDocumentLoadError) as ei:
            await resolve_entity("label", storage=bad_store)
        assert f"{DS}.{BAD}" in str(ei.value)

    @pytest.mark.parametrize(("items", "data_source"), [
        pytest.param(["label"], DS, id="bare-name-scoping"),
        pytest.param(["customers.name"], None, id="recommend-root-model"),
    ])
    async def test_recommend_root_model(
        self, bad_store: StorageBackend, items: list[str], data_source: str | None,
    ) -> None:
        engine = SlayerQueryEngine(storage=bad_store)
        try:
            with pytest.raises(StoredDocumentLoadError) as ei:
                await engine.recommend_root_model(items, data_source=data_source)
        finally:
            engine.close()
        assert f"{DS}.{BAD}" in str(ei.value)

    async def test_detection_scope(self, bad_store: StorageBackend) -> None:
        engine = SlayerQueryEngine(storage=bad_store)
        try:
            with pytest.raises(StoredDocumentLoadError) as ei:
                await engine.detect_join_cardinality(data_source=DS)
        finally:
            engine.close()
        assert f"{DS}.{BAD}" in str(ei.value)
