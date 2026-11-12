"""A fresh legacy root extension in ``source_model`` converts into ``extensions`` with a deprecation warning.

Spec: compatibility/removal-tripwire › Deprecated input forms convert with a structured warning.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.profiling import refresh_table_backed_model_sampled
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.facade.catalog import build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.mcp.server import create_mcp_server
from slayer.memories.models import Memory
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._deprecated_input_helpers import SAVED, deprecated_in, deprecations, mcp_payload, orders_engine, payload_of

LEGACY = {
    "source_name": "orders",
    "columns": [{"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"}],
    "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
}


def _future_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in caught if issubclass(w.category, FutureWarning)]


@pytest.fixture
def recorded() -> Iterator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


class TestFreshInputConverts:
    def test_dict_converts_with_a_deprecation_warning(self) -> None:
        with pytest.warns(FutureWarning) as caught:
            q = SlayerQuery.model_validate({"source_model": LEGACY, "measures": ["sum(double_amount)"]})
        message = " ".join(str(w.message) for w in caught)
        assert "extensions" in message, message
        assert "3.0" in message, message
        assert q.source_model == "orders"
        (extension,) = q.extensions or []
        assert isinstance(extension, ModelExtension)
        assert extension.source_name == "orders"
        assert [c.name for c in extension.columns or []] == ["double_amount"]
        assert [j.target_model for j in extension.joins or []] == ["customers"]

    def test_python_instance_converts_with_a_deprecation_warning(self) -> None:
        instance = ModelExtension.model_validate(LEGACY)
        with pytest.warns(FutureWarning, match="extensions"):
            q = SlayerQuery(source_model=instance)  # pyright: ignore[reportArgumentType] — the deprecated form, on purpose
        assert q.source_model == "orders"
        assert q.extensions == [instance]
        assert q.source_model_name == "orders"

    def test_a_stage_of_a_fresh_model_warns(self) -> None:
        with pytest.warns(FutureWarning, match="extensions"):
            model = SlayerModel.model_validate({"name": "qb", "data_source": "shop", "source_queries": [
                {"source_model": LEGACY, "measures": ["sum(double_amount)"]}]})
        assert [q.source_model for q in model.source_queries or []] == ["orders"]

    def test_the_query_of_a_fresh_memory_warns(self) -> None:
        with pytest.warns(FutureWarning, match="extensions"):
            memory = Memory.model_validate({"id": "m", "learning": "x", "entities": [], "query": {
                "source_model": LEGACY, "measures": ["sum(double_amount)"]}})
        assert memory.query is not None
        assert memory.query.source_model == "orders"

    @pytest.mark.parametrize("extensions", [1, 0, "", {}], ids=["one", "zero", "empty-string", "empty-object"])
    @pytest.mark.parametrize("version", [None, 9], ids=["fresh", "stored"])
    def test_an_invalid_extensions_container_fails_validation(
        self, recorded, extensions: object, version: int | None,
    ) -> None:
        data = {"source_model": LEGACY, "extensions": extensions, **({"version": version} if version else {})}
        with pytest.raises(ValidationError):
            SlayerQuery.model_validate(data)

    def test_legacy_form_next_to_an_extension_of_another_model(self) -> None:
        with pytest.warns(FutureWarning):
            q = SlayerQuery.model_validate({"source_model": LEGACY, "extensions": [{"source_name": "customers"}]})
        assert sorted(e.source_name for e in q.extensions or []) == ["customers", "orders"]

    def test_legacy_form_conflicting_with_an_extensions_entry(self) -> None:
        with pytest.raises(ValidationError) as info:
            SlayerQuery.model_validate({"source_model": LEGACY, "extensions": [{"source_name": "orders"}]})
        assert all(e["type"] != "extra_forbidden" for e in info.value.errors()), info.value
        assert "orders" in str(info.value)

    def test_current_version_cannot_use_the_legacy_form(self) -> None:
        with pytest.raises(ValidationError, match="extensions"):
            SlayerQuery.model_validate({"version": CURRENT_VERSIONS["SlayerQuery"], "source_model": LEGACY})

    def test_the_current_form_emits_no_warning(self, recorded) -> None:
        SlayerQuery.model_validate({"source_model": "orders", "extensions": [LEGACY]})
        assert _future_warnings(recorded) == []


#: The legacy form over the deprecated-input helpers' engine.
LEGACY_RUN = {"source_model": {"source_name": "orders"}, "measures": ["count(*)"]}


class TestStructuredDeprecation:
    def test_the_query_keeps_one_payload(self, recorded) -> None:
        q = SlayerQuery.model_validate({"source_model": LEGACY})
        [w] = deprecations(recorded)
        [payload] = q.deprecated_inputs
        assert payload is payload_of(w)
        assert (payload.kind, payload.removal) == ("deprecated_input", "3.0")
        assert "source_model" in payload.old
        assert "extensions" in payload.new

    def test_a_stored_document_carries_none(self, recorded) -> None:
        q = SlayerQuery.model_validate({"version": 9, "source_model": LEGACY})
        assert q.deprecated_inputs == ()
        assert deprecations(recorded) == []

    def test_strict_next_to_the_legacy_form_reports_both(self, recorded) -> None:
        q = SlayerQuery.model_validate({"source_model": LEGACY, "strict": True})
        assert sorted(p.old for p in q.deprecated_inputs) == sorted(payload_of(w).old for w in deprecations(recorded))
        assert len(q.deprecated_inputs) == 2

    @pytest.mark.parametrize("as_list", [False, True], ids=["mapping", "list"])
    async def test_the_engine_reports_a_mapping_once(self, recorded, as_list: bool) -> None:
        async with orders_engine() as engine:
            response = await engine.execute([LEGACY_RUN] if as_list else LEGACY_RUN)
        assert len(deprecated_in(response.warnings)) == 1
        assert len(deprecations(recorded)) == 1

    async def test_rest_reports_it(self, recorded) -> None:
        async with orders_engine() as engine:
            resp = TestClient(create_app(storage=engine.storage)).post("/query", json=LEGACY_RUN)
        assert resp.status_code == 200, resp.text
        [w] = deprecated_in(resp.json()["warnings"])
        assert "extensions" in w["new"]

    async def test_mcp_reports_it(self, recorded) -> None:
        async with orders_engine() as engine:
            server = create_mcp_server(storage=engine.storage)
            blocks, _ = await server.call_tool(name="query", arguments={"query": LEGACY_RUN, "format": "json"})
        assert len(deprecated_in(mcp_payload(getattr(next(iter(blocks)), "text")).get("warnings"))) == 1

    async def test_the_local_client_reports_it(self, recorded) -> None:
        async with orders_engine() as engine:
            response = await SlayerClient(storage=engine.storage).query(LEGACY_RUN)
        assert len(deprecated_in(response.warnings)) == 1

    async def test_a_model_save_reports_its_stage(self, recorded) -> None:
        async with orders_engine() as engine:
            model = SlayerModel.model_validate({"name": f"{SAVED}_legacy", "source_queries": [LEGACY_RUN]})
            notes: list = []
            await engine.save_model(model, warnings_out=notes)
        assert len(deprecated_in(notes)) == 1


class TestInternalProducers:
    async def test_profiling_emits_the_current_form(self, tmp_path, recorded, monkeypatch) -> None:
        db = str(tmp_path / "data.db")
        with transaction(db) as conn:
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount REAL, qty INTEGER)")
            conn.executemany("INSERT INTO t VALUES (?, ?, ?)", [(1, 10.0, 1), (2, 20.0, 3)])
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=db))
        model = SlayerModel(name="t", sql_table="t", data_source="ds", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE), Column(name="qty", type=DataType.INT)])
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        sent: list[SlayerQuery] = []
        real = engine.execute

        async def recording(*args, **kwargs):
            sent.append(kwargs["query"])
            return await real(*args, **kwargs)

        monkeypatch.setattr(engine, "execute", recording)
        try:
            errors = await refresh_table_backed_model_sampled(model=model, engine=engine, storage=storage)
        finally:
            engine.close()
        assert errors == []
        loaded = await storage.get_model("t", data_source="ds")
        assert loaded is not None
        assert next(c for c in loaded.columns if c.name == "amount").sampled == "10.0 .. 20.0"
        assert [q.source_model for q in sent if q.extensions] == ["t"]
        assert _future_warnings(recorded) == []

    def test_facade_dynamic_join_emits_the_current_form(self, recorded) -> None:
        orders = SlayerModel(name="orders", data_source="jaffle", sql_table="orders", columns=[
            Column(name="id", type=DataType.INT, primary_key=True), Column(name="store_id", type=DataType.INT)])
        stores = SlayerModel(name="stores", data_source="jaffle", sql_table="stores", columns=[
            Column(name="id", type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT)])
        sql = (
            'SELECT "Stores"."name" AS "Stores__name" FROM "public"."orders" '
            'LEFT JOIN (SELECT "public"."stores"."id" AS "id", "public"."stores"."name" AS "name" '
            'FROM "public"."stores") AS "Stores" ON "public"."orders"."store_id" = "Stores"."id"'
        )
        result = translate(sql=sql, catalog=build_catalog(models_by_datasource={"jaffle": [orders, stores]}))
        assert isinstance(result, QueryResult)
        assert result.query.source_model == "orders"
        (extension,) = result.query.extensions or []
        assert extension.source_name == "orders"
        assert [j.target_model for j in extension.joins or []] == ["stores"]
        assert _future_warnings(recorded) == []
