"""Every read surface over a ``{fin}`` view reads as the store with ``hr`` and its dependents deleted."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from collections.abc import AsyncGenerator
from typing import cast

import pytest
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousModelError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.embeddings import client as embedding_client
from slayer.engine.population import infer_population
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect.service import InspectService
from slayer.pg_facade.connection import PgConnection
from slayer.search import graph as search_graph
from slayer.search.service import SearchService
from slayer.storage.tag_filtered import TagFilteredStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._model_access_fixtures import (
    ACCESS_PARAMS,
    DS,
    HIDDEN_TOKEN,
    HR_LEARNING,
    LEDGER_TOTAL,
    UNLOADABLE,
    VISIBLE_TO_FIN,
    AccessStore,
    access_store,
    mcp_text,
    outcome,
    reference_store,
)


class Pair(BaseModel):
    """The full store and the store a ``{fin}`` caller should be indistinguishable from."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    full: AccessStore
    reference: AccessStore

    @property
    def fin(self) -> TagFilteredStorage:
        return TagFilteredStorage(self.full.storage, tags={"fin"}, bypass=False)

    @property
    def everything(self) -> TagFilteredStorage:
        return TagFilteredStorage(self.full.storage, tags=set(), bypass=True)


@pytest.fixture(params=ACCESS_PARAMS)
async def pair(request) -> AsyncGenerator[Pair]:
    backend, dialect = request.param
    async with access_store(backend=backend, dialect=dialect) as full:
        async with reference_store(backend=backend, dialect=dialect) as reference:
            yield Pair(full=full, reference=reference)


@pytest.fixture(autouse=True)
def _fresh_graph_cache():
    search_graph.clear_cache()
    yield
    search_graph.clear_cache()


def leaks(text: str) -> list[str]:
    return HIDDEN_TOKEN.findall(text)


async def engine_outcome(storage, query: dict) -> tuple[str, str]:
    engine = SlayerQueryEngine(storage=storage)
    try:
        return await outcome(engine.execute(SlayerQuery.model_validate(query)))
    finally:
        engine.close()


class TestInspect:
    @pytest.mark.parametrize("compact", [True, False])
    @pytest.mark.parametrize("fmt", ["markdown", "json"])
    @pytest.mark.parametrize("name", sorted(VISIBLE_TO_FIN))
    async def test_visible_model_never_names_hr(self, pair: Pair, name: str, compact: bool, fmt: str) -> None:
        out = await InspectService(storage=pair.fin).inspect(
            reference=f"{DS}.{name}", entity_type="model", compact=compact, format=fmt,
        )
        assert name in out
        assert leaks(out) == []

    @pytest.mark.parametrize("compact", [True, False])
    @pytest.mark.parametrize("fmt", ["markdown", "json"])
    async def test_collection_lists_only_visible_models(self, pair: Pair, compact: bool, fmt: str) -> None:
        out = await InspectService(storage=pair.fin).inspect(reference=None, entity_type="model", compact=compact, format=fmt)
        assert "fin_report" in out
        assert leaks(out) == []
        assert UNLOADABLE not in out

    async def test_datasource_inspect_never_names_hr(self, pair: Pair) -> None:
        out = await InspectService(storage=pair.fin).inspect(reference=DS, entity_type="datasource", compact=False)
        assert leaks(out) == []

    @pytest.mark.parametrize("reference", [f"{DS}.hr", "hr", f"{DS}.hr.dept", "hr_summary", f"{DS}.fin.hr.dept"])
    async def test_hidden_reference_reads_like_a_missing_one(self, pair: Pair, reference: str) -> None:
        def inspect(storage):
            return InspectService(storage=storage).inspect(reference=reference, entity_type="model", compact=False)

        assert await outcome(inspect(pair.fin)) == await outcome(inspect(pair.reference.storage))

    async def test_models_summary_never_names_hr(self, pair: Pair) -> None:
        text = await mcp_text(pair.fin, "models_summary", datasource_name=DS)
        assert "fin_report" in text
        assert leaks(text) == []


class TestJoinPruning:
    @pytest.mark.parametrize("name", ["fin", "pub_link"])
    async def test_join_to_hidden_model_is_omitted(self, pair: Pair, name: str) -> None:
        pruned = await pair.fin.get_model(name, data_source=DS)
        full = await pair.everything.get_model(name, data_source=DS)
        assert pruned is not None
        assert full is not None
        assert [j.target_model for j in full.joins] == ["hr"]
        assert pruned.joins == []

    async def test_pruning_does_not_change_the_stored_model(self, pair: Pair) -> None:
        await pair.fin.get_model("fin", data_source=DS)
        stored = await pair.full.storage.get_model("fin", data_source=DS)
        assert stored is not None
        assert [j.target_model for j in stored.joins] == ["hr"]

    async def test_dangling_join_is_kept(self, pair: Pair) -> None:
        pruned = await pair.fin.get_model("dangling", data_source=DS)
        full = await pair.everything.get_model("dangling", data_source=DS)
        assert pruned is not None
        assert pruned == full
        assert [j.target_model for j in pruned.joins] == ["ghost"]

    async def test_dangling_join_inspects_as_for_bypass(self, pair: Pair) -> None:
        def inspect(storage):
            return InspectService(storage=storage).inspect(reference=f"{DS}.dangling", entity_type="model", compact=False)

        out = await inspect(pair.fin)
        assert "ghost" in out
        assert out == await inspect(pair.everything)


class TestSearch:
    async def test_keyword_search_never_names_hr(self, pair: Pair) -> None:
        response = await SearchService(storage=pair.fin).search(
            question="payroll staff roster salary dept department grades ledger", compact=False, max_results=50,
        )
        assert response.results
        for hit in response.results:
            assert leaks(f"{hit.id} {hit.text} {hit.description or ''} {hit.matched_entities}") == [], hit
        assert leaks(" ".join(response.warnings)) == []
        assert all(UNLOADABLE not in w for w in response.warnings)

    async def test_hidden_memory_learning_is_not_searchable(self, pair: Pair) -> None:
        response = await SearchService(storage=pair.fin).search(question=HR_LEARNING, compact=False, max_results=50)
        for hit in response.results:
            assert "ZEBRA" not in hit.text
            assert leaks(f"{hit.id} {hit.matched_entities}") == [], hit
        everything = await SearchService(storage=pair.everything).search(question=HR_LEARNING, compact=False, max_results=50)
        assert any("ZEBRA" in hit.text for hit in everything.results)

    async def test_embedding_search_never_names_hr(self, pair: Pair, monkeypatch: pytest.MonkeyPatch) -> None:
        embedding_client._reset_query_cache()
        monkeypatch.setattr(embedding_client, "is_available", lambda: True)

        async def aligned(*_a, **_kw) -> list[float]:  # NOSONAR(S7503) — stub matches embed_query async signature
            return [1.0, 0.0]

        monkeypatch.setattr(embedding_client, "embed_query", aligned)
        response = await SearchService(storage=pair.fin).search(question="zzzz qqqq", compact=False, max_results=50)
        ids = {hit.id for hit in response.results}
        assert f"{DS}.fin" in ids
        assert [i for i in ids if leaks(i)] == []

    async def test_entity_search_on_visible_model_never_names_hr(self, pair: Pair) -> None:
        response = await SearchService(storage=pair.fin).search(entities=[f"{DS}.fin"], compact=False, max_results=50)
        for hit in response.results:
            assert leaks(f"{hit.id} {hit.text} {hit.matched_entities} {hit.query}") == [], hit
        assert leaks(" ".join(response.warnings)) == []

    async def test_graph_holds_only_visible_models_and_joins(self, pair: Pair) -> None:
        models = await search_graph.get_filtered_ids("MATCH (m:Model) RETURN m.id AS id", pair.fin)
        assert models == {f"{DS}.{n}" for n in VISIBLE_TO_FIN}
        joined = await search_graph.get_filtered_ids("MATCH (a:Model)-[:JOINS]->(b:Model) RETURN b.id AS id", pair.fin)
        assert [i for i in joined if leaks(i)] == []


class TestQueries:
    async def test_visible_model_queries_normally(self, pair: Pair) -> None:
        kind, text = await engine_outcome(pair.fin, {"source_model": "fin", "measures": [{"formula": "sum(amount)", "name": "total"}]})
        assert kind == "ok"
        assert str(LEDGER_TOTAL) in text

    @pytest.mark.parametrize("query", [
        {"source_model": "hr", "measures": [{"formula": "count(*)"}]},
        {"source_model": "hr_summary", "measures": [{"formula": "sum(total)"}]},
        {"source_model": "pay", "measures": [{"formula": "sum(gross)"}]},
        {"source_model": "fin", "dimensions": ["hr.dept"], "measures": [{"formula": "sum(amount)"}]},
        {"source_model": "fin", "measures": [{"formula": "sum(hr.salary)"}]},
        {"source_model": "fin", "measures": [{"formula": "sum(amount)"}], "filters": ["hr.dept = 'eng'"]},
        {"source_model": {"source_name": "fin", "joins": [{"target_model": "hr", "join_pairs": [["staff_id", "id"]]}]},
         "dimensions": ["hr.dept"]},
        {"source_model": "fin", "dimensions": ["dept"], "measures": [{"formula": "sum(amount)"}]},
        {"source_model": "fin", "dimensions": ["hrr.dept"], "measures": [{"formula": "sum(amount)"}]},
        {"source_model": "pub_link", "dimensions": ["salary"]},
        {"dimensions": ["hr.dept"]},
        {"dimensions": ["fin.id", "hr.dept"]},
    ], ids=[
        "root", "query-backed-root", "reader-root", "hop", "hop-measure", "hop-filter", "join-target",
        "short-form", "typo", "pruned-neighbour", "rootless", "rootless-hop",
    ])
    async def test_hidden_model_fails_like_a_deleted_one(self, pair: Pair, query: dict) -> None:
        assert await engine_outcome(pair.fin, query) == await engine_outcome(pair.reference.storage, query)


class TestRootInference:
    async def test_population_inference_picks_visible_root(self, pair: Pair) -> None:
        choice = await infer_population(query=SlayerQuery.model_validate({"dimensions": ["fin.staff_id"]}), storage=pair.fin)
        assert choice.model_name == "fin"

    @pytest.mark.parametrize("dimensions", [["hr.dept"], ["fin.staff_id", "hr.dept"]])
    async def test_population_inference_through_hidden_model_fails_like_deleted(
        self, pair: Pair, dimensions: list[str],
    ) -> None:
        def infer(storage):
            return infer_population(query=SlayerQuery.model_validate({"dimensions": dimensions}), storage=storage)

        assert await outcome(infer(pair.fin)) == await outcome(infer(pair.reference.storage))

    async def test_recommend_root_never_names_hr(self, pair: Pair) -> None:
        engine = SlayerQueryEngine(storage=pair.fin)
        try:
            rec = await engine.recommend_root_model(items=["fin.amount", "pub.label"])
        finally:
            engine.close()
        assert leaks(rec.model_dump_json()) == []

    @pytest.mark.parametrize("items", [["fin.amount", "hr.dept"], ["hr.salary"], ["dept"]])
    async def test_recommend_through_hidden_model_fails_like_deleted(self, pair: Pair, items: list[str]) -> None:
        async def recommend(storage):
            engine = SlayerQueryEngine(storage=storage)
            try:
                return await engine.recommend_root_model(items=items)
            finally:
                engine.close()

        assert await outcome(recommend(pair.fin)) == await outcome(recommend(pair.reference.storage))


class TestFacadeCatalog:
    async def test_catalog_lists_only_visible_models(self, pair: Pair) -> None:
        engine = SlayerQueryEngine(storage=pair.fin)
        try:
            conn = PgConnection(asyncio.StreamReader(), cast(asyncio.StreamWriter, None), engine=engine, storage=pair.fin)
            catalog = await conn._build_catalog()
        finally:
            engine.close()
        tables = {t.name for s in catalog.schemas for t in s.tables}
        assert "fin" in tables
        assert leaks(catalog.model_dump_json()) == []


class TestValidateModels:
    async def test_view_reports_no_hidden_drift(self, pair: Pair) -> None:
        engine = SlayerQueryEngine(storage=pair.fin)
        try:
            entries = await engine.validate_models(data_source=DS)
        finally:
            engine.close()
        dumped = json.dumps([e.model_dump(mode="json") for e in entries])
        assert leaks(dumped) == []
        assert UNLOADABLE not in dumped
        assert "ghost" in dumped


class TestBareNameAmbiguity:
    @pytest.fixture
    async def two_datasources(self) -> AsyncGenerator[YAMLStorage]:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            for ds in ("east", "west"):
                await storage.save_datasource(DatasourceConfig(name=ds, type="sqlite", database=os.path.join(tmp, "x.db")))
            for ds, tags in (("east", []), ("west", ["hr"])):
                await storage.save_model(SlayerModel(
                    name="shared", sql_table="t", data_source=ds, access_tags=tags,
                    columns=[Column(name="id", type=DataType.INT, primary_key=True)],
                ), _validate=False)
            yield storage

    async def test_name_resolves_to_the_visible_copy(self, two_datasources: YAMLStorage) -> None:
        with pytest.raises(AmbiguousModelError):
            await two_datasources.resolve_model_identity("shared")
        fin = TagFilteredStorage(two_datasources, tags={"fin"}, bypass=False)
        assert await fin.resolve_model_identity("shared") == ("east", "shared")
        model = await fin.get_model("shared")
        assert model is not None
        assert model.data_source == "east"
