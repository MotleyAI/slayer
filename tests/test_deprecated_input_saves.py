"""Saves report a converted ``strict`` on their response and persist only ``to_many_handling``; REST model saves
also return narrowed-access warnings.

Spec: compatibility/removal-tripwire › Deprecated input forms convert with a structured warning;
models/model-access › Narrowed access is reported.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.models import SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.access_filtered import AccessFilteredStorage
from slayer.storage.sqlite_conn import transaction
from tests._deprecated_input_helpers import deprecated_in, deprecations, recorded, strict
from tests._model_access_fixtures import ACCESS_PARAMS, DS, AccessStore, access_store, mcp_text, must_get

STRICT_WORD = re.compile(r"(?<![\w])strict(?![\w])")
PUB_COUNT = {"source_model": "pub", "measures": [{"formula": "count(*)", "name": "v"}]}
ROOTLESS = {"dimensions": ["pub.label"], "measures": [{"formula": "count(pub.id)", "name": "n"}]}
STAGE = {"name": "s", "source_model": "pub", "dimensions": ["label"], "measures": [{"formula": "count(*)", "name": "n"}]}
ON_STAGE = {"source_model": "s", "measures": [{"formula": "sum(n)", "name": "total"}]}
HR_READER = {"source_model": "hr", "measures": [{"formula": "sum(salary)", "name": "v"}]}

#: (source_queries, deprecated_input warnings the save reports)
SAVED_QUERIES = {
    "rooted": ([strict(PUB_COUNT)], 1),
    "rootless": ([strict(ROOTLESS)], 1),
    "staged_one": ([strict(STAGE), ON_STAGE], 1),
    "staged_two": ([strict(STAGE), strict(ON_STAGE)], 2),
}


@pytest.fixture(params=ACCESS_PARAMS)
async def store(request) -> AsyncGenerator[AccessStore]:
    backend, dialect = request.param
    async with access_store(backend=backend, dialect=dialect, unloadable=False) as s:
        yield s


def stored_documents(store: AccessStore) -> str:
    """Every stored model and memory document, as raw text."""
    if store.backend == "yaml":
        texts = []
        for root, _, files in os.walk(os.path.join(store.base, "store")):
            for name in files:
                if name.endswith((".yaml", ".yml", ".md")):
                    with open(os.path.join(root, name), encoding="utf-8") as f:  # NOSONAR(S7493) — test read
                        texts.append(f.read())
        return "\n".join(texts)
    with transaction(os.path.join(store.base, "store.db")) as conn:
        rows = conn.execute("SELECT data FROM models UNION ALL SELECT data FROM memories").fetchall()
    return "\n".join(r[0] for r in rows)


async def assert_persisted_as_error_mode(store: AccessStore, name: str) -> None:
    model = await must_get(store.storage, name)
    assert model.source_queries
    assert any(q.to_many_handling == "error" for q in model.source_queries)
    assert not STRICT_WORD.search(stored_documents(store))


def mentions_strict_deprecation(text: str) -> bool:
    return bool(STRICT_WORD.search(text)) and "to_many_handling" in text


class TestEngineSave:
    @pytest.mark.parametrize("case", sorted(SAVED_QUERIES))
    async def test_warnings_out_carries_each_payload(self, store: AccessStore, case: str) -> None:
        stages, expected = SAVED_QUERIES[case]
        engine = SlayerQueryEngine(storage=store.storage)
        try:
            with recorded():
                model = SlayerModel.model_validate({"name": "saved", "source_queries": stages})
            notes: list = []
            await engine.save_model(model, warnings_out=notes)
        finally:
            engine.close()
        assert len(deprecated_in(notes)) == expected
        await assert_persisted_as_error_mode(store, "saved")

    async def test_access_filtered_storage(self, store: AccessStore) -> None:
        engine = SlayerQueryEngine(storage=AccessFilteredStorage(store.storage, tags={"fin"}, bypass=False))
        try:
            with recorded():
                model = SlayerModel.model_validate({"name": "saved", "source_queries": [strict(PUB_COUNT)]})
            notes: list = []
            await engine.save_model(model, warnings_out=notes)
        finally:
            engine.close()
        assert len(deprecated_in(notes)) == 1
        await assert_persisted_as_error_mode(store, "saved")

    async def test_create_model_from_query(self, store: AccessStore) -> None:
        engine = SlayerQueryEngine(storage=store.storage)
        notes: list = []
        try:
            with recorded():
                await engine.create_model_from_query(query=strict(PUB_COUNT), name="saved", warnings_out=notes)
        finally:
            engine.close()
        assert len(deprecated_in(notes)) == 1
        await assert_persisted_as_error_mode(store, "saved")


class TestRestModelSave:
    @pytest.mark.parametrize("case", sorted(SAVED_QUERIES))
    async def test_post_returns_the_deprecation(self, store: AccessStore, case: str) -> None:
        stages, expected = SAVED_QUERIES[case]
        client = TestClient(create_app(storage=store.storage))
        with recorded():
            resp = client.post("/models", json={"name": "saved", "source_queries": stages})
        assert resp.status_code == 200, resp.text
        assert len(deprecated_in(resp.json()["warnings"])) == expected
        await assert_persisted_as_error_mode(store, "saved")

    async def test_put_returns_the_deprecation(self, store: AccessStore) -> None:
        client = TestClient(create_app(storage=store.storage))
        assert client.post("/models", json={"name": "saved", "source_queries": [PUB_COUNT]}).status_code == 200
        with recorded():
            resp = client.put("/models/saved", json={"name": "saved", "source_queries": [strict(PUB_COUNT)]})
        assert resp.status_code == 200, resp.text
        [w] = deprecated_in(resp.json()["warnings"])
        assert "to_many_handling" in w["new"]
        await assert_persisted_as_error_mode(store, "saved")

    async def test_clean_save_returns_no_warnings(self, store: AccessStore) -> None:
        client = TestClient(create_app(storage=store.storage))
        resp = client.post("/models", json={"name": "saved", "source_queries": [PUB_COUNT]})
        assert resp.status_code == 200, resp.text
        assert resp.json()["warnings"] == []

    @pytest.mark.parametrize("method", ["post", "put"])
    async def test_narrowed_access_is_returned(self, store: AccessStore, method: str) -> None:
        """Scenario: a REST model save returns the warning."""
        client = TestClient(create_app(storage=store.storage))
        body = {"name": "pay_report", "source_queries": [HR_READER]}
        with recorded():
            resp = client.post("/models", json=body) if method == "post" else client.put("/models/pay_report", json=body)
        assert resp.status_code == 200, resp.text
        [w] = [w for w in resp.json()["warnings"] if w.get("kind") == "narrowed_access"]
        assert "hr" in w["narrowed_by"]


class TestMcpModelSave:
    async def test_create_model_lists_the_deprecation(self, store: AccessStore) -> None:
        with recorded():
            text = await mcp_text(store.storage, "create_model", name="saved", query=strict(PUB_COUNT))
        assert mentions_strict_deprecation(text)
        await assert_persisted_as_error_mode(store, "saved")

    async def test_create_model_from_stages_lists_each_deprecation(self, store: AccessStore) -> None:
        with recorded():
            text = await mcp_text(store.storage, "create_model", name="saved", query=[strict(STAGE), strict(ON_STAGE)])
        assert len(STRICT_WORD.findall(text)) >= 2
        await assert_persisted_as_error_mode(store, "saved")

    async def test_edit_model_lists_the_deprecation(self, store: AccessStore) -> None:
        with recorded():
            await mcp_text(store.storage, "create_model", name="saved", query=PUB_COUNT)
            reply = json.loads(await mcp_text(
                store.storage, "edit_model", model_name="saved", source_queries=[strict(PUB_COUNT)],
            ))
        assert any(mentions_strict_deprecation(w) for w in reply.get("warnings", []))
        await assert_persisted_as_error_mode(store, "saved")


class TestMemorySave:
    @staticmethod
    async def _stored_query_mode(store: AccessStore, memory_id: str) -> str:
        memory = await store.storage.get_memory(memory_id)
        assert memory.query is not None
        return memory.query.to_many_handling

    async def test_mcp(self, store: AccessStore) -> None:
        with recorded():
            reply = json.loads(await mcp_text(
                store.storage, "save_memory", learning="Item count", linked_entities=strict(PUB_COUNT),
            ))
        assert any(mentions_strict_deprecation(w) for w in reply["warnings"])
        assert await self._stored_query_mode(store, reply["memory_id"]) == "error"
        assert not STRICT_WORD.search(stored_documents(store))

    async def test_rest(self, store: AccessStore) -> None:
        client = TestClient(create_app(storage=store.storage))
        with recorded():
            resp = client.post("/memories", json={"learning": "Item count", "linked_entities": strict(PUB_COUNT)})
        assert resp.status_code == 200, resp.text
        reply: dict[str, Any] = resp.json()
        assert any(mentions_strict_deprecation(w) for w in reply["warnings"])
        assert await self._stored_query_mode(store, reply["memory_id"]) == "error"
        assert not STRICT_WORD.search(stored_documents(store))

    async def test_clean_query_adds_no_deprecation(self, store: AccessStore) -> None:
        reply = json.loads(await mcp_text(store.storage, "save_memory", learning="Item count", linked_entities=PUB_COUNT))
        assert not any(STRICT_WORD.search(w) for w in reply["warnings"])


async def test_reload_keeps_no_strict(store: AccessStore) -> None:
    """A saved model reloaded from storage carries only the replacement form."""
    client = TestClient(create_app(storage=store.storage))
    with recorded():
        assert client.post("/models", json={"name": "saved", "source_queries": [strict(PUB_COUNT)]}).status_code == 200
    with recorded() as caught:
        model = await must_get(store.storage, "saved", DS)
    assert model.source_queries is not None
    assert model.source_queries[0].to_many_handling == "error"
    assert deprecations(caught) == []
