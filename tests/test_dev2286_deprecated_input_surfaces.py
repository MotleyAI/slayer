"""Deprecated-input reports on REST ``strict: null``, two forms in one query, MCP dry runs and MCP model saves.

Spec: compatibility/removal-tripwire › Deprecated input forms convert with a structured warning;
mcp/query-tool › Execution wrappers and output shaping; mcp/response-row-cap › Explain plan rows capped without a limit.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import QueryRequest, create_app
from slayer.core.query import SlayerQuery
from slayer.storage.migrations import CURRENT_VERSIONS, migrating_document
from tests._deprecated_input_helpers import (
    CLEAN,
    CROSS,
    SAVED,
    deprecated_in,
    deprecations,
    orders_engine,
    payload_of,
    recorded,
    rows,
    strict,
)
from tests._library_names_fixtures import agg, measure, stored_engine, t_doc
from tests._model_access_fixtures import mcp_text

EXTENSION_OLD = "An extension object in `source_model`"
LEGACY_CLEAN = {"source_model": {"source_name": "orders"}, "measures": ["count(*)"]}
TWO_FORMS = strict(LEGACY_CLEAN)


def _strict_message() -> str:
    with recorded():
        [payload] = SlayerQuery.model_validate(strict(CLEAN)).deprecated_inputs
    return payload.human_message()


class TestRestExplicitNullStrict:
    async def test_runs_unchanged_and_reports_once(self) -> None:
        async with orders_engine() as engine:
            client = TestClient(create_app(storage=engine.storage))
            plain = client.post("/query", json=CROSS)
            with recorded() as caught:
                nulled = client.post("/query", json=strict(CROSS, None))
        assert plain.status_code == 200, plain.text
        assert nulled.status_code == 200, nulled.text
        assert rows(nulled.json()["data"]) == rows(plain.json()["data"])
        [raised] = deprecations(caught)
        assert (payload_of(raised).old, payload_of(raised).removal) == ("strict", "3.0")
        [w] = deprecated_in(nulled.json()["warnings"])
        assert (w["old"], w["removal"]) == ("strict", "3.0")

    @pytest.mark.parametrize("value", [None, False], ids=["null", "false"])
    async def test_run_by_name_refuses_it(self, value: Any) -> None:
        async with orders_engine() as engine:
            client = TestClient(create_app(storage=engine.storage))
            with recorded():
                resp = client.post("/query", json={"name": SAVED, "strict": value})
        assert resp.status_code == 400, resp.text


class TestTwoDeprecatedForms:
    async def test_in_process(self) -> None:
        async with orders_engine() as engine:
            with recorded() as caught:
                response = await engine.execute(TWO_FORMS)
        assert sorted(payload_of(w).old for w in deprecations(caught)) == [EXTENSION_OLD, "strict"]
        assert sorted(w.old for w in deprecated_in(response.warnings)) == [EXTENSION_OLD, "strict"]

    async def test_rest(self) -> None:
        async with orders_engine() as engine:
            client = TestClient(create_app(storage=engine.storage))
            with recorded() as caught:
                resp = client.post("/query", json=TWO_FORMS)
        assert resp.status_code == 200, resp.text
        assert sorted(payload_of(w).old for w in deprecations(caught)) == [EXTENSION_OLD, "strict"]
        assert sorted(w["old"] for w in deprecated_in(resp.json()["warnings"])) == [EXTENSION_OLD, "strict"]


_FRESH_VERSIONS = {
    "no_version": {},
    "current_version": {"version": CURRENT_VERSIONS["SlayerQuery"]},
    "malformed_version": {"version": 3.9},
}


class TestFreshVersusStoredBoundary:
    @pytest.mark.parametrize("version", list(_FRESH_VERSIONS.values()), ids=list(_FRESH_VERSIONS))
    def test_fresh_strict_converts_in_the_query(self, version: dict[str, Any]) -> None:
        with recorded() as caught:
            q = SlayerQuery.model_validate({**strict(CLEAN), **version})
        assert q.to_many_handling == "error"
        assert [p.old for p in q.deprecated_inputs] == ["strict"]
        assert len(deprecations(caught)) == 1

    @pytest.mark.parametrize("version", list(_FRESH_VERSIONS.values()), ids=list(_FRESH_VERSIONS))
    def test_fresh_strict_converts_in_the_request(self, version: dict[str, Any]) -> None:
        with recorded() as caught:
            request = QueryRequest.model_validate({**strict(CLEAN), **version})
        assert request.to_many_handling == "error"
        assert "strict" not in (request.model_extra or {})
        assert [p.old for p in request.deprecated_inputs] == ["strict"]
        assert len(deprecations(caught)) == 1

    @pytest.mark.parametrize("model", [SlayerQuery, QueryRequest], ids=["query", "request"])
    def test_stored_v3_strict_upgrades_silently(self, model: type[SlayerQuery] | type[QueryRequest]) -> None:
        with recorded() as caught:
            built = model.model_validate({**strict(CLEAN), "version": 3})
        assert built.to_many_handling == "error"
        assert built.deprecated_inputs == ()
        assert [w for w in caught if issubclass(w.category, FutureWarning)] == []

    @pytest.mark.parametrize("model", [SlayerQuery, QueryRequest], ids=["query", "request"])
    def test_root_extension_in_a_stored_document_converts_silently(
        self, model: type[SlayerQuery] | type[QueryRequest],
    ) -> None:
        with recorded() as caught, migrating_document("doc", stored=True):
            built = model.model_validate(LEGACY_CLEAN)
        assert [e.source_name for e in built.extensions or []] == ["orders"]
        assert built.deprecated_inputs == ()
        assert deprecations(caught) == []


async def _dry_run(storage: Any, query: Any, fmt: str) -> str:
    with recorded():
        return await mcp_text(storage, "query", query=query, dry_run=True, format=fmt)


class TestDryRun:
    async def test_rest_reports_the_conversion(self) -> None:
        async with orders_engine() as engine:
            client = TestClient(create_app(storage=engine.storage))
            with recorded():
                resp = client.post("/query", json={**strict(CLEAN), "dry_run": True})
        assert resp.status_code == 200, resp.text
        assert "SELECT" in (resp.json()["sql"] or "")
        assert len(deprecated_in(resp.json()["warnings"])) == 1

    async def test_mcp_json(self) -> None:
        async with orders_engine() as engine:
            text = await _dry_run(engine.storage, strict(CLEAN), "json")
        reply = json.loads(text)
        assert isinstance(reply, dict)
        assert "SELECT" in reply["sql"]
        [w] = deprecated_in(reply["warnings"])
        assert (w["old"], w["removal"]) == ("strict", "3.0")

    async def test_mcp_csv(self) -> None:
        async with orders_engine() as engine:
            text = await _dry_run(engine.storage, strict(CLEAN), "csv")
        lines = text.splitlines()
        assert lines[0] == f"# warning: {_strict_message()}"
        assert lines[1] == "SQL:"
        assert "SELECT" in text

    async def test_mcp_markdown(self) -> None:
        async with orders_engine() as engine:
            text = await _dry_run(engine.storage, strict(CLEAN), "markdown")
        assert text.startswith("SQL:\n")
        sql, _, warnings_block = text.partition("\n\nWarnings:\n")
        assert "SELECT" in sql
        assert warnings_block.splitlines() == [f"  - {_strict_message()}"]

    async def test_mcp_json_carries_a_library_name_shadow(self) -> None:
        query = {"source_model": "t", "measures": [measure("reverse(a)")]}
        async with stored_engine(models=[t_doc(aggregations=[agg("reverse")])]) as engine:
            text = await _dry_run(engine.storage, query, "json")
        shadows = [w for w in json.loads(text)["warnings"] if w.get("kind") == "library_name_shadow"]
        assert [(w["model"], w["aggregation"], w["function"]) for w in shadows] == [("t", "reverse", "reverse")]

    async def test_mcp_json_without_warnings_is_sql_only(self) -> None:
        async with orders_engine() as engine:
            text = await _dry_run(engine.storage, CLEAN, "json")
        reply = json.loads(text)
        assert list(reply) == ["sql"]
        assert "SELECT" in reply["sql"]


class TestMcpModelSaveMessage:
    async def test_create_model(self) -> None:
        async with orders_engine() as engine:
            with recorded():
                text = await mcp_text(engine.storage, "create_model", name="saved_strict", query=strict(CLEAN))
        assert text.count(_strict_message()) == 1

    async def test_edit_model(self) -> None:
        async with orders_engine() as engine:
            with recorded():
                await mcp_text(engine.storage, "create_model", name="saved_strict", query=CLEAN)
                reply = json.loads(await mcp_text(
                    engine.storage, "edit_model", model_name="saved_strict", source_queries=[strict(CLEAN)],
                ))
        assert sum(w.count(_strict_message()) for w in reply["warnings"]) == 1
