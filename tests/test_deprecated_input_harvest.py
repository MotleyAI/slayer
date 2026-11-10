"""Every run that consumes converted input reports each payload once on ``SlayerResponse.warnings``, never re-warns.

Spec: compatibility/removal-tripwire › Deprecated input forms convert with a structured warning.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from mcp.server.fastmcp.exceptions import ToolError

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.errors import SlayerError
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from tests._deprecated_input_helpers import (
    CLEAN,
    CM,
    SAVED,
    deprecated_in,
    deprecations,
    dumped,
    mcp_payload,
    orders_engine,
    payload_of,
    recorded,
    strict,
)

STAGE_A = {"name": "a", "source_model": "orders", "dimensions": ["status"], "measures": [{"formula": "count(*)", "name": "n"}]}
STAGE_B = {"name": "b", "source_model": "a", "measures": [{"formula": "sum(n)", "name": "total"}]}
ROOT_ON_A = {"source_model": "a", "measures": [{"formula": "sum(n)", "name": "grand"}]}
ROOT_ON_B = {"source_model": "b", "measures": [{"formula": "sum(total)", "name": "grand"}]}

#: (stages, deprecated_input warnings the run reports)
STAGED = {
    "root_only": ([STAGE_A, strict(ROOT_ON_A)], 1),
    "one_named_stage": ([strict(STAGE_A), ROOT_ON_A], 1),
    "two_named_stages": ([strict(STAGE_A), strict(STAGE_B), ROOT_ON_B], 2),
    "stage_and_root": ([strict(STAGE_A), strict(ROOT_ON_A)], 2),
    "none": ([STAGE_A, ROOT_ON_A], 0),
}


@pytest.fixture
async def engine() -> AsyncGenerator[SlayerQueryEngine]:
    async with orders_engine() as e:
        yield e


def _built(body: dict[str, Any]) -> tuple[SlayerQuery, Any]:
    with recorded() as caught:
        query = SlayerQuery.model_validate(body)
    [w] = deprecations(caught)
    return query, payload_of(w)


class TestEngine:
    async def test_built_once_run_three_times(self, engine: SlayerQueryEngine) -> None:
        query, payload = _built(strict(CLEAN))
        for _ in range(3):
            with recorded() as caught:
                response = await engine.execute(query)
            assert deprecated_in(response.warnings) == [payload]
            assert deprecations(caught) == []

    @pytest.mark.parametrize("path", ["dry_run", "explain"])
    async def test_every_execution_path_reports(self, engine: SlayerQueryEngine, path: str) -> None:
        query, payload = _built(strict(CLEAN))
        response = await engine.execute(query, dry_run=path == "dry_run", explain=path == "explain")
        assert deprecated_in(response.warnings) == [payload]

    @pytest.mark.parametrize("case", sorted(STAGED))
    async def test_each_query_object_reports_its_own_payload(self, engine: SlayerQueryEngine, case: str) -> None:
        stages, expected = STAGED[case]
        with recorded():
            response = await engine.execute(stages)
        assert len(deprecated_in(response.warnings)) == expected

    async def test_prebuilt_stage_in_a_list_of_mappings(self, engine: SlayerQueryEngine) -> None:
        stage, payload = _built(strict(STAGE_A))
        response = await engine.execute([stage, ROOT_ON_A])
        assert deprecated_in(response.warnings) == [payload]

    async def test_runtime_variables_keep_the_report(self, engine: SlayerQueryEngine) -> None:
        query, payload = _built(strict({**CLEAN, "filters": ["status = '{s}'"]}))
        response = await engine.execute(query, variables={"s": "ok"})
        assert deprecated_in(response.warnings) == [payload]


class TestRefinement:
    async def test_converted_refinement_is_reported(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            response = await engine.execute(SAVED, refine={"strict": True})
        [payload] = [payload_of(w) for w in deprecations(caught)]
        assert deprecated_in(response.warnings) == [payload]

    async def test_refinement_runs_under_error_mode(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(SlayerError) as strict_err, recorded():
            await engine.execute(SAVED, refine={"strict": True, "measures": [CM]})
        with pytest.raises(SlayerError) as error_err:
            await engine.execute(SAVED, refine={"to_many_handling": "error", "measures": [CM]})
        assert str(strict_err.value) == str(error_err.value)

    async def test_a_dropped_strict_false_is_still_reported(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            response = await engine.execute(SAVED, refine={"strict": False})
        assert len(deprecated_in(response.warnings)) == 1

    async def test_reused_refinement_reports_on_every_run(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            refinement = QueryRefinement.model_validate({"strict": True})
        [payload] = [payload_of(w) for w in deprecations(caught)]
        for _ in range(3):
            with recorded() as caught:
                response = await engine.execute(SAVED, refine=refinement)
            assert deprecated_in(response.warnings) == [payload]
            assert deprecations(caught) == []

    def test_rest_run_by_name(self, engine: SlayerQueryEngine) -> None:
        client = TestClient(create_app(storage=engine.storage))
        with recorded():
            resp = client.post("/query", json={"name": SAVED, "refine": {"strict": True}})
            assert resp.status_code == 200, resp.text
            assert len(deprecated_in(resp.json()["warnings"])) == 1
            failed = client.post("/query", json={"name": SAVED, "refine": {"strict": True, "measures": [CM]}})
        reference = client.post("/query", json={"name": SAVED, "refine": {"to_many_handling": "error", "measures": [CM]}})
        assert reference.status_code == 400
        assert (failed.status_code, failed.json()) == (reference.status_code, reference.json())

    async def test_mcp_run_by_name(self, engine: SlayerQueryEngine) -> None:
        server = create_mcp_server(storage=engine.storage)

        async def call(refine: dict[str, Any]) -> Any:
            return await server.call_tool(name="query", arguments={"query": SAVED, "refine": refine, "format": "json"})

        with recorded():
            blocks, _ = await call({"strict": True})
            assert len(deprecated_in(mcp_payload(getattr(next(iter(blocks)), "text")).get("warnings"))) == 1
            with pytest.raises(ToolError) as failed:
                await call({"strict": True, "measures": [CM]})
        with pytest.raises(ToolError) as reference:
            await call({"to_many_handling": "error", "measures": [CM]})
        assert str(failed.value) == str(reference.value)


class TestSurfaces:
    @pytest.mark.parametrize("case", sorted(STAGED))
    def test_rest_list_of_stages(self, engine: SlayerQueryEngine, case: str) -> None:
        stages, expected = STAGED[case]
        client = TestClient(create_app(storage=engine.storage))
        with recorded():
            resp = client.post("/query", json={"queries": stages})
        assert resp.status_code == 200, resp.text
        assert len(deprecated_in(resp.json()["warnings"])) == expected

    @pytest.mark.parametrize("case", sorted(STAGED))
    async def test_mcp_list_of_stages(self, engine: SlayerQueryEngine, case: str) -> None:
        stages, expected = STAGED[case]
        server = create_mcp_server(storage=engine.storage)
        with recorded():
            blocks, _ = await server.call_tool(name="query", arguments={"query": stages, "format": "json"})
        assert len(deprecated_in(mcp_payload(getattr(next(iter(blocks)), "text")).get("warnings"))) == expected


def _http_client(engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch) -> SlayerClient:
    """A remote ``SlayerClient`` whose sync and async transports post to an in-process app."""
    server = TestClient(create_app(storage=engine.storage))

    def via_app(*, method: str, path: str, json: dict | None = None, params: dict | None = None) -> Any:
        resp = server.request(method, path, json=json, params=params)
        resp.raise_for_status()
        return resp.json()

    async def via_app_async(*, method: str, path: str, json: dict | None = None, params: dict | None = None) -> Any:  # NOSONAR(S7503) — matches the awaited transport
        return via_app(method=method, path=path, json=json, params=params)

    client = SlayerClient(url="http://testserver")
    monkeypatch.setattr(client, "_request_sync", via_app)
    monkeypatch.setattr(client, "_request", via_app_async)
    return client


class TestHttpClient:
    """The client converts locally, so the server never sees ``strict``; the client reports what it converted."""

    @pytest.fixture(params=["sync", "async"])
    def run(self, request, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch):
        client = _http_client(engine, monkeypatch)

        async def _run(query: Any, **kwargs: Any) -> Any:
            if request.param == "sync":
                return client.query_sync(query, **kwargs)
            return await client.query(query, **kwargs)

        return _run

    async def test_mapping(self, run) -> None:
        with recorded():
            response = await run(strict(CLEAN))
        assert len(deprecated_in(response.warnings)) == 1

    @pytest.mark.parametrize("case", sorted(STAGED))
    async def test_list_of_stages(self, run, case: str) -> None:
        stages, expected = STAGED[case]
        with recorded():
            response = await run(stages)
        assert len(deprecated_in(response.warnings)) == expected

    async def test_prebuilt_query(self, run) -> None:
        query, payload = _built(strict(CLEAN))
        response = await run(query)
        assert [dumped(w) for w in deprecated_in(response.warnings)] == [dumped(payload)]

    async def test_prebuilt_query_run_twice(self, run) -> None:
        query, payload = _built(strict(CLEAN))
        for _ in range(2):
            response = await run(query)
            assert [dumped(w) for w in deprecated_in(response.warnings)] == [dumped(payload)]

    @pytest.mark.parametrize("refine", [{"strict": True}, {"strict": False}], ids=["true", "false"])
    async def test_refinement_mapping(self, run, refine: dict[str, Any]) -> None:
        with recorded():
            response = await run(SAVED, refine=refine)
        assert len(deprecated_in(response.warnings)) == 1

    async def test_prebuilt_refinement(self, run) -> None:
        with recorded() as caught:
            refinement = QueryRefinement.model_validate({"strict": True})
        [payload] = [payload_of(w) for w in deprecations(caught)]
        response = await run(SAVED, refine=refinement)
        assert [dumped(w) for w in deprecated_in(response.warnings)] == [dumped(payload)]
