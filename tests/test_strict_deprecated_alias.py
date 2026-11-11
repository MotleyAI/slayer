"""``strict`` is a deprecated alias of ``to_many_handling``: fresh input converts with a deprecation, stored pre-v4
queries migrate silently, an unrecognized token is a typed error.

Spec: queries/attribution-modes › The strict flag is a deprecated alias; mcp/query-tool › Query-object execution.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from mcp.server.fastmcp.exceptions import ToolError

from slayer.api.server import create_app
from slayer.core.errors import SlayerError, UnrecognizedStrictValueError
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage import migrations as mig
from tests._deprecated_input_helpers import (
    CLEAN,
    CROSS,
    SAVED,
    deprecated_in,
    deprecations,
    errors_in,
    mcp_payload,
    mode,
    orders_engine,
    payload_of,
    recorded,
    rows,
    strict,
)

TRUE_TOKENS = [True, "true", "True", " TRUE ", "1", "yes", "y", "t", "on", 1]
FALSE_TOKENS = [False, "false", "0", "no", "n", "f", "off", "", 0]
FRESH_VERSIONS = [4, "4", 3.9]


@pytest.fixture
async def engine() -> AsyncGenerator[SlayerQueryEngine]:
    async with orders_engine() as e:
        yield e


def _q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **kw})


def _typed(exc: BaseException) -> UnrecognizedStrictValueError:
    [typed] = [e for e in errors_in(exc) if isinstance(e, UnrecognizedStrictValueError)]
    assert isinstance(typed, SlayerError)
    assert "to_many_handling" in str(typed)
    return typed


class TestPythonConversion:
    @pytest.mark.parametrize("token", TRUE_TOKENS)
    def test_true_becomes_error_mode(self, token: Any) -> None:
        with recorded() as caught:
            query = _q(strict=token)
        assert query.to_many_handling == "error"
        assert "strict" not in query.model_dump()
        [w] = deprecations(caught)
        assert "to_many_handling" in payload_of(w).new

    @pytest.mark.parametrize("token", FALSE_TOKENS)
    def test_false_is_dropped(self, token: Any) -> None:
        with recorded() as caught:
            query = _q(strict=token)
        assert query.to_many_handling == "broadcast"
        assert "to_many_handling" not in query.model_fields_set
        assert len(deprecations(caught)) == 1

    @pytest.mark.parametrize(("value", "explicit"), [(True, "associate"), (False, "error"), ("on", "broadcast")])
    def test_explicit_mode_wins(self, value: Any, explicit: str) -> None:
        with recorded() as caught:
            query = _q(strict=value, to_many_handling=explicit)
        assert query.to_many_handling == explicit
        assert len(deprecations(caught)) == 1

    @pytest.mark.parametrize("version", FRESH_VERSIONS)
    def test_current_or_malformed_version_is_fresh(self, version: Any) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({"version": version, "source_model": "orders", "strict": True})
        assert query.to_many_handling == "error"
        assert len(deprecations(caught)) == 1

    def test_one_deprecation_per_query_object(self) -> None:
        with recorded() as caught:
            _q(strict=True)
            _q(strict=False)
        assert len(deprecations(caught)) == 2

    @pytest.mark.parametrize("version", [None, 4, 3.9])
    def test_unrecognized_token_is_a_typed_error(self, version: Any) -> None:
        payload = {"source_model": "orders", "strict": "maybe"}
        if version is not None:
            payload["version"] = version
        with pytest.raises(ValueError) as ei:
            SlayerQuery.model_validate(payload)
        _typed(ei.value)


class TestStoredMigration:
    @pytest.mark.parametrize(("value", "expected"), [
        (True, "error"), ("yes", "error"), (1, "error"), (False, "broadcast"), ("off", "broadcast"), (0, "broadcast"),
    ])
    def test_pre_v4_migrates_silently(self, value: Any, expected: str) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({"version": 3, "source_model": "orders", "strict": value})
        assert query.to_many_handling == expected
        assert deprecations(caught) == []

    def test_unrecognized_stored_token_is_a_typed_error(self) -> None:
        with pytest.raises(ValueError) as ei:
            SlayerQuery.model_validate({"version": 3, "source_model": "orders", "strict": "maybe"})
        _typed(ei.value)

    def test_migration_step_raises_the_typed_error(self) -> None:
        with pytest.raises(UnrecognizedStrictValueError):
            mig.migrate(entity="SlayerQuery", data={"version": 3, "source_model": "orders", "strict": "maybe"})

    async def test_migrated_queries_execute_under_their_modes(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            stored_true = SlayerQuery.model_validate({"version": 3, **strict(CROSS, True)})
            stored_false = SlayerQuery.model_validate({"version": 3, **strict(CROSS, False)})
        explicit = mode(CROSS, "error")
        with pytest.raises(SlayerError) as strict_err:
            await engine.execute(stored_true)
        with pytest.raises(SlayerError) as error_err:
            await engine.execute(explicit)
        assert str(strict_err.value) == str(error_err.value)
        assert rows((await engine.execute(stored_false)).data) == rows((await engine.execute(CROSS)).data)
        assert deprecations(caught) == []


class TestRefinementConversion:
    def test_true_becomes_error_mode(self) -> None:
        with recorded() as caught:
            refinement = QueryRefinement.model_validate({"strict": True})
        assert refinement.to_many_handling == "error"
        assert len(deprecations(caught)) == 1

    def test_false_is_dropped(self) -> None:
        with recorded() as caught:
            refinement = QueryRefinement.model_validate({"strict": "off"})
        assert refinement.to_many_handling is None
        assert len(deprecations(caught)) == 1

    def test_explicit_mode_wins(self) -> None:
        with recorded():
            refinement = QueryRefinement.model_validate({"strict": True, "to_many_handling": "associate"})
        assert refinement.to_many_handling == "associate"

    def test_unrecognized_token_is_a_typed_error(self) -> None:
        with pytest.raises(ValueError) as ei:
            QueryRefinement.model_validate({"strict": "maybe"})
        _typed(ei.value)


class TestEngineRuns:
    async def test_true_runs_under_error_mode_and_reports(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            response = await engine.execute(strict(CLEAN))
        [w] = deprecated_in(response.warnings)
        assert "to_many_handling" in w.new
        converted, explicit = strict(CROSS), mode(CROSS, "error")
        with recorded():
            with pytest.raises(SlayerError) as strict_err:
                await engine.execute(converted)
        with pytest.raises(SlayerError) as error_err:
            await engine.execute(explicit)
        assert str(strict_err.value) == str(error_err.value)

    @pytest.mark.parametrize("value", [False, "off"])
    async def test_false_runs_under_the_default(self, engine: SlayerQueryEngine, value: Any) -> None:
        with recorded():
            response = await engine.execute(strict(CROSS, value))
        assert rows(response.data) == rows((await engine.execute(CROSS)).data)
        assert len(deprecated_in(response.warnings)) == 1

    async def test_explicit_mode_wins(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            response = await engine.execute(mode(strict(CROSS), "associate"))
        assert rows(response.data) == rows((await engine.execute(mode(CROSS, "associate"))).data)
        assert len(deprecated_in(response.warnings)) == 1


class TestRestSurface:
    def test_true_runs_under_error_mode_and_reports(self, engine: SlayerQueryEngine) -> None:
        client = TestClient(create_app(storage=engine.storage))
        with recorded():
            resp = client.post("/query", json=strict(CLEAN))
            assert resp.status_code == 200, resp.text
            [w] = deprecated_in(resp.json()["warnings"])
            assert "to_many_handling" in w["new"]
            failed = client.post("/query", json=strict(CROSS))
        reference = client.post("/query", json=mode(CROSS, "error"))
        assert (failed.status_code, failed.json()) == (reference.status_code, reference.json())
        assert reference.status_code == 400

    @pytest.mark.parametrize("value", [False, "off"])
    def test_false_runs_under_the_default(self, engine: SlayerQueryEngine, value: Any) -> None:
        client = TestClient(create_app(storage=engine.storage))
        with recorded():
            resp = client.post("/query", json=strict(CROSS, value))
        assert resp.status_code == 200, resp.text
        assert rows(resp.json()["data"]) == rows(client.post("/query", json=CROSS).json()["data"])
        assert len(deprecated_in(resp.json()["warnings"])) == 1

    def test_explicit_mode_wins(self, engine: SlayerQueryEngine) -> None:
        client = TestClient(create_app(storage=engine.storage))
        with recorded():
            resp = client.post("/query", json=mode(strict(CROSS), "associate"))
        assert resp.status_code == 200, resp.text
        reference = client.post("/query", json=mode(CROSS, "associate"))
        assert rows(resp.json()["data"]) == rows(reference.json()["data"])
        assert len(deprecated_in(resp.json()["warnings"])) == 1

    def test_unrecognized_token_is_a_client_error(self, engine: SlayerQueryEngine) -> None:
        client = TestClient(create_app(storage=engine.storage))
        resp = client.post("/query", json=strict(CLEAN, "maybe"))
        assert resp.status_code in (400, 422)
        assert "to_many_handling" in resp.text

    def test_run_by_name_top_level_strict_stays_rejected(self, engine: SlayerQueryEngine) -> None:
        client = TestClient(create_app(storage=engine.storage))
        rejected = client.post("/query", json={"name": SAVED, "strict": True})
        other_clause = client.post("/query", json={"name": SAVED, "limit": 1})
        assert rejected.status_code == other_clause.status_code == 400
        assert rejected.json() == other_clause.json()


class TestMcpSurface:
    async def _call(self, engine: SlayerQueryEngine, query: Any) -> dict[str, Any]:
        server = create_mcp_server(storage=engine.storage)
        blocks, _ = await server.call_tool(name="query", arguments={"query": query, "format": "json"})
        return mcp_payload(getattr(next(iter(blocks)), "text"))

    async def _error(self, engine: SlayerQueryEngine, query: Any) -> str:
        server = create_mcp_server(storage=engine.storage)
        with pytest.raises(ToolError) as ei:
            await server.call_tool(name="query", arguments={"query": query, "format": "json"})
        return str(ei.value)

    async def test_true_runs_under_error_mode_and_reports(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            payload = await self._call(engine, strict(CLEAN))
            [w] = deprecated_in(payload.get("warnings"))
            assert "to_many_handling" in w["new"]
            failed = await self._error(engine, strict(CROSS))
        assert failed == await self._error(engine, mode(CROSS, "error"))

    async def test_explicit_error_mode_is_honored(self, engine: SlayerQueryEngine) -> None:
        explicit = mode(CROSS, "error")
        with pytest.raises(SlayerError) as direct:
            await engine.execute(explicit)
        assert str(direct.value) in await self._error(engine, explicit)

    @pytest.mark.parametrize("value", [False, "off"])
    async def test_false_runs_under_the_default(self, engine: SlayerQueryEngine, value: Any) -> None:
        with recorded():
            payload = await self._call(engine, strict(CROSS, value))
        assert rows(payload["data"]) == rows((await self._call(engine, CROSS))["data"])
        assert len(deprecated_in(payload.get("warnings"))) == 1

    async def test_explicit_mode_wins(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            payload = await self._call(engine, mode(strict(CROSS), "associate"))
        reference = await self._call(engine, mode(CROSS, "associate"))
        assert rows(payload["data"]) == rows(reference["data"])
        assert len(deprecated_in(payload.get("warnings"))) == 1
