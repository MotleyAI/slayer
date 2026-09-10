"""DEV-1841 task 1.3 — the REST query request and the MCP query tool swap
``strict`` for ``to_many_handling``: the mode reaches the engine and governs
resolution, and ``strict`` is rejected on both surfaces.

Spec: openspec …/specs/queries/attribution-modes — "Surfaces accept the mode
parameter", "Strict input is rejected with the remedy".
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest
from fastapi.testclient import TestClient
from mcp.server.fastmcp.exceptions import ToolError

from slayer.api.server import QueryRequest, create_app
from slayer.mcp.server import create_mcp_server

from tests._dev1840_fixtures import _engine_for, _seed_sqlite, dev1840_models

_ASSOCIATE_BODY = {
    "source_model": "orders",
    "dimensions": ["status"],
    "measures": [{"formula": "customers.spend:sum", "name": "cm"}],
    "to_many_handling": "associate",
}


@pytest.fixture
async def storage() -> AsyncIterator[object]:
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "data.sqlite")
        _seed_sqlite(db_path)
        engine = await _engine_for(
            dialect="sqlite", db_path=db_path, models=dev1840_models())
        yield engine.storage


def test_query_request_swaps_strict_for_mode() -> None:
    assert "to_many_handling" in QueryRequest.model_fields
    assert "strict" not in QueryRequest.model_fields


class TestRestSurface:
    async def test_mode_reaches_engine(self, storage) -> None:
        """Scenario: surfaces accept the mode parameter — associate resolves
        per-cell through the REST path exactly as a direct field would."""
        client = TestClient(create_app(storage=storage))
        resp = client.post("/query", json=_ASSOCIATE_BODY)
        assert resp.status_code == 200, resp.text
        by = {r["orders.status"]: float(r["orders.cm"]) for r in resp.json()["data"]}
        assert by["ok"] == pytest.approx(420.0)
        assert by["new"] == pytest.approx(290.0)

    async def test_strict_rejected(self, storage) -> None:
        """Scenario: strict input is rejected on the REST surface."""
        client = TestClient(create_app(storage=storage))
        body = {"source_model": "orders", "measures": [{"formula": "*:count"}],
                "strict": True}
        resp = client.post("/query", json=body)
        assert resp.status_code != 200
        assert "to_many_handling" in resp.text

    async def test_association_slayer_error_maps_to_400(self, storage) -> None:
        """An association-eligibility ``SlayerError`` (here a column-reference param) is a client error (400), not a 500."""
        client = TestClient(create_app(storage=storage))
        body = {"source_model": "orders", "dimensions": ["status"],
                "measures": [{"formula":
                              "customers.spend:weighted_avg(weight=customers.spend)"}],
                "to_many_handling": "associate"}
        resp = client.post("/query", json=body)
        assert resp.status_code == 400, resp.text


class TestMcpSurface:
    async def test_mode_reaches_engine(self, storage) -> None:
        """The mode rides inside the unified ``query`` object (DEV-1858 tool shape)."""
        server = create_mcp_server(storage=storage)
        blocks, _ = await server.call_tool(name="query", arguments={
            "query": _ASSOCIATE_BODY, "format": "json"})
        text = blocks[0].text
        assert "420" in text
        assert "290" in text
        assert "460" not in text  # not the broadcast total

    async def test_strict_rejected(self, storage) -> None:
        server = create_mcp_server(storage=storage)
        with pytest.raises((ToolError, ValueError)):
            await server.call_tool(name="query", arguments={"query": {
                "source_model": "orders",
                "measures": [{"formula": "*:count"}],
                "strict": True}})
