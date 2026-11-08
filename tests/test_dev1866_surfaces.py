"""DEV-1866 — a rootless query reaches every query surface (REST + MCP).

Both surfaces must accept a body/call that omits ``source_model``, run inference
end to end, and report the inferred population in their response.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.mcp.server import create_mcp_server

from tests._dev1866_fixtures import make_chain_sqlite_storage


# --------------------------------------------------------------------------- #
# REST.
# --------------------------------------------------------------------------- #
@pytest.fixture
def rest_client() -> TestClient:
    storage = asyncio.run(make_chain_sqlite_storage())
    return TestClient(create_app(storage=storage))


def test_rest_rootless_query_executes_and_reports_population(rest_client) -> None:
    resp = rest_client.post("/query", json={
        "dimensions": ["customers.region"],
        "measures": [{"formula": "orders.amount:sum", "name": "rev"}],
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["population"] == "customers"
    assert body["population_inferred"] is True
    regions = {row["customers.region"] for row in body["data"]}
    assert regions == {"North", "South", "West"}


# --------------------------------------------------------------------------- #
# MCP.
# --------------------------------------------------------------------------- #
async def _call(server, *, name: str, arguments: dict[str, Any]) -> str:
    content_blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return content_blocks[0].text


async def test_mcp_rootless_query_executes_and_reports_population() -> None:
    storage = await make_chain_sqlite_storage()
    server = create_mcp_server(storage=storage)
    try:
        result = await _call(server, name="query", arguments={
            "query": {
                "dimensions": ["customers.region"],
                "measures": [{"formula": "orders.amount:sum", "name": "rev"}],
            },
            "format": "json",
        })
        # Decode the leading JSON value, tolerating any trailing footer text.
        payload, _ = json.JSONDecoder().raw_decode(result)
        rows = payload["data"] if isinstance(payload, dict) else payload
        regions = {row["customers.region"] for row in rows}
        assert regions == {"North", "South", "West"}
        # The inferred population rides in the JSON envelope (both fields, since inferred).
        assert payload["population"] == "customers"
        assert payload["population_inferred"] is True
    finally:
        await server._slayer_engine.aclose()
