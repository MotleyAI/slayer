"""Each surface opens one ``request`` span that names the surface and its operation, as usage reporting counts it."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from mcp.server.fastmcp.exceptions import ToolError

from slayer.api.server import create_app
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests._otel_helpers import OtelCapture
from tests._saved_query_refinement_fixtures import build_refine_storage
from tests._usage_reporting_helpers import build_storage, cli

_QUERY = {"population": "orders", "metrics": ["count(*)"]}


@pytest.fixture
def storage(tmp_path: Path) -> YAMLStorage:
    return asyncio.run(build_refine_storage(str(tmp_path)))


def _request(otel_env: OtelCapture) -> tuple[str, str]:
    assert otel_env.span_names().count("slayer.request") == 1, otel_env.span_names()
    attributes = otel_env.attributes("slayer.request")
    return attributes["slayer.surface"], attributes["slayer.operation"]


# --- REST ----------------------------------------------------------------------------


def test_a_rest_query_is_one_request_span_over_the_query(otel_env: OtelCapture, storage: YAMLStorage) -> None:
    with TestClient(create_app(storage=storage)) as client:
        response = client.post("/query", json=_QUERY)
    assert (response.status_code, _request(otel_env), otel_env.parent_name("slayer.query")) == (
        200, ("rest", "/query"), "slayer.request",
    )


def test_a_rest_route_is_named_by_its_template(otel_env: OtelCapture, storage: YAMLStorage) -> None:
    with TestClient(create_app(storage=storage)) as client:
        client.get("/datasets/orders")
    assert _request(otel_env) == ("rest", "/datasets/{name}")


def test_an_unknown_rest_path_is_other(otel_env: OtelCapture, storage: YAMLStorage) -> None:
    with TestClient(create_app(storage=storage)) as client:
        response = client.get("/zqxj/unknown")
    assert (response.status_code, _request(otel_env)) == (404, ("rest", "other"))


def test_the_mcp_mount_opens_no_rest_request(otel_env: OtelCapture, storage: YAMLStorage) -> None:
    with TestClient(create_app(storage=storage)) as client:
        client.post("/mcp", json={})
    surfaces = [dict(span.attributes or {}).get("slayer.surface") for span in otel_env.spans()]
    assert "rest" not in surfaces


# --- MCP -----------------------------------------------------------------------------


async def test_an_mcp_tool_call_follows_the_otel_mcp_conventions(otel_env: OtelCapture, tmp_path: Path) -> None:
    server = create_mcp_server(storage=await build_refine_storage(str(tmp_path)), _seed_help=False)
    await server.call_tool(name="query", arguments={"query": _QUERY, "format": "json"})
    attributes = otel_env.attributes("slayer.request")
    assert (
        attributes["slayer.operation"], attributes["mcp.method.name"],
        attributes["gen_ai.operation.name"], attributes["gen_ai.tool.name"],
    ) == ("query", "tools/call", "execute_tool", "query")


async def test_an_unknown_mcp_tool_is_named_other(otel_env: OtelCapture, tmp_path: Path) -> None:
    server = create_mcp_server(storage=await build_refine_storage(str(tmp_path)), _seed_help=False)
    with pytest.raises((ToolError, ValueError)):
        await server.call_tool(name="zqxj_tool", arguments={})
    assert otel_env.attributes("slayer.request")["gen_ai.tool.name"] == "other"


# --- CLI -----------------------------------------------------------------------------


def test_a_cli_query_is_one_request_span(otel_env: OtelCapture, tmp_path: Path) -> None:
    store = build_storage(tmp_path)
    result = cli(["query", json.dumps(_QUERY), "--storage", store])
    assert (result.returncode, _request(otel_env), otel_env.parent_name("slayer.query")) == (
        0, ("cli", "query"), "slayer.request",
    )
