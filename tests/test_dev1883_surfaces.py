"""DEV-1883 — string ``time_dimensions`` entries at the MCP and REST surfaces; tool docs.

Spec: openspec/changes/dev-1883-support-functional-time-granularity-form-monthcol-in-query/
specs/queries/time-dimensions (string entries at the API surfaces; advertised form).
"""
import json
import re
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import QueryRequest, create_app
from slayer.core.enums import TimeGranularity
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from tests import _dev1883_fixtures as fx

GRANULARITIES = [g.value for g in TimeGranularity]


@pytest.fixture
async def storage(tmp_path):
    engine = await fx.build_exec_engine(tmp_path)
    return engine.storage


@pytest.fixture
async def mcp_server(storage):
    return create_mcp_server(storage=storage)


@pytest.fixture
def rest_client(storage) -> TestClient:
    return TestClient(create_app(storage=storage))


async def _call(server, **arguments: Any) -> str:
    content_blocks, _ = await server.call_tool(name="query", arguments=arguments)
    return content_blocks[0].text


def _rows(out: str) -> list:
    decoded = json.loads(out)
    return decoded["data"] if isinstance(decoded, dict) else decoded


EXPLICIT_TD_QUERY = {
    "source_model": "orders",
    "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
    "measures": [{"formula": "amount:sum"}],
}


class TestMcpSurface:
    async def test_string_time_dimensions_entry_accepted(self, mcp_server) -> None:
        string_form = await _call(mcp_server, query={
            "source_model": "orders",
            "time_dimensions": ["month(created_at)"],
            "measures": [{"formula": "amount:sum"}],
        }, format="json")
        explicit_form = await _call(
            mcp_server, query=EXPLICIT_TD_QUERY, format="json",
        )
        assert _rows(string_form) == _rows(explicit_form)

    async def test_functional_dimensions_entry_accepted(self, mcp_server) -> None:
        out = await _call(mcp_server, query={
            "source_model": "orders",
            "dimensions": ["month(created_at)"],
            "measures": [{"formula": "amount:sum"}],
        }, format="json")
        assert {
            r["orders.created_at"]: r["orders.amount_sum"] for r in _rows(out)
        } == fx.MONTH_SUMS

    async def test_tool_docs_name_all_granularities_and_functional_form(
        self, mcp_server,
    ) -> None:
        tools = {t.name: t for t in await mcp_server.list_tools()}
        description = tools["query"].description or ""
        for gran in GRANULARITIES:
            # Whole-token: ``week`` must not be satisfied by ``week_sunday`` alone.
            assert re.search(rf"\b{re.escape(gran)}\b", description), (
                f"tool docs must name granularity {gran!r}"
            )
        # Both the dimensions and time_dimensions arg docs must show the form.
        for arg in ("dimensions", "time_dimensions"):
            block = re.search(
                rf"\n\s+{arg}: .*?(?=\n\s+\w+:)", description, re.S,
            )
            assert block, f"no {arg} arg block in tool docs"
            assert "month(created_at)" in block.group(0), (
                f"{arg} docs must show the functional gran(col) form:\n{block.group(0)}"
            )


class TestSchemaAdvertisesString:
    def test_time_dimensions_input_schema_allows_string(self) -> None:
        """The derived JSON/MCP input schema advertises the functional string form,
        not only the TimeDimension object (DEV-1883)."""
        schema = SlayerQuery.model_json_schema()
        td = schema["properties"]["time_dimensions"]
        array_schema = next(o for o in td["anyOf"] if o.get("type") == "array")
        item_options = array_schema["items"]["anyOf"]
        assert any(o.get("type") == "string" for o in item_options), td


class TestStoredQuerySurface:
    async def test_stored_query_document_accepts_string_entry(self, storage) -> None:
        """A query-backed model whose stored document carries the string form
        loads and runs identically to the explicit form."""
        model = SlayerModel.model_validate({
            "name": "monthly_rev", "data_source": "test",
            "source_queries": [{
                "source_model": "orders",
                "time_dimensions": ["month(created_at)"],
                "measures": [{"formula": "amount:sum"}],
            }],
        })
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        resp = await engine.execute("monthly_rev")
        assert {
            r["orders.created_at"]: r["orders.amount_sum"] for r in resp.data
        } == fx.MONTH_SUMS


class TestRestSurface:
    def test_string_time_dimensions_entry_accepted(self, rest_client) -> None:
        string_form = rest_client.post("/query", json={
            "source_model": "orders",
            "time_dimensions": ["month(created_at)"],
            "measures": [{"formula": "amount:sum"}],
        })
        assert string_form.status_code == 200, string_form.text
        explicit_form = rest_client.post("/query", json=EXPLICIT_TD_QUERY)
        assert explicit_form.status_code == 200, explicit_form.text
        assert string_form.json()["data"] == explicit_form.json()["data"]

    def test_query_request_annotation_accepts_strings(self) -> None:
        req = QueryRequest.model_validate({
            "source_model": "orders",
            "time_dimensions": ["month(created_at)"],
            "measures": [{"formula": "amount:sum"}],
        })
        query = SlayerQuery.model_validate(req.model_dump(exclude_none=True))
        assert query.time_dimensions == [
            fx.td(dimension="created_at", granularity="month")
        ]
