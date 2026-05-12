"""End-to-end surface tests for the `search` MCP tool, REST endpoint,
CLI subcommand, and Python client (DEV-1375).

Each surface delegates to the same `SearchService`; these tests confirm
the wiring + the surface-specific I/O contract (JSON wrapping, error
formatting, CLI flag shape).
"""

from __future__ import annotations

import json
import sys
import tempfile
from typing import AsyncIterator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage.base import StorageBackend, resolve_storage


@pytest_asyncio.fixture
async def storage_with_corpus() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = resolve_storage(tmpdir)
        await storage.save_datasource(DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="warehouse",
            description="Checkout orders.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount_paid", type=DataType.DOUBLE,
                       description="Net paid in USD."),
            ],
        ))
        await storage.save_memory(
            learning="amount_paid is gross of refunds.",
            entities=["warehouse.orders.amount_paid"],
        )
        yield storage


# ---------------------------------------------------------------------------
# MCP tool
# ---------------------------------------------------------------------------


async def _call_mcp_tool(*, mcp, name: str, arguments: dict) -> str:  # NOSONAR(S3776) — small test helper that branches over three FastMCP result shapes; splitting hurts readability
    """Invoke an MCP tool and return its text result."""
    result = await mcp.call_tool(name, arguments)
    if isinstance(result, tuple):
        # Some FastMCP versions return (content_list, structured_result).
        for block in result[0]:
            if hasattr(block, "text"):
                return block.text
    if isinstance(result, list):
        for block in result:
            if hasattr(block, "text"):
                return block.text
    if hasattr(result, "content"):
        for block in result.content:
            if hasattr(block, "text"):
                return block.text
    return str(result)


@pytest.mark.asyncio
async def test_mcp_search_tool_returns_json_with_three_lists(
    storage_with_corpus: StorageBackend,
) -> None:
    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage_with_corpus)
    tools = await mcp.list_tools()
    assert any(t.name == "search" for t in tools)
    # Old recall surface is gone.
    assert not any(t.name == "recall_memories" for t in tools)
    result_text = await _call_mcp_tool(
        mcp=mcp,
        name="search",
        arguments={
            "entities": ["warehouse.orders.amount_paid"],
            "question": "gross refunds",
            "max_memories": 5,
            "max_example_queries": 2,
            "max_entities": 5,
        },
    )
    payload = json.loads(result_text)
    assert "memories" in payload
    assert "example_queries" in payload
    assert "entities" in payload
    assert "resolved_input_entities" in payload
    assert "warehouse.orders.amount_paid" in payload["resolved_input_entities"]


@pytest.mark.asyncio
async def test_mcp_search_friendly_error_on_unknown_entity(
    storage_with_corpus: StorageBackend,
) -> None:
    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage_with_corpus)
    result_text = await _call_mcp_tool(
        mcp=mcp,
        name="search",
        arguments={"entities": ["warehouse.nope.col"]},
    )
    assert "Error" in result_text


# ---------------------------------------------------------------------------
# REST
# ---------------------------------------------------------------------------


def test_rest_post_search_returns_response_shape(tmp_path) -> None:
    """POST /search round-trips through the API."""
    from slayer.api.server import create_app
    import asyncio
    storage = resolve_storage(str(tmp_path / "storage"))

    async def _seed():
        await storage.save_datasource(DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="warehouse",
            columns=[Column(name="amount_paid", type=DataType.DOUBLE)],
        ))
        await storage.save_memory(
            learning="amount_paid is net of refunds.",
            entities=["warehouse.orders.amount_paid"],
        )

    asyncio.run(_seed())
    app = create_app(storage=storage)
    client = TestClient(app)
    res = client.post("/search", json={
        "entities": ["warehouse.orders.amount_paid"],
        "question": "refunds",
        "max_memories": 5,
        "max_example_queries": 2,
        "max_entities": 5,
    })
    assert res.status_code == 200
    body = res.json()
    assert "memories" in body
    assert "example_queries" in body
    assert "entities" in body
    assert "resolved_input_entities" in body
    # Recall endpoint is gone (FastAPI returns 405 because /memories/{id}
    # captures the path with the wrong method, or 404 if no route matches).
    assert client.post(
        "/memories/recall", json={"about": ["warehouse.orders.amount_paid"]}
    ).status_code in (404, 405)


def test_rest_post_search_unknown_entity_returns_400(tmp_path) -> None:
    from slayer.api.server import create_app
    import asyncio
    storage = resolve_storage(str(tmp_path / "storage"))

    async def _seed():
        await storage.save_datasource(DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"))

    asyncio.run(_seed())
    app = create_app(storage=storage)
    client = TestClient(app)
    res = client.post("/search", json={"entities": ["warehouse.nope.col"]})
    assert res.status_code == 400


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _run_cli(args: list[str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> tuple[int, str]:
    """Invoke the slayer CLI's main() with mocked argv. Returns (exit_code, stdout)."""
    from slayer.cli import main
    monkeypatch.setattr(sys, "argv", ["slayer"] + args)
    try:
        main()
        code = 0
    except SystemExit as e:  # NOSONAR(S5754) — _run_cli intentionally captures SystemExit so tests can assert on exit codes
        code = int(e.code or 0)
    captured = capsys.readouterr()
    return code, captured.out


def test_cli_search_subcommand_help(monkeypatch, capsys) -> None:
    code, out = _run_cli(["search", "--help"], monkeypatch, capsys)
    assert code == 0
    assert "search" in out.lower()


def test_cli_search_refresh_samples_subcommand_help(monkeypatch, capsys) -> None:
    code, _ = _run_cli(["search", "refresh-samples", "--help"], monkeypatch, capsys)
    assert code == 0


def test_cli_search_runs_against_storage(tmp_path, monkeypatch, capsys) -> None:
    """End-to-end: storage + search subcommand."""
    import asyncio
    storage_dir = str(tmp_path / "storage")
    storage = resolve_storage(storage_dir)

    async def _seed():
        await storage.save_datasource(DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="warehouse",
            columns=[Column(name="amount_paid", type=DataType.DOUBLE)],
        ))
        await storage.save_memory(
            learning="amount_paid is net of refunds.",
            entities=["warehouse.orders.amount_paid"],
        )

    asyncio.run(_seed())
    code, out = _run_cli(
        [
            "search",
            "--storage", storage_dir,
            "--entity", "warehouse.orders.amount_paid",
            "--max-example-queries", "1",
            "--format", "json",
        ],
        monkeypatch, capsys,
    )
    assert code == 0
    payload = json.loads(out)
    assert "memories" in payload
    assert "example_queries" in payload
    assert "resolved_input_entities" in payload


def test_cli_memory_recall_subcommand_removed(monkeypatch, capsys) -> None:
    """`slayer memory recall` is gone — argparse should reject it."""
    code, _ = _run_cli(["memory", "recall", "--help"], monkeypatch, capsys)
    assert code != 0


# ---------------------------------------------------------------------------
# Python client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_search_round_trip(
    storage_with_corpus: StorageBackend,
) -> None:
    """`SlayerClient(storage=...).search(...)` round-trips through the
    in-process ``SearchService`` and returns a populated ``SearchResponse``."""
    from slayer.client.slayer_client import SlayerClient
    from slayer.search.service import (
        EntityHit,
        ExampleQueryHit,
        MemoryHit,
        SearchResponse,
    )

    assert hasattr(SlayerClient, "search")
    # Old client method is gone.
    assert not hasattr(SlayerClient, "recall_memories")

    client = SlayerClient(storage=storage_with_corpus)
    response = await client.search(
        entities=["warehouse.orders.amount_paid"],
        question="refunds",
        max_example_queries=2,
    )

    assert isinstance(response, SearchResponse)
    assert isinstance(response.memories, list)
    assert isinstance(response.example_queries, list)
    assert isinstance(response.entities, list)
    assert isinstance(response.warnings, list)
    assert all(isinstance(m, MemoryHit) for m in response.memories)
    assert all(isinstance(e, ExampleQueryHit) for e in response.example_queries)
    assert all(isinstance(e, EntityHit) for e in response.entities)
    assert len(response.memories) >= 1
    assert "warehouse.orders.amount_paid" in response.memories[0].matched_entities
    assert "warehouse.orders.amount_paid" in response.resolved_input_entities
