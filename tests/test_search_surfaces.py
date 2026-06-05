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

from tests.search_helpers import call_mcp_tool as _call_mcp_tool


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
            "max_results": 20,
        },
    )
    payload = json.loads(result_text)
    assert "results" in payload
    assert "resolved_input_entities" in payload
    assert "warehouse.orders.amount_paid" in payload["resolved_input_entities"]


@pytest.mark.asyncio
async def test_mcp_search_path_wires_engine_into_search_service(
    storage_with_corpus: StorageBackend,
) -> None:
    """DEV-1516 codex finding #1: ``create_mcp_server`` constructs a
    ``SearchService`` with an engine kwarg so the search-side refresh
    actually fires in the MCP product path. This test catches a regression
    where the engine wiring is dropped (the helper would silently no-op)."""
    from slayer.mcp.server import create_mcp_server
    from slayer.search.service import SearchService

    constructed: list = []

    real_init = SearchService.__init__

    def capturing_init(self, *args, **kwargs):
        constructed.append(kwargs)
        return real_init(self, *args, **kwargs)

    # Patch on the class so the MCP factory's `SearchService(storage=..., engine=...)`
    # call funnels through us.
    original = SearchService.__init__
    SearchService.__init__ = capturing_init  # type: ignore[assignment]
    try:
        create_mcp_server(storage=storage_with_corpus)
    finally:
        SearchService.__init__ = original  # type: ignore[assignment]

    assert constructed, "create_mcp_server should construct SearchService"
    # At least one construction must include a non-None engine kwarg.
    kw_lists = constructed
    assert any(
        kw.get("engine") is not None for kw in kw_lists
    ), (
        "MCP wiring regression: SearchService constructed without engine. "
        "Search-side sample-refresh would silently no-op."
    )


@pytest.mark.asyncio
async def test_rest_search_path_wires_engine_into_search_service(
    storage_with_corpus: StorageBackend,
) -> None:
    """REST counterpart of the MCP wiring test. ``create_app`` must also
    pass the engine to SearchService."""
    from slayer.api.server import create_app
    from slayer.search.service import SearchService

    constructed: list = []
    real_init = SearchService.__init__

    def capturing_init(self, *args, **kwargs):
        constructed.append(kwargs)
        return real_init(self, *args, **kwargs)

    original = SearchService.__init__
    SearchService.__init__ = capturing_init  # type: ignore[assignment]
    try:
        create_app(storage=storage_with_corpus)
    finally:
        SearchService.__init__ = original  # type: ignore[assignment]

    assert constructed, "create_app should construct SearchService"
    assert any(
        kw.get("engine") is not None for kw in constructed
    ), (
        "REST wiring regression: SearchService constructed without engine. "
        "Search-side sample-refresh would silently no-op on REST calls."
    )


@pytest.mark.asyncio
async def test_mcp_search_friendly_warning_on_unknown_entity(
    storage_with_corpus: StorageBackend,
) -> None:
    """DEV-1428: unknown entities become warnings; the MCP tool still
    returns the regular JSON response (the warning is visible inside)."""
    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage_with_corpus)
    result_text = await _call_mcp_tool(
        mcp=mcp,
        name="search",
        arguments={"entities": ["warehouse.nope.col"]},
    )
    assert "warehouse.nope.col" in result_text
    assert "warnings" in result_text


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
        "max_results": 20,
    })
    assert res.status_code == 200
    body = res.json()
    assert "results" in body
    assert "resolved_input_entities" in body
    # Recall endpoint is gone (FastAPI returns 405 because /memories/{id}
    # captures the path with the wrong method, or 404 if no route matches).
    assert client.post(
        "/memories/recall", json={"about": ["warehouse.orders.amount_paid"]}
    ).status_code in (404, 405)


def test_rest_post_search_unknown_entity_returns_warning(tmp_path) -> None:
    """DEV-1428: unknown entities become warnings rather than 400 — the
    REST endpoint returns 200 with the warning in ``warnings``."""
    from slayer.api.server import create_app
    import asyncio
    storage = resolve_storage(str(tmp_path / "storage"))

    async def _seed():
        await storage.save_datasource(DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"))

    asyncio.run(_seed())
    app = create_app(storage=storage)
    client = TestClient(app)
    res = client.post("/search", json={"entities": ["warehouse.nope.col"]})
    assert res.status_code == 200
    body = res.json()
    assert any("warehouse.nope.col" in w for w in body["warnings"])


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


def _seed_cli_storage(tmp_path) -> str:
    """Set up a one-datasource, one-model, one-memory storage tree and
    return its directory. Shared by the CLI-search surface tests."""
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
    return storage_dir


def test_cli_search_runs_against_storage(tmp_path, monkeypatch, capsys) -> None:
    """End-to-end: storage + search subcommand."""
    storage_dir = _seed_cli_storage(tmp_path)
    code, out = _run_cli(
        [
            "search",
            "--storage", storage_dir,
            "--entity", "warehouse.orders.amount_paid",
            "--max-results", "10",
            "--format", "json",
        ],
        monkeypatch, capsys,
    )
    assert code == 0
    payload = json.loads(out)
    assert "results" in payload
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
        SearchHit,
        SearchResponse,
    )

    assert hasattr(SlayerClient, "search")
    # Old client method is gone.
    assert not hasattr(SlayerClient, "recall_memories")

    client = SlayerClient(storage=storage_with_corpus)
    response = await client.search(
        entities=["warehouse.orders.amount_paid"],
        question="refunds",
        max_results=20,
    )

    assert isinstance(response, SearchResponse)
    assert isinstance(response.results, list)
    assert isinstance(response.warnings, list)
    assert all(isinstance(h, SearchHit) for h in response.results)
    memory_hits = [h for h in response.results if h.kind == "memory" and h.query is None]
    assert len(memory_hits) >= 1
    assert "warehouse.orders.amount_paid" in memory_hits[0].matched_entities
    assert "warehouse.orders.amount_paid" in response.resolved_input_entities


# ---------------------------------------------------------------------------
# datasource filter (DEV-1409) — one test per surface
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mcp_search_accepts_datasource_arg(
    storage_with_corpus: StorageBackend,
) -> None:
    """MCP ``search`` tool accepts the new ``datasource`` argument."""
    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage_with_corpus)
    result_text = await _call_mcp_tool(
        mcp=mcp,
        name="search",
        arguments={
            "entities": ["warehouse.orders.amount_paid"],
            "datasource": "warehouse",
        },
    )
    # Round-trips a populated response.
    assert "Error" not in result_text


@pytest.mark.asyncio
async def test_mcp_search_unknown_datasource_returns_error(
    storage_with_corpus: StorageBackend,
) -> None:
    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage_with_corpus)
    result_text = await _call_mcp_tool(
        mcp=mcp,
        name="search",
        arguments={
            "entities": ["warehouse.orders.amount_paid"],
            "datasource": "does_not_exist",
        },
    )
    assert "Error" in result_text
    assert "does_not_exist" in result_text


def test_rest_post_search_accepts_datasource(tmp_path) -> None:
    """POST /search accepts ``datasource`` in the JSON body."""
    from slayer.api.server import create_app
    import asyncio
    storage = resolve_storage(str(tmp_path / "storage"))

    async def _seed():
        await storage.save_datasource(DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="warehouse",
            columns=[Column(name="amount_paid", type=DataType.DOUBLE)],
        ))

    asyncio.run(_seed())
    app = create_app(storage=storage)
    client = TestClient(app)
    res = client.post("/search", json={
        "entities": ["warehouse.orders.amount_paid"],
        "datasource": "warehouse",
    })
    assert res.status_code == 200


def test_rest_post_search_unknown_datasource_returns_400(tmp_path) -> None:
    from slayer.api.server import create_app
    import asyncio
    storage = resolve_storage(str(tmp_path / "storage"))

    async def _seed():
        await storage.save_datasource(DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"))

    asyncio.run(_seed())
    app = create_app(storage=storage)
    client = TestClient(app)
    res = client.post("/search", json={
        "entities": ["warehouse.orders.amount_paid"],
        "datasource": "does_not_exist",
    })
    assert res.status_code == 400


def test_cli_search_accepts_datasource_flag(tmp_path, monkeypatch, capsys) -> None:
    """``slayer search --datasource X`` parses cleanly."""
    storage_dir = _seed_cli_storage(tmp_path)
    code, out = _run_cli(
        args=[
            "search",
            "--storage", storage_dir,
            "--entity", "warehouse.orders.amount_paid",
            "--datasource", "warehouse",
            "--format", "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0
    payload = json.loads(out)
    assert "results" in payload


@pytest.mark.asyncio
async def test_client_search_accepts_datasource(
    storage_with_corpus: StorageBackend,
) -> None:
    """``SlayerClient.search`` accepts ``datasource=`` and forwards it
    to the service."""
    from slayer.client.slayer_client import SlayerClient

    client = SlayerClient(storage=storage_with_corpus)
    response = await client.search(
        entities=["warehouse.orders.amount_paid"],
        datasource="warehouse",
        max_results=20,
    )
    assert "warehouse.orders.amount_paid" in response.resolved_input_entities
