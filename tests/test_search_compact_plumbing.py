"""DEV-1549: ``compact`` plumbing across MCP / REST / CLI / SlayerClient
for ``search``. Codex#7 — symmetric default everywhere; CLI flips via
``--verbose``.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from typing import Any, AsyncIterator, Generator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from tests.search_helpers import seed_warehouse_models

from slayer.api.server import SearchRequest, create_app
from slayer.client.slayer_client import SlayerClient
from slayer.mcp.server import create_mcp_server
from slayer.storage.base import StorageBackend, resolve_storage
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    tmpdir = tempfile.mkdtemp()
    try:
        s = resolve_storage(tmpdir)
        await seed_warehouse_models(s)
        await s.save_memory(
            learning="full body of memory for plumbing tests",
            entities=["warehouse.orders.amount_paid"],
            description="plumbing preview",
        )
        yield s
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def yaml_storage() -> Generator[YAMLStorage, None, None]:
    tmpdir = tempfile.mkdtemp()
    try:
        s = YAMLStorage(base_dir=tmpdir)
        asyncio.run(seed_warehouse_models(s))
        asyncio.run(s.save_memory(
            learning="full body of memory for plumbing tests",
            entities=["warehouse.orders.amount_paid"],
            description="plumbing preview",
        ))
        yield s
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# REST
# ---------------------------------------------------------------------------


def test_rest_search_request_model_lists_compact_field() -> None:
    assert "compact" in SearchRequest.model_fields
    assert SearchRequest.model_fields["compact"].default is True


def test_rest_post_search_default_is_compact(yaml_storage: YAMLStorage) -> None:
    app = create_app(storage=yaml_storage)
    client = TestClient(app)
    resp = client.post(
        "/search",
        json={"entities": ["warehouse.orders.amount_paid"]},
    )
    assert resp.status_code == 200
    results = resp.json()["results"]
    mem_hits = [h for h in results if h["kind"] == "memory"]
    assert mem_hits
    for h in mem_hits:
        assert h["text"] == ""
        assert h["description"] is not None


def test_rest_post_search_compact_false_returns_verbose(
    yaml_storage: YAMLStorage,
) -> None:
    app = create_app(storage=yaml_storage)
    client = TestClient(app)
    resp = client.post("/search", json={
        "entities": ["warehouse.orders.amount_paid"],
        "compact": False,
    })
    assert resp.status_code == 200
    results = resp.json()["results"]
    mem_hits = [h for h in results if h["kind"] == "memory"]
    assert any("full body of memory" in h["text"] for h in mem_hits)


# ---------------------------------------------------------------------------
# SlayerClient (local)
# ---------------------------------------------------------------------------


def test_slayer_client_search_signature_has_compact() -> None:
    sig = inspect.signature(SlayerClient.search)
    assert "compact" in sig.parameters
    assert sig.parameters["compact"].default is True


@pytest.mark.asyncio
async def test_slayer_client_local_search_default_is_compact(
    storage: StorageBackend,
) -> None:
    client = SlayerClient(storage=storage)
    response = await client.search(
        entities=["warehouse.orders.amount_paid"],
    )
    mem_hits = [h for h in response.results if h.kind == "memory"]
    assert mem_hits
    for h in mem_hits:
        assert h.text == ""
        assert h.description is not None


@pytest.mark.asyncio
async def test_slayer_client_local_search_compact_false(
    storage: StorageBackend,
) -> None:
    client = SlayerClient(storage=storage)
    response = await client.search(
        entities=["warehouse.orders.amount_paid"], compact=False,
    )
    mem_hits = [h for h in response.results if h.kind == "memory"]
    assert any("full body of memory" in h.text for h in mem_hits)


# ---------------------------------------------------------------------------
# CLI — `slayer search` defaults to compact; `--verbose` opts out
# ---------------------------------------------------------------------------


def test_cli_search_default_is_compact(yaml_storage: YAMLStorage) -> None:
    """Without --verbose, the JSON output shows description set and text empty."""
    from slayer.cli import _run_search_query

    args = SimpleNamespace(
        storage=yaml_storage.base_dir,
        models_dir=None,
        entities=["warehouse.orders.amount_paid"],
        query=None,
        question=None,
        datasource=None,
        max_results=10,
        cypher_filter=None,
        format="json",
        verbose=False,
    )
    # Capture stdout
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _run_search_query(args=args, storage=yaml_storage)
    payload = json.loads(buf.getvalue())
    mem_hits = [h for h in payload["results"] if h["kind"] == "memory"]
    assert mem_hits
    for h in mem_hits:
        assert h["text"] == ""
        assert h.get("description") is not None


def test_cli_search_verbose_flag_opts_out_of_compact(
    yaml_storage: YAMLStorage,
) -> None:
    from slayer.cli import _run_search_query

    args = SimpleNamespace(
        storage=yaml_storage.base_dir,
        models_dir=None,
        entities=["warehouse.orders.amount_paid"],
        query=None,
        question=None,
        datasource=None,
        max_results=10,
        cypher_filter=None,
        format="json",
        verbose=True,
    )
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _run_search_query(args=args, storage=yaml_storage)
    payload = json.loads(buf.getvalue())
    mem_hits = [h for h in payload["results"] if h["kind"] == "memory"]
    assert any("full body of memory" in h["text"] for h in mem_hits)


def test_cli_argparse_subprocess_search_verbose_flag(
    yaml_storage: YAMLStorage,
) -> None:
    """End-to-end smoke that the argparse spec accepts ``--verbose``."""
    cmd = [
        sys.executable, "-m", "slayer",
        "search",
        "--storage", yaml_storage.base_dir,
        "--entity", "warehouse.orders.amount_paid",
        "--format", "json",
        "--verbose",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    mem_hits = [h for h in payload["results"] if h["kind"] == "memory"]
    assert any("full body of memory" in h["text"] for h in mem_hits)


# ---------------------------------------------------------------------------
# MCP tool descriptor + JSON round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mcp_search_tool_descriptor_lists_compact_arg(
    storage: StorageBackend,
) -> None:
    mcp = create_mcp_server(storage=storage)
    tools = await mcp.list_tools()
    tool: Any = next(t for t in tools if t.name == "search")
    schema = tool.inputSchema
    assert "compact" in schema["properties"]


@pytest.mark.asyncio
async def test_mcp_search_json_round_trip_compact_shape(
    storage: StorageBackend,
) -> None:
    """Codex#11: parse the JSON string returned by MCP and assert the
    compact field set."""
    mcp = create_mcp_server(storage=storage)
    content_blocks, _ = await mcp.call_tool(name="search", arguments={
        "entities": ["warehouse.orders.amount_paid"],
    })
    payload = json.loads(content_blocks[0].text)
    mem_hits = [h for h in payload["results"] if h["kind"] == "memory"]
    assert mem_hits
    for h in mem_hits:
        assert h["text"] == ""
        assert h.get("description") is not None
        # query field is preserved as-is (None for non-example hits).
        assert "query" in h
