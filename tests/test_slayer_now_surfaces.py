"""``SLAYER_NOW`` reaches the MCP and REST servers, and an invalid value fails before any side effect."""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

import slayer.api.server as api_server
import slayer.mcp.server as mcp_server_mod
from slayer import cli
from slayer.api.server import create_app
from slayer.async_utils import run_sync
from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig
from slayer.engine import ingestion as ingestion_module
from slayer.mcp.server import create_mcp_server
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

from tests._cli_inprocess import run_cli_in_process
from tests._slayer_now_fixtures import LAST_3_MONTHS, PIN, PINNED_WINDOW, ids_query, row_ids, seeded_storage


@pytest.fixture
def ev_storage(tmp_path):
    storage = run_sync(seeded_storage(str(tmp_path)))
    yield storage
    engine_factory.invalidate_engine(DatasourceConfig(name="test", type="sqlite", database=str(tmp_path / "ev.db")))


async def _mcp_query_ids(server) -> set[int]:
    blocks, _ = await server.call_tool(
        name="query", arguments={"query": ids_query(LAST_3_MONTHS), "format": "json"},
    )
    decoded = json.loads(blocks[0].text)
    return row_ids(decoded["data"] if isinstance(decoded, dict) else decoded)


async def test_mcp_query_tool_honours_pin(monkeypatch, ev_storage) -> None:
    monkeypatch.setenv("SLAYER_NOW", PIN)
    server = create_mcp_server(storage=ev_storage)
    try:
        assert await _mcp_query_ids(server) == PINNED_WINDOW
    finally:
        await server._slayer_engine.aclose()


def test_rest_and_embedded_mcp_honour_pin(monkeypatch, ev_storage) -> None:
    monkeypatch.setenv("SLAYER_NOW", PIN)
    servers: list[Any] = []

    def capturing_create_mcp_server(*args, **kwargs):
        servers.append(create_mcp_server(*args, **kwargs))
        return servers[-1]

    monkeypatch.setattr(api_server, "create_mcp_server", capturing_create_mcp_server)
    with TestClient(create_app(storage=ev_storage)) as client:
        resp = client.post("/query", json=ids_query(LAST_3_MONTHS))
    assert resp.status_code == 200, resp.text
    assert row_ids(resp.json()["data"]) == PINNED_WINDOW
    assert len(servers) == 1
    try:
        assert run_sync(_mcp_query_ids(servers[0])) == PINNED_WINDOW
    finally:
        run_sync(servers[0]._slayer_engine.aclose())


@pytest.mark.parametrize("factory", ["mcp", "rest"])
def test_server_factories_fail_before_side_effects(monkeypatch, tmp_path, factory) -> None:
    monkeypatch.setenv("SLAYER_NOW", "yesterday-ish")
    calls: list[str] = []

    async def spy_seed(*args, **kwargs):  # NOSONAR(S7503) — replaces an async seam
        calls.append("seed")

    async def spy_ingest(*args, **kwargs):  # NOSONAR(S7503) — replaces an async seam
        calls.append("ingest")

    monkeypatch.setattr(mcp_server_mod, "seed_help_memories", spy_seed)
    monkeypatch.setattr(api_server, "seed_help_memories", spy_seed)
    monkeypatch.setattr(ingestion_module, "ingest_all_datasources_idempotent", spy_ingest)
    storage = YAMLStorage(base_dir=str(tmp_path))
    build = create_mcp_server if factory == "mcp" else create_app
    with pytest.raises(SlayerError, match="SLAYER_NOW"):
        build(storage=storage, ingest_on_startup=True)
    assert calls == []


@pytest.mark.parametrize("command", ["mcp", "serve"])
def test_cli_fails_before_running_a_command(monkeypatch, tmp_path, command) -> None:
    monkeypatch.setenv("SLAYER_NOW", "yesterday-ish")
    calls: list[str] = []

    def demo_seam(*args, **kwargs):
        calls.append("demo")
        raise RuntimeError("demo seam reached")

    monkeypatch.setattr(cli, "ensure_demo_datasource", demo_seam)
    result = run_cli_in_process([command, "--demo", "--storage", str(tmp_path / "store")])
    assert result.returncode != 0
    assert "SLAYER_NOW" in result.stderr
    assert "yesterday-ish" in result.stderr
    assert result.stdout == ""
    assert calls == []


def test_cli_fails_before_a_non_server_command(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SLAYER_NOW", "yesterday-ish")
    calls: list[str] = []
    monkeypatch.setattr(cli, "_run_query", lambda args: calls.append("query"))
    result = run_cli_in_process(["query", json.dumps(ids_query(LAST_3_MONTHS)), "--storage", str(tmp_path / "store")])
    assert result.returncode != 0
    assert "SLAYER_NOW" in result.stderr
    assert calls == []
