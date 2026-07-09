"""DEV-1549: ``description`` plumbing across MCP / REST / CLI / SlayerClient
for ``save_memory``. Codex#7 — symmetric default everywhere.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
from typing import Any
from collections.abc import AsyncIterator, Generator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.mcp.server import create_mcp_server
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    tmpdir = tempfile.mkdtemp()
    try:
        s = YAMLStorage(base_dir=tmpdir)
        await s.save_datasource(
            DatasourceConfig(name="mydb", type="sqlite", database=":memory:")
        )
        await s.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="mydb",
            columns=[
                Column(name="id", sql="id", primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        ))
        yield s
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def yaml_storage() -> Generator[YAMLStorage, None, None]:
    tmpdir = tempfile.mkdtemp()
    try:
        s = YAMLStorage(base_dir=tmpdir)
        asyncio.run(s.save_datasource(
            DatasourceConfig(name="mydb", type="sqlite", database=":memory:")
        ))
        asyncio.run(s.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="mydb",
            columns=[
                Column(name="id", sql="id", primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )))
        yield s
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


async def _call_mcp(mcp_server, *, name: str, arguments: dict) -> str:
    content_blocks, _ = await mcp_server.call_tool(name=name, arguments=arguments)
    return content_blocks[0].text


# ---------------------------------------------------------------------------
# MCP path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mcp_save_memory_description_persists(
    storage: StorageBackend,
) -> None:
    mcp = create_mcp_server(storage=storage)
    raw = await _call_mcp(mcp, name="save_memory", arguments={
        "learning": "x",
        "linked_entities": ["mydb.orders.amount"],
        "description": "amount in cents",
    })
    response = json.loads(raw)
    reloaded = await storage.get_memory(response["memory_id"])
    assert reloaded.description == "amount in cents"


@pytest.mark.asyncio
async def test_mcp_save_memory_description_too_long_friendly_error(
    storage: StorageBackend,
) -> None:
    mcp = create_mcp_server(storage=storage)
    raw = await _call_mcp(mcp, name="save_memory", arguments={
        "learning": "x",
        "linked_entities": ["mydb.orders.amount"],
        "description": "a" * 501,
    })
    # MCP convention: friendly error text, not a raised exception.
    # No traceback markers, the error must reference description and the
    # cap, and the memory must not have been persisted.
    assert "Traceback" not in raw
    assert "raise" not in raw
    assert "description" in raw.lower()
    assert "500" in raw
    # Storage holds no user memory from the rejected save (DEV-1658: the
    # seeded help.* memories are infrastructure, not persisted by this call).
    memories = await storage.list_memories(entities=None)
    assert [m for m in memories if not m.id.startswith("help.")] == []


@pytest.mark.asyncio
async def test_mcp_save_memory_tool_descriptor_includes_description(
    storage: StorageBackend,
) -> None:
    mcp = create_mcp_server(storage=storage)
    tools = await mcp.list_tools()
    tool: Any = next(t for t in tools if t.name == "save_memory")
    schema = tool.inputSchema
    assert "description" in schema["properties"]


# ---------------------------------------------------------------------------
# REST path
# ---------------------------------------------------------------------------


def test_rest_post_memories_description_persists(
    yaml_storage: YAMLStorage,
) -> None:
    app = create_app(storage=yaml_storage)
    client = TestClient(app)
    resp = client.post("/memories", json={
        "learning": "x",
        "linked_entities": ["mydb.orders.amount"],
        "description": "rest cents note",
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    mem = asyncio.run(yaml_storage.get_memory(body["memory_id"]))
    assert mem.description == "rest cents note"


def test_rest_post_memories_description_too_long_400(
    yaml_storage: YAMLStorage,
) -> None:
    app = create_app(storage=yaml_storage)
    client = TestClient(app)
    resp = client.post("/memories", json={
        "learning": "x",
        "linked_entities": ["mydb.orders.amount"],
        "description": "a" * 501,
    })
    assert resp.status_code in (400, 422)


# ---------------------------------------------------------------------------
# SlayerClient (local mode)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_slayer_client_local_save_memory_description(
    storage: StorageBackend,
) -> None:
    client = SlayerClient(storage=storage)
    resp = await client.save_memory(
        learning="x",
        linked_entities=["mydb.orders.amount"],
        description="client cents",
    )
    mem = await storage.get_memory(resp.memory_id)
    assert mem.description == "client cents"


# ---------------------------------------------------------------------------
# CLI path
# ---------------------------------------------------------------------------


def test_cli_memory_save_accepts_description_flag(
    yaml_storage: YAMLStorage,
) -> None:
    """``_run_memory`` builds the Namespace inline; this mirrors the
    pattern from ``tests/test_memories_cli.py`` while exercising the
    new ``--description`` flag."""
    from types import SimpleNamespace

    from slayer.cli import _run_memory

    args = SimpleNamespace(
        storage=yaml_storage.base_dir,
        models_dir=None,
        memory_command="save",
        learning="x",
        entities="mydb.orders.amount",
        query=None,
        id=None,
        description="cli cents",
    )
    _run_memory(args)
    memories = asyncio.run(yaml_storage.list_memories(entities=None))
    assert any(m.description == "cli cents" for m in memories)


def test_cli_argparse_subprocess_accepts_description_flag(
    yaml_storage: YAMLStorage,
) -> None:
    """End-to-end smoke (subprocess): the argparse spec accepts
    ``--description`` and the saved memory carries the field. The
    project CLI puts ``--storage`` on each subparser, so it appears
    after the subcommand.

    The subprocess inherits an env where litellm's logger is forced to
    ``ERROR`` — its default WARNING-level chatter (the model-cost-map
    fetch and SOCKS-proxy fallback paths fired during the embedding
    refresh) writes large stderr bursts that race pytest's fd-level
    output capture and can starve the subprocess pipe until the
    ``subprocess.run`` timeout kicks in. Silencing the logger removes
    the bursts and the test is deterministic at ~5s.
    """
    import os
    import subprocess
    import sys

    env = {**os.environ, "LITELLM_LOG": "ERROR"}
    cmd = [
        sys.executable, "-m", "slayer",
        "memory",
        "--storage", yaml_storage.base_dir,
        "save",
        "--learning", "x",
        "--entities", "mydb.orders.amount",
        "--description", "subproc cents",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=env)
    assert result.returncode == 0, result.stderr
    memories = asyncio.run(yaml_storage.list_memories(entities=None))
    assert any(m.description == "subproc cents" for m in memories)
