"""``inspect`` of a model at ``compact=False`` keeps each learning's full text; the tool exposes ``compact``."""

from __future__ import annotations

import json
import shutil
import tempfile
from typing import Any
from collections.abc import Generator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def yaml_storage() -> Generator[YAMLStorage, None, None]:
    tmpdir = tempfile.mkdtemp()
    try:
        yield YAMLStorage(base_dir=tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


async def _call(mcp_server, *, name: str, arguments: dict) -> str:
    content_blocks, _ = await mcp_server.call_tool(name=name, arguments=arguments)
    return content_blocks[0].text


async def _seed_orders_with_memory(
    storage: YAMLStorage,
    *,
    learning: str,
    description=None,
) -> str:
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="sqlite", database=":memory:")
    )
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="mydb",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    ))
    mem = await storage.save_memory(
        learning=learning,
        entities=["mydb.orders.amount"],
        description=description,
    )
    return mem.id


# ---------------------------------------------------------------------------
# Full learnings
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verbose_markdown_dumps_full_learning(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders_with_memory(
        yaml_storage,
        learning="first paragraph.\n\nsecond paragraph body.",
        description="short summary",
    )
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="inspect", arguments={
        "entity_type": "model", "reference": "orders", "compact": False,
    })
    assert "first paragraph." in result
    assert "second paragraph body." in result


@pytest.mark.asyncio
async def test_verbose_json_learnings_keeps_full_learning(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders_with_memory(
        yaml_storage,
        learning="full body json verbose",
        description="json preview",
    )
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="inspect", arguments={
        "entity_type": "model", "reference": "orders", "compact": False, "format": "json",
    })
    payload = json.loads(result)
    entry = payload["learnings"][0]
    assert entry["learning"] == "full body json verbose"


# ---------------------------------------------------------------------------
# Tool descriptor exposes compact
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_descriptor_lists_compact_arg(
    yaml_storage: YAMLStorage,
) -> None:
    mcp = create_mcp_server(storage=yaml_storage)
    tools = await mcp.list_tools()
    tool: Any = next(t for t in tools if t.name == "inspect")
    schema = tool.inputSchema
    assert "compact" in schema["properties"]


@pytest.mark.asyncio
async def test_tool_descriptor_omits_reachable_fields_depth_arg(
    yaml_storage: YAMLStorage,
) -> None:
    """The removed ``reachable_fields_depth`` kwarg stays out of the ``inspect`` tool schema."""
    mcp = create_mcp_server(storage=yaml_storage)
    tools = await mcp.list_tools()
    tool: Any = next(t for t in tools if t.name == "inspect")
    schema = tool.inputSchema
    assert "reachable_fields_depth" not in schema["properties"]
