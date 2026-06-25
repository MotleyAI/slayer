"""DEV-1549: inspect_model gains ``compact: bool = True``.

The Learnings section flips from the full ``memory.learning`` body to
``memory.description`` when set, with the same first-paragraph fallback
used by ``search``. compact=False is the regression-pin for today's
verbose rendering.
"""

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
# Markdown rendering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_markdown_uses_description_when_set(
    yaml_storage: YAMLStorage,
) -> None:
    mem_id = await _seed_orders_with_memory(
        yaml_storage,
        learning="The full body that should NOT appear in compact mode.",
        description="amount is in cents",
    )
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="inspect_model", arguments={
        "model_name": "orders", "compact": True,
    })
    assert f"M{mem_id}" in result
    assert "amount is in cents" in result
    assert "full body" not in result


@pytest.mark.asyncio
async def test_compact_markdown_falls_back_to_first_paragraph(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders_with_memory(
        yaml_storage,
        learning="first paragraph.\n\nsecond paragraph body.",
    )
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="inspect_model", arguments={
        "model_name": "orders", "compact": True,
    })
    assert "first paragraph." in result
    assert "second paragraph body." not in result


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
    result = await _call(mcp, name="inspect_model", arguments={
        "model_name": "orders", "compact": False,
    })
    assert "first paragraph." in result
    assert "second paragraph body." in result


@pytest.mark.asyncio
async def test_compact_default_is_true(yaml_storage: YAMLStorage) -> None:
    await _seed_orders_with_memory(
        yaml_storage,
        learning="full body",
        description="preview",
    )
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="inspect_model", arguments={
        "model_name": "orders",
    })
    assert "preview" in result
    assert "full body" not in result


# ---------------------------------------------------------------------------
# JSON rendering — Learnings section flips symmetrically
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_json_learnings_uses_description(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders_with_memory(
        yaml_storage,
        learning="full body json",
        description="json preview",
    )
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="inspect_model", arguments={
        "model_name": "orders", "compact": True, "format": "json",
    })
    payload = json.loads(result)
    learnings = payload["learnings"]
    assert len(learnings) == 1
    entry = learnings[0]
    # In compact JSON, the field that holds the surfaced text is
    # ``description`` (the field name agents see); ``learning`` is omitted.
    assert entry["description"] == "json preview"
    assert "learning" not in entry


@pytest.mark.asyncio
async def test_compact_json_learnings_uses_fallback_when_no_description(
    yaml_storage: YAMLStorage,
) -> None:
    """Compact JSON fallback path: when Memory.description is None, the
    JSON description key holds the first-paragraph fallback of learning."""
    await _seed_orders_with_memory(
        yaml_storage,
        learning="first paragraph json.\n\nsecond paragraph body.",
    )
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="inspect_model", arguments={
        "model_name": "orders", "compact": True, "format": "json",
    })
    payload = json.loads(result)
    entry = payload["learnings"][0]
    assert entry["description"] == "first paragraph json."
    assert "learning" not in entry


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
    result = await _call(mcp, name="inspect_model", arguments={
        "model_name": "orders", "compact": False, "format": "json",
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
    tool: Any = next(t for t in tools if t.name == "inspect_model")
    schema = tool.inputSchema
    assert "compact" in schema["properties"]


@pytest.mark.asyncio
async def test_tool_descriptor_omits_reachable_fields_depth_arg(
    yaml_storage: YAMLStorage,
) -> None:
    """DEV-1560: the ``reachable_fields_depth`` kwarg was removed from the
    ``inspect_model`` signature. The generated MCP tool schema is a
    caller-facing surface — a regression that re-introduces the kwarg
    would silently re-document a dropped knob. Pin its absence here.
    """
    mcp = create_mcp_server(storage=yaml_storage)
    tools = await mcp.list_tools()
    tool: Any = next(t for t in tools if t.name == "inspect_model")
    schema = tool.inputSchema
    assert "reachable_fields_depth" not in schema["properties"]
