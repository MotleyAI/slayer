"""DEV-1549: models_summary compact-by-default.

Tests cover:
* MCP tool ``models_summary(compact: bool = True)``.
* compact=True markdown renders: name, description, ``Columns: N`` line,
  ``Measures: <names>`` line, ``Joins to: ...``. NO per-column table,
  NO per-measure formula block.
* compact=True JSON renders the new shape (Codex#6):
  ``{name, description, column_count, measure_names, joins_to}``.
* compact=False markdown retains the verbose shape's load-bearing
  sections (column table, per-measure formula block).
* compact=False JSON retains the structured ``columns`` / ``measures``
  payloads.
* Empty datasource string is identical across modes.
* No-joins / no-measures branches.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from typing import Any, Generator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
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


async def _seed_orders(storage: YAMLStorage) -> None:
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="postgres", host="h")
    )
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="mydb",
        description="Orders fact.",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT, description="Order state."),
            Column(name="amount", type=DataType.DOUBLE,
                   description="USD amount."),
        ],
        measures=[
            ModelMeasure(name="revenue", formula="amount:sum"),
            ModelMeasure(name="orders_count", formula="*:count"),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))
    await storage.save_model(SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="mydb",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    ))


# ---------------------------------------------------------------------------
# compact=True markdown
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_markdown_keeps_name_and_description(
    yaml_storage: YAMLStorage,
) -> None:
    """The plan requires model name + description as the first two lines."""
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb", "compact": True,
    })
    assert "## `orders`" in result
    assert "Orders fact." in result


@pytest.mark.asyncio
async def test_compact_markdown_drops_per_column_table(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb", "compact": True,
    })
    # Today's verbose markdown shape — must NOT appear in compact mode.
    assert "**Columns (3):**" not in result
    assert "| status |" not in result
    assert "USD amount." not in result


@pytest.mark.asyncio
async def test_compact_markdown_emits_column_count_line(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb", "compact": True,
    })
    assert "Columns: 3" in result


@pytest.mark.asyncio
async def test_compact_markdown_emits_measure_names_only(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb", "compact": True,
    })
    assert "Measures: revenue, orders_count" in result
    # The formula must NOT leak into compact output.
    assert "amount:sum" not in result
    assert "*:count" not in result


@pytest.mark.asyncio
async def test_compact_markdown_default_is_true(
    yaml_storage: YAMLStorage,
) -> None:
    """Compact is the default — calling without ``compact`` returns
    the compact shape."""
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb",
    })
    assert "Columns: 3" in result
    assert "**Columns (3):**" not in result


@pytest.mark.asyncio
async def test_compact_markdown_joins_to_rendered(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb", "compact": True,
    })
    assert "Joins to: `customers`" in result


@pytest.mark.asyncio
async def test_compact_markdown_no_joins_marker(
    yaml_storage: YAMLStorage,
) -> None:
    await yaml_storage.save_datasource(
        DatasourceConfig(name="ds", type="postgres", host="h")
    )
    await yaml_storage.save_model(SlayerModel(
        name="solo", sql_table="t", data_source="ds",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    ))
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "ds", "compact": True,
    })
    assert "Joins to: _(none)_" in result


# ---------------------------------------------------------------------------
# compact=True JSON
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_json_shape(yaml_storage: YAMLStorage) -> None:
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb", "compact": True, "format": "json",
    })
    data = json.loads(result)
    assert data["datasource_name"] == "mydb"
    assert data["model_count"] == 2
    orders = next(m for m in data["models"] if m["name"] == "orders")
    assert set(orders.keys()) == {
        "name", "description", "column_count", "measure_names", "joins_to",
    }
    assert orders["description"] == "Orders fact."
    assert orders["column_count"] == 3
    assert orders["measure_names"] == ["revenue", "orders_count"]
    assert orders["joins_to"] == ["customers"]


@pytest.mark.asyncio
async def test_compact_json_no_columns_or_formulas(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb", "compact": True, "format": "json",
    })
    data = json.loads(result)
    orders = next(m for m in data["models"] if m["name"] == "orders")
    assert "columns" not in orders
    assert "measures" not in orders


# ---------------------------------------------------------------------------
# compact=False regression pin — verbose shape unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verbose_markdown_preserves_per_column_table(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb", "compact": False,
    })
    assert "**Columns (3):**" in result
    assert "| status |" in result
    assert "USD amount." in result
    assert "**Measures (2):**" in result
    assert "amount:sum" in result
    assert "*:count" in result


@pytest.mark.asyncio
async def test_verbose_json_keeps_full_column_and_measure_payloads(
    yaml_storage: YAMLStorage,
) -> None:
    await _seed_orders(yaml_storage)
    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "mydb", "compact": False, "format": "json",
    })
    data = json.loads(result)
    orders = next(m for m in data["models"] if m["name"] == "orders")
    assert "columns" in orders
    assert orders["columns"][0]["name"] == "id"
    assert "type" in orders["columns"][0]
    assert "measures" in orders
    revenue = next(mm for mm in orders["measures"] if mm["name"] == "revenue")
    assert revenue["formula"] == "amount:sum"


# ---------------------------------------------------------------------------
# Empty datasource — identical message across modes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_datasource_message_identical_across_modes(
    yaml_storage: YAMLStorage,
) -> None:
    await yaml_storage.save_datasource(
        DatasourceConfig(name="emptyds", type="postgres", host="h")
    )
    mcp = create_mcp_server(storage=yaml_storage)
    compact = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "emptyds", "compact": True,
    })
    verbose = await _call(mcp, name="models_summary", arguments={
        "datasource_name": "emptyds", "compact": False,
    })
    assert compact == verbose
    assert "has no models" in compact


# ---------------------------------------------------------------------------
# Tool descriptor exposes compact arg
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_descriptor_lists_compact_arg(
    yaml_storage: YAMLStorage,
) -> None:
    mcp = create_mcp_server(storage=yaml_storage)
    tools = await mcp.list_tools()
    tool: Any = next(t for t in tools if t.name == "models_summary")
    schema = tool.inputSchema
    assert "compact" in schema["properties"]
