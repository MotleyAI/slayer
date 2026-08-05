"""Shared helpers for search-related tests."""

from __future__ import annotations

from typing import Any

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage.base import StorageBackend


async def seed_warehouse_models(storage: StorageBackend) -> None:
    """Seed the standard warehouse datasource + orders + customers models.

    Centralised here to avoid Sonar duplication-density failures across
    tests that each need the same model corpus.
    """
    await storage.save_datasource(
        DatasourceConfig(name="warehouse", type="sqlite", database=":memory:")
    )
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="warehouse",
        description="Checkout orders.",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount_paid", type=DataType.DOUBLE,
                   description="Net paid in USD."),
            Column(name="status", type=DataType.TEXT,
                   description="paid|refunded|cancelled."),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="warehouse",
        description="Customer master data.",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="email", type=DataType.TEXT),
        ],
    ))


async def call_mcp_tool(*, mcp: Any, name: str, arguments: dict) -> str:
    """Invoke an MCP tool and return its text result."""
    result = await mcp.call_tool(name, arguments)
    if isinstance(result, tuple):
        candidates: list = list(result[0]) if result else []
    elif isinstance(result, list):
        candidates = result
    elif hasattr(result, "content"):
        candidates = list(result.content)
    else:
        return str(result)
    for block in candidates:
        if hasattr(block, "text"):
            return block.text
    return str(result)
