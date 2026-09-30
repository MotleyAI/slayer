"""The MCP ``query`` tool documents the time-point forms."""

from __future__ import annotations

from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage


async def test_query_tool_lists_time_point_forms(tmp_path) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)))
    tools = {t.name: t for t in await server.list_tools()}
    doc = tools["query"].description or ""
    for needle in ("2025-Q1", "2025-W05", "last month", "last 7 days", "year to date", "in '2025-Q1'"):
        assert needle in doc, f"query tool docs must show {needle!r}"
    lowered = doc.lower()
    assert "excludes the current" in lowered
    assert "one-sided" in lowered
    assert "whole day" in lowered
