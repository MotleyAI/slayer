"""The time-point forms are documented on the ``filters`` / ``date_range`` fields and in ``help.time``."""

from __future__ import annotations

from slayer.mcp.server import create_mcp_server
from slayer.memories.help_seed import HELP_TOPICS
from slayer.storage.yaml_storage import YAMLStorage


async def test_query_schema_and_help_list_time_point_forms(tmp_path) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)), _seed_help=False)
    tools = {t.name: t for t in await server.list_tools()}
    defs = tools["query"].inputSchema["$defs"]
    help_time = next(t for t in HELP_TOPICS if t.id == "help.time")
    doc = "\n".join((
        defs["SlayerQuery"]["properties"]["filters"].get("description") or "",
        defs["TimeDimension"]["properties"]["date_range"].get("description") or "",
        help_time.learning,
    ))
    for needle in ("2025-Q1", "2025-W05", "last month", "last 7 days", "year to date", "in '2025-Q1'"):
        assert needle in doc, f"time-point docs must show {needle!r}"
    lowered = doc.lower()
    assert "excludes the current" in lowered
    assert "one-sided" in lowered
    assert "whole day" in lowered
