"""An invalid MCP ``query`` argument reports only the errors of the form its JSON type selects."""

from __future__ import annotations

import re
from typing import Any

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

ONE_ERROR = re.compile(r"\b1 validation error for\b")
_BAD_NAME_QUERY = {"source_model": "orders", "measures": [{"formula": "count(*)", "name": "bad name"}]}
_FIRST_STAGE = {"name": "a", "source_model": "orders", "measures": ["count(*)"]}


@pytest.fixture
def server(tmp_path) -> Any:
    return create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)), _seed_help=False)


async def _error(server: Any, query: Any) -> str:
    with pytest.raises(ToolError) as ei:
        await server.call_tool(name="query", arguments={"query": query})
    return str(ei.value)


async def test_invalid_query_object_reports_only_its_errors(server: Any) -> None:
    message = await _error(server, _BAD_NAME_QUERY)
    assert ONE_ERROR.search(message), message
    assert "Invalid name 'bad name'" in message
    assert "valid string" not in message
    assert "valid list" not in message


async def test_invalid_stage_reports_only_its_errors_at_that_stage(server: Any) -> None:
    message = await _error(server, [_FIRST_STAGE, _BAD_NAME_QUERY])
    assert ONE_ERROR.search(message), message
    assert "Invalid name 'bad name'" in message
    assert ".1.measures.0.name" in message
    assert "valid string" not in message
    assert "valid dictionary" not in message


async def test_string_still_runs_by_name(server: Any) -> None:
    message = await _error(server, "no_such_saved_query")
    assert "validation error" not in message
    assert "no_such_saved_query" in message


async def test_unsupported_json_type_reports_one_error(server: Any) -> None:
    assert ONE_ERROR.search(await _error(server, 42))
