"""What the MCP server advertises fits Claude Code's view: ≤2048-char descriptions, full input schemas."""

from __future__ import annotations

import asyncio
import inspect
import json
import re
import tempfile
from functools import cache
from typing import Annotated, Any

import pytest
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

import slayer.mcp.server as server_mod
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

from tests._mcp_idiom_fixtures import EXAMPLE_OWNERS

DESCRIPTION_BUDGET = 2048
# The query schema ships with every request; set from the size measured after the rewrite + ~10%.
QUERY_SCHEMA_BUDGET = 28_000
FIVE_TOPICS = {"help.aggregations", "help.transforms", "help.time", "help.joins", "help.queries"}

#: Distinctive guidance phrase → its one owning ($defs model, field) in the query schema.
MARKER_OWNERS: dict[str, tuple[str, str]] = {
    "partition_by=[]": ("SlayerQuery", "measures"),
    "window='90d'": ("SlayerQuery", "measures"),
    "'3m'": ("SlayerQuery", "measures"),
    "change_pct(": ("SlayerQuery", "measures"),
    "lag(": ("SlayerQuery", "measures"),
    "lead(": ("SlayerQuery", "measures"),
    "dense_rank(": ("SlayerQuery", "measures"),
    "ntile(": ("SlayerQuery", "measures"),
    "consecutive_periods(": ("SlayerQuery", "measures"),
    "rows before the range": ("TimeDimension", "date_range"),
    "range start": ("TimeDimension", "date_range"),
    **EXAMPLE_OWNERS,
}


_STORE = tempfile.TemporaryDirectory()


@cache
def _server_and_tools() -> tuple[Any, dict[str, Any]]:
    server = create_mcp_server(storage=YAMLStorage(base_dir=_STORE.name), _seed_help=False)
    return server, {t.name: t for t in asyncio.run(server.list_tools())}


def _tools() -> dict[str, Any]:
    return _server_and_tools()[1]


TOOL_NAMES = sorted(_tools())


def _descriptions(node: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str]]:
    """Every string ``description`` in a JSON schema, with its path."""
    out: list[tuple[tuple[str, ...], str]] = []
    if isinstance(node, dict):
        for key, child in node.items():
            if key == "description" and isinstance(child, str):
                out.append((path, child))
            else:
                out.extend(_descriptions(child, (*path, key)))
    elif isinstance(node, list):
        for i, child in enumerate(node):
            out.extend(_descriptions(child, (*path, str(i))))
    return out


def _field_path(model: str, field: str) -> tuple[str, ...]:
    return ("$defs", model, "properties", field)


def _query_texts() -> dict[tuple[str, ...], str]:
    query = _tools()["query"]
    texts = dict(_descriptions(query.inputSchema))
    texts[("<tool description>",)] = query.description or ""
    return texts


def _inspect_call_refs(text: str) -> list[set[str]]:
    """The ``memory:help.*`` ids inside each ``inspect(...)`` call in ``text``."""
    return [
        set(re.findall(r"memory:(help(?:\.\w+)+)", call))
        for call in re.findall(r"inspect\((.*?)\)", text, re.S)
    ]


# --------------------------------------------------------------------------- #
# Tool descriptions
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("name", TOOL_NAMES)
def test_description_within_budget(name: str) -> None:
    assert len(_tools()[name].description or "") <= DESCRIPTION_BUDGET


@pytest.mark.parametrize("name", TOOL_NAMES)
def test_description_is_dedented(name: str) -> None:
    description = _tools()[name].description or ""
    assert description == inspect.cleandoc(description)


@pytest.mark.parametrize("name", TOOL_NAMES)
def test_description_has_no_args_section(name: str) -> None:
    assert not re.search(r"^\s*Args:\s*$", _tools()[name].description or "", re.M)


@pytest.mark.parametrize("name", TOOL_NAMES)
def test_every_parameter_described(name: str) -> None:
    props = _tools()[name].inputSchema.get("properties", {})
    undescribed = [p for p, schema in props.items() if not (schema.get("description") or "").strip()]
    assert undescribed == []


def test_budget_constant_is_the_claude_code_default() -> None:
    assert server_mod.MAX_TOOL_DESCRIPTION_CHARS == DESCRIPTION_BUDGET


def test_over_budget_description_fails() -> None:
    def too_long() -> None:
        pass

    too_long.__doc__ = "x" * (DESCRIPTION_BUDGET + 1)
    with pytest.raises(ValueError) as ei:
        server_mod._agent_description(too_long)
    assert "too_long" in str(ei.value)
    assert str(DESCRIPTION_BUDGET + 1) in str(ei.value)


def test_agent_description_dedents_the_docstring() -> None:
    def tool() -> None:
        """First line.

        Indented body.
        """

    assert server_mod._agent_description(tool) == "First line.\n\nIndented body."


# --------------------------------------------------------------------------- #
# Server instructions
# --------------------------------------------------------------------------- #
def test_instructions_within_budget_and_carry_the_help_call() -> None:
    instructions = _server_and_tools()[0].instructions or ""
    assert len(instructions) <= DESCRIPTION_BUDGET
    assert any(FIVE_TOPICS <= refs for refs in _inspect_call_refs(instructions))
    lowered = instructions.lower()
    for stem in ("share", "rank", "window", "period"):
        assert stem in lowered, stem
    assert "joined" in lowered or "cross-model" in lowered


def test_instructions_come_from_the_module_constant() -> None:
    assert _server_and_tools()[0].instructions == server_mod.SERVER_INSTRUCTIONS


def test_over_budget_instructions_fail_the_build(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(server_mod, "SERVER_INSTRUCTIONS", "x" * (DESCRIPTION_BUDGET + 1))
    storage = YAMLStorage(base_dir=str(tmp_path))
    with pytest.raises(ValueError, match="instructions"):
        create_mcp_server(storage=storage, _seed_help=False)


# --------------------------------------------------------------------------- #
# Description mechanism
# --------------------------------------------------------------------------- #
class _Payload(BaseModel):
    x: int = 0


async def test_fastmcp_description_kwarg_and_annotated_field_description() -> None:
    mcp = FastMCP("probe")

    @mcp.tool(description="replaced")
    async def probe(p: Annotated[_Payload | None, Field(description="param text")] = None) -> str:
        """Docstring text."""
        return ""

    tool = next(t for t in await mcp.list_tools() if t.name == "probe")
    assert tool.description == "replaced"
    prop = tool.inputSchema["properties"]["p"]
    assert prop["description"] == "param text"
    assert "anyOf" in prop or "$ref" in prop


def test_refine_property_carries_its_own_description() -> None:
    prop = _tools()["query"].inputSchema["properties"]["refine"]
    assert (prop.get("description") or "").strip()
    assert "anyOf" in prop or "$ref" in prop


# --------------------------------------------------------------------------- #
# The query tool description is a capability map
# --------------------------------------------------------------------------- #
def test_query_description_points_to_fields_help_refine_and_cap() -> None:
    description = _tools()["query"].description or ""
    for field in ("measures", "filters", "order", "time_dimensions[].date_range", "source_model", "dimensions"):
        assert field in description, field
    assert any(FIVE_TOPICS <= refs for refs in _inspect_call_refs(description))
    assert "refine" in description
    assert "20 rows" in description
    lowered = description.lower()
    for stem in (
        "join", "share", "grand total", "rank", "top", "display", "period-over-period",
        "running total", "trailing window", "double counting", "nest", "stage", "prefer", "list",
    ):
        assert stem in lowered, stem


def test_query_schema_within_size_budget() -> None:
    size = len(json.dumps(_tools()["query"].inputSchema))
    assert size <= QUERY_SCHEMA_BUDGET, f"query inputSchema is {size} chars"


# --------------------------------------------------------------------------- #
# Query-language guidance lives on its owning field, once
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("marker", sorted(MARKER_OWNERS))
def test_guidance_marker_has_one_owner(marker: str) -> None:
    owner = _field_path(*MARKER_OWNERS[marker])
    texts = _query_texts()
    assert marker in texts.get(owner, ""), f"{marker!r} missing from {owner}"
    elsewhere = [path for path, text in texts.items() if path != owner and marker in text]
    assert elsewhere == []


def test_refinement_clauses_point_to_the_query_fields() -> None:
    defs = _tools()["query"].inputSchema["$defs"]
    query_props = defs["SlayerQuery"]["properties"]
    for field, schema in defs["QueryRefinement"]["properties"].items():
        text = schema.get("description") or ""
        assert field in text, field
        assert len(text) <= 120, (field, text)
        assert text != query_props[field].get("description"), field


def test_measure_formula_points_to_the_measures_field() -> None:
    text = _tools()["query"].inputSchema["$defs"]["ModelMeasure"]["properties"]["formula"].get("description") or ""
    assert "measures" in text
    assert len(text) <= 200, text


def test_date_range_is_described() -> None:
    text = _tools()["query"].inputSchema["$defs"]["TimeDimension"]["properties"]["date_range"].get("description") or ""
    for needle in ("change", "time_shift", "window=", "cumsum"):
        assert needle in text, needle
