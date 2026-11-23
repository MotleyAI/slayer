"""The MCP query texts, server instructions and help topics document only the qualified syntax.

Spec: mcp/tool-surface › Documented examples are written qualified.
"""

from __future__ import annotations

import asyncio
import json
import re
import tempfile
import warnings
from collections.abc import AsyncIterator, Iterator
from functools import cache
from typing import Any

import pytest

from slayer.core.deprecation import DeprecatedInputFutureWarning
from slayer.core.query import SlayerQuery
from slayer.core.syntax import AggCall, DateTrunc, Ref, TransformCall, parse_expr, walk_parsed
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.memories.help_seed import load_help_topics
from slayer.storage.yaml_storage import YAMLStorage

from tests._doc_mode_b_examples import mode_b_values
from tests._mcp_idiom_fixtures import idiom_datasets, make_idiom_engine

_STORE = tempfile.TemporaryDirectory()
_CALL_START = re.compile(r"\b[A-Za-z_]\w*\(")
#: A definition's expression (an extension or saved metric) reads its own dataset's fields.
_DEFINITION = re.compile(r'"expression"\s*:\s*"[^"]*"')
_JSON_FENCE = re.compile(r"^```json\s*\n(.*?)^```", re.S | re.M)
#: Single-letter names are signature metavariables (`count(x)`), not references.
_METAVARIABLE = re.compile(r"^[A-Za-z]$")
_QUERY_KEYS = frozenset({"population", "dimensions", "time_dimensions", "metrics", "filter", "order"})
_ENTRY_KEYS = ("dimensions", "time_dimensions", "metrics")


@cache
def _server_and_tools() -> tuple[Any, dict[str, Any]]:
    server = create_mcp_server(storage=YAMLStorage(base_dir=_STORE.name), _seed_help=False)
    return server, {t.name: t for t in asyncio.run(server.list_tools())}


def _schema_descriptions(node: Any, path: tuple[str, ...] = ()) -> Iterator[tuple[str, str]]:
    if isinstance(node, dict):
        for key, child in node.items():
            if key == "description" and isinstance(child, str):
                yield "/".join(path), child
            else:
                yield from _schema_descriptions(child, (*path, key))
    elif isinstance(node, list):
        for i, child in enumerate(node):
            yield from _schema_descriptions(child, (*path, str(i)))


@cache
def _texts() -> dict[str, str]:
    """Source label → text: the query tool's description and schema, the instructions, every help topic."""
    server, tools = _server_and_tools()
    query = tools["query"]
    out = {"query tool description": query.description or "", "server instructions": server.instructions or ""}
    out.update({f"query schema {path}": text for path, text in _schema_descriptions(query.inputSchema)})
    out.update({f"help topic {t.id}": t.learning for t in load_help_topics()})
    return out


SOURCES = sorted(_texts())


def _calls(text: str) -> Iterator[str]:
    """Every balanced ``name(...)`` substring of ``text``."""
    for match in _CALL_START.finditer(text):
        depth = 0
        for i in range(match.end() - 1, len(text)):
            depth += {"(": 1, ")": -1}.get(text[i], 0)
            if depth == 0:
                yield text[match.start(): i + 1]
                break


def _bare_names(expression: str, *, allowed: frozenset[str] = frozenset()) -> list[str]:
    """The unquoted, undotted references in ``expression`` other than metavariables and ``allowed`` aliases."""
    return sorted({
        n.name for n in walk_parsed(parse_expr(expression))
        if isinstance(n, Ref) and not n.quoted and not _METAVARIABLE.match(n.name) and n.name not in allowed
    })


def _expression_violations(text: str) -> list[tuple[str, list[str]]]:
    """Aggregate, transform and ``date_trunc`` examples in ``text`` that read a bare name."""
    tools = set(_server_and_tools()[1])
    out: list[tuple[str, list[str]]] = []
    for call in _calls(_DEFINITION.sub('""', text)):
        try:
            parsed = parse_expr(call)
        except ValueError:
            continue
        if isinstance(parsed, AggCall) and parsed.agg in tools:
            continue
        if not any(isinstance(n, (AggCall, TransformCall, DateTrunc)) for n in walk_parsed(parsed)):
            continue
        if bare := _bare_names(call):
            out.append((call, bare))
    return out


def _json_examples() -> list[tuple[str, Any]]:
    """``(source, value)`` of every JSON fence holding a query object or a stage list."""
    out: list[tuple[str, Any]] = []
    for source, text in _texts().items():
        for block in _JSON_FENCE.findall(text):
            value = json.loads(block)
            stages = value if isinstance(value, list) else [value]
            if stages and all(isinstance(s, dict) and _QUERY_KEYS & s.keys() for s in stages):
                out.append((source, value))
    return out


def _aliases(stage: dict[str, Any]) -> frozenset[str]:
    return frozenset(
        e["alias"] for key in _ENTRY_KEYS for e in stage.get(key) or [] if isinstance(e, dict) and e.get("alias")
    )


JSON_EXAMPLES = _json_examples()


# --------------------------------------------------------------------------- #
# Collected texts
# --------------------------------------------------------------------------- #
def test_every_surface_is_collected() -> None:
    assert {"query tool description", "server instructions"} <= set(SOURCES)
    assert any(s.startswith("query schema $defs/SlayerQuery/properties/metrics") for s in SOURCES)
    assert sum(s.startswith("help topic ") for s in SOURCES) == len(load_help_topics())
    assert JSON_EXAMPLES, "no JSON query example found in any help topic"


@pytest.mark.parametrize("source", SOURCES)
def test_no_text_documents_an_unqualified_row_count(source: str) -> None:
    assert "count(*)" not in _texts()[source]


@pytest.mark.parametrize("source", SOURCES)
def test_expression_examples_name_no_bare_field(source: str) -> None:
    assert _expression_violations(_texts()[source]) == []


# --------------------------------------------------------------------------- #
# Whole query examples
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("source", "example"), JSON_EXAMPLES, ids=[s for s, _ in JSON_EXAMPLES])
def test_query_examples_qualify_every_reference(source: str, example: Any) -> None:
    stages = example if isinstance(example, list) else [example]
    bare = {text: names for stage in stages for text in mode_b_values(stage)
            if (names := _bare_names(text, allowed=_aliases(stage)))}
    assert bare == {}, source


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    async for e in make_idiom_engine():
        yield e


@pytest.mark.parametrize(("source", "example"), JSON_EXAMPLES, ids=[s for s, _ in JSON_EXAMPLES])
async def test_query_examples_run_without_a_deprecated_input(
    engine: SlayerQueryEngine, source: str, example: Any,
) -> None:
    stages = example if isinstance(example, list) else [example]
    names = {s["name"] for s in stages if s.get("name")}
    seeded = {d.name for d in idiom_datasets()}
    assert {s["population"] for s in stages if s.get("population") not in names} <= seeded, source
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        resp = await engine.execute(query=[SlayerQuery.model_validate(s) for s in stages])
    assert not [w for w in caught if isinstance(w.message, DeprecatedInputFutureWarning)], source
    assert not [w for w in resp.warnings or [] if getattr(w, "kind", None) == "deprecated_input"], source
