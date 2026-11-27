"""The semantic-model tools fit the surface rules; query guidance names bare model-scoped metrics and AGG.

Spec: mcp/tool-surface › The semantic-model tools fit the surface rules; Query guidance names bare model-scoped
metrics and AGG.
"""

from __future__ import annotations

import asyncio
import json
import re
from functools import cache
from pathlib import Path
from typing import Any

import pytest

import slayer.mcp.server as server_mod
from slayer.mcp.server import create_mcp_server
from slayer.memories.help_seed import load_help_topics
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev2243_fixtures import document_engine

NEW_TOOLS = {
    "create_semantic_model": {"name", "data_source", "content", "datasets"},
    "edit_semantic_model": {"name", "data_source", "description", "ai_context", "metrics", "remove_metrics",
                            "relationships", "remove_relationships", "add_datasets"},
    "delete_semantic_model": {"name", "data_source"},
}
METRICS_FIELD = ("$defs", "SlayerQuery", "properties", "metrics")
_SEMANTIC_MODEL = re.compile(r"semantic[ _-]model", re.IGNORECASE)
_AGG = re.compile(r"\bAGG\(\w+\.\w+\)")
_MODEL_SCOPED = re.compile(r"model-scoped", re.IGNORECASE)
_HELP_REF = re.compile(r"memory:(help(?:\.\w+)+)")
#: The documented bare-metric example's query, as the help topic writes it.
_EXAMPLE = re.compile(r'\{[^{}]*"metrics"\s*:\s*\[[^\]]*"revenue_per_customer"[^\]]*\][^{}]*\}')


@cache
def _tools(base: str) -> dict[str, Any]:
    server = create_mcp_server(storage=YAMLStorage(base_dir=base), _seed_help=False)
    return {t.name: t for t in asyncio.run(server.list_tools())}


@pytest.fixture(scope="module")
def tools(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return _tools(str(tmp_path_factory.mktemp("mcp_surface")))


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


def _all_text(tool: Any) -> str:
    return "\n".join([tool.description or "", *(text for _, text in _descriptions(tool.input_schema))])


def _model_scoped_topic() -> Any:
    """The help topic holding the long-form model-scoped metric reference."""
    hits = [t for t in load_help_topics() if _MODEL_SCOPED.search(t.learning) and _AGG.search(t.learning)]
    assert len(hits) == 1, [t.id for t in hits]
    return hits[0]


class TestTheSemanticModelTools:
    @pytest.mark.parametrize("name", sorted(NEW_TOOLS))
    def test_the_new_tools_fit_the_budget(self, tools: dict[str, Any], name: str) -> None:
        """Spec: mcp/tool-surface › The new tools fit the budget."""
        assert name in tools, sorted(tools)
        assert len(tools[name].description or "") <= server_mod.MAX_TOOL_DESCRIPTION_CHARS

    @pytest.mark.parametrize("name", sorted(NEW_TOOLS))
    def test_every_parameter_is_described(self, tools: dict[str, Any], name: str) -> None:
        """Spec: mcp/tool-surface › The new tools fit the budget (every parameter described)."""
        assert name in tools, sorted(tools)
        props = tools[name].input_schema.get("properties", {})
        assert NEW_TOOLS[name] <= set(props), sorted(props)
        undescribed = [p for p, schema in props.items() if not (schema.get("description") or "").strip()]
        assert undescribed == []

    def test_create_dataset_describes_semantic_model(self, tools: dict[str, Any]) -> None:
        """Spec: mcp/tool-surface › The semantic-model tools fit the surface rules (create_dataset)."""
        props = tools["create_dataset"].input_schema.get("properties", {})
        assert "semantic_model" in props, sorted(props)
        assert (props["semantic_model"].get("description") or "").strip()

    def test_edit_dataset_describes_semantic_model(self, tools: dict[str, Any]) -> None:
        """Spec: mcp/tool-surface › The semantic-model tools fit the surface rules (edit_dataset)."""
        assert _SEMANTIC_MODEL.search(_all_text(tools["edit_dataset"]))

    def test_delete_semantic_model_states_it_deletes_the_datasets(self, tools: dict[str, Any]) -> None:
        """Spec: mcp/tool-surface › The semantic-model tools fit the surface rules (delete_semantic_model)."""
        assert "delete_semantic_model" in tools, sorted(tools)
        description = (tools["delete_semantic_model"].description or "").lower()
        assert "delete" in description
        assert "datasets" in description


class TestQueryGuidance:
    @pytest.mark.parametrize("marker", [_AGG, _MODEL_SCOPED], ids=["agg", "model-scoped"])
    def test_the_metrics_field_alone_names_bare_model_scoped_metrics_and_agg(
        self, tools: dict[str, Any], marker: re.Pattern[str],
    ) -> None:
        """Spec: mcp/tool-surface › Query guidance names bare model-scoped metrics and AGG."""
        query = tools["query"]
        texts = [*_descriptions(query.input_schema), (("<tool description>",), query.description or "")]
        owners = [path for path, text in texts if marker.search(text)]
        assert owners == [METRICS_FIELD], owners

    def test_a_help_topic_holds_the_reference_reachable_from_the_instructions(self) -> None:
        """Spec: mcp/tool-surface › Query guidance names bare model-scoped metrics and AGG (help topic)."""
        topic = _model_scoped_topic()
        bodies = {t.id: t.learning for t in load_help_topics()}
        reached: set[str] = set()
        frontier = set(_HELP_REF.findall(server_mod.SERVER_INSTRUCTIONS))
        while frontier:
            current = frontier.pop()
            if current in reached or current not in bodies:
                continue
            reached.add(current)
            frontier |= set(_HELP_REF.findall(bodies[current]))
        assert topic.id in reached, (topic.id, sorted(reached))

    async def test_a_documented_bare_metric_query_works_as_documented(self, tmp_path: Path) -> None:
        """Spec: mcp/tool-surface › A documented bare-metric query works as documented (the ``sales`` store)."""
        topic = _model_scoped_topic()
        match = _EXAMPLE.search(topic.learning)
        assert match is not None, topic.learning
        query = json.loads(match.group(0))
        assert "162.5" in topic.learning
        async with document_engine(base=tmp_path) as engine:
            resp = await engine.execute(query)
        (row,) = resp.data
        assert float(row["revenue_per_customer"]) == pytest.approx(162.5)
