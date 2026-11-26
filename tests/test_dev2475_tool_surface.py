"""The ``search`` and ``validate_datasets`` tool descriptions name the ontology use, within the budget.

Spec: mcp/tool-surface › Search and validate_datasets describe their ontology use within the budget.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

import slayer.mcp.server as server_mod
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(scope="module")
def tools(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    base: Path = tmp_path_factory.mktemp("mcp_ontology_use")
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(base)), _seed_help=False)
    return {t.name: t for t in asyncio.run(server.list_tools())}


def test_the_descriptions_fit_and_name_the_ontology_use(tools: dict[str, Any]) -> None:
    """Spec scenario: The descriptions fit and name the ontology use."""
    entities = tools["search"].input_schema["properties"]["entities"]["description"]
    assert "concept:<Concept>" in entities
    assert "concept:<Ontology>.<Concept>" in entities
    validate = tools["validate_datasets"].description or ""
    assert "ontology" in validate.lower()
    assert all(check in validate for check in ("identifier", "multiplicity", "constraint", "required", "not checked"))
    for name in ("search", "validate_datasets"):
        assert len(tools[name].description or "") <= server_mod.MAX_TOOL_DESCRIPTION_CHARS
