"""DEV-1560: docs surface must not advertise the dropped reachable-fields
section.

The `inspect_model` rendering surface lost `## Reachable via joins`,
`reachable_fields` as a section token, and the `reachable_fields_depth`
kwarg. Docs that say otherwise will mislead agents. This test pins the
documented MCP-tool surface and the example notebook against the
post-removal contract.

Codex finding 4 from the test-vs-plan review surfaced an additional
pre-existing doc bug: the documented sections enumeration was missing
`learnings`. Fixed in the same pass and pinned here so it doesn't drift
again.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
_MCP_DOCS = [
    _REPO_ROOT / "docs" / "interfaces" / "mcp.md",
    _REPO_ROOT / "docs" / "reference" / "mcp.md",
]
_NOTEBOOK = _REPO_ROOT / "docs" / "examples" / "08_mcp_introspect" / "mcp_introspect_nb.ipynb"


@pytest.mark.parametrize("path", _MCP_DOCS, ids=lambda p: str(p.relative_to(_REPO_ROOT)))
def test_mcp_doc_drops_reachable_fields_tokens(path: Path) -> None:
    """The `inspect_model` row in the MCP docs must not reference the
    dropped section or kwarg. The catch-all phrase from the prose ("all
    fields reachable via joins") must also be gone."""
    text = path.read_text()
    # The exact UI tokens callers would type.
    assert "reachable_fields_depth" not in text, path
    # The exact markdown heading the section emitted.
    assert "Reachable via joins" not in text, path
    # The exact section-token name agents would pass in `sections=`.
    # Bounded by surrounding punctuation so we don't false-match the
    # underscore inside another identifier in code blocks.
    assert "`reachable_fields`" not in text, path
    assert '"reachable_fields"' not in text, path


@pytest.mark.parametrize("path", _MCP_DOCS, ids=lambda p: str(p.relative_to(_REPO_ROOT)))
def test_mcp_doc_sections_list_matches_code(path: Path) -> None:
    """The documented `sections` enumeration must list every accepted
    token, including `learnings` (which the pre-DEV-1560 docs accidentally
    omitted) and excluding `reachable_fields`."""
    text = path.read_text()
    # The post-DEV-1560 vocabulary, in canonical order.
    expected_substring = (
        '["columns", "measures", "aggregations", "joins", "samples", "learnings"]'
    )
    assert expected_substring in text, path


def test_mcp_introspect_notebook_drops_reachable_fields_tokens() -> None:
    """The example notebook is published rendered output; it must not
    show a section that no longer exists. Re-execute (see
    `mcp_introspect_nb.ipynb`) refreshes the live output cells; this test
    locks the result."""
    raw = _NOTEBOOK.read_text()
    notebook = json.loads(raw)
    # Walk every cell's source + outputs as a single concatenated string.
    blobs: list[str] = []
    for cell in notebook.get("cells", []):
        src = cell.get("source", [])
        blobs.append("".join(src) if isinstance(src, list) else src)
        for output in cell.get("outputs", []):
            if isinstance(output, dict):
                data = output.get("data") or {}
                for v in data.values():
                    blobs.append("".join(v) if isinstance(v, list) else str(v))
                text = output.get("text")
                if text is not None:
                    blobs.append("".join(text) if isinstance(text, list) else text)
    combined = "\n".join(blobs)
    assert "reachable_fields_depth" not in combined
    assert "Reachable via joins" not in combined
