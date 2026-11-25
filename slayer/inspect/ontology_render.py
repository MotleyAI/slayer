"""Rendering of stored ontology and mapping documents for ``inspect``."""

from __future__ import annotations

import json
from typing import Any

from slayer.core.document_text import parse_document_text
from slayer.core.errors import UnreadableDocumentError


def _data(text: str) -> dict[str, Any]:
    try:
        data = parse_document_text(text)
    except UnreadableDocumentError:
        return {}
    return data if isinstance(data, dict) else {}


def _names(items: Any, key: str) -> list[str]:
    return [str(i.get(key)) for i in items if isinstance(i, dict)] if isinstance(items, list) else []


def ontology_document_view(*, kind: str, name: str, data_source: str | None, text: str,
                           problems: list[str] | None, compact: bool, fmt: str) -> str:
    """``inspect``'s view of an ontology (``data_source`` ``None``) or a mapping, in ``fmt``."""
    data = _data(text)
    mapping = kind == "ontology_mapping"
    names = _names(data.get("concept_mappings" if mapping else "ontology"), "concept")
    payload: dict[str, Any] = {"name": name, "entity_type": kind, "description": data.get("description"),
                               "concept_mappings" if mapping else "concepts": names}
    if mapping:
        payload.update(data_source=data_source, ontology=(data.get("ontology_ref") or {}).get("name"),
                       semantic_model=(data.get("semantic_model_ref") or {}).get("name"),
                       resolves=not problems, problems=problems or [])
    if not compact:
        payload["text"] = text
    if fmt == "json":
        return json.dumps(payload)
    title = f"# Mapping `{name}` (datasource `{data_source}`)" if mapping else f"# Ontology `{name}`"
    lines = [title, *([str(payload["description"])] if payload["description"] else []), ""]
    if mapping:
        lines.append(f"Ontology: `{payload['ontology']}`; semantic model: `{payload['semantic_model']}`")
        lines.append("Concept mappings: " + (", ".join(f"`{n}`" for n in names) or "_(none)_"))
        lines.append("Resolves." if not problems else "Unresolved:")
        lines += [f"- {p}" for p in problems or []]
    else:
        lines.append("Concepts: " + (", ".join(f"`{n}`" for n in names) or "_(none)_"))
    if not compact:
        lines += ["", "```yaml", text.rstrip("\n"), "```"]
    return "\n".join(lines)
