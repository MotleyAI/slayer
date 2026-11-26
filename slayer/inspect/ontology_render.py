"""Rendering of stored ontology and mapping documents for ``inspect``."""

from __future__ import annotations

import json
from typing import Any

from slayer.core.document_text import parse_document_text
from slayer.core.errors import UnreadableDocumentError
from slayer.ontology.ontology import Ontology
from slayer.ontology.use import ConceptRef, DatasetAnnotations, FieldAnnotation, RelationshipAnnotation


def _data(text: str) -> dict[str, Any]:
    try:
        data = parse_document_text(text)
    except UnreadableDocumentError:
        return {}
    return data if isinstance(data, dict) else {}


def _names(items: Any, key: str) -> list[str]:
    return [str(i.get(key)) for i in items if isinstance(i, dict)] if isinstance(items, list) else []


def ontology_document_view(*, kind: str, name: str, data_source: str | None, text: str,
                           problems: list[str] | None, compact: bool, fmt: str,
                           fields: dict[str, list[str]] | None = None) -> str:
    """``inspect``'s view of an ontology (``data_source`` ``None``; its concept graph, ``fields`` the canonical ids
    annotated with each concept) or a mapping, in ``fmt``."""
    data = _data(text)
    mapping = kind == "ontology_mapping"
    names = _names(data.get("concept_mappings" if mapping else "ontology"), "concept")
    payload: dict[str, Any] = {"name": name, "entity_type": kind, "description": data.get("description"),
                               "concept_mappings" if mapping else "concepts": names}
    if mapping:
        payload.update(data_source=data_source, ontology=(data.get("ontology_ref") or {}).get("name"),
                       semantic_model=(data.get("semantic_model_ref") or {}).get("name"),
                       resolves=not problems, problems=problems or [])
    else:
        payload["concept_graph"] = concept_graph(Ontology.of(data), fields=fields or {})
    if not compact:
        payload["text"] = text
    if fmt == "json":
        return json.dumps(payload)
    title = f"# Mapping `{name}` (datasource `{data_source}`)" if mapping else f"# Ontology `{name}`"
    lines = [title, *([str(payload["description"])] if payload["description"] else []), ""]
    lines += _mapping_lines(payload, names=names, problems=problems or []) if mapping else _graph_lines(
        payload["concept_graph"])
    if not compact:
        lines += ["", "```yaml", text.rstrip("\n"), "```"]
    return "\n".join(lines)


def _listed(names: list[str]) -> str:
    return ", ".join(f"`{n}`" for n in names) or "_(none)_"


def _mapping_lines(payload: dict[str, Any], *, names: list[str], problems: list[str]) -> list[str]:
    return [f"Ontology: `{payload['ontology']}`; semantic model: `{payload['semantic_model']}`",
            "Concept mappings: " + _listed(names), "Unresolved:" if problems else "Resolves.",
            *(f"- {p}" for p in problems)]


def concept_graph(ontology: Ontology, *, fields: dict[str, list[str]]) -> list[dict[str, Any]]:
    """Each concept of ``ontology`` as declared, its relationships and the ``fields`` annotated with it."""
    return [{"concept": concept, "type": kind, "extends": ontology.supertypes.get(concept, []),
             "description": ontology.descriptions.get(concept), "identify_by": ontology.identifiers.get(concept, []),
             "requires": ontology.requires.get(concept, []),
             "relationships": [{"name": r.name, "roles": r.roles, "multiplicity": r.multiplicity,
                                "verbalizes": r.verbalizes, "requires": r.requires}
                               for r in ontology.relationships.get(concept, {}).values()],
             "fields": sorted(fields.get(concept, []))}
            for concept, kind in ontology.kinds.items()]


def _graph_lines(graph: list[dict[str, Any]]) -> list[str]:
    lines = ["Concepts:"]
    for entry in graph:
        parts = [f"- `{entry['concept']}` ({entry['type']})"
                 + (f" extends {_listed(entry['extends'])}" if entry["extends"] else "")
                 + (f": {entry['description']}" if entry["description"] else "")]
        if entry["identify_by"]:
            parts.append(f"identified by {_listed(entry['identify_by'])}")
        if entry["requires"]:
            parts.append(f"requires {_listed(entry['requires'])}")
        if entry["fields"]:
            parts.append(f"fields {_listed(entry['fields'])}")
        lines.append("; ".join(parts))
        for r in entry["relationships"]:
            roles = ", ".join(r["roles"])
            shown = [f"  - `{entry['concept']}.{r['name']}`" + (f" ({roles})" if roles else "")
                     + (f" {r['multiplicity']}" if r["multiplicity"] else "")]
            shown += [f"`{v}`" for v in r["verbalizes"]]
            shown += [f"requires {_listed(r['requires'])}"] if r["requires"] else []
            lines.append(": ".join(shown[:2]) + "".join(f"; {p}" for p in shown[2:]))
    return lines


def _refs(refs: list[ConceptRef]) -> str:
    return ", ".join(f"`{r.concept}` (`{r.ontology}`, mapping `{r.mapping}`)" for r in refs)


def dataset_concepts_payload(held: DatasetAnnotations) -> dict[str, Any]:
    """A dataset's own and also-populated concepts, for inspect json."""
    return {"own_concepts": [r.model_dump() for r in held.own],
            "also_populated": [r.model_dump() for r in held.also]}


def dataset_concept_lines(held: DatasetAnnotations) -> list[str]:
    """A dataset's own and also-populated concepts, for inspect markdown."""
    return [*([f"Concepts: {_refs(held.own)}"] if held.own else []),
            *([f"Also populates: {_refs(held.also)}"] if held.also else [])]


def field_concept_lines(annotations: list[FieldAnnotation]) -> list[str]:
    """A field's concept annotations, for inspect markdown."""
    lines: list[str] = []
    for a in annotations:
        where = f" of `{a.entity}`" if a.entity and a.role != "value" else ""
        via = f" in `{a.relationship}`" if a.relationship else ""
        lines.append(f"- `{a.concept}` ({' → '.join(a.chain)}): {a.role}{where}{via}; ontology `{a.ontology}`, "
                     f"mapping `{a.mapping}`")
        lines += [f"  {line}" for line in (a.description, a.verbalization) if line]
        lines += [f"  Expression: `{a.expression}`"] if a.expression else []
        lines += [f"  Domain: {'; '.join(a.domains)}"] if a.domains else []
        lines += [f"  Not checked: {'; '.join(a.not_checked)}"] if a.not_checked else []
    return ["Concepts:", *lines] if lines else []


def relationship_concept_lines(name: str, annotations: list[RelationshipAnnotation]) -> list[str]:
    """The ontology relationships a dataset relationship lines up with, for inspect markdown."""
    return [f"- `{name}`: `{a.relationship}`" + (f" ({a.multiplicity})" if a.multiplicity else "")
            + (f" — {a.verbalization}" if a.verbalization else "") + f"; mapping `{a.mapping}`" for a in annotations]
