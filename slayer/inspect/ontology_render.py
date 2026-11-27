"""Rendering of stored ontology and mapping documents for ``inspect``."""

from __future__ import annotations

import json
from typing import Any

from slayer.core.document_text import parse_document_text
from slayer.core.errors import UnreadableDocumentError
from slayer.inspect.dataset_render import _truncate_description
from slayer.ontology.documents import ref_name
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
        payload.update(data_source=data_source, ontology=ref_name(data, "ontology_ref"),
                       semantic_model=ref_name(data, "semantic_model_ref"),
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
        lines.append(_concept_line(entry))
        lines += [_relationship_line(entry["concept"], r) for r in entry["relationships"]]
    return lines


def _concept_line(entry: dict[str, Any]) -> str:
    parts = [f"- `{entry['concept']}` ({entry['type']})"
             + (f" extends {_listed(entry['extends'])}" if entry["extends"] else "")
             + (f": {entry['description']}" if entry["description"] else "")]
    parts += [f"{label} {_listed(entry[key])}" for key, label in (
        ("identify_by", "identified by"), ("requires", "requires"), ("fields", "fields")) if entry[key]]
    return "; ".join(parts)


def _relationship_line(concept: str, r: dict[str, Any]) -> str:
    roles = ", ".join(r["roles"])
    shown = [f"  - `{concept}.{r['name']}`" + (f" ({roles})" if roles else "")
             + (f" {r['multiplicity']}" if r["multiplicity"] else "")]
    shown += [f"`{v}`" for v in r["verbalizes"]]
    shown += [f"requires {_listed(r['requires'])}"] if r["requires"] else []
    return ": ".join(shown[:2]) + "".join(f"; {p}" for p in shown[2:])


def _refs(refs: list[ConceptRef]) -> str:
    return ", ".join(f"`{r.concept}` (`{r.ontology}`, mapping `{r.mapping}`)" for r in refs)


def capped_descriptions(held: DatasetAnnotations, max_chars: int | None) -> DatasetAnnotations:
    """``held`` with each field annotation's concept description cut to ``max_chars`` (``None``: whole)."""
    if max_chars is None:
        return held
    return held.model_copy(update={"fields": {
        key: [a.model_copy(update={"description": _truncate_description(a.description, max_chars)}) for a in found]
        for key, found in held.fields.items()}})


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
