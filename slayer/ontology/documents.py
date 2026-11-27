"""Ontology and mapping documents as text: recognition, the save-time checks and structured edits."""

from __future__ import annotations

from typing import Any, Literal

import yaml

from slayer.core.document_text import edit_list_items, edit_text, parse_document_text
from slayer.core.errors import OntologyDocumentInvalidError, OntologyMappingInvalidError, UnreadableDocumentError
from slayer.core.semantic_model import OSSIE_VERSION, ossie_validity_errors
from slayer.core.slayer_extension import checked
from slayer.ontology.ontology import ontology_problems
from slayer.ontology.schema import schema_errors

MAPPING_KEYS = ("concept_mappings", "ontology_ref", "semantic_model_ref")
DocumentKind = Literal["semantic_model", "ontology", "mapping"]
UNSET: Any = object()


def document_kind(data: Any) -> DocumentKind | None:
    """Which Ossie document ``data`` is by its root keys; ``None`` for none of them."""
    if not isinstance(data, dict):
        return None
    if "ontology" in data:
        return "ontology"
    if "datasets" in data or "semantic_model" in data:
        return "semantic_model"
    return "mapping" if all(k in data for k in MAPPING_KEYS) else None


def _read(text: str) -> tuple[Any, list[str]]:
    try:
        return parse_document_text(text), []
    except UnreadableDocumentError as exc:
        return None, list(exc.reasons)


def _version(data: dict[str, Any]) -> list[str]:
    version = data.get("version")
    if version is not None and str(version) == OSSIE_VERSION:
        return []
    return [f"version {version!r} is not the supported Ossie version {OSSIE_VERSION!r}"]


def _inline_model_problems(data: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for index, mapping in enumerate(data.get("ontology_mappings") or []):
        model = mapping.get("semantic_model") if isinstance(mapping, dict) else None
        out += [f"ontology_mappings -> {index} -> semantic_model: {r}" for r in ossie_validity_errors(model)]
    return out


def ontology_reasons(data: Any) -> list[str]:
    """Every reason ``data`` is not a valid ontology document; empty when it is."""
    if not isinstance(data, dict) or "ontology" not in data:
        return ["the document has no root `ontology`, so it is not an ontology document"]
    reasons = _version(data) or schema_errors(data, kind="ontology")
    return reasons or ontology_problems(data) + _inline_model_problems(data)


def _extension_reasons(data: dict[str, Any]) -> list[str]:
    return [f"nonportable_extensions -> {i}: `data` holds a value that is not JSON"
            for i, entry in enumerate(data.get("nonportable_extensions") or [])
            if isinstance(entry, dict) and isinstance(entry.get("data"), dict)
            and checked(vendor_name=entry.get("vendor_name") or "?", data=entry["data"]) is None]


def mapping_shape_reasons(data: Any) -> list[str]:
    """Every reason ``data`` is not a schema-valid mapping document, before its references are resolved."""
    missing = [k for k in MAPPING_KEYS if not isinstance(data, dict) or k not in data]
    if missing:
        return [f"the document is not a mapping document: it has no root {', '.join(f'`{k}`' for k in missing)}"]
    return _version(data) or schema_errors(data, kind="mapping") or _extension_reasons(data)


def read_ontology(text: str) -> tuple[Any, list[str]]:
    """``text`` as data, and every reason it is not a valid ontology document."""
    data, reasons = _read(text)
    return data, reasons or ontology_reasons(data)


def checked_ontology(text: str, *, name: str) -> dict[str, Any]:
    """``text`` read as a valid ontology document; raises ``OntologyDocumentInvalidError`` with every reason."""
    data, reasons = read_ontology(text)
    if reasons:
        raise OntologyDocumentInvalidError(name=name, reasons=reasons)
    return data


def stored_name_reasons(data: Any, *, name: str) -> list[str]:
    """Why document ``data`` cannot be stored as ``name``: it declares another name."""
    declared = data.get("name") if isinstance(data, dict) else None
    if declared is None or declared == name:
        return []
    return [f"its name {declared!r} differs from the name it is stored under ({name!r})"]


def checked_mapping_shape(text: str, *, name: str, data_source: str) -> dict[str, Any]:
    """``text`` read as a schema-valid mapping document; raises ``OntologyMappingInvalidError`` with every reason."""
    data, reasons = _read(text)
    reasons = reasons or mapping_shape_reasons(data) or stored_name_reasons(data, name=name)
    if reasons:
        raise OntologyMappingInvalidError(name=name, data_source=data_source, reasons=reasons)
    return data


def content_text(content: str | dict[str, Any]) -> str:
    """Document ``content`` given as text or data, as text."""
    return content if isinstance(content, str) else yaml.safe_dump(content, sort_keys=False, allow_unicode=True)


def ref_name(data: Any, key: str) -> str | None:
    """The ``name`` of reference ``key`` of stored mapping ``data``, whatever its shape; ``None`` when absent."""
    ref = data.get(key) if isinstance(data, dict) else None
    name = ref.get("name") if isinstance(ref, dict) else None
    return None if name is None else str(name)


def document_name(text: str) -> str:
    """The root ``name`` a document's text declares."""
    data, reasons = _read(text)
    name = data.get("name") if isinstance(data, dict) else None
    if not isinstance(name, str) or not name:
        raise ValueError("; ".join(reasons) or "the document declares no `name`")
    return name


def _plain(text: str) -> dict[str, Any]:
    data, reasons = _read(text)
    if not isinstance(data, dict):
        raise ValueError("; ".join(reasons) or "the stored document is not a mapping")
    return data


def _edited(text: str, doc: dict[str, Any], *, key: str) -> str:
    """``text`` rewritten to ``doc``: its other root keys edited in place, then the items of list ``key``."""
    held = _plain(text)
    if {k: v for k, v in held.items() if k != key} != {k: v for k, v in doc.items() if k != key}:
        text = edit_text(text, lambda current: {k: current.get(k, v) if k == key else v for k, v in doc.items()})
    return edit_list_items(text, key=key, item_key="concept", desired=doc[key])


def _set(doc: dict[str, Any], key: str, value: Any) -> None:
    if value is UNSET:
        return
    if value is None:
        doc.pop(key, None)
    else:
        doc[key] = value


def _upsert(items: list[Any], new: list[dict[str, Any]], *, key: str, remove: list[str]) -> list[Any]:
    replaced = {str(n.get(key)): n for n in new}
    out = [replaced.pop(str(i.get(key)), i) for i in items if not (isinstance(i, dict) and i.get(key) in remove)]
    return out + list(replaced.values())


def _missing(kind: str, names: list[str], held: list[Any], *, key: str) -> None:
    known = {i.get(key) for i in held if isinstance(i, dict)}
    unknown = [n for n in names if n not in known]
    if unknown:
        raise ValueError(f"No {kind} named {', '.join(map(repr, unknown))}")


def _relationship_edits(doc: dict[str, Any], *, relationships: list[dict[str, Any]], remove: list[str]) -> None:
    components = {c.get("concept"): c for c in doc.get("ontology") or [] if isinstance(c, dict)}
    for qualified in remove:
        concept, _, name = qualified.partition(".")
        component = components.get(concept)
        if component is None or not name:
            raise ValueError(f"No relationship {qualified!r}; name it as <Concept>.<relationship>")
        _missing("relationship", [qualified], [{"q": f"{concept}.{r.get('name')}"} for r in
                                               component.get("relationships") or []], key="q")
        component["relationships"] = _upsert(component["relationships"], [], key="name", remove=[name])
        if not component["relationships"]:
            del component["relationships"]
    for edit in relationships:
        component = components.get(edit.get("concept"))
        if component is None:
            raise ValueError(f"No concept {edit.get('concept')!r} to add a relationship to")
        component["relationships"] = _upsert(component.get("relationships") or [], [edit["relationship"]],
                                             key="name", remove=[])


def edited_ontology(text: str, *, description: Any = UNSET, ai_context: Any = UNSET, prefixes: Any = UNSET,
                    requires: Any = UNSET, concepts: list[dict[str, Any]] | None = None,
                    remove_concepts: list[str] | None = None, relationships: list[dict[str, Any]] | None = None,
                    remove_relationships: list[str] | None = None) -> str:
    """``text`` with the document fields set (``None`` removes one), whole concepts upserted or removed by name and
    relationships ``{concept, relationship}`` upserted or removed by ``Concept.name``; the rest keeps its text."""

    doc = _plain(text)
    for key, value in (("description", description), ("ai_context", ai_context), ("prefixes", prefixes),
                       ("requires", requires)):
        _set(doc, key, value)
    _missing("concept", remove_concepts or [], doc.get("ontology") or [], key="concept")
    doc["ontology"] = _upsert(doc.get("ontology") or [], concepts or [], key="concept", remove=remove_concepts or [])
    _relationship_edits(doc, relationships=relationships or [], remove=remove_relationships or [])
    return _edited(text, doc, key="ontology")


def edited_mapping(text: str, *, description: Any = UNSET, ontology_ref: Any = UNSET, semantic_model_ref: Any = UNSET,
                   concept_mappings: list[dict[str, Any]] | None = None,
                   remove_concept_mappings: list[str] | None = None) -> str:
    """``text`` with its description and references set, and concept mappings upserted or removed by concept."""

    doc = _plain(text)
    for key, value in (("description", description), ("ontology_ref", ontology_ref),
                       ("semantic_model_ref", semantic_model_ref)):
        _set(doc, key, value)
    _missing("concept mapping", remove_concept_mappings or [], doc.get("concept_mappings") or [], key="concept")
    doc["concept_mappings"] = _upsert(doc.get("concept_mappings") or [], concept_mappings or [], key="concept",
                                      remove=remove_concept_mappings or [])
    return _edited(text, doc, key="concept_mappings")
