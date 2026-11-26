"""Ontology concepts in search: the text a dataset's or field's concepts add, and ``concept:`` entity references."""

from __future__ import annotations

from collections.abc import Iterable

from slayer.core.models import SlayerDataset
from slayer.ontology.ontology import Ontology
from slayer.ontology.use import Annotations, ConceptRef, FieldAnnotation, annotate
from slayer.storage.base import StorageBackend

CONCEPT_PREFIX = "concept:"


async def concept_annotations(storage: StorageBackend, data_source: str) -> Annotations | None:
    """The concept annotations ``storage``'s caller sees in ``data_source``; ``None`` without mappings."""
    if not await storage.list_ontology_mappings(data_source):
        return None
    return annotate(await storage.ontology_use(data_source))


def _descriptions(ontology: Ontology | None, concepts: Iterable[str]) -> list[str]:
    if ontology is None:
        return []
    seen = list(dict.fromkeys(c for concept in concepts for c in ontology.ancestors(concept)))
    return [f"{c}: {ontology.descriptions[c]}" for c in seen if c in ontology.descriptions]


def _field_lines(annotation: FieldAnnotation, ontology: Ontology | None) -> list[str]:
    where = f" of {annotation.entity}" if annotation.entity and annotation.role != "value" else ""
    via = f" in {annotation.relationship}" if annotation.relationship else ""
    lines = [f"Concept: {annotation.concept} ({annotation.role}{where}{via}; ontology {annotation.ontology}, "
             f"mapping {annotation.mapping})", f"Concept chain: {' → '.join(annotation.chain)}"]
    concepts = [annotation.concept, *([annotation.entity] if annotation.entity else [])]
    lines += [f"Concept description: {d}" for d in _descriptions(ontology, concepts)]
    if annotation.verbalization:
        lines.append(f"Verbalization: {annotation.verbalization}")
    if annotation.domains:
        lines.append(f"Domain: {'; '.join(annotation.domains)}")
    return lines


def field_concept_lines(concepts: Annotations | None, *, dataset: SlayerDataset, field: str) -> list[str]:
    """The search text a field's concept annotations add."""
    if concepts is None:
        return []
    return [line for a in concepts.of(dataset.name).of_field(field)
            for line in _field_lines(a, concepts.ontologies.get(a.ontology))]


def _refs_line(label: str, refs: list[ConceptRef]) -> list[str]:
    return [f"{label}: " + ", ".join(f"{r.concept} (ontology {r.ontology}, mapping {r.mapping})" for r in refs)] \
        if refs else []


def dataset_concept_lines(concepts: Annotations | None, *, dataset: SlayerDataset) -> list[str]:
    """The search text a dataset's own and also-populated concepts add."""
    if concepts is None:
        return []
    held = concepts.of(dataset.name)
    lines = [*_refs_line("Concepts", held.own), *_refs_line("Also populates concepts", held.also)]
    for ref in [*held.own, *held.also]:
        lines += [f"Concept description: {d}" for d in _descriptions(concepts.ontologies.get(ref.ontology),
                                                                    [ref.concept])]
    for lineup in held.lineups:
        verbalization = f": {lineup.annotation.verbalization}" if lineup.annotation.verbalization else ""
        lines.append(f"Relationship concept: {lineup.annotation.relationship}{verbalization}")
    return list(dict.fromkeys(lines))


def _matches(ontology: Ontology | None, concept: str | None, *, wanted: str) -> bool:
    return ontology is not None and concept is not None and ontology.subtype_of(concept, wanted)


def concept_matches(concepts: Annotations, *, reference: str, datasets: list[SlayerDataset]) -> list[str]:
    """The canonical ids of the visible ``datasets`` and their fields annotated with the concept ``reference``
    (``Concept`` or ``Ontology.Concept``) names, or a subtype of it, each once."""
    def hit(ontology_name: str, concept: str | None) -> bool:
        for name, wanted in candidates:
            if name in (None, ontology_name) and _matches(concepts.ontologies.get(ontology_name), concept,
                                                         wanted=wanted):
                return True
        return False

    candidates: list[tuple[str | None, str]] = [(None, reference)]
    if "." in reference:
        prefix, rest = reference.split(".", 1)
        candidates = [(prefix, rest)] if prefix in concepts.ontologies else candidates
    out: list[str] = []
    for dataset in (d for d in datasets if not d.hidden):
        held = concepts.of(dataset.name)
        qualifier = f"{dataset.data_source}.{dataset.name}"
        if any(hit(r.ontology, r.concept) for r in [*held.own, *held.also]):
            out.append(qualifier)
        for field in (f for f in dataset.fields if not f.hidden):
            if any(hit(a.ontology, a.concept) or hit(a.ontology, a.entity) for a in held.of_field(field.name)):
                out.append(f"{qualifier}.{field.name}")
    return list(dict.fromkeys(out))
