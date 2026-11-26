"""Stored mappings in use: the concepts of datasets, fields and relationships, and the checks of a semantic model and
its data against the ontology (report-only: nothing here changes a model)."""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from slayer.core.models import DatasetRelationship, SlayerDataset, is_key_set_unique
from slayer.core.syntax import (
    BoolOp, Cmp, DottedRef, Literal as LiteralNode, Ref, TupleLit, UnaryOp, mapped_value, parse_mapping_expression,
    read_requires, requires_violation, walk_parsed,
)
from slayer.ontology.findings import OntologyFinding
from slayer.ontology.names import identifier_key
from slayer.ontology.ontology import Ontology, Relationship
from slayer.ontology.scope import MappingScope

OBJECTS_STAGE = "ontology_check_objects"
LINK_STAGE = "ontology_check_link_"
SAMPLE_SIZE = 5
_TO_ONE = ("ManyToOne", "OneToOne")
_PLACEHOLDER = re.compile(r"\{([^{}:]*)(?::([^{}]*))?\}")
_HYPHEN_BOUND = re.compile(r"(\w)- ")


def _items(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dicts(value: Any) -> Iterator[dict[str, Any]]:
    return (v for v in _items(value) if isinstance(v, dict))


def _same(a: str, b: str) -> bool:
    return identifier_key(a) == identifier_key(b)


def _same_names(a: Iterable[str], b: Iterable[str]) -> bool:
    return [identifier_key(n) for n in a] == [identifier_key(n) for n in b]


class MappingDocument(BaseModel):
    """A stored mapping that resolves, as data."""

    name: str
    data: dict[str, Any]


class UnresolvedMapping(BaseModel):
    """A stored mapping that does not resolve, with its problems."""

    name: str
    ontology: str
    problems: list[str]


class StoredUse(MappingScope):
    """One datasource's mappings and what they read, as a caller sees them."""

    mappings: list[MappingDocument] = []
    unresolved: list[UnresolvedMapping] = []


# --------------------------------------------------------------------------- #
# Walking a mapping
# --------------------------------------------------------------------------- #
class Leaf(BaseModel):
    """One mapped expression: the concept expected there, the identifying relationship it fills (``via``) and the
    ``(dataset, field)`` pairs it reads, by declared name."""

    expression: str
    concept: str
    via: str | None = None
    reads: list[tuple[str, str]] = []
    plain: bool = False

    @property
    def datasets(self) -> set[str]:
        return {d for d, _ in self.reads}


class ObjectMap(BaseModel):
    """An object mapping: a ``concept`` and its expressions, identifier leaves in ``identify_by`` order."""

    concept: str
    entity: bool
    leaves: list[Leaf]

    @property
    def datasets(self) -> set[str]:
        return {d for leaf in self.leaves for d in leaf.datasets}

    @property
    def expressions(self) -> tuple[str, ...]:
        return tuple(leaf.expression for leaf in self.leaves)

    def plain_fields(self) -> tuple[str, list[str]] | None:
        """``(dataset, fields)`` when every leaf is a plain field of one dataset."""
        if not self.leaves or not all(leaf.plain for leaf in self.leaves) or len(self.datasets) != 1:
            return None
        return next(iter(self.datasets)), [leaf.reads[0][1] for leaf in self.leaves]


class Link(BaseModel):
    """A link mapping: ``subject`` takes part in ``relationship`` with ``roles``, one per further role mapped."""

    subject: ObjectMap
    relationship: Relationship
    roles: list[ObjectMap] = []


class Occurrence(BaseModel):
    """Where an object mapping occurs: an ``object`` or link ``subject`` of its concept mapping, a link ``role``, or a
    nested identifying ``referent``."""

    kind: Literal["object", "subject", "role", "referent"]
    object: ObjectMap
    relationship: Relationship | None = None
    link: Link | None = None


class Edge(BaseModel):
    """A mapped binary link from one entity object mapping to another, identifying referents included."""

    source: ObjectMap
    relationship: Relationship
    target: ObjectMap


class _Walker:
    def __init__(self, *, ontology: Ontology, datasets: list[SlayerDataset]) -> None:
        self.ontology = ontology
        self.fields: dict[str, tuple[str, dict[str, str]]] = {}
        for dataset in datasets:
            _, fields = self.fields.setdefault(identifier_key(dataset.name), (dataset.name, {}))
            for field in dataset.fields:
                fields.setdefault(identifier_key(field.name), field.name)
        self.occurrences: list[Occurrence] = []
        self.links: list[Link] = []
        self.edges: list[Edge] = []

    def leaf(self, text: Any, *, concept: str, via: str | None) -> Leaf:
        try:
            parsed = parse_mapping_expression(str(text))
        except ValueError:
            return Leaf(expression=str(text), concept=concept, via=via)
        reads: list[tuple[str, str]] = []
        for node in [*walk_parsed(parsed.value), *walk_parsed(parsed.predicate)]:
            if not isinstance(node, DottedRef) or len(node.parts) != 2:
                continue
            dataset = self.fields.get(identifier_key(node.parts[0], quoted=node.quoted_at(0)))
            field = dataset[1].get(identifier_key(node.parts[1], quoted=node.quoted_at(1))) if dataset else None
            if dataset and field and (dataset[0], field) not in reads:
                reads.append((dataset[0], field))
        plain = isinstance(parsed.value, DottedRef) and parsed.predicate is None and len(reads) == 1
        return Leaf(expression=str(text), concept=concept, via=via, reads=reads, plain=plain)

    def object(self, node: Any, *, expected: str | None) -> ObjectMap | None:
        if not isinstance(node, dict):
            return None
        concept = node["concept"] if isinstance(node.get("concept"), str) else expected
        if concept is None:
            return None
        entity = self.ontology.is_entity(concept)
        if "expression" in node:
            identifier = self.ontology.simple_identifier(concept) if entity else None
            leaf_concept = identifier.last_role if identifier and identifier.last_role else concept
            leaf = self.leaf(node["expression"], concept=leaf_concept, via=identifier.label if identifier else None)
            return ObjectMap(concept=concept, entity=entity, leaves=[leaf])
        leaves, nested = self.referents(node.get("referent_mappings"), concept=concept)
        out = ObjectMap(concept=concept, entity=entity, leaves=leaves)
        self.edges += [Edge(source=out, relationship=r, target=n) for r, n in nested]
        return out

    def referents(self, items: Any, *, concept: str) -> tuple[list[Leaf], list[tuple[Relationship, ObjectMap]]]:
        order = [r.label for n in self.ontology.identify_by(concept) if (r := self.ontology.find(concept, n))]
        parts: list[tuple[int, list[Leaf]]] = []
        nested: list[tuple[Relationship, ObjectMap]] = []
        for referent in _dicts(items):
            name = referent.get("relationship")
            relationship = self.ontology.resolve(concept, name) if isinstance(name, str) else None
            if relationship is None or relationship.last_role is None:
                continue
            target = relationship.last_role
            if "expression" in referent:
                leaves = [self.leaf(referent["expression"], concept=target, via=relationship.label)]
            else:
                inner, deeper = self.referents(referent.get("referent_mappings"), concept=target)
                inner_object = ObjectMap(concept=target, entity=True, leaves=inner)
                self.edges += [Edge(source=inner_object, relationship=r, target=n) for r, n in deeper]
                self.occurrences.append(Occurrence(kind="referent", object=inner_object, relationship=relationship))
                nested.append((relationship, inner_object))
                leaves = [leaf.model_copy(update={"via": relationship.label}) for leaf in inner]
            position = order.index(relationship.label) if relationship.label in order else len(order)
            parts.append((position, leaves))
        return [leaf for _, leaves in sorted(parts, key=lambda p: p[0]) for leaf in leaves], nested

    def concept_mapping(self, node: Any) -> None:
        concept = node.get("concept") if isinstance(node, dict) else None
        if not isinstance(concept, str) or not self.ontology.declares(concept):
            return
        for raw in _items(node.get("object_mappings")):
            if (found := self.object(raw, expected=concept)) is not None:
                self.occurrences.append(Occurrence(kind="object", object=found))
        for raw in _dicts(node.get("link_mappings")):
            self.link(raw, concept=concept)

    def link(self, node: dict[str, Any], *, concept: str) -> None:
        subject = self.object(node.get("object_mapping"), expected=concept)
        if subject is None:
            return
        name = node.get("relationship")
        relationship = self.ontology.resolve(concept, name) if isinstance(name, str) else None
        unary = relationship if relationship is not None and relationship.arity == 1 else None
        if unary is not None:
            self.links.append(Link(subject=subject, relationship=unary))
        self.occurrences.append(Occurrence(kind="subject", object=subject, relationship=unary))
        for child in _dicts(node.get("children")):
            self.child(child, subject=subject, concept=concept, roles=[])

    def child(self, node: dict[str, Any], *, subject: ObjectMap, concept: str, roles: list[ObjectMap]) -> None:
        level = len(roles) + 2
        name = node.get("relationship")
        relationship = self.ontology.resolve(concept, name) if isinstance(name, str) else None
        expected = relationship.roles[level - 2] if relationship is not None and len(relationship.roles) >= level - 1 \
            else None
        found = self.object(node.get("object_mapping"), expected=expected)
        chain = [*roles, found] if found is not None else roles
        if found is not None:
            link = None
            if relationship is not None and relationship.arity == level:
                link = Link(subject=subject, relationship=relationship, roles=chain)
                self.links.append(link)
                if level == 2 and found.entity:
                    self.edges.append(Edge(source=subject, relationship=relationship, target=found))
            self.occurrences.append(Occurrence(kind="role", object=found, relationship=relationship, link=link))
        for grandchild in _dicts(node.get("children")):
            self.child(grandchild, subject=subject, concept=concept, roles=chain)


def verbalized(relationship: Relationship, *, subject: ObjectMap, roles: list[ObjectMap]) -> str | None:
    """``relationship``'s first verbalization, each placeholder filled by its concept and mapped expressions."""
    if not relationship.verbalizes:
        return None
    slots: list[tuple[str, str | None, ObjectMap | None]] = [(relationship.concept, None, subject)]
    slots += [(c, n, roles[i] if i < len(roles) else None)
              for i, (c, n) in enumerate(zip(relationship.roles, relationship.role_names))]
    used: set[int] = set()

    def fill(match: re.Match[str]) -> str:
        name, role = match.group(1).strip(), match.group(2)
        index = next((i for i, (c, n, _) in enumerate(slots)
                      if i not in used and (n == role if role else name in (c, n))), None)
        if index is None:
            index = next((i for i in range(len(slots)) if i not in used), None)
        if index is None:
            return match.group(0)
        used.add(index)
        concept, _, mapped = slots[index]
        shown = name if name and name != "_" else concept
        return shown if mapped is None else " ".join([shown, *(f"`{leaf.expression}`" for leaf in mapped.leaves)])

    return _HYPHEN_BOUND.sub(r"\1 ", _PLACEHOLDER.sub(fill, relationship.verbalizes[0]))


# --------------------------------------------------------------------------- #
# Annotations
# --------------------------------------------------------------------------- #
class ConceptRef(BaseModel):
    """A concept of an ontology, as one mapping populates it."""

    ontology: str
    mapping: str
    concept: str


class FieldAnnotation(BaseModel):
    """What one mapped position says about a field it reads."""

    ontology: str
    mapping: str
    concept: str
    chain: list[str]
    role: Literal["value", "identifies", "references"]
    entity: str | None = None
    relationship: str | None = None
    expression: str | None = None
    description: str | None = None
    verbalization: str | None = None
    domains: list[str] = []
    not_checked: list[str] = []


class RelationshipAnnotation(BaseModel):
    """The ontology relationship a dataset relationship lines up with."""

    ontology: str
    mapping: str
    relationship: str
    multiplicity: str | None = None
    verbalization: str | None = None


class LineUp(BaseModel):
    """Fields ``source_fields`` of ``source`` (the role side) pair, in order, with ``target_fields`` of ``target``
    (the referenced entity's own object mapping) for ``annotation``."""

    source: str
    source_fields: list[str]
    target: str
    target_fields: list[str]
    annotation: RelationshipAnnotation

    def orientation(self, *, dataset: str, to: str, from_columns: list[str],
                    to_columns: list[str]) -> Literal["forward", "reverse"] | None:
        """How a relationship of ``dataset`` to ``to`` lines up: from the role side (``forward``), the other way
        (``reverse``), or not at all."""
        if _same(dataset, self.source) and _same(to, self.target) and _same_names(from_columns, self.source_fields) \
                and _same_names(to_columns, self.target_fields):
            return "forward"
        if _same(dataset, self.target) and _same(to, self.source) and _same_names(from_columns, self.target_fields) \
                and _same_names(to_columns, self.source_fields):
            return "reverse"
        return None


class DatasetAnnotations(BaseModel):
    """A dataset's concepts: its own, the others it populates, its fields' annotations by field key and the line-ups
    it takes part in."""

    own: list[ConceptRef] = []
    also: list[ConceptRef] = []
    fields: dict[str, list[FieldAnnotation]] = {}
    lineups: list[LineUp] = []

    def of_field(self, name: str) -> list[FieldAnnotation]:
        return self.fields.get(identifier_key(name), [])

    def of_relationship(self, *, dataset: str, to: str, from_columns: list[str],
                        to_columns: list[str]) -> list[RelationshipAnnotation]:
        return [lu.annotation for lu in self.lineups if lu.orientation(
            dataset=dataset, to=to, from_columns=from_columns, to_columns=to_columns) is not None]

    @property
    def empty(self) -> bool:
        return not (self.own or self.also or self.fields)


class Annotations(BaseModel):
    """The annotations of one datasource's datasets, by dataset key, and the ontologies they name."""

    data_source: str
    datasets: dict[str, DatasetAnnotations] = {}
    ontologies: dict[str, Ontology] = {}

    def of(self, dataset: str) -> DatasetAnnotations:
        return self.datasets.get(identifier_key(dataset), DatasetAnnotations())

    def held(self, dataset: str) -> DatasetAnnotations:
        """``dataset``'s annotations, added when absent."""
        return self.datasets.setdefault(identifier_key(dataset), DatasetAnnotations())


def _domain(tree: Any, *, variable: str) -> tuple[str, Any] | str | None:
    """A plain constraint's bound ``(op, value)``, its enumeration text, or ``None``."""
    if isinstance(tree, Cmp) and tree.op in ("<", "<=", ">", ">=", "!=", "=="):
        flipped = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "!=": "!=", "==": "=="}
        if isinstance(tree.left, Ref) and tree.left.name == variable:
            value, op = _literal(tree.right), tree.op
        elif isinstance(tree.right, Ref) and tree.right.name == variable:
            value, op = _literal(tree.left), flipped[tree.op]
        else:
            return None
        return None if value is None else (op, value)
    values = _enumerated(tree, variable=variable)
    return f"one of {', '.join(str(v) for v in values)}" if values else None


def _literal(node: Any) -> Any:
    if isinstance(node, LiteralNode) and node.value is not None and not isinstance(node.value, bool):
        return node.value
    if isinstance(node, UnaryOp) and node.op == "-" and isinstance(node.operand, LiteralNode) \
            and isinstance(node.operand.value, Decimal):
        return -node.operand.value
    return None


def _enumerated(tree: Any, *, variable: str) -> list[Any] | None:
    if isinstance(tree, Cmp) and isinstance(tree.left, Ref) and tree.left.name == variable:
        if tree.op == "==" and (value := _literal(tree.right)) is not None:
            return [value]
        if tree.op == "in" and isinstance(tree.right, TupleLit):
            return [e.value for e in tree.right.elements]
    if isinstance(tree, BoolOp) and tree.op == "or":
        parts = [_enumerated(o, variable=variable) for o in tree.operands]
        return None if any(p is None for p in parts) else [v for p in parts if p for v in p]
    return None


def _number(value: Any) -> str:
    return f"{value:f}".rstrip("0").rstrip(".") if isinstance(value, Decimal) and "." in f"{value:f}" else str(value)


def domains(ontology: Ontology, concept: str) -> tuple[list[str], list[str]]:
    """``concept``'s value constraints, its supertypes' included: the plain ones as domains, the rest as written."""
    lows: list[tuple[str, Any]] = []
    highs: list[tuple[str, Any]] = []
    shown: list[str] = []
    other: list[str] = []
    for declaring, text in ontology.inherited_requires(concept):
        reading = read_requires(text)
        if reading.kind != "plain" or reading.variable != declaring:
            other.append(text)
            continue
        domain = _domain(reading.tree, variable=declaring)
        if isinstance(domain, tuple) and domain[0] in (">", ">="):
            lows.append(domain)
        elif isinstance(domain, tuple) and domain[0] in ("<", "<="):
            highs.append(domain)
        elif isinstance(domain, tuple):
            shown.append(f"{domain[0]} {_number(domain[1])}")
        else:
            shown.append(domain or text)
    if len(lows) == 1 and len(highs) == 1 and (lows[0][0], highs[0][0]) == (">=", "<="):
        return [f"{_number(lows[0][1])}–{_number(highs[0][1])}", *shown], other
    return [*(f"{op} {_number(v)}" for op, v in [*lows, *highs]), *shown], other


class _Mapped(BaseModel):
    """One resolved mapping walked: what it maps and where."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    ontology: Ontology
    datasets: list[SlayerDataset]
    occurrences: list[Occurrence]
    links: list[Link]
    edges: list[Edge]
    own: dict[str, list[str]]
    also: dict[str, list[str]]
    lineups: list[LineUp]


def _own_concepts(walker: _Walker) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    produced: dict[str, list[str]] = {}
    for occurrence in walker.occurrences:
        found = occurrence.object
        if found.entity and len(found.datasets) == 1:
            concepts = produced.setdefault(next(iter(found.datasets)), [])
            if found.concept not in concepts:
                concepts.append(found.concept)
    reached: dict[str, set[str]] = {}
    for edge in walker.edges:
        if edge.relationship.multiplicity in _TO_ONE and len(edge.source.datasets) == 1 \
                and edge.source.datasets == edge.target.datasets:
            reached.setdefault(next(iter(edge.source.datasets)), set()).add(edge.target.concept)
    own = {d: [c for c in cs if c not in reached.get(d, set())] for d, cs in produced.items()}
    also = {d: [c for c in cs if c in reached.get(d, set())] for d, cs in produced.items()}
    return own, also


def _lineups(walker: _Walker, *, mapping: str) -> list[LineUp]:
    targets = [o.object for o in walker.occurrences if o.kind in ("object", "subject") and o.object.entity]
    out: dict[tuple[Any, ...], LineUp] = {}
    for edge in walker.edges:
        source = edge.target.plain_fields()
        if source is None or edge.relationship.arity != 2:
            continue
        for target in (t for t in targets if walker.ontology.subtype_of(t.concept, edge.target.concept)):
            fields = target.plain_fields()
            if fields is None or len(fields[1]) != len(source[1]):
                continue
            annotation = RelationshipAnnotation(
                ontology=walker.ontology.name, mapping=mapping, relationship=edge.relationship.label,
                multiplicity=edge.relationship.multiplicity,
                verbalization=verbalized(edge.relationship, subject=edge.source, roles=[edge.target]))
            key = (source[0], tuple(source[1]), fields[0], tuple(fields[1]), edge.relationship.label)
            out.setdefault(key, LineUp(source=source[0], source_fields=source[1], target=fields[0],
                                       target_fields=fields[1], annotation=annotation))
    return list(out.values())


def walked(use: StoredUse) -> list[_Mapped]:
    """Every resolved mapping of ``use`` walked against its ontology and semantic model."""
    out: list[_Mapped] = []
    for mapping in use.mappings:
        ontology_data = use.ontologies.get(str((mapping.data.get("ontology_ref") or {}).get("name")))
        datasets = use.model_of(str((mapping.data.get("semantic_model_ref") or {}).get("name")))
        if ontology_data is None or datasets is None:
            continue
        walker = _Walker(ontology=Ontology.of(ontology_data), datasets=datasets)
        for node in _items(mapping.data.get("concept_mappings")):
            walker.concept_mapping(node)
        own, also = _own_concepts(walker)
        out.append(_Mapped(name=mapping.name, ontology=walker.ontology, datasets=datasets,
                           occurrences=walker.occurrences, links=walker.links, edges=walker.edges, own=own,
                           also=also, lineups=_lineups(walker, mapping=mapping.name)))
    return out


def _role(mapped: _Mapped, occurrence: Occurrence, *, dataset: str) -> Literal["value", "identifies", "references"]:
    if not occurrence.object.entity:
        return "value"
    own = occurrence.kind in ("object", "subject") and occurrence.object.concept in mapped.own.get(dataset, [])
    return "identifies" if own else "references"


def _annotation_relationship(occurrence: Occurrence, leaf: Leaf) -> tuple[str | None, str | None]:
    """The relationship label a position takes part in and its filled verbalization."""
    mapped, ontology_relationship = occurrence.object, occurrence.relationship
    if occurrence.kind == "role" and occurrence.link is not None:
        link = occurrence.link
        return link.relationship.label, verbalized(link.relationship, subject=link.subject, roles=link.roles)
    if occurrence.kind == "role":
        return (ontology_relationship.label if ontology_relationship else None), None
    if ontology_relationship is not None:
        return ontology_relationship.label, verbalized(ontology_relationship, subject=mapped, roles=[])
    return leaf.via, None


def _identifying_verbalization(ontology: Ontology, occurrence: Occurrence, leaf: Leaf) -> str | None:
    """The filled verbalization of the identifying relationship an object's ``leaf`` is reached by."""
    if not leaf.via or occurrence.kind not in ("object", "subject"):
        return None
    identifying = ontology.resolve(occurrence.object.concept, leaf.via)
    if identifying is None:
        return None
    value = ObjectMap(concept=leaf.concept, entity=False, leaves=[leaf])
    return verbalized(identifying, subject=occurrence.object, roles=[value])


def _field_annotations(mapped: _Mapped, occurrence: Occurrence) -> Iterator[tuple[str, str, FieldAnnotation]]:
    ontology = mapped.ontology
    entity = occurrence.object.concept if occurrence.object.entity else None
    for leaf in occurrence.object.leaves:
        relationship, verbalization = _annotation_relationship(occurrence, leaf)
        verbalization = verbalization or _identifying_verbalization(ontology, occurrence, leaf)
        shown, other = domains(ontology, leaf.concept) if ontology.is_value(leaf.concept) else ([], [])
        description = ontology.description(leaf.concept) or (ontology.description(entity) if entity else None)
        for dataset, field in leaf.reads:
            yield dataset, field, FieldAnnotation(
                ontology=ontology.name, mapping=mapped.name, concept=leaf.concept,
                chain=ontology.ancestors(leaf.concept), role=_role(mapped, occurrence, dataset=dataset),
                entity=entity, relationship=relationship,
                expression=None if leaf.plain else leaf.expression, description=description,
                verbalization=verbalization, domains=shown, not_checked=other)


def annotate(use: StoredUse) -> Annotations:
    """The concept annotations ``use``'s resolved mappings give its datasets."""
    out = Annotations(data_source=use.data_source)
    for mapped in walked(use):
        out.ontologies.setdefault(mapped.ontology.name, mapped.ontology)
        for dataset, concepts in mapped.own.items():
            out.held(dataset).own += _refs(mapped, concepts)
        for dataset, concepts in mapped.also.items():
            out.held(dataset).also += _refs(mapped, concepts)
        _add_field_annotations(out, mapped)
        for lineup in mapped.lineups:
            for dataset in {identifier_key(lineup.source), identifier_key(lineup.target)}:
                out.held(dataset).lineups.append(lineup)
    return out


def _add_field_annotations(out: Annotations, mapped: _Mapped) -> None:
    for occurrence in (o for o in mapped.occurrences if o.kind != "referent"):
        for dataset, field, annotation in _field_annotations(mapped, occurrence):
            held = out.held(dataset).fields.setdefault(identifier_key(field), [])
            if annotation not in held:
                held.append(annotation)


def _refs(mapped: _Mapped, concepts: list[str]) -> list[ConceptRef]:
    return [ConceptRef(ontology=mapped.ontology.name, mapping=mapped.name, concept=c) for c in concepts]


# --------------------------------------------------------------------------- #
# Model checks
# --------------------------------------------------------------------------- #
def _dataset(mapped: _Mapped, name: str) -> SlayerDataset | None:
    return next((d for d in mapped.datasets if _same(d.name, name)), None)


def _identifier_suggestion(dataset: SlayerDataset, fields: list[str]) -> str:
    primary = [f.name for f in dataset.fields if f.primary_key]
    if not primary:
        return f"declare {', '.join(fields)} the primary key of dataset '{dataset.name}'"
    if len(fields) == 1:
        return f"mark field '{fields[0]}' of dataset '{dataset.name}' unique"
    return (f"a composite unique key ({', '.join(fields)}) beside the primary key ({', '.join(primary)}) cannot be "
            "declared yet")


def _identifier_findings(mapped: _Mapped, *, data_source: str) -> Iterator[OntologyFinding]:
    seen: set[tuple[Any, ...]] = set()
    for occurrence in mapped.occurrences:
        fields = occurrence.object.plain_fields() if occurrence.kind in ("object", "subject") else None
        if fields is None or occurrence.object.concept not in mapped.own.get(fields[0], []):
            continue
        dataset = _dataset(mapped, fields[0])
        key = (occurrence.object.concept, fields[0], tuple(fields[1]))
        if dataset is None or key in seen:
            continue
        seen.add(key)
        keys = [[f.name for f in dataset.fields if f.primary_key], *([f.name] for f in dataset.fields if f.unique),
                *dataset.enforced_keys]
        if is_key_set_unique(key_fields=fields[1], unique_key_sets=keys):
            continue
        yield OntologyFinding(check="identifier", data_source=data_source, mapping=mapped.name,
                              ontology=mapped.ontology.name, concept=occurrence.object.concept,
                              dataset_name=dataset.name, fields=fields[1],
                              suggestion=_identifier_suggestion(dataset, fields[1]))


_EXPECTED = {("ManyToOne", "forward"): ("many_to_one", {"many_to_one", "one_to_one"}),
             ("ManyToOne", "reverse"): ("one_to_many", {"one_to_many", "one_to_one"}),
             ("OneToOne", "forward"): ("one_to_one", {"one_to_one"}),
             ("OneToOne", "reverse"): ("one_to_one", {"one_to_one"})}


def _multiplicity_findings(mapped: _Mapped, *, data_source: str) -> Iterator[OntologyFinding]:
    for dataset in mapped.datasets:
        for relationship in dataset.relationships:
            for lineup in mapped.lineups:
                finding = _multiplicity_finding(mapped, dataset=dataset, relationship=relationship, lineup=lineup,
                                                data_source=data_source)
                if finding is not None:
                    yield finding


def _multiplicity_finding(mapped: _Mapped, *, dataset: SlayerDataset, relationship: DatasetRelationship,
                          lineup: LineUp, data_source: str) -> OntologyFinding | None:
    """How ``relationship``'s cardinality disagrees with the ontology relationship of ``lineup``; ``None`` when it
    agrees or does not line up."""
    from_columns = [a for a, _ in relationship.column_pairs]
    orientation = lineup.orientation(dataset=dataset.name, to=relationship.to, from_columns=from_columns,
                                     to_columns=[b for _, b in relationship.column_pairs])
    expected = _EXPECTED.get((lineup.annotation.multiplicity or "", orientation or ""))
    declared = relationship.cardinality.value if relationship.cardinality is not None else None
    if expected is None or declared in expected[1]:
        return None
    suggestion = f"declare its cardinality {expected[0]}" if declared is None else (
        f"it is declared {declared}, but {lineup.annotation.multiplicity} makes it {expected[0]} from "
        f"'{dataset.name}'; declare it {expected[0]}")
    pairs = ", ".join(f"{a} = {b}" for a, b in relationship.column_pairs)
    shown = f"'{relationship.name}' " if relationship.name else ""
    return OntologyFinding(
        check="multiplicity", data_source=data_source, mapping=mapped.name,
        ontology=mapped.ontology.name, dataset_name=dataset.name, fields=from_columns,
        relationship=lineup.annotation.relationship, multiplicity=lineup.annotation.multiplicity,
        dataset_relationship=f"{shown}({dataset.name} → {relationship.to} on {pairs})",
        suggestion=suggestion)


def model_findings(use: StoredUse) -> list[OntologyFinding]:
    """The identifier and multiplicity mismatches of ``use``'s resolved mappings."""
    return [finding for mapped in walked(use) for finding in (
        *_identifier_findings(mapped, data_source=use.data_source),
        *_multiplicity_findings(mapped, data_source=use.data_source))]


# --------------------------------------------------------------------------- #
# Data checks
# --------------------------------------------------------------------------- #
class CheckPlan(BaseModel):
    """A data check: ``count`` returns the violations as ``n``, ``sample`` up to five as ``s0``, ``s1``, …"""

    finding: OntologyFinding
    count: Any
    sample: Any
    width: int


def link_requirement(ontology: Ontology, concept: str, text: str) -> Relationship | None:
    """The relationship an entity's single-path ``requires`` entry demands a link through; ``None`` otherwise."""
    reading = read_requires(text)
    if reading.kind != "path" or not ontology.is_entity(concept):
        return None
    found = ontology.resolve(concept, ".".join(reading.path))
    return found if found is not None and found.arity <= 2 else None


def _value_requirement(ontology: Ontology, concept: str, text: str) -> bool:
    reading = read_requires(text)
    return ontology.is_value(concept) and reading.kind == "plain" and reading.variable == concept


_NUMERIC = frozenset({"Integer", "Decimal", "Float"})


def _literal_mismatch(ontology: Ontology, concept: str, text: str) -> str | None:
    """Why plain ``text`` compares ``concept`` with a literal its built-in type cannot hold; ``None`` when it fits."""
    built_in = ontology.built_in(concept)
    if built_in in (None, "Any"):
        return None
    fits = {"Boolean": built_in == "Boolean", "number": built_in in _NUMERIC,
            "string": built_in not in _NUMERIC | {"Boolean"}}
    for node in walk_parsed(read_requires(text).tree):
        kind = _literal_kind(node.value) if isinstance(node, LiteralNode) else None
        if kind is not None and not fits[kind]:
            return f"it compares '{concept}' ({built_in}) with a {kind} literal, which SLayer cannot check"
    return None


def _literal_kind(value: Any) -> str | None:
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, Decimal):
        return "number"
    return "string" if isinstance(value, str) else None


def _not_checked(mapped: _Mapped, *, data_source: str, **values: Any) -> OntologyFinding:
    return OntologyFinding(check="not_checked", data_source=data_source, mapping=mapped.name,
                           ontology=mapped.ontology.name, **values)


_OUTSIDE = ("outside the forms SLayer checks (value-type constraints in the plain Layer 1 subset and single-path "
            "required links)")


def _rules_not_checked(mapped: _Mapped, *, data_source: str) -> Iterator[OntologyFinding]:
    ontology = mapped.ontology
    for text in ontology.document_requires:
        yield _not_checked(mapped, data_source=data_source, constraint=text, reason=f"a document-level rule, {_OUTSIDE}")
    for concept in ontology.kinds:
        for text in ontology.requires.get(concept, []):
            if not (_value_requirement(ontology, concept, text) or link_requirement(ontology, concept, text)):
                yield _not_checked(mapped, data_source=data_source, concept=concept, constraint=text,
                                   reason=f"a rule of '{concept}' {_OUTSIDE}")
        for relationship in ontology.relationships.get(concept, {}).values():
            for text in relationship.requires:
                yield _not_checked(mapped, data_source=data_source, concept=concept,
                                   relationship=relationship.label, constraint=text,
                                   reason=f"a rule of relationship '{relationship.label}', {_OUTSIDE}")


def _leaves(mapped: _Mapped) -> list[Leaf]:
    out: dict[tuple[str, str], Leaf] = {}
    for occurrence in mapped.occurrences:
        if occurrence.kind != "referent":
            for leaf in occurrence.object.leaves:
                out.setdefault((leaf.concept, leaf.expression), leaf)
    return list(out.values())


def _value_plans(mapped: _Mapped, *, data_source: str) -> Iterator[CheckPlan | OntologyFinding]:
    ontology = mapped.ontology
    listed: set[str] = set()
    for leaf in _leaves(mapped):
        if len(leaf.datasets) > 1 and leaf.expression not in listed:
            listed.add(leaf.expression)
            yield _not_checked(mapped, data_source=data_source, concept=leaf.concept, position=leaf.expression,
                               reason="the expression reads more than one dataset")
        if len(leaf.datasets) != 1 or not ontology.is_value(leaf.concept):
            continue
        for declaring, text in ontology.inherited_requires(leaf.concept):
            if _value_requirement(ontology, declaring, text):
                yield _value_plan(mapped, leaf=leaf, declaring=declaring, text=text, data_source=data_source)


def _value_plan(mapped: _Mapped, *, leaf: Leaf, declaring: str, text: str,
                data_source: str) -> CheckPlan | OntologyFinding:
    """The data check of value constraint ``text`` (declared on ``declaring``) on single-dataset ``leaf``, or why it
    is not checked."""
    ontology = mapped.ontology
    dataset = next(iter(leaf.datasets))
    finding = OntologyFinding(check="constraint", data_source=data_source, mapping=mapped.name,
                              ontology=ontology.name, concept=leaf.concept, dataset_name=dataset,
                              fields=[f for _, f in leaf.reads], constraint=text, position=leaf.expression)
    mismatch = _literal_mismatch(ontology, declaring, text)
    if mismatch is not None:
        return finding.model_copy(update={"check": "not_checked", "reason": mismatch})
    try:
        filters = requires_violation(text, variable=declaring, expression=leaf.expression)
        value = mapped_value(leaf.expression).value
    except ValueError as exc:
        return finding.model_copy(update={"check": "not_checked", "reason": str(exc)})
    return CheckPlan(
        finding=finding, width=1,
        count={"population": dataset, "filter": filters,
               "metrics": [{"metric": f"count({dataset}.*)", "alias": "n"}]},
        sample={"population": dataset, "filter": filters, "dimensions": [{"field": value, "alias": "s0"}],
                "limit": SAMPLE_SIZE})


def _stage(name: str, mapped_object: ObjectMap, *, extra: list[ObjectMap]) -> dict[str, Any]:
    dataset = next(iter(mapped_object.datasets | {d for o in extra for d in o.datasets}))
    values = [mapped_value(leaf.expression) for leaf in mapped_object.leaves]
    conditions = [c for leaf in [*mapped_object.leaves, *(lf for o in extra for lf in o.leaves)]
                  for c in mapped_value(leaf.expression).conditions()]
    return {"name": name, "population": dataset, "filter": conditions,
            "dimensions": [{"field": v.value, "alias": f"k{i}"} for i, v in enumerate(values)]}


def _link_plan(producer: ObjectMap, linkers: list[Link], finding: OntologyFinding) -> CheckPlan:
    keys = [f"k{i}" for i in range(len(producer.leaves))]
    stages = [_stage(OBJECTS_STAGE, producer, extra=[]),
              *(_stage(f"{LINK_STAGE}{i}", link.subject, extra=link.roles) for i, link in enumerate(linkers))]
    main: dict[str, Any] = {"population": OBJECTS_STAGE, "filter": [f"{LINK_STAGE}{i}.k0 IS NULL"
                                                                     for i in range(len(linkers))]}
    if linkers:
        main["extensions"] = [{"dataset": OBJECTS_STAGE, "relationships": [
            {"name": f"{LINK_STAGE}{i}", "to": f"{LINK_STAGE}{i}", "from_columns": keys, "to_columns": keys}
            for i in range(len(linkers))]}]
    count = [*stages, {**main, "metrics": [{"metric": f"count({OBJECTS_STAGE}.*)", "alias": "n"}]}]
    sample = [*stages, {**main, "dimensions": [{"field": f"{OBJECTS_STAGE}.{k}", "alias": f"s{i}"}
                                               for i, k in enumerate(keys)], "limit": SAMPLE_SIZE}]
    return CheckPlan(finding=finding, count=count, sample=sample, width=len(keys))


def _link_plans(mapped: _Mapped, *, data_source: str) -> Iterator[CheckPlan | OntologyFinding]:
    ontology = mapped.ontology
    for concept in ontology.kinds:
        for text in ontology.requires.get(concept, []):
            relationship = link_requirement(ontology, concept, text)
            if relationship is not None and relationship.name not in ontology.identify_by(concept):
                yield from _requirement_plans(mapped, concept=concept, text=text, relationship=relationship,
                                              data_source=data_source)


def _requirement_plans(mapped: _Mapped, *, concept: str, text: str, relationship: Relationship,
                       data_source: str) -> Iterator[CheckPlan | OntologyFinding]:
    """The data checks of required link ``text`` of ``concept``, one per position producing its objects."""
    ontology = mapped.ontology
    base: dict[str, Any] = {"data_source": data_source, "mapping": mapped.name, "ontology": ontology.name,
                            "concept": concept, "relationship": relationship.label, "constraint": text}
    linkers = [link for link in mapped.links if link.relationship.label == relationship.label
               and ontology.related(link.subject.concept, concept)]
    if any(len(link.subject.datasets | {d for o in link.roles for d in o.datasets}) != 1 for link in linkers):
        yield OntologyFinding(check="not_checked", **base, reason="a link of it reads more than one dataset")
        return
    producers: dict[tuple[str, ...], ObjectMap] = {}
    for found in (o.object for o in mapped.occurrences):
        if found.entity and found.leaves and ontology.subtype_of(found.concept, concept):
            producers.setdefault(found.expressions, found)
    for producer in producers.values():
        yield _producer_plan(producer, linkers=linkers, base=base)


def _producer_plan(producer: ObjectMap, *, linkers: list[Link], base: dict[str, Any]) -> CheckPlan | OntologyFinding:
    position = ", ".join(producer.expressions)
    if len(producer.datasets) != 1:
        return OntologyFinding(check="not_checked", **base, position=position,
                               reason="the position reads more than one dataset")
    finding = OntologyFinding(check="required_link", **base, position=position,
                              dataset_name=next(iter(producer.datasets)),
                              fields=[f for leaf in producer.leaves for _, f in leaf.reads])
    matching = [link for link in linkers if len(link.subject.leaves) == len(producer.leaves)]
    try:
        return _link_plan(producer, matching, finding)
    except ValueError as exc:
        return finding.model_copy(update={"check": "not_checked", "reason": str(exc)})


def ontology_checks(use: StoredUse) -> tuple[list[OntologyFinding], list[CheckPlan]]:
    """``use``'s findings that need no database, and the plans of the data checks."""
    findings: list[OntologyFinding] = [
        OntologyFinding(check="not_checked", data_source=use.data_source, mapping=u.name, ontology=u.ontology,
                        reason="the mapping does not resolve: " + "; ".join(u.problems)) for u in use.unresolved]
    plans: list[CheckPlan] = []
    for mapped in walked(use):
        findings += [*_identifier_findings(mapped, data_source=use.data_source),
                     *_multiplicity_findings(mapped, data_source=use.data_source),
                     *_rules_not_checked(mapped, data_source=use.data_source)]
        for item in [*_value_plans(mapped, data_source=use.data_source),
                     *_link_plans(mapped, data_source=use.data_source)]:
            if isinstance(item, CheckPlan):
                plans.append(item)
            else:
                findings.append(item)
    return findings, plans
