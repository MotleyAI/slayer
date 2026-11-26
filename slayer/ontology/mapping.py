"""Mapping documents resolved against an ontology and one semantic model's datasets."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from slayer.core.models import SlayerDataset
from slayer.core.syntax import (
    BoolOp, Cast, Cmp, DottedRef, Literal, MappingExpression, Ref, ScalarCall, TemporalLiteral, UnaryOp, Arith,
    parse_mapping_expression, walk_parsed,
)
from slayer.ontology.names import identifier_key
from slayer.ontology.ontology import BUILT_INS, Ontology, Relationship

_NUMERIC = frozenset({"Integer", "Decimal", "Float"})
_FITS = {"Boolean": frozenset({"Boolean"}), "String": frozenset({"String"}), "Date": frozenset({"Date"}),
         "DateTime": frozenset({"DateTime", "DateTimeTz", "Date"}), **dict.fromkeys(_NUMERIC, _NUMERIC)}
_BOOLEAN_CALLS = frozenset({"like", "ilike", "not_like", "not_ilike", "regexp_like", "starts_with", "ends_with",
                            "contains", "isnan", "is_nan"})
_STRING_CALLS = frozenset({"concat", "lower", "upper", "trim", "ltrim", "rtrim", "substr", "substring", "replace"})
_CAST_TYPES = {"INT": "Integer", "INTEGER": "Integer", "BIGINT": "Integer", "SMALLINT": "Integer", "DOUBLE": "Float",
               "FLOAT": "Float", "REAL": "Float", "DECIMAL": "Decimal", "NUMERIC": "Decimal", "VARCHAR": "String",
               "TEXT": "String", "STRING": "String", "CHAR": "String", "BOOLEAN": "Boolean", "BOOL": "Boolean",
               "DATE": "Date", "TIMESTAMP": "DateTime"}


class MappingProblem(BaseModel):
    """One reason a mapping does not resolve, at ``path``; ``entry`` is its ``concept_mappings`` index."""

    message: str
    path: str = ""
    entry: int | None = None

    def __str__(self) -> str:
        return f"{self.path}: {self.message}" if self.path else self.message


class ModelFields(BaseModel):
    """The datasets a mapping reads, by Ossie comparison key: each its declared name and its fields' datatypes."""

    datasets: dict[str, tuple[str, dict[str, tuple[str, str | None]]]] = {}

    @classmethod
    def of(cls, datasets: Iterable[SlayerDataset]) -> ModelFields:
        out = cls()
        for dataset in datasets:
            name, fields = out.datasets.get(identifier_key(dataset.name), (dataset.name, {}))
            for field in dataset.fields:
                datatype = field.datatype.value if field.datatype is not None else None
                fields.setdefault(identifier_key(field.name), (field.name, None if datatype == "Opaque" else datatype))
            out.datasets[identifier_key(dataset.name)] = (name, fields)
        return out


class MappingResolution(BaseModel):
    """The problems of a mapping, and the paths of its expressions whose type is unknown."""

    problems: list[MappingProblem] = []
    untyped: list[str] = []

    def note(self, name: str) -> str | None:
        if not self.untyped:
            return None
        return (f"Mapping {name!r}: the types of {len(self.untyped)} expression(s) are unknown, so they are not checked "
                f"against their concepts: {'; '.join(self.untyped)}")


def _items(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _references(parsed: MappingExpression) -> list[Ref | DottedRef]:
    nodes = [*walk_parsed(parsed.value), *walk_parsed(parsed.predicate)]
    return [n for n in nodes if isinstance(n, (Ref, DottedRef))]


def datasets_read(expression: Any) -> set[str] | None:
    """The comparison keys of the datasets ``expression`` reads; ``None`` when it does not read."""
    try:
        parsed = parse_mapping_expression(str(expression))
    except ValueError:
        return None
    return {identifier_key(r.parts[0], quoted=r.quoted_at(0)) for r in _references(parsed) if isinstance(r, DottedRef)}


def entry_reads(entry: Any) -> set[str] | None:
    """The datasets every expression of a ``concept_mappings`` entry reads, at any depth; ``None`` when one does not
    read."""
    out: set[str] = set()
    pending = [entry]
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            if "expression" in node:
                read = datasets_read(node["expression"])
                if read is None:
                    return None
                out |= read
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)
    return out


def _constant_type(node: Literal | TemporalLiteral | Cast) -> str | None:
    if isinstance(node, TemporalLiteral):
        return "Date" if node.kind == "date" else "DateTime"
    if isinstance(node, Cast):
        return _CAST_TYPES.get(node.type_text.split("(")[0].strip().upper())
    if node.value is None:
        return None
    if isinstance(node.value, bool):
        return "Boolean"
    return "String" if isinstance(node.value, str) else "Decimal"


class _Resolver:
    def __init__(self, *, ontology: Ontology | None, model: ModelFields | None) -> None:
        self.ontology = ontology
        self.model = model
        self.out = MappingResolution()
        self.entry: int | None = None

    def problem(self, path: str, message: str) -> None:
        self.out.problems.append(MappingProblem(path=path, message=message, entry=self.entry))

    # ---- structure ------------------------------------------------------

    def declared(self, concept: Any, path: str) -> str | None:
        if not isinstance(concept, str) or self.ontology is None:
            return None
        if not self.ontology.declares(concept):
            self.problem(path, f"concept '{concept}' is not declared in the ontology")
            return None
        return concept

    def resolve(self, concept: str | None, name: Any, path: str) -> Relationship | None:
        if concept is None or self.ontology is None or not isinstance(name, str):
            return None
        written = name
        if "." in name:
            prefix, name = name.split(".", 1)
            if not self.ontology.declares(prefix):
                self.problem(path, f"relationship '{written}' names concept '{prefix}', which is not declared in the "
                                   "ontology")
                return None
            if prefix not in self.ontology.ancestors(concept):
                self.problem(path, f"relationship '{written}' names concept '{prefix}' but this mapping maps "
                                   f"'{concept}'")
                return None
            concept = prefix
        found = self.ontology.find(concept, name)
        if found is None:
            self.problem(path, f"relationship '{written}' is not declared on concept '{concept}' or a concept it "
                               "extends")
        return found

    def concept_mapping(self, index: int, node: Any) -> None:
        if not isinstance(node, dict):
            return
        self.entry = index
        path = f"concept_mappings -> {index}"
        concept = self.declared(node.get("concept"), path)
        for j, mapping in enumerate(_items(node.get("object_mappings"))):
            self.object_mapping(mapping, expected=concept, path=f"{path} -> object_mappings -> {j}")
        for j, link in enumerate(_items(node.get("link_mappings"))):
            self.link(link, concept=concept, path=f"{path} -> link_mappings -> {j}", level=1)

    def object_mapping(self, node: Any, *, expected: str | None, path: str) -> None:
        if not isinstance(node, dict):
            return
        concept = expected
        if "concept" in node:
            concept = self.declared(node["concept"], path)
            if concept is not None and expected is not None and self.ontology is not None \
                    and not self.ontology.subtype_of(concept, expected):
                self.problem(path, f"the object mapping maps '{concept}', which is neither '{expected}' nor a subtype "
                                   "of it")
        if "expression" in node:
            if concept is not None and self.ontology is not None and self.ontology.is_entity(concept) \
                    and self.ontology.simple_identifier(concept) is None:
                self.problem(path, f"'{concept}' has no simple identifier, so an expression cannot map it; map it by "
                                   "referent_mappings")
            self.expression(node["expression"], path=path, concept=concept)
        if "referent_mappings" in node:
            self.referents(node["referent_mappings"], concept=concept, path=path)

    def referents(self, items: Any, *, concept: str | None, path: str) -> None:
        named: list[Relationship] = []
        for k, referent in enumerate(_items(items)):
            if not isinstance(referent, dict):
                continue
            here = f"{path} -> referent_mappings -> {k}"
            relationship = self.resolve(concept, referent.get("relationship"), here)
            target = relationship.last_role if relationship is not None else None
            if relationship is not None:
                named.append(relationship)
                if referent.get("referent_mappings") and target is None:
                    self.problem(here, f"relationship '{referent.get('relationship')}' has no role for its "
                                       "referent_mappings to resolve on")
            if "expression" in referent:
                self.expression(referent["expression"], path=here, concept=target)
            if "referent_mappings" in referent:
                self.referents(referent["referent_mappings"], concept=target, path=here)
        self.coverage(named, concept=concept, path=path)

    def coverage(self, named: list[Relationship], *, concept: str | None, path: str) -> None:
        if concept is None or self.ontology is None or not self.ontology.is_entity(concept):
            return
        identifying = [r for n in self.ontology.identify_by(concept) if (r := self.ontology.find(concept, n))]
        labels = [r.label for r in named]
        missing = [r.name for r in identifying if r.label not in labels]
        repeated = list(dict.fromkeys(r.name for r in identifying if labels.count(r.label) > 1))
        if missing:
            self.problem(path, f"'{concept}' is identified by {', '.join(repr(r.name) for r in identifying)}; the "
                               f"referent_mappings do not name {', '.join(map(repr, missing))}")
        if repeated:
            self.problem(path, f"the referent_mappings name the identifying relationship(s) "
                               f"{', '.join(map(repr, repeated))} of '{concept}' more than once")

    def link(self, node: Any, *, concept: str | None, path: str, level: int) -> None:
        if not isinstance(node, dict):
            return
        relationship = self.resolve(concept, node.get("relationship"), path)
        if relationship is not None and relationship.arity != level:
            self.problem(path, f"relationship '{node.get('relationship')}' has arity {relationship.arity} but is named "
                               f"at level {level}")
        expected = concept
        if level != 1:
            expected = relationship.last_role if relationship is not None else None
        if "object_mapping" in node:
            self.object_mapping(node["object_mapping"], expected=expected, path=f"{path} -> object_mapping")
        for c, child in enumerate(_items(node.get("children"))):
            self.link(child, concept=concept, path=f"{path} -> children -> {c}", level=level + 1)

    # ---- expressions ----------------------------------------------------

    def expression(self, text: Any, *, path: str, concept: str | None) -> None:
        if self.model is None or not isinstance(text, str):
            return
        try:
            parsed = parse_mapping_expression(text)
        except ValueError as exc:
            self.problem(path, f"expression {text!r} does not read: {str(exc).splitlines()[0]}")
            return
        known = True
        for reference in _references(parsed):
            known = self.field(reference, text=text, path=path) and known
        if not known:
            return
        if parsed.predicate is not None and not self.boolean(parsed.predicate):
            self.problem(path, f"the predicate of expression {text!r} is not boolean")
            return
        self.typed(parsed, text=text, path=path, concept=concept)

    def field(self, reference: Ref | DottedRef, *, text: str, path: str) -> bool:
        assert self.model is not None
        if isinstance(reference, Ref) or len(reference.parts) != 2:
            written = reference.name if isinstance(reference, Ref) else ".".join(reference.parts)
            self.problem(path, f"expression {text!r} names '{written}', which is not <dataset>.<field>")
            return False
        dataset = self.model.datasets.get(identifier_key(reference.parts[0], quoted=reference.quoted_at(0)))
        if dataset is None:
            self.problem(path, f"expression names '{'.'.join(reference.parts)}' but the semantic model declares no "
                               f"dataset '{reference.parts[0]}'")
            return False
        if identifier_key(reference.parts[1], quoted=reference.quoted_at(1)) not in dataset[1]:
            self.problem(path, f"expression names '{'.'.join(reference.parts)}' but dataset '{dataset[0]}' declares no "
                               f"field '{reference.parts[1]}'")
            return False
        return True

    def type_of(self, node: Any) -> str | None:
        if isinstance(node, DottedRef):
            return self.field_type(node)
        if isinstance(node, Arith):
            return "Decimal" if {self.type_of(node.left), self.type_of(node.right)} <= _NUMERIC else None
        if isinstance(node, UnaryOp) and node.op != "not":
            return self.type_of(node.operand)
        if isinstance(node, (Literal, TemporalLiteral, Cast)):
            return _constant_type(node)
        if isinstance(node, ScalarCall) and node.name in _STRING_CALLS:
            return "String"
        return "Boolean" if self.boolean(node) else None

    def field_type(self, node: DottedRef) -> str | None:
        dataset = self.model.datasets.get(identifier_key(node.parts[0], quoted=node.quoted_at(0))) if self.model else None
        field = dataset[1].get(identifier_key(node.parts[1], quoted=node.quoted_at(1))) if dataset else None
        return field[1] if field else None

    def boolean(self, node: Any) -> bool:
        if isinstance(node, (Cmp, BoolOp)) or (isinstance(node, UnaryOp) and node.op == "not"):
            return True
        if isinstance(node, ScalarCall) and node.name in _BOOLEAN_CALLS:
            return True
        return (isinstance(node, Literal) and isinstance(node.value, bool)) or (
            isinstance(node, DottedRef) and self.type_of(node) == "Boolean")

    def expected_built_in(self, concept: str | None) -> str | None:
        if concept is None or self.ontology is None:
            return None
        if self.ontology.is_entity(concept):
            identifier = self.ontology.simple_identifier(concept)
            return self.ontology.built_in(identifier.last_role) if identifier and identifier.last_role else None
        return self.ontology.built_in(concept)

    def typed(self, parsed: MappingExpression, *, text: str, path: str, concept: str | None) -> None:
        built_in = self.expected_built_in(concept)
        if built_in is None or built_in == "Any" or built_in not in BUILT_INS:
            return
        datatype = self.type_of(parsed.value)
        if datatype is None:
            self.out.untyped.append(path)
        elif datatype not in _FITS.get(built_in, frozenset()):
            self.problem(path, f"expression {text!r} has type {datatype}, which does not fit concept '{concept}' "
                               f"({built_in})")


def resolve_mapping(data: dict[str, Any], *, ontology: Ontology | None, model: ModelFields | None) -> MappingResolution:
    """Check a schema-valid mapping's structure against ``ontology`` and its expressions against ``model``; either is
    ``None`` when its reference did not resolve, which skips that half."""
    resolver = _Resolver(ontology=ontology, model=model)
    for index, node in enumerate(_items(data.get("concept_mappings"))):
        resolver.concept_mapping(index, node)
    return resolver.out
