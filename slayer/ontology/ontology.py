"""Ontology documents: the concept graph and every check made when one is saved."""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

from pydantic import BaseModel

from slayer.ontology.names import identifier_key

BUILT_INS = ("Any", "Boolean", "Date", "DateTime", "Decimal", "Float", "Integer", "String")
ENTITY_TYPE = "EntityType"
VALUE_TYPE = "ValueType"

_URI_SCHEMES = frozenset({"http", "https", "urn", "mailto", "tel", "file", "ftp", "data", "did", "doi", "geo", "ldap",
                          "news", "sms", "tag"})
_IRI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


class Relationship(BaseModel):
    """A relationship of ``concept``; ``roles`` are the concepts of its further roles."""

    concept: str
    name: str
    roles: list[str]
    raw: dict[str, Any]

    @property
    def label(self) -> str:
        return f"{self.concept}.{self.name}"

    @property
    def arity(self) -> int:
        return len(self.roles) + 1

    @property
    def last_role(self) -> str | None:
        return self.roles[-1] if self.roles else None

    @property
    def multiplicity(self) -> str | None:
        value = self.raw.get("multiplicity")
        return value if isinstance(value, str) else None

    @property
    def verbalizes(self) -> list[str]:
        return [v for v in _items(self.raw.get("verbalizes")) if isinstance(v, str)]

    @property
    def requires(self) -> list[str]:
        return [r for r in _items(self.raw.get("requires")) if isinstance(r, str)]

    @property
    def role_names(self) -> list[str | None]:
        """The name of each further role, ``None`` where unnamed."""
        return [r.get("name") if isinstance(r.get("name"), str) else None for r in _dicts(self.raw.get("roles"))]


def _items(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dicts(value: Any) -> Iterator[dict[str, Any]]:
    return (v for v in _items(value) if isinstance(v, dict))


def _strings(value: Any) -> list[str]:
    return [v for v in _items(value) if isinstance(v, str)]


class Ontology(BaseModel):
    """The concepts, supertypes and relationships an ontology document declares (first declaration wins)."""

    name: str
    kinds: dict[str, str] = {}
    supertypes: dict[str, list[str]] = {}
    relationships: dict[str, dict[str, Relationship]] = {}
    identifiers: dict[str, list[str]] = {}
    descriptions: dict[str, str] = {}
    requires: dict[str, list[str]] = {}
    document_requires: list[str] = []

    @classmethod
    def of(cls, data: dict[str, Any]) -> Ontology:
        out = cls(name=str(data.get("name")), document_requires=_strings(data.get("requires")))
        for component in _dicts(data.get("ontology")):
            concept = component.get("concept")
            if not isinstance(concept, str):
                continue
            if concept not in out.kinds and isinstance(component.get("description"), str):
                out.descriptions[concept] = component["description"]
            out.requires.setdefault(concept, _strings(component.get("requires")))
            out.kinds.setdefault(concept, str(component.get("type")))
            out.supertypes.setdefault(concept, []).extend(s for s in _items(component.get("extends"))
                                                          if isinstance(s, str))
            out.identifiers.setdefault(concept, [i for i in _items(component.get("identify_by")) if isinstance(i, str)])
            declared = out.relationships.setdefault(concept, {})
            for raw in _dicts(component.get("relationships")):
                name = raw.get("name")
                if isinstance(name, str) and name not in declared:
                    roles = [str(r.get("concept")) for r in _dicts(raw.get("roles"))]
                    declared[name] = Relationship(concept=concept, name=name, roles=roles, raw=raw)
        return out

    def declares(self, concept: str) -> bool:
        return concept in self.kinds or concept in BUILT_INS

    def ancestors(self, concept: str) -> list[str]:
        """``concept`` and every concept it extends, each once."""
        found: list[str] = []
        pending = [concept]
        while pending:
            current = pending.pop(0)
            if current not in found:
                found.append(current)
                pending.extend(self.supertypes.get(current, []))
        return found

    def find(self, concept: str, name: str) -> Relationship | None:
        """Relationship ``name`` declared on ``concept`` or a concept it extends."""
        for current in self.ancestors(concept):
            found = self.relationships.get(current, {}).get(name)
            if found is not None:
                return found
        return None

    def resolve(self, concept: str, name: str) -> Relationship | None:
        """Relationship ``name`` of ``concept``, qualified ``Base.name`` (``Base`` ``concept`` or an ancestor)
        included."""
        if "." in name:
            prefix, name = name.split(".", 1)
            if prefix not in self.ancestors(concept):
                return None
            concept = prefix
        return self.find(concept, name)

    def is_entity(self, concept: str) -> bool:
        return self.kinds.get(concept) == ENTITY_TYPE

    def is_value(self, concept: str) -> bool:
        return concept in BUILT_INS or self.kinds.get(concept) == VALUE_TYPE

    def related(self, concept: str, other: str) -> bool:
        """``concept`` and ``other`` share their objects: one extends the other."""
        return self.subtype_of(concept, other) or self.subtype_of(other, concept)

    def description(self, concept: str) -> str | None:
        """The description of ``concept``, else of its nearest supertype declaring one."""
        return next((self.descriptions[c] for c in self.ancestors(concept) if c in self.descriptions), None)

    def inherited_requires(self, concept: str) -> list[tuple[str, str]]:
        """Every ``requires`` of ``concept`` and the concepts it extends, as ``(declaring concept, text)``."""
        return [(c, r) for c in self.ancestors(concept) for r in self.requires.get(c, [])]

    def subtype_of(self, concept: str, other: str) -> bool:
        return other in self.ancestors(concept)

    def built_in(self, concept: str) -> str | None:
        """The built-in a value type reaches through ``extends``."""
        return next((c for c in self.ancestors(concept) if c in BUILT_INS), None)

    def identify_by(self, concept: str) -> list[str]:
        """The identifying relationship names of ``concept``, inherited from the nearest supertype declaring them."""
        return next((self.identifiers[c] for c in self.ancestors(concept) if self.identifiers.get(c)), [])

    def simple_identifier(self, concept: str) -> Relationship | None:
        """The one binary identifying relationship whose last role reaches a value type."""
        names = self.identify_by(concept)
        found = self.find(concept, names[0]) if len(names) == 1 else None
        if found is None or found.arity != 2 or found.last_role is None:
            return None
        return found if self.built_in(found.last_role) is not None else None


def _absolute_iri(value: Any) -> bool:
    return isinstance(value, str) and _IRI_SCHEME.match(value) is not None and not any(c.isspace() for c in value)


def undeclared_prefix(iri: Any, prefixes: dict[str, Any]) -> str | None:
    """The prefix of a QName ``iri`` that ``prefixes`` does not declare (Ossie's reading of QName vs full IRI)."""
    if not isinstance(iri, str) or ":" not in iri:
        return None
    prefix, local = iri.split(":", 1)
    if local.startswith("//") or ":" in local or prefix.lower() in _URI_SCHEMES:
        return None
    return None if prefix in prefixes else prefix


def _duplicates(names: list[str]) -> list[str]:
    seen: dict[str, str] = {}
    out: list[str] = []
    for name in names:
        key = identifier_key(name)
        if key in seen and name not in out:
            out.append(name)
        seen.setdefault(key, name)
    return out


def _cycles(supertypes: dict[str, list[str]]) -> list[list[str]]:
    """Each cycle of the ``extends`` graph once, as a path."""
    cycles: list[list[str]] = []
    state: dict[str, str] = {}

    def visit(concept: str, path: list[str]) -> None:
        state[concept] = "active"
        for supertype in supertypes.get(concept, []):
            if state.get(supertype) == "active":
                cycles.append([*path[path.index(supertype):], supertype])
            elif supertype not in state:
                visit(supertype, [*path, supertype])
        state[concept] = "done"

    for concept in supertypes:
        if concept not in state:
            visit(concept, [concept])
    return cycles


def _role_problems(component: dict[str, Any], relationship: dict[str, Any], *, declared: set[str]) -> list[str]:
    concept = str(component.get("concept"))
    label = f"{concept}.{relationship.get('name')}"
    out: list[str] = []
    effective = [concept]
    for role in _dicts(relationship.get("roles")):
        played = role.get("concept")
        if isinstance(played, str) and played not in declared:
            out.append(f"Relationship '{label}' has a role played by unknown concept '{played}'")
        effective.append(str(role.get("name") or played))
    out += [f"Relationship '{label}' repeats the role name '{name}'; name the roles apart"
            for name in _duplicates(effective)]
    return out


def _type_problems(ontology: Ontology, concept: str, *, cyclic: set[str]) -> list[str]:
    kind, supertypes = ontology.kinds.get(concept), ontology.supertypes.get(concept, [])
    known = [s for s in supertypes if ontology.declares(s)]
    if kind == VALUE_TYPE:
        entities = [s for s in known if ontology.is_entity(s)]
        if entities:
            return [f"Value type '{concept}' extends entity type '{entities[0]}'"]
        if concept not in cyclic and len(known) == len(supertypes) and ontology.built_in(concept) is None:
            return [f"Value type '{concept}' does not reach a built-in value type ({', '.join(BUILT_INS)})"]
    if kind == ENTITY_TYPE:
        return [f"Entity type '{concept}' extends '{s}', which is not an entity type" for s in known
                if not ontology.is_entity(s)]
    return []


def _identifier_problems(ontology: Ontology, component: dict[str, Any]) -> list[str]:
    concept = str(component.get("concept"))
    out: list[str] = []
    for identifier in _items(component.get("identify_by")):
        found = ontology.find(concept, identifier) if isinstance(identifier, str) else None
        if found is None:
            out.append(f"Concept '{concept}' identify_by references unknown relationship '{identifier}'")
        elif found.arity != 2:
            out.append(f"Concept '{concept}' identify_by names '{identifier}', which has arity {found.arity}; an "
                       "identifying relationship is binary")
    return out


def _iri_problems(component: dict[str, Any], *, prefixes: dict[str, Any]) -> list[str]:
    concept = str(component.get("concept"))
    out: list[str] = []
    prefix = undeclared_prefix(component.get("iri"), prefixes)
    if prefix:
        out.append(f"Concept '{concept}' iri uses undeclared prefix '{prefix}'")
    for relationship in _dicts(component.get("relationships")):
        prefix = undeclared_prefix(relationship.get("iri"), prefixes)
        if prefix:
            out.append(f"Relationship '{concept}.{relationship.get('name')}' iri uses undeclared prefix '{prefix}'")
    return out


def ontology_problems(data: dict[str, Any]) -> list[str]:
    """Every problem of a schema-valid ontology document: Ossie's validator's checks, then SLayer's own."""
    ontology = Ontology.of(data)
    components = list(_dicts(data.get("ontology")))
    raw_prefixes = data.get("prefixes")
    prefixes: dict[str, Any] = raw_prefixes if isinstance(raw_prefixes, dict) else {}
    out = [f"Prefix '{p}' has an invalid IRI value" for p, iri in prefixes.items() if not _absolute_iri(iri)]
    out += [f"Duplicate concept '{c}' in ontology '{ontology.name}'"
            for c in _duplicates([str(c.get("concept")) for c in components])]
    declared = {*ontology.kinds, *BUILT_INS}
    cycles = _cycles(ontology.supertypes)
    cyclic = {c for cycle in cycles for c in cycle}
    for component in components:
        concept = str(component.get("concept"))
        out += [f"Duplicate relationship '{r}' in concept '{concept}'"
                for r in _duplicates([str(r.get("name")) for r in _dicts(component.get("relationships"))])]
        out += [f"Concept '{concept}' extends unknown concept '{s}'" for s in _items(component.get("extends"))
                if isinstance(s, str) and s not in declared]
        out += _identifier_problems(ontology, component)
        out += _iri_problems(component, prefixes=prefixes)
        for relationship in _dicts(component.get("relationships")):
            out += _role_problems(component, relationship, declared=declared)
    for concept in ontology.kinds:
        out += _type_problems(ontology, concept, cyclic=cyclic)
    out += [f"Concept '{cycle[0]}' extends itself: {' -> '.join(cycle)}" for cycle in cycles]
    return list(dict.fromkeys(out))
