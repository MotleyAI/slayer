"""Datasource-wide FK edge planning: one edge per FK relationship, parallel FK sets named."""

from __future__ import annotations

import re
from collections import defaultdict

from pydantic import BaseModel, Field

from slayer.core.relationship_edges import RelationshipSignature, is_valid_edge_name, relationship_signature
from slayer.core.models import DatasetRelationship, SlayerDataset
from slayer.storage.base import _inverse_survivor

_STEM_SUFFIX_RE = re.compile(r"_(id|fk)$", re.IGNORECASE)


class FkEdgePlan(BaseModel):
    """Every model after planning, plus what the plan changed per declaring model."""

    datasets: dict[str, SlayerDataset]
    added: dict[str, list[DatasetRelationship]] = Field(default_factory=dict)
    named: dict[str, list[str]] = Field(default_factory=dict)


def _field_stem(field: str) -> str:
    return _STEM_SUFFIX_RE.sub("", field) or field


def _fk_stem(relationship: DatasetRelationship) -> str:
    return "_".join(_field_stem(p[0]) for p in relationship.join_pairs)


def _name_candidates(*, declaring: str, stem: str):
    yield stem
    prefixed = f"{declaring}_{stem}"
    if not is_valid_edge_name(prefixed):
        return
    yield prefixed
    n = 2
    while True:
        yield f"{prefixed}_{n}"
        n += 1


def _surviving_candidates(
    *, datasets: dict[str, SlayerDataset], candidates: dict[str, list[DatasetRelationship]],
) -> dict[RelationshipSignature, tuple[str, DatasetRelationship]]:
    """Live FKs not yet represented by any edge; an exact-inverse pair keeps one half."""
    stored = {relationship_signature(dataset_name=m.name, relationship=j) for m in datasets.values() for j in m.relationships}
    chosen: dict[RelationshipSignature, tuple[str, DatasetRelationship]] = {}
    for model_name in sorted(candidates):
        for join in candidates[model_name]:
            if join.to not in datasets:
                continue
            sig = relationship_signature(dataset_name=model_name, relationship=join)
            if sig in stored:
                continue
            prev = chosen.get(sig)
            if prev is None or _inverse_survivor(
                dataset_a=prev[0], relationship_a=prev[1].model_dump(mode="json"),
                dataset_b=model_name, relationship_b=join.model_dump(mode="json"),
            ) == model_name:
                chosen[sig] = (model_name, join.model_copy(update={"name": None}))
    return chosen


def _assign_names(
    *, relationships: dict[str, list[DatasetRelationship]], live: set[RelationshipSignature], dataset_names: set[str],
) -> dict[tuple[str, int], str]:
    """Names for every unnamed edge of a parallel set holding a FK-backed edge."""
    groups: dict[tuple[str, str], list[tuple[str, int, DatasetRelationship]]] = defaultdict(list)
    incident: dict[str, set[str]] = defaultdict(set)
    for declaring, model_joins in relationships.items():
        for i, join in enumerate(model_joins):
            pair = tuple(sorted((declaring, join.to)))
            groups[(pair[0], pair[1])].append((declaring, i, join))
            if join.name:
                incident[declaring].add(join.name)
                incident[join.to].add(join.name)
    pending = sorted(
        (
            (pair, declaring, _fk_stem(join), sorted((p[0], p[1]) for p in join.join_pairs), i, join)
            for pair, members in groups.items()
            if len(members) >= 2
            and any(relationship_signature(dataset_name=d, relationship=j) in live for d, _, j in members)
            for declaring, i, join in members if not join.name
        ),
        key=lambda e: e[:5],
    )
    names: dict[tuple[str, int], str] = {}
    for _, declaring, stem, _, i, join in pending:
        taken = dataset_names | incident[declaring] | incident[join.to]
        name = next(
            (c for c in _name_candidates(declaring=declaring, stem=stem) if is_valid_edge_name(c) and c not in taken),
            None,
        )
        if name is None:
            continue
        names[(declaring, i)] = name
        incident[declaring].add(name)
        incident[join.to].add(name)
    return names


def plan_fk_edges(*, datasets: dict[str, SlayerDataset], candidates: dict[str, list[DatasetRelationship]]) -> FkEdgePlan:
    """Add each unrepresented live FK in ``candidates`` to its declaring model, then name parallel FK sets."""
    live = {
        relationship_signature(dataset_name=m, relationship=j)
        for m, js in candidates.items() for j in js if j.to in datasets
    }
    added: dict[str, list[DatasetRelationship]] = defaultdict(list)
    for declaring, join in _surviving_candidates(datasets=datasets, candidates=candidates).values():
        added[declaring].append(join)
    joins = {n: [*m.relationships, *added.get(n, [])] for n, m in datasets.items()}
    names = _assign_names(relationships=joins, live=live, dataset_names=set(datasets))

    named: dict[str, list[str]] = defaultdict(list)
    out: dict[str, SlayerDataset] = {}
    for model_name, model in datasets.items():
        final = [
            j.model_copy(update={"name": names[(model_name, i)]}) if (model_name, i) in names else j
            for i, j in enumerate(joins[model_name])
        ]
        named[model_name] = [names[(model_name, i)] for i in range(len(final)) if (model_name, i) in names]
        out[model_name] = model.model_copy(update={"relationships": final}) if final != model.relationships else model
    return FkEdgePlan(
        datasets=out,
        added={n: [out[n].relationships[len(datasets[n].relationships) + k] for k in range(len(js))] for n, js in added.items()},
        named={n: v for n, v in named.items() if v},
    )
