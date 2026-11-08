"""Datasource-wide FK edge planning: one edge per FK relationship, parallel FK sets named."""

from __future__ import annotations

import re
from collections import defaultdict

from pydantic import BaseModel, Field

from slayer.core.join_edges import RelationshipSignature, is_valid_edge_name, relationship_signature
from slayer.core.models import ModelJoin, SlayerModel
from slayer.storage.base import _inverse_survivor

_STEM_SUFFIX_RE = re.compile(r"_(id|fk)$", re.IGNORECASE)


class FkEdgePlan(BaseModel):
    """Every model after planning, plus what the plan changed per declaring model."""

    models: dict[str, SlayerModel]
    added: dict[str, list[ModelJoin]] = Field(default_factory=dict)
    named: dict[str, list[str]] = Field(default_factory=dict)


def _column_stem(column: str) -> str:
    return _STEM_SUFFIX_RE.sub("", column) or column


def _fk_stem(join: ModelJoin) -> str:
    return "_".join(_column_stem(p[0]) for p in join.join_pairs)


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
    *, models: dict[str, SlayerModel], candidates: dict[str, list[ModelJoin]],
) -> dict[RelationshipSignature, tuple[str, ModelJoin]]:
    """Live FKs not yet represented by any edge; an exact-inverse pair keeps one half."""
    stored = {relationship_signature(model_name=m.name, join=j) for m in models.values() for j in m.joins}
    chosen: dict[RelationshipSignature, tuple[str, ModelJoin]] = {}
    for model_name in sorted(candidates):
        for join in candidates[model_name]:
            if join.target_model not in models:
                continue
            sig = relationship_signature(model_name=model_name, join=join)
            if sig in stored:
                continue
            prev = chosen.get(sig)
            if prev is None or _inverse_survivor(
                model_a=prev[0], join_a=prev[1].model_dump(mode="json"),
                model_b=model_name, join_b=join.model_dump(mode="json"),
            ) == model_name:
                chosen[sig] = (model_name, join.model_copy(update={"name": None}))
    return chosen


def _assign_names(
    *, joins: dict[str, list[ModelJoin]], live: set[RelationshipSignature], model_names: set[str],
) -> dict[tuple[str, int], str]:
    """Names for every unnamed edge of a parallel set holding a FK-backed edge."""
    groups: dict[tuple[str, str], list[tuple[str, int, ModelJoin]]] = defaultdict(list)
    incident: dict[str, set[str]] = defaultdict(set)
    for declaring, model_joins in joins.items():
        for i, join in enumerate(model_joins):
            pair = tuple(sorted((declaring, join.target_model)))
            groups[(pair[0], pair[1])].append((declaring, i, join))
            if join.name:
                incident[declaring].add(join.name)
                incident[join.target_model].add(join.name)
    pending = sorted(
        (
            (pair, declaring, _fk_stem(join), sorted((p[0], p[1]) for p in join.join_pairs), i, join)
            for pair, members in groups.items()
            if len(members) >= 2
            and any(relationship_signature(model_name=d, join=j) in live for d, _, j in members)
            for declaring, i, join in members if not join.name
        ),
        key=lambda e: e[:5],
    )
    names: dict[tuple[str, int], str] = {}
    for _, declaring, stem, _, i, join in pending:
        taken = model_names | incident[declaring] | incident[join.target_model]
        name = next(
            (c for c in _name_candidates(declaring=declaring, stem=stem) if is_valid_edge_name(c) and c not in taken),
            None,
        )
        if name is None:
            continue
        names[(declaring, i)] = name
        incident[declaring].add(name)
        incident[join.target_model].add(name)
    return names


def plan_fk_edges(*, models: dict[str, SlayerModel], candidates: dict[str, list[ModelJoin]]) -> FkEdgePlan:
    """Add each unrepresented live FK in ``candidates`` to its declaring model, then name parallel FK sets."""
    live = {
        relationship_signature(model_name=m, join=j)
        for m, js in candidates.items() for j in js if j.target_model in models
    }
    added: dict[str, list[ModelJoin]] = defaultdict(list)
    for declaring, join in _surviving_candidates(models=models, candidates=candidates).values():
        added[declaring].append(join)
    joins = {n: [*m.joins, *added.get(n, [])] for n, m in models.items()}
    names = _assign_names(joins=joins, live=live, model_names=set(models))

    named: dict[str, list[str]] = defaultdict(list)
    out: dict[str, SlayerModel] = {}
    for model_name, model in models.items():
        final = [
            j.model_copy(update={"name": names[(model_name, i)]}) if (model_name, i) in names else j
            for i, j in enumerate(joins[model_name])
        ]
        named[model_name] = [names[(model_name, i)] for i in range(len(final)) if (model_name, i) in names]
        out[model_name] = model.model_copy(update={"joins": final}) if final != model.joins else model
    return FkEdgePlan(
        models=out,
        added={n: [out[n].joins[len(models[n].joins) + k] for k in range(len(js))] for n, js in added.items()},
        named={n: v for n, v in named.items() if v},
    )
