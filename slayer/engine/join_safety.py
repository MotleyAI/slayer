"""Join-arity safety: a hop is *provably many-to-one* iff, on its traversal
orientation, its target-side columns cover a declared PK/unique set or its
oriented cardinality is ``many_to_one``/``one_to_one``. Unknown = unsafe.
Both orientations of every declared edge participate; proof is per orientation
(DEV-1853), so one direction may be provably to-one while the other fans out."""

from __future__ import annotations

import re
from typing import Optional, Sequence, Union

from pydantic import BaseModel

from slayer.core.enums import JoinCardinality, invert_cardinality
from slayer.core.join_walker import OrientedJoin, walk
from slayer.core.models import ModelJoin, SlayerModel
from slayer.engine.cardinality import (
    CardinalityVerdict,
    JoinCardinalityReport,
    is_key_set_unique,
)

__all__ = [
    "provably_to_one",
    "safe_reachable",
    "may_inline_crossing_inputs",
    "audit_join_safety",
    "JoinSafetyFinding",
]

#: An oriented hop, or a declared join read in its declared orientation — both
#: expose ``cardinality`` and target-side ``join_pairs``, which is all the proof
#: predicate reads.
OrientedLike = Union[OrientedJoin, ModelJoin]


def may_inline_crossing_inputs(crossed_paths: Sequence[tuple]) -> bool:  # NOSONAR(S1172) — crossed_paths is the documented DEV-1688 seam; the cardinality-aware decision reads it, hardcoded False until then.
    """Whether a crossing-input local aggregate may stay inline in the host base.
    Hardcoded ``False`` (always a producer); the DEV-1688 seam that will flip."""
    return False


#: A bare-identifier ``Column.sql`` rename carries the column's uniqueness.
_BARE_IDENT_RE = re.compile(r"[A-Za-z_]\w*")


def _physical_name(column) -> str:
    """Physical spelling: a bare-identifier ``sql`` rename, else the model name."""
    sql = (column.sql or "").strip()
    return sql if sql and _BARE_IDENT_RE.fullmatch(sql) else column.name


def _unique_key_sets(model: SlayerModel) -> list[list[str]]:
    # PHYSICAL spelling: composite PK as one set, then each solo-unique singleton.
    sets: list[list[str]] = []
    pk = [_physical_name(c) for c in model.columns if c.primary_key]
    if pk:
        sets.append(pk)
    for c in model.columns:
        if c.unique:
            sets.append([_physical_name(c)])
    return sets


def provably_to_one(*, edge: OrientedLike, target_model: SlayerModel) -> bool:
    """Is ``edge`` provably many-to-one onto ``target_model`` in its orientation?
    True iff the oriented cardinality is m:1/1:1, or the traversal-target columns
    fully cover a unique key-set of ``target_model`` (PHYSICAL spelling)."""
    if edge.cardinality in (JoinCardinality.MANY_TO_ONE, JoinCardinality.ONE_TO_ONE):
        return True
    by_name = {c.name: c for c in target_model.columns}
    target_cols = [
        _physical_name(by_name[pair[1]]) if pair[1] in by_name else pair[1]
        for pair in edge.join_pairs
    ]
    return is_key_set_unique(
        key_columns=target_cols, unique_key_sets=_unique_key_sets(target_model)
    )


def safe_reachable(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> bool:
    """Is ``path`` a reachable chain of provably many-to-one hops from ``root``?
    Any declared edge traverses in either orientation (DEV-1853); an unresolvable
    or revisiting hop fails the walk. Empty path is safe. An ambiguous hop raises."""
    chain = walk(root=root, path=path, models_by_name=models_by_name)
    if chain is None:
        return False
    for edge in chain:
        target_model = models_by_name.get(edge.target_model)
        if target_model is None or not provably_to_one(
            edge=edge, target_model=target_model
        ):
            return False
    return True


class JoinSafetyFinding(BaseModel):
    """One audit record per declared edge: the provability of both orientations
    plus, when the declared (forward) orientation is unproven, the broadcast
    consequence and remedies. Detection contradictions are separate findings."""

    data_source: str
    model: str
    target_model: str
    message: str
    severity: str = "warning"
    forward_provably_to_one: bool = False
    reverse_provably_to_one: bool = False
    contradiction: bool = False


_UNPROVEN_REMEDY = (
    "declare `cardinality` many_to_one/one_to_one, declare a covering unique "
    "key on the target, or run cardinality detection"
)


def audit_join_safety(
    *,
    models: Sequence[SlayerModel],
    detection: Optional[JoinCardinalityReport] = None,
) -> list[JoinSafetyFinding]:
    """One finding per declared edge, recording both orientations' provability.
    The forward-unproven edges carry the broadcast consequence and remedies;
    data-contradicted declarations are appended as separate error findings."""
    # Joins resolve within the parent model's datasource — no cross-datasource shadowing.
    models_by_key = {(m.data_source, m.name): m for m in models}
    findings: list[JoinSafetyFinding] = []
    for model in models:
        for join in model.joins:
            target = models_by_key.get((model.data_source, join.target_model))
            if target is not None:
                findings.append(_edge_finding(model=model, join=join, target=target))
    if detection is not None:
        findings.extend(
            _contradiction_finding(finding)
            for finding in detection.findings
            if finding.verdict is CardinalityVerdict.CONTRADICTS_HARD
        )
    return findings


def _edge_finding(
    *, model: SlayerModel, join, target: SlayerModel
) -> JoinSafetyFinding:
    """Provability of one declared edge in both orientations. Orients THIS
    declaration directly (never searches by pairs — parallel edges may share
    them)."""
    fwd_edge = OrientedJoin(
        source_model=model.name, target_model=join.target_model,
        join_pairs=[list(p) for p in join.join_pairs],
        join_type=join.join_type, cardinality=join.cardinality,
        name=join.name, declaring_model=model.name,
    )
    rev_edge = OrientedJoin(
        source_model=join.target_model, target_model=model.name,
        join_pairs=[[p[1], p[0]] for p in join.join_pairs],
        join_type=join.join_type,
        cardinality=invert_cardinality(join.cardinality),
        name=join.name, declaring_model=model.name,
    )
    fwd_ok = provably_to_one(edge=fwd_edge, target_model=target)
    rev_ok = provably_to_one(edge=rev_edge, target_model=model)
    if fwd_ok:
        message = (
            f"Join {model.name} → {join.target_model} is provably "
            f"to-one; the reverse hop "
            f"{join.target_model} → {model.name} "
            f"{'is provably to-one' if rev_ok else 'fans out'}."
        )
    else:
        message = (
            f"Join {model.name} → {join.target_model} is unproven: "
            f"metrics crossing it will broadcast rather than join "
            f"through. Remedies: {_UNPROVEN_REMEDY}."
        )
    return JoinSafetyFinding(
        data_source=model.data_source,
        model=model.name,
        target_model=join.target_model,
        message=message,
        severity="warning" if not fwd_ok else "info",
        forward_provably_to_one=fwd_ok,
        reverse_provably_to_one=rev_ok,
    )


def _contradiction_finding(finding) -> JoinSafetyFinding:
    return JoinSafetyFinding(
        data_source=finding.data_source,
        model=finding.model,
        target_model=finding.target_model,
        message=(
            f"Join {finding.model} → {finding.target_model} declares "
            f"{finding.stored}, but cardinality detection contradicts "
            f"it (observed {finding.detected})."
        ),
        severity="error",
        contradiction=True,
    )
