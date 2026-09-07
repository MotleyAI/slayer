"""Join-arity safety: a hop is *provably many-to-one* iff its target-side columns
cover a declared PK/unique set or it declares ``many_to_one``/``one_to_one``.
Unknown = unsafe; only stored edges count (no synthesized reverse hops)."""

from __future__ import annotations

import re
from typing import Optional, Sequence

from pydantic import BaseModel

from slayer.core.enums import JoinCardinality
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
    "resolve_correlation_hop",
    "JoinSafetyFinding",
]


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


def provably_to_one(*, join: ModelJoin, target_model: SlayerModel) -> bool:
    """Is ``join`` provably many-to-one onto ``target_model``? True iff declared
    m:1/1:1, or its target columns fully cover a unique key-set (PHYSICAL spelling)."""
    if join.cardinality in (JoinCardinality.MANY_TO_ONE, JoinCardinality.ONE_TO_ONE):
        return True
    by_name = {c.name: c for c in target_model.columns}
    target_cols = [
        _physical_name(by_name[pair[1]]) if pair[1] in by_name else pair[1]
        for pair in join.join_pairs
    ]
    return is_key_set_unique(
        key_columns=target_cols, unique_key_sets=_unique_key_sets(target_model)
    )


def _find_join(model: SlayerModel, target_name: str) -> Optional[ModelJoin]:
    return next((j for j in model.joins if j.target_model == target_name), None)


def resolve_correlation_hop(
    *, from_model: SlayerModel, to_model: SlayerModel,
) -> Optional[list[tuple[str, str]]]:
    """Oriented ``(from_col, to_col)`` pairs for the hop ``from_model → to_model``,
    for semi-join EXISTS correlation ONLY (never inline/safe classification): a
    unique stored edge wins, else the unique stored forward edge
    ``to_model → from_model`` is inverted; ambiguity or no edge → ``None``."""
    stored = [j for j in from_model.joins if j.target_model == to_model.name]
    if len(stored) == 1:
        return [(src, tgt) for src, tgt in stored[0].join_pairs]
    if len(stored) > 1:
        return None
    forward = [j for j in to_model.joins if j.target_model == from_model.name]
    if len(forward) != 1:
        return None
    return [(tgt, src) for src, tgt in forward[0].join_pairs]


def safe_reachable(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> bool:
    """Is every hop of ``path`` a stored, provably many-to-one edge? Empty is safe;
    an absent edge is never synthesized — a missing stored join fails the walk."""
    current = root
    for target_name in path:
        join = _find_join(current, target_name)
        if join is None:
            return False
        target_model = models_by_name.get(target_name)
        if target_model is None:
            return False
        if not provably_to_one(join=join, target_model=target_model):
            return False
        current = target_model
    return True


class JoinSafetyFinding(BaseModel):
    """One validation flag about a join edge (unproven hop or contradicted declaration)."""

    data_source: str
    model: str
    target_model: str
    message: str
    severity: str = "warning"


_UNPROVEN_REMEDY = (
    "declare `cardinality` many_to_one/one_to_one, declare a covering unique "
    "key on the target, or run cardinality detection"
)


def audit_join_safety(
    *,
    models: Sequence[SlayerModel],
    detection: Optional[JoinCardinalityReport] = None,
) -> list[JoinSafetyFinding]:
    """Flag joins neither declared m:1/1:1 nor structurally proven, plus data-contradicted declarations."""
    # Joins resolve within the parent model's datasource — no cross-datasource shadowing.
    models_by_key = {(m.data_source, m.name): m for m in models}
    findings: list[JoinSafetyFinding] = []
    for model in models:
        for join in model.joins:
            target = models_by_key.get((model.data_source, join.target_model))
            if target is None:
                continue
            if provably_to_one(join=join, target_model=target):
                continue
            findings.append(JoinSafetyFinding(
                data_source=model.data_source,
                model=model.name,
                target_model=join.target_model,
                message=(
                    f"Join {model.name} → {join.target_model} is unproven: "
                    f"metrics crossing it will broadcast rather than join "
                    f"through. Remedies: {_UNPROVEN_REMEDY}."
                ),
            ))
    if detection is not None:
        for finding in detection.findings:
            if finding.verdict is CardinalityVerdict.CONTRADICTS_HARD:
                findings.append(JoinSafetyFinding(
                    data_source=finding.data_source,
                    model=finding.model,
                    target_model=finding.target_model,
                    message=(
                        f"Join {finding.model} → {finding.target_model} declares "
                        f"{finding.stored}, but cardinality detection contradicts "
                        f"it (observed {finding.detected})."
                    ),
                    severity="error",
                ))
    return findings
