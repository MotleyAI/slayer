"""Join-arity safety predicate (DEV-1836 D1).

A join hop is *provably many-to-one* iff its target-side join columns cover a
declared PK/unique set of the target (structural proof) or the join declares
``cardinality`` of ``many_to_one``/``one_to_one``. Unknown = unsafe (fail
closed). Evaluated over EXISTING stored edges only — proving a forward join
never makes an absent reverse hop traversable (F1).

This replaces the arity-blind reachability tests as the authority for value
paths: a metric may vary along a dimension (or read an input through a join)
only when every hop between is provably many-to-one.
"""

from __future__ import annotations

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
    "audit_join_safety",
    "JoinSafetyFinding",
]


def _unique_key_sets(model: SlayerModel) -> list[list[str]]:
    """The declared unique key-sets of ``model``: the composite primary key as
    one set, plus every solo-``unique`` column as a singleton."""
    sets: list[list[str]] = []
    pk = [c.name for c in model.columns if c.primary_key]
    if pk:
        sets.append(pk)
    for c in model.columns:
        if c.unique:
            sets.append([c.name])
    return sets


def provably_to_one(*, join: ModelJoin, target_model: SlayerModel) -> bool:
    """Is ``join`` provably many-to-one onto ``target_model``?

    True iff a trusted declaration says so, or the join's target-side columns
    cover a declared unique key-set of the target (structural proof). Composite
    uniqueness proves only under complete coverage (F6): ``is_key_set_unique``
    requires a full unique set to be a subset of the join's target columns.
    """
    if join.cardinality in (JoinCardinality.MANY_TO_ONE, JoinCardinality.ONE_TO_ONE):
        return True
    target_cols = [pair[1] for pair in join.join_pairs]
    return is_key_set_unique(
        key_columns=target_cols, unique_key_sets=_unique_key_sets(target_model)
    )


def _find_join(model: SlayerModel, target_name: str) -> Optional[ModelJoin]:
    return next((j for j in model.joins if j.target_model == target_name), None)


def safe_reachable(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> bool:
    """Is every hop of ``path`` (a sequence of target model names walked from
    ``root``) a stored, provably many-to-one edge?

    An empty path is safe. An absent edge is never synthesized (F1): a missing
    stored join fails the walk regardless of any forward join's cardinality.
    """
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


# --------------------------------------------------------------------------- #
# Validation pressure (D1/D5, F5)
# --------------------------------------------------------------------------- #
class JoinSafetyFinding(BaseModel):
    """One validation flag about a join edge — an unproven hop or a
    detection-contradicted declaration."""

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
    """Flag every join that is neither declared m:1/1:1 nor structurally proven
    (metrics crossing it broadcast), plus — given a detection report —
    declarations the observed data hard-contradicts."""
    # Joins resolve within the parent model's datasource; same-named models in
    # other datasources must not shadow the real target.
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
