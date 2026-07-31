"""Join-cardinality inference helpers and the detection report (DEV-1688).

Two layers of inference:

* **Structural** (free, constraint metadata only) — ``infer_structural_cardinality``
  returns a value only when the target key-set is *verified* unique. A declared
  relationship whose target isn't a known PK/unique stays ``None``.
* **Data-profiling** (opt-in, a strong guess) — ``classify_cardinality`` maps the
  two sides' observed uniqueness to a definite cardinality. Uniqueness is
  asymmetric evidence: a full-scan can *disprove* uniqueness (a duplicate is a
  counterexample) but only *suggest* it. ``compute_verdict`` encodes that
  asymmetry — a stored value is only ``CONTRADICTS_HARD`` when observed data
  disproves a uniqueness the stored value asserted.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from slayer.core.enums import JoinCardinality, StrEnum


# ---------------------------------------------------------------------------
# Pure inference
# ---------------------------------------------------------------------------


def is_key_set_unique(
    *, key_columns: list[str], unique_key_sets: list[list[str]]
) -> bool:
    """Is the ``key_columns`` tuple unique given the known unique key-sets?

    A join key-set is unique iff some PK/unique key-set is a NON-EMPTY SUBSET
    of it: if ``(a)`` is unique then ``(a, b)`` is unique. A superset
    constraint (unique on ``(a, b)``) does NOT make ``(a)`` alone unique.
    """
    key_set = set(key_columns)
    for uks in unique_key_sets:
        if uks and set(uks) <= key_set:
            return True
    return False


def classify_cardinality(
    *, source_unique: bool, target_unique: bool
) -> JoinCardinality:
    """Total classification from the two sides' uniqueness (profiling path)."""
    if source_unique and target_unique:
        return JoinCardinality.ONE_TO_ONE
    if target_unique:
        return JoinCardinality.MANY_TO_ONE
    if source_unique:
        return JoinCardinality.ONE_TO_MANY
    return JoinCardinality.MANY_TO_MANY


def infer_structural_cardinality(
    *, source_unique: bool, target_verified_unique: bool
) -> JoinCardinality | None:
    """Ingest-time guess. ``None`` unless the target is *verified* unique."""
    if not target_verified_unique:
        return None
    return classify_cardinality(source_unique=source_unique, target_unique=True)


def _claims_source_unique(c: JoinCardinality) -> bool:
    return c in (JoinCardinality.ONE_TO_ONE, JoinCardinality.ONE_TO_MANY)


def _claims_target_unique(c: JoinCardinality) -> bool:
    return c in (JoinCardinality.ONE_TO_ONE, JoinCardinality.MANY_TO_ONE)


# ---------------------------------------------------------------------------
# Detection report (Pydantic, list-of-named-entries — no dict fields)
# ---------------------------------------------------------------------------


class CardinalityVerdict(StrEnum):
    CONFIRMS = "confirms"
    REFINES = "refines"
    FILLS_NONE = "fills_none"
    CONTRADICTS_HARD = "contradicts_hard"
    SKIPPED_UNSUPPORTED = "skipped_unsupported"


class SideStats(BaseModel):
    row_count: int  # non-null key rows
    distinct_count: int
    observed_unique: bool


class JoinCardinalityFinding(BaseModel):
    data_source: str
    model: str
    target_model: str
    join_pairs: list[list[str]]
    stored: JoinCardinality | None = None
    detected: JoinCardinality | None = None
    source_side: SideStats | None = None
    target_side: SideStats | None = None
    verdict: CardinalityVerdict
    unique_contradictions: list[str] = Field(default_factory=list)
    note: str | None = None


class JoinCardinalityReport(BaseModel):
    findings: list[JoinCardinalityFinding] = Field(default_factory=list)


def compute_verdict(
    *,
    stored: JoinCardinality | None,
    detected: JoinCardinality,
    source_observed_unique: bool,
    target_observed_unique: bool,
) -> CardinalityVerdict:
    """Classify a detected value against the stored one.

    ``CONTRADICTS_HARD`` fires only when the data *disproves* a uniqueness the
    stored value asserted (a side claimed unique but observed to have dups).
    Every other mismatch is a soft ``REFINES`` — "no dups observed" cannot
    disprove a non-uniqueness claim.
    """
    if stored is None:
        return CardinalityVerdict.FILLS_NONE
    if detected == stored:
        return CardinalityVerdict.CONFIRMS
    hard = (_claims_source_unique(stored) and not source_observed_unique) or (
        _claims_target_unique(stored) and not target_observed_unique
    )
    return (
        CardinalityVerdict.CONTRADICTS_HARD if hard else CardinalityVerdict.REFINES
    )
