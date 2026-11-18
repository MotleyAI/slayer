"""Relationship-cardinality inference helpers and the detection report.

Uniqueness is asymmetric evidence: a scan disproves it with one duplicate, but
can never prove it.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from slayer.core.enums import RelationshipCardinality, StrEnum
from slayer.core.models import is_identifier


# ---------------------------------------------------------------------------
# Pure inference
# ---------------------------------------------------------------------------


def declares_solo_unique(*, fields, field) -> bool:
    """Does ``field`` ALONE carry a declared uniqueness among ``fields``?"""
    return field.unique or is_identifier(field=field, fields=fields)


def classify_cardinality(
    *, source_unique: bool, target_unique: bool
) -> RelationshipCardinality:
    """Total classification from the two sides' uniqueness (profiling path)."""
    if source_unique and target_unique:
        return RelationshipCardinality.ONE_TO_ONE
    if target_unique:
        return RelationshipCardinality.MANY_TO_ONE
    if source_unique:
        return RelationshipCardinality.ONE_TO_MANY
    return RelationshipCardinality.MANY_TO_MANY


def infer_structural_cardinality(
    *, source_unique: bool, target_verified_unique: bool
) -> RelationshipCardinality | None:
    """Ingest-time guess. ``None`` unless the target is *verified* unique."""
    if not target_verified_unique:
        return None
    return classify_cardinality(source_unique=source_unique, target_unique=True)


def _claims_source_unique(c: RelationshipCardinality) -> bool:
    return c in (RelationshipCardinality.ONE_TO_ONE, RelationshipCardinality.ONE_TO_MANY)


def _claims_target_unique(c: RelationshipCardinality) -> bool:
    return c in (RelationshipCardinality.ONE_TO_ONE, RelationshipCardinality.MANY_TO_ONE)


# ---------------------------------------------------------------------------
# Detection report (Pydantic, list-of-named-entries — no dict fields)
# ---------------------------------------------------------------------------


class CardinalityVerdict(StrEnum):
    CONFIRMS = "confirms"
    REFINES = "refines"
    FILLS_NONE = "fills_none"
    CONTRADICTS_HARD = "contradicts_hard"
    SKIPPED_UNSUPPORTED = "skipped_unsupported"
    #: Profiled fine but the key population was empty — an empty scan is no
    #: evidence of arity. Unlike SKIPPED_UNSUPPORTED, worth re-running later.
    NO_EVIDENCE = "no_evidence"
    #: The scan itself failed; the message is in ``note``. Contained per relationship
    #: so one unreadable table cannot abort the whole report.
    SCAN_FAILED = "scan_failed"


class SideStats(BaseModel):
    row_count: int  # non-null key rows
    distinct_count: int
    observed_unique: bool


class RelationshipCardinalityFinding(BaseModel):
    data_source: str
    dataset: str
    to: str
    join_pairs: list[list[str]]
    stored: RelationshipCardinality | None = None
    detected: RelationshipCardinality | None = None
    source_side: SideStats | None = None
    target_side: SideStats | None = None
    verdict: CardinalityVerdict
    unique_contradictions: list[str] = Field(default_factory=list)
    note: str | None = None


class RelationshipCardinalityReport(BaseModel):
    findings: list[RelationshipCardinalityFinding] = Field(default_factory=list)


def compute_verdict(
    *,
    stored: RelationshipCardinality | None,
    detected: RelationshipCardinality,
    source_observed_unique: bool,
    target_observed_unique: bool,
) -> CardinalityVerdict:
    """Classify a detected value against the stored one.

    ``CONTRADICTS_HARD`` only when the data disproves a uniqueness the stored
    value asserted; every other mismatch is a soft ``REFINES``.
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
