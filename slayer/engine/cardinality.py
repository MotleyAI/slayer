"""Join-cardinality inference helpers and the detection report.

Uniqueness is asymmetric evidence: a scan disproves it with one duplicate, but
can never prove it.
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

    Unique iff some key-set is a non-empty SUBSET: unique ``(a)`` makes
    ``(a, b)`` unique, but unique ``(a, b)`` says nothing about ``(a)``.
    """
    key_set = set(key_columns)
    for uks in unique_key_sets:
        if uks and set(uks) <= key_set:
            return True
    return False


def declares_solo_unique(*, columns, column) -> bool:
    """Does ``column`` ALONE carry a declared uniqueness among ``columns``?

    ``primary_key`` is stamped on every member of a composite PK, so it
    implies solo uniqueness only when the column IS the whole primary key.
    """
    if column.unique:
        return True
    return column.primary_key and sum(1 for c in columns if c.primary_key) == 1


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
    #: Profiled fine but the key population was empty — an empty scan is no
    #: evidence of arity. Unlike SKIPPED_UNSUPPORTED, worth re-running later.
    NO_EVIDENCE = "no_evidence"
    #: The scan itself failed; the message is in ``note``. Contained per join
    #: so one unreadable table cannot abort the whole report.
    SCAN_FAILED = "scan_failed"


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
