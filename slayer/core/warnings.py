"""Warning payload types on ``SlayerResponse.warnings`` — surfaced as Python warnings and as
structured payloads. Live in ``slayer.core`` so schemas reference them without engine code."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, Field, model_validator

_caller_announces: ContextVar[bool] = ContextVar("slayer_caller_announces", default=False)


@contextmanager
def announced_by_caller() -> Iterator[None]:
    """Inside the block the caller (a query response, a save) announces stored-document advisories, so loads do not."""
    token = _caller_announces.set(True)
    try:
        yield
    finally:
        _caller_announces.reset(token)


def caller_announces() -> bool:
    return _caller_announces.get()


class SlayerWarning(BaseModel):
    """Base of the warning family on ``SlayerResponse.warnings``; every payload declares a ``kind`` discriminator so consumers switch on it, not on a field's presence."""

    kind: str

    def human_message(self) -> str:
        """One operator-readable line; subclasses override, the base is the honest last resort."""
        return f"{self.kind}: {self.model_dump(exclude={'kind'})}"


class NormalizationWarning(SlayerWarning):
    """One slack-normalization event — a rewrite or a report-only advisory (``rewritten=False``). ``rule_id`` names the rule; ``location`` points into the input."""

    kind: Literal["normalization"] = "normalization"
    rule_id: str
    original: str
    normalized: str
    location: str
    rule_doc_url: Optional[str] = None
    # Report-only rules: the message must not claim a
    # transform that never happened.
    rewritten: bool = True

    def human_message(self) -> str:
        if not self.rewritten:
            return (
                f"[{self.rule_id}] flagged {self.original}: {self.normalized} "
                f"(at {self.location})"
            )
        return (
            f"[{self.rule_id}] rewrote {self.original} → {self.normalized} "
            f"(at {self.location})"
        )


class BroadcastDimension(BaseModel):
    """One query dimension a metric could not attribute, with the broadcast reason."""

    dimension: str
    reason: str


#: Dice–slice disclosure on every requested broadcast.
DICE_SLICE_HINT = (
    "filtering on this dimension restricts the metric by association while "
    "slicing only repeats the value; omit to_many_handling (or set 'associate') "
    "to reconcile the two."
)


def association_fallback_hint(reason: str) -> str:
    """The hint of a broadcast the default mode fell back to because association does not support ``reason``."""
    return (
        f"association does not support {reason} yet, so the value repeats across the cells; "
        "filter on the dimension instead, or drop the dimension(s) it cannot be attributed to."
    )


class BroadcastGrainWarningPayload(SlayerWarning):
    """A cross-model aggregate whose implicit grain lost query dimensions to broadcasting; ``measure`` names the metric, ``dimensions`` lists each broadcast dimension and its reason, ``association_unsupported`` why the default mode could not associate."""

    kind: Literal["broadcast"] = "broadcast"
    measure: str
    location: str
    dimensions: list[BroadcastDimension]
    association_unsupported: Optional[str] = None
    hint: str = DICE_SLICE_HINT

    @model_validator(mode="after")
    def _fallback_hint(self) -> "BroadcastGrainWarningPayload":
        if self.association_unsupported is not None and "hint" not in self.model_fields_set:
            self.hint = association_fallback_hint(self.association_unsupported)
        return self

    def human_message(self) -> str:
        dims = ", ".join(f"{d.dimension} ({d.reason})" for d in self.dimensions)
        return (
            f"metric {self.measure!r} (at {self.location}) broadcast across "
            f"unattributable dimension(s): {dims} — {self.hint}"
        )


class AssociatedWarningPayload(SlayerWarning):
    """An aggregate resolved by distinct-entity association over unattributable dimension(s); the cells' entity populations may overlap and are not additive across ``dimensions``."""

    kind: Literal["associated"] = "associated"
    measure: str
    location: str
    dimensions: list[str]

    def human_message(self) -> str:
        dims = ", ".join(self.dimensions)
        return (
            f"metric {self.measure!r} (at {self.location}) associated over "
            f"unattributable dimension(s) {dims}; cell populations may overlap "
            f"and are not additive across them"
        )


class UnrelatedDatasetWarningPayload(SlayerWarning):
    """Aggregates over ``dataset``, which no join path reaches from ``population``: each is computed over the dataset's own rows and repeated on every row."""

    kind: Literal["unrelated_dataset"] = "unrelated_dataset"
    dataset: str
    population: str
    measures: list[str]
    location: str

    def human_message(self) -> str:
        return (
            f"{', '.join(repr(m) for m in self.measures)} (at {self.location}) read {self.dataset!r}, which no "
            f"join path reaches from {self.population!r}: unrelated dataset, aggregated over its own rows and "
            f"repeated on every row"
        )


class DegenerateReaggregationWarningPayload(SlayerWarning):
    """A second-order aggregation whose operand grain equals the outer grain, so it is the identity; ``operand_grain`` / ``outer_grain`` name the equal grains and ``hint`` the partition_by remedy."""

    kind: Literal["degenerate_reaggregation"] = "degenerate_reaggregation"
    measure: str
    location: str
    operand_grain: list[str]
    outer_grain: list[str]
    hint: str = (
        "add a finer partition_by= to the inner aggregate so the outer "
        "aggregation combines across distinct cells"
    )

    def human_message(self) -> str:
        og = ", ".join(self.operand_grain) or "<global>"
        return (
            f"metric {self.measure!r} (at {self.location}) is a degenerate "
            f"re-aggregation: the operand grain ({og}) equals the outer grain, "
            f"so it returns the operand unchanged — {self.hint}"
        )


class SemiJoinPushedWarningPayload(SlayerWarning):
    """A ROW filter pushed as a semi-join (EXISTS): correctly applied, informational only — carried on the response, never a Python warning and never an error. ``measure`` names the affected aggregate, or is ``null`` when the query population itself is restricted."""

    kind: Literal["semi_join_pushed"] = "semi_join_pushed"
    measure: Optional[str] = None
    location: str
    filter_text: str

    def human_message(self) -> str:
        if self.measure is None:
            return (
                f"filter {self.filter_text!r} restricts the population "
                f"(at {self.location}) by semi-join"
            )
        return (
            f"filter {self.filter_text!r} pushed into metric {self.measure!r} "
            f"(at {self.location}) by semi-join"
        )


class ResponseTruncationWarning(SlayerWarning):
    """A response sliced to a row cap; ``hint`` tells the caller how to get more rows. Emitted by the MCP layer only, never by the engine."""

    kind: Literal["truncated"] = "truncated"
    returned_rows: int
    hint: str

    def human_message(self) -> str:
        return (
            f"showing first {self.returned_rows} rows — more rows exist; {self.hint}"
        )


class StatementTimeoutSkippedWarning(SlayerWarning):
    """A statement ran without the caller's statement timeout; ``reason`` says why."""

    kind: Literal["statement_timeout_skipped"] = "statement_timeout_skipped"
    datasource: str
    timeout_seconds: int
    reason: Literal["readonly_user", "timeout_rejected"]

    def human_message(self) -> str:
        head = f"datasource {self.datasource!r} ran without SLayer's {self.timeout_seconds}s statement timeout"
        if self.reason == "readonly_user":
            return (
                f"{head}: its ClickHouse user has readonly = 1 and may change no settings; "
                "the server profile's limit applies — grant readonly = 2 to enable the timeout"
            )
        return f"{head}: the database rejected the timeout statement"


class WholePeriodsNonNestingWarningPayload(SlayerWarning):
    """``whole_periods_only`` over time dimensions on one ``column`` whose ``granularities`` do not nest into each other."""

    kind: Literal["whole_periods_non_nesting"] = "whole_periods_non_nesting"
    column: str
    granularities: list[str]
    location: str
    hint: str = (
        "whole_periods_only snaps to the earlier bucket boundary, so edge buckets of "
        "one granularity may be partial"
    )

    def human_message(self) -> str:
        return (
            f"granularities {' and '.join(self.granularities)} on {self.column!r} "
            f"(at {self.location}) do not nest — {self.hint}"
        )


def _single_line(text: str) -> str:
    return text.replace("\r", "\\r").replace("\n", "\\n")


class UnloadableDocumentWarning(SlayerWarning):
    """A stored model or memory that could not be loaded was skipped; ``data_source`` is ``None`` for a memory."""

    kind: Literal["unloadable_document"] = "unloadable_document"
    document_kind: Literal["model", "memory"]
    data_source: Optional[str] = None
    name: str
    cause: str

    def human_message(self) -> str:
        document = f"{self.data_source}.{self.name}" if self.data_source is not None else f"memory:{self.name}"
        return _single_line(f"skipped stored {self.document_kind} {document!r}, which cannot be loaded: {self.cause}")


class NarrowedAccessWarning(SlayerWarning):
    """Model ``model`` reads tagged models (``narrowed_by``) whose tags do not cover its own, so some of its readers cannot see it."""

    kind: Literal["narrowed_access"] = "narrowed_access"
    data_source: str
    model: str
    narrowed_by: list[str]

    def human_message(self) -> str:
        return (
            f"narrowed access: model {self.data_source}.{self.model} reads {', '.join(self.narrowed_by)}, "
            f"whose access tags do not cover its own; callers lacking those tags cannot see it"
        )


class LibraryNameShadowWarning(SlayerWarning):
    """Model ``model``'s aggregation ``aggregation`` shadows the library function written ``function`` in its own model."""

    kind: Literal["library_name_shadow"] = "library_name_shadow"
    model: str
    aggregation: str
    function: str

    def human_message(self) -> str:
        return (
            f"model {self.model!r} defines the aggregation {self.aggregation!r}, so {self.function}(…) over its "
            f"columns is that aggregation, not the library function {self.function.lower()}()"
        )


class BrokenDependentWarning(SlayerWarning):
    """A re-ingest re-typed ``column`` to a list, and the stored definition ``name`` that planned before it fails
    after (``checked=False``: the check could not run, for ``reason``)."""

    kind: Literal["broken_dependent"] = "broken_dependent"
    model: str
    data_source: str
    dependent_kind: Literal["measure", "column", "filter", "saved_query"]
    name: str
    column: str
    checked: bool = True
    error_type: Optional[str] = None
    error: Optional[str] = None
    reason: Optional[str] = None

    def human_message(self) -> str:
        where = f"{self.dependent_kind} {self.data_source}.{self.model}.{self.name}"
        if not self.checked:
            return _single_line(f"{where} reads {self.column}, re-typed to a list, and could not be checked: "
                                f"{self.reason}")
        return _single_line(f"{where} breaks now that {self.column} is a list: {self.error}")


class WholeListWarning(SlayerWarning):
    """A query reads the whole list ``list`` as its element set: grouping by it (``operation="group"``) or counting
    its distinct element sets (``operation="count_distinct"``)."""

    kind: Literal["whole_list"] = "whole_list"
    operation: Literal["group", "count_distinct"]
    list: str

    def human_message(self) -> str:
        if self.operation == "group":
            return (f"grouping by the list {self.list!r} reads each whole list as a set of its distinct elements; "
                    f"group by {self.list}.value for one row per element")
        return (f"count_distinct({self.list}) counts distinct whole lists, each read as a set of its distinct "
                f"elements; count_distinct({self.list}.value) counts distinct elements")


class NameCollisionWarning(SlayerWarning):
    """Stored model ``model`` holds ``names`` in one ``namespace``, differing only in case; each resolves only when
    spelled exactly (or double-quoted)."""

    kind: Literal["name_collision"] = "name_collision"
    model: str
    namespace: str
    names: List[str]

    def human_message(self) -> str:
        return (f"model {self.model!r} declares {' and '.join(repr(n) for n in self.names)} in its {self.namespace}, "
                f"differing only in case; spell one exactly or double-quote it, and rename one to remove the "
                f"ambiguity")


class DeprecatedSpellingWarning(SlayerWarning):
    """A deprecated Mode-B ``spelling`` was read (``written``); ``canonical`` is its twin, removed in ``removal``."""

    kind: Literal["deprecated_spelling"] = "deprecated_spelling"
    spelling: str
    written: str
    canonical: str
    removal: str = "3.0"

    def human_message(self) -> str:
        return (f"the {self.spelling} spelling `{self.written}` is deprecated and removed in SLayer {self.removal}; "
                f"write `{self.canonical}`")


# Discriminated union, not the bare base: a ``List[SlayerWarning]`` would validate
# down to the base type and drop subclass fields. Keyed on ``kind``, each round-trips.
AnySlayerWarning = Annotated[
    Union[
        NormalizationWarning,
        BroadcastGrainWarningPayload,
        AssociatedWarningPayload,
        UnrelatedDatasetWarningPayload,
        DegenerateReaggregationWarningPayload,
        SemiJoinPushedWarningPayload,
        ResponseTruncationWarning,
        StatementTimeoutSkippedWarning,
        WholePeriodsNonNestingWarningPayload,
        UnloadableDocumentWarning,
        NarrowedAccessWarning,
        LibraryNameShadowWarning,
        BrokenDependentWarning,
        WholeListWarning,
        DeprecatedSpellingWarning,
        NameCollisionWarning,
    ],
    Field(discriminator="kind"),
]


class SlayerNormalizationWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``NormalizationWarning`` payload — one wording on both channels."""

    def __init__(self, payload: NormalizationWarning) -> None:
        super().__init__(payload)  # arg mirrors param; __str__ is the one wording (pytest-xdist degrades to str() for the unserializable payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerUnrelatedDatasetWarning(UserWarning):
    """Carrier ``UserWarning`` for an ``UnrelatedDatasetWarningPayload`` — one wording on both channels."""

    def __init__(self, payload: UnrelatedDatasetWarningPayload) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerWholePeriodsWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``WholePeriodsNonNestingWarningPayload`` — one wording on both channels."""

    def __init__(self, payload: WholePeriodsNonNestingWarningPayload) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerUnloadableDocumentWarning(UserWarning):
    """Carrier ``UserWarning`` for an ``UnloadableDocumentWarning`` payload — one wording on both channels."""

    def __init__(self, payload: UnloadableDocumentWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerNarrowedAccessWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``NarrowedAccessWarning`` payload — one wording on both channels."""

    def __init__(self, payload: NarrowedAccessWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerStatementTimeoutSkippedWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``StatementTimeoutSkippedWarning`` payload — one wording on both channels."""

    def __init__(self, payload: StatementTimeoutSkippedWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerLibraryNameShadowWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``LibraryNameShadowWarning`` payload — one wording on both channels."""

    def __init__(self, payload: LibraryNameShadowWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerBrokenDependentWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``BrokenDependentWarning`` payload — one wording on both channels."""

    def __init__(self, payload: BrokenDependentWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerWholeListWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``WholeListWarning`` payload — one wording on both channels."""

    def __init__(self, payload: WholeListWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerNameCollisionWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``NameCollisionWarning`` payload — one wording on both channels."""

    def __init__(self, payload: NameCollisionWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()

