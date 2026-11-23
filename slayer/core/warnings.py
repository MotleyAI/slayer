"""Warning payload types on ``SlayerResponse.warnings`` — surfaced as Python warnings and as
structured payloads. Live in ``slayer.core`` so schemas reference them without engine code."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Annotated, Any, List, Literal, Optional, Union

from pydantic import BaseModel, Field, ModelWrapValidatorHandler, model_validator
from typing_extensions import deprecated

from slayer.core import vocabulary  # a module reference: vocabulary reaches this module through deprecation

_caller_announces: ContextVar[bool] = ContextVar("slayer_caller_announces", default=False)


@contextmanager
def announced_by_caller() -> Generator[None, None, None]:
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

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler[SlayerWarning]) -> SlayerWarning:
        table = vocabulary.WARNING_KEYS.get(cls.model_fields["kind"].default)
        if not table or not isinstance(data, dict):
            return handler(data)
        return vocabulary.renamed_input(data, handler, table, stored=False)

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


def broadcast_hint(*, call: str, partition_by: list[str]) -> str:
    """A broadcast warning's hint: the ``partition_by=`` giving ``call`` the same values, then ``DICE_SLICE_HINT``."""
    return (
        f"{call} with partition_by=[{', '.join(partition_by)}] returns the same values without a warning; "
        f"{DICE_SLICE_HINT}"
    )


class BroadcastGrainWarningPayload(SlayerWarning):
    """A cross-dataset aggregate whose implicit grain lost query dimensions to broadcasting; ``metric`` names the metric, ``dimensions`` lists each broadcast dimension and its reason."""

    kind: Literal["broadcast"] = "broadcast"
    metric: str
    location: str
    dimensions: list[BroadcastDimension]
    #: The dimensions the aggregate attributes: its ``partition_by=`` returning the same values without a warning.
    partition_by: list[str] = Field(default_factory=list)
    hint: str = DICE_SLICE_HINT

    def human_message(self) -> str:
        dims = ", ".join(f"{d.dimension} ({d.reason})" for d in self.dimensions)
        return (
            f"metric {self.metric!r} (at {self.location}) broadcast across "
            f"unattributable dimension(s): {dims} — {self.hint}"
        )

    @property
    @deprecated("measure is deprecated; use metric", category=None)
    def measure(self) -> str:
        vocabulary.attribute_used(self, old="measure", new="metric")
        return self.metric

    @measure.setter
    @deprecated("measure is deprecated; use metric", category=None)
    def measure(self, value: str) -> None:
        vocabulary.attribute_used(self, old="measure", new="metric")
        self.metric = value


class AssociatedWarningPayload(SlayerWarning):
    """An aggregate resolved by distinct-entity association over unattributable dimension(s); the cells' entity populations may overlap and are not additive across ``dimensions``."""

    kind: Literal["associated"] = "associated"
    metric: str
    location: str
    dimensions: list[str]

    def human_message(self) -> str:
        dims = ", ".join(self.dimensions)
        return (
            f"metric {self.metric!r} (at {self.location}) associated over "
            f"unattributable dimension(s) {dims}; cell populations may overlap "
            f"and are not additive across them"
        )

    @property
    @deprecated("measure is deprecated; use metric", category=None)
    def measure(self) -> str:
        vocabulary.attribute_used(self, old="measure", new="metric")
        return self.metric

    @measure.setter
    @deprecated("measure is deprecated; use metric", category=None)
    def measure(self, value: str) -> None:
        vocabulary.attribute_used(self, old="measure", new="metric")
        self.metric = value


class UnrelatedDatasetWarningPayload(SlayerWarning):
    """Aggregates over ``dataset``, which no relationship path reaches from ``population``: each is computed over the dataset's own rows and repeated on every row."""

    kind: Literal["unrelated_dataset"] = "unrelated_dataset"
    dataset: str
    #: ``None`` in a query without dimensions, whose measure homes no relationship path connects.
    population: Optional[str] = None
    metrics: list[str]
    location: str

    def human_message(self) -> str:
        if self.population is None:
            return (
                f"{', '.join(repr(m) for m in self.metrics)} (at {self.location}) read {self.dataset!r}, which no "
                f"relationship path connects to the query's other measures: each is aggregated over its own rows"
            )
        return (
            f"{', '.join(repr(m) for m in self.metrics)} (at {self.location}) read {self.dataset!r}, which no "
            f"relationship path reaches from {self.population!r}: unrelated dataset, aggregated over its own rows and "
            f"repeated on every row"
        )

    @property
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self) -> list[str]:
        vocabulary.attribute_used(self, old="measures", new="metrics")
        return self.metrics

    @measures.setter
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self, value: list[str]) -> None:
        vocabulary.attribute_used(self, old="measures", new="metrics")
        self.metrics = value


class DegenerateReaggregationWarningPayload(SlayerWarning):
    """A second-order aggregation whose operand grain equals the outer grain, so it is the identity; ``operand_grain`` / ``outer_grain`` name the equal grains and ``hint`` the partition_by remedy."""

    kind: Literal["degenerate_reaggregation"] = "degenerate_reaggregation"
    metric: str
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
            f"metric {self.metric!r} (at {self.location}) is a degenerate "
            f"re-aggregation: the operand grain ({og}) equals the outer grain, "
            f"so it returns the operand unchanged — {self.hint}"
        )

    @property
    @deprecated("measure is deprecated; use metric", category=None)
    def measure(self) -> str:
        vocabulary.attribute_used(self, old="measure", new="metric")
        return self.metric

    @measure.setter
    @deprecated("measure is deprecated; use metric", category=None)
    def measure(self, value: str) -> None:
        vocabulary.attribute_used(self, old="measure", new="metric")
        self.metric = value


class SemiJoinPushedWarningPayload(SlayerWarning):
    """A ROW filter pushed as a semi-join (EXISTS): correctly applied, informational only — carried on the response, never a Python warning and never an error. ``metric`` names the affected aggregate, or is ``null`` when the query population itself is restricted."""

    kind: Literal["semi_join_pushed"] = "semi_join_pushed"
    metric: Optional[str] = None
    location: str
    filter_text: str

    def human_message(self) -> str:
        if self.metric is None:
            return (
                f"filter {self.filter_text!r} restricts the population "
                f"(at {self.location}) by semi-join"
            )
        return (
            f"filter {self.filter_text!r} pushed into metric {self.metric!r} "
            f"(at {self.location}) by semi-join"
        )

    @property
    @deprecated("measure is deprecated; use metric", category=None)
    def measure(self) -> Optional[str]:
        vocabulary.attribute_used(self, old="measure", new="metric")
        return self.metric

    @measure.setter
    @deprecated("measure is deprecated; use metric", category=None)
    def measure(self, value: Optional[str]) -> None:
        vocabulary.attribute_used(self, old="measure", new="metric")
        self.metric = value


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
    """``whole_periods_only`` over time dimensions on one ``field`` whose ``granularities`` do not nest into each other."""

    kind: Literal["whole_periods_non_nesting"] = "whole_periods_non_nesting"
    field: str
    granularities: list[str]
    location: str
    hint: str = (
        "whole_periods_only snaps to the earlier bucket boundary, so edge buckets of "
        "one granularity may be partial"
    )

    def human_message(self) -> str:
        return (
            f"granularities {' and '.join(self.granularities)} on {self.field!r} "
            f"(at {self.location}) do not nest — {self.hint}"
        )

    @property
    @deprecated("column is deprecated; use field", category=None)
    def column(self) -> str:
        vocabulary.attribute_used(self, old="column", new="field")
        return self.field

    @column.setter
    @deprecated("column is deprecated; use field", category=None)
    def column(self, value: str) -> None:
        vocabulary.attribute_used(self, old="column", new="field")
        self.field = value


def _single_line(text: str) -> str:
    return text.replace("\r", "\\r").replace("\n", "\\n")


class ExtensionFieldInlinedWarning(SlayerWarning):
    """A stored query's extension field was inlined where it is used; its text now reads as Mode B."""

    kind: Literal["extension_field_inlined"] = "extension_field_inlined"
    document: str
    field: str

    def human_message(self) -> str:
        return _single_line(
            f"stored document {self.document!r}: extension field {self.field!r} was inlined where it is used; "
            f"its text is now read as a SLayer expression"
        )

    @property
    @deprecated("column is deprecated; use field", category=None)
    def column(self) -> str:
        vocabulary.attribute_used(self, old="column", new="field")
        return self.field

    @column.setter
    @deprecated("column is deprecated; use field", category=None)
    def column(self, value: str) -> None:
        vocabulary.attribute_used(self, old="column", new="field")
        self.field = value


class UnloadableDocumentWarning(SlayerWarning):
    """A stored dataset or memory that could not be loaded was skipped; ``data_source`` is ``None`` for a memory."""

    kind: Literal["unloadable_document"] = "unloadable_document"
    document_kind: Literal["dataset", "memory"]
    data_source: Optional[str] = None
    name: str
    cause: str

    def human_message(self) -> str:
        document = f"{self.data_source}.{self.name}" if self.data_source is not None else f"memory:{self.name}"
        return _single_line(f"skipped stored {self.document_kind} {document!r}, which cannot be loaded: {self.cause}")


class NarrowedAccessWarning(SlayerWarning):
    """Dataset ``dataset`` reads tagged datasets (``narrowed_by``) whose tags do not cover its own, so some of its readers cannot see it."""

    kind: Literal["narrowed_access"] = "narrowed_access"
    data_source: str
    dataset: str
    narrowed_by: list[str]

    def human_message(self) -> str:
        return (
            f"narrowed access: dataset {self.data_source}.{self.dataset} reads {', '.join(self.narrowed_by)}, "
            f"whose access tags do not cover its own; callers lacking those tags cannot see it"
        )

    @property
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self) -> str:
        vocabulary.attribute_used(self, old="model", new="dataset")
        return self.dataset

    @model.setter
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self, value: str) -> None:
        vocabulary.attribute_used(self, old="model", new="dataset")
        self.dataset = value


class LibraryNameShadowWarning(SlayerWarning):
    """Dataset ``dataset``'s aggregation ``aggregation`` shadows the library function written ``function`` in its own dataset."""

    kind: Literal["library_name_shadow"] = "library_name_shadow"
    dataset: str
    aggregation: str
    function: str

    def human_message(self) -> str:
        return (
            f"dataset {self.dataset!r} defines the aggregation {self.aggregation!r}, so {self.function}(…) over its "
            f"fields is that aggregation, not the library function {self.function.lower()}()"
        )

    @property
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self) -> str:
        vocabulary.attribute_used(self, old="model", new="dataset")
        return self.dataset

    @model.setter
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self, value: str) -> None:
        vocabulary.attribute_used(self, old="model", new="dataset")
        self.dataset = value


class BrokenDependentWarning(SlayerWarning):
    """A re-ingest re-typed ``field`` to a list, and the stored definition ``name`` that planned before it fails
    after (``checked=False``: the check could not run, for ``reason``)."""

    kind: Literal["broken_dependent"] = "broken_dependent"
    dataset: str
    data_source: str
    dependent_kind: Literal["metric", "field", "filter", "saved_query"]
    name: str
    field: str
    checked: bool = True
    error_type: Optional[str] = None
    error: Optional[str] = None
    reason: Optional[str] = None

    def human_message(self) -> str:
        where = f"{self.dependent_kind} {self.data_source}.{self.dataset}.{self.name}"
        if not self.checked:
            return _single_line(f"{where} reads {self.field}, re-typed to a list, and could not be checked: "
                                f"{self.reason}")
        return _single_line(f"{where} breaks now that {self.field} is a list: {self.error}")

    @property
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self) -> str:
        vocabulary.attribute_used(self, old="model", new="dataset")
        return self.dataset

    @model.setter
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self, value: str) -> None:
        vocabulary.attribute_used(self, old="model", new="dataset")
        self.dataset = value

    @property
    @deprecated("column is deprecated; use field", category=None)
    def column(self) -> str:
        vocabulary.attribute_used(self, old="column", new="field")
        return self.field

    @column.setter
    @deprecated("column is deprecated; use field", category=None)
    def column(self, value: str) -> None:
        vocabulary.attribute_used(self, old="column", new="field")
        self.field = value


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


class DeprecatedInputWarning(SlayerWarning):
    """The input form ``old`` was converted to ``new``; ``old`` stops being accepted in SLayer ``removal``."""

    kind: Literal["deprecated_input"] = "deprecated_input"
    old: str
    new: str
    removal: str

    def human_message(self) -> str:
        return f"{self.old} is deprecated and will be removed in SLayer {self.removal}; use {self.new} instead"


class NameCollisionWarning(SlayerWarning):
    """Stored dataset ``dataset`` holds ``names`` in one ``namespace``, differing only in case; each resolves only when
    spelled exactly (or double-quoted)."""

    kind: Literal["name_collision"] = "name_collision"
    dataset: str
    namespace: str
    names: List[str]

    def human_message(self) -> str:
        return (f"dataset {self.dataset!r} declares {' and '.join(repr(n) for n in self.names)} in its {self.namespace}, "
                f"differing only in case; spell one exactly or double-quote it, and rename one to remove the "
                f"ambiguity")

    @property
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self) -> str:
        vocabulary.attribute_used(self, old="model", new="dataset")
        return self.dataset

    @model.setter
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self, value: str) -> None:
        vocabulary.attribute_used(self, old="model", new="dataset")
        self.dataset = value


class SpineRelationshipDroppedWarning(SlayerWarning):
    """A stored relationship of ``dataset`` into the built-in spine (``relationship``, its name or target) was dropped on load."""

    kind: Literal["spine_relationship_dropped"] = "spine_relationship_dropped"
    dataset: str
    relationship: str

    def human_message(self) -> str:
        return (f"dataset {self.dataset!r}: its relationship {self.relationship!r} into the built-in time_spine was dropped; the spine "
                f"reaches each dataset through its time axis, so a declared relationship to it is never needed")

    @property
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self) -> str:
        vocabulary.attribute_used(self, old="model", new="dataset")
        return self.dataset

    @model.setter
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self, value: str) -> None:
        vocabulary.attribute_used(self, old="model", new="dataset")
        self.dataset = value

    @property
    @deprecated("join is deprecated; use relationship", category=None)
    def join(self) -> str:
        vocabulary.attribute_used(self, old="join", new="relationship")
        return self.relationship

    @join.setter
    @deprecated("join is deprecated; use relationship", category=None)
    def join(self, value: str) -> None:
        vocabulary.attribute_used(self, old="join", new="relationship")
        self.relationship = value


class UnknownStoredKeyWarning(SlayerWarning):
    """Stored dataset ``dataset`` carried keys SLayer does not read (``paths``), dropped on load."""

    kind: Literal["unknown_stored_key"] = "unknown_stored_key"
    dataset: str
    paths: List[str]

    def human_message(self) -> str:
        return (f"stored dataset {self.dataset!r} carried keys SLayer does not read, dropped on load: "
                f"{', '.join(self.paths)}; save the dataset to persist it without them")


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


class DateOnlyMidnightWarning(SlayerWarning):
    """``comparison`` reads its date-only literal as midnight, where it once read the whole day; ``whole_day`` keeps the
    day, ``variables`` names the variables that bound the date there, if any."""

    kind: Literal["date_only_midnight"] = "date_only_midnight"
    comparison: str
    whole_day: str
    variables: List[str] = Field(default_factory=list)
    message: str
    removal: str = "3.0"

    def human_message(self) -> str:
        return self.message


class ChangedReferenceWarning(SlayerWarning):
    """A dataset-first ``reference`` now reaches its anchor by ``path``, where it once read the population's direct
    relationship, which ``spelling`` still names."""

    kind: Literal["changed_reference"] = "changed_reference"
    reference: str
    path: str
    spelling: str
    message: str
    removal: str = "3.0"

    def human_message(self) -> str:
        return self.message


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
        ExtensionFieldInlinedWarning,
        NarrowedAccessWarning,
        LibraryNameShadowWarning,
        BrokenDependentWarning,
        WholeListWarning,
        DeprecatedInputWarning,
        DeprecatedSpellingWarning,
        NameCollisionWarning,
        SpineRelationshipDroppedWarning,
        UnknownStoredKeyWarning,
        DateOnlyMidnightWarning,
        ChangedReferenceWarning,
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


class SlayerExtensionFieldInlinedWarning(UserWarning):
    """Carrier ``UserWarning`` for an ``ExtensionFieldInlinedWarning`` payload — one wording on both channels."""

    def __init__(self, payload: ExtensionFieldInlinedWarning) -> None:
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


class SlayerSpineRelationshipDroppedWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``SpineRelationshipDroppedWarning`` payload — one wording on both channels."""

    def __init__(self, payload: SpineRelationshipDroppedWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerUnknownStoredKeyWarning(UserWarning):
    """Carrier ``UserWarning`` for an ``UnknownStoredKeyWarning`` payload — one wording on both channels."""

    def __init__(self, payload: UnknownStoredKeyWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class SlayerDateOnlyMidnightWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``DateOnlyMidnightWarning`` payload — one wording on both channels."""

    def __init__(self, payload: DateOnlyMidnightWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


_RENAMED_CLASSES = {
    "ExtensionColumnInlinedWarning": "ExtensionFieldInlinedWarning",
    "SlayerExtensionColumnInlinedWarning": "SlayerExtensionFieldInlinedWarning",
    "SpineJoinDroppedWarning": "SpineRelationshipDroppedWarning",
    "SlayerSpineJoinDroppedWarning": "SlayerSpineRelationshipDroppedWarning",
}

if TYPE_CHECKING:
    @deprecated("ExtensionColumnInlinedWarning is deprecated; use ExtensionFieldInlinedWarning", category=None)
    class ExtensionColumnInlinedWarning(ExtensionFieldInlinedWarning): ...

    @deprecated("SlayerExtensionColumnInlinedWarning is deprecated; use SlayerExtensionFieldInlinedWarning",
                category=None)
    class SlayerExtensionColumnInlinedWarning(SlayerExtensionFieldInlinedWarning): ...

    @deprecated("SpineJoinDroppedWarning is deprecated; use SpineRelationshipDroppedWarning", category=None)
    class SpineJoinDroppedWarning(SpineRelationshipDroppedWarning): ...

    @deprecated("SlayerSpineJoinDroppedWarning is deprecated; use SlayerSpineRelationshipDroppedWarning",
                category=None)
    class SlayerSpineJoinDroppedWarning(SlayerSpineRelationshipDroppedWarning): ...


def __getattr__(name: str) -> Any:
    return vocabulary.renamed_name(name, namespace=globals(), renames=_RENAMED_CLASSES)
