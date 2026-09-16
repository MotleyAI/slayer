"""Warning payload types on ``SlayerResponse.warnings`` — surfaced as Python warnings and as
structured payloads. Live in ``slayer.core`` so schemas reference them without engine code."""

from __future__ import annotations

from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field


class SlayerWarning(BaseModel):
    """Base of the warning family on ``SlayerResponse.warnings``; every payload declares a ``kind`` discriminator so consumers switch on it, not on a field's presence."""

    kind: str

    def human_message(self) -> str:
        """One operator-readable line; subclasses override, the base is the honest last resort."""
        return f"{self.kind}: {self.model_dump(exclude={'kind'})}"


class NormalizationWarning(SlayerWarning):
    """One slack-normalization event — a rewrite or a report-only advisory (``rewritten=False``, e.g. ``MALFORMED_DATE_RANGE``). ``rule_id`` names the rule; ``location`` points into the input."""

    kind: Literal["normalization"] = "normalization"
    rule_id: str
    original: str
    normalized: str
    location: str
    rule_doc_url: Optional[str] = None
    # Report-only rules (MALFORMED_DATE_RANGE): the message must not claim a
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


class DroppedFilterWarning(SlayerWarning):
    """A user filter that couldn't be applied where routed; carries its original author text, surface, and reason."""

    kind: Literal["unreachable_filter_dropped"] = "unreachable_filter_dropped"
    filter_text: str
    location: str
    reason: str

    def human_message(self) -> str:
        return (
            f"dropped filter {self.filter_text!r} (at {self.location}): "
            f"{self.reason}"
        )


class BroadcastDimension(BaseModel):
    """One query dimension a metric could not attribute, with the broadcast reason."""

    dimension: str
    reason: str


#: Unconditional dice–slice disclosure on every broadcast payload.
DICE_SLICE_HINT = (
    "filtering on this dimension restricts the metric by association while "
    "slicing only repeats the value; set to_many_handling='associate' to "
    "reconcile the two."
)


class BroadcastGrainWarningPayload(SlayerWarning):
    """A cross-model aggregate whose implicit grain lost query dimensions to broadcasting; ``measure`` names the metric, ``dimensions`` lists each broadcast dimension and its reason."""

    kind: Literal["broadcast"] = "broadcast"
    measure: str
    location: str
    dimensions: list[BroadcastDimension]
    hint: str = DICE_SLICE_HINT

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


class DegenerateReaggregationWarningPayload(SlayerWarning):
    """A second-order aggregation whose operand grain equals the outer grain, so it is the identity (DEV-1847); ``operand_grain`` / ``outer_grain`` name the equal grains and ``hint`` the partition_by remedy."""

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
    """A ROW filter pushed into a producer as a semi-join (EXISTS): correctly applied, informational only — carried on the response, never a Python warning and never an error."""

    kind: Literal["semi_join_pushed"] = "semi_join_pushed"
    measure: str
    location: str
    filter_text: str

    def human_message(self) -> str:
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


# Discriminated union, not the bare base: a ``List[SlayerWarning]`` would validate
# down to the base type and drop subclass fields. Keyed on ``kind``, each round-trips.
AnySlayerWarning = Annotated[
    Union[
        NormalizationWarning,
        DroppedFilterWarning,
        BroadcastGrainWarningPayload,
        AssociatedWarningPayload,
        DegenerateReaggregationWarningPayload,
        SemiJoinPushedWarningPayload,
        ResponseTruncationWarning,
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
