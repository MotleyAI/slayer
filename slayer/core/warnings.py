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


class BroadcastGrainWarningPayload(SlayerWarning):
    """A cross-model aggregate whose implicit grain lost query dimensions to broadcasting; ``measure`` names the metric, ``dimensions`` lists each broadcast dimension and its reason."""

    kind: Literal["broadcast"] = "broadcast"
    measure: str
    location: str
    dimensions: list[BroadcastDimension]

    def human_message(self) -> str:
        dims = ", ".join(f"{d.dimension} ({d.reason})" for d in self.dimensions)
        return (
            f"metric {self.measure!r} (at {self.location}) broadcast across "
            f"unattributable dimension(s): {dims}"
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
        ResponseTruncationWarning,
    ],
    Field(discriminator="kind"),
]


class SlayerNormalizationWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``NormalizationWarning`` payload — one wording on both channels."""

    def __init__(self, payload: NormalizationWarning) -> None:
        self.payload = payload
        # One source of truth for the wording across both channels.
        super().__init__(payload.human_message())
