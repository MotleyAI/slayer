"""Stage 5 (DEV-1450) — slack-normalization warning types.

The slack-normalization layer (stage 6) rewrites tolerant-but-unambiguous
agent input to canonical form before the typed pipeline sees it, and
emits one ``NormalizationWarning`` payload per rewrite. The payload is
surfaced two ways:

- Emitted as a Python warning via ``warnings.warn(SlayerNormalizationWarning(payload), ...)``
  so callers using ``warnings.catch_warnings()`` see the rewrite.
- Appended to ``SlayerResponse.warnings: List[NormalizationWarning]`` so
  REST/MCP/CLI consumers get the structured payload alongside the result.

Living in ``slayer.core.warnings`` (not ``slayer.engine.normalization``)
lets memory/storage/REST schemas reference the Pydantic payload without
pulling in engine code.
"""

from __future__ import annotations

from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field


class SlayerWarning(BaseModel):
    """Base of the warning family carried on ``SlayerResponse.warnings``.

    ``SlayerResponse.warnings`` used to be normalization-only, so consumers
    could assume every element had a ``rule_id``. It now carries more than one
    kind of advisory, so every payload declares a ``kind`` discriminator and a
    consumer switches on it rather than on the presence of a field.
    """

    kind: str

    def human_message(self) -> str:
        """One operator-readable line describing this advisory.

        Lives on the payload so MCP, the CLI, and any future surface render a
        given kind identically, and so a NEW kind cannot silently fall back to
        a Pydantic repr on one surface and a hand-written string on another.
        Subclasses override; the base is the honest last resort.
        """
        return f"{self.kind}: {self.model_dump(exclude={'kind'})}"


class NormalizationWarning(SlayerWarning):
    """Structured payload describing one slack-normalization rewrite.

    ``rule_id`` identifies the rule that fired (``FUNC_STYLE_AGG``,
    ``DOT_PATH_IN_SQL``, ``MISPLACED_MEASURE``). ``location`` is a
    human-readable pointer into the query input (e.g.
    ``measures[2].formula``). ``rule_doc_url`` is an optional anchor
    into ``docs/agent_input_slack.md``.
    """

    kind: Literal["normalization"] = "normalization"
    rule_id: str
    original: str
    normalized: str
    location: str
    rule_doc_url: Optional[str] = None

    def human_message(self) -> str:
        return (
            f"[{self.rule_id}] rewrote {self.original} → {self.normalized} "
            f"(at {self.location})"
        )


class DroppedFilterWarning(SlayerWarning):
    """A user filter that could not be applied where it was routed.

    Carries the filter's ORIGINAL author text (not the normalized, prequoted
    or re-rendered form — the author has to recognise it), the surface it came
    from, and why it was dropped.
    """

    kind: Literal["unreachable_filter_dropped"] = "unreachable_filter_dropped"
    filter_text: str
    location: str
    reason: str

    def human_message(self) -> str:
        return (
            f"dropped filter {self.filter_text!r} (at {self.location}): "
            f"{self.reason}"
        )


# The response carries a DISCRIMINATED union, not the bare base class: Pydantic
# validates a ``List[SlayerWarning]`` down to the base type and would silently
# drop every subclass field on the way through. Keyed on ``kind``, each payload
# round-trips as itself.
AnySlayerWarning = Annotated[
    Union[NormalizationWarning, DroppedFilterWarning],
    Field(discriminator="kind"),
]


class SlayerNormalizationWarning(UserWarning):
    """Carrier ``UserWarning`` for a ``NormalizationWarning`` payload.

    Lets callers route both via ``warnings.catch_warnings(...)`` and
    via the structured ``SlayerResponse.warnings`` list — same data,
    two surfaces, one source of truth.
    """

    def __init__(self, payload: NormalizationWarning) -> None:
        self.payload = payload
        super().__init__(
            f"[{payload.rule_id}] {payload.original!s} → {payload.normalized!s} "
            f"(at {payload.location})"
        )
