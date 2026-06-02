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

from typing import Optional

from pydantic import BaseModel


class NormalizationWarning(BaseModel):
    """Structured payload describing one slack-normalization rewrite.

    ``rule_id`` identifies the rule that fired (``FUNC_STYLE_AGG``,
    ``DOT_PATH_IN_SQL``, ``MISPLACED_MEASURE``). ``location`` is a
    human-readable pointer into the query input (e.g.
    ``measures[2].formula``). ``rule_doc_url`` is an optional anchor
    into ``docs/agent_input_slack.md``.
    """

    rule_id: str
    original: str
    normalized: str
    location: str
    rule_doc_url: Optional[str] = None


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
