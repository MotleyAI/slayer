"""Stage 6 (DEV-1450) — slack normalization layer.

Rewrites tolerant-but-unambiguous agent input to canonical form before the
typed pipeline sees it, returning every rewrite as a typed
``NormalizationWarning`` (P0). Downstream stages never see the slack form.

Active rules:

- ``MISPLACED_MEASURE`` (query shape): bare column-looking entries in
  ``SlayerQuery.measures`` that resolve as a column (not a named
  ``ModelMeasure``) move to ``SlayerQuery.dimensions``. Mirrors the
  existing ``_auto_move_fields_to_dimensions`` heuristic but emits a
  structured warning.

- ``MALFORMED_DATE_RANGE`` (report-only): a ``date_range`` the planner
  would silently ignore gets a structured warning naming the drop.

Retired rules: ``FUNC_STYLE_AGG`` (DEV-1826 — the parser accepts the
functional aggregation spelling natively as a first-class equivalent of
colon syntax, so there is nothing to rewrite or warn about) and
``DOT_PATH_IN_SQL`` (DEV-1743 — Mode-A free SQL is dotted-canonical).

Each rule emits a ``SlayerNormalizationWarning`` via ``warnings.warn(...)``
AND appends a ``NormalizationWarning`` payload to the returned result,
so REST / MCP / CLI consumers see the rewrite alongside the response and
``warnings.catch_warnings()`` callers see it via the standard channel.
"""

from __future__ import annotations

import warnings as _warnings_module
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.warnings import NormalizationWarning, SlayerNormalizationWarning


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


class NormalizationResult(BaseModel):
    """Output of a normalization pass.

    ``query`` and ``model`` are either the same object the caller passed
    in (if no rewrite fired) or a new instance with the slack form
    rewritten. ``warnings`` lists one ``NormalizationWarning`` per
    rewrite — empty when the input was already canonical.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    query: Optional[SlayerQuery] = None
    model: Optional[SlayerModel] = None
    warnings: List[NormalizationWarning] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Rule: MISPLACED_MEASURE
# ---------------------------------------------------------------------------


def _apply_misplaced_measure(
    query: SlayerQuery,
    *,
    model: Optional[SlayerModel],
) -> tuple[SlayerQuery, List[NormalizationWarning]]:
    """Move bare (no-colon, no-function) entries from ``query.measures`` to
    ``query.dimensions`` when they name a column on the model that isn't
    a ``ModelMeasure`` formula.

    Mirrors the existing ``_auto_move_fields_to_dimensions`` heuristic but
    emits a structured warning. When ``model`` is None we can't classify,
    so the rule is a no-op.
    """
    if not query.measures or model is None:
        return query, []

    measure_formula_names = {m.name for m in model.measures}
    column_names = {c.name for c in model.columns}

    new_measures = list(query.measures)
    moved_dim_strings: List[str] = []
    emitted: List[NormalizationWarning] = []

    kept: List = []
    for i, m in enumerate(new_measures):
        formula = getattr(m, "formula", None)
        if not isinstance(formula, str):
            kept.append(m)
            continue
        if ":" in formula or "(" in formula:
            kept.append(m)
            continue
        # Bare token. If it names a known ModelMeasure formula, keep it as
        # a measure. If it names a column on the model, move to dimensions.
        bare = formula.strip()
        if bare in measure_formula_names:
            kept.append(m)
            continue
        if bare in column_names:
            moved_dim_strings.append(bare)
            emitted.append(NormalizationWarning(
                rule_id="MISPLACED_MEASURE",
                original=bare,
                normalized=f"dimensions += {bare!r}",
                location=f"measures[{i}].formula",
                rule_doc_url="docs/agent_input_slack.md#misplaced-measure",
            ))
            _warnings_module.warn(
                SlayerNormalizationWarning(emitted[-1]), stacklevel=2,
            )
            continue
        # Unknown bare token — leave for downstream resolver to error on.
        kept.append(m)

    if not emitted:
        return query, []

    existing_dims = list(query.dimensions or [])
    # Append each moved bare column name as a dimension entry. We add as
    # plain strings since SlayerQuery.dimensions accepts string entries
    # alongside ColumnRefs (the pydantic union validators handle the
    # coercion).
    new_dimensions = existing_dims + moved_dim_strings
    return (
        query.model_copy(update={"measures": kept, "dimensions": new_dimensions}),
        emitted,
    )


# ---------------------------------------------------------------------------
# Top-level entry points
# ---------------------------------------------------------------------------


def normalize_query(
    query: SlayerQuery,
    *,
    model: Optional[SlayerModel] = None,
) -> NormalizationResult:
    """Apply all enabled slack rules to a ``SlayerQuery``.

    Returns the (possibly rewritten) query and the structured warnings.
    Formula TEXT is never rewritten — both aggregation spellings are
    first-class parser input (DEV-1826).
    """
    all_warnings: List[NormalizationWarning] = []

    # Rule 1: MISPLACED_MEASURE.
    query, ws = _apply_misplaced_measure(query, model=model)
    all_warnings.extend(ws)

    # Rule 2: MALFORMED_DATE_RANGE.
    all_warnings.extend(_apply_malformed_date_range(query))

    return NormalizationResult(query=query, warnings=all_warnings)


def _apply_malformed_date_range(
    query: SlayerQuery,
) -> List[NormalizationWarning]:
    """Warn when a ``time_dimensions[i].date_range`` is present but is not the
    two-element ``[start, end]`` the planner requires.

    The planner's silent ``continue`` on such a range is deliberate and stays
    exactly as it is — this rule changes NO behaviour, it only stops the drop
    from being invisible. The trigger is the planner's own drop condition
    (``date_range is not None and len(date_range) != 2``), so the warning fires
    if and only if the range is actually ignored: ``[]``, one element, or three
    or more. An absent ``date_range`` is legitimately optional and never warns.

    Reports, but does not rewrite: there is no unambiguous canonical form to
    rewrite a malformed range TO, and inventing one would change results.
    """
    emitted: List[NormalizationWarning] = []
    for i, td in enumerate(query.time_dimensions or []):
        date_range = getattr(td, "date_range", None)
        if date_range is None or len(date_range) == 2:
            continue
        payload = NormalizationWarning(
            rule_id="MALFORMED_DATE_RANGE",
            original=f"time_dimensions[{i}].date_range={list(date_range)!r}",
            normalized="(ignored — no date filter emitted)",
            location=f"time_dimensions[{i}].date_range",
            # Reports but does NOT rewrite (planner silently no-ops the range);
            # the message must not claim a transform (DEV-1783).
            rewritten=False,
            # No rule_doc_url: docs/agent_input_slack.md does not exist, and a
            # link to a missing page is worse than no link.
        )
        emitted.append(payload)
        _warnings_module.warn(
            SlayerNormalizationWarning(payload), stacklevel=2,
        )
    return emitted


