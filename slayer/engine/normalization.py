"""Slack normalization layer.

Rewrites tolerant-but-unambiguous agent input to canonical form before the
typed pipeline sees it, returning every rewrite as a typed
``NormalizationWarning`` (P0). Downstream stages never see the slack form.

Active rules:

- ``MISPLACED_METRIC`` (query shape): bare field-looking entries in
  ``SlayerQuery.metrics`` that resolve as a field (not a named
  ``DatasetMetric``) move to ``SlayerQuery.dimensions``. Mirrors the
  existing ``_auto_move_fields_to_dimensions`` heuristic but emits a
  structured warning.

- ``STALE_PATH_SPELLING`` (stage boundary): a flat name written against a
  non-canonical path spelling binds to its canonical upstream field; recorded
  at binding, surfaced via :func:`stale_spelling_warnings`.

Retired rules: ``FUNC_STYLE_AGG`` (the parser accepts the functional
aggregation spelling natively),
``DOT_PATH_IN_SQL`` (Mode-A free SQL is dotted-canonical) and
``MALFORMED_DATE_RANGE`` (a malformed ``date_range`` is a construction error).

Each rule emits a ``SlayerNormalizationWarning`` via ``warnings.warn(...)``
AND appends a ``NormalizationWarning`` payload to the returned result,
so REST / MCP / CLI consumers see the rewrite alongside the response and
``warnings.catch_warnings()`` callers see it via the standard channel.
"""

from __future__ import annotations

import warnings as _warnings_module
from typing import List, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.models import SlayerDataset
from slayer.core.query import QueryDimension, SlayerQuery
from slayer.core.scope import StaleSpelling
from slayer.core.syntax import Ref, parse_expr, root_relative
from slayer.core.warnings import NormalizationWarning, SlayerNormalizationWarning


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


class NormalizationResult(BaseModel):
    """Output of a normalization pass.

    ``query`` and ``dataset`` are either the same object the caller passed
    in (if no rewrite fired) or a new instance with the slack form
    rewritten. ``warnings`` lists one ``NormalizationWarning`` per
    rewrite — empty when the input was already canonical.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    query: Optional[SlayerQuery] = None
    dataset: Optional[SlayerDataset] = None
    warnings: List[NormalizationWarning] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Rule: MISPLACED_METRIC
# ---------------------------------------------------------------------------


def _bare_name(expression: str, *, root: str) -> Optional[str]:
    """The field-or-metric name ``expression`` is when it is a lone reference, else ``None``."""
    try:
        node = root_relative(parse_expr(expression), root)
    except ValueError:
        return None
    return node.name if isinstance(node, Ref) else None


def _apply_misplaced_metric(
    query: SlayerQuery,
    *,
    dataset: Optional[SlayerDataset],
) -> tuple[SlayerQuery, List[NormalizationWarning]]:
    """Move bare (non-aggregate, non-call) entries from ``query.metrics`` to
    ``query.dimensions`` when they name a field on the dataset that isn't
    a ``DatasetMetric`` expression.

    Mirrors the existing ``_auto_move_fields_to_dimensions`` heuristic but
    emits a structured warning. When ``dataset`` is None we can't classify,
    so the rule is a no-op.
    """
    if not query.metrics or dataset is None:
        return query, []

    metric_expression_names = {m.name for m in dataset.metrics}
    field_names = {c.name for c in dataset.fields}

    new_metrics = list(query.metrics)
    moved_dims: List[QueryDimension] = []
    emitted: List[NormalizationWarning] = []

    kept: List = []
    for i, m in enumerate(new_metrics):
        bare = _bare_name(m.metric, root=dataset.spelling)
        # A bare name of a saved metric stays a metric; of a field, it moves to dimensions.
        if bare is None or bare in metric_expression_names:
            kept.append(m)
            continue
        if bare in field_names:
            moved_dims.append(QueryDimension(field=m.metric))
            emitted.append(NormalizationWarning(
                rule_id="MISPLACED_METRIC",
                original=bare,
                normalized=f"dimensions += {m.metric!r}",
                location=f"metrics[{i}].metric",
                rule_doc_url="docs/agent_input_slack.md#misplaced-metric",
            ))
            _warnings_module.warn(
                SlayerNormalizationWarning(emitted[-1]), stacklevel=2,
            )
            continue
        # Unknown bare token — leave for downstream resolver to error on.
        kept.append(m)

    if not emitted:
        return query, []

    # model_copy skips validation, so the entries are built here.
    new_dimensions = list(query.dimensions or []) + moved_dims
    return (
        query.model_copy(update={"metrics": kept, "dimensions": new_dimensions}),
        emitted,
    )


# ---------------------------------------------------------------------------
# Top-level entry points
# ---------------------------------------------------------------------------


def normalize_query(
    query: SlayerQuery,
    *,
    dataset: Optional[SlayerDataset] = None,
) -> NormalizationResult:
    """Apply all enabled slack rules to a ``SlayerQuery``.

    Returns the (possibly rewritten) query and the structured warnings.
    Expression TEXT is never rewritten — both aggregation spellings are
    first-class parser input.
    """
    all_warnings: List[NormalizationWarning] = []

    # Rule 1: MISPLACED_METRIC.
    query, ws = _apply_misplaced_metric(query, dataset=dataset)
    all_warnings.extend(ws)

    return NormalizationResult(query=query, warnings=all_warnings)




STALE_PATH_SPELLING = "STALE_PATH_SPELLING"


def stale_spelling_warnings(records: Sequence[StaleSpelling]) -> List[NormalizationWarning]:
    """One ``STALE_PATH_SPELLING`` warning per recorded stale spelling."""
    emitted = [
        NormalizationWarning(
            rule_id=STALE_PATH_SPELLING, original=r.original,
            normalized=r.normalized, location=r.location,
        )
        for r in records
    ]
    for payload in emitted:
        _warnings_module.warn(SlayerNormalizationWarning(payload), stacklevel=2)
    return emitted
