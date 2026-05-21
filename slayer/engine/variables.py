"""Stage 7b.1 (DEV-1450) — variable substitution in the new pipeline.

Moves the ``{var}`` placeholder substitution that previously lived inside
the legacy enrichment path (``slayer.engine.enrichment.py:1162``) into a
small, pipeline-friendly module.

Public surface:

- :func:`merge_query_variables` collapses the four configured variable
  layers (model defaults < outer query < stage query < runtime kwarg)
  into the effective dict that populates
  ``ResolvedSourceBundle.query_variables``. Precedence: runtime > stage >
  outer > model_defaults.
- :func:`apply_variables_to_query` returns a copy of the input
  ``SlayerQuery`` with ``{var}`` substituted in its ``filters`` list. The
  helper always returns a fresh ``SlayerQuery`` instance for predictable
  pipeline semantics. ``dry_run_placeholders=True`` fills any unresolved
  valid placeholder with the legacy ``"0"`` sentinel instead of raising
  — used by save-time dry-run SQL generation. Invalid placeholder names
  still raise regardless of ``dry_run_placeholders``.

Scope deliberately matches the legacy enrichment scope —
``SlayerQuery.filters`` is the only field this helper substitutes into.
Formula text, ``Column.sql``, ``Column.filter``, and
``SlayerModel.filters`` are NOT variable-substituted today, and this
module preserves that contract.

The dormant module is unwired from the engine in this commit; stage
7b.15 (engine cutover) makes it the substitution path used by
``engine.execute`` and ``engine.save_model``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from slayer.core.query import (
    SlayerQuery,
    extract_placeholder_names,
    substitute_variables,
)

_PLACEHOLDER_FILL_VALUE = "0"


def merge_query_variables(
    *,
    runtime: Optional[Dict[str, Any]],
    stage: Optional[Dict[str, Any]],
    outer: Optional[Dict[str, Any]],
    model_defaults: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Collapse the four variable layers into the effective dict.

    Precedence (highest wins): runtime > stage > outer > model_defaults.
    ``None`` and empty-dict layers are identities.
    """
    return {
        **(model_defaults or {}),
        **(outer or {}),
        **(stage or {}),
        **(runtime or {}),
    }


def apply_variables_to_query(
    *,
    query: SlayerQuery,
    variables: Optional[Dict[str, Any]] = None,
    dry_run_placeholders: bool = False,
) -> SlayerQuery:
    """Return a copy of ``query`` with ``{var}`` substituted in ``filters``.

    The returned ``SlayerQuery`` is always a fresh instance, including in
    the no-op cases (``query.filters`` is ``None`` / empty / contains no
    placeholders). ``variables=None`` is normalized to an empty dict.
    When ``dry_run_placeholders=True``, unresolved valid placeholders are
    filled with ``"0"`` instead of raising — the legacy save-time
    dry-run behaviour. Invalid placeholder names still raise
    ``ValueError`` regardless of ``dry_run_placeholders``, because the
    dry-run shortcut is for missing *values*, not for bypassing name
    validation.
    """
    if query.filters is None:
        return query.model_copy()

    effective: Dict[str, Any] = dict(variables or {})
    if dry_run_placeholders:
        for placeholder in extract_placeholder_names(query):
            effective.setdefault(placeholder, _PLACEHOLDER_FILL_VALUE)

    substituted = [
        substitute_variables(filter_str=f, variables=effective)
        for f in query.filters
    ]
    return query.model_copy(update={"filters": substituted})


__all__ = [
    "apply_variables_to_query",
    "extract_placeholder_names",
    "merge_query_variables",
    "substitute_variables",
]
