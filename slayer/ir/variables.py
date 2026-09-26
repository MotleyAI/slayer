"""Variable substitution — ``{var}`` placeholder handling for the pipeline.

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

- :func:`substitute_model_sql_surfaces` substitutes a model's four Mode-A
  surfaces (``sql``, ``filters``, ``Column.sql`` / ``Column.filter``).

This is the active substitution path used by ``engine.execute`` and
``engine.save_model``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from slayer.core.models import SlayerModel
from slayer.core.query import (
    SlayerQuery,
    _contains_block_delimiter,
    coerce_declared_list_variables,
    declares_variables,
    extract_placeholder_names,
    extract_variable_refs,
    list_valued_variable_names,
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

    # Mode-B (Python-AST) query filters take the ``python`` escaping regime —
    # SQL quote-doubling would silently corrupt a value via adjacent-literal
    # concatenation once the AST layer re-renders it (DEV-1625).
    substituted = [
        substitute_variables(filter_str=f, variables=effective, escape="python")
        for f in query.filters
    ]
    return query.model_copy(update={"filters": substituted})


def _mode_a_surfaces(model: SlayerModel) -> list:
    return [model.sql, *(model.filters or []), *(t for c in model.columns for t in (c.sql, c.filter))]


def model_needs_substitution_pass(model: SlayerModel) -> bool:
    """True if substitution must run with no variables (a ``{? ?}`` block or declared variables)."""
    return any(
        s and _contains_block_delimiter(s) for s in _mode_a_surfaces(model)
    ) or declares_variables(model)


def model_placeholder_names(model: SlayerModel) -> set:
    """Every ``{var}`` a model's Mode-A surfaces read."""
    out: set = set()
    for text in _mode_a_surfaces(model):
        if text:
            bare, blocked = extract_variable_refs(text)
            out |= bare | blocked
    return out


def substitute_model_sql_surfaces(
    *, model: SlayerModel, variables: Dict[str, Any], backslash_escapes: bool,
) -> SlayerModel:
    """Copy of ``model`` with ``{var}`` substituted into its four Mode-A surfaces
    (``sql``, ``filters``, ``Column.sql`` / ``Column.filter``); no-op when unneeded."""
    if not variables and not model_needs_substitution_pass(model):
        return model
    variables = coerce_declared_list_variables(
        variables, list_valued=list_valued_variable_names(model)
    )

    def _sub(text: str) -> str:
        return substitute_variables(
            filter_str=text, variables=variables, escape="sql",
            backslash_escapes=backslash_escapes,
        )

    columns = []
    for col in model.columns:
        updates: Dict[str, Any] = {}
        if col.sql is not None:
            updates["sql"] = _sub(col.sql)
        if col.filter is not None:
            updates["filter"] = _sub(col.filter)
        columns.append(col.model_copy(update=updates) if updates else col)
    update: Dict[str, Any] = {"columns": columns}
    if model.sql is not None:
        update["sql"] = _sub(model.sql)
    if model.filters:
        update["filters"] = [_sub(f) for f in model.filters]
    return model.model_copy(update=update)


__all__ = [
    "apply_variables_to_query",
    "model_needs_substitution_pass",
    "model_placeholder_names",
    "substitute_model_sql_surfaces",
    "extract_placeholder_names",
    "merge_query_variables",
    "substitute_variables",
]
