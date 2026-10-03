"""Variable substitution — ``{var}`` placeholder handling for the pipeline.

- :func:`merge_query_variables` collapses the four variable layers into the
  effective dict (runtime > stage > outer > model_defaults).
- :func:`apply_variables_to_query` substitutes ``{var}`` (Mode-B regime) into a
  query's filters, measure formulas, computed-dimension expressions, order
  expressions and ``date_range`` bounds.
- :func:`substitute_model_sql_surfaces` substitutes a model's four Mode-A
  surfaces (SQL regime) and its saved measure formulas (Mode-B regime).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from slayer.core.models import ModelMeasure, SlayerModel
from slayer.core.query import (
    ComputedDimension,
    OrderItem,
    SlayerQuery,
    TimeDimension,
    _contains_block_delimiter,
    coerce_declared_list_variables,
    declares_variables,
    extract_placeholder_names,
    extract_variable_refs,
    has_variable_syntax,
    list_valued_variable_names,
    substitute_variables,
)


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


def _python_sub(text: str, variables: Dict[str, Any]) -> str:
    # Mode-B takes the python regime: SQL quote-doubling would concatenate adjacent literals.
    return substitute_variables(filter_str=text, variables=variables, escape="python")


def _substituted_measure(m: ModelMeasure, variables: Dict[str, Any]) -> ModelMeasure:
    text = _python_sub(m.formula, variables)
    return m if text == m.formula else m.substituted(text)


def _substituted_dimension(d: Any, variables: Dict[str, Any]) -> Any:
    if not isinstance(d, ComputedDimension):
        return d
    text = _python_sub(d.expression, variables)
    return d if text == d.expression else d.substituted(text)


def _substituted_order(o: OrderItem, variables: Dict[str, Any]) -> OrderItem:
    if not o.raw_formula:
        return o
    text = _python_sub(o.raw_formula, variables)
    return o if text == o.raw_formula else o.model_copy(update={"raw_formula": text})


def _substituted_time_dimension(td: TimeDimension, variables: Dict[str, Any]) -> TimeDimension:
    if not td.date_range:
        return td
    bounds = [None if b is None else _python_sub(b, variables) for b in td.date_range]
    if bounds == td.date_range:
        return td
    # Revalidated so a substituted bound is shape-checked like a literal one.
    return TimeDimension.model_validate({
        "dimension": td.dimension, "granularity": td.granularity, "date_range": bounds, "label": td.label,
    })


def apply_variables_to_query(
    *,
    query: SlayerQuery,
    variables: Optional[Dict[str, Any]] = None,
) -> SlayerQuery:
    """A fresh copy of ``query`` with ``{var}`` substituted on every Mode-B surface.

    Changed measures and computed dimensions keep their template in ``_template``;
    the query is rebuilt so construction validators re-run on the substituted text.
    """
    effective: Dict[str, Any] = dict(variables or {})
    changed: Dict[str, Any] = {}
    if query.filters is not None:
        changed["filters"] = [_python_sub(f, effective) for f in query.filters]
    surfaces = (
        ("measures", lambda m: _substituted_measure(m, effective)),
        ("dimensions", lambda d: _substituted_dimension(d, effective)),
        ("order", lambda o: _substituted_order(o, effective)),
        ("time_dimensions", lambda td: _substituted_time_dimension(td, effective)),
    )
    for field, substitute in surfaces:
        entries = getattr(query, field)
        if entries:
            new = [substitute(e) for e in entries]
            if any(n is not o for n, o in zip(new, entries)):
                changed[field] = new
    if not changed:
        return query.model_copy()
    # ``name`` may already be a minted stage identity the user-name validator refuses.
    data = {f: getattr(query, f) for f in query.model_fields_set | {"version"} if f != "name"}
    rebuilt = SlayerQuery.model_validate(data | changed)
    return rebuilt.model_copy(update={"name": query.name}) if "name" in query.model_fields_set else rebuilt


def _mode_a_surfaces(model: SlayerModel) -> list:
    return [model.sql, *(model.filters or []), *(t for c in model.columns for t in (c.sql, c.filter))]


def _measure_needs_pass(measure: ModelMeasure) -> bool:
    return has_variable_syntax(measure.formula)


def model_needs_substitution_pass(model: SlayerModel) -> bool:
    """True if substitution must run with no variables (a Mode-A ``{? ?}`` block, declared
    variables, or any ``{...}`` token in a saved measure formula)."""
    return (
        any(s and _contains_block_delimiter(s) for s in _mode_a_surfaces(model))
        or declares_variables(model)
        or any(_measure_needs_pass(m) for m in model.measures)
    )


def model_placeholder_names(model: SlayerModel) -> set:
    """Every ``{var}`` a model's Mode-A surfaces and saved measure formulas read."""
    out: set = set()
    for text in [*_mode_a_surfaces(model), *(m.formula for m in model.measures)]:
        if text:
            bare, blocked = extract_variable_refs(text)
            out |= bare | blocked
    return out


def substitute_model_sql_surfaces(
    *, model: SlayerModel, variables: Dict[str, Any], backslash_escapes: bool,
) -> SlayerModel:
    """Copy of ``model`` with ``{var}`` substituted into its four Mode-A surfaces (SQL regime)
    and its saved measure formulas (python regime); no-op when unneeded."""
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
    if model.measures:
        update["measures"] = [
            m.model_copy(update={"formula": _python_sub(m.formula, variables)}) if _measure_needs_pass(m) else m
            for m in model.measures
        ]
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
