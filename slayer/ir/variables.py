"""Variable substitution — ``{var}`` placeholder handling for the pipeline.

- :func:`merge_query_variables` collapses the four variable layers into the
  effective dict (runtime > stage > outer > model_defaults).
- :func:`apply_variables_to_query` binds ``{var}`` as literals into a query's
  filters, measure formulas, computed- and time-dimension expressions and
  order expressions, and substitutes its ``date_range`` bounds.
- :func:`substitute_model_sql_surfaces` binds a model's four Mode-A
  surfaces (as literals, by the caller's dialect binder) and its saved measure formulas;
  :func:`bind_extension_measures` binds a query's extension measures.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Dict, List, Optional, Tuple

from slayer.core.date_only import date_comparisons
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.names import match_name
from slayer.core.syntax import bind_variables, has_bindings, variable_names
from slayer.core.time_points import is_date_only
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
    list_valued_variable_names,
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


def _substituted_measure(m: ModelMeasure, variables: Dict[str, Any]) -> ModelMeasure:
    if not has_bindings(m.formula):
        return m
    return m.substituted(bind_variables(m.formula, variables), variables=variables)


def _substituted_dimension(d: Any, variables: Dict[str, Any]) -> Any:
    if not isinstance(d, ComputedDimension):
        return d
    text = bind_variables(d.expression, variables)
    if text == d.expression:
        return d
    bound = d.model_copy(update={"expression": text})
    bound._template = d.template
    return bound


def _substituted_order(o: OrderItem, variables: Dict[str, Any]) -> OrderItem:
    if not o.raw_formula:
        return o
    text = bind_variables(o.raw_formula, variables)
    return o if text == o.raw_formula else o.model_copy(update={"raw_formula": text})


_BOUND_VARIABLE_RE = re.compile(r"\{([A-Za-z_]\w*)\}", re.ASCII)


def _date_range_bound(text: str, variables: Dict[str, Any]) -> str:
    """A ``date_range`` bound with each ``{name}`` replaced by its value's text (a string or a whole number);
    the result is then checked as a time point."""
    def _value(match: "re.Match[str]") -> str:
        name = match.group(1)
        if name not in variables:
            raise ValueError(
                f"Undefined variable '{name}' in date_range bound {text!r}. Available variables: {sorted(variables)}"
            )
        value = variables[name]
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError(f"Variable '{name}' in a date_range bound must be a string or an integer year.")
        return str(value)

    return _BOUND_VARIABLE_RE.sub(_value, text)


def _substituted_time_dimension(td: TimeDimension, variables: Dict[str, Any]) -> TimeDimension:
    dimension = bind_variables(td.dimension, variables)
    bounds = td.date_range and [None if b is None else _date_range_bound(b, variables) for b in td.date_range]
    if dimension == td.dimension and bounds == td.date_range:
        return td
    # Revalidated so a substituted bound is shape-checked like a literal one.
    bound = TimeDimension.model_validate({
        "dimension": dimension, "granularity": td.granularity, "date_range": bounds, "name": td.name,
        "label": td.label,
    })
    bound._template = td.template
    return bound


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
    date_variables = _date_variables(query, effective)
    changed: Dict[str, Any] = {}
    if query.filters is not None:
        changed["filters"] = [bind_variables(f, effective) for f in query.filters]
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
        return _with_date_variables(_carrying(query.model_copy(), effective), date_variables)
    # ``name`` may already be a minted stage identity the user-name validator refuses.
    data = {f: getattr(query, f) for f in query.model_fields_set | {"version"} if f != "name"}
    rebuilt = SlayerQuery.model_validate(data | changed)
    rebuilt = rebuilt.model_copy(update={"name": query.name}) if "name" in query.model_fields_set else rebuilt
    return _with_date_variables(_carrying(rebuilt, effective), date_variables)


def _with_date_variables(query: SlayerQuery, date_variables: DateVariables) -> SlayerQuery:
    query._date_variables = date_variables
    return query


def _texts(query: SlayerQuery) -> List[str]:
    return [
        *(query.filters or []), *(m.formula for m in query.measures or [] if isinstance(m, ModelMeasure)),
        *(d.expression for d in query.dimensions or [] if isinstance(d, ComputedDimension)),
        *(o.raw_formula for o in query.order or [] if o.raw_formula),
    ]


def _carrying(query: SlayerQuery, variables: Dict[str, Any]) -> SlayerQuery:
    """``query`` carrying ``variables`` when a whole-``IN`` placeholder stays unwritten in it."""
    if any(variable_names(t) for t in _texts(query)):
        query._bound_variables = variables
    return query


#: Stand-in dates, one of which marks a variable's comparisons (the first the text and the values do not hold).
_SENTINELS = ("0001-01-01", "0001-01-02", "0001-01-03")


DateVariables = Dict[Tuple[Tuple[str, ...], str, str], Tuple[str, ...]]


def _date_variables(query: SlayerQuery, variables: Dict[str, Any]) -> DateVariables:
    """``(operand column path as spelled, operand-left op, date)`` per changed comparison a date-only variable binds,
    mapped to every variable binding it; a qualifier naming the query's source model is dropped."""
    dated = {n: v.strip() for n, v in variables.items() if isinstance(v, str) and is_date_only(v)}
    roots = [query.source_model] if isinstance(query.source_model, str) else []
    out: DateVariables = {}
    for text in _texts(query):
        for name in (n for n in variable_names(text) if n in dated):
            for parts, op in _variable_slots(text, name=name, variables=variables):
                rooted = len(parts) > 1 and match_name(name=parts[0], declared=roots, quoted=False) is not None
                key = (parts[1:] if rooted else parts, op, dated[name])
                out[key] = tuple(sorted({*out.get(key, ()), name}))
    return out


def _variable_slots(text: str, *, name: str, variables: Dict[str, Any]) -> List[Tuple[Tuple[str, ...], str]]:
    """``(operand column parts, operand-left op)`` per changed comparison of ``text`` with ``'{name}'``."""
    taken = {*(str(v).strip() for v in variables.values())}
    sentinel = next((s for s in _SENTINELS if s not in text and s not in taken), None)
    if sentinel is None:
        return []
    try:
        bound = bind_variables(text, {**variables, name: sentinel})
    except ValueError:
        return []
    return [(parts, op) for parts, op, date in date_comparisons(bound) if date == sentinel and parts]


def _mode_a_surfaces(model: SlayerModel) -> list:
    return [model.sql, *(model.filters or []), *(t for c in model.columns for t in (c.sql, c.filter))]


def _measure_needs_pass(measure: ModelMeasure) -> bool:
    return has_bindings(measure.formula)


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
    out: set = {name for m in model.measures for name in variable_names(m.formula)}
    for text in _mode_a_surfaces(model):
        if text:
            bare, blocked = extract_variable_refs(text)
            out |= bare | blocked
    return out


#: Binds one Mode-A text: ``bind(text, variables, field=...)``.
ModeABind = Callable[..., str]


def substitute_model_sql_surfaces(
    *, model: SlayerModel, variables: Dict[str, Any], bind_mode_a: ModeABind,
) -> SlayerModel:
    """Copy of ``model`` with ``{var}`` bound as literals into its four Mode-A surfaces by ``bind_mode_a``
    and into its saved measure formulas; no-op when unneeded."""
    if not variables and not model_needs_substitution_pass(model):
        return model
    variables = coerce_declared_list_variables(
        variables, list_valued=list_valued_variable_names(model)
    )

    def _sub(text: str, field: str) -> str:
        return bind_mode_a(text, variables, field=field)

    update: Dict[str, Any] = {"columns": _substituted_columns(model.columns, _sub)}
    if model.sql is not None:
        update["sql"] = _sub(model.sql, f"Model {model.name!r} sql")
    if model.filters:
        update["filters"] = [_sub(f, f"Model {model.name!r} filters[{i}]") for i, f in enumerate(model.filters)]
    if model.measures:
        update["measures"] = _substituted_measures(model.measures, variables)
    return model.model_copy(update=update)


def _substituted_columns(columns: List[Column], sub: Callable[[str, str], str]) -> List[Column]:
    out = []
    for col in columns:
        updates: Dict[str, Any] = {}
        if col.sql is not None:
            updates["sql"] = sub(col.sql, f"Column {col.name!r} sql")
        if col.filter is not None:
            updates["filter"] = sub(col.filter, f"Column {col.name!r} filter")
        out.append(col.model_copy(update=updates) if updates else col)
    return out


def _substituted_measures(measures: List[ModelMeasure], variables: Dict[str, Any]) -> List[ModelMeasure]:
    return [
        m.substituted(bind_variables(m.formula, variables), variables=variables) if _measure_needs_pass(m) else m
        for m in measures
    ]


def bind_extension_measures(*, query: SlayerQuery, variables: Dict[str, Any]) -> SlayerQuery:
    """``query`` with ``{var}`` bound into its extensions' measures."""
    if not any(e.measures for e in query.extensions or []):
        return query
    extensions = [
        e.model_copy(update={"measures": _substituted_measures(e.measures, variables)}) if e.measures else e
        for e in query.extensions or []
    ]
    return query.model_copy(update={"extensions": extensions})


__all__ = [
    "apply_variables_to_query",
    "model_needs_substitution_pass",
    "model_placeholder_names",
    "bind_extension_measures",
    "substitute_model_sql_surfaces",
    "extract_placeholder_names",
    "merge_query_variables",
]
