"""Variable substitution — ``{var}`` placeholder handling for the pipeline.

- :func:`merge_query_variables` collapses the four variable layers into the
  effective dict (runtime > stage > outer > model_defaults).
- :func:`apply_variables_to_query` binds ``{var}`` as literals into a query's
  filters, metric expressions, computed- and time-dimension expressions and
  order expressions, and substitutes its ``date_range`` bounds.
- :func:`substitute_dataset_sql_surfaces` binds a dataset's four Mode-A
  surfaces (as literals, by the caller's dialect binder) and its saved metric expressions;
  :func:`bind_extension_metrics` binds a query's extension metrics.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Dict, List, Optional, Tuple

from slayer.core.date_only import date_comparisons
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.names import match_name
from slayer.core.syntax import bind_variables, has_bindings, variable_names
from slayer.core.time_points import is_date_only
from slayer.core.query import (
    OrderItem,
    QueryDimension,
    QueryMetric,
    SlayerQuery,
    TimeDimension,
    _contains_block_delimiter,
    coerce_declared_list_variables,
    declares_variables,
    extract_placeholder_names,
    extract_variable_refs,
    list_valued_variable_names,
    map_filter,
)


def merge_query_variables(
    *,
    runtime: Optional[Dict[str, Any]],
    stage: Optional[Dict[str, Any]],
    outer: Optional[Dict[str, Any]],
    dataset_defaults: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Collapse the four variable layers into the effective dict.

    Precedence (highest wins): runtime > stage > outer > model_defaults.
    ``None`` and empty-dict layers are identities.
    """
    return {
        **(dataset_defaults or {}),
        **(outer or {}),
        **(stage or {}),
        **(runtime or {}),
    }


def _substituted_metric(m: QueryMetric, variables: Dict[str, Any]) -> QueryMetric:
    if not has_bindings(m.metric):
        return m
    return m.substituted(bind_variables(m.metric, variables), variables=variables)


def _substituted_dimension(d: QueryDimension, variables: Dict[str, Any]) -> QueryDimension:
    text = bind_variables(d.field, variables)
    if text == d.field:
        return d
    bound = d.model_copy(update={"field": text})
    bound._template = d.template
    return bound


def _substituted_order(o: OrderItem, variables: Dict[str, Any]) -> OrderItem:
    if not o.raw_expression:
        return o
    text = bind_variables(o.raw_expression, variables)
    return o if text == o.raw_expression else o.model_copy(update={"raw_expression": text})


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
    dimension = bind_variables(td.field, variables)
    bounds = td.date_range and [None if b is None else _date_range_bound(b, variables) for b in td.date_range]
    if dimension == td.field and bounds == td.date_range:
        return td
    # Revalidated so a substituted bound is shape-checked like a literal one.
    bound = TimeDimension.model_validate({
        "field": dimension, "granularity": td.granularity, "date_range": bounds, "alias": td.alias,
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

    Changed metrics and computed dimensions keep their template in ``_template``;
    the query is rebuilt so construction validators re-run on the substituted text.
    """
    effective: Dict[str, Any] = dict(variables or {})
    date_variables = _date_variables(query, effective)
    changed: Dict[str, Any] = {}
    if query.filter is not None:
        changed["filter"] = map_filter(query.filter, lambda f: bind_variables(f, effective))
    surfaces = (
        ("metrics", lambda m: _substituted_metric(m, effective)),
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
        *query.filter_conditions, *(m.metric for m in query.metrics or []),
        *(d.field for d in query.dimensions or []),
        *(o.raw_expression for o in query.order or [] if o.raw_expression),
    ]


def _carrying(query: SlayerQuery, variables: Dict[str, Any]) -> SlayerQuery:
    """``query`` carrying ``variables`` when a whole-``IN`` placeholder stays unwritten in it."""
    if any(variable_names(t) for t in _texts(query)):
        query._bound_variables = variables
    return query


#: Stand-in dates, one of which marks a variable's comparisons (the first the text and the values do not hold).
_SENTINELS = ("0001-01-01", "0001-01-02", "0001-01-03")


#: ``(operand field path as spelled, operand-left op, date, literal on the left)`` → the variables binding it there.
DateVariables = Dict[Tuple[Tuple[str, ...], str, str, bool], Tuple[str, ...]]


def _date_variables(query: SlayerQuery, variables: Dict[str, Any]) -> DateVariables:
    """Each changed comparison a date-only variable binds, mapped to every variable binding it; a qualifier naming the
    query's source dataset is dropped."""
    dated = {n: v.strip() for n, v in variables.items() if isinstance(v, str) and is_date_only(v)}
    roots = [query.population] if isinstance(query.population, str) else []
    texts = [*((t, None) for t in _texts(query)),
             *((m.expression, e.dataset) for e in query.extensions or [] for m in e.metrics or [])]
    out: DateVariables = {}
    for text, owner in texts:
        for name in (n for n in variable_names(text) if n in dated):
            for parts, op, literal_left in _variable_slots(text, name=name, variables=variables):
                key = (_from_root(parts, owner=owner, roots=roots), op, dated[name], literal_left)
                out[key] = tuple(sorted({*out.get(key, ()), name}))
    return out


def _from_root(parts: Tuple[str, ...], *, owner: Optional[str], roots: List[str]) -> Tuple[str, ...]:
    """``parts`` as a path from the query's source dataset: an extension's own qualifier replaced by its owner, a
    qualifier naming the source dataset dropped."""
    if owner is not None:
        own = len(parts) > 1 and match_name(name=parts[0], declared=[owner], quoted=False) is not None
        parts = (owner, *(parts[1:] if own else parts))
    rooted = len(parts) > 1 and match_name(name=parts[0], declared=roots, quoted=False) is not None
    return parts[1:] if rooted else parts


def _variable_slots(
    text: str, *, name: str, variables: Dict[str, Any],
) -> List[Tuple[Tuple[str, ...], str, bool]]:
    """``(operand field parts, operand-left op, literal on the left)`` per changed comparison of ``text`` with
    ``'{name}'``."""
    taken = {*(str(v).strip() for v in variables.values())}
    marker = next((s for s in _SENTINELS if s not in text and s not in taken), None)
    if marker is None:
        return []
    try:
        bound = bind_variables(text, {**variables, name: marker})
    except ValueError:
        return []
    return [(parts, op, left) for parts, op, date, left in date_comparisons(bound) if date == marker and parts]


def _mode_a_surfaces(dataset: SlayerDataset) -> list:
    return [dataset.sql, *(dataset.filters or []), *(t for c in dataset.fields for t in (c.sql, c.filter))]


def _metric_needs_pass(metric: DatasetMetric) -> bool:
    return has_bindings(metric.expression)


def dataset_needs_substitution_pass(dataset: SlayerDataset) -> bool:
    """True if substitution must run with no variables (a Mode-A ``{? ?}`` block, declared
    variables, or any ``{...}`` token in a saved metric expression)."""
    return (
        any(s and _contains_block_delimiter(s) for s in _mode_a_surfaces(dataset))
        or declares_variables(dataset)
        or any(_metric_needs_pass(m) for m in dataset.metrics)
    )


def dataset_placeholder_names(dataset: SlayerDataset) -> set:
    """Every ``{var}`` a dataset's Mode-A surfaces and saved metric expressions read."""
    out: set = {name for m in dataset.metrics for name in variable_names(m.expression)}
    for text in _mode_a_surfaces(dataset):
        if text:
            bare, blocked = extract_variable_refs(text)
            out |= bare | blocked
    return out


#: Binds one Mode-A text: ``bind(text, variables, field=...)``.
ModeABind = Callable[..., str]


def substitute_dataset_sql_surfaces(
    *, dataset: SlayerDataset, variables: Dict[str, Any], bind_mode_a: ModeABind,
) -> SlayerDataset:
    """Copy of ``dataset`` with ``{var}`` bound as literals into its four Mode-A surfaces by ``bind_mode_a``
    and into its saved metric expressions; no-op when unneeded."""
    if not variables and not dataset_needs_substitution_pass(dataset):
        return dataset
    variables = coerce_declared_list_variables(
        variables, list_valued=list_valued_variable_names(dataset)
    )

    def _sub(text: str, field: str) -> str:
        return bind_mode_a(text, variables, field=field)

    update: Dict[str, Any] = {"fields": _substituted_fields(dataset.fields, _sub)}
    if dataset.sql is not None:
        update["sql"] = _sub(dataset.sql, f"Dataset {dataset.name!r} sql")
    if dataset.filters:
        update["filters"] = [_sub(f, f"Dataset {dataset.name!r} filters[{i}]") for i, f in enumerate(dataset.filters)]
    if dataset.metrics:
        update["metrics"] = _substituted_metrics(dataset.metrics, variables)
    return dataset.model_copy(update=update)


def _substituted_fields(fields: List[DatasetField], sub: Callable[[str, str], str]) -> List[DatasetField]:
    out = []
    for col in fields:
        updates: Dict[str, Any] = {}
        if col.sql is not None:
            updates["sql"] = sub(col.sql, f"Field {col.name!r} sql")
        if col.filter is not None:
            updates["filter"] = sub(col.filter, f"Field {col.name!r} filter")
        out.append(col.model_copy(update=updates) if updates else col)
    return out


def _substituted_metrics(metrics: List[DatasetMetric], variables: Dict[str, Any]) -> List[DatasetMetric]:
    return [
        m.substituted(bind_variables(m.expression, variables), variables=variables) if _metric_needs_pass(m) else m
        for m in metrics
    ]


def bind_extension_metrics(*, query: SlayerQuery, variables: Dict[str, Any]) -> SlayerQuery:
    """``query`` with ``{var}`` bound into its extensions' metrics."""
    if not any(e.metrics for e in query.extensions or []):
        return query
    extensions = [
        e.model_copy(update={"metrics": _substituted_metrics(e.metrics, variables)}) if e.metrics else e
        for e in query.extensions or []
    ]
    return query.model_copy(update={"extensions": extensions})


__all__ = [
    "apply_variables_to_query",
    "dataset_needs_substitution_pass",
    "dataset_placeholder_names",
    "bind_extension_metrics",
    "substitute_dataset_sql_surfaces",
    "extract_placeholder_names",
    "merge_query_variables",
]
