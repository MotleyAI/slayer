"""One registry table for aggregation rendering.

Each aggregation is one :class:`AggEntry`; :func:`render_aggregate` is the one
dispatch over a fully resolved :class:`AggregateSpec`, shared by the SQL
generator and the Ossie exporter.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, Literal, Optional, Type

from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import (
    BOOLEAN_LOWERED_AGGREGATIONS,
    BOOLEAN_RESTORED_AGGREGATIONS,
    BUILTIN_AGGREGATIONS,
    DataType,
)
from slayer.core.models import VALUE_PLACEHOLDER, reserved_value_param_message
from slayer.sql.dialects.base import SqlDialect, is_stat_agg1, is_stat_agg2
from slayer.sql.render.row_expr import cast_target, int_value
from slayer.sql.sql_template import SqlTemplateError, sql_template

# Which mechanism renders an aggregation. Retained as data so the generator's
# dispatch is a table lookup rather than five stacked conditionals.
DISPATCH_SIMPLE = "simple"        # direct sqlglot node: COUNT/SUM/AVG/MIN/MAX
DISPATCH_RANKED = "ranked"        # first/last — needs the ranked-subquery state
DISPATCH_STAT = "stat"            # stddev/var/corr/covar — dialect UDF split
DISPATCH_DIALECT_HOOK = "dialect_hook"  # percentile/median/approx-distinct
DISPATCH_DISTINCT = "distinct"    # COUNT(DISTINCT ...)
DISPATCH_FORMULA = "formula"      # {value}/{param} template substitution

# Closed set of dispatch names (mirrors the DISPATCH_* constants above). An
# entry built with a value outside this set fails at import, not at render.
DispatchKind = Literal[
    "simple", "ranked", "stat", "dialect_hook", "distinct", "formula",
]


class AggEntry(BaseModel):
    """How one aggregation renders."""

    model_config = ConfigDict(
        frozen=True, arbitrary_types_allowed=True, extra="forbid",
    )

    name: str
    dispatch: DispatchKind
    # The sqlglot class for the simple path, when there is one.
    node_class: Optional[Type[Expression]] = None


def _entry(*, name: str, dispatch: DispatchKind, **kw) -> AggEntry:
    return AggEntry(name=name, dispatch=dispatch, **kw)


AGG_REGISTRY: Dict[str, AggEntry] = {
    e.name: e
    for e in (
        _entry(name="sum", dispatch=DISPATCH_SIMPLE, node_class=exp.Sum),
        _entry(name="avg", dispatch=DISPATCH_SIMPLE, node_class=exp.Avg),
        _entry(name="count", dispatch=DISPATCH_SIMPLE, node_class=exp.Count),
        _entry(name="min", dispatch=DISPATCH_SIMPLE, node_class=exp.Min),
        _entry(name="max", dispatch=DISPATCH_SIMPLE, node_class=exp.Max),
        _entry(name="count_distinct", dispatch=DISPATCH_DISTINCT, node_class=exp.Count),
        _entry(name="count_distinct_approx", dispatch=DISPATCH_DIALECT_HOOK),
        _entry(name="first", dispatch=DISPATCH_RANKED),
        _entry(name="last", dispatch=DISPATCH_RANKED),
        _entry(name="median", dispatch=DISPATCH_DIALECT_HOOK),
        _entry(name="percentile", dispatch=DISPATCH_DIALECT_HOOK),
        _entry(name="weighted_avg", dispatch=DISPATCH_FORMULA),
        _entry(name="stddev_samp", dispatch=DISPATCH_STAT),
        _entry(name="stddev_pop", dispatch=DISPATCH_STAT),
        _entry(name="var_samp", dispatch=DISPATCH_STAT),
        _entry(name="var_pop", dispatch=DISPATCH_STAT),
        _entry(name="corr", dispatch=DISPATCH_STAT),
        _entry(name="covar_samp", dispatch=DISPATCH_STAT),
        _entry(name="covar_pop", dispatch=DISPATCH_STAT),
        _entry(name="collect", dispatch=DISPATCH_DIALECT_HOOK),
    )
}

# Every built-in must be in the table, or a lookup would fall through to the
# custom-formula path and render a built-in as if it were user-defined.
# Checked BOTH ways: a missing built-in would fall through to the custom-formula
# path, and a registry key that is NOT a built-in (a typo such as sumn) would
# make is_builtin_agg accept it and route it away from that path.
_registered = set(AGG_REGISTRY)
_missing = set(BUILTIN_AGGREGATIONS) - _registered
_unknown = _registered - set(BUILTIN_AGGREGATIONS)
if _missing or _unknown:  # pragma: no cover — import-time invariant
    raise RuntimeError(
        f"Aggregation registry disagrees with BUILTIN_AGGREGATIONS: "
        f"missing={sorted(_missing)}, unknown={sorted(_unknown)}",
    )


def resolve_agg_entry(name: str) -> AggEntry:
    """Return the registry entry for a BUILT-IN aggregation.

    Raises ``ValueError`` for anything else. Custom model-level aggregations
    are deliberately not registered: they carry their own ``formula`` and take
    the template path, so callers check :func:`is_builtin_agg` first.
    """
    entry = AGG_REGISTRY.get(name)
    if entry is None:
        raise ValueError(
            f"Unknown aggregation {name!r}. Built-ins: "
            f"{sorted(AGG_REGISTRY)}; anything else must be defined as a "
            f"model-level aggregation with a formula.",
        )
    return entry


def is_builtin_agg(name: str) -> bool:
    return name in AGG_REGISTRY


def aggregate_input(*, value: Expression, aggregation: str, input_type: Optional[DataType]) -> Expression:
    """The value ``aggregation`` reads: a BOOLEAN input to a numeric aggregation is its integer."""
    if input_type is DataType.BOOLEAN and aggregation in BOOLEAN_LOWERED_AGGREGATIONS:
        return int_value(value)
    return value


def apply_aggregate(
    *, entry: AggEntry, value: Expression, input_type: Optional[DataType], dialect: SqlDialect,
) -> Expression:
    """``entry``'s direct-node aggregate over ``value`` (read via :func:`aggregate_input`); ``avg`` is fractional
    on every dialect; a boolean ``min`` / ``max`` converts the result back to the dialect's boolean."""
    if entry.node_class is None:
        raise ValueError(f"Aggregation {entry.name!r} has no SQL node to render.")
    if entry.dispatch == DISPATCH_DISTINCT:
        return entry.node_class(this=exp.Distinct(expressions=[value]))
    read = aggregate_input(value=value, aggregation=entry.name, input_type=input_type)
    # A DOUBLE input is already fractional (and may be an exact decimal whose precision is kept).
    if entry.name == "avg" and dialect.integer_avg and input_type is not DataType.DOUBLE:
        read = exp.Cast(this=read, to=exp.DataType.build("FLOAT"))
    aggregate = entry.node_class(this=read)
    restored = dialect.declared_cast_type(DataType.BOOLEAN)
    if input_type is not DataType.BOOLEAN or entry.name not in BOOLEAN_RESTORED_AGGREGATIONS or restored is None:
        return aggregate
    return exp.Cast(this=aggregate, to=cast_target(restored))


class AggregateSpec(BaseModel):
    """A fully resolved aggregate; ``value`` ``None`` is the row itself (``COUNT(*)``), ``formula`` the template."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    aggregation: str
    value: Optional[Expression] = None
    #: Each parameter's bound SQL.
    bindings: Dict[str, Expression] = {}
    input_type: Optional[DataType] = None
    element_type: Optional[DataType] = None
    formula: Optional[str] = None


def percentile_literal(p: Expression) -> Expression:
    """The numeric literal inside ``p`` (optionally signed / parenthesised), validated to [0, 1]."""
    node, negative = p, False
    while isinstance(node, (exp.Paren, exp.Neg)):
        negative ^= isinstance(node, exp.Neg)
        node = node.this
    if not isinstance(node, exp.Literal) or node.is_string:
        raise ValueError(
            f"Aggregation 'percentile' parameter 'p' must be a numeric literal "
            f"in [0, 1]; got {p.sql()!r}."
        )
    try:
        value = Decimal(node.this)
    except ArithmeticError:
        value = Decimal("NaN")
    if negative:
        value = -value
    if not value.is_finite() or not 0 <= value <= 1:
        raise ValueError(
            f"Aggregation 'percentile' parameter 'p' must be in [0, 1]; got {p.sql()}."
        )
    return node.copy()


def _param(spec: AggregateSpec, name: str) -> Expression:
    """A named parameter's bound SQL; a missing one fails closed."""
    value = spec.bindings.get(name)
    if value is None:
        agg_name = spec.aggregation
        raise ValueError(
            f"Aggregation '{agg_name}' requires parameter '{name}'. "
            f"Set it in the model's aggregation definition or at query time "
            f"(e.g., '{agg_name}(measure, {name}=column)')."
        )
    return value.copy()


def _read_value(spec: AggregateSpec) -> Expression:
    """The value the aggregation reads (a boolean input to a numeric aggregation as its integer)."""
    if spec.value is None:
        raise ValueError(f"Aggregation {spec.aggregation!r} needs a value; only 'count' is defined over '*'.")
    return aggregate_input(value=spec.value.copy(), aggregation=spec.aggregation, input_type=spec.input_type)


def _formula_aggregate(spec: AggregateSpec, *, dialect: SqlDialect) -> Expression:
    agg_name = spec.aggregation
    if spec.formula is None:
        raise ValueError(
            f"Aggregation '{agg_name}' has no formula. "
            f"Custom aggregations must define a formula."
        )
    if VALUE_PLACEHOLDER in spec.bindings:
        raise SqlTemplateError(reserved_value_param_message(agg_name))
    try:
        template = sql_template(text=spec.formula, dialect=dialect.sqlglot_name)
    except SqlTemplateError as e:
        raise SqlTemplateError(f"Aggregation '{agg_name}': {e}") from e
    bindings = {
        name: _param(spec, name) for name in template.placeholder_names if name != VALUE_PLACEHOLDER
    }
    # A source ``Column.filter`` is already baked into the value.
    bindings[VALUE_PLACEHOLDER] = exp.Star() if spec.value is None else _read_value(spec)
    try:
        return template.render(bindings)
    except SqlTemplateError as e:
        raise SqlTemplateError(f"Aggregation '{agg_name}': {e}") from e


def render_aggregate(spec: AggregateSpec, *, dialect: SqlDialect) -> Expression:
    """The one aggregate dispatch: ``spec`` as ``dialect``'s aggregate SQL."""
    agg_name = spec.aggregation
    if not is_builtin_agg(agg_name) or spec.formula:
        return _formula_aggregate(spec, dialect=dialect)
    entry = resolve_agg_entry(agg_name)
    if entry.dispatch == DISPATCH_STAT:
        if is_stat_agg2(agg_name):
            # other= first, so a missing-required-param error outranks the dialect-unsupported one.
            other = _param(spec, "other")
            return dialect.build_covar_2arg(agg_name=agg_name, col_expr=_read_value(spec), other_expr=other)
        if is_stat_agg1(agg_name):
            return dialect.build_stat_agg_1arg(agg_name=agg_name, col_expr=_read_value(spec))
        raise ValueError(f"Unknown statistical aggregation {agg_name!r}.")
    if agg_name == "percentile":
        p = percentile_literal(_param(spec, "p"))
        return dialect.build_percentile(p=p, col_expr=_read_value(spec))
    if agg_name == "collect":
        return dialect.build_collect(_value(spec), spec.element_type)
    if agg_name == "count_distinct_approx":
        return dialect.build_approx_count_distinct(col_expr=_value(spec))
    if agg_name == "median":
        return dialect.build_median(inner=_read_value(spec))
    inner = exp.Star() if spec.value is None and agg_name == "count" else _value(spec)
    return apply_aggregate(entry=entry, value=inner, input_type=spec.input_type, dialect=dialect)


def _value(spec: AggregateSpec) -> Expression:
    if spec.value is None:
        raise ValueError(f"Aggregation {spec.aggregation!r} needs a value; only 'count' is defined over '*'.")
    return spec.value.copy()
