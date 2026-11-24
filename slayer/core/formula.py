"""Moved to :mod:`slayer.core.syntax` in SLayer 2.0; a known 1.x integrator's names stay, each deprecated."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Union

from pydantic import BaseModel, Field

from slayer.core import syntax
from slayer.core.deprecation import deprecated_input, raise_moved
from slayer.core.direction import rank_direction
from slayer.core.enums import BUILTIN_AGGREGATIONS, normalize_aggregation_name


class _AggregatedMeasureRef(BaseModel):
    measure_name: str
    aggregation_name: str
    agg_args: list[str] = Field(default_factory=list)
    agg_kwargs: dict[str, str] = Field(default_factory=dict)


class _ArithmeticField(BaseModel):
    sql: str
    measure_names: list[str]
    agg_refs: dict[str, _AggregatedMeasureRef] = Field(default_factory=dict)
    is_predicate: bool = False


class _TransformField(BaseModel):
    transform: str
    inner: _FieldSpec
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)


class _MixedArithmeticField(BaseModel):
    sql: str
    measure_names: list[str]
    sub_transforms: list[tuple[str, _TransformField]]
    agg_refs: dict[str, _AggregatedMeasureRef] = Field(default_factory=dict)
    is_predicate: bool = False


_FieldSpec = Union[_AggregatedMeasureRef, _ArithmeticField, _TransformField, _MixedArithmeticField]
_TransformField.model_rebuild()
_MixedArithmeticField.model_rebuild()

_PREDICATES = (syntax.Cmp, syntax.BoolOp)


def _expanded(formula: str, named: Mapping[str, str], chain: tuple[str, ...]) -> Any:
    """``formula``'s tree with each bare sibling-measure name replaced by its own expanded tree."""
    def expand(ref: syntax.Ref | syntax.DottedRef) -> Any:
        if not isinstance(ref, syntax.Ref) or ref.name not in named:
            return ref
        if ref.name in chain:
            raise ValueError(f"Saved measure '{ref.name}' has a cyclic reference: {' → '.join([*chain, ref.name])}")
        return _expanded(named[ref.name], named, (*chain, ref.name))

    return syntax.map_refs(syntax.parse_expr(formula), expand)


def _path(node: Any) -> str:
    if isinstance(node, syntax.Ref):
        return node.name
    if isinstance(node, syntax.DottedRef):
        return ".".join(node.parts)
    raise ValueError(f"expected a column name or dotted path, got {syntax.canonical_metric_text(node)!r}")


def _value(node: Any) -> Any:
    """A literal argument's Python value."""
    if isinstance(node, syntax.Literal):
        value = node.value
        if not isinstance(value, Decimal):
            return value
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(node, syntax.UnaryOp) and node.op == "-" and isinstance(node.operand, syntax.Literal):
        return -_value(node.operand)
    raise ValueError(f"expected a literal value, got {syntax.canonical_metric_text(node)!r}")


def _split(args: tuple[Any, ...], kwargs: tuple[tuple[str, Any], ...]) -> tuple[list[Any], list[tuple[str, Any]]]:
    positional = [a for a in args if not isinstance(a, syntax.NamedArg)]
    named = [*kwargs, *((a.name, a.value) for a in args if isinstance(a, syntax.NamedArg))]
    return positional, named


class _Projection:
    def __init__(self, *, original: str, agg_names: frozenset[str], custom: frozenset[str]) -> None:
        self.original = original
        self.agg_names = agg_names
        self.custom = custom

    def aggregation(self, node: syntax.AggCall) -> _AggregatedMeasureRef:
        if node.agg not in self.agg_names:
            raise ValueError(f"Unknown aggregation '{node.agg}' in formula {self.original!r}")
        source = node.source
        if isinstance(source, syntax.StarSource):
            source_path = "*"
        elif isinstance(source, (syntax.Ref, syntax.DottedRef)):
            source_path = _path(source)
        else:
            raise ValueError(f"'{node.agg}' aggregates a column or '*', not an expression, in formula {self.original!r}")
        positional, named = _split(node.args, node.kwargs)
        return _AggregatedMeasureRef(
            measure_name=source_path,
            aggregation_name=node.agg if node.agg in self.custom else normalize_aggregation_name(node.agg),
            agg_args=[syntax.canonical_metric_text(a) for a in positional],
            agg_kwargs={k: syntax.canonical_metric_text(v) for k, v in named},
        )

    def transform(self, node: syntax.TransformCall) -> _TransformField:
        positional, named = _split(node.args, node.kwargs)
        kwargs: dict[str, Any] = {}
        direction: Any = None
        for key, value in named:
            if key == "direction":
                direction = value
            elif key == "partition_by":
                values = value if isinstance(value, (tuple, syntax.ArrayLit)) else (value,)
                kwargs[key] = [_path(v) for v in (values.elements if isinstance(values, syntax.ArrayLit) else values)]
            else:
                kwargs[key] = _value(value)
        literal = direction.value if isinstance(direction, syntax.Literal) else None
        resolved = rank_direction(op=node.op, given=direction is not None, value=literal)
        if resolved is not None:
            kwargs["direction"] = resolved
        return _TransformField(transform=node.op, inner=self.spec(node.input),
                               args=[_value(a) for a in positional], kwargs=kwargs)

    def spec(self, node: Any) -> _FieldSpec:
        if isinstance(node, syntax.AggCall):
            return self.aggregation(node)
        if isinstance(node, syntax.TransformCall):
            return self.transform(node)
        if isinstance(node, (syntax.Arith, syntax.UnaryOp, syntax.Cmp, syntax.BoolOp, syntax.ScalarCall,
                             syntax.Literal)):
            return self.arithmetic(node)
        if isinstance(node, syntax.DottedRef):
            raise ValueError(f"Cross-model measure '{_path(node)}' must include an aggregation (e.g., 'sum({_path(node)})').")
        if isinstance(node, syntax.Ref):
            raise ValueError(f"'{node.name}' is not a saved measure. Reference a saved measure by its bare name, or "
                             f"aggregate a column (e.g., 'sum({node.name})'). For COUNT(*), use 'count(*)'.")
        raise ValueError(f"Unsupported formula syntax: {self.original!r}")

    def arithmetic(self, node: Any) -> _ArithmeticField | _MixedArithmeticField:
        lifter = _Lifter(self)
        sql = syntax.canonical_metric_text(lifter.lift(node))
        predicate = isinstance(node, _PREDICATES)
        if lifter.calls:
            return _MixedArithmeticField(sql=sql, measure_names=lifter.names, sub_transforms=lifter.transforms,
                                         agg_refs=lifter.agg_refs, is_predicate=predicate)
        return _ArithmeticField(sql=sql, measure_names=lifter.names, agg_refs=lifter.agg_refs, is_predicate=predicate)


_REBUILT = (syntax.Arith, syntax.UnaryOp, syntax.Cmp, syntax.BoolOp, syntax.ScalarCall, syntax.TupleLit,
            syntax.ArrayLit)


class _Lifter:
    """An arithmetic tree with each aggregation and transform lifted out into a named placeholder."""

    def __init__(self, projection: _Projection) -> None:
        self.projection = projection
        self.agg_refs: dict[str, _AggregatedMeasureRef] = {}
        self.transforms: list[tuple[str, _TransformField]] = []
        self.names: list[str] = []
        self.calls = False

    def lift(self, item: Any) -> Any:
        if isinstance(item, syntax.AggCall):
            key = f"__agg{len(self.agg_refs)}__"
            self.agg_refs[key] = self.projection.aggregation(item)
            self.names.append(key)
            return syntax.Ref(name=key)
        if isinstance(item, syntax.TransformCall):
            key = f"_t{len(self.transforms)}"
            self.transforms.append((key, self.projection.transform(item)))
            self.calls = True
            return syntax.Ref(name=key)
        if isinstance(item, (syntax.Ref, syntax.DottedRef)):
            self.projection.spec(item)
        if isinstance(item, syntax.ScalarCall):
            self.calls = True
        if isinstance(item, (tuple, list)):
            return type(item)(self.lift(i) for i in item)
        if isinstance(item, _REBUILT):
            # ``written_args`` repeats a spelling's arguments as written.
            return item.model_copy(update={
                f: self.lift(getattr(item, f)) for f in type(item).model_fields if f != "written_args"})
        if isinstance(item, BaseModel) and not isinstance(item, (syntax.Literal, syntax.Placeholder)):
            raise ValueError(f"Unsupported formula syntax: {self.projection.original!r}")
        return item


def _parse_formula(
    formula: str, extra_agg_names: frozenset[str] | None = None, named_measures: Mapping[str, str] | None = None,
) -> _FieldSpec:
    """``formula`` read by the 2.0 parser, as the 1.x result shape."""
    custom = frozenset(extra_agg_names or ())
    tree = _expanded(formula, dict(named_measures or {}), ())
    return _Projection(original=formula, agg_names=BUILTIN_AGGREGATIONS | custom, custom=custom).spec(tree)


_NEW = "slayer.core.syntax.parse_expr (a different result shape: see the upgrade guide, Integrators reading SLayer objects)"
_KEPT: dict[str, tuple[Callable[[], object], object]] = {
    "parse_formula": (
        lambda: deprecated_input("slayer.core.formula.parse_formula", _NEW, removal="3.0"), _parse_formula),
    "AggregatedMeasureRef": (
        lambda: deprecated_input("slayer.core.formula.AggregatedMeasureRef", _NEW, removal="3.0"),
        _AggregatedMeasureRef),
    "ArithmeticField": (
        lambda: deprecated_input("slayer.core.formula.ArithmeticField", _NEW, removal="3.0"), _ArithmeticField),
    "MixedArithmeticField": (
        lambda: deprecated_input("slayer.core.formula.MixedArithmeticField", _NEW, removal="3.0"),
        _MixedArithmeticField),
    "TransformField": (
        lambda: deprecated_input("slayer.core.formula.TransformField", _NEW, removal="3.0"), _TransformField),
}

if not TYPE_CHECKING:  # the moved names stay static errors

    def __getattr__(name: str) -> object:
        if name.startswith("__"):
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        if name not in _KEPT:
            raise_moved("slayer.core.formula", "slayer.core.syntax", removal="3.0")
        warn, value = _KEPT[name]
        warn()
        return value
