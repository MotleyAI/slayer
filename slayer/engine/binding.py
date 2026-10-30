"""ExpressionBinder + FilterBinder: bind a ``ParsedExpr`` against a scope
(``ModelScope`` / ``StageSchema``) into a typed ``BoundExpr`` / ``BoundFilter``."""

from __future__ import annotations

import difflib
import os
from datetime import datetime
from decimal import Decimal
from functools import partial
from typing import Callable, Dict, Final, List, Mapping, Optional, Tuple, TypeGuard, cast

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import (
    ScalarArgumentError,
    ScalarOperandTypeError,
    AggregationArgumentError,
    AggregationFilterError,
    AggregationNotAllowedError,
    CastTypeError,
    CircularJoinPathError,
    GranularityCallError,
    IllegalScopeReferenceError,
    IllegalWindowInFilterError,
    MeasureCycleError,
    MeasureRecursionLimitError,
    PartitionKeyError,
    TimeLiteralError,
    TransformArgumentError,
    UnknownFunctionError,
    UnknownReferenceError,
    UnresolvableDimensionJoinError,
    UnresolvedPlaceholderError,
)
from slayer.core.enums import AMBIGUOUS_WEEKDAY_NAMES, BUILTIN_AGGREGATION_PARAM_ORDER, BUILTIN_AGGREGATIONS, DATE_PART_ALIASES, DEFAULT_AGGREGATIONS_BY_TYPE, GRANULARITY_NAMES, NUMERIC_ONLY_AGGREGATIONS, PRIMARY_KEY_AGGREGATIONS, RANKED_AGGREGATIONS, DataType, ambiguous_weekday_message, normalize_aggregation_name, ALL_TRANSFORMS
from slayer.core.direction import rank_direction, with_direction_kwarg
from slayer.core.enums import RANK_FAMILY_TRANSFORMS
from slayer.core.granularity import CustomGranularity, Granularity, resolve_granularity
from slayer.core.refs import EXPRESSION_SOURCE_KINDS, key_display
from slayer.core.keys import source_anchor_path, window_kwarg_of, DATE_ADD_COUNT_ARG, DATE_OPERAND_ARGS, MIRRORED_TIME_POINT_OP, SCALAR_FUNCTIONS, cast_data_type, check_scalar_arity, type_date_values, AggregateKey, AggregateSource, ArithmeticKey, ArrayLiteralKey, CastKey, ColumnKey, ColumnSqlKey, Grain, InKey, LiteralKey, Phase, ScalarCallKey, SqlFragmentKey, StarKey, TimePointCmpKey, TimePointOp, TimeTruncKey, TransformKey, VALUE_KEY_TYPES, ValueKey, boolean_valued, column_leaf, column_path, is_attached_source, normalize_scalar, prepend_value_key, temporal_type, walk_value_keys, TEXT_RESULT_SCALARS, numeric_valued
from slayer.core.join_walker import (
    LIST_ELEMENT_COLUMN,
    resolved_terminal,
    aggregation_owner,
    canonical_path,
    terminal_model,
    walk,
    walk_miss,
    walk_terminal,
)
from slayer.core.models import (
    VALUE_PLACEHOLDER,
    SlayerModel,
    aggregation_definition,
    is_identifier,
    reserved_value_param_message,
)
from slayer.engine import dimension_routing
from slayer.engine.key_metadata import bundle_dialect, scope_column_type
from slayer.engine.param_binding import bind_aggregation_params
from slayer.core.query import TimeDimension
from slayer.core.scope import ModelScope, StageSchema, resolve_generated_column
from slayer.core.time_points import is_time_point, parse_temporal_value
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.core.syntax import (
    AggCall,
    ArrayLit,
    Arith,
    BoolOp,
    Cast,
    Cmp,
    DottedRef,
    Literal,
    ParsedExpr,
    Ref,
    ScalarCall,
    StarSource,
    TemporalLiteral,
    TransformCall,
    TupleLit,
    UnaryOp,
    canonical_measure_text,
    comparison_hint,
    keyword_spelling,
    parse_expr,
    placeholder_names,
    positional_hint,
)
from slayer.core.scalar_args import (
    REGEX_FUNCTIONS,
    check_domain,
    check_flags,
    check_group,
    check_path,
    check_pattern,
    check_replacement,
    check_split_index,
    slot_name,
)
from slayer.sql.dialects.base import LIBRARY_FUNCTIONS, is_list_fn, require_library
from slayer.sql.sql_expr import build_cast_type, has_window_function, native_json_type
from slayer.sql.sql_template import SqlTemplateError, aggregation_reads
from slayer.ir.bound import BoundExpr, BoundFilter, BoundTimeDimension

__all__ = [
    "bind_expr",
    "bind_filter",
    "bind_time_dimension",
]


_DEFAULT_MEASURE_DEPTH = 32
_MEASURE_DEPTH_ENV_VAR = "SLAYER_MEASURE_EXPANSION_DEPTH"
_NO_ANCHOR_SCOPE_SUMMARY = "(no source_model anchor; anchor-less mode not implemented)"


def _measure_depth_limit() -> int:
    """Saved-measure expansion depth cap (env override, default 32)."""
    raw = os.environ.get(_MEASURE_DEPTH_ENV_VAR)
    if raw is None:
        return _DEFAULT_MEASURE_DEPTH
    try:
        return max(1, int(raw))
    except ValueError:
        return _DEFAULT_MEASURE_DEPTH


class MeasureResolutionCtx(BaseModel):
    """Non-``None`` makes saved-measure refs legal here; carries the
    ``(model, measure)`` chain for cycle/depth detection. Dropped at
    aggregation boundaries so a measure errors there."""

    model_config = ConfigDict(frozen=True)

    chain: Tuple[Tuple[str, str], ...] = ()
    depth_limit: int

    def descend(self, *, model: str, measure: str) -> "MeasureResolutionCtx":
        return self.model_copy(update={"chain": self.chain + ((model, measure),)})


def _fmt_measure_chain(chain: Tuple[Tuple[str, str], ...]) -> List[str]:
    """Render a ``(model, measure)`` chain as ``model.measure`` steps for errors."""
    return [f"{model}.{measure}" for model, measure in chain]


def bind_expr(
    parsed: ParsedExpr,
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    allow_measures: bool = False,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    dimension_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> BoundExpr:
    """Bind a parsed expression against a scope into a ``BoundExpr``.

    ``allow_measures`` enables saved-measure resolution (bare and dotted) in
    the eligible positions — measure formulas and computed-dimension
    expressions; off everywhere else, so a saved-measure name there errors.
    ``alias_map`` resolves query aliases as in :func:`bind_filter`.
    ``dimension_alias_map`` resolves ``partition_by=<computed dim name>`` to
    the dimension's bound key; it applies ONLY there."""
    _reject_placeholders(parsed)
    measure_ctx = (
        MeasureResolutionCtx(depth_limit=_measure_depth_limit())
        if allow_measures else None
    )
    value_key = _bind(
        parsed, scope=scope, bundle=bundle, in_filter=False, alias_map=alias_map, measure_ctx=measure_ctx,
        dim_alias_map=dimension_alias_map,
    )
    return BoundExpr(
        value_key=value_key,
        routed_dotted=_canonical_if_routed(
            parsed=parsed, value_key=value_key, scope=scope,
        ),
    )


def spelled_aggregate_key(
    *, parsed: ParsedExpr, key: AggregateKey, bundle: ResolvedSourceBundle,
) -> AggregateKey:
    """``key`` keeping only the parameters the query wrote (the public name's basis);
    a bound definition default is never part of the spelling."""
    if not isinstance(parsed, AggCall):
        return key
    positional = _declared_agg_param_names(agg=key.agg, source=key.source, bundle=bundle)
    written = {k for k, _ in parsed.kwargs} | set(positional[:len(parsed.args)])
    return key.model_copy(update={
        "kwargs": tuple((k, v) for k, v in key.kwargs if k in written),
    })


def bind_time_dimension(
    td: TimeDimension,
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    granularity: Optional[Granularity] = None,
) -> BoundTimeDimension:
    """Bind a ``TimeDimension`` into a ``BoundTimeDimension``: a ``BoundExpr`` carrying a ``TimeTruncKey`` plus the facts the checker judges (a column's type; every bucketed temporal leaf's recorded granularity — from a ``StageColumn`` or a model ``Column``). A column resolves like a Mode-B identifier ref against a ``ModelScope`` (joins) or a flat ``StageSchema``, any other text binds as a Mode-B expression; the row-level / temporal / re-bucketing rules are the checker's (P9)."""
    text = td.dimension
    resolved = granularity or resolve_granularity(td.granularity, defined=bundle.granularities)
    if resolved is None:
        raise ValueError(f"time dimension {text!r}: unknown granularity {td.granularity!r}")
    parsed = parse_expr(text)
    if not isinstance(parsed, (Ref, DottedRef)):
        source = bind_expr(parsed=parsed, scope=scope, bundle=bundle).value_key
        return BoundTimeDimension(
            bound=BoundExpr(value_key=TimeTruncKey(source=source, granularity=resolved)),
            bucketed_leaves=_bucketed_leaves(source, scope=scope, bundle=bundle),
            is_expression=True,
        )
    bound_col = _resolve_column_ref(text, scope=scope, bundle=bundle)
    column_type, upstream = _column_facts(bound_col, scope=scope, bundle=bundle)
    routed = _canonical_if_routed(parsed=parsed, value_key=bound_col, scope=scope)
    return BoundTimeDimension(
        bound=BoundExpr(value_key=TimeTruncKey(source=bound_col, granularity=resolved), routed_dotted=routed),
        column_type=column_type,
        bucketed_leaves=() if upstream is None else ((text, upstream),),
    )


def _resolve_column_ref(
    full: str, *, scope: ModelScope | StageSchema, bundle: ResolvedSourceBundle,
) -> ColumnKey | ColumnSqlKey:
    """A time dimension's column against ``scope``: a stage ref is a flat ``StageColumn`` (dotted → illegal-scope, unknown → unknown-reference); a model ref walks joins."""
    if isinstance(scope, ModelScope) and scope.source_model is None:
        raise UnknownReferenceError(
            name=full,
            scope_kind="ModelScope",
            scope_summary=_NO_ANCHOR_SCOPE_SUMMARY,
            suggestion=None,
        )
    if "." in full:
        bound_col = _resolve_dotted(tuple(full.split(".")), scope=scope, bundle=bundle)
    else:
        bound_col = _resolve_ref(full, scope=scope, bundle=bundle)
    if not isinstance(bound_col, (ColumnKey, ColumnSqlKey)):
        # Defensive: an identifier ref against a ModelScope is always a column.
        raise ValueError(
            f"TimeDimension {full!r} did not resolve to a column "
            f"reference (got {type(bound_col).__name__})."
        )
    return bound_col


def _column_facts(
    col: ColumnKey | ColumnSqlKey, *, scope: ModelScope | StageSchema, bundle: ResolvedSourceBundle,
) -> Tuple[Optional[DataType], Optional[Granularity]]:
    """A column's (type, recorded bucket granularity): a flat ``StageColumn`` on a stage, else the terminal model ``Column`` its path reaches."""
    if isinstance(scope, StageSchema):
        stage_col = scope.get(column_leaf(col))
        assert stage_col is not None  # binding already validated existence
        upstream = (
            None if stage_col.granularity is None
            else resolve_granularity(stage_col.granularity, defined=bundle.granularities)
        )
        return stage_col.type, upstream
    terminal = _terminal_model_for_path(path=column_path(col), scope=scope, bundle=bundle)
    if terminal is None:
        # Defensive: _resolve_ref / _resolve_dotted would already have raised.
        name = ".".join((*column_path(col), column_leaf(col)))
        raise UnknownReferenceError(
            name=name,
            scope_kind="ModelScope",
            scope_summary=f"could not resolve terminal model for {name!r}",
            suggestion=None,
        )
    found = next((c for c in terminal.columns if c.name == column_leaf(col)), None)
    upstream = None
    if found is not None and found.granularity is not None:
        upstream = resolve_granularity(found.granularity, defined=bundle.granularities)
    return (found.type if found is not None else None), upstream


def _bucketed_leaves(
    source: ValueKey, *, scope: ModelScope | StageSchema, bundle: ResolvedSourceBundle,
) -> Tuple[Tuple[str, Granularity], ...]:
    """Every temporal column leaf of ``source`` that carries a recorded bucket granularity, by its dotted name."""
    out: Dict[str, Granularity] = {}
    for leaf in walk_value_keys(source):
        if not isinstance(leaf, (ColumnKey, ColumnSqlKey)):
            continue
        column_type, upstream = _column_facts(leaf, scope=scope, bundle=bundle)
        if upstream is not None and column_type in (DataType.DATE, DataType.TIMESTAMP):
            out.setdefault(".".join((*column_path(leaf), column_leaf(leaf))), upstream)
    return tuple(out.items())


def _canonical_if_routed(
    *,
    parsed: ParsedExpr,
    value_key: ValueKey,
    scope: ModelScope | StageSchema,
) -> Optional[str]:
    """Full canonical dotted path when a ``DottedRef``'s bound path differs from
    its typed hop path (auto-routed or respelled) — or the canonical name of a
    stale flat ``Ref`` — else ``None``, so every already-canonical ref keeps a
    byte-identical result key."""
    key = value_key.source if isinstance(value_key, TimeTruncKey) else value_key
    if not isinstance(key, (ColumnKey, ColumnSqlKey)):
        return None  # saved measures canonicalize in bind_inputs._resolve_saved_measure_ref
    if isinstance(parsed, Ref):
        leaf = column_leaf(key)
        return leaf if not column_path(key) and leaf != parsed.name else None
    if not isinstance(parsed, DottedRef):
        return None
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    typed = parsed.parts
    if typed and typed[0] == scope.source_model.name:
        typed = typed[1:]
    if tuple(column_path(key)) == tuple(typed[:-1]):
        return None
    return ".".join((*column_path(key), column_leaf(key)))


def _terminal_model_for_path(
    *,
    path: Tuple[str, ...],
    scope: ModelScope,
    bundle: ResolvedSourceBundle,
) -> Optional[SlayerModel]:
    """The terminal model of ``path`` from ``scope.source_model`` (host if empty)."""
    if scope.source_model is None:
        return None
    return terminal_model(root=scope.source_model, path=path, models_by_name=bundle.models_by_name)


def bind_filter(
    parsed: ParsedExpr,
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    dimension_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> BoundFilter:
    """Bind a parsed filter predicate + classify its phase.

    Walks the bound tree to gather every referenced ``ValueKey``; raises
    ``IllegalWindowInFilterError`` if a referenced ``Column.sql`` is windowed.
    ``alias_map`` maps a stage's declared-measure names to their bound
    ``ValueKey`` so a bare ref matching an alias interns onto that slot rather
    than resolving against model columns (the aggregate and its alias share one slot).
    ``dimension_alias_map`` resolves ``partition_by=<computed dim name>`` only."""
    _reject_placeholders(parsed)
    value_key = _bind(
        parsed, scope=scope, bundle=bundle, in_filter=True, alias_map=alias_map,
        dim_alias_map=dimension_alias_map,
    )
    refs = tuple(walk_value_keys(value_key))
    phase = max(
        (k.phase for k in refs),
        default=value_key.phase,
    )
    _reject_windowed_column_sql(refs, scope=scope, bundle=bundle, parsed=parsed)
    return BoundFilter(
        value_key=value_key, phase=phase, referenced_keys=refs,
    )


def _bind(
    parsed: ParsedExpr,
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> ValueKey:
    # ``measure_ctx`` rides eligible operand edges, dropped at the aggregation
    # boundary — a measure is legal at value level but not inside an aggregation.
    # ``dim_alias_map`` rides every edge but resolves ONLY inside an
    # aggregation's ``partition_by``.
    leaf = _bind_leaf(parsed, scope=scope, bundle=bundle, alias_map=alias_map, measure_ctx=measure_ctx)
    if leaf is not None:
        return leaf
    sub = partial(
        _bind, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map,
        measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
    )
    if isinstance(parsed, Cast):
        return _bind_cast(parsed, source=sub(parsed.source), bundle=bundle)
    if isinstance(parsed, AggCall):
        return _bind_agg_call(
            parsed, scope=scope, bundle=bundle, alias_map=alias_map,
            measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
        )
    if isinstance(parsed, TransformCall):
        return _bind_transform(
            parsed, scope=scope, bundle=bundle, alias_map=alias_map,
            measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
        )
    if isinstance(parsed, ScalarCall):
        return _bind_scalar(
            parsed, scope=scope, bundle=bundle, in_filter=in_filter,
            alias_map=alias_map, measure_ctx=measure_ctx,
            dim_alias_map=dim_alias_map,
        )
    if isinstance(parsed, Cmp):
        return _bind_cmp(
            parsed, sub=sub, scope=scope, bundle=bundle, in_filter=in_filter,
            alias_map=alias_map, measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
        )
    if isinstance(parsed, Arith):
        return ArithmeticKey(op=parsed.op, operands=(sub(parsed.left), sub(parsed.right)))
    if isinstance(parsed, UnaryOp):
        return ArithmeticKey(op=parsed.op, operands=(sub(parsed.operand),))
    if isinstance(parsed, BoolOp):
        return ArithmeticKey(op=parsed.op, operands=tuple(sub(v) for v in parsed.operands))
    if isinstance(parsed, ArrayLit):
        return _bind_array_literal(parsed, sub=sub)
    raise ValueError(
        f"Unsupported ParsedExpr node: {type(parsed).__name__}"
    )


def _bind_array_literal(parsed: ArrayLit, *, sub: Callable[[ParsedExpr], ValueKey]) -> ArrayLiteralKey:
    """A literal list: every element a literal (its NULL / type-family rules are the checker's)."""
    values = []
    for element in parsed.elements:
        key = sub(element)
        if isinstance(key, ArithmeticKey) and key.op == "-" and len(key.operands) == 1 \
                and isinstance(key.operands[0], LiteralKey) and isinstance(key.operands[0].value, Decimal):
            key = LiteralKey(value=-key.operands[0].value)
        if not isinstance(key, LiteralKey):
            raise ScalarArgumentError(
                summary=f"Every element of the list literal `{canonical_measure_text(parsed)}` must be a literal; "
                f"`{canonical_measure_text(element)}` is not.",
                suggestion="Compare a list with another list column through contains_any / contains_all instead.",
            )
        values.append(key)
    return ArrayLiteralKey(values=tuple(values))


def _bind_leaf(
    parsed: ParsedExpr,
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]],
    measure_ctx: Optional[MeasureResolutionCtx],
) -> Optional[ValueKey]:
    """A literal, reference or ``*`` leaf bound; ``None`` for a composite node."""
    if isinstance(parsed, Literal):
        return LiteralKey(value=normalize_scalar(parsed.value))
    if isinstance(parsed, TemporalLiteral):
        return LiteralKey(value=_temporal_literal_value(parsed))
    if isinstance(parsed, StarSource):
        return StarKey()
    if not isinstance(parsed, (Ref, DottedRef)):
        return None
    try:
        if isinstance(parsed, Ref):
            return _resolve_ref(
                parsed.name, scope=scope, bundle=bundle, alias_map=alias_map,
                measure_ctx=measure_ctx,
            )
        return _resolve_dotted(
            parsed.parts, scope=scope, bundle=bundle, alias_map=alias_map,
            measure_ctx=measure_ctx,
        )
    except UnknownReferenceError as e:
        raise _with_hint(e, hint=_QUOTE_HINT.format(name=e.name)) if parsed.quoted else e


def _bind_cmp(
    parsed: Cmp,
    *,
    sub: Callable[[ParsedExpr], ValueKey],
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]],
    measure_ctx: Optional[MeasureResolutionCtx],
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
) -> ValueKey:
    time_point = _time_point_comparison(parsed, units=bundle.granularities)
    if time_point is not None:
        operand, op, point, literal_on_left = time_point
        return TimePointCmpKey(op=op, point=point, literal_on_left=literal_on_left, operand=sub(operand))
    # ``IN`` / ``NOT IN`` fold into a single ``InKey`` (structured
    # column + literal-tuple handle for the generator).
    if parsed.op in _MEMBERSHIP_OP:
        return _bind_in(
            parsed,
            scope=scope, bundle=bundle, in_filter=in_filter,
            alias_map=alias_map, measure_ctx=measure_ctx,
            dim_alias_map=dim_alias_map,
        )
    return ArithmeticKey(op=parsed.op, operands=(sub(parsed.left), sub(parsed.right)))


_QUOTE_HINT = "{name!r} was written in double quotes, which name a column; strings use single quotes ('…')."


def _with_hint(error: UnknownReferenceError, *, hint: str) -> UnknownReferenceError:
    """``error`` with ``hint`` leading its suggestion."""
    return UnknownReferenceError(
        name=error.name, scope_kind=error.scope_kind, scope_summary=error.scope_summary,
        suggestion=f"{hint} {error.suggestion}" if error.suggestion else hint,
    )


def _temporal_literal_value(parsed: TemporalLiteral):
    """A ``DATE '…'`` / ``TIMESTAMP '…'`` literal's value, checked like an ISO literal in a date position."""
    value = parse_temporal_value(parsed.text)
    if parsed.kind == "date" and isinstance(value, datetime):
        value = None
    if value is None:
        raise TimeLiteralError(
            summary=f"{parsed.kind.upper()} '{parsed.text}' is not a valid {parsed.kind} literal.",
            suggestion="write an ISO time point: DATE 'YYYY-MM-DD' or TIMESTAMP 'YYYY-MM-DD HH:MM[:SS]'.",
        )
    if parsed.kind == "timestamp" and not isinstance(value, datetime):
        value = datetime(value.year, value.month, value.day)
    return value


def _bind_cast(parsed: Cast, *, source: ValueKey, bundle: ResolvedSourceBundle) -> CastKey:
    try:
        build_cast_type(parsed.type_text, dialect=bundle.dialect)
    except ValueError as e:
        raise CastTypeError(summary=str(e), suggestion="cast to a type this database has.") from None
    return CastKey(
        source=source, type_text=parsed.type_text, data_type=cast_data_type(parsed.type_text), try_=parsed.try_,
    )


def _reject_placeholders(parsed: ParsedExpr, *, text: Optional[str] = None) -> None:
    """Raise ``UnresolvedPlaceholderError`` for the first unsubstituted ``{name}`` in ``parsed``."""
    names = placeholder_names(parsed)
    if names:
        raise UnresolvedPlaceholderError(name=names[0], expression=text or canonical_measure_text(parsed))


_OPERAND_LEFT_OP: Dict[str, TimePointOp] = {
    "==": "=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">=",
}
_NOT_IN: Final = "not in"
_MEMBERSHIP_OP: Dict[str, TimePointOp] = {"in": "in", _NOT_IN: _NOT_IN}


def _is_time_point_literal(node: ParsedExpr, *, units: Mapping[str, CustomGranularity]) -> TypeGuard[Literal]:
    return isinstance(node, Literal) and isinstance(node.value, str) and is_time_point(node.value, units=units)


def _time_point_comparison(
    parsed: Cmp, *, units: Mapping[str, CustomGranularity],
) -> Optional[Tuple[ParsedExpr, TimePointOp, str, bool]]:
    """``(operand, operand-left op, point, literal_on_left)`` when ``parsed`` compares a
    non-literal with a time-point string (or is a single-string ``in``), else ``None``."""
    left, right = parsed.left, parsed.right
    if parsed.op in _MEMBERSHIP_OP:
        if isinstance(right, Literal) and isinstance(right.value, str):
            return left, _MEMBERSHIP_OP[parsed.op], right.value, False
        return None
    op = _OPERAND_LEFT_OP.get(parsed.op)
    if op is None:
        return None
    if _is_time_point_literal(right, units=units) and isinstance(right.value, str) and not isinstance(left, Literal):
        return left, op, right.value, False
    if _is_time_point_literal(left, units=units) and isinstance(left.value, str) and not isinstance(right, Literal):
        return right, MIRRORED_TIME_POINT_OP[op], left.value, True
    return None


def _bind_in(
    parsed: Cmp,
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> InKey:
    """Bind an ``IN`` / ``NOT IN`` predicate into an ``InKey``.

    LHS binds through the normal column-resolution path; RHS is a ``TupleLit``
    of ``Literal`` nodes, each bound to a ``LiteralKey`` after normalization."""
    if not isinstance(parsed.right, TupleLit):
        # Defensive: the parser guarantees a TupleLit RHS for in / not in.
        raise ValueError(
            f"_bind_in: expected TupleLit on RHS of {parsed.op!r}, got "
            f"{type(parsed.right).__name__}."
        )
    column = _bind(
        parsed.left,
        scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map,
        measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
    )
    values = tuple(
        LiteralKey(value=normalize_scalar(elt.value))
        for elt in parsed.right.elements
    )
    # SQL three-valued logic makes NULL in the list a silent trap:
    # ``col NOT IN (a, NULL)`` is NULL for every row (zero rows returned).
    if any(v.value is None for v in values):
        raise ValueError(
            f"NULL is not allowed inside an {parsed.op!r} list: SQL compares "
            f"it by three-valued logic, so 'not in' with a NULL matches NO "
            f"rows at all. Test for null separately — e.g. "
            f"`col is null` / `col is not null` — combined with the "
            f"{parsed.op!r} over the non-null values."
        )
    return InKey(
        column=column,
        values=values,
        negated=(parsed.op == _NOT_IN),
    )


def _name_suggestion(*, name: str, model: "SlayerModel") -> str | None:
    """A ``Did you mean 'X'?`` clause over the model's columns + named measures."""
    known = sorted(
        {c.name for c in model.columns}
        | {m.name for m in model.measures if m.name is not None}
    )
    match = difflib.get_close_matches(word=name, possibilities=known, n=1)
    return f"Did you mean '{match[0]}'?" if match else None


def _resolve_ref(
    name: str,
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
) -> ValueKey:
    """Resolve a bare identifier against the scope.

    Resolution order: declared alias → column → saved measure. A saved-measure
    name (no matching column) resolves inline only when ``measure_ctx`` is set
    (eligible position), else errors."""
    if alias_map and name in alias_map:
        return alias_map[name]

    if isinstance(scope, StageSchema):
        col = scope.resolve_flat_name(name)
        if col is None:
            raise UnknownReferenceError(
                name=name,
                scope_kind="StageSchema",
                scope_summary=(
                    f"{scope.label} columns: "
                    f"{[c.name for c in scope.columns]}"
                ),
                suggestion=None,
            )
        return ColumnKey(path=(), leaf=col.name)

    assert isinstance(scope, ModelScope)
    if scope.source_model is None:
        raise UnknownReferenceError(
            name=name,
            scope_kind="ModelScope",
            scope_summary=_NO_ANCHOR_SCOPE_SUMMARY,
            suggestion=None,
        )
    model = scope.source_model

    # A ``__``-bearing name is not special: it resolves by ordinary exact-match.
    col = resolve_generated_column(model=model, name=name)
    if col is not None:
        if col.needs_expansion:
            return ColumnSqlKey(path=(), model=model.name, column_name=col.name)
        return ColumnKey(path=(), leaf=col.name)

    # No column: a saved measure resolves inline in an eligible position.
    mm = model.get_measure(name)
    if mm is not None:
        return _resolve_saved_measure(
            measure=mm, terminal_model=model, scope=scope, bundle=bundle,
            measure_ctx=measure_ctx, ref_text=name, host_path=(),
        )
    raise UnknownReferenceError(
        name=name,
        scope_kind="ModelScope",
        scope_summary=(
            f"model {model.name!r} columns: "
            f"{[c.name for c in model.columns]}"
        ),
        suggestion=_name_suggestion(name=name, model=model),
    )


def _walk_join_chain(
    *,
    hop_path: Tuple[str, ...],
    host,
    bundle: ResolvedSourceBundle,
    parts: Tuple[str, ...],
    leaf: str,
    original_parts: Optional[Tuple[str, ...]] = None,
):
    """Walk ``hop_path`` join hops from ``host`` through the shared bidirectional
    walker; returns ``(terminal_model, canonical effective_hop_path)``. Each token
    resolves as an edge name then a neighbour model, in either orientation.

    When a token resolves to no incident edge, a bare ``Target`` short form
    (``len(hop_path) == 1``) auto-routes to its full datasource-scoped path
    — the effective path is the routed one; a ``len >= 2`` chain is a
    broken chain, rejected (never silently repaired) with a short-form suggestion
    when the target is uniquely routable. ``AmbiguousJoinPathError`` from a
    parallel-pair hop propagates untouched; an edge that resolves onto a target
    absent from the bundle stays ``UnknownReferenceError``. ``parts`` is the full
    dotted ref, for error messages; ``original_parts`` (pre-self-prefix-strip,
    defaulting to ``parts``) spells the circular error's reference in full."""
    models_by_name = bundle.models_by_name
    models_by_name.setdefault(host.name, host)
    spelled = parts if original_parts is None else original_parts
    try:
        walked = walk_terminal(root=host, path=hop_path, models_by_name=models_by_name)
    except CircularJoinPathError as e:
        raise CircularJoinPathError(
            reference=".".join(spelled), root_model=host.name,
            revisited=e.revisited, hop=e.hop, via=e.via,
        ) from None
    if walked is not None:
        return walked
    miss = walk_miss(root=host, path=hop_path, models_by_name=models_by_name)
    assert miss is not None
    if miss.kind == "unloaded":
        assert miss.target is not None
        raise _target_not_in_bundle(parts=parts, target=miss.target)
    _reject_opaque_elements(host=host, hop_path=hop_path, index=miss.index, parts=parts, models_by_name=models_by_name)
    return _route_edgeless_hop(
        host=host, hop=miss.token, hop_path=hop_path, leaf=leaf, parts=parts,
        models_by_name=models_by_name,
    )


def _reject_opaque_elements(
    *, host: SlayerModel, hop_path: Tuple[str, ...], index: int, parts: Tuple[str, ...],
    models_by_name: Dict[str, SlayerModel],
) -> None:
    """A hop naming a list of opaque elements: they have no element access."""
    owner = resolved_terminal(root=host, path=hop_path[:index], models_by_name=models_by_name)
    column = owner.get_column(hop_path[index]) if owner is not None else None
    if column is not None and column.type is DataType.ARRAY:
        raise UnknownReferenceError(
            name=".".join(parts), scope_kind="ModelScope",
            scope_summary=f"list column {column.name!r} holds opaque (UNKNOWN) elements, which have no element access",
            suggestion="Declare the column's element_type, or use length() / IS NULL on the list.",
        )


def _route_edgeless_hop(
    *,
    host: SlayerModel,
    hop: str,
    hop_path: Tuple[str, ...],
    leaf: str,
    parts: Tuple[str, ...],
    models_by_name: Dict[str, SlayerModel],
) -> Tuple[SlayerModel, Tuple[str, ...]]:
    """A hop with no incident edge: a bare ``Target`` short form auto-routes to
    its datasource-scoped path; a longer chain is broken."""
    if len(hop_path) != 1:
        raise _broken_chain_error(
            host=host, hop_path=hop_path, leaf=leaf, parts=parts,
            models_by_name=models_by_name,
        )
    route = dimension_routing.route_dotted_target(
        root=host, target_model=hop, leaf=leaf, models_by_name=models_by_name,
    )
    terminal = models_by_name.get(hop)
    if terminal is None:
        raise _target_not_in_bundle(parts=parts, target=hop)
    chain = walk(root=host, path=tuple(route), models_by_name=models_by_name)
    assert chain is not None  # a safe route walks by construction
    return terminal, canonical_path(chain)


def _target_not_in_bundle(*, parts: Tuple[str, ...], target: str) -> UnknownReferenceError:
    return UnknownReferenceError(
        name=".".join(parts), scope_kind="ModelScope",
        scope_summary=f"target {target!r} not in source bundle", suggestion=None,
    )


def _broken_chain_error(
    *,
    host: SlayerModel,
    hop_path: Tuple[str, ...],
    leaf: str,
    parts: Tuple[str, ...],
    models_by_name: Dict[str, SlayerModel],
) -> UnresolvableDimensionJoinError:
    """A multi-hop dotted chain with an unresolvable hop — rejected, never
    auto-repaired. Suggests the short form ``Target.leaf`` when its target
    (``hop_path[-1]``) is itself uniquely routable, else no suggestion."""
    target = hop_path[-1]
    routable = dimension_routing.short_form_route_or_none(
        root=host, target_model=target, models_by_name=models_by_name,
    )
    return UnresolvableDimensionJoinError(
        reference=".".join(parts),
        root_model=host.name,
        reason=f"'{'.'.join(hop_path)}' is not a valid join chain",
        suggested_path=f"{target}.{leaf}" if routable is not None else None,
    )


def _strip_self_prefix(
    parts: Tuple[str, ...], *, host: SlayerModel
) -> Tuple[str, ...]:
    """Drop a leading same-model self-prefix (``orders.orders.x`` → ``orders.x``)."""
    remainder = parts[1:]
    if not remainder:
        raise UnknownReferenceError(
            name=host.name,
            scope_kind="ModelScope",
            scope_summary=f"model {host.name!r}",
            suggestion="self-prefix only — expected a column or join target.",
        )
    return remainder


def _resolve_dotted(
    parts: Tuple[str, ...],
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
) -> ValueKey:
    """Resolve a dotted ref against the scope.

    Resolution order: declared alias (full dotted text) → join walk → leaf
    column → saved measure (re-anchored into host coords, eligible position)."""
    if alias_map:
        dotted_text = ".".join(parts)
        if dotted_text in alias_map:
            return alias_map[dotted_text]

    if isinstance(scope, StageSchema):
        scope = _stage_list_scope(parts, scope=scope, bundle=bundle, leaf=LIST_ELEMENT_COLUMN)

    assert isinstance(scope, ModelScope)
    if scope.source_model is None:
        raise UnknownReferenceError(
            name=".".join(parts),
            scope_kind="ModelScope",
            scope_summary=_NO_ANCHOR_SCOPE_SUMMARY,
            suggestion=None,
        )

    # C14: strip same-model self-prefix, then resolve any single local ref.
    host = scope.source_model
    original_parts = parts
    if parts and parts[0] == host.name:
        parts = _strip_self_prefix(parts, host=host)

    if len(parts) == 1:
        return _resolve_ref(
            parts[0], scope=scope, bundle=bundle, alias_map=alias_map,
            measure_ctx=measure_ctx,
        )

    # parts[:-1] are join targets; parts[-1] is the leaf column.
    hop_path = parts[:-1]
    leaf = parts[-1]
    current, effective_hop_path = _walk_join_chain(
        hop_path=hop_path, host=host, bundle=bundle, parts=parts, leaf=leaf,
        original_parts=original_parts,
    )

    return _resolve_terminal_leaf(
        current=current, leaf=leaf, hop_path=effective_hop_path,
        original_parts=original_parts, scope=scope, bundle=bundle,
        measure_ctx=measure_ctx,
    )


def _stage_list_scope(
    parts: Tuple[str, ...], *, scope: StageSchema, bundle: ResolvedSourceBundle, leaf: str,
) -> ModelScope:
    """The stage's model scope for ``L.value`` / ``L.*`` over its list column ``L``; any other dotted ref is illegal."""
    head = scope.resolve_flat_name(parts[0]) if len(parts) == 2 and parts[1] == leaf else None
    if head is None or head.type != DataType.ARRAY or bundle.source_model is None:
        raise IllegalScopeReferenceError(
            name=".".join(parts),
            scope_kind="StageSchema",
            reason=(
                "downstream stages see a flat schema — dotted refs are not legal "
                "bar a list column's elements (L.value, L.*). Use the flat column name."
            ),
        )
    return ModelScope(source_model=bundle.source_model)


def _resolve_terminal_leaf(
    *,
    current: SlayerModel,
    leaf: str,
    hop_path: Tuple[str, ...],
    original_parts: Tuple[str, ...],
    scope: ModelScope,
    bundle: ResolvedSourceBundle,
    measure_ctx: Optional[MeasureResolutionCtx],
) -> ValueKey:
    """Resolve ``leaf`` on terminal model ``current``: column (plain or derived)
    → saved measure (re-anchored into host coords) → unresolved error."""
    col = resolve_generated_column(model=current, name=leaf)
    if col is not None:
        if col.needs_expansion:
            # Derived / filtered column on a joined model — path is part of the
            # key so the cross-model planner can route via the join graph.
            return ColumnSqlKey(
                path=tuple(hop_path), model=current.name, column_name=col.name,
            )
        return ColumnKey(path=tuple(hop_path), leaf=col.name)
    mm = current.get_measure(leaf)
    if mm is not None:
        return _resolve_saved_measure(
            measure=mm, terminal_model=current, scope=scope, bundle=bundle,
            measure_ctx=measure_ctx, ref_text=".".join(original_parts),
            host_path=tuple(hop_path),
        )
    raise _unresolved_dotted_error(parts=original_parts, terminal_model=current)


_MEASURE_LEGAL_POSITIONS = (
    "saved measures may be referenced only in a measure formula or a "
    "computed dimension expression"
)


def _ineligible_saved_measure_error(
    *, ref_text: str, model_name: str,
) -> UnknownReferenceError:
    """Error: a saved-measure reference where measures are not legal."""
    return UnknownReferenceError(
        name=ref_text,
        scope_kind="ModelScope",
        scope_summary=f"model {model_name!r}",
        suggestion=(
            f"{ref_text!r} is a saved measure on {model_name!r}, not a column, "
            f"so it takes no aggregation. {_MEASURE_LEGAL_POSITIONS}; reference "
            f"it there as {ref_text!r}."
        ),
    )


def _unresolved_dotted_error(
    *, parts: Tuple[str, ...], terminal_model: SlayerModel,
) -> UnknownReferenceError:
    """Error: a dotted leaf matching neither column nor saved measure; names
    both namespaces and offers close matches."""
    dotted = ".".join(parts)
    columns = [c.name for c in terminal_model.columns]
    measures = [m.name for m in terminal_model.measures if m.name]
    detail = (
        f"{dotted!r} is neither a column nor a saved measure on "
        f"{terminal_model.name!r}. Columns: {columns}; saved measures: {measures}."
    )
    match = _name_suggestion(name=parts[-1], model=terminal_model)
    if match:
        detail = f"{detail} {match}"
    return UnknownReferenceError(
        name=dotted,
        scope_kind="ModelScope",
        scope_summary=f"model {terminal_model.name!r}",
        suggestion=detail,
    )


def _rerooted_bundle(
    *, bundle: ResolvedSourceBundle, target: SlayerModel,
) -> ResolvedSourceBundle:
    """A copy of ``bundle`` re-rooted at ``target`` (same referenced models), so
    a measure formula binds its filters / gates against the owning model."""
    others = [m for m in bundle.referenced_models if m.name != target.name]
    return bundle.model_copy(update={
        "source_model": target,
        "referenced_models": [target] + others,
    })


def _resolve_saved_measure(
    *,
    measure,
    terminal_model: SlayerModel,
    scope: ModelScope,
    bundle: ResolvedSourceBundle,
    measure_ctx: Optional[MeasureResolutionCtx],
    ref_text: str,
    host_path: Tuple[str, ...],
) -> ValueKey:
    """Resolve a saved ``ModelMeasure`` into a bound ``ValueKey``. ``host_path``
    is the host→terminal join path (``()`` for bare/local); a cross-model
    measure binds against the target then prepends ``host_path``. Raises the
    ineligible-position error when ``measure_ctx`` is None."""
    if measure_ctx is None:
        raise _ineligible_saved_measure_error(
            ref_text=ref_text, model_name=terminal_model.name,
        )
    step = (terminal_model.name, measure.name)
    if step in measure_ctx.chain:
        raise MeasureCycleError(
            chain=_fmt_measure_chain(measure_ctx.chain + (step,)),
        )
    child = measure_ctx.descend(model=terminal_model.name, measure=measure.name)
    if len(child.chain) > measure_ctx.depth_limit:
        raise MeasureRecursionLimitError(
            chain=_fmt_measure_chain(child.chain), limit=measure_ctx.depth_limit,
        )
    parsed = parse_expr(measure.formula)
    _reject_placeholders(parsed, text=measure.formula)
    if not host_path:
        # Bare/local: measure lives on the host; bind inline at this scope.
        return _bind(
            parsed, scope=scope, bundle=bundle, in_filter=False, measure_ctx=child,
        )
    # Cross-model: bind against the target, then prepend into host coordinates.
    host_model = scope.source_model
    assert host_model is not None
    target_scope = ModelScope(source_model=terminal_model)
    target_bundle = _rerooted_bundle(bundle=bundle, target=terminal_model)
    bound = _bind(
        parsed, scope=target_scope, bundle=target_bundle, in_filter=False,
        measure_ctx=child,
    )
    return prepend_value_key(bound, host_path=host_path)


def _resolve_dotted_star(
    parts: Tuple[str, ...],
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
) -> StarKey:
    """Resolve a dotted star (``customers.*``) to a ``StarKey`` whose ``path`` is
    the validated hop chain (empty path = local star)."""
    assert parts and parts[-1] == "*"
    if isinstance(scope, StageSchema):
        scope = _stage_list_scope(parts, scope=scope, bundle=bundle, leaf="*")
    assert isinstance(scope, ModelScope)
    host = scope.source_model
    if host is None:
        raise UnknownReferenceError(
            name=".".join(parts),
            scope_kind="ModelScope",
            scope_summary=_NO_ANCHOR_SCOPE_SUMMARY,
            suggestion=None,
        )
    hop_path = parts[:-1]
    # Strip same-model self-prefix (``orders.*`` on ``orders``).
    if hop_path and hop_path[0] == host.name:
        hop_path = hop_path[1:]
    # Validate the hop chain (raises on missing / circular join, auto-routes a
    # short form); the routed effective path carries the star.
    _, effective_hop_path = _walk_join_chain(
        hop_path=hop_path, host=host, bundle=bundle, parts=parts, leaf="*",
    )
    return StarKey(path=tuple(effective_hop_path))


def _bind_partition_keys(
    value, *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
    label: str,
) -> Grain:
    """Bind a ``partition_by`` value (aggregation or rank-family transform) to its ``Grain``."""
    elements = value if isinstance(value, tuple) else (value,)
    pks: List = []
    for elem in elements:
        if dim_alias_map and isinstance(elem, Ref) and elem.name in dim_alias_map:
            pks.append(dim_alias_map[elem.name])
            continue
        bound = _bind(parsed=elem, scope=scope, bundle=bundle, in_filter=False)
        # Whether a key is a query dimension is the checker's; an aggregate-bearing one must already be one.
        if _carries_attached_value(bound) and bound not in (dim_alias_map or {}).values():
            raise PartitionKeyError(
                summary=f"The partition_by key '{key_display(bound)}' carries an aggregate or transform.",
                location=f"{label} partition_by",
                suggestion="Declare it as a computed dimension of the query and name it in partition_by=.",
            )
        pks.append(bound)
    return Grain.of(pks)


def _carries_attached_value(key: ValueKey) -> bool:
    return isinstance(key, SqlFragmentKey) or any(
        isinstance(k, (AggregateKey, TransformKey)) for k in walk_value_keys(key)
    )


def _bind_expression_agg_source(
    parsed_source: ParsedExpr, *,
    agg: str,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
) -> ArithmeticKey | ScalarCallKey | LiteralKey | TimeTruncKey | InKey | TimePointCmpKey | CastKey:
    """Bind a scalar-expression aggregate source.

    Dotted joined-model leaves and operands carrying ``Column.filter`` are both
    admitted: the home rule roots the aggregation and the filter
    desugars to ``CASE WHEN``. The source must still resolve to a row-level
    expression (a column, star, or arithmetic/scalar/predicate composite of them)."""
    bound = _bind(parsed_source, scope=scope, bundle=bundle, in_filter=False)
    if not isinstance(bound, EXPRESSION_SOURCE_KINDS):
        raise _not_an_aggregation_source(bound, agg=agg)
    return bound


def _not_an_aggregation_source(bound: ValueKey, *, agg: str) -> AggregationNotAllowedError:
    return AggregationNotAllowedError(
        column=key_display(bound), agg=agg,
        reason=f"an aggregation source must be a column, star, or a row-level expression; "
        f"got {type(bound).__name__}.",
    )


# Null-handling / min-max scalars whose result class follows their arguments.
_ARG_CLASS_SCALARS = frozenset({
    "coalesce", "ifnull", "nullif", "greatest", "least",
})
def _expression_is_confidently_text(key, *, model: Optional[SlayerModel]) -> bool:
    """Best-effort: True only when the expression's value is certainly text."""
    if isinstance(key, str):
        return True
    if isinstance(key, ScalarCallKey):
        if key.name in TEXT_RESULT_SCALARS:
            return True
        if key.name in _ARG_CLASS_SCALARS:
            return any(
                _expression_is_confidently_text(a, model=model)
                for a in key.args
            )
        return False
    if isinstance(key, LiteralKey):
        return isinstance(key.value, str)
    if isinstance(key, ColumnKey) and not key.path and model is not None:
        col = model.get_column(key.leaf)
        return col is not None and col.type == DataType.TEXT
    if isinstance(key, ColumnSqlKey) and not key.path and model is not None:
        col = model.get_column(key.column_name)
        return col is not None and col.type == DataType.TEXT
    # Arithmetic coerces numeric; anything unresolved defaults to numeric.
    return False


def _reject_non_numeric_expression_agg(
    *, source: ValueKey, agg: str,
    scope: ModelScope | StageSchema, bundle: ResolvedSourceBundle,
) -> None:
    """A numeric-only aggregation over a text or temporal expression fails here, not in the database."""
    if agg not in NUMERIC_ONLY_AGGREGATIONS:
        return
    column_type = scope_column_type(scope=scope, bundle=bundle)
    if boolean_valued(source, column_type=column_type):
        return
    if (temporal := temporal_type(source, column_type=column_type)) is not None:
        kind = temporal.value
    elif _expression_is_confidently_text(
        source, model=scope.source_model if isinstance(scope, ModelScope) else None,
    ):
        kind = "text"
    else:
        return
    remedy = " (numbers stored as text: TRY_CAST(x AS DOUBLE))" if kind == "text" else ""
    raise AggregationNotAllowedError(
        column=key_display(source), agg=agg,
        reason=f"{agg!r} requires a numeric value, but the aggregated expression is non-numeric ({kind}). Use a "
        f"counting or min/max aggregation, or make the expression numeric{remedy}.",
    )


def _callee_granularity(name: str, *, bundle: Optional[ResolvedSourceBundle]) -> Optional[Granularity]:
    """The granularity a call's callee names (built-in or the datasource's), else ``None``."""
    return resolve_granularity(name, defined=bundle.granularities if bundle is not None else {})


def _source_is_reaggregation(node, *, bundle: Optional[ResolvedSourceBundle] = None) -> bool:
    """Whether a parsed aggregation source carries an attached value — a nested
    AggCall or a grained transform, alone or composed. Such a source is bound
    structurally (its inner AggCalls / TransformCalls become nested keys) whether
    it is a pure re-aggregation or a row-grain mix."""
    if isinstance(node, AggCall) and _callee_granularity(node.agg, bundle=bundle) is not None:
        return False  # a granularity call is row-level
    if isinstance(node, (AggCall, TransformCall)):
        return True
    if isinstance(node, (Arith, Cmp)):
        return _source_is_reaggregation(node.left, bundle=bundle) or _source_is_reaggregation(node.right, bundle=bundle)
    if isinstance(node, ScalarCall):
        return any(_source_is_reaggregation(a, bundle=bundle) for a in node.args)
    if isinstance(node, UnaryOp):
        return _source_is_reaggregation(node.operand, bundle=bundle)
    if isinstance(node, BoolOp):
        return any(_source_is_reaggregation(o, bundle=bundle) for o in node.operands)
    return False


def _bind_agg_call(
    parsed: AggCall, *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> AggregateKey | TransformKey | TimeTruncKey:
    """Bind an ``AggCall`` (a granularity callee is a row-level bucket); ``first`` / ``last`` dispatch by the bound operand's
    type — attached → the series transform, row grain → the ranked aggregation."""
    if parsed.filter is not None:
        return _bind_filtered_agg(
            parsed, scope=scope, bundle=bundle, alias_map=alias_map, measure_ctx=measure_ctx,
            dim_alias_map=dim_alias_map,
        )
    granularity = _callee_granularity(parsed.agg, bundle=bundle)
    if granularity is not None:
        return _bind_granularity_call(parsed, scope=scope, bundle=bundle, granularity=granularity)
    op = normalize_aggregation_name(parsed.agg)
    if op in RANKED_AGGREGATIONS:
        operand = _bind_transform_input(
            parsed.source, scope=scope, bundle=bundle, alias_map=alias_map,
            measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
        )
        if is_attached_source(operand):
            return _bind_transform_params(
                op=op, inp=operand, args=parsed.args, kwargs=parsed.kwargs,
                scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
            )
    return _bind_agg(parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map)


_FILTER_CLAUSE = "FILTER (WHERE …)"


def _filter_error(reason: str) -> AggregationFilterError:
    return AggregationFilterError(
        summary=f"{_FILTER_CLAUSE} {reason}",
        location=_FILTER_CLAUSE,
        suggestion="filter on columns of the aggregated rows, e.g. sum(amount) FILTER (WHERE status = 'paid').",
    )


def _bind_filtered_agg(
    parsed: AggCall, *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> AggregateKey:
    """``agg(…) FILTER (WHERE c)``: the aggregation over the rows where ``c`` holds. Every row-level
    input is masked by ``c``; a star source and a ranked pick carry ``c`` on the key instead."""
    condition = _bind(parsed.filter, scope=scope, bundle=bundle, in_filter=False, dim_alias_map=dim_alias_map)
    if any(isinstance(k, (AggregateKey, TransformKey)) for k in walk_value_keys(condition)):
        raise _filter_error("needs a row-level condition; it cannot read an aggregate, a transform or a measure.")
    key = _bind_agg_call(
        parsed.model_copy(update={"filter": None}), scope=scope, bundle=bundle, alias_map=alias_map,
        measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
    )
    if not isinstance(key, AggregateKey) or is_attached_source(key.source):
        raise _filter_error("applies only to an aggregation over rows.")
    if key.agg in RANKED_AGGREGATIONS:
        return _carrying_filter(key, condition=condition)
    value = LiteralKey(value=normalize_scalar(1)) if isinstance(key.source, StarKey) else key.source
    masked = key.model_copy(update={
        "source": _masked(value, condition=condition),
        "kwargs": tuple((k, _masked(v, condition=condition) if isinstance(v, _ROW_INPUT_KINDS) else v)
                        for k, v in key.kwargs),
    })
    if source_anchor_path(masked.source) == source_anchor_path(key.source):
        return masked
    if isinstance(key.source, StarKey) and not source_anchor_path(key.source):
        # The condition reads a joined model: the star keeps the count's home.
        return _carrying_filter(key, condition=condition)
    raise _filter_error("must read the aggregated rows' own model or models joined to it one-to-one.")


def _carrying_filter(key: AggregateKey, *, condition: ValueKey) -> AggregateKey:
    if window_kwarg_of(key) is not None:
        raise _filter_error("cannot combine with window= here; filter the query's rows instead.")
    return key.model_copy(update={"filter": condition})


# Row-valued aggregation parameters a FILTER masks (literals and attached values are not rows).
_ROW_INPUT_KINDS = (ColumnKey, ColumnSqlKey, SqlFragmentKey)


def _masked(value: ValueKey, *, condition: ValueKey) -> ScalarCallKey:
    return ScalarCallKey(name="iif", args=(condition, value, LiteralKey(value=None)))


def _bind_granularity_call(
    parsed: AggCall, *, scope: ModelScope | StageSchema, bundle: ResolvedSourceBundle,
    granularity: Granularity,
) -> TimeTruncKey:
    """``gran(expr)`` as the row-level bucket of ``expr``."""
    if parsed.args or parsed.kwargs or isinstance(parsed.source, StarSource):
        raise GranularityCallError.wrong_shape(f"{parsed.agg}(...)")
    source = _bind(parsed.source, scope=scope, bundle=bundle, in_filter=False)
    if source.phase is not Phase.ROW or isinstance(source, SqlFragmentKey) or any(
        isinstance(k, (AggregateKey, TransformKey)) for k in walk_value_keys(source)
    ):
        raise GranularityCallError.wrong_shape(f"{parsed.agg}(...)")
    return TimeTruncKey(source=source, granularity=granularity)


def _bind_agg_source(
    parsed: AggCall, *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
) -> AggregateSource:
    if _source_is_reaggregation(parsed.source, bundle=bundle):
        # Re-aggregation: bind the operand subtree — inner AggCalls
        # become AggregateKeys — so the outer key carries a nested-aggregate
        # source (axiom 6). Discovery/planning lift it to a producer-over-producer.
        bound = _bind(
            parsed.source, scope=scope, bundle=bundle, in_filter=False,
            dim_alias_map=dim_alias_map,
        )
        if isinstance(bound, (SqlFragmentKey, ArrayLiteralKey)):
            raise _not_an_aggregation_source(bound, agg=parsed.agg)
        return bound
    if isinstance(parsed.source, StarSource):
        return StarKey()
    if (
        isinstance(parsed.source, DottedRef)
        and parsed.source.parts
        and parsed.source.parts[-1] == "*"
    ):
        # Cross-model star: ``count(customers.*)`` → a StarKey carrying the join
        # path so the planner routes COUNT(*) through the join graph.
        return _resolve_dotted_star(
            parsed.source.parts, scope=scope, bundle=bundle,
        )
    if isinstance(parsed.source, (Ref, DottedRef)):
        bound_source = _bind(
            parsed.source, scope=scope, bundle=bundle, in_filter=False,
        )
        if not isinstance(bound_source, (ColumnKey, ColumnSqlKey, StarKey)):
            raise ValueError(
                f"Aggregation source must resolve to a column / star, "
                f"got {type(bound_source).__name__}."
            )
        return bound_source
    # Same-model scalar EXPRESSION source (``sum(amount - cost)``).
    return _bind_expression_agg_source(
        parsed.source, agg=parsed.agg, scope=scope, bundle=bundle,
    )


def _bind_agg(
    parsed: AggCall, *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> AggregateKey:
    source = _bind_agg_source(parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map)

    # ``partition_by`` is lifted out of kwargs onto ``partition_keys``
    # (``None`` means no partition, ``[]`` means grand total).
    # Gate per-column aggregation eligibility, then store the EFFECTIVE
    # (alias-healed) name so the generator resolves the canonical aggregation.
    effective_agg = _validate_agg_eligibility(
        source=source, agg=parsed.agg, bundle=bundle,
    )
    # Names first: no argument value binds before its name is accepted.
    folded = _declared_agg_param_names(agg=effective_agg, source=source, bundle=bundle)[:len(parsed.args)]
    _check_agg_kwarg_names(
        agg=effective_agg, source=source, bundle=bundle,
        names=[*folded, *(k for k, _ in parsed.kwargs if k != "partition_by")],
        keywords=frozenset(k for k, _ in parsed.kwargs),
    )
    args = tuple(
        _bind_agg_arg(a, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map)
        for a in parsed.args
    )
    partition_keys: Optional[Grain] = None
    kwargs_list: List = []
    for k, v in parsed.kwargs:
        if k == "partition_by":
            partition_keys = _bind_partition_keys(
                value=v, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
                label="aggregation",
            )
            continue
        kwargs_list.append((
            k, _bind_agg_arg(v, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map),
        ))
    kwargs = tuple(kwargs_list)
    args, kwargs = _fold_positional_agg_args(
        agg=effective_agg, source=source, bundle=bundle, args=args, kwargs=kwargs,
    )
    # Numeric-only aggregations over an expression source are rejected when it is
    # confidently non-numeric (per-column gates don't apply).
    if isinstance(source, EXPRESSION_SOURCE_KINDS):
        _reject_non_numeric_expression_agg(
            source=source, agg=effective_agg, scope=scope, bundle=bundle,
        )
    kwargs = bind_aggregation_params(
        agg=effective_agg, source=source, kwargs=kwargs, bundle=bundle,
        resolve_query_ref=lambda parts: _bind(
            Ref(name=parts[0]) if len(parts) == 1 else DottedRef(parts=parts),
            scope=scope, bundle=bundle, in_filter=False,
        ),
    )
    return AggregateKey(
        source=source,
        agg=effective_agg,
        args=args,
        kwargs=kwargs,
        partition_keys=partition_keys,
    )


def _resolve_agg_owner(
    source, bundle: ResolvedSourceBundle,
) -> "tuple[Optional[SlayerModel], Optional[str]]":
    """``(owning_model, gate_leaf)`` for an aggregate source; ``(None, None)`` when
    the owner can't be confirmed (no host model, unresolved join hop)."""
    owner = aggregation_owner(
        root=bundle.source_model, source=source, models_by_name=bundle.models_by_name,
    )
    if owner is None:
        return None, None
    return owner, getattr(source, "leaf", None) or getattr(source, "column_name", None)


def _declared_agg_param_names(
    *, agg: str, source, bundle: ResolvedSourceBundle,
) -> List[str]:
    """Declared parameter order for ``agg`` — the owning model's custom
    definition wins over the built-in registry; ``[]`` when none declared."""
    owner, _leaf = _resolve_agg_owner(source, bundle)
    custom = aggregation_definition(owner=owner, agg=agg)
    if custom is not None:
        return [p.name for p in custom.params]
    return list(BUILTIN_AGGREGATION_PARAM_ORDER.get(agg, ()))


def _check_agg_kwarg_names(
    *, agg: str, source, bundle: ResolvedSourceBundle, names: List[str], keywords: frozenset[str],
) -> None:
    """Reject an argument name the rendered ``agg`` never reads (only its placeholders or own parameters, plus ``window``)."""
    if not names:
        return
    owner, _leaf = _resolve_agg_owner(source, bundle)
    definition = aggregation_definition(owner=owner, agg=agg)
    if definition is None and agg not in BUILTIN_AGGREGATIONS:
        return  # unresolved custom aggregation: eligibility / render reports it
    try:
        reads = aggregation_reads(agg=agg, definition=definition, dialect=bundle.dialect)
    except SqlTemplateError as e:
        raise SqlTemplateError(f"Aggregation '{agg}': {e}") from e
    accepted = (reads - {VALUE_PLACEHOLDER}) | {"window"}
    for name in names:
        if name == VALUE_PLACEHOLDER and VALUE_PLACEHOLDER in reads:
            raise SqlTemplateError(reserved_value_param_message(agg))
        if name in accepted:
            continue
        hint = f" Note: {comparison_hint(name)}" if name in keywords else ""
        if accepted == {"window"}:
            raise AggregationArgumentError(
                f"Aggregation '{agg}' takes no args or kwargs other than window; got '{name}'.{hint}"
            )
        raise AggregationArgumentError(
            f"Aggregation '{agg}' does not accept argument '{name}'; "
            f"accepted: {', '.join(sorted(accepted))}.{hint}"
        )


def _fold_positional_agg_args(
    *, agg: str, source, bundle: ResolvedSourceBundle, args: tuple, kwargs: tuple,
) -> "tuple[tuple, tuple]":
    """Fold positional call values onto declared parameter names, Python-call
    style, so ``percentile(x, 0.9)`` interns identically to ``p=0.9``. Ranked
    ``first``/``last`` declare no parameters — their optional positional ranking
    column stays in ``args``; any other parameterless aggregation takes no
    positional, so an attached (aggregate-valued) parameter is always a kwarg
    after binding."""
    if not args:
        return args, kwargs
    names = _declared_agg_param_names(agg=agg, source=source, bundle=bundle)
    if not names:
        if agg not in RANKED_AGGREGATIONS:
            raise ValueError(
                f"Aggregation {agg!r} takes no parameters; got {len(args)} "
                f"positional value(s)."
            )
        if len(args) != 1:
            raise ValueError(
                f"Aggregation {agg!r} ranks by at most one column or row-level expression; got "
                f"{len(args)} positional value(s)."
            )
        if not _is_row_level_ranking_key(args[0]):
            raise ValueError(
                f"Aggregation {agg!r} ranks by a column or a row-level expression of columns; got "
                f"{key_display(args[0]) if isinstance(args[0], VALUE_KEY_TYPES) else repr(args[0])} "
                f"as its ranking key."
            )
        return args, kwargs
    if len(args) > len(names):
        raise ValueError(
            f"Aggregation {agg!r} takes at most {len(names)} parameter(s) "
            f"({', '.join(names)}); got {len(args)} positional value(s)."
        )
    given = {k for k, _ in kwargs}
    folded = list(kwargs)
    for name, value in zip(names, args):
        if name in given:
            raise ValueError(
                f"Aggregation {agg!r} got parameter {name!r} both positionally "
                f"and by name."
            )
        folded.append((name, value))
    return (), tuple(folded)


def _is_row_level_ranking_key(key: object) -> bool:
    """A ranking key reads at least one column and no aggregate, transform or attached value."""
    return isinstance(key, VALUE_KEY_TYPES) and not _carries_attached_value(cast(ValueKey, key)) and any(
        isinstance(k, (ColumnKey, ColumnSqlKey)) for k in walk_value_keys(cast(ValueKey, key))
    )


def _unknown_function_error(name: str, *, aggregations, bundle: ResolvedSourceBundle) -> UnknownFunctionError:
    """Unknown callable ``name``: the callable categories and a close match."""
    granularities = sorted(GRANULARITY_NAMES | set(bundle.granularities))
    callable_names = [*aggregations, *SCALAR_FUNCTIONS, *DATE_PART_ALIASES, *ALL_TRANSFORMS, *granularities]
    match = difflib.get_close_matches(word=name, possibilities=sorted(set(callable_names)), n=1)
    hint = f" Did you mean '{match[0]}'?" if match else ""
    if name.lower() in AMBIGUOUS_WEEKDAY_NAMES:
        suggestion = ambiguous_weekday_message(name)
    else:
        suggestion = (
            f"Known: aggregations {sorted(aggregations)}, scalar functions "
            f"{sorted(SCALAR_FUNCTIONS)}, transforms {sorted(ALL_TRANSFORMS)}, time "
            f"granularities {granularities}."
        )
    return UnknownFunctionError(
        name=name, location=None, summary=f"Unknown function '{name}'.{hint}", suggestion=suggestion,
    )


def _validate_agg_eligibility(
    *, source, agg: str, bundle: ResolvedSourceBundle,
) -> str:
    """Heal the aggregation name, validate it globally, and enforce per-column
    eligibility gates, returning the effective (alias-healed) name for
    ``AggregateKey.agg``.

    Healing is skipped when the raw token exactly matches a custom aggregation
    on the owning model (a custom ``countd`` wins over the alias). Gate order:
    0. unknown-name-first, for EVERY source shape (column, star, expression),
    so ``bogus(*)`` never escapes to SQL generation;
    1. a sole primary key (an identifier) restricted to
    ``PRIMARY_KEY_AGGREGATIONS`` — composite-key members fall through; 2. explicit
    ``Column.allowed_aggregations`` whitelist; 3. else
    ``DEFAULT_AGGREGATIONS_BY_TYPE`` (custom aggregations exempt).

    Star and expression sources have no column, so the per-column gates (1-3)
    don't apply (gate 0 still validates the name); unresolvable targets pass
    through (best-effort).
    """
    owner_model, leaf = _resolve_agg_owner(source, bundle)
    if owner_model is None:
        return _known_aggregation(normalize_aggregation_name(agg), BUILTIN_AGGREGATIONS, bundle=bundle)
    # Alias healing — custom aggregation named like an alias wins.
    custom_names = {a.name for a in (owner_model.aggregations or [])}
    effective = agg if agg in custom_names else normalize_aggregation_name(agg)
    # Gate 0: unknown-name-first (precedence over PK / whitelist / type).
    _known_aggregation(effective, BUILTIN_AGGREGATIONS | custom_names, bundle=bundle)
    col = None if leaf is None else next((c for c in owner_model.columns if c.name == leaf), None)
    if col is None:
        return effective
    reason = _column_agg_refusal(
        col=col, columns=owner_model.columns, agg=effective, custom=effective in custom_names,
    )
    if reason is not None:
        raise AggregationNotAllowedError(column=col.name, agg=effective, reason=reason)
    return effective


def _known_aggregation(name: str, known, *, bundle: ResolvedSourceBundle) -> str:
    if name not in known:
        raise _unknown_function_error(name, aggregations=known, bundle=bundle)
    return name


def _column_agg_refusal(*, col, columns, agg: str, custom: bool) -> Optional[str]:
    """Why gates 1-3 refuse ``agg`` on ``col``, else ``None``."""
    leaf = col.name
    if is_identifier(column=col, columns=columns):
        if agg in PRIMARY_KEY_AGGREGATIONS:
            return None
        return f"primary-key column {leaf!r} restricted to {sorted(PRIMARY_KEY_AGGREGATIONS)}; got {agg!r}."
    if col.allowed_aggregations is not None:
        if agg in col.allowed_aggregations:
            return None
        return (f"column {leaf!r} restricts allowed_aggregations to "
                f"{sorted(col.allowed_aggregations)}; got {agg!r}.")
    # Model-custom aggregations are exempt from the type-default gate.
    allowed = DEFAULT_AGGREGATIONS_BY_TYPE.get(col.type, frozenset())
    if custom or agg in allowed:
        return None
    remedy = (
        f" For numbers stored as text, convert them: {agg}(TRY_CAST({leaf} AS DOUBLE))."
        if col.type is DataType.TEXT and agg in NUMERIC_ONLY_AGGREGATIONS else ""
    )
    if col.type is DataType.ARRAY:
        remedy = f" {leaf!r} is a list; aggregate its elements: {agg}({leaf}.value)."
    return (f"aggregation {agg!r} is not applicable to {col.type} column {leaf!r}; "
            f"default aggregations are {sorted(allowed)}.{remedy}")


def _bind_agg_arg(
    parsed: ParsedExpr, *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
):
    """Bind one aggregation arg: a nested aggregate → ``AggregateKey`` (aggregate-valued
    parameter), a grained transform → ``TransformKey`` at its result grain, literals → inline
    scalar via ``normalize_scalar`` (stored inline, not as LiteralKey), anything else as a
    Mode-B expression (a column being the one-leaf case).
    ``dim_alias_map`` rides into a nested aggregate / transform so its
    ``partition_by=`` can name a computed dimension (as the outer aggregate's can)."""
    if isinstance(parsed, Literal):
        return normalize_scalar(parsed.value)
    if isinstance(parsed, AggCall):
        return _bind_agg_call(
            parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
        )
    if isinstance(parsed, TransformCall):
        # A transform param binds like a source constituent (Axiom 2.3 / 11.4): its
        # INPUT only, no alias_map / measure_ctx (a measure is illegal inside an aggregation).
        return _bind_transform(
            parsed=parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
        )
    return _bind(parsed, scope=scope, bundle=bundle, in_filter=False, dim_alias_map=dim_alias_map)


_NOT_SCALAR = object()  # sentinel returned by _fold_to_scalar when the input isn't a literal-resolvable scalar


def _fold_to_scalar(parsed: ParsedExpr):
    """Resolve a parsed expression to a scalar literal if possible.

    Folds ``Literal`` and unary ``-`` over a numeric ``Literal`` (``periods=-1``);
    returns ``_NOT_SCALAR`` otherwise (transform kwargs must be scalar)."""
    if isinstance(parsed, Literal):
        return normalize_scalar(parsed.value)
    if (
        isinstance(parsed, UnaryOp)
        and parsed.op == "-"
        and isinstance(parsed.operand, Literal)
    ):
        inner = parsed.operand.value
        if isinstance(inner, bool):
            # ``-True`` is nonsense; bool is an int subclass, so reject first.
            return _NOT_SCALAR
        if isinstance(inner, (int, float, Decimal)):
            return normalize_scalar(-inner)
    return _NOT_SCALAR


# Per-op kwarg whitelist. ``partition_by`` is handled separately (rank family
# only); every other op takes just the kwargs listed here.
_TRANSFORM_KWARG_RULES: dict = {
    "cumsum": frozenset(),
    "change": frozenset(),
    "change_pct": frozenset(),
    "first": frozenset(),
    "last": frozenset(),
    "time_shift": frozenset({"periods", "granularity"}),
    "lag": frozenset({"periods"}),
    "lead": frozenset({"periods"}),
    "rank": frozenset(),
    "percent_rank": frozenset(),
    "dense_rank": frozenset(),
    "ntile": frozenset({"n"}),
    "consecutive_periods": frozenset(),
}

# Positional-param signature (after the value) mapping the i-th positional onto
# a kwarg; transforms absent here are keyword-only after the value.
_TRANSFORM_POSITIONAL_KWARGS: dict = {
    "time_shift": ("periods", "granularity"),
    "lag": ("periods",),
    "lead": ("periods",),
}


def _transform_positional_pairs(
    *, op: str, args: Tuple[ParsedExpr, ...], kwargs: Tuple[Tuple[str, ParsedExpr], ...],
) -> List:
    """Map a transform's extra positional params onto kwarg names; most transforms
    are keyword-only after the value."""
    if not args:
        return []
    pos_names = _TRANSFORM_POSITIONAL_KWARGS.get(op)
    if pos_names is None:
        raise ValueError(
            f"Transform {op!r} accepts exactly one positional "
            f"argument (the value to transform); pass any offset, "
            f"partition, or other settings as keyword arguments "
            f"(e.g. ``{op}(value, partition_by=...)``)."
        )
    if len(args) > len(pos_names):
        raise ValueError(
            f"Transform {op!r} accepts at most {len(pos_names)} "
            f"positional argument(s) after the value "
            f"({', '.join(pos_names)}); got {len(args)}."
        )
    explicit_kw_names = {k for k, _ in kwargs}
    for k in pos_names[:len(args)]:
        if k in explicit_kw_names:
            raise ValueError(
                f"Transform {op!r} got {k!r} both positionally and "
                f"as a keyword argument."
            )
    return list(zip(pos_names, args))


def _bind_transform(
    parsed: TransformCall, *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> TransformKey:
    inp = _bind_transform_input(
        parsed.input, scope=scope, bundle=bundle, alias_map=alias_map,
        measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
    )
    return _bind_transform_params(
        op=parsed.op, inp=inp, args=parsed.args, kwargs=parsed.kwargs,
        scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
    )


def _bind_transform_input(
    parsed: ParsedExpr, *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]],
    measure_ctx: Optional[MeasureResolutionCtx],
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
) -> ValueKey:
    # ``measure_ctx`` rides the transform INPUT only — partition_by / scalar
    # kwargs drop it.
    return _bind(
        parsed, scope=scope, bundle=bundle, in_filter=False,
        alias_map=alias_map, measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
    )


def _bind_transform_params(
    *, op: str, inp: ValueKey,
    args: Tuple[ParsedExpr, ...], kwargs: Tuple[Tuple[str, ParsedExpr], ...],
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
) -> TransformKey:
    positional_pairs = _transform_positional_pairs(op=op, args=args, kwargs=kwargs)
    bound_kwargs: List = []
    partition_keys: Grain = Grain.EMPTY
    allowed_kwargs = _TRANSFORM_KWARG_RULES.get(op, frozenset())
    seen_kwargs: set = set()
    rank_family = op in RANK_FAMILY_TRANSFORMS
    advertised = with_direction_kwarg(
        op=op, accepted=allowed_kwargs | {"partition_by"} if rank_family else allowed_kwargs,
    )
    direction_value: object = _NOT_SCALAR
    for k, v in [*positional_pairs, *kwargs]:
        if k == "partition_by" and rank_family:
            partition_keys = _bind_partition_keys(
                value=v, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
                label=f"transform {op!r}",
            )
            continue
        if k == "direction" and rank_family:
            seen_kwargs.add(k)
            direction_value = _fold_to_scalar(v)
            continue
        if k not in allowed_kwargs:
            raise TransformArgumentError(
                summary=f"Transform {op!r} does not accept keyword "
                f"argument {k!r}. Accepted: {sorted(advertised)}.",
                suggestion=comparison_hint(k),
            )
        seen_kwargs.add(k)
        scalar = _fold_to_scalar(v)
        if scalar is _NOT_SCALAR:
            raise TransformArgumentError(
                summary=f"Transform {op!r} keyword {k!r} must be a "
                f"scalar literal; got expression of kind "
                f"{type(v).__name__}."
            )
        bound_kwargs.append((k, scalar))
    direction = rank_direction(op=op, given="direction" in seen_kwargs, value=direction_value)
    if direction is not None:
        bound_kwargs.append(("direction", direction))
    bound_kwargs = _apply_transform_kwarg_defaults(
        op=op, kwargs=bound_kwargs, seen=seen_kwargs,
    )
    return TransformKey(
        op=op,
        input=inp,
        args=(),
        kwargs=tuple(bound_kwargs),
        partition_keys=partition_keys,
    )


def _is_positive_integer(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, Decimal):
        return value.is_finite() and value == value.to_integral_value() and value > 0
    return isinstance(value, int) and value > 0


def _apply_transform_kwarg_defaults(
    *, op: str, kwargs: list, seen: set,
) -> list:
    """Validate required kwargs and apply per-op defaults for the TransformKey.

    ``ntile`` requires positive-integer ``n``; ``time_shift`` requires integer
    ``periods`` (may be negative); ``lag`` / ``lead`` default ``periods=1``.
    Integer checks accept integral ``Decimal`` (``normalize_scalar`` wraps numbers)."""
    if op == "ntile":
        if "n" not in seen:
            raise TransformArgumentError(
                summary="Transform 'ntile' requires keyword argument n (the "
                "number of buckets, a positive integer)."
            )
        n_value = next(v for k, v in kwargs if k == "n")
        if not _is_positive_integer(n_value):
            raise TransformArgumentError(
                summary=f"Transform {op!r} keyword n must be a positive "
                f"integer; got {n_value!r}."
            )
    if op == "time_shift" and "periods" not in seen:
        raise TransformArgumentError(
            summary="Transform 'time_shift' requires keyword argument periods "
            "(the integer offset, negative for a backward shift)."
        )
    if op in ("lag", "lead") and "periods" not in seen:
        kwargs.append(("periods", normalize_scalar(1)))
    return kwargs


def _bind_scalar(
    parsed: ScalarCall, *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> ScalarCallKey:
    if parsed.name not in SCALAR_FUNCTIONS:
        # Defence in depth: direct ParsedExpr construction bypasses the parser.
        raise UnknownFunctionError(
            name=parsed.name,
            location="(binder)",
            suggestion=(
                f"Mode-B scalar calls are restricted to "
                f"{sorted(SCALAR_FUNCTIONS)}."
            ),
        )
    # Check arity for every allowlisted scalar: sqlglot's handling is
    # inconsistent (silent arg-drop / DB-rejected SQL), so error clearly here.
    spelled = [n for n in map(keyword_spelling, parsed.args) if n is not None]
    arity_error = check_scalar_arity(
        name=parsed.name, argc=len(parsed.args),
    )
    if arity_error is not None and spelled:
        raise ValueError(f"{arity_error} Note: {positional_hint(spelled[0])}")
    if arity_error is not None:
        if parsed.name == "like":
            raise ValueError(
                f"Scalar function 'like' takes exactly 2 arguments "
                f"(value, pattern); got {len(parsed.args)}."
            )
        raise ValueError(arity_error)
    if parsed.name == "round" and len(parsed.args) == 2 and not _is_integer_literal(parsed.args[1]):
        raise ValueError(
            "'round' ndigits (2nd argument) must be an integer literal; got "
            f"{canonical_measure_text(parsed.args[1])!r}."
        )

    _check_library_literals(parsed, bundle=bundle)

    def bind(a: ParsedExpr, *, in_temporal_slot: bool) -> ValueKey:
        key = _bind(
            a, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map,
            measure_ctx=measure_ctx, dim_alias_map=dim_alias_map,
        )
        # ISO string literals in a DATE/TIMESTAMP slot bind as date values.
        return type_date_values(key) if in_temporal_slot else key

    slots = set(DATE_OPERAND_ARGS.get(parsed.name, ()))
    try:
        args = tuple(bind(a, in_temporal_slot=i in slots) for i, a in enumerate(parsed.args))
    except UnknownReferenceError as e:
        if e.name not in spelled:
            raise
        raise _with_hint(e, hint=positional_hint(e.name)) from e
    if parsed.name == "date_add":
        _check_literal_count(args[DATE_ADD_COUNT_ARG])
    if parsed.name == "json_value":
        args = (_json_input(args[0], scope=scope, bundle=bundle), *args[1:])
    return ScalarCallKey(name=parsed.name, args=args)


def _text_literal(parsed: ScalarCall, pos: int) -> str:
    """The string literal at ``pos`` of a library call's text slot (pattern, replacement, path)."""
    value = _fold_to_scalar(parsed.args[pos])
    if not isinstance(value, str):
        slot = slot_name(parsed.name, pos)
        raise ScalarArgumentError(
            summary=f"the {slot} of {parsed.name}() must be a string literal or a {{variable}}; got "
            f"`{canonical_measure_text(parsed.args[pos])}`.",
            suggestion=f"Write the {slot} as a literal, e.g. '…', or pass it through `variables`.",
        )
    return value


def _check_library_literals(parsed: ScalarCall, *, bundle: ResolvedSourceBundle) -> None:
    """The literal-only arguments of a library call: patterns, replacements, paths, flags, groups, indices, domains."""
    name, args = parsed.name, parsed.args
    if name in LIBRARY_FUNCTIONS and not is_list_fn(name):
        # A refusal outranks argument errors; a list-overloaded name is decided once its arguments are typed.
        require_library(bundle_dialect(bundle), name)
    if name in REGEX_FUNCTIONS:
        pattern = check_pattern(name, _text_literal(parsed, 1))
        if name == "regexp_replace":
            check_replacement(name, _text_literal(parsed, 2), pattern=pattern)
        extra = 3 if name == "regexp_replace" else 2
        if len(args) > extra and name == "regexp_extract":
            check_group(name, _fold_to_scalar(args[extra]), pattern=pattern)
        elif len(args) > extra:
            check_flags(name, _fold_to_scalar(args[extra]))
    elif name == "json_value":
        check_path(name, _text_literal(parsed, 1))
    elif name == "split_part":
        check_split_index(_fold_to_scalar(args[2]))
    check_domain(name, [_fold_to_scalar(arg) for arg in args], texts=[canonical_measure_text(arg) for arg in args])


def _json_input(arg: ValueKey, *, scope: ModelScope | StageSchema, bundle: ResolvedSourceBundle) -> ValueKey:
    """``json_value``'s input: TEXT as is, a native JSON column read as its own type (``CAST(col AS JSONB)``)."""
    column_type = scope_column_type(scope=scope, bundle=bundle)
    db_type = _json_db_type(arg, scope=scope, bundle=bundle)
    if db_type is not None:
        return CastKey(source=arg, type_text=db_type, data_type=DataType.UNKNOWN)
    declared = column_type(arg) if isinstance(arg, (ColumnKey, ColumnSqlKey)) else None
    if declared in (None, DataType.TEXT) and not (
        numeric_valued(arg, column_type=column_type) or boolean_valued(arg, column_type=column_type)
        or temporal_type(arg, column_type=column_type) is not None
    ):
        return arg
    raise ScalarOperandTypeError(
        summary=f"json_value() reads JSON text or a JSON column; `{key_display(arg)}` is "
        f"{(declared or DataType.UNKNOWN).value if declared not in (None, DataType.TEXT) else 'not text'}.",
        suggestion="Pass a TEXT column holding JSON, or a column of the database's JSON type.",
    )


def _json_db_type(arg: ValueKey, *, scope: ModelScope | StageSchema, bundle: ResolvedSourceBundle) -> Optional[str]:
    """The database JSON type of a column ``arg`` (``JSONB``, ``VARIANT``, …), when it has one."""
    if not isinstance(arg, ColumnKey) or not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    owner = resolved_terminal(root=scope.source_model, path=arg.path, models_by_name=bundle.models_by_name)
    col = owner.get_column(arg.leaf) if owner is not None else None
    if col is None or not col.db_type:
        return None
    return native_json_type(col.db_type, dialect=bundle.dialect)


def _is_integer_literal(parsed: ParsedExpr) -> bool:
    """An integer literal, signed or not (``2``, ``-2``)."""
    if isinstance(parsed, UnaryOp) and parsed.op == "-":
        parsed = parsed.operand
    return (
        isinstance(parsed, Literal) and isinstance(parsed.value, Decimal)
        and parsed.value == parsed.value.to_integral_value()
    )


def _check_literal_count(count: ValueKey) -> None:
    if not isinstance(count, LiteralKey):
        return
    value = count.value
    if not isinstance(value, Decimal) or value != value.to_integral_value():
        raise ValueError(
            f"date_add() / interval() count must be an integer or a numeric expression; "
            f"got the literal {value!r}."
        )


def _reject_windowed_column_sql(
    refs: Tuple[ValueKey, ...],
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    parsed: ParsedExpr,
) -> None:
    """Raise ``IllegalWindowInFilterError`` if any referenced ``ColumnSqlKey``
    has a windowed ``Column.sql`` body (no predicate-promotion)."""
    if isinstance(scope, StageSchema):
        # StageSchema columns carry no Column.sql; windows caught upstream.
        return
    for k in refs:
        if not isinstance(k, ColumnSqlKey):
            continue
        model = _lookup_model(name=k.model, scope=scope, bundle=bundle)
        if model is None:
            continue
        col = next((c for c in model.columns if c.name == k.column_name), None)
        if col is None or col.sql is None:
            continue
        if has_window_function(col.sql):
            raise IllegalWindowInFilterError(
                filter_expr=str(parsed),
                source=(
                    f"filter references column {k.column_name!r} on model "
                    f"{k.model!r} whose Column.sql contains a window "
                    f"function"
                ),
                suggestion=(
                    "use a rank-family transform (rank, percent_rank, "
                    "dense_rank, ntile) in the formula instead, or "
                    "compute the windowed value in an earlier stage."
                ),
            )


def _lookup_model(
    *,
    name: str,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
) -> Optional[SlayerModel]:
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        if scope.source_model.name == name:
            return scope.source_model
    return bundle.get_referenced_model(name)
