"""ExpressionBinder + FilterBinder: bind a ``ParsedExpr`` against a scope
(``DatasetScope``) into a typed ``BoundExpr`` / ``BoundFilter``."""

from __future__ import annotations

import contextlib
import difflib
import os
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from decimal import Decimal
from functools import partial
from typing import Any, Callable, Dict, Final, List, Mapping, Optional, Tuple, TypeGuard, cast

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import (
    AggArgumentError,
    AmbiguousRelationshipPathError,
    UnevaluableEntityError,
    ScalarArgumentError,
    ScalarOperandTypeError,
    AggregationArgumentError,
    AggregationFilterError,
    AggregationNotAllowedError,
    CastTypeError,
    ListTypeError,
    CallArgumentError,
    CircularRelationshipPathError,
    GranularityCallError,
    IllegalWindowInFilterError,
    MetricCycleError,
    MetricRecursionLimitError,
    PartitionKeyError,
    ReservedFunctionNameError,
    TimeLiteralError,
    TransformArgumentError,
    UnknownFunctionError,
    UnknownGranularityError,
    UnknownReferenceError,
    UnresolvableDimensionRelationshipError,
    UnresolvedPlaceholderError,
)
from slayer.core.enums import (
    ALL_TRANSFORMS,
    AMBIGUOUS_WEEKDAY_NAMES,
    BUILTIN_AGGREGATIONS,
    BUILTIN_AGGREGATION_PARAM_ORDER,
    DATE_PART_ALIASES,
    DEFAULT_AGGREGATIONS_BY_TYPE,
    DataType,
    GRANULARITY_NAMES,
    NUMERIC_DATATYPES,
    NUMERIC_ONLY_AGGREGATIONS,
    PRIMARY_KEY_AGGREGATIONS,
    RANKED_AGGREGATIONS,
    RESERVED_NAMES,
    TEMPORAL_DATATYPES,
    ambiguous_weekday_message,
    normalize_aggregation_name,
)
from slayer.core.direction import rank_direction, with_direction_kwarg
from slayer.core.enums import RANK_FAMILY_TRANSFORMS
from slayer.core.granularity import CustomGranularity, Granularity, resolve_granularity, unknown_granularity_message
from slayer.core.refs import EXPRESSION_SOURCE_KINDS, key_display
from slayer.core.vocabulary import deprecated_spelling, renamed_arguments
from slayer.core.keys import FieldTypeFn, input_masked, is_ranked, source_anchor_path, window_kwarg_of, DATE_ADD_COUNT_ARG, DATE_OPERAND_ARGS, MIRRORED_TIME_POINT_OP, SCALAR_FUNCTIONS, cast_type_parts, check_scalar_arity, type_date_values, AggregateKey, AggregateSource, ArithmeticKey, ArrayLiteralKey, CastKey, FieldKey, FieldSqlKey, Grain, InKey, ListSetKey, LiteralKey, Phase, ScalarCallKey, SqlFragmentKey, StarKey, TimePointCmpKey, TimePointOp, TimeTruncKey, TransformKey, VALUE_KEY_TYPES, ValueKey, boolean_valued, field_leaf, field_path, is_attached_source, normalize_scalar, prepend_value_key, temporal_type, walk_value_keys, TEXT_RESULT_SCALARS, integer_text_valued, list_valued, numeric_valued
from slayer.core.relationship_walker import (
    canonical_token,
    resolve_hop,
    resolved_terminal,
    aggregation_owner,
    canonical_path,
    terminal_dataset,
    walk,
    WalkMiss,
    walk_miss,
    walk_terminal,
)
from slayer.core.names import match_name, match_path, spelled_name
from slayer.core.models import (
    VALUE_PLACEHOLDER,
    Aggregation,
    SlayerDataset,
    aggregation_definition,
    builtin_semantics,
    is_identifier,
    reserved_value_param_message,
    shadows_library_function,
    source_reader,
)
from slayer.core.deprecation import scheduled_notice
from slayer.core.warnings import ChangedReferenceWarning, LibraryNameShadowWarning
from slayer.engine.population import dimension_routing
from slayer.engine.population.anchors import (
    anchor_route,
    edge_first_refusal,
    find_anchor,
    unknown_dataset,
    unknown_metric,
    unqualified_count,
)
from slayer.engine.key_metadata import bundle_dialect, scope_field_type
from slayer.engine.param_binding import QuotedArgumentRefused, bind_aggregation_params
from slayer.core.query import TimeDimension
from slayer.core.scope import DatasetScope, is_anchored, peek_dataset_field, resolve_generated_field, root_spelling
from slayer.core.time_points import is_time_point, parse_temporal_value
from slayer.core.time_spine import TIME_SPINE_DATASET
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.core.syntax import (
    AggCall,
    ArrayLit,
    Arith,
    BoolOp,
    BoundVariable,
    Cast,
    Cmp,
    DateTrunc,
    DottedRef,
    Literal,
    NamedArg,
    ParsedExpr,
    Placeholder,
    Ref,
    ScalarCall,
    StarSource,
    TemporalLiteral,
    TransformCall,
    TupleLit,
    UnaryOp,
    aggregation_reading,
    canonical_metric_text,
    classify_arguments,
    comparison_hint,
    dotted_quoting,
    drop_root_qualifier,
    keyword_spelling,
    parse_bound_expr,
    parse_expr,
    placeholder_names,
    walk_parsed,
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
from slayer.sql.sql_expr import build_cast_type, cast_slayer_type, has_window_function, native_json_type
from slayer.sql.sql_template import SqlTemplateError, aggregation_reads
from slayer.ir.bound import BoundExpr, BoundFilter, BoundTimeDimension
from slayer.core.models import DatasetField
from slayer.core.models import DatasetMetric

__all__ = [
    "bind_expr",
    "bind_dataset_expression",
    "bind_filter",
    "bind_time_dimension",
    "collect_bind_records",
]


class BindRecords(BaseModel):
    """What binding a statement resolved through, beyond its keys: saved metrics ``(dataset, metric)`` and library
    functions its datasets' aggregations shadowed."""

    metrics: Dict[Tuple[str, str], None] = {}
    shadows: Dict[Tuple[str, str, str], LibraryNameShadowWarning] = {}
    notices: Dict[str, ChangedReferenceWarning] = {}


_bind_records: ContextVar[Optional[BindRecords]] = ContextVar("slayer_bind_records", default=None)


@contextmanager
def collect_bind_records() -> Generator[BindRecords, None, None]:
    """Collect the records of every binding inside the block."""
    records = BindRecords()
    token = _bind_records.set(records)
    try:
        yield records
    finally:
        _bind_records.reset(token)


_RANGE_READ: ContextVar[bool] = ContextVar("slayer_range_read", default=False)
_RANGE_AGGREGATIONS = frozenset({"min", "max"})


@contextmanager
def range_read() -> Generator[None, None, None]:
    """Inside the block, ``min`` / ``max`` pass a field's ``allowed_aggregations`` (sample profiling's range)."""
    token = _RANGE_READ.set(True)
    try:
        yield
    finally:
        _RANGE_READ.reset(token)


_DEFAULT_METRIC_DEPTH = 32
_METRIC_DEPTH_ENV_VAR = "SLAYER_METRIC_EXPANSION_DEPTH"
_OLD_METRIC_DEPTH_ENV_VAR = "SLAYER_MEASURE_EXPANSION_DEPTH"
_NO_ANCHOR_SCOPE_SUMMARY = "(no population anchor; anchor-less mode not implemented)"


def _metric_depth_limit() -> int:
    """Saved-metric expansion depth cap (env override, default 32)."""
    env = renamed_arguments(
        {k: v for k, v in os.environ.items() if k in (_METRIC_DEPTH_ENV_VAR, _OLD_METRIC_DEPTH_ENV_VAR)},
        renames={_OLD_METRIC_DEPTH_ENV_VAR: _METRIC_DEPTH_ENV_VAR},
    )
    if _OLD_METRIC_DEPTH_ENV_VAR in os.environ:
        deprecated_spelling(_OLD_METRIC_DEPTH_ENV_VAR, _METRIC_DEPTH_ENV_VAR)
    raw = env.get(_METRIC_DEPTH_ENV_VAR)
    if raw is None:
        return _DEFAULT_METRIC_DEPTH
    try:
        return max(1, int(raw))
    except ValueError:
        return _DEFAULT_METRIC_DEPTH


class MetricResolutionCtx(BaseModel):
    """Non-``None`` makes saved-metric refs legal here; carries the
    ``(dataset, metric)`` chain for cycle/depth detection. Dropped at
    aggregation boundaries so a metric errors there."""

    model_config = ConfigDict(frozen=True)

    chain: Tuple[Tuple[str, str], ...] = ()
    depth_limit: int

    def descend(self, *, dataset: str, metric: str) -> "MetricResolutionCtx":
        return self.model_copy(update={"chain": self.chain + ((dataset, metric),)})


def _fmt_metric_chain(chain: Tuple[Tuple[str, str], ...]) -> List[str]:
    """Render a ``(dataset, metric)`` chain as ``dataset.metric`` steps for errors."""
    return [f"{dataset}.{metric}" for dataset, metric in chain]


def bind_expr(
    parsed: ParsedExpr,
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    allow_metrics: bool = False,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    dimension_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> BoundExpr:
    """Bind a parsed expression against a scope into a ``BoundExpr``.

    ``allow_metrics`` enables saved-metric resolution (bare and dotted) in
    the eligible positions — metric expressions and computed-dimension
    expressions; off everywhere else, so a saved-metric name there errors.
    ``alias_map`` resolves query aliases as in :func:`bind_filter`.
    ``dimension_alias_map`` resolves ``partition_by=<computed dim name>`` to
    the dimension's bound key; it applies ONLY there."""
    _reject_placeholders(parsed)
    metric_ctx = (
        MetricResolutionCtx(depth_limit=_metric_depth_limit())
        if allow_metrics else None
    )
    value_key = _bind(
        parsed, scope=scope, bundle=bundle, in_filter=False, alias_map=alias_map, metric_ctx=metric_ctx,
        dim_alias_map=dimension_alias_map,
    )
    return BoundExpr(
        value_key=value_key,
        routed_dotted=_canonical_if_routed(parsed=parsed, value_key=value_key),
    )


def bind_dataset_expression(
    expression: str, *, dataset: SlayerDataset, datasets: Sequence[SlayerDataset], dialect: str,
    granularities: Optional[Mapping[str, CustomGranularity]] = None,
) -> ValueKey:
    """``expression`` bound on ``dataset`` (saved metrics inlined) against the ``datasets`` graph, reading no database;
    ``granularities`` are the datasource's custom granularities, keyed by ``granularity_key``."""
    bundle = ResolvedSourceBundle(dialect=dialect, population=dataset, referenced_datasets=list(datasets),
                                  granularities=dict(granularities or {}))
    return bind_expr(
        parse_expr(expression), scope=DatasetScope(population=dataset), bundle=bundle, allow_metrics=True,
    ).value_key


def spelled_aggregate_key(
    *, parsed: ParsedExpr, key: AggregateKey, bundle: ResolvedSourceBundle,
) -> AggregateKey:
    """``key`` keeping only the parameters the query wrote (the public name's basis);
    a bound definition default is never part of the spelling."""
    if not isinstance(parsed, AggCall):
        return key
    parsed = _classified(parsed, bundle=bundle)
    positional = _declared_agg_param_names(agg=key.agg, source=key.source, bundle=bundle)
    written = {k for k, _ in parsed.kwargs} | set(positional[:len(parsed.args)])
    return key.model_copy(update={
        "kwargs": tuple((k, v) for k, v in key.kwargs if k in written),
    })


def bind_time_dimension(
    td: TimeDimension,
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    granularity: Optional[Granularity] = None,
) -> BoundTimeDimension:
    """Bind a ``TimeDimension`` into a ``BoundTimeDimension``: a ``BoundExpr`` carrying a ``TimeTruncKey`` plus the facts the checker judges (a field's type; every bucketed temporal leaf's recorded granularity, from its ``DatasetField``). A field resolves like a Mode-B identifier ref against the ``DatasetScope``, any other text binds as a Mode-B expression; the row-level / temporal / re-bucketing rules are the checker's (P9)."""
    text = td.field
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
    bound_col = _resolve_field_ref(parsed, scope=scope, bundle=bundle)
    field_type, upstream = _field_facts(bound_col, scope=scope, bundle=bundle)
    routed = _canonical_if_routed(parsed=parsed, value_key=bound_col)
    return BoundTimeDimension(
        bound=BoundExpr(value_key=TimeTruncKey(source=bound_col, granularity=resolved), routed_dotted=routed),
        field_type=field_type,
        bucketed_leaves=() if upstream is None else ((text, upstream),),
    )


def _resolve_field_ref(
    parsed: Ref | DottedRef, *, scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> FieldKey | FieldSqlKey:
    """A time dimension's field against ``scope``, walking relationships."""
    full = parsed.name if isinstance(parsed, Ref) else ".".join(parsed.parts)
    if scope.population is None:
        raise UnknownReferenceError(
            name=full,
            scope_kind="DatasetScope",
            scope_summary=_NO_ANCHOR_SCOPE_SUMMARY,
            suggestion=None,
        )
    if isinstance(parsed, DottedRef):
        bound_col = _resolve_dotted(parsed.parts, scope=scope, bundle=bundle, quoted=dotted_quoting(parsed))
    else:
        bound_col = _resolve_ref(parsed.name, scope=scope, bundle=bundle, quoted=parsed.quoted)
    if not isinstance(bound_col, (FieldKey, FieldSqlKey)):
        # Defensive: an identifier ref against a DatasetScope is always a field.
        raise ValueError(
            f"TimeDimension {full!r} did not resolve to a field "
            f"reference (got {type(bound_col).__name__})."
        )
    return bound_col


def _field_facts(
    col: FieldKey | FieldSqlKey, *, scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> Tuple[Optional[DataType], Optional[Granularity]]:
    """A field's (type, recorded bucket granularity): the terminal ``DatasetField`` its path reaches."""
    terminal = _terminal_dataset_for_path(path=field_path(col), scope=scope, bundle=bundle)
    if terminal is None:
        # Defensive: _resolve_ref / _resolve_dotted would already have raised.
        name = ".".join((*field_path(col), field_leaf(col)))
        raise UnknownReferenceError(
            name=name,
            scope_kind="DatasetScope",
            scope_summary=f"could not resolve terminal dataset for {name!r}",
            suggestion=None,
        )
    found = next((c for c in terminal.fields if c.name == field_leaf(col)), None)
    upstream = None
    if found is not None and found.granularity is not None:
        upstream = resolve_granularity(found.granularity, defined=bundle.granularities)
    return (found.datatype if found is not None else None), upstream


def _bucketed_leaves(
    source: ValueKey, *, scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> Tuple[Tuple[str, Granularity], ...]:
    """Every temporal field leaf of ``source`` that carries a recorded bucket granularity, by its dotted name."""
    out: Dict[str, Granularity] = {}
    for leaf in walk_value_keys(source):
        if not isinstance(leaf, (FieldKey, FieldSqlKey)):
            continue
        field_type, upstream = _field_facts(leaf, scope=scope, bundle=bundle)
        if upstream is not None and field_type in TEMPORAL_DATATYPES:
            out.setdefault(".".join((*field_path(leaf), field_leaf(leaf))), upstream)
    return tuple(out.items())


def _canonical_if_routed(
    *,
    parsed: ParsedExpr,
    value_key: ValueKey,
) -> Optional[str]:
    """Full canonical dotted path when a ``DottedRef``'s bound path differs from
    its typed hop path (auto-routed or respelled) — or the canonical name of a
    stale flat ``Ref`` — else ``None``, so every already-canonical ref keeps a
    byte-identical result key."""
    key = value_key.source if isinstance(value_key, TimeTruncKey) else value_key
    if not isinstance(key, (FieldKey, FieldSqlKey)):
        return None  # saved metrics canonicalize in bind_inputs._resolve_saved_metric_ref
    if isinstance(parsed, Ref):
        leaf = field_leaf(key)
        return leaf if not field_path(key) and leaf != parsed.name else None
    if not isinstance(parsed, DottedRef):
        return None
    canonical = (*field_path(key), field_leaf(key))
    return None if tuple(parsed.parts) == canonical else ".".join(canonical)


def _terminal_dataset_for_path(
    *,
    path: Tuple[str, ...],
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
) -> Optional[SlayerDataset]:
    """The terminal dataset of ``path`` from ``scope.population`` (host if empty)."""
    if scope.population is None:
        return None
    return terminal_dataset(root=scope.population, path=path, datasets_by_name=bundle.datasets_by_name)


def bind_filter(
    parsed: ParsedExpr,
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    dimension_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> BoundFilter:
    """Bind a parsed filter predicate + classify its phase.

    Walks the bound tree to gather every referenced ``ValueKey``; raises
    ``IllegalWindowInFilterError`` if a referenced ``DatasetField.sql`` is windowed.
    ``alias_map`` maps a stage's declared-metric names to their bound
    ``ValueKey`` so a bare ref matching an alias interns onto that slot rather
    than resolving against dataset fields (the aggregate and its alias share one slot).
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
    _reject_windowed_field_sql(refs, scope=scope, bundle=bundle, parsed=parsed)
    return BoundFilter(
        value_key=value_key, phase=phase, referenced_keys=refs,
    )


def _bind(
    parsed: ParsedExpr,
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    metric_ctx: Optional[MetricResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> ValueKey:
    # ``metric_ctx`` rides eligible operand edges, dropped at the aggregation
    # boundary — a metric is legal at value level but not inside an aggregation.
    # ``dim_alias_map`` rides every edge but resolves ONLY inside an
    # aggregation's ``partition_by``.
    leaf = _bind_leaf(parsed, scope=scope, bundle=bundle, alias_map=alias_map, metric_ctx=metric_ctx)
    if leaf is not None:
        return leaf
    sub = partial(
        _bind, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map,
        metric_ctx=metric_ctx, dim_alias_map=dim_alias_map,
    )
    if isinstance(parsed, Cast):
        return _bind_cast(parsed, source=sub(parsed.source), scope=scope, bundle=bundle)
    if isinstance(parsed, DateTrunc):
        return _bind_date_trunc(parsed, source=sub(parsed.value), bundle=bundle)
    if isinstance(parsed, NamedArg):
        raise CallArgumentError(f"`{canonical_metric_text(parsed)}` is only an argument of a call.")
    if isinstance(parsed, AggCall):
        return _bind_agg_call(
            parsed, scope=scope, bundle=bundle, alias_map=alias_map,
            metric_ctx=metric_ctx, dim_alias_map=dim_alias_map,
        )
    if isinstance(parsed, TransformCall):
        return _bind_transform(
            parsed, scope=scope, bundle=bundle, alias_map=alias_map,
            metric_ctx=metric_ctx, dim_alias_map=dim_alias_map,
        )
    if isinstance(parsed, ScalarCall):
        return _bind_scalar(
            parsed, scope=scope, bundle=bundle, in_filter=in_filter,
            alias_map=alias_map, metric_ctx=metric_ctx,
            dim_alias_map=dim_alias_map,
        )
    if isinstance(parsed, Cmp):
        return _bind_cmp(
            parsed, sub=sub, scope=scope, bundle=bundle, in_filter=in_filter,
            alias_map=alias_map, metric_ctx=metric_ctx, dim_alias_map=dim_alias_map,
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
                summary=f"Every element of the list literal `{canonical_metric_text(parsed)}` must be a literal; "
                f"`{canonical_metric_text(element)}` is not.",
                suggestion="Compare a list with another list field through contains_any / contains_all instead.",
            )
        values.append(key)
    return ArrayLiteralKey(values=tuple(values))


def _bind_leaf(
    parsed: ParsedExpr,
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]],
    metric_ctx: Optional[MetricResolutionCtx],
) -> Optional[ValueKey]:
    """A literal, reference or ``*`` leaf bound; ``None`` for a composite node."""
    if isinstance(parsed, Literal):
        return LiteralKey(value=normalize_scalar(parsed.value))
    if isinstance(parsed, TemporalLiteral):
        return LiteralKey(value=_temporal_literal_value(parsed))
    if isinstance(parsed, StarSource):
        return _bare_star(scope=scope)
    if not isinstance(parsed, (Ref, DottedRef)):
        return None
    if parsed.evaluated:
        return _bind_evaluated(parsed, scope=scope, bundle=bundle, metric_ctx=metric_ctx)
    try:
        if isinstance(parsed, Ref):
            return _resolve_ref(
                parsed.name, scope=scope, bundle=bundle, alias_map=alias_map,
                metric_ctx=metric_ctx, quoted=parsed.quoted,
            )
        return _resolve_dotted(
            parsed.parts, scope=scope, bundle=bundle, alias_map=alias_map,
            metric_ctx=metric_ctx, quoted=dotted_quoting(parsed),
        )
    except UnknownReferenceError as e:
        raise _with_hint(e, hint=_QUOTE_HINT.format(name=e.name)) if parsed.quoted else e


def _bind_evaluated(
    parsed: Ref | DottedRef,
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    metric_ctx: Optional[MetricResolutionCtx],
) -> ValueKey:
    """``AGG(ref)``: the dataset metric ``ref`` names, bound as the reference itself; anything else is refused."""
    plain = parsed.model_copy(update={"evaluated": False})
    key = _bind_leaf(plain, scope=scope, bundle=bundle, alias_map=None, metric_ctx=metric_ctx)
    if key is None or isinstance(key, (FieldKey, FieldSqlKey)):
        raise AggArgumentError(expression=canonical_metric_text(parsed),
                               reason=f"{canonical_metric_text(plain)!r} is a field")
    return key


def _bind_cmp(
    parsed: Cmp,
    *,
    sub: Callable[[ParsedExpr], ValueKey],
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]],
    metric_ctx: Optional[MetricResolutionCtx],
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
) -> ValueKey:
    time_point = _time_point_comparison(parsed, units=bundle.granularities)
    if time_point is not None:
        operand, op, point, literal_on_left = time_point
        return TimePointCmpKey(op=op, point=point, literal_on_left=literal_on_left, operand=sub(operand))
    # ``IN`` / ``NOT IN`` fold into a single ``InKey`` (structured
    # field + literal-tuple handle for the generator).
    if parsed.op in _MEMBERSHIP_OP:
        return _bind_in(
            parsed,
            scope=scope, bundle=bundle, in_filter=in_filter,
            alias_map=alias_map, metric_ctx=metric_ctx,
            dim_alias_map=dim_alias_map,
        )
    return ArithmeticKey(op=parsed.op, operands=(sub(parsed.left), sub(parsed.right)))


_QUOTE_HINT = "{name!r} was written in double quotes, which name a field; strings use single quotes ('…')."


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


def _try_cast_source(source: ValueKey, *, field_type: FieldTypeFn) -> Optional[DataType]:
    """What a ``TRY_CAST`` reads ``source`` as: integer text (``INT``), another number (``DOUBLE``), else text."""
    if integer_text_valued(source, field_type=field_type):
        return DataType.INTEGER
    return DataType.FLOAT if numeric_valued(source, field_type=field_type) else None


def _bind_cast(
    parsed: Cast, *, source: ValueKey, scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> CastKey:
    if parsed.try_ and cast_type_parts(parsed.type_text).array_rank:
        raise CastTypeError(
            summary=f"TRY_CAST to the array type {parsed.type_text} is not supported.",
            suggestion="use CAST(x AS <type>[]), whose invalid input is the database's own.",
        )
    try:
        build_cast_type(parsed.type_text, dialect=bundle.dialect)
    except ValueError as e:
        raise CastTypeError(summary=str(e), suggestion="cast to a type this database has.") from None
    data_type, element_type = cast_slayer_type(parsed.type_text, dialect=bundle.dialect)
    field_type = scope_field_type(scope=scope, bundle=bundle)
    if parsed.try_ and data_type in NUMERIC_DATATYPES and boolean_valued(source, field_type=field_type):
        source = CastKey(source=source, type_text="INT", data_type=DataType.INTEGER)  # a boolean reads as its 1 / 0
    return CastKey(
        source=source, type_text=parsed.type_text, data_type=data_type, try_=parsed.try_,
        element_type=element_type,
        numeric_source=_try_cast_source(source, field_type=field_type) if parsed.try_ else None,
    )


def _reject_placeholders(parsed: ParsedExpr, *, text: Optional[str] = None) -> None:
    """Raise ``UnresolvedPlaceholderError`` for the first unsubstituted ``{name}`` in ``parsed``."""
    names = placeholder_names(parsed)
    if names:
        raise UnresolvedPlaceholderError(name=names[0], expression=text or canonical_metric_text(parsed))


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
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    metric_ctx: Optional[MetricResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> ValueKey:
    """Bind an ``IN`` / ``NOT IN`` predicate into an ``InKey``.

    LHS binds through the normal field-resolution path; RHS is a ``TupleLit``
    of ``Literal`` nodes, each bound to a ``LiteralKey`` after normalization. A
    ``BoundVariable`` RHS is one list beside a list LHS, else the whole list."""
    if isinstance(parsed.right, Placeholder):
        _reject_placeholders(parsed.right)
    ctx: Dict[str, Any] = {"scope": scope, "bundle": bundle, "in_filter": in_filter, "alias_map": alias_map,
                           "metric_ctx": metric_ctx, "dim_alias_map": dim_alias_map}
    if isinstance(parsed.right, BoundVariable):
        listed = list_valued(_bind(parsed.left, **ctx), field_type=scope_field_type(scope=scope, bundle=bundle))
        rewritten = _variable_in(parsed, right=parsed.right, list_left=listed)
        if not isinstance(rewritten, Cmp) or rewritten.op not in _MEMBERSHIP_OP:
            return _bind(rewritten, **ctx)
        parsed = rewritten
    if not isinstance(parsed.right, TupleLit):
        # Defensive: the parser guarantees a TupleLit RHS for in / not in.
        raise ValueError(
            f"_bind_in: expected TupleLit on RHS of {parsed.op!r}, got "
            f"{type(parsed.right).__name__}."
        )
    field = _bind(parsed.left, **ctx)
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
        field=field,
        values=values,
        negated=(parsed.op == _NOT_IN),
    )


def _variable_in(parsed: Cmp, *, right: BoundVariable, list_left: bool) -> ParsedExpr:
    """``L IN ({v})`` over a list ``L`` is ``L = v``; over a scalar, ``v``'s values fill the whole list."""
    elements = tuple(_variable_literal(right.name, v) for v in right.values)
    if list_left:
        same = Cmp(op="==", left=parsed.left, right=ArrayLit(elements=elements) if right.listed else elements[0])
        return UnaryOp(op="not", operand=same) if parsed.op == _NOT_IN else same
    if not elements:
        raise ValueError(
            f"Variable '{right.name}' cannot be an empty list; 'IN ()' is invalid SQL. "
            f"For 'no filter' semantics, use a sentinel default."
        )
    return parsed.model_copy(update={"right": TupleLit(elements=elements)})


def _variable_literal(name: str, value: object) -> Literal:
    """One value of variable ``name`` as a literal (a string, number or boolean)."""
    scalar = normalize_scalar(value) if isinstance(value, (str, int, float, bool, Decimal)) else value
    if not isinstance(scalar, (str, Decimal, bool)):
        raise ValueError(f"Variable '{name}' must hold strings, numbers or booleans, got {type(value).__name__}.")
    return Literal(value=scalar)


def _name_suggestion(*, name: str, dataset: "SlayerDataset") -> str | None:
    """A ``Did you mean 'X'?`` clause over the dataset's fields + named metrics."""
    known = sorted(
        {c.name for c in dataset.fields}
        | {m.name for m in dataset.metrics if m.name is not None}
    )
    match = difflib.get_close_matches(word=name, possibilities=known, n=1)
    return f"Did you mean '{match[0]}'?" if match else None


def _resolve_ref(
    name: str,
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    metric_ctx: Optional[MetricResolutionCtx] = None,
    quoted: bool = False,
) -> ValueKey:
    """Resolve a bare identifier against the scope (``slayer.core.names.match_name``).

    Resolution order: declared alias → field → saved metric. A saved-metric
    name (no matching field) resolves inline only when ``metric_ctx`` is set
    (eligible position), else errors."""
    alias = match_name(name=name, declared=alias_map, quoted=quoted) if alias_map else None
    if alias_map and alias is not None:
        return alias_map[alias]
    if is_anchored(scope):
        raise unknown_metric(name, datasets=_visible_datasets(scope=scope, bundle=bundle))

    if scope.population is None:
        raise UnknownReferenceError(
            name=name,
            scope_kind="DatasetScope",
            scope_summary=_NO_ANCHOR_SCOPE_SUMMARY,
            suggestion=None,
        )
    dataset = scope.population

    # A ``__``-bearing name is not special: it resolves like any other name.
    col = resolve_generated_field(dataset=dataset, name=name, quoted=quoted)
    if col is not None:
        _check_evaluable(col, dataset=dataset)
        if col.needs_expansion:
            return FieldSqlKey(path=(), dataset=dataset.name, field_name=col.name)
        return FieldKey(path=(), leaf=col.name)

    # No field: a saved metric resolves inline in an eligible position.
    mm = dataset.get_metric(name, quoted=quoted)
    if mm is not None:
        return _resolve_saved_metric(
            metric=mm, terminal_dataset=dataset, scope=scope, bundle=bundle,
            metric_ctx=metric_ctx, ref_text=name, host_path=(),
        )
    raise UnknownReferenceError(
        name=name,
        scope_kind="DatasetScope",
        scope_summary=f"{_dataset_label(dataset, bundle=bundle)} fields: {[c.name for c in dataset.fields]}",
        suggestion=_name_suggestion(name=name, dataset=dataset),
    )


def _check_evaluable(entity: "DatasetField | DatasetMetric", *, dataset: SlayerDataset) -> None:
    """Refuse reading a document entity SLayer loads but cannot evaluate yet."""
    if entity.load_warning is not None:
        kind = "Field" if isinstance(entity, DatasetField) else "Metric"
        raise UnevaluableEntityError(entity=f"{kind} '{dataset.spelling}.{entity.name}'", reason=entity.load_warning)


def _visible_datasets(*, scope: DatasetScope, bundle: ResolvedSourceBundle) -> List[SlayerDataset]:
    """The datasets a bare name's suggestions may name: the bundle's, the reading dataset included."""
    found = dict(bundle.datasets_by_name)
    if scope.population is not None:
        found.setdefault(scope.population.name, scope.population)
    return [d for d in found.values() if d.name != TIME_SPINE_DATASET]


def _anchored_parts(
    parts: Tuple[str, ...], *, scope: DatasetScope, bundle: ResolvedSourceBundle, quoted: Tuple[bool, ...],
) -> Tuple[Tuple[str, ...], Tuple[bool, ...]]:
    """``parts`` as a path from the reading dataset: its anchor replaced by the route reaching it."""
    host = scope.population
    assert host is not None
    datasets = bundle.datasets_by_name
    anchor = find_anchor(parts[0], quoted=quoted[0], reader=host, datasets_by_name=datasets)
    if anchor is None:
        raise edge_first_refusal(parts, datasets_by_name={**datasets, host.name: host}) or unknown_dataset(parts)
    reached = anchor_route(reader=host, anchor=anchor, datasets_by_name=datasets, rest=parts[1:])
    if anchor.name != host.name:
        _announce_changed_reference(parts, host=host, anchor=anchor, route=reached.route, datasets=datasets,
                                    quoted=quoted[0])
    return (*reached.route, *parts[1:]), (*(False,) * len(reached.route), *quoted[1:])


def _announce_changed_reference(
    parts: Tuple[str, ...], *, host: SlayerDataset, anchor: SlayerDataset, route: Tuple[str, ...],
    datasets: Dict[str, SlayerDataset], quoted: bool,
) -> None:
    """Record the notice of a dataset-first reference whose direct relationship from ``host`` is not its path."""
    records = _bind_records.get()
    if records is None:
        return
    try:
        direct = resolve_hop(current=host, token=parts[0], datasets_by_name={**datasets, host.name: host},
                             quoted=quoted)
    except AmbiguousRelationshipPathError:
        return
    if direct is None or direct.to != anchor.name or route == (canonical_token(direct),):
        return
    reference = ".".join(parts)
    path = " → ".join((host.spelling, *route))
    spelling = ".".join((host.spelling, canonical_token(direct), *parts[1:]))
    message = scheduled_notice(
        f"`{reference}` reads {anchor.spelling!r} along {path}, its only to-one path; before 2.0 it read the direct "
        f"relationship, which `{spelling}` names.",
        removal="3.0",
    )
    records.notices.setdefault(message, ChangedReferenceWarning(
        reference=reference, path=path, spelling=spelling, message=message,
    ))


def _dataset_label(dataset: SlayerDataset, *, bundle: ResolvedSourceBundle) -> str:
    """How messages name ``dataset``: a stage by its user spelling, else the dataset."""
    display = bundle.stage_displays.get(dataset.name)
    return display.label if display is not None else f"dataset {dataset.spelling!r}"


def _walk_relationship_chain(
    *,
    hop_path: Tuple[str, ...],
    host,
    bundle: ResolvedSourceBundle,
    parts: Tuple[str, ...],
    leaf: str,
    original_parts: Optional[Tuple[str, ...]] = None,
    quoted: Tuple[bool, ...] = (),
    original_quoted: Tuple[bool, ...] = (),
    routes_short_form: bool = True,
    anchor: Optional[Tuple[str, int]] = None,
):
    """Walk ``hop_path`` relationship hops from ``host`` through the shared bidirectional
    walker; returns ``(terminal_dataset, canonical effective_hop_path)``. Each token
    resolves as an edge name then a neighbour dataset, in either orientation.

    When a token resolves to no incident edge, a bare ``Target`` short form
    (``len(hop_path) == 1``) auto-routes to its full datasource-scoped path
    — the effective path is the routed one; a ``len >= 2`` chain is a
    broken chain, rejected (never silently repaired) with a short-form suggestion
    when the target is uniquely routable. ``AmbiguousRelationshipPathError`` from a
    parallel-pair hop propagates untouched; an edge that resolves onto a target
    absent from the bundle stays ``UnknownReferenceError``. A failed walk whose
    remaining tokens spell a flat field raises the flat-spelling refusal instead.
    ``parts`` is the full dotted ref, for error messages; ``quoted`` flags each of
    its segments; ``original_parts`` (pre-self-prefix-strip, defaulting to
    ``parts``) spells the reference in full, ``original_quoted`` flagging its segments."""
    datasets_by_name = bundle.datasets_by_name
    datasets_by_name.setdefault(host.name, host)
    spelled = parts if original_parts is None else original_parts
    hop_quoted = quoted[:len(hop_path)]
    written = tuple(
        _quoted_segment(t, quoted=q) for t, q in zip(spelled[:len(spelled) - len(parts)], original_quoted)
    )
    refusal = partial(
        _flat_spelling_refusal, host=host, hop_path=hop_path, leaf=leaf, spelled=spelled,
        written=written, quoted=quoted, datasets_by_name=datasets_by_name, anchor=anchor,
    )
    try:
        walked = walk_terminal(root=host, path=hop_path, datasets_by_name=datasets_by_name, quoted=hop_quoted)
    except CircularRelationshipPathError as e:
        raise refusal() or CircularRelationshipPathError(
            reference=".".join(spelled), root_dataset=host.spelling,
            revisited=_dataset_spelling(e.revisited, datasets_by_name=datasets_by_name), hop=e.hop,
            via=_dataset_spelling(e.via, datasets_by_name=datasets_by_name),
        ) from None
    if walked is not None:
        return walked
    miss = walk_miss(root=host, path=hop_path, datasets_by_name=datasets_by_name, quoted=hop_quoted)
    assert miss is not None
    if miss.kind == "unloaded":
        assert miss.target is not None
        match_name(name=miss.target, declared=datasets_by_name, quoted=False)  # raises when several match by case
        raise _target_not_in_bundle(parts=parts, target=miss.target)
    broken = len(hop_path) != 1 or not routes_short_form
    flat = refusal() if broken else None
    if flat is not None:
        raise flat
    _reject_opaque_elements(
        host=host, hop_path=hop_path, index=miss.index, parts=parts, datasets_by_name=datasets_by_name, quoted=hop_quoted,
    )
    if broken:
        raise _broken_chain_error(
            host=host, hop_path=hop_path, leaf=leaf, parts=parts, miss=miss, datasets_by_name=datasets_by_name,
            quoted=hop_quoted,
        )
    try:
        return _route_edgeless_hop(
            host=host, hop=miss.token, leaf=leaf, parts=parts,
            datasets_by_name=datasets_by_name, quoted=bool(hop_quoted) and hop_quoted[0],
        )
    except UnresolvableDimensionRelationshipError:
        flat = refusal()
        if flat is not None:
            raise flat from None
        raise


def _dataset_spelling(name: str, *, datasets_by_name: Dict[str, SlayerDataset]) -> str:
    dataset = datasets_by_name.get(name)
    return name if dataset is None else dataset.spelling


def _quoted_segment(name: str, *, quoted: bool) -> str:
    return f'"{name}"' if quoted else name


def _flat_spelling_refusal(
    *,
    host: SlayerDataset,
    hop_path: Tuple[str, ...],
    leaf: str,
    spelled: Tuple[str, ...],
    written: Tuple[str, ...],
    quoted: Tuple[bool, ...],
    datasets_by_name: Dict[str, SlayerDataset],
    anchor: Optional[Tuple[str, int]] = None,
) -> Optional[UnknownReferenceError]:
    """The refusal of a failed walk whose remaining tokens, joined with ``__``, name a field of a dataset on its walked
    prefix (nearest the root first); ``written`` is the dropped root qualifier, if any; ``anchor`` is an anchored
    reference's written anchor and the number of route hops before it, which the hint spells as written."""
    tokens = (*hop_path, leaf)
    for k in range(len(hop_path)):
        try:
            walked = walk_terminal(root=host, path=hop_path[:k], datasets_by_name=datasets_by_name, quoted=quoted[:k])
        except CircularRelationshipPathError:
            return None
        if walked is None:
            return None
        dataset = walked[0]
        exact = any(quoted[k:])
        col = peek_dataset_field(dataset=dataset, name="__".join(tokens[k:]), quoted=exact)
        if col is not None:
            segments = tuple(_quoted_segment(t, quoted=q) for t, q in zip(hop_path[:k], quoted))
            if anchor is None:
                prefix = (*written, *segments)
            else:
                prefix = (anchor[0], *segments[anchor[1]:]) if k >= anchor[1] else (spelled_name(dataset.spelling),)
            flat = ".".join((*prefix, _quoted_segment(col.name, quoted=exact)))
            reference = ".".join(spelled)
            return UnknownReferenceError(
                name=reference, scope_kind="DatasetScope",
                scope_summary=f"dataset {dataset.spelling!r} fields: {[c.name for c in dataset.fields]}",
                suggestion=f"{reference!r} spells the flattened field {col.name!r}; read it by its flat name {flat!r}.",
            )
    return None


def _reject_opaque_elements(
    *, host: SlayerDataset, hop_path: Tuple[str, ...], index: int, parts: Tuple[str, ...],
    datasets_by_name: Dict[str, SlayerDataset], quoted: Tuple[bool, ...],
) -> None:
    """A hop naming a list of opaque elements: they have no element access."""
    walked = walk_terminal(root=host, path=hop_path[:index], datasets_by_name=datasets_by_name, quoted=quoted[:index])
    hop_quoted = index < len(quoted) and quoted[index]
    field = walked[0].get_field(hop_path[index], quoted=hop_quoted) if walked is not None else None
    if field is not None and field.datatype is DataType.ARRAY:
        raise UnknownReferenceError(
            name=".".join(parts), scope_kind="DatasetScope",
            scope_summary=f"list field {field.name!r} holds opaque (Opaque) elements, which have no element access",
            suggestion=(
                "Declare the field's element_type to reach its elements; on the list itself, "
                "count() counts non-NULL lists (not elements) and IS NULL tests for a missing list."
            ),
        )


def _route_edgeless_hop(
    *,
    host: SlayerDataset,
    hop: str,
    leaf: str,
    parts: Tuple[str, ...],
    datasets_by_name: Dict[str, SlayerDataset],
    quoted: bool = False,
) -> Tuple[SlayerDataset, Tuple[str, ...]]:
    """A single-hop short form with no incident edge auto-routes to its datasource-scoped path."""
    found = match_name(name=hop, declared=datasets_by_name, quoted=quoted)
    if found is None and quoted:
        raise _target_not_in_bundle(parts=parts, target=hop)
    hop = found or hop
    route = dimension_routing.route_dotted_target(
        root=host, target_dataset=hop, leaf=leaf, datasets_by_name=datasets_by_name,
    )
    terminal = datasets_by_name.get(hop)
    if terminal is None:
        raise _target_not_in_bundle(parts=parts, target=hop)
    chain = walk(root=host, path=tuple(route), datasets_by_name=datasets_by_name)
    assert chain is not None  # a safe route walks by construction
    return terminal, canonical_path(chain)


def _target_not_in_bundle(*, parts: Tuple[str, ...], target: str) -> UnknownReferenceError:
    return UnknownReferenceError(
        name=".".join(parts), scope_kind="DatasetScope",
        scope_summary=f"target {target!r} not in source bundle", suggestion=None,
    )


def _broken_chain_error(
    *,
    host: SlayerDataset,
    hop_path: Tuple[str, ...],
    leaf: str,
    parts: Tuple[str, ...],
    miss: WalkMiss,
    datasets_by_name: Dict[str, SlayerDataset],
    quoted: Tuple[bool, ...],
) -> UnresolvableDimensionRelationshipError:
    """A multi-hop dotted chain with an unresolvable hop — rejected, never
    auto-repaired. Names the dataset the hop leaves and its fields; suggests the short form ``Target.leaf`` when its
    target (``hop_path[-1]``) is itself uniquely routable."""
    target = hop_path[-1]
    routable = dimension_routing.short_form_route_or_none(
        root=host, target_dataset=target, datasets_by_name=datasets_by_name,
    )
    left = walk_terminal(
        root=host, path=hop_path[:miss.index], datasets_by_name=datasets_by_name, quoted=quoted[:miss.index],
    )
    assert left is not None  # the walk reached the failing hop
    return UnresolvableDimensionRelationshipError(
        reference=".".join(parts),
        root_dataset=host.spelling,
        reason=(
            f"'{'.'.join(hop_path)}' is not a valid relationship chain: hop {miss.token!r} does not resolve from dataset "
            f"{left[0].spelling!r}, whose fields are {[c.name for c in left[0].fields]}."
        ),
        suggested_path=f"{target}.{leaf}" if routable is not None else None,
    )


def _source_column_key(
    parts: Tuple[str, ...], *, scope: DatasetScope, quoted: Tuple[bool, ...],
) -> FieldSqlKey | None:
    """The key of the runtime reader ``source.column`` names on the scope's population, else ``None``."""
    if scope.population is None or len(parts) != 2:
        return None
    reader = source_reader(scope.population, qualifier=parts[0], column=parts[1], quoted=quoted[1:2] == (True,))
    return None if reader is None else FieldSqlKey(path=(), dataset=scope.population.name, field_name=reader.name)


def _resolve_dotted(
    parts: Tuple[str, ...],
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    metric_ctx: Optional[MetricResolutionCtx] = None,
    quoted: Tuple[bool, ...] = (),
) -> ValueKey:
    """Resolve a dotted ref against the scope; ``quoted`` flags each double-quoted segment, matched exactly.

    Resolution order: the ``source`` qualifier's source column → root qualifier dropped → declared alias (dotted text)
    → relationship walk → leaf field → saved metric (re-anchored into host coords, eligible position). An anchored
    scope reads the first segment as the reference's anchor instead (``queries/references``)."""
    original_parts = parts
    original_quoted = tuple(quoted[:len(parts)]) + (False,) * max(0, len(parts) - len(quoted))
    if is_anchored(scope):
        return _resolve_anchored(
            parts, scope=scope, bundle=bundle, alias_map=alias_map, metric_ctx=metric_ctx, quoted=original_quoted,
        )
    source_key = _source_column_key(parts, scope=scope, quoted=original_quoted)
    if source_key is not None:
        return source_key
    parts = drop_root_qualifier(parts, root_spelling(scope), quoted=original_quoted)
    quoted = original_quoted[len(original_parts) - len(parts):]
    if alias_map:
        candidates = [(parts, quoted)] + ([(original_parts, original_quoted)] if parts != original_parts else [])
        for path, flags in candidates:
            alias = match_path(parts=path, declared=alias_map, quoted=flags)
            if alias is not None:
                return alias_map[alias]

    if len(parts) == 1:
        return _resolve_ref(
            parts[0], scope=scope, bundle=bundle, alias_map=alias_map,
            metric_ctx=metric_ctx, quoted=quoted[0],
        )

    if scope.population is None:
        raise UnknownReferenceError(
            name=".".join(parts),
            scope_kind="DatasetScope",
            scope_summary=_NO_ANCHOR_SCOPE_SUMMARY,
            suggestion=None,
        )

    host = scope.population

    # parts[:-1] are relationship targets; parts[-1] is the leaf field.
    hop_path = parts[:-1]
    leaf = parts[-1]
    current, effective_hop_path = _walk_relationship_chain(
        hop_path=hop_path, host=host, bundle=bundle, parts=parts, leaf=leaf,
        original_parts=original_parts, quoted=quoted, original_quoted=original_quoted,
    )

    return _resolve_terminal_leaf(
        current=current, leaf=leaf, hop_path=effective_hop_path,
        original_parts=original_parts, scope=scope, bundle=bundle,
        metric_ctx=metric_ctx, quoted=quoted[-1],
    )


def _resolve_anchored(
    parts: Tuple[str, ...],
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]],
    metric_ctx: Optional[MetricResolutionCtx],
    quoted: Tuple[bool, ...],
) -> ValueKey:
    """A query's dotted reference: an alias by its text, else anchored at its first segment."""
    if alias_map:
        alias = match_path(parts=parts, declared=alias_map, quoted=quoted)
        if alias is not None:
            return alias_map[alias]
    if scope.population is None:
        raise UnknownReferenceError(
            name=".".join(parts), scope_kind="DatasetScope", scope_summary=_NO_ANCHOR_SCOPE_SUMMARY, suggestion=None,
        )
    host = scope.population
    path, flags = _anchored_parts(parts, scope=scope, bundle=bundle, quoted=quoted)
    if not path[:-1]:
        return _resolve_terminal_leaf(
            current=host, leaf=path[-1], hop_path=(), original_parts=parts, scope=scope, bundle=bundle,
            metric_ctx=metric_ctx, quoted=flags[-1],
        )
    current, effective_hop_path = _walk_relationship_chain(
        hop_path=path[:-1], host=host, bundle=bundle, parts=parts, leaf=path[-1], quoted=flags,
        routes_short_form=False,
        anchor=(_quoted_segment(parts[0], quoted=quoted[0]), len(path) - len(parts) + 1),
    )
    if TIME_SPINE_DATASET in effective_hop_path[len(path) - len(parts) + 1:]:  # hops after the anchor's route
        raise UnresolvableDimensionRelationshipError(
            reference=".".join(parts), root_dataset=host.spelling,
            reason=f"a route never crosses {TIME_SPINE_DATASET!r}, which is read only as an anchor.",
            suggested_path=f"{TIME_SPINE_DATASET}.{path[-1]}",
        )
    return _resolve_terminal_leaf(
        current=current, leaf=path[-1], hop_path=effective_hop_path, original_parts=parts, scope=scope,
        bundle=bundle, metric_ctx=metric_ctx, quoted=flags[-1],
    )


def _resolve_terminal_leaf(
    *,
    current: SlayerDataset,
    leaf: str,
    hop_path: Tuple[str, ...],
    original_parts: Tuple[str, ...],
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    metric_ctx: Optional[MetricResolutionCtx],
    quoted: bool = False,
) -> ValueKey:
    """Resolve ``leaf`` on terminal dataset ``current``: field (plain or derived)
    → saved metric (re-anchored into host coords) → unresolved error."""
    col = resolve_generated_field(dataset=current, name=leaf, quoted=quoted)
    if col is not None:
        _check_evaluable(col, dataset=current)
        if col.needs_expansion:
            # Derived / filtered field on a joined dataset — path is part of the
            # key so the cross-dataset planner can route via the relationship graph.
            return FieldSqlKey(
                path=tuple(hop_path), dataset=current.name, field_name=col.name,
            )
        return FieldKey(path=tuple(hop_path), leaf=col.name)
    mm = current.get_metric(leaf, quoted=quoted)
    if mm is not None:
        return _resolve_saved_metric(
            metric=mm, terminal_dataset=current, scope=scope, bundle=bundle,
            metric_ctx=metric_ctx, ref_text=".".join(original_parts),
            host_path=tuple(hop_path),
        )
    raise _unresolved_dotted_error(parts=original_parts, terminal_dataset=current, bundle=bundle)


_METRIC_LEGAL_POSITIONS = (
    "saved metrics may be referenced only in a metric expression or a "
    "computed dimension expression"
)


def _ineligible_saved_metric_error(
    *, ref_text: str, dataset_name: str,
) -> UnknownReferenceError:
    """Error: a saved-metric reference where metrics are not legal."""
    return UnknownReferenceError(
        name=ref_text,
        scope_kind="DatasetScope",
        scope_summary=f"dataset {dataset_name!r}",
        suggestion=(
            f"{ref_text!r} is a saved metric on {dataset_name!r}, not a field, "
            f"so it takes no aggregation. {_METRIC_LEGAL_POSITIONS}; reference "
            f"it there as {ref_text!r}."
        ),
    )


def _unresolved_dotted_error(
    *, parts: Tuple[str, ...], terminal_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
) -> UnknownReferenceError:
    """Error: a dotted leaf matching neither field nor saved metric; names
    both namespaces and offers close matches."""
    dotted = ".".join(parts)
    fields = [c.name for c in terminal_dataset.fields]
    metrics = [m.name for m in terminal_dataset.metrics if m.name]
    # A list child's identity is minted: name it by the list path the user wrote.
    label = ".".join(parts[:-1]) if terminal_dataset.list_of is not None else terminal_dataset.spelling
    detail = (
        f"{dotted!r} is neither a field nor a saved metric on "
        f"{label!r}. Fields: {fields}; saved metrics: {metrics}."
    )
    match = _name_suggestion(name=parts[-1], dataset=terminal_dataset)
    if match:
        detail = f"{detail} {match}"
    return UnknownReferenceError(
        name=dotted,
        scope_kind="DatasetScope",
        scope_summary=(f"dataset {label!r}" if terminal_dataset.list_of is not None
                       else _dataset_label(terminal_dataset, bundle=bundle)),
        suggestion=detail,
    )


def _rerooted_bundle(
    *, bundle: ResolvedSourceBundle, target: SlayerDataset,
) -> ResolvedSourceBundle:
    """A copy of ``bundle`` re-rooted at ``target`` (same referenced datasets), so
    a metric expression binds its filters / gates against the owning dataset."""
    others = [m for m in bundle.referenced_datasets if m.name != target.name]
    return bundle.model_copy(update={
        "population": target,
        "referenced_datasets": [target] + others,
    })


def _resolve_saved_metric(
    *,
    metric: DatasetMetric,
    terminal_dataset: SlayerDataset,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    metric_ctx: Optional[MetricResolutionCtx],
    ref_text: str,
    host_path: Tuple[str, ...],
) -> ValueKey:
    """Resolve a saved ``DatasetMetric`` into a bound ``ValueKey``. ``host_path``
    is the host→terminal relationship path (``()`` for bare/local); a cross-dataset
    metric binds against the target then prepends ``host_path``. Raises the
    ineligible-position error when ``metric_ctx`` is None."""
    _check_evaluable(metric, dataset=terminal_dataset)
    if metric_ctx is None:
        raise _ineligible_saved_metric_error(
            ref_text=ref_text, dataset_name=terminal_dataset.spelling,
        )
    assert metric.name is not None  # a saved metric is named
    step = (terminal_dataset.name, metric.name)
    if step in metric_ctx.chain:
        raise MetricCycleError(
            chain=_fmt_metric_chain(metric_ctx.chain + (step,)),
        )
    child = metric_ctx.descend(dataset=terminal_dataset.name, metric=metric.name)
    if len(child.chain) > metric_ctx.depth_limit:
        raise MetricRecursionLimitError(
            chain=_fmt_metric_chain(child.chain), limit=metric_ctx.depth_limit,
        )
    records = _bind_records.get()
    if records is not None:
        records.metrics[(terminal_dataset.name, metric.name)] = None
    parsed = parse_bound_expr(metric.text, metric.bound_variables)
    _reject_placeholders(parsed, text=metric.text)
    if metric.name not in terminal_dataset.query_local_names:
        # A stored definition never sees a query's extensions.
        bundle = bundle.definition_view()
        terminal_dataset = terminal_dataset.definition_view()
        if scope.population is not None:
            scope = DatasetScope(population=scope.population.definition_view())
    elif scope.anchored:
        scope = DatasetScope(population=scope.population)
    if not host_path:
        # Bare/local: metric lives on the host; bind inline at this scope.
        return _bind(
            parsed, scope=scope, bundle=bundle, in_filter=False, metric_ctx=child,
        )
    # Cross-dataset: bind against the target, then prepend into host coordinates.
    host_dataset = scope.population
    assert host_dataset is not None
    target_scope = DatasetScope(population=terminal_dataset)
    target_bundle = _rerooted_bundle(bundle=bundle, target=terminal_dataset)
    bound = _bind(
        parsed, scope=target_scope, bundle=target_bundle, in_filter=False,
        metric_ctx=child,
    )
    return prepend_value_key(bound, host_path=host_path)


def _resolve_dotted_star(
    parts: Tuple[str, ...],
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    quoted: Tuple[bool, ...],
) -> StarKey:
    """Resolve a dotted star (``customers.*``) to a ``StarKey`` whose ``path`` is
    the validated hop chain (empty path = local star)."""
    assert parts and parts[-1] == "*"
    if is_anchored(scope):
        return _resolve_anchored_star(parts, scope=scope, bundle=bundle, quoted=quoted)
    original_parts = parts
    parts = drop_root_qualifier(parts, root_spelling(scope), quoted=quoted)
    quoted = quoted[len(original_parts) - len(parts):]
    if parts == ("*",):
        return StarKey()
    host = scope.population
    if host is None:
        raise UnknownReferenceError(
            name=".".join(parts),
            scope_kind="DatasetScope",
            scope_summary=_NO_ANCHOR_SCOPE_SUMMARY,
            suggestion=None,
        )
    hop_path = parts[:-1]
    # Validate the hop chain (raises on missing / circular relationship, auto-routes a
    # short form); the routed effective path carries the star.
    _, effective_hop_path = _walk_relationship_chain(
        hop_path=hop_path, host=host, bundle=bundle, parts=parts, leaf="*", quoted=quoted,
    )
    return StarKey(path=tuple(effective_hop_path))


def _resolve_anchored_star(
    parts: Tuple[str, ...], *, scope: DatasetScope, bundle: ResolvedSourceBundle, quoted: Tuple[bool, ...],
) -> StarKey:
    """A query's ``<anchor>[.<route>].*``."""
    quoted = tuple(quoted[:len(parts)]) + (False,) * max(0, len(parts) - len(quoted))
    host = scope.population
    if host is None:
        raise UnknownReferenceError(
            name=".".join(parts), scope_kind="DatasetScope", scope_summary=_NO_ANCHOR_SCOPE_SUMMARY, suggestion=None,
        )
    path, flags = _anchored_parts(parts, scope=scope, bundle=bundle, quoted=quoted)
    if len(path) == 1:
        return StarKey()
    _, effective_hop_path = _walk_relationship_chain(
        hop_path=path[:-1], host=host, bundle=bundle, parts=parts, leaf="*", quoted=flags, routes_short_form=False,
    )
    return StarKey(path=tuple(effective_hop_path))


def _bare_star(*, scope: DatasetScope) -> StarKey:
    """A bare ``*``: refused in a query's own scope, where a row count names its dataset."""
    if is_anchored(scope):
        raise unqualified_count(root_spelling(scope))
    return StarKey()


def _bind_partition_keys(
    value, *,
    scope: DatasetScope,
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
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
) -> ArithmeticKey | ScalarCallKey | LiteralKey | TimeTruncKey | InKey | TimePointCmpKey | CastKey | ListSetKey:
    """Bind a scalar-expression aggregate source.

    Dotted joined-dataset leaves and operands carrying ``DatasetField.filter`` are both
    admitted: the home rule roots the aggregation and the filter
    desugars to ``CASE WHEN``. The source must still resolve to a row-level
    expression (a field, star, or arithmetic/scalar/predicate composite of them)."""
    bound = _bind(parsed_source, scope=scope, bundle=bundle, in_filter=False)
    if not isinstance(bound, EXPRESSION_SOURCE_KINDS):
        raise _not_an_aggregation_source(bound, agg=agg)
    return bound


def _not_an_aggregation_source(bound: ValueKey, *, agg: str) -> AggregationNotAllowedError:
    return AggregationNotAllowedError(
        field=key_display(bound), agg=agg,
        reason=f"an aggregation source must be a field, star, or a row-level expression; "
        f"got {type(bound).__name__}.",
    )


# Null-handling / min-max scalars whose result class follows their arguments.
_ARG_CLASS_SCALARS = frozenset({
    "coalesce", "ifnull", "nullif", "greatest", "least",
})
def _expression_is_confidently_text(key, *, dataset: Optional[SlayerDataset]) -> bool:
    """Best-effort: True only when the expression's value is certainly text."""
    if isinstance(key, str):
        return True
    if isinstance(key, ScalarCallKey):
        if key.name in TEXT_RESULT_SCALARS:
            return True
        if key.name in _ARG_CLASS_SCALARS:
            return any(
                _expression_is_confidently_text(a, dataset=dataset)
                for a in key.args
            )
        return False
    if isinstance(key, LiteralKey):
        return isinstance(key.value, str)
    if isinstance(key, FieldKey) and not key.path and dataset is not None:
        col = dataset.get_field(key.leaf)
        return col is not None and col.datatype == DataType.STRING
    if isinstance(key, FieldSqlKey) and not key.path and dataset is not None:
        col = dataset.get_field(key.field_name)
        return col is not None and col.datatype == DataType.STRING
    # Arithmetic coerces numeric; anything unresolved defaults to numeric.
    return False


def _reject_non_numeric_expression_agg(
    *, source: ValueKey, agg: str,
    scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> None:
    """A numeric-only aggregation over a text or temporal expression fails here, not in the database."""
    if agg not in NUMERIC_ONLY_AGGREGATIONS:
        return
    field_type = scope_field_type(scope=scope, bundle=bundle)
    if boolean_valued(source, field_type=field_type):
        return
    if (temporal := temporal_type(source, field_type=field_type)) is not None:
        kind = temporal.value
    elif _expression_is_confidently_text(
        source, dataset=scope.population,
    ):
        kind = "text"
    else:
        return
    remedy = " (numbers stored as text: TRY_CAST(x AS DOUBLE))" if kind == "text" else ""
    raise AggregationNotAllowedError(
        field=key_display(source), agg=agg,
        reason=f"{agg!r} requires a numeric value, but the aggregated expression is non-numeric ({kind}). Use a "
        f"counting or min/max aggregation, or make the expression numeric{remedy}.",
    )


def _callee_granularity(name: str, *, bundle: Optional[ResolvedSourceBundle]) -> Optional[Granularity]:
    """The granularity a call's callee names (built-in or the datasource's), else ``None``."""
    return resolve_granularity(name, defined=bundle.granularities if bundle is not None else {})


def _source_is_reaggregation(node) -> bool:
    """Whether a parsed aggregation source carries an attached value — a nested
    AggCall or a grained transform, alone or composed. Such a source is bound
    structurally (its inner AggCalls / TransformCalls become nested keys) whether
    it is a pure re-aggregation or a row-grain mix."""
    if isinstance(node, (AggCall, TransformCall)):
        return True
    if isinstance(node, (Arith, Cmp)):
        return _source_is_reaggregation(node.left) or _source_is_reaggregation(node.right)
    if isinstance(node, ScalarCall):
        return any(_source_is_reaggregation(a) for a in node.args)
    if isinstance(node, (UnaryOp, Cast)):
        return _source_is_reaggregation(node.operand if isinstance(node, UnaryOp) else node.source)
    if isinstance(node, DateTrunc):
        return _source_is_reaggregation(node.value)
    if isinstance(node, BoolOp):
        return any(_source_is_reaggregation(o) for o in node.operands)
    return False


def _bind_agg_call(
    parsed: AggCall, *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    metric_ctx: Optional[MetricResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> AggregateKey | TransformKey | TimeTruncKey:
    """Bind an ``AggCall`` (a granularity called like a function is refused); ``first`` / ``last`` dispatch by the bound operand's
    type — attached → the series transform, row grain → the ranked aggregation."""
    if parsed.agg.lower() in RESERVED_NAMES:
        raise ReservedFunctionNameError(name=parsed.agg, where=f"{canonical_metric_text(parsed)}")
    if parsed.filter is not None:
        return _bind_filtered_agg(
            parsed, scope=scope, bundle=bundle, alias_map=alias_map, metric_ctx=metric_ctx,
            dim_alias_map=dim_alias_map,
        )
    if parsed.library_error is not None and _owner_definition(
        parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
    ) is None:
        raise (GranularityCallError if parsed.agg.lower() == "date_trunc" else ValueError)(parsed.library_error)
    parsed = _classified(parsed, bundle=bundle)
    _refuse_granularity_call(parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map)
    op = normalize_aggregation_name(parsed.agg)
    custom = [a.name for m in _bundle_datasets(bundle) for a in m.aggregations]
    if op not in BUILTIN_AGGREGATIONS and match_name(name=parsed.agg, declared=custom, quoted=False) is None:
        raise _unknown_function_error(parsed.agg, aggregations=BUILTIN_AGGREGATIONS | set(custom), bundle=bundle)
    if op in RANKED_AGGREGATIONS:
        operand = _bind_transform_input(
            parsed.source, scope=scope, bundle=bundle, alias_map=alias_map,
            metric_ctx=metric_ctx, dim_alias_map=dim_alias_map,
        )
        owner, _leaf = _resolve_agg_owner(operand, bundle)
        if is_attached_source(operand) and builtin_semantics(aggregation_definition(owner=owner, agg=parsed.agg)):
            return _bind_transform_params(
                op=op, inp=operand, args=parsed.args, kwargs=parsed.kwargs,
                scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
            )
    return _bind_agg(parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map)


def _refuse_granularity_call(
    parsed: AggCall, *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
) -> None:
    """A granularity called like a function, unless an aggregation of that name shadows it, is refused."""
    granularity = _callee_granularity(parsed.agg, bundle=bundle)
    if granularity is not None and _owner_definition(
        parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
    ) is None:
        raise GranularityCallError.no_call_form(
            str(granularity), operand=", ".join(canonical_metric_text(a) for a in (parsed.source, *parsed.args)),
        )


_FILTER_CLAUSE = "FILTER (WHERE …)"


def _filter_error(reason: str) -> AggregationFilterError:
    return AggregationFilterError(
        summary=f"{_FILTER_CLAUSE} {reason}",
        location=_FILTER_CLAUSE,
        suggestion="filter on fields of the aggregated rows, e.g. sum(orders.amount) FILTER (WHERE orders.status = 'paid').",
    )


def _bind_filtered_agg(
    parsed: AggCall, *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    metric_ctx: Optional[MetricResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> AggregateKey:
    """``agg(…) FILTER (WHERE c)``: the aggregation over the rows where ``c`` holds. A built-in masks
    every row-level input by ``c``; a star source, a ranked pick and a non-built-in carry ``c`` on the key."""
    condition = _bind(parsed.filter, scope=scope, bundle=bundle, in_filter=False, dim_alias_map=dim_alias_map)
    if any(isinstance(k, (AggregateKey, TransformKey)) for k in walk_value_keys(condition)):
        raise _filter_error("needs a row-level condition; it cannot read an aggregate, a transform or a metric.")
    key = _bind_agg_call(
        parsed.model_copy(update={"filter": None}), scope=scope, bundle=bundle, alias_map=alias_map,
        metric_ctx=metric_ctx, dim_alias_map=dim_alias_map,
    )
    if not isinstance(key, AggregateKey) or is_attached_source(key.source):
        raise _filter_error("applies only to an aggregation over rows.")
    if is_ranked(key):
        return _carrying_filter(key, condition=condition)
    masked = input_masked(key, condition=condition)
    if source_anchor_path(masked.source) == source_anchor_path(key.source):
        return masked if key.builtin else key.model_copy(update={"filter": condition})
    if isinstance(key.source, StarKey) and not source_anchor_path(key.source):
        # The condition reads a joined dataset: the star keeps the count's home.
        return _carrying_filter(key, condition=condition) if key.builtin else key.model_copy(
            update={"filter": condition})
    raise _filter_error("must read the aggregated rows' own dataset or datasets joined to it one-to-one.")


def _carrying_filter(key: AggregateKey, *, condition: ValueKey) -> AggregateKey:
    if window_kwarg_of(key) is not None:
        raise _filter_error("cannot combine with window= here; filter the query's rows instead.")
    return key.model_copy(update={"filter": condition})


def _bind_date_trunc(parsed: DateTrunc, *, source: ValueKey, bundle: ResolvedSourceBundle) -> TimeTruncKey:
    """``date_trunc('<unit>', expr)`` as the row-level bucket of ``expr``."""
    granularity = resolve_granularity(parsed.unit, defined=bundle.granularities)
    if granularity is None:
        raise UnknownGranularityError(summary=unknown_granularity_message(
            name=parsed.unit, defined=bundle.granularities.values(), where=canonical_metric_text(parsed),
        ))
    if source.phase is not Phase.ROW or isinstance(source, SqlFragmentKey) or any(
        isinstance(k, (AggregateKey, TransformKey)) for k in walk_value_keys(source)
    ):
        raise GranularityCallError.wrong_shape(canonical_metric_text(parsed))
    return TimeTruncKey(source=source, granularity=granularity)


def _aggregation_parameters(agg: str, *, bundle: ResolvedSourceBundle) -> List[str]:
    """The keyword names a call of ``agg`` may take: a built-in's, or any dataset definition's, plus the routing ones."""
    names = ["partition_by", "window", *BUILTIN_AGGREGATION_PARAM_ORDER.get(normalize_aggregation_name(agg), ())]
    for dataset in _bundle_datasets(bundle):
        definition = aggregation_definition(owner=dataset, agg=agg)
        if definition is None:
            continue
        names += _declared_agg_param_names(agg=definition.name, source=None, bundle=bundle, definition=definition)
        names += _expression_placeholders(definition, dialect=bundle.dialect)
    return list(dict.fromkeys(names))


def _expression_placeholders(definition: Aggregation, *, dialect: str) -> List[str]:
    """The placeholders ``definition``'s expression reads on ``dialect`` or as generic SQL (a dialect may hide one)."""
    try:
        names = sorted(aggregation_reads(agg=definition.name, definition=definition, dialect=dialect))
    except SqlTemplateError as e:
        raise SqlTemplateError(f"Aggregation '{definition.name}': {e}") from e
    with contextlib.suppress(SqlTemplateError):
        names += sorted(aggregation_reads(agg=definition.name, definition=definition, dialect=""))
    return names


def _classified(parsed: AggCall, *, bundle: ResolvedSourceBundle) -> AggCall:
    """``parsed`` with each ``name = value`` argument a keyword (a parameter of the aggregation) or a comparison."""
    items = (parsed.source, *parsed.args)
    if not any(isinstance(i, NamedArg) for i in items):
        return parsed
    positional, keywords = classify_arguments(
        items, parameters=_aggregation_parameters(parsed.agg, bundle=bundle), call=parsed.agg,
    )
    if not positional:
        raise AggregationArgumentError(
            f"Aggregation '{parsed.agg}' requires a value argument, e.g. {parsed.agg}(field)."
        )
    return parsed.model_copy(update={
        "source": positional[0], "args": positional[1:], "kwargs": (*parsed.kwargs, *keywords),
    })


def _bind_agg_source(
    parsed: AggCall, *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
) -> AggregateSource:
    if _source_is_reaggregation(parsed.source):
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
        return _bare_star(scope=scope)
    if (
        isinstance(parsed.source, DottedRef)
        and parsed.source.parts
        and parsed.source.parts[-1] == "*"
    ):
        # Cross-dataset star: ``count(customers.*)`` → a StarKey carrying the relationship
        # path so the planner routes COUNT(*) through the relationship graph.
        return _resolve_dotted_star(
            parsed.source.parts, scope=scope, bundle=bundle, quoted=dotted_quoting(parsed.source),
        )
    if isinstance(parsed.source, (Ref, DottedRef)):
        bound_source = _bind(
            parsed.source, scope=scope, bundle=bundle, in_filter=False,
        )
        if not isinstance(bound_source, (FieldKey, FieldSqlKey, StarKey)):
            raise ValueError(
                f"Aggregation source must resolve to a field / star, "
                f"got {type(bound_source).__name__}."
            )
        return bound_source
    # Same-dataset scalar EXPRESSION source (``sum(amount - cost)``).
    return _bind_expression_agg_source(
        parsed.source, agg=parsed.agg, scope=scope, bundle=bundle,
    )


def _bind_agg(
    parsed: AggCall, *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> AggregateKey:
    source = _bind_agg_source(parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map)

    # ``partition_by`` is lifted out of kwargs onto ``partition_keys``
    # (``None`` means no partition, ``[]`` means grand total).
    # Gate per-field aggregation eligibility, then store the EFFECTIVE
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
    _check_slotless_comparisons(parsed, agg=effective_agg, source=source, bundle=bundle)
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
    # confidently non-numeric (per-field gates don't apply).
    if isinstance(source, EXPRESSION_SOURCE_KINDS):
        _reject_non_numeric_expression_agg(
            source=source, agg=effective_agg, scope=scope, bundle=bundle,
        )
    kwargs = bind_aggregation_params(
        agg=effective_agg, source=source, kwargs=kwargs, bundle=bundle,
        bind_quoted=lambda text: _bind_quoted_arg(text, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map),
    )
    owner, _leaf = _resolve_agg_owner(source, bundle)
    return AggregateKey(
        source=source,
        agg=effective_agg,
        args=args,
        kwargs=kwargs,
        partition_keys=partition_keys,
        builtin=builtin_semantics(aggregation_definition(owner=owner, agg=effective_agg)),
    )


def _owner_definition(
    parsed: AggCall, *, scope: DatasetScope, bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> Optional[Aggregation]:
    """The aggregation ``parsed.agg`` its field-reading source's owner declares, else ``None``."""
    if all(aggregation_definition(owner=m, agg=parsed.agg) is None for m in _bundle_datasets(bundle)):
        return None
    try:
        source = _bind_agg_source(
            _classified(parsed, bundle=bundle), scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
        )
    except (ValueError, LookupError):
        return None
    if not any(isinstance(k, (FieldKey, FieldSqlKey)) for k in walk_value_keys(source)):
        return None
    owner, _leaf = _resolve_agg_owner(source, bundle)
    return aggregation_definition(owner=owner, agg=parsed.agg)


def _bundle_datasets(bundle: ResolvedSourceBundle) -> List[SlayerDataset]:
    return [*bundle.datasets_by_name.values(), *([bundle.population] if bundle.population is not None else [])]


def _record_shadow(*, owner: SlayerDataset, definition: Aggregation, written: str) -> None:
    records = _bind_records.get()
    if records is not None and shadows_library_function(definition):
        records.shadows[(owner.name, definition.name, written)] = LibraryNameShadowWarning(
            dataset=owner.name, aggregation=definition.name, function=written,
        )


def _resolve_agg_owner(
    source: ValueKey, bundle: ResolvedSourceBundle,
) -> "tuple[Optional[SlayerDataset], Optional[str]]":
    """``(owning_dataset, gate_leaf)`` for an aggregate source; ``(None, None)`` when
    the owner can't be confirmed (no host dataset, unresolved relationship hop)."""
    owner = aggregation_owner(
        root=bundle.population, source=source, datasets_by_name=bundle.datasets_by_name,
    )
    if owner is None:
        return None, None
    return owner, getattr(source, "leaf", None) or getattr(source, "field_name", None)


def _declared_agg_param_names(
    *, agg: str, source, bundle: ResolvedSourceBundle, definition: Optional[Aggregation] = None,
) -> List[str]:
    """Declared parameter order for ``agg`` — ``definition``, else the owning dataset's custom
    definition, wins over the built-in registry; ``[]`` when none declared."""
    custom = definition
    if custom is None:
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


def _check_slotless_comparisons(parsed: AggCall, *, agg: str, source, bundle: ResolvedSourceBundle) -> None:
    """A ``name = value`` comparison past ``agg``'s positional slots fails as the argument it was likely meant as."""
    owner, _leaf = _resolve_agg_owner(source, bundle)
    ranked = agg in RANKED_AGGREGATIONS and builtin_semantics(aggregation_definition(owner=owner, agg=agg))
    slots = len(_declared_agg_param_names(agg=agg, source=source, bundle=bundle)) or int(ranked)
    name = next((n for n in map(keyword_spelling, parsed.args[slots:]) if n is not None), None)
    if name is None:
        return
    accepted = sorted({*_aggregation_parameters(agg, bundle=bundle)} - {VALUE_PLACEHOLDER, "partition_by"})
    takes = "takes no args or kwargs other than window" if accepted == ["window"] else (
        f"takes no further value; its keywords: {', '.join(accepted)}"
    )
    raise AggregationArgumentError(
        f"Aggregation '{agg}' {takes}. `{name} = …` names none of its parameters, so it reads as a comparison."
    )


def _check_ranking_positional(*, agg: str, source, bundle: ResolvedSourceBundle, args: tuple) -> None:
    """A parameterless aggregation's positionals: only a built-in ranked one's single row-level ranking key."""
    owner, _leaf = _resolve_agg_owner(source, bundle)
    if agg not in RANKED_AGGREGATIONS or not builtin_semantics(aggregation_definition(owner=owner, agg=agg)):
        raise ValueError(
            f"Aggregation {agg!r} takes no parameters; got {len(args)} "
            f"positional value(s)."
        )
    if len(args) != 1:
        raise ValueError(
            f"Aggregation {agg!r} ranks by at most one field or row-level expression; got "
            f"{len(args)} positional value(s)."
        )
    if not _is_row_level_ranking_key(args[0]):
        raise ValueError(
            f"Aggregation {agg!r} ranks by a field or a row-level expression of fields; got "
            f"{key_display(args[0]) if isinstance(args[0], VALUE_KEY_TYPES) else repr(args[0])} "
            f"as its ranking key."
        )


def _fold_positional_agg_args(
    *, agg: str, source, bundle: ResolvedSourceBundle, args: tuple, kwargs: tuple,
) -> "tuple[tuple, tuple]":
    """Fold positional call values onto declared parameter names, Python-call
    style, so ``percentile(x, 0.9)`` interns identically to ``p=0.9``. Ranked
    ``first``/``last`` declare no parameters — their optional positional ranking
    field stays in ``args``; any other parameterless aggregation takes no
    positional, so an attached (aggregate-valued) parameter is always a kwarg
    after binding."""
    if not args:
        return args, kwargs
    names = _declared_agg_param_names(agg=agg, source=source, bundle=bundle)
    if not names:
        _check_ranking_positional(agg=agg, source=source, bundle=bundle, args=args)
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
    """A ranking key reads at least one field and no aggregate, transform or attached value."""
    return isinstance(key, VALUE_KEY_TYPES) and not _carries_attached_value(cast(ValueKey, key)) and any(
        isinstance(k, (FieldKey, FieldSqlKey)) for k in walk_value_keys(cast(ValueKey, key))
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
    """Heal the aggregation name, validate it globally, and enforce per-field
    eligibility gates, returning the effective (alias-healed) name for
    ``AggregateKey.agg``.

    Healing is skipped when the raw token exactly matches a custom aggregation
    on the owning dataset (a custom ``countd`` wins over the alias). Gate order:
    0. unknown-name-first, for EVERY source shape (field, star, expression),
    so ``bogus(*)`` never escapes to SQL generation;
    1. a sole primary key (an identifier) restricted to
    ``PRIMARY_KEY_AGGREGATIONS`` — composite-key members fall through; 2. explicit
    ``DatasetField.allowed_aggregations`` whitelist; 3. else
    ``DEFAULT_AGGREGATIONS_BY_TYPE`` (custom aggregations exempt).

    Star and expression sources have no field, so the per-field gates (1-3)
    don't apply (gate 0 still validates the name); unresolvable targets pass
    through (best-effort).
    """
    owner_dataset, leaf = _resolve_agg_owner(source, bundle)
    if owner_dataset is None:
        return _known_aggregation(normalize_aggregation_name(agg), BUILTIN_AGGREGATIONS, bundle=bundle)
    # Alias healing — custom aggregation named like an alias wins.
    custom_names = {a.name for a in (owner_dataset.aggregations or [])}
    definition = aggregation_definition(owner=owner_dataset, agg=agg)
    if definition is not None:
        _record_shadow(owner=owner_dataset, definition=definition, written=agg)
    effective = definition.name if definition is not None else normalize_aggregation_name(agg)
    # Gate 0: unknown-name-first (precedence over PK / whitelist / type).
    _known_aggregation(effective, BUILTIN_AGGREGATIONS | custom_names, bundle=bundle)
    col = None if leaf is None else next((c for c in owner_dataset.fields if c.name == leaf), None)
    if col is None:
        return effective
    reason = _field_agg_refusal(
        col=col, fields=owner_dataset.fields, agg=effective, custom=effective in custom_names,
    )
    if reason is not None and col.datatype is DataType.ARRAY:
        raise ListTypeError(
            summary=f"`{col.name}` is a list: an aggregation over a list is only count or count_distinct; {effective}() "
            f"cannot take it.",
            suggestion=f"Aggregate its elements: {effective}({col.name}.value).",
        )
    if reason is not None:
        raise AggregationNotAllowedError(field=col.name, agg=effective, reason=reason)
    return effective


def _known_aggregation(name: str, known, *, bundle: ResolvedSourceBundle) -> str:
    if name not in known:
        raise _unknown_function_error(name, aggregations=known, bundle=bundle)
    return name


def _field_agg_refusal(*, col: DatasetField, fields, agg: str, custom: bool) -> Optional[str]:
    """Why gates 1-3 refuse ``agg`` on ``col``, else ``None``."""
    leaf = col.name
    if is_identifier(field=col, fields=fields):
        if agg in PRIMARY_KEY_AGGREGATIONS:
            return None
        return f"primary-key field {leaf!r} restricted to {sorted(PRIMARY_KEY_AGGREGATIONS)}; got {agg!r}."
    if col.allowed_aggregations is not None:
        if agg in col.allowed_aggregations or (agg in _RANGE_AGGREGATIONS and _RANGE_READ.get()):
            return None
        return (f"field {leaf!r} restricts allowed_aggregations to "
                f"{sorted(col.allowed_aggregations)}; got {agg!r}.")
    # Dataset-custom aggregations, and fields of a type still unknown, are exempt from the type-default gate.
    if col.datatype is None:
        return None
    allowed = DEFAULT_AGGREGATIONS_BY_TYPE.get(col.datatype, frozenset())
    if custom or agg in allowed:
        return None
    remedy = (
        f" For numbers stored as text, convert them: {agg}(TRY_CAST({leaf} AS DOUBLE))."
        if col.datatype is DataType.STRING and agg in NUMERIC_ONLY_AGGREGATIONS else ""
    )
    if col.datatype is DataType.ARRAY:
        remedy = f" {leaf!r} is a list; aggregate its elements: {agg}({leaf}.value)."
    return (f"aggregation {agg!r} is not applicable to {col.datatype} field {leaf!r}; "
            f"default aggregations are {sorted(allowed)}.{remedy}")


def _bind_quoted_arg(
    text: str, *, scope: DatasetScope, bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
):
    """A quoted aggregation argument read as Mode B and bound as its unquoted spelling; a row expression only."""
    try:
        parsed = parse_expr(text)
    except ValueError as exc:
        raise QuotedArgumentRefused(f"it is not a SLayer expression: {exc}") from exc
    if any(isinstance(node, (AggCall, TransformCall)) for node in walk_parsed(parsed)):
        raise QuotedArgumentRefused("it contains an aggregate")
    return _bind_agg_arg(parsed, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map)


def _bind_agg_arg(
    parsed: ParsedExpr, *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
):
    """Bind one aggregation arg: a nested aggregate → ``AggregateKey`` (aggregate-valued
    parameter), a grained transform → ``TransformKey`` at its result grain, literals → inline
    scalar via ``normalize_scalar`` (stored inline, not as LiteralKey), anything else as a
    Mode-B expression (a field being the one-leaf case).
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
        # INPUT only, no alias_map / measure_ctx (a metric is illegal inside an aggregation).
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
    *, op: str, args: Tuple[ParsedExpr, ...], kwargs: Tuple[Tuple[str, ParsedExpr], ...], hint: str = "",
) -> List:
    """Map a transform's extra positional params onto kwarg names; most transforms
    are keyword-only after the value."""
    if not args:
        return []
    pos_names = _TRANSFORM_POSITIONAL_KWARGS.get(op)
    if pos_names is None:
        accepted = sorted(_transform_parameters(op))
        rest = f"pass {', '.join(accepted)} by keyword" if accepted else "it takes no other arguments"
        raise ValueError(
            f"Transform {op!r} accepts exactly one positional argument (the value to transform); {rest}.{hint}"
        )
    if len(args) > len(pos_names):
        raise ValueError(
            f"Transform {op!r} accepts at most {len(pos_names)} "
            f"positional argument(s) after the value "
            f"({', '.join(pos_names)}); got {len(args)}.{hint}"
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
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    metric_ctx: Optional[MetricResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> TransformKey:
    parameters = _transform_parameters(parsed.op)
    stray = next((a.name for a in parsed.args if isinstance(a, NamedArg)
                  and match_name(name=a.name, declared=parameters, quoted=False) is None), None)
    positional, keywords = classify_arguments((parsed.input, *parsed.args), parameters=parameters, call=parsed.op)
    if not positional:
        raise TransformArgumentError(summary=f"Transform {parsed.op!r} requires the value to transform first.")
    inp = _bind_transform_input(
        positional[0], scope=scope, bundle=bundle, alias_map=alias_map,
        metric_ctx=metric_ctx, dim_alias_map=dim_alias_map,
    )
    return _bind_transform_params(
        op=parsed.op, inp=inp, args=positional[1:], kwargs=(*parsed.kwargs, *keywords),
        scope=scope, bundle=bundle, dim_alias_map=dim_alias_map, stray=stray,
    )


def _stray_hint(op: str, stray: Optional[str]) -> str:
    """Why a comparison reached ``op``'s arguments: ``stray = …`` names none of its parameters."""
    if stray is None:
        return ""
    accepted = ", ".join(sorted(_transform_parameters(op))) or "none"
    return f" `{stray} = …` is no parameter of {op}, so it reads as a comparison; its keywords: {accepted}."


def _transform_parameters(op: str) -> frozenset[str]:
    """The keyword names transform ``op`` takes."""
    names = _TRANSFORM_KWARG_RULES.get(op, frozenset()) | frozenset(_TRANSFORM_POSITIONAL_KWARGS.get(op, ()))
    return names | {"partition_by", "direction"} if op in RANK_FAMILY_TRANSFORMS else names


def _bind_transform_input(
    parsed: ParsedExpr, *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]],
    metric_ctx: Optional[MetricResolutionCtx],
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
) -> ValueKey:
    # ``metric_ctx`` rides the transform INPUT only — partition_by / scalar
    # kwargs drop it.
    return _bind(
        parsed, scope=scope, bundle=bundle, in_filter=False,
        alias_map=alias_map, metric_ctx=metric_ctx, dim_alias_map=dim_alias_map,
    )


def _bind_transform_params(
    *, op: str, inp: ValueKey,
    args: Tuple[ParsedExpr, ...], kwargs: Tuple[Tuple[str, ParsedExpr], ...],
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
    stray: Optional[str] = None,
) -> TransformKey:
    positional_pairs = _transform_positional_pairs(op=op, args=args, kwargs=kwargs, hint=_stray_hint(op, stray))
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
                f"{type(v).__name__}.{_stray_hint(op, stray)}"
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
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    metric_ctx: Optional[MetricResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> ValueKey:
    reading = aggregation_reading(parsed)
    if reading is not None and _owner_definition(
        reading, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
    ) is not None:
        return _bind_agg_call(
            reading, scope=scope, bundle=bundle, alias_map=alias_map, metric_ctx=metric_ctx,
            dim_alias_map=dim_alias_map,
        )
    spelled = [n for n in map(keyword_spelling, parsed.args) if n is not None]
    _check_scalar_call(parsed, spelled=spelled, bundle=bundle)

    def bind(a: ParsedExpr, *, in_temporal_slot: bool) -> ValueKey:
        key = _bind(
            a, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map,
            metric_ctx=metric_ctx, dim_alias_map=dim_alias_map,
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


def _check_scalar_call(parsed: ScalarCall, *, spelled: list[str], bundle: ResolvedSourceBundle) -> None:
    """An allowlisted name, its arity and its literal-only arguments; ``spelled`` are ``name = value`` arguments."""
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
            f"{canonical_metric_text(parsed.args[1])!r}."
        )
    _check_library_literals(parsed, bundle=bundle)


def _text_literal(parsed: ScalarCall, pos: int) -> str:
    """The string literal at ``pos`` of a library call's text slot (pattern, replacement, path)."""
    value = _fold_to_scalar(parsed.args[pos])
    if not isinstance(value, str):
        slot = slot_name(parsed.name, pos)
        raise ScalarArgumentError(
            summary=f"the {slot} of {parsed.name}() must be a string literal or a {{variable}}; got "
            f"`{canonical_metric_text(parsed.args[pos])}`.",
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
    check_domain(name, [_fold_to_scalar(arg) for arg in args], texts=[canonical_metric_text(arg) for arg in args])


def _json_input(arg: ValueKey, *, scope: DatasetScope, bundle: ResolvedSourceBundle) -> ValueKey:
    """``json_value``'s input: TEXT as is, a native JSON field read as its own type (``CAST(col AS JSONB)``)."""
    field_type = scope_field_type(scope=scope, bundle=bundle)
    db_type = _json_db_type(arg, scope=scope, bundle=bundle)
    if db_type is not None:
        return CastKey(source=arg, type_text=db_type, data_type=DataType.OPAQUE)
    declared = field_type(arg) if isinstance(arg, (FieldKey, FieldSqlKey)) else None
    if declared in (None, DataType.STRING) and not (
        numeric_valued(arg, field_type=field_type) or boolean_valued(arg, field_type=field_type)
        or temporal_type(arg, field_type=field_type) is not None
    ):
        return arg
    raise ScalarOperandTypeError(
        summary=f"json_value() reads JSON text or a JSON field; `{key_display(arg)}` is "
        f"{(declared or DataType.OPAQUE).value if declared not in (None, DataType.STRING) else 'not text'}.",
        suggestion="Pass a TEXT field holding JSON, or a field of the database's JSON type.",
    )


def _json_db_type(arg: ValueKey, *, scope: DatasetScope, bundle: ResolvedSourceBundle) -> Optional[str]:
    """The database JSON type of a field ``arg`` (``JSONB``, ``VARIANT``, …), when it has one."""
    if not isinstance(arg, FieldKey) or scope.population is None:
        return None
    owner = resolved_terminal(root=scope.population, path=arg.path, datasets_by_name=bundle.datasets_by_name)
    col = owner.get_field(arg.leaf) if owner is not None else None
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


def _reject_windowed_field_sql(
    refs: Tuple[ValueKey, ...],
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    parsed: ParsedExpr,
) -> None:
    """Raise ``IllegalWindowInFilterError`` if any referenced ``FieldSqlKey``
    has a windowed ``DatasetField.sql`` body (no predicate-promotion)."""
    for k in refs:
        if not isinstance(k, FieldSqlKey):
            continue
        dataset = _lookup_dataset(name=k.dataset, scope=scope, bundle=bundle)
        if dataset is None:
            continue
        col = next((c for c in dataset.fields if c.name == k.field_name), None)
        if col is None or col.definition_sql is None:
            continue
        if has_window_function(col.definition_sql):
            raise IllegalWindowInFilterError(
                filter_expr=str(parsed),
                source=(
                    f"filter references field {k.field_name!r} on dataset "
                    f"{k.dataset!r} whose DatasetField.sql contains a window "
                    f"function"
                ),
                suggestion=(
                    "use a rank-family transform (rank, percent_rank, "
                    "dense_rank, ntile) in the expression instead, or "
                    "compute the windowed value in an earlier stage."
                ),
            )


def _lookup_dataset(
    *,
    name: str,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
) -> Optional[SlayerDataset]:
    if scope.population is not None and scope.population.name == name:
        return scope.population
    return bundle.get_referenced_dataset(name)
