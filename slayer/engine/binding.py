"""ExpressionBinder + FilterBinder: bind a ``ParsedExpr`` against a scope
(``ModelScope`` / ``StageSchema``) into a typed ``BoundExpr`` / ``BoundFilter``."""

from __future__ import annotations

import difflib
import os
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import (
    AggregationArgumentError,
    AggregationNotAllowedError,
    CircularJoinPathError,
    IllegalScopeReferenceError,
    IllegalWindowInFilterError,
    MeasureCycleError,
    MeasureRecursionLimitError,
    UnknownFunctionError,
    UnknownReferenceError,
    UnresolvableDimensionJoinError,
)
from slayer.core.enums import (
    BUILTIN_AGGREGATION_PARAM_ORDER,
    BUILTIN_AGGREGATIONS,
    DEFAULT_AGGREGATIONS_BY_TYPE,
    NUMERIC_ONLY_AGGREGATIONS,
    PRIMARY_KEY_AGGREGATIONS,
    RANKED_AGGREGATIONS,
    DataType,
    TimeGranularity,
    format_unknown_aggregation,
    normalize_aggregation_name,
)
from slayer.core.enums import RANK_FAMILY_TRANSFORMS
from slayer.core.refs import EXPRESSION_SOURCE_KINDS
from slayer.core.keys import SCALAR_FUNCTIONS, check_scalar_arity, AggregateKey, ArithmeticKey, ColumnKey, ColumnSqlKey, Grain, InKey, LiteralKey, ScalarCallKey, StarKey, TimeTruncKey, TransformKey, ValueKey, column_leaf, column_path, is_attached_source, normalize_scalar, prepend_value_key, source_anchor_path, walk_value_keys
from slayer.core.join_walker import (
    OrientedJoin,
    canonical_path,
    resolve_hop,
    terminal_model,
    walk,
)
from slayer.core.models import VALUE_PLACEHOLDER, SlayerModel, reserved_value_param_message
from slayer.engine import dimension_routing
from slayer.core.query import TimeDimension
from slayer.core.scope import ModelScope, StageSchema, resolve_generated_column
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.syntax import (
    AggCall,
    Arith,
    BoolOp,
    Cmp,
    DottedRef,
    Literal,
    ParsedExpr,
    Ref,
    ScalarCall,
    StarSource,
    TransformCall,
    TupleLit,
    UnaryOp,
    parse_expr,
)
from slayer.sql.sql_expr import has_window_function
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
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    allow_measures: bool = False,
    dimension_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> BoundExpr:
    """Bind a parsed expression against a scope into a ``BoundExpr``.

    ``allow_measures`` enables saved-measure resolution (bare and dotted) in
    the eligible positions — measure formulas and computed-dimension
    expressions; off everywhere else, so a saved-measure name there errors.
    ``dimension_alias_map`` resolves ``partition_by=<computed dim name>`` to
    the dimension's bound key; it applies ONLY there."""
    measure_ctx = (
        MeasureResolutionCtx(depth_limit=_measure_depth_limit())
        if allow_measures else None
    )
    value_key = _bind(
        parsed, scope=scope, bundle=bundle, in_filter=False, measure_ctx=measure_ctx,
        dim_alias_map=dimension_alias_map,
    )
    return BoundExpr(
        value_key=value_key,
        routed_dotted=_canonical_if_routed(
            parsed=parsed, value_key=value_key, scope=scope,
        ),
    )


def bind_time_dimension(
    td: TimeDimension,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> BoundTimeDimension:
    """Bind a ``TimeDimension`` into a ``BoundTimeDimension``: a ``BoundExpr`` carrying a ``TimeTruncKey`` plus the column facts the checker judges (its type, its recorded bucket granularity — from a ``StageColumn`` or a model ``Column``). The column resolves like a Mode-B identifier ref against a ``ModelScope`` (joins) or a flat ``StageSchema``; the temporal / re-bucketing rules are the checker's (P9)."""
    full = td.dimension.full_name
    bound_col, column_type, upstream_granularity = _time_dimension_column_facts(
        full, scope=scope, bundle=bundle,
    )
    time_key = TimeTruncKey(
        column=bound_col, granularity=str(td.granularity.value),
    )
    routed = _canonical_if_routed(
        parsed=DottedRef(parts=tuple(full.split("."))) if "." in full else Ref(name=full),
        value_key=bound_col, scope=scope,
    )
    return BoundTimeDimension(
        bound=BoundExpr(value_key=time_key, routed_dotted=routed),
        column_type=column_type,
        upstream_granularity=upstream_granularity,
    )


def _time_dimension_column_facts(
    full: str,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> Tuple[Union[ColumnKey, ColumnSqlKey], Optional[DataType], Optional[TimeGranularity]]:
    """Resolve a time dimension's column against ``scope`` and read its facts — (bound column key, column type, recorded bucket granularity). Stage arm reads the flat ``StageColumn`` (dotted → illegal-scope, unknown → unknown-reference); model arm walks joins to the terminal ``Column`` and returns its ``granularity``, so a bucketed model column re-buckets under the same rule as a stage column."""
    if isinstance(scope, StageSchema):
        if "." in full:
            bound_col = _resolve_dotted(tuple(full.split(".")), scope=scope, bundle=bundle)
        else:
            bound_col = _resolve_ref(full, scope=scope, bundle=bundle)
        assert isinstance(bound_col, ColumnKey)  # a stage ref is always a flat ColumnKey
        stage_col = scope.get(bound_col.leaf)
        assert stage_col is not None  # _resolve_ref already validated existence
        return bound_col, stage_col.type, stage_col.granularity

    assert isinstance(scope, ModelScope)
    if scope.source_model is None:
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
    terminal = _terminal_model_for_path(
        path=column_path(bound_col), scope=scope, bundle=bundle,
    )
    if terminal is None:
        # Defensive: _resolve_ref / _resolve_dotted would already have raised.
        raise UnknownReferenceError(
            name=full,
            scope_kind="ModelScope",
            scope_summary=f"could not resolve terminal model for {full!r}",
            suggestion=None,
        )
    col = next(
        (c for c in terminal.columns if c.name == column_leaf(bound_col)), None,
    )
    return (
        bound_col,
        (col.type if col is not None else None),
        (col.granularity if col is not None else None),
    )


def _canonical_if_routed(
    *,
    parsed: ParsedExpr,
    value_key: ValueKey,
    scope: Union[ModelScope, StageSchema],
) -> Optional[str]:
    """Full canonical dotted path when a ``DottedRef``'s bound path differs from
    its typed hop path (auto-routed or respelled) — or the canonical name of a
    stale flat ``Ref`` — else ``None``, so every already-canonical ref keeps a
    byte-identical result key."""
    key = value_key.column if isinstance(value_key, TimeTruncKey) else value_key
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
    """Walk ``path`` from ``scope.source_model`` to the terminal model (host if
    empty) through the shared bidirectional walker — reverse hops and edge-name
    tokens resolve, so the terminal model comes from the resolved edge, never
    from reading the token as a model name."""
    current = scope.source_model
    if current is None:
        return None
    models_by_name = bundle.models_by_name
    models_by_name.setdefault(current.name, current)
    for hop in path:
        edge = resolve_hop(current=current, token=hop, models_by_name=models_by_name)
        if edge is None:
            return None
        current = models_by_name.get(edge.target_model)
        if current is None:
            return None
    return current


def bind_filter(
    parsed: ParsedExpr,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    dimension_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> BoundFilter:
    """Bind a parsed filter predicate + classify its phase.

    Walks the bound tree to gather every referenced ``ValueKey``; raises
    ``IllegalWindowInFilterError`` if a referenced ``Column.sql`` is windowed.
    ``alias_map`` maps a stage's declared-measure names to their bound
    ``ValueKey`` so a bare ref matching an alias interns onto that slot rather
    than resolving against model columns (colon form and alias form share one slot).
    ``dimension_alias_map`` resolves ``partition_by=<computed dim name>`` only."""
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
    scope: Union[ModelScope, StageSchema],
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
    if isinstance(parsed, Literal):
        return LiteralKey(value=normalize_scalar(parsed.value))

    if isinstance(parsed, Ref):
        return _resolve_ref(
            parsed.name, scope=scope, bundle=bundle, alias_map=alias_map,
            measure_ctx=measure_ctx,
        )

    if isinstance(parsed, DottedRef):
        return _resolve_dotted(
            parsed.parts, scope=scope, bundle=bundle, alias_map=alias_map,
            measure_ctx=measure_ctx,
        )

    if isinstance(parsed, StarSource):
        return StarKey()

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

    if isinstance(parsed, Arith):
        return ArithmeticKey(
            op=parsed.op,
            operands=(
                _bind(parsed.left, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx, dim_alias_map=dim_alias_map),
                _bind(parsed.right, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx, dim_alias_map=dim_alias_map),
            ),
        )

    if isinstance(parsed, UnaryOp):
        return ArithmeticKey(
            op=parsed.op,
            operands=(_bind(parsed.operand, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx, dim_alias_map=dim_alias_map),),
        )

    if isinstance(parsed, Cmp):
        # ``IN`` / ``NOT IN`` fold into a single ``InKey`` (structured
        # column + literal-tuple handle for the generator).
        if parsed.op in ("in", "not in"):
            return _bind_in(
                parsed,
                scope=scope, bundle=bundle, in_filter=in_filter,
                alias_map=alias_map, measure_ctx=measure_ctx,
                dim_alias_map=dim_alias_map,
            )
        return ArithmeticKey(
            op=parsed.op,
            operands=(
                _bind(parsed.left, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx, dim_alias_map=dim_alias_map),
                _bind(parsed.right, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx, dim_alias_map=dim_alias_map),
            ),
        )

    if isinstance(parsed, BoolOp):
        operands = tuple(
            _bind(v, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx, dim_alias_map=dim_alias_map)
            for v in parsed.operands
        )
        return ArithmeticKey(op=parsed.op, operands=operands)

    raise ValueError(
        f"Unsupported ParsedExpr node: {type(parsed).__name__}"
    )


def _bind_in(
    parsed: Cmp,
    *,
    scope: Union[ModelScope, StageSchema],
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
        negated=(parsed.op == "not in"),
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
    scope: Union[ModelScope, StageSchema],
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
                    f"stage {scope.relation_name!r} columns: "
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
    current = host
    visited_models = {host.name}
    chain: List[OrientedJoin] = []
    for hop in hop_path:
        edge = resolve_hop(current=current, token=hop, models_by_name=models_by_name)
        if edge is None:
            return _route_edgeless_hop(
                host=host, hop=hop, hop_path=hop_path, leaf=leaf, parts=parts,
                models_by_name=models_by_name,
            )
        nxt = models_by_name.get(edge.target_model)
        if nxt is None:
            raise _target_not_in_bundle(parts=parts, target=edge.target_model)
        # Revisiting a model is a circular join (``a -> b -> a``): reject here
        # rather than fail confusingly on the leaf. Same class as the derived
        # save-time refusal; still a ValueError, wording preserved.
        if nxt.name in visited_models:
            raise CircularJoinPathError(
                reference=".".join(spelled), root_model=host.name,
                revisited=nxt.name, hop=hop, via=current.name,
            )
        visited_models.add(nxt.name)
        chain.append(edge)
        current = nxt
    return current, canonical_path(chain)


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
    scope: Union[ModelScope, StageSchema],
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
        raise IllegalScopeReferenceError(
            name=".".join(parts),
            scope_kind="StageSchema",
            reason=(
                "downstream stages see a flat schema — dotted refs are "
                "not legal. Use the flat column name."
            ),
        )

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


def _reject_round_trip(
    host_key: ValueKey,
    *,
    host: SlayerModel,
    bundle: ResolvedSourceBundle,
    ref_text: str,
    terminal_name: str,
) -> None:
    """Reject a re-anchored measure whose join path revisits a model on the
    host→target chain (round trip) — parity with the circular-join rejection."""
    models_by_name = bundle.models_by_name
    models_by_name.setdefault(host.name, host)
    for sub in walk_value_keys(host_key):
        path = getattr(sub, "path", None)
        if not path:
            continue
        visited = {host.name}
        current = host
        for hop in path:
            # Tokens may be edge names — resolve via the shared walker.
            edge = resolve_hop(
                current=current, token=hop, models_by_name=models_by_name)
            nxt = models_by_name.get(edge.target_model) if edge else None
            if nxt is None:
                break
            if nxt.name in visited:
                raise ValueError(
                    f"Round-trip reference: saved measure {ref_text!r} expands "
                    f"across a join back to {nxt.name!r}, a model already on the "
                    f"host->{terminal_name} chain, so it cannot be re-anchored "
                    f"(the identical hand-written path is rejected as circular)."
                )
            visited.add(nxt.name)
            current = nxt


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
    host_key = prepend_value_key(bound, host_path=host_path)
    _reject_round_trip(
        host_key, host=host_model, bundle=bundle,
        ref_text=ref_text, terminal_name=terminal_model.name,
    )
    return host_key


def _resolve_dotted_star(
    parts: Tuple[str, ...],
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> StarKey:
    """Resolve a dotted star (``customers.*``) to a ``StarKey`` whose ``path`` is
    the validated hop chain (empty path = local star)."""
    assert parts and parts[-1] == "*"
    if isinstance(scope, StageSchema):
        raise IllegalScopeReferenceError(
            name=".".join(parts),
            scope_kind="StageSchema",
            reason=(
                "downstream stages see a flat schema — dotted refs are "
                "not legal. Use the flat column name."
            ),
        )
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
    scope: Union[ModelScope, StageSchema],
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
        if not isinstance(bound, (ColumnKey, ColumnSqlKey)):
            raise ValueError(
                f"{label} partition_by must resolve to a column reference; "
                f"got {type(bound).__name__}."
            )
        pks.append(bound)
    return Grain.of(pks)


def _bind_expression_agg_source(
    parsed_source: ParsedExpr, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> ValueKey:
    """Bind a scalar-expression aggregate source.

    Dotted joined-model leaves and operands carrying ``Column.filter`` are both
    admitted: the home rule roots the aggregation and the filter
    desugars to ``CASE WHEN``. The source must still resolve to a row-level
    expression (a column, star, or arithmetic/scalar composite of them)."""
    bound = _bind(parsed_source, scope=scope, bundle=bundle, in_filter=False)
    if not isinstance(bound, EXPRESSION_SOURCE_KINDS):
        raise ValueError(
            f"Aggregation source must resolve to a column, star, or a "
            f"row-level expression; got {type(bound).__name__}."
        )
    return bound


# Scalar functions whose result is certainly text, for the best-effort
# expression type inference.
_TEXT_RESULT_SCALARS = frozenset({
    "lower", "upper", "trim", "ltrim", "rtrim", "replace", "substr",
    "substring", "concat",
})
# Null-handling / min-max scalars whose result class follows their arguments.
_ARG_CLASS_SCALARS = frozenset({
    "coalesce", "ifnull", "nullif", "greatest", "least",
})
# Comparison-family scalars whose result is certainly boolean.
_BOOL_RESULT_SCALARS = frozenset({"like"})


def _expression_is_confidently_text(key, *, model: Optional[SlayerModel]) -> bool:
    """Best-effort: True only when the expression's value is certainly text."""
    if isinstance(key, str):
        return True
    if isinstance(key, ScalarCallKey):
        if key.name in _TEXT_RESULT_SCALARS:
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


def _expression_is_confidently_boolean(key, *, model: Optional[SlayerModel]) -> bool:
    """Best-effort: True only when the expression's value is certainly boolean."""
    if isinstance(key, LiteralKey):
        return isinstance(key.value, bool)
    if isinstance(key, ScalarCallKey):
        if key.name in _BOOL_RESULT_SCALARS:
            return True
        if key.name == "iif":
            # iif's result follows its two branches, not the condition (arg 0).
            branches = key.args[1:3]
            return len(branches) == 2 and all(
                _expression_is_confidently_boolean(a, model=model)
                for a in branches
            )
        if key.name in _ARG_CLASS_SCALARS:
            return any(
                _expression_is_confidently_boolean(a, model=model)
                for a in key.args
            )
        return False
    if isinstance(key, ColumnKey) and not key.path and model is not None:
        col = model.get_column(key.leaf)
        return col is not None and col.type == DataType.BOOLEAN
    if isinstance(key, ColumnSqlKey) and not key.path and model is not None:
        col = model.get_column(key.column_name)
        return col is not None and col.type == DataType.BOOLEAN
    return False


def _reject_non_numeric_expression_agg(
    *, source: ValueKey, agg: str,
    scope: Union[ModelScope, StageSchema],
) -> None:
    if agg not in NUMERIC_ONLY_AGGREGATIONS:
        return
    model = (
        scope.source_model if isinstance(scope, ModelScope) else None
    )
    if _expression_is_confidently_text(source, model=model):
        raise ValueError(
            f"Aggregation {agg!r} requires a numeric value, but the "
            f"aggregated expression is non-numeric (text). Use a counting "
            f"or min/max aggregation, or make the expression numeric."
        )
    if _expression_is_confidently_boolean(source, model=model):
        raise ValueError(
            f"Aggregation {agg!r} requires a numeric value, but the "
            f"aggregated expression is boolean. Use a counting aggregation, "
            f"or cast the expression to a number."
        )


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
    if isinstance(node, UnaryOp):
        return _source_is_reaggregation(node.operand)
    if isinstance(node, BoolOp):
        return any(_source_is_reaggregation(o) for o in node.operands)
    return False


def _bind_agg_call(
    parsed: AggCall, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> Union[AggregateKey, TransformKey]:
    """Bind an ``AggCall``; ``first`` / ``last`` dispatch by the bound operand's
    type — attached → the series transform, row grain → the ranked aggregation."""
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


def _bind_agg(
    parsed: AggCall, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> AggregateKey:
    if _source_is_reaggregation(parsed.source):
        # Re-aggregation: bind the operand subtree — inner AggCalls
        # become AggregateKeys — so the outer key carries a nested-aggregate
        # source (axiom 6). Discovery/planning lift it to a producer-over-producer.
        source = _bind(
            parsed.source, scope=scope, bundle=bundle, in_filter=False,
            dim_alias_map=dim_alias_map,
        )
    elif isinstance(parsed.source, StarSource):
        source = StarKey()
    elif (
        isinstance(parsed.source, DottedRef)
        and parsed.source.parts
        and parsed.source.parts[-1] == "*"
    ):
        # Cross-model star: ``count(customers.*)`` → a StarKey carrying the join
        # path so the planner routes COUNT(*) through the join graph.
        source = _resolve_dotted_star(
            parsed.source.parts, scope=scope, bundle=bundle,
        )
    elif isinstance(parsed.source, (Ref, DottedRef)):
        bound_source = _bind(
            parsed.source, scope=scope, bundle=bundle, in_filter=False,
        )
        if not isinstance(bound_source, (ColumnKey, ColumnSqlKey, StarKey)):
            raise ValueError(
                f"Aggregation source must resolve to a column / star, "
                f"got {type(bound_source).__name__}."
            )
        source = bound_source
    else:
        # Same-model scalar EXPRESSION source (``sum(amount - cost)``).
        source = _bind_expression_agg_source(
            parsed.source, scope=scope, bundle=bundle,
        )

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
    # Expression sources: order-sensitive first/last need a plain
    # column (the ranked kernel can't rank an expression), and numeric-only
    # aggregations are rejected when the expression is confidently non-numeric
    # (per-column gates don't apply).
    if isinstance(source, EXPRESSION_SOURCE_KINDS):
        if effective_agg in ("first", "last"):
            raise ValueError(
                f"Aggregation {effective_agg!r} is not supported over an "
                f"expression; use a plain column."
            )
        _reject_non_numeric_expression_agg(
            source=source, agg=effective_agg, scope=scope,
        )
    return AggregateKey(
        source=source,
        agg=effective_agg,
        args=args,
        kwargs=kwargs,
        partition_keys=partition_keys,
    )


def _walk_tokens_best_effort(
    *, host: SlayerModel, path, bundle: ResolvedSourceBundle,
) -> Optional[SlayerModel]:
    """Terminal model of ``path`` from ``host`` via the shared walker (tokens
    may be edge names); ``None`` when a hop doesn't resolve — callers skip
    their validation best-effort."""
    return terminal_model(
        root=host, path=tuple(path),
        models_by_name=bundle.models_by_name,
    )




def _resolve_agg_owner(
    source, bundle: ResolvedSourceBundle,
) -> "tuple[Optional[SlayerModel], Optional[str]]":
    """``(owning_model, gate_leaf)`` for an aggregate source.

    Star and expression sources own no column (``leaf`` is ``None``) but still
    resolve an owning model for custom-aggregation names — the join-path
    terminal for a pathed star, the host otherwise. ``(None, None)`` when the
    model can't be confirmed (no host model, unresolved join hop): the caller
    best-effort skips validation there (the compile-time path validator
    catches truly broken refs).
    """
    host = bundle.source_model
    if host is None:
        return None, None
    leaf = getattr(source, "leaf", None) or getattr(source, "column_name", None)
    current = _walk_tokens_best_effort(
        host=host, path=source_anchor_path(source), bundle=bundle)
    if current is None:
        return None, None
    return current, leaf


def _declared_agg_param_names(
    *, agg: str, source, bundle: ResolvedSourceBundle,
) -> List[str]:
    """Declared parameter order for ``agg`` — the owning model's custom
    definition wins over the built-in registry; ``[]`` when none declared."""
    owner, _leaf = _resolve_agg_owner(source, bundle)
    if owner is not None:
        custom = next(
            (a for a in (owner.aggregations or []) if a.name == agg), None,
        )
        if custom is not None:
            return [p.name for p in custom.params]
    return list(BUILTIN_AGGREGATION_PARAM_ORDER.get(agg, ()))


def _check_agg_kwarg_names(
    *, agg: str, source, bundle: ResolvedSourceBundle, names: List[str],
) -> None:
    """Reject an argument name the rendered ``agg`` never reads (only its placeholders or own parameters, plus ``window``)."""
    if not names:
        return
    owner, _leaf = _resolve_agg_owner(source, bundle)
    definition = next((a for a in (owner.aggregations or []) if a.name == agg), None) if owner else None
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
        if accepted == {"window"}:
            raise AggregationArgumentError(
                f"Aggregation '{agg}' takes no args or kwargs other than window; got '{name}'."
            )
        raise AggregationArgumentError(
            f"Aggregation '{agg}' does not accept argument '{name}'; "
            f"accepted: {', '.join(sorted(accepted))}."
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
                f"Aggregation {agg!r} ranks by at most one column; got "
                f"{len(args)} positional value(s)."
            )
        if not isinstance(args[0], (ColumnKey, ColumnSqlKey)):
            raise ValueError(
                f"Aggregation {agg!r} ranks by a column; got "
                f"{type(args[0]).__name__} as its ranking key."
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


def _unknown_aggregation_message(name: str, known) -> str:
    """The standard unknown-aggregation error, plus a scalar-allowlist hint
    when the name is a near-miss for a scalar function (typo UX)."""
    msg = format_unknown_aggregation(name, known)
    scalar_match = difflib.get_close_matches(
        word=name.lower(), possibilities=sorted(SCALAR_FUNCTIONS), n=1,
    )
    if scalar_match:
        msg += (
            f" Note: {scalar_match[0]!r} is a scalar function, not an "
            f"aggregation (scalar allowlist: {sorted(SCALAR_FUNCTIONS)})."
        )
    return msg


def _validate_agg_eligibility(
    *, source, agg: str, bundle: ResolvedSourceBundle,
) -> str:
    """Heal the aggregation name, validate it globally, and enforce per-column
    eligibility gates, returning the effective (alias-healed) name for
    ``AggregateKey.agg``.

    Healing is skipped when the raw token exactly matches a custom aggregation
    on the owning model (a custom ``countd`` wins over the alias). Gate order:
    0. unknown-name-first, for EVERY source shape (column, star, expression),
    so ``*:bogus`` / ``bogus(*)`` never escape to SQL generation;
    1. PK columns restricted to count / count_distinct; 2. explicit
    ``Column.allowed_aggregations`` whitelist; 3. else
    ``DEFAULT_AGGREGATIONS_BY_TYPE`` (custom aggregations exempt).

    Star and expression sources have no column, so the per-column gates (1-3)
    don't apply (gate 0 still validates the name); unresolvable targets pass
    through (best-effort).
    """
    owner_model, leaf = _resolve_agg_owner(source, bundle)
    if owner_model is None:
        return normalize_aggregation_name(agg)
    # Alias healing — custom aggregation named like an alias wins.
    custom_names = {a.name for a in (owner_model.aggregations or [])}
    effective = agg if agg in custom_names else normalize_aggregation_name(agg)
    # Gate 0: unknown-name-first (precedence over PK / whitelist / type).
    known = BUILTIN_AGGREGATIONS | custom_names
    if effective not in known:
        raise ValueError(_unknown_aggregation_message(effective, known))
    if leaf is None:
        return effective
    col = next((c for c in owner_model.columns if c.name == leaf), None)
    if col is None:
        return effective
    if col.primary_key:
        if effective not in PRIMARY_KEY_AGGREGATIONS:
            raise AggregationNotAllowedError(
                column=leaf,
                agg=effective,
                reason=(
                    f"primary-key column {leaf!r} restricted to "
                    f"{sorted(PRIMARY_KEY_AGGREGATIONS)}; got {effective!r}."
                ),
            )
        return effective
    if col.allowed_aggregations is not None:
        if effective not in col.allowed_aggregations:
            raise AggregationNotAllowedError(
                column=leaf,
                agg=effective,
                reason=(
                    f"column {leaf!r} restricts allowed_aggregations to "
                    f"{sorted(col.allowed_aggregations)}; got {effective!r}."
                ),
            )
        return effective
    # Model-custom aggregations are exempt from the type-default gate.
    if effective in custom_names:
        return effective
    allowed = DEFAULT_AGGREGATIONS_BY_TYPE.get(col.type, frozenset())
    if effective not in allowed:
        raise AggregationNotAllowedError(
            column=leaf,
            agg=effective,
            reason=(
                f"aggregation {effective!r} is not applicable to "
                f"{col.type} column {leaf!r}; default aggregations are "
                f"{sorted(allowed)}."
            ),
        )
    return effective


def _bind_agg_arg(
    parsed: ParsedExpr, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]] = None,
):
    """Bind one aggregation arg: identifiers → ``ColumnKey`` / ``ColumnSqlKey``,
    a nested aggregate → ``AggregateKey`` (aggregate-valued parameter), a grained
    transform → ``TransformKey`` at its result grain, literals → inline
    scalar via ``normalize_scalar`` (stored inline, not as LiteralKey).
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
    if isinstance(parsed, (Ref, DottedRef)):
        return _bind(parsed, scope=scope, bundle=bundle, in_filter=False)
    raise ValueError(
        f"Aggregation argument of kind {type(parsed).__name__} is not "
        f"supported. Pass a column reference, a scalar, a partitioned aggregate, "
        f"or a grained transform."
    )


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
    "consecutive_periods": frozenset({"period"}),
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
    scope: Union[ModelScope, StageSchema],
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
    scope: Union[ModelScope, StageSchema],
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
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, "ValueKey"]],
) -> TransformKey:
    positional_pairs = _transform_positional_pairs(op=op, args=args, kwargs=kwargs)
    bound_kwargs: List = []
    partition_keys: Grain = Grain.EMPTY
    allowed_kwargs = _TRANSFORM_KWARG_RULES.get(op, frozenset())
    seen_kwargs: set = set()
    rank_partition_ok = op in RANK_FAMILY_TRANSFORMS
    for k, v in [*positional_pairs, *kwargs]:
        if k == "partition_by" and rank_partition_ok:
            partition_keys = _bind_partition_keys(
                value=v, scope=scope, bundle=bundle, dim_alias_map=dim_alias_map,
                label=f"transform {op!r}",
            )
            continue
        if k not in allowed_kwargs:
            advertised = allowed_kwargs | ({"partition_by"} if rank_partition_ok else set())
            raise ValueError(
                f"Transform {op!r} does not accept keyword "
                f"argument {k!r}. Accepted: {sorted(advertised)}."
            )
        seen_kwargs.add(k)
        scalar = _fold_to_scalar(v)
        if scalar is _NOT_SCALAR:
            raise ValueError(
                f"Transform {op!r} keyword {k!r} must be a "
                f"scalar literal; got expression of kind "
                f"{type(v).__name__}."
            )
        bound_kwargs.append((k, scalar))
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
            raise ValueError(
                "Transform 'ntile' requires keyword argument n (the "
                "number of buckets, a positive integer)."
            )
        n_value = next(v for k, v in kwargs if k == "n")
        if not _is_positive_integer(n_value):
            raise ValueError(
                f"Transform {op!r} keyword n must be a positive "
                f"integer; got {n_value!r}."
            )
    if op == "time_shift" and "periods" not in seen:
        raise ValueError(
            "Transform 'time_shift' requires keyword argument periods "
            "(the integer offset, negative for a backward shift)."
        )
    if op in ("lag", "lead") and "periods" not in seen:
        kwargs.append(("periods", normalize_scalar(1)))
    return kwargs


def _bind_scalar(
    parsed: ScalarCall, *,
    scope: Union[ModelScope, StageSchema],
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
    arity_error = check_scalar_arity(
        name=parsed.name, argc=len(parsed.args),
    )
    if arity_error is not None:
        if parsed.name == "like":
            raise ValueError(
                f"Scalar function 'like' takes exactly 2 arguments "
                f"(value, pattern); got {len(parsed.args)}."
            )
        raise ValueError(arity_error)
    args = tuple(
        _bind(a, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx, dim_alias_map=dim_alias_map)
        for a in parsed.args
    )
    return ScalarCallKey(name=parsed.name, args=args)


def _reject_windowed_column_sql(
    refs: Tuple[ValueKey, ...],
    *,
    scope: Union[ModelScope, StageSchema],
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
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> Optional[SlayerModel]:
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        if scope.source_model.name == name:
            return scope.source_model
    return bundle.get_referenced_model(name)
