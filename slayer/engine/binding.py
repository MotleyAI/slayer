"""ExpressionBinder + FilterBinder: bind a ``ParsedExpr`` against a scope
(``ModelScope`` / ``StageSchema``) into a typed ``BoundExpr`` / ``BoundFilter``."""

from __future__ import annotations

import difflib
import os
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.errors import (
    AggregationNotAllowedError,
    IllegalScopeReferenceError,
    IllegalWindowInFilterError,
    MeasureCycleError,
    MeasureRecursionLimitError,
    UnknownFunctionError,
    UnknownReferenceError,
)
from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    DEFAULT_AGGREGATIONS_BY_TYPE,
    NUMERIC_ONLY_AGGREGATIONS,
    PRIMARY_KEY_AGGREGATIONS,
    DataType,
    format_unknown_aggregation,
    normalize_aggregation_name,
)
from slayer.core.formula import RANK_FAMILY_TRANSFORMS
from slayer.core.refs import EXPRESSION_SOURCE_KINDS
from slayer.core.keys import (
    SCALAR_FUNCTIONS,
    check_scalar_arity,
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    SqlExprKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    column_leaf,
    column_path,
    normalize_scalar,
    prepend_value_key,
)
from slayer.core.models import SlayerModel
from slayer.core.query import TimeDimension
from slayer.core.scope import ModelScope, StageSchema
from slayer.engine.column_filter_paths import compute_column_filter_join_paths
from slayer.engine.source_bundle import ResolvedSourceBundle
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
    walk_parsed_refs,
)
from slayer.sql.sql_expr import has_window_function

__all__ = [
    "BoundExpr",
    "BoundFilter",
    "bind_expr",
    "bind_filter",
    "bind_time_dimension",
    "walk_value_keys",
]


_TEMPORAL_TYPES = frozenset({DataType.DATE, DataType.TIMESTAMP})

_DEFAULT_MEASURE_DEPTH = 32
_MEASURE_DEPTH_ENV_VAR = "SLAYER_MEASURE_EXPANSION_DEPTH"


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


class BoundExpr(BaseModel):
    """A bound expression — its leaves are resolved ``ValueKey``s."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    value_key: ValueKey

    @property
    def phase(self) -> Phase:
        return self.value_key.phase


class BoundFilter(BaseModel):
    """A bound filter predicate: ``value_key`` (like ``BoundExpr``), ``phase``
    (max phase any referenced slot reaches), and ``referenced_keys`` (every
    ``ValueKey`` in the tree, for the cross-model planner's filter routing)."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    value_key: ValueKey
    phase: Phase
    referenced_keys: Tuple[ValueKey, ...] = Field(default_factory=tuple)


def bind_expr(
    parsed: ParsedExpr,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    allow_measures: bool = False,
) -> BoundExpr:
    """Bind a parsed expression against a scope into a ``BoundExpr``.

    ``allow_measures`` enables saved-measure resolution (bare and dotted) in
    the eligible positions — measure formulas and computed-dimension
    expressions; off everywhere else, so a saved-measure name there errors."""
    measure_ctx = (
        MeasureResolutionCtx(depth_limit=_measure_depth_limit())
        if allow_measures else None
    )
    value_key = _bind(
        parsed, scope=scope, bundle=bundle, in_filter=False, measure_ctx=measure_ctx,
    )
    return BoundExpr(value_key=value_key)


def bind_time_dimension(
    td: TimeDimension,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> BoundExpr:
    """Bind a ``TimeDimension`` into a ``BoundExpr`` carrying a ``TimeTruncKey``.

    The column resolves against ``scope`` like a Mode-B identifier ref and must
    be temporal (``DATE`` / ``TIMESTAMP``). Only ``ModelScope`` with a non-None
    ``source_model`` is accepted; a ``StageSchema`` raises."""
    if isinstance(scope, StageSchema):
        raise IllegalScopeReferenceError(
            name=td.dimension.full_name,
            scope_kind="StageSchema",
            reason=(
                "time dimensions only bind against a ModelScope; downstream "
                "stages already see the truncated column as a flat name "
                "from the upstream stage's schema."
            ),
        )

    assert isinstance(scope, ModelScope)
    if scope.source_model is None:
        raise UnknownReferenceError(
            name=td.dimension.full_name,
            scope_kind="ModelScope",
            scope_summary="(no source_model anchor; anchor-less mode not implemented)",
            suggestion=None,
        )

    full = td.dimension.full_name
    if "." in full:
        parts = tuple(full.split("."))
        bound_col = _resolve_dotted(parts, scope=scope, bundle=bundle)
    else:
        bound_col = _resolve_ref(full, scope=scope, bundle=bundle)

    if not isinstance(bound_col, (ColumnKey, ColumnSqlKey)):
        # Defensive: an identifier ref against a ModelScope is always a column.
        raise ValueError(
            f"TimeDimension {full!r} did not resolve to a column "
            f"reference (got {type(bound_col).__name__})."
        )

    # Leaf / path read via kind-agnostic helpers (ColumnKey or ColumnSqlKey).
    terminal_model = _terminal_model_for_path(
        path=column_path(bound_col),
        scope=scope,
        bundle=bundle,
    )
    if terminal_model is None:
        # Defensive: _resolve_ref / _resolve_dotted would already have raised.
        raise UnknownReferenceError(
            name=full,
            scope_kind="ModelScope",
            scope_summary=f"could not resolve terminal model for {full!r}",
            suggestion=None,
        )
    col = next(
        (c for c in terminal_model.columns if c.name == column_leaf(bound_col)),
        None,
    )
    if col is None or col.type not in _TEMPORAL_TYPES:
        observed = col.type if col is not None else "<missing>"
        raise ValueError(
            f"TimeDimension {full!r} must reference a temporal column "
            f"(DATE / TIMESTAMP); got column type {observed!r}."
        )

    return BoundExpr(
        value_key=TimeTruncKey(
            column=bound_col, granularity=str(td.granularity.value),
        ),
    )


def _terminal_model_for_path(
    *,
    path: Tuple[str, ...],
    scope: ModelScope,
    bundle: ResolvedSourceBundle,
) -> Optional[SlayerModel]:
    """Walk ``path`` from ``scope.source_model`` to the terminal model (host if empty)."""
    current = scope.source_model
    if current is None:
        return None
    for hop in path:
        nxt = bundle.get_referenced_model(hop)
        if nxt is None:
            return None
        current = nxt
    return current


def bind_filter(
    parsed: ParsedExpr,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> BoundFilter:
    """Bind a parsed filter predicate + classify its phase.

    Walks the bound tree to gather every referenced ``ValueKey``; raises
    ``IllegalWindowInFilterError`` if a referenced ``Column.sql`` is windowed.
    ``alias_map`` maps a stage's declared-measure names to their bound
    ``ValueKey`` so a bare ref matching an alias interns onto that slot rather
    than resolving against model columns (colon form and alias form share one slot)."""
    value_key = _bind(
        parsed, scope=scope, bundle=bundle, in_filter=True, alias_map=alias_map,
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


_VALUE_KEY_TYPES = (
    ColumnKey, ColumnSqlKey, StarKey, LiteralKey,
    AggregateKey, TransformKey, ArithmeticKey, ScalarCallKey,
    BetweenKey, InKey, TimeTruncKey,
)


def walk_value_keys(key: ValueKey):
    """Yield every ``ValueKey`` reachable from ``key``, including ``key``."""
    yield key
    if isinstance(key, AggregateKey):
        if isinstance(key.source, _VALUE_KEY_TYPES):
            yield from walk_value_keys(key.source)
        for a in key.args:
            if isinstance(a, _VALUE_KEY_TYPES):
                yield from walk_value_keys(a)
        for _, v in key.kwargs:
            if isinstance(v, _VALUE_KEY_TYPES):
                yield from walk_value_keys(v)
        for pk in key.partition_keys or ():
            yield from walk_value_keys(pk)
    elif isinstance(key, TransformKey):
        if isinstance(key.input, _VALUE_KEY_TYPES):
            yield from walk_value_keys(key.input)
        for a in key.args:
            if isinstance(a, _VALUE_KEY_TYPES):
                yield from walk_value_keys(a)
        for _, v in key.kwargs:
            if isinstance(v, _VALUE_KEY_TYPES):
                yield from walk_value_keys(v)
        for pk in key.partition_keys:
            yield from walk_value_keys(pk)
        if key.time_key is not None:
            yield from walk_value_keys(key.time_key)
    elif isinstance(key, ArithmeticKey):
        for op in key.operands:
            yield from walk_value_keys(op)
    elif isinstance(key, ScalarCallKey):
        for arg in key.args:
            if isinstance(arg, _VALUE_KEY_TYPES):
                yield from walk_value_keys(arg)
    elif isinstance(key, BetweenKey):
        yield from walk_value_keys(key.column)
        yield from walk_value_keys(key.low)
        yield from walk_value_keys(key.high)
    elif isinstance(key, InKey):
        # Walk column LHS + every literal RHS, like BetweenKey.
        yield from walk_value_keys(key.column)
        for v in key.values:
            yield from walk_value_keys(v)


def _bind(
    parsed: ParsedExpr,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
) -> ValueKey:
    # ``measure_ctx`` rides eligible operand edges, dropped at the aggregation
    # boundary — a measure is legal at value level but not inside an aggregation.
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
        return _bind_agg(parsed, scope=scope, bundle=bundle)

    if isinstance(parsed, TransformCall):
        return _bind_transform(
            parsed, scope=scope, bundle=bundle, alias_map=alias_map,
            measure_ctx=measure_ctx,
        )

    if isinstance(parsed, ScalarCall):
        return _bind_scalar(
            parsed, scope=scope, bundle=bundle, in_filter=in_filter,
            alias_map=alias_map, measure_ctx=measure_ctx,
        )

    if isinstance(parsed, Arith):
        return ArithmeticKey(
            op=parsed.op,
            operands=(
                _bind(parsed.left, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx),
                _bind(parsed.right, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx),
            ),
        )

    if isinstance(parsed, UnaryOp):
        return ArithmeticKey(
            op=parsed.op,
            operands=(_bind(parsed.operand, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx),),
        )

    if isinstance(parsed, Cmp):
        # ``IN`` / ``NOT IN`` fold into a single ``InKey`` (structured
        # column + literal-tuple handle for the generator).
        if parsed.op in ("in", "not in"):
            return _bind_in(
                parsed,
                scope=scope, bundle=bundle, in_filter=in_filter,
                alias_map=alias_map, measure_ctx=measure_ctx,
            )
        return ArithmeticKey(
            op=parsed.op,
            operands=(
                _bind(parsed.left, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx),
                _bind(parsed.right, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx),
            ),
        )

    if isinstance(parsed, BoolOp):
        operands = tuple(
            _bind(v, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx)
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
        measure_ctx=measure_ctx,
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
        col = scope.get(name)
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
        return ColumnKey(path=(), leaf=name)

    assert isinstance(scope, ModelScope)
    if scope.source_model is None:
        raise UnknownReferenceError(
            name=name,
            scope_kind="ModelScope",
            scope_summary="(no source_model anchor; anchor-less mode not implemented)",
            suggestion=None,
        )
    model = scope.source_model

    # A ``__``-bearing name is not special: it resolves by ordinary exact-match.
    col = next((c for c in model.columns if c.name == name), None)
    if col is not None:
        if col.sql is not None and col.sql.strip() != name:
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
):
    """Walk ``hop_path`` join hops from ``host``, validating each and rejecting a
    hop that revisits a model (circular join). Returns the terminal model;
    ``parts`` is the full dotted ref, for error messages only."""
    current = host
    visited_models = {host.name}
    for hop in hop_path:
        join = next(
            (j for j in current.joins if j.target_model == hop), None,
        )
        if join is None:
            raise UnknownReferenceError(
                name=".".join(parts),
                scope_kind="ModelScope",
                scope_summary=(
                    f"model {current.name!r} joins: "
                    f"{[j.target_model for j in current.joins]}"
                ),
                suggestion=f"model {current.name!r} has no join to {hop!r}.",
            )
        nxt = bundle.get_referenced_model(hop)
        if nxt is None:
            raise UnknownReferenceError(
                name=".".join(parts),
                scope_kind="ModelScope",
                scope_summary=f"target {hop!r} not in source bundle",
                suggestion=None,
            )
        # Revisiting a model is a circular join (``a -> b -> a``): reject here
        # rather than fail confusingly on the leaf.
        if nxt.name in visited_models:
            raise ValueError(
                f"Circular join detected resolving {'.'.join(parts)!r}: "
                f"revisits model {nxt.name!r}."
            )
        visited_models.add(nxt.name)
        current = nxt
    return current


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
            scope_summary="(no source_model anchor; anchor-less mode not implemented)",
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
    current = _walk_join_chain(
        hop_path=hop_path, host=host, bundle=bundle, parts=parts,
    )

    return _resolve_terminal_leaf(
        current=current, leaf=leaf, hop_path=hop_path,
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
    col = next((c for c in current.columns if c.name == leaf), None)
    if col is not None:
        if col.sql is not None and col.sql.strip() != leaf:
            # Derived column on a joined model — path is part of the key so the
            # cross-model planner can route via the join graph.
            return ColumnSqlKey(
                path=tuple(hop_path), model=current.name, column_name=leaf,
            )
        return ColumnKey(path=tuple(hop_path), leaf=leaf)
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
    for sub in walk_value_keys(host_key):
        path = getattr(sub, "path", None)
        if not path:
            continue
        visited = {host.name}
        for hop in path:
            nxt = bundle.get_referenced_model(hop)
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
            scope_summary="(no source_model anchor; anchor-less mode not implemented)",
            suggestion=None,
        )
    hop_path = parts[:-1]
    # Strip same-model self-prefix (``orders.*`` on ``orders``).
    if hop_path and hop_path[0] == host.name:
        hop_path = hop_path[1:]
    # Validate the hop chain (raises on missing / circular join); leaf ``*``
    # needs only the validated path, not the terminal model.
    _walk_join_chain(hop_path=hop_path, host=host, bundle=bundle, parts=parts)
    return StarKey(path=tuple(hop_path))


def _bind_agg_partition_keys(
    value, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> frozenset:
    """Bind an aggregation ``partition_by`` value to a frozenset of column keys."""
    elements = value if isinstance(value, tuple) else (value,)
    pks: List = []
    for elem in elements:
        bound = _bind(parsed=elem, scope=scope, bundle=bundle, in_filter=False)
        if not isinstance(bound, (ColumnKey, ColumnSqlKey)):
            raise ValueError(
                f"aggregation partition_by must resolve to a column reference; "
                f"got {type(bound).__name__}."
            )
        pks.append(bound)
    return frozenset(pks)


def _bind_expression_agg_source(
    parsed_source: ParsedExpr, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> ValueKey:
    """Bind a same-model scalar-expression aggregate source (DEV-1826).

    Boundaries with clear errors (cross-model semantics: DEV-1832): dotted
    paths inside the expression are cross-model; operands carrying
    ``Column.filter`` are rejected; nested aggregations / transforms were
    already rejected at parse time.
    """
    for node in walk_parsed_refs(parsed_source):
        if isinstance(node, DottedRef):
            raise ValueError(
                f"Cross-model expression aggregation is not supported: the "
                f"aggregated expression references the dotted path "
                f"{'.'.join(node.parts)!r}. Only bare same-model columns may "
                f"appear inside an aggregated expression (DEV-1832)."
            )
    bound = _bind(parsed_source, scope=scope, bundle=bundle, in_filter=False)
    if not isinstance(bound, EXPRESSION_SOURCE_KINDS):
        raise ValueError(
            f"Aggregation source must resolve to a column, star, or a "
            f"row-level expression; got {type(bound).__name__}."
        )
    _reject_filtered_expression_operands(bound, scope=scope)
    return bound


def _reject_filtered_expression_operands(
    bound: ValueKey, *, scope: Union[ModelScope, StageSchema],
) -> None:
    """A ``Column.filter`` applies at aggregation time over ONE column; inside
    a multi-operand expression its semantics are undefined until DEV-1832."""
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return  # StageSchema outputs carry no Column.filter
    model = scope.source_model
    for k in walk_value_keys(bound):
        if isinstance(k, ColumnKey) and not k.path:
            leaf = k.leaf
        elif isinstance(k, ColumnSqlKey) and not k.path:
            leaf = k.column_name
        else:
            continue
        col = next((c for c in model.columns if c.name == leaf), None)
        if col is not None and col.filter:
            raise ValueError(
                f"Column {leaf!r} carries a column-level filter "
                f"({col.filter!r}) and cannot be used inside an aggregated "
                f"expression. Define a derived model column for the "
                f"expression and aggregate it with the colon form instead "
                f"(DEV-1832)."
            )


# Scalar functions whose result is certainly text, for the best-effort
# expression type inference (DEV-1826).
_TEXT_RESULT_SCALARS = frozenset({
    "lower", "upper", "trim", "ltrim", "rtrim", "replace", "substr",
    "substring", "concat",
})
# Null-handling / min-max scalars whose result class follows their arguments.
_ARG_CLASS_SCALARS = frozenset({
    "coalesce", "ifnull", "nullif", "greatest", "least",
})
# Comparison-family scalars whose result is certainly boolean (DEV-1826).
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


def _bind_agg(
    parsed: AggCall, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> AggregateKey:
    if isinstance(parsed.source, StarSource):
        source = StarKey()
    elif (
        isinstance(parsed.source, DottedRef)
        and parsed.source.parts
        and parsed.source.parts[-1] == "*"
    ):
        # Cross-model star: ``customers.*:count`` → a StarKey carrying the join
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
        # DEV-1826: same-model scalar EXPRESSION source (``sum(amount - cost)``).
        source = _bind_expression_agg_source(
            parsed.source, scope=scope, bundle=bundle,
        )

    # ``partition_by`` is lifted out of kwargs onto ``partition_keys``
    # (``None`` means no partition, ``[]`` means grand total).
    args = tuple(
        _bind_agg_arg(a, scope=scope, bundle=bundle) for a in parsed.args
    )
    partition_keys: Optional[frozenset] = None
    kwargs_list: List = []
    for k, v in parsed.kwargs:
        if k == "partition_by":
            partition_keys = _bind_agg_partition_keys(value=v, scope=scope, bundle=bundle)
            continue
        kwargs_list.append((k, _bind_agg_arg(v, scope=scope, bundle=bundle)))
    kwargs = tuple(kwargs_list)
    # Propagate ``Column.filter`` into the AggregateKey's identity: two
    # aggregates over the same column with different filters differ at the key
    # level (wrapped as ``SUM(CASE WHEN ... THEN col END)``); same-filter intern.
    column_filter_key = _resolve_column_filter_key(
        source=source, bundle=bundle,
    )
    # Gate per-column aggregation eligibility, then store the EFFECTIVE
    # (alias-healed) name so the generator resolves the canonical aggregation.
    effective_agg = _validate_agg_eligibility(
        source=source, agg=parsed.agg, bundle=bundle,
    )
    # DEV-1826 expression sources: order-sensitive first/last need a plain
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
        column_filter_key=column_filter_key,
        partition_keys=partition_keys,
    )


def _resolve_column_filter_key(
    *, source, bundle: ResolvedSourceBundle,
) -> Optional[SqlExprKey]:
    """Look up the resolved source's ``Column.filter`` and convert to a
    ``SqlExprKey``. ``None`` for ``StarKey``, unset filters, or an unresolvable
    target model (best-effort — the compile-time path validator catches those)."""
    if isinstance(source, StarKey):
        return None
    path = getattr(source, "path", ())
    leaf = getattr(source, "leaf", None) or getattr(source, "column_name", None)
    if leaf is None:
        return None
    host = bundle.source_model
    if host is None:
        return None
    current: SlayerModel = host
    for hop in path:
        nxt = bundle.get_referenced_model(hop)
        if nxt is None:
            return None
        current = nxt
    col = next((c for c in current.columns if c.name == leaf), None)
    if col is None or not col.filter:
        return None
    # Stamp typed non-anchor join paths on the SqlExprKey so the planner's
    # isolation trigger reads typed data, not parsed SQL. The anchor relation
    # is the ``__``-canonical path alias when the anchor is a joined model.
    anchor_relation = "__".join(path) if path else current.name
    paths = compute_column_filter_join_paths(
        canonical_sql=col.filter,
        anchor_model=current,
        anchor_relation=anchor_relation,
        bundle=bundle,
    )
    return SqlExprKey(canonical_sql=col.filter, referenced_join_paths=paths)


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
    current: SlayerModel = host
    for hop in tuple(getattr(source, "path", ())):
        nxt = bundle.get_referenced_model(hop)
        if nxt is None:
            return None, None
        current = nxt
    return current, leaf


def _unknown_aggregation_message(name: str, known) -> str:
    """The standard unknown-aggregation error, plus a scalar-allowlist hint
    when the name is a near-miss for a scalar function (typo UX, DEV-1826)."""
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
    0. unknown-name-first, for EVERY source shape (column, star, expression —
    DEV-1826), so ``*:bogus`` / ``bogus(*)`` never escape to SQL generation;
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
    # DEV-1576 alias healing — custom aggregation named like an alias wins.
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
):
    """Bind one aggregation arg: identifiers → ``ColumnKey`` / ``ColumnSqlKey``,
    literals → inline scalar via ``normalize_scalar`` (stored inline, not as LiteralKey)."""
    if isinstance(parsed, Literal):
        return normalize_scalar(parsed.value)
    if isinstance(parsed, (Ref, DottedRef)):
        return _bind(parsed, scope=scope, bundle=bundle, in_filter=False)
    raise ValueError(
        f"Aggregation argument of kind {type(parsed).__name__} is not "
        f"supported. Pass a column reference or a scalar."
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


def _bind_transform(
    parsed: TransformCall, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
    measure_ctx: Optional[MeasureResolutionCtx] = None,
) -> TransformKey:
    # ``measure_ctx`` rides the transform INPUT only — partition_by / scalar
    # kwargs drop it (and partition_by binds without ``alias_map``).
    inp = _bind(
        parsed.input, scope=scope, bundle=bundle, in_filter=False,
        alias_map=alias_map, measure_ctx=measure_ctx,
    )
    # A few transforms accept further positional params (mapped onto kwargs);
    # every other transform is keyword-only after the value.
    positional_pairs: List = []
    pos_names = _TRANSFORM_POSITIONAL_KWARGS.get(parsed.op)
    if parsed.args:
        if pos_names is None:
            raise ValueError(
                f"Transform {parsed.op!r} accepts exactly one positional "
                f"argument (the value to transform); pass any offset, "
                f"partition, or other settings as keyword arguments "
                f"(e.g. ``{parsed.op}(value, partition_by=...)``)."
            )
        if len(parsed.args) > len(pos_names):
            raise ValueError(
                f"Transform {parsed.op!r} accepts at most {len(pos_names)} "
                f"positional argument(s) after the value "
                f"({', '.join(pos_names)}); got {len(parsed.args)}."
            )
        positional_pairs = list(zip(pos_names, parsed.args))
    args: List = []
    kwargs: List = []
    partition_keys: List = []
    allowed_kwargs = _TRANSFORM_KWARG_RULES.get(parsed.op, frozenset())
    seen_kwargs: set = set()
    # A name supplied both positionally and as a kwarg is ambiguous → error.
    _explicit_kw_names = {k for k, _ in parsed.kwargs}
    for k, _ in positional_pairs:
        if k in _explicit_kw_names:
            raise ValueError(
                f"Transform {parsed.op!r} got {k!r} both positionally and "
                f"as a keyword argument."
            )
    rank_partition_ok = parsed.op in RANK_FAMILY_TRANSFORMS
    for k, v in [*positional_pairs, *parsed.kwargs]:
        if k == "partition_by" and rank_partition_ok:
            # A single ref, or a tuple/list (``rank(x, partition_by=[a, b])``).
            elements = v if isinstance(v, tuple) else (v,)
            for elem in elements:
                bound_elem = _bind(
                    elem, scope=scope, bundle=bundle, in_filter=False,
                )
                if isinstance(bound_elem, (ColumnKey, ColumnSqlKey)):
                    partition_keys.append(bound_elem)
                else:
                    raise ValueError(
                        f"transform {parsed.op!r} partition_by must resolve "
                        f"to a column reference; got "
                        f"{type(bound_elem).__name__}."
                    )
            continue
        if k not in allowed_kwargs:
            advertised = allowed_kwargs | ({"partition_by"} if rank_partition_ok else set())
            raise ValueError(
                f"Transform {parsed.op!r} does not accept keyword "
                f"argument {k!r}. Accepted: {sorted(advertised)}."
            )
        seen_kwargs.add(k)
        scalar = _fold_to_scalar(v)
        if scalar is _NOT_SCALAR:
            raise ValueError(
                f"Transform {parsed.op!r} keyword {k!r} must be a "
                f"scalar literal; got expression of kind "
                f"{type(v).__name__}."
            )
        kwargs.append((k, scalar))
    kwargs = _apply_transform_kwarg_defaults(
        op=parsed.op, kwargs=kwargs, seen=seen_kwargs,
    )
    return TransformKey(
        op=parsed.op,
        input=inp,
        args=tuple(args),
        kwargs=tuple(kwargs),
        partition_keys=frozenset(partition_keys),
    )


def _apply_transform_kwarg_defaults(
    *, op: str, kwargs: list, seen: set,
) -> list:
    """Validate required kwargs and apply per-op defaults for the TransformKey.

    ``ntile`` requires positive-integer ``n``; ``time_shift`` requires integer
    ``periods`` (may be negative); ``lag`` / ``lead`` default ``periods=1``.
    Integer checks accept integral ``Decimal`` (``normalize_scalar`` wraps numbers)."""
    def _ensure_positive_integer(value: object, *, kw: str) -> None:
        if isinstance(value, bool):
            raise ValueError(
                f"Transform {op!r} keyword {kw} must be a positive "
                f"integer; got {value!r}."
            )
        if isinstance(value, int):
            ival = value
        elif isinstance(value, Decimal):
            if value != value.to_integral_value():
                raise ValueError(
                    f"Transform {op!r} keyword {kw} must be a positive "
                    f"integer; got {value!r}."
                )
            ival = int(value)
        else:
            raise ValueError(
                f"Transform {op!r} keyword {kw} must be a positive "
                f"integer; got {value!r}."
            )
        if ival <= 0:
            raise ValueError(
                f"Transform {op!r} keyword {kw} must be a positive "
                f"integer; got {value!r}."
            )

    if op == "ntile":
        if "n" not in seen:
            raise ValueError(
                "Transform 'ntile' requires keyword argument n (the "
                "number of buckets, a positive integer)."
            )
        n_value = next(v for k, v in kwargs if k == "n")
        _ensure_positive_integer(n_value, kw="n")
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
        _bind(a, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map, measure_ctx=measure_ctx)
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
