"""Stage 7a.5 (DEV-1450) — ExpressionBinder + FilterBinder.

The binder consumes a ``ParsedExpr`` (from ``slayer/engine/syntax.py``)
plus a scope (``ModelScope`` or ``StageSchema``) and a
``ResolvedSourceBundle`` (for join resolution). It produces a typed
``BoundExpr`` whose leaves are resolved ``ValueKey``s.

Public surface:

* ``bind_expr(parsed, *, scope, bundle) -> BoundExpr``
* ``bind_filter(parsed, *, scope, bundle) -> BoundFilter``

Two scope kinds (P5):

* ``ModelScope``: joins exist; dotted refs walk the join graph rooted
  at ``source_model``. ``__``-bearing refs raise
  ``IllegalScopeReferenceError`` unless they exact-match a column on
  the model. I2: ``source_model is not None`` is asserted.
* ``StageSchema``: flat namespace; dotted refs raise
  ``IllegalScopeReferenceError``; flat names with ``__`` are legal.

C14: same-model self-prefix in Mode-B (`orders.status` over an
``orders``-rooted query) is stripped before the join walk.

FilterBinder layers on top: ``bind_expr`` + phase classification
(``Phase.ROW`` / ``AGGREGATE`` / ``POST`` = the max phase of any
referenced slot) + walk for the referenced ``ValueKey``s + reject
filters that touch a windowed ``Column.sql``.

Dormant in 7a — no engine wiring. The planner (7a.6) is the first
consumer.
"""

from __future__ import annotations

from typing import List, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.errors import (
    IllegalScopeReferenceError,
    IllegalWindowInFilterError,
    UnknownFunctionError,
    UnknownReferenceError,
)
from slayer.core.enums import DataType
from slayer.core.keys import (
    SCALAR_FUNCTIONS,
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    SqlExprKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    normalize_scalar,
)
from slayer.core.models import SlayerModel
from slayer.core.query import TimeDimension
from slayer.core.scope import ModelScope, StageSchema
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
    UnaryOp,
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


# ---------------------------------------------------------------------------
# BoundExpr / BoundFilter
# ---------------------------------------------------------------------------


class BoundExpr(BaseModel):
    """A bound expression — its leaves are resolved ``ValueKey``s.

    ``value_key`` is the structural identity of the entire expression.
    ``phase`` is the property of ``value_key.phase`` (lifted for
    convenience).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    value_key: ValueKey

    @property
    def phase(self) -> Phase:
        return self.value_key.phase


class BoundFilter(BaseModel):
    """A bound filter predicate.

    The same ``value_key`` shape as ``BoundExpr`` (boolean ops and
    comparisons are encoded as ``ArithmeticKey`` with the corresponding
    op string), plus:

    * ``phase`` — the maximum phase any referenced slot reaches.
    * ``referenced_keys`` — every ``ValueKey`` touched anywhere in the
      bound tree (used by the cross-model planner's filter routing).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    value_key: ValueKey
    phase: Phase
    referenced_keys: Tuple[ValueKey, ...] = Field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def bind_expr(
    parsed: ParsedExpr,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> BoundExpr:
    """Bind a parsed expression against a scope.

    Returns a ``BoundExpr`` carrying the structural identity of the
    entire expression. Raises ``UnknownReferenceError`` if a ref doesn't
    resolve; ``IllegalScopeReferenceError`` if a dotted ref is used
    against a ``StageSchema`` (or vice versa for ``__`` against a
    ``ModelScope``).
    """
    value_key = _bind(parsed, scope=scope, bundle=bundle, in_filter=False)
    return BoundExpr(value_key=value_key)


def bind_time_dimension(
    td: TimeDimension,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> BoundExpr:
    """Bind a ``TimeDimension`` into a ``BoundExpr`` carrying a
    ``TimeTruncKey``.

    The underlying column is resolved against ``scope`` exactly like a
    Mode-B identifier ref (local name or dotted-join path); the bound
    column must be a plain ``ColumnKey`` whose ``Column.type`` is in the
    temporal bucket (``DATE`` / ``TIMESTAMP``).

    Stage 7b.3b limitations:

    * Only ``ModelScope`` with a non-None ``source_model`` is accepted.
      Downstream stages bind upstream-emitted truncated columns by flat
      name through ``bind_expr``; they do not re-truncate at a different
      grain through this entry point. Passing a ``StageSchema`` raises
      ``IllegalScopeReferenceError``.
    * Derived (``Column.sql`` is set) temporal columns route through
      ``ColumnSqlKey`` rather than ``ColumnKey``, and ``TimeTruncKey``
      is typed as ``column: ColumnKey``. Rather than silently widen the
      typed key, this stage rejects derived-TD columns with
      ``NotImplementedError`` and a clear message.
    """
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

    if isinstance(bound_col, ColumnSqlKey):
        raise NotImplementedError(
            f"TimeDimension {full!r} resolves to a derived column "
            f"(Column.sql set); derived TD columns are not yet supported "
            f"by the typed pipeline. Use the base temporal column "
            f"directly, or apply the granularity in an upstream stage. "
            f"(TimeTruncKey.column is typed as ColumnKey; widening it to "
            f"accept ColumnSqlKey is tracked as a follow-up.)"
        )
    if not isinstance(bound_col, ColumnKey):
        # Defensive — the binder should never produce a non-column key
        # for an identifier ref against a ModelScope.
        raise ValueError(
            f"TimeDimension {full!r} did not resolve to a column "
            f"reference (got {type(bound_col).__name__})."
        )

    terminal_model = _terminal_model_for_path(
        path=bound_col.path,
        scope=scope,
        bundle=bundle,
    )
    if terminal_model is None:
        # Shouldn't be reachable: _resolve_ref / _resolve_dotted would
        # already have raised. Defensive only.
        raise UnknownReferenceError(
            name=full,
            scope_kind="ModelScope",
            scope_summary=f"could not resolve terminal model for {full!r}",
            suggestion=None,
        )
    col = next(
        (c for c in terminal_model.columns if c.name == bound_col.leaf),
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
    """Walk ``path`` from ``scope.source_model`` and return the terminal
    model. Returns the host when ``path`` is empty.
    """
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
) -> BoundFilter:
    """Bind a parsed filter predicate + classify its phase.

    Walks the bound tree to gather every referenced ``ValueKey`` and
    raises ``IllegalWindowInFilterError`` if any referenced
    ``Column.sql`` contains a window function (DEV-1369: no
    auto-promotion).
    """
    value_key = _bind(parsed, scope=scope, bundle=bundle, in_filter=True)
    refs = tuple(walk_value_keys(value_key))
    phase = max(
        (k.phase for k in refs),
        default=value_key.phase,
    )
    _reject_windowed_column_sql(refs, scope=scope, bundle=bundle, parsed=parsed)
    return BoundFilter(
        value_key=value_key, phase=phase, referenced_keys=refs,
    )


# ---------------------------------------------------------------------------
# Walk helper
# ---------------------------------------------------------------------------


_VALUE_KEY_TYPES = (
    ColumnKey, ColumnSqlKey, StarKey, LiteralKey,
    AggregateKey, TransformKey, ArithmeticKey, ScalarCallKey,
    BetweenKey, TimeTruncKey,
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


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _bind(
    parsed: ParsedExpr,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    in_filter: bool,
) -> ValueKey:
    if isinstance(parsed, Literal):
        return LiteralKey(value=normalize_scalar(parsed.value))

    if isinstance(parsed, Ref):
        return _resolve_ref(parsed.name, scope=scope, bundle=bundle)

    if isinstance(parsed, DottedRef):
        return _resolve_dotted(parsed.parts, scope=scope, bundle=bundle)

    if isinstance(parsed, StarSource):
        return StarKey()

    if isinstance(parsed, AggCall):
        return _bind_agg(parsed, scope=scope, bundle=bundle)

    if isinstance(parsed, TransformCall):
        return _bind_transform(parsed, scope=scope, bundle=bundle)

    if isinstance(parsed, ScalarCall):
        return _bind_scalar(parsed, scope=scope, bundle=bundle, in_filter=in_filter)

    if isinstance(parsed, Arith):
        return ArithmeticKey(
            op=parsed.op,
            operands=(
                _bind(parsed.left, scope=scope, bundle=bundle, in_filter=in_filter),
                _bind(parsed.right, scope=scope, bundle=bundle, in_filter=in_filter),
            ),
        )

    if isinstance(parsed, UnaryOp):
        return ArithmeticKey(
            op=parsed.op,
            operands=(_bind(parsed.operand, scope=scope, bundle=bundle, in_filter=in_filter),),
        )

    if isinstance(parsed, Cmp):
        return ArithmeticKey(
            op=parsed.op,
            operands=(
                _bind(parsed.left, scope=scope, bundle=bundle, in_filter=in_filter),
                _bind(parsed.right, scope=scope, bundle=bundle, in_filter=in_filter),
            ),
        )

    if isinstance(parsed, BoolOp):
        operands = tuple(
            _bind(v, scope=scope, bundle=bundle, in_filter=in_filter)
            for v in parsed.operands
        )
        return ArithmeticKey(op=parsed.op, operands=operands)

    raise ValueError(
        f"Unsupported ParsedExpr node: {type(parsed).__name__}"
    )


def _resolve_ref(
    name: str,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> ValueKey:
    """Resolve a bare identifier against the scope."""
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

    if "__" in name:
        # The Mode-B parser already rejects `__` for user input; this
        # branch is reached only via direct ParsedExpr.Ref construction
        # (e.g., downstream binders for StageSchema flat columns). The
        # `__` is legal iff it exact-matches a column literally named
        # that way on the model (legacy persisted query-backed columns).
        if any(c.name == name for c in model.columns):
            return ColumnKey(path=(), leaf=name)
        raise IllegalScopeReferenceError(
            name=name,
            scope_kind="ModelScope",
            reason=(
                "`__` is reserved for internal join-path aliases. "
                "Use single-dot DSL paths in queries."
            ),
        )

    col = next((c for c in model.columns if c.name == name), None)
    if col is None:
        # Try ModelMeasure as a fallback for bare measure refs.
        mm = next((m for m in model.measures if m.name == name), None)
        if mm is not None:
            # ModelMeasure expansion lives in the planner; the binder
            # raises here so callers know expansion is required.
            raise UnknownReferenceError(
                name=name,
                scope_kind="ModelScope",
                scope_summary=f"model {model.name!r}",
                suggestion=(
                    f"{name!r} is a saved measure on {model.name!r}; "
                    f"expand via ModelMeasure expansion before binding."
                ),
            )
        raise UnknownReferenceError(
            name=name,
            scope_kind="ModelScope",
            scope_summary=(
                f"model {model.name!r} columns: "
                f"{[c.name for c in model.columns]}"
            ),
            suggestion=None,
        )

    if col.sql is not None and col.sql.strip() != name:
        return ColumnSqlKey(path=(), model=model.name, column_name=col.name)
    return ColumnKey(path=(), leaf=col.name)


def _resolve_dotted(
    parts: Tuple[str, ...],
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> ValueKey:
    """Resolve a dotted ref against the scope."""
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

    # C14: strip same-model self-prefix.
    host = scope.source_model
    if parts and parts[0] == host.name:
        parts = parts[1:]
        if not parts:
            raise UnknownReferenceError(
                name=host.name,
                scope_kind="ModelScope",
                scope_summary=f"model {host.name!r}",
                suggestion="self-prefix only — expected a column or join target.",
            )
        if len(parts) == 1:
            return _resolve_ref(parts[0], scope=scope, bundle=bundle)

    # parts now has the join walk to perform.
    if len(parts) == 1:
        # Single-segment after possible stripping — already a local ref.
        return _resolve_ref(parts[0], scope=scope, bundle=bundle)

    # Walk join chain. parts[:-1] are join targets; parts[-1] is the leaf column.
    hop_path = parts[:-1]
    leaf = parts[-1]
    current = host
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
                suggestion=f"no join from {current.name!r} to {hop!r}.",
            )
        nxt = bundle.get_referenced_model(hop)
        if nxt is None:
            raise UnknownReferenceError(
                name=".".join(parts),
                scope_kind="ModelScope",
                scope_summary=f"target {hop!r} not in source bundle",
                suggestion=None,
            )
        current = nxt

    # `current` is the terminal model; `leaf` is the column on it.
    col = next((c for c in current.columns if c.name == leaf), None)
    if col is None:
        raise UnknownReferenceError(
            name=".".join(parts),
            scope_kind="ModelScope",
            scope_summary=(
                f"model {current.name!r} columns: "
                f"{[c.name for c in current.columns]}"
            ),
            suggestion=None,
        )

    if col.sql is not None and col.sql.strip() != leaf:
        # Derived column on a joined model. The path is part of the key
        # so the cross-model planner can route via the join graph.
        return ColumnSqlKey(
            path=tuple(hop_path), model=current.name, column_name=leaf,
        )
    return ColumnKey(path=tuple(hop_path), leaf=leaf)


def _bind_agg(
    parsed: AggCall, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> AggregateKey:
    if isinstance(parsed.source, StarSource):
        source = StarKey()
    else:
        bound_source = _bind(
            parsed.source, scope=scope, bundle=bundle, in_filter=False,
        )
        if not isinstance(bound_source, (ColumnKey, ColumnSqlKey, StarKey)):
            raise ValueError(
                f"Aggregation source must resolve to a column / star, "
                f"got {type(bound_source).__name__}."
            )
        source = bound_source

    # Bind args / kwargs. For aggregations, identifier args/kwargs become
    # ColumnKey via the binder; scalars normalise.
    args = tuple(
        _bind_agg_arg(a, scope=scope, bundle=bundle) for a in parsed.args
    )
    kwargs = tuple(
        (k, _bind_agg_arg(v, scope=scope, bundle=bundle))
        for k, v in parsed.kwargs
    )
    # DEV-1450 stage 7b.12: propagate ``Column.filter`` into the
    # AggregateKey's structural identity. The resolved source's column
    # may carry a Mode-A SQL fragment (``filter="status = 'paid'"``)
    # that wraps the aggregate argument as ``SUM(CASE WHEN ... THEN col
    # END)``. Two aggregates over the same column with different
    # ``Column.filter`` therefore differ at the key level; same-filter
    # ones intern (legacy CASE-WHEN-at-agg-time semantics, preserved by
    # the spec's C5 + ``column_filter_key`` invariants).
    column_filter_key = _resolve_column_filter_key(
        source=source, bundle=bundle,
    )
    return AggregateKey(
        source=source,
        agg=parsed.agg,
        args=args,
        kwargs=kwargs,
        column_filter_key=column_filter_key,
    )


def _resolve_column_filter_key(
    *, source, bundle: ResolvedSourceBundle,
) -> Optional[SqlExprKey]:
    """Look up the resolved source's ``Column.filter`` and convert it
    to a ``SqlExprKey``.

    Returns ``None`` for ``StarKey`` sources (``*:count`` has no column
    to attach a filter to) and for any column whose ``filter`` is
    unset. For ``ColumnKey`` / ``ColumnSqlKey`` sources the resolver
    walks ``source.path`` through the bundle and reads the target
    model's column entry. Models the planner doesn't have access to
    (e.g. an unresolved join target) are tolerated — no exception is
    raised; the key just stays ``None`` (the compile-time validator
    in path resolution would have caught a genuinely missing model).
    """
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
    return SqlExprKey(canonical_sql=col.filter)


def _bind_agg_arg(
    parsed: ParsedExpr, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
):
    """Bind one positional / kwarg argument of an aggregation.

    The AggregateKey shape stores Scalars inline (not as LiteralKey)
    so identity matches the spec — see ``slayer/core/keys.py``.
    Identifier args become ``ColumnKey`` / ``ColumnSqlKey``; literal
    args normalise via ``normalize_scalar``.
    """
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

    Folds ``Literal`` directly, and unary ``-`` over a numeric ``Literal``
    (the AST shape Python emits for ``periods=-1``) into the negated
    literal value. Returns ``_NOT_SCALAR`` for anything that doesn't
    reduce — transform kwargs are typed as ``Scalar``, so a non-scalar
    expression is a binding error.
    """
    if isinstance(parsed, Literal):
        return normalize_scalar(parsed.value)
    if (
        isinstance(parsed, UnaryOp)
        and parsed.op == "-"
        and isinstance(parsed.operand, Literal)
    ):
        from decimal import Decimal

        inner = parsed.operand.value
        if isinstance(inner, bool):
            # Reject explicitly — ``-True`` is nonsense and bool is an
            # int subclass that would otherwise pass the next branch.
            return _NOT_SCALAR
        if isinstance(inner, (int, float, Decimal)):
            return normalize_scalar(-inner)
    return _NOT_SCALAR


# Per-op kwarg whitelist for the typed pipeline. Broader than the legacy
# ``slayer.core.formula._ALLOWED_TRANSFORM_KWARGS`` because the new
# pipeline allows ``partition_by`` on more than just the rank family
# (DEV-1450 C6: ``change(measure, partition_by=...)`` threads through to
# the desugared time_shift). Every transform also implicitly accepts
# ``partition_by`` — that branch is handled before the whitelist check.
_TRANSFORM_KWARG_RULES: dict = {
    "cumsum": frozenset(),
    "change": frozenset(),
    "change_pct": frozenset(),
    "first": frozenset(),
    "last": frozenset(),
    "time_shift": frozenset({"periods"}),
    "lag": frozenset({"periods"}),
    "lead": frozenset({"periods"}),
    "rank": frozenset(),
    "percent_rank": frozenset(),
    "dense_rank": frozenset(),
    "ntile": frozenset({"n"}),
    "consecutive_periods": frozenset({"period"}),
}


def _bind_transform(
    parsed: TransformCall, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> TransformKey:
    inp = _bind(parsed.input, scope=scope, bundle=bundle, in_filter=False)
    # Typed pipeline: transforms take one positional (the value to
    # transform) and the rest as kwargs. Reject any extra positional
    # args to force the kwarg form (avoids ambiguity like
    # ``lag(amount:sum, 2)`` where ``2`` might be ``periods``).
    if parsed.args:
        raise ValueError(
            f"Transform {parsed.op!r} accepts exactly one positional "
            f"argument (the value to transform); pass any offset, "
            f"partition, or other settings as keyword arguments "
            f"(e.g. ``{parsed.op}(value, periods=-1)``)."
        )
    args: List = []
    kwargs: List = []
    partition_keys: List = []
    allowed_kwargs = _TRANSFORM_KWARG_RULES.get(parsed.op, frozenset())
    seen_kwargs: set = set()
    for k, v in parsed.kwargs:
        if k == "partition_by":
            bound_v = _bind(v, scope=scope, bundle=bundle, in_filter=False)
            if isinstance(bound_v, (ColumnKey, ColumnSqlKey)):
                partition_keys.append(bound_v)
            else:
                raise ValueError(
                    f"transform {parsed.op!r} partition_by must resolve "
                    f"to a column reference; got "
                    f"{type(bound_v).__name__}."
                )
            continue
        if k not in allowed_kwargs:
            raise ValueError(
                f"Transform {parsed.op!r} does not accept keyword "
                f"argument {k!r}. Accepted: "
                f"{sorted(allowed_kwargs | {'partition_by'})}."
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
    # Per-op required-kwarg validation + defaults.
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
    """Validate required kwargs and apply per-op defaults for the typed
    TransformKey.

    Validation:
    * ``ntile`` requires ``n``; ``n`` must be a positive integer
      (``bool`` rejected — it's an ``int`` subclass in Python but a
      boolean ``True``/``False`` is never a sensible bucket count).
    * ``time_shift`` requires ``periods`` (integer; may be negative).

    Defaults:
    * ``lag`` / ``lead`` default ``periods=1`` when missing so the
      typed TransformKey carries the resolved kwarg list; the SQL
      generator can render PARTITION/ORDER without re-applying defaults.

    ``normalize_scalar`` wraps numeric literals in ``Decimal``, so the
    integer checks accept ``Decimal`` whose value is integral as well
    as plain ``int``.
    """
    from decimal import Decimal

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
) -> ScalarCallKey:
    if parsed.name not in SCALAR_FUNCTIONS:
        # Defence in depth: the parser already enforces the allowlist,
        # but direct ParsedExpr construction can bypass the parser.
        # Re-check here so the typed key family is always sound.
        raise UnknownFunctionError(
            name=parsed.name,
            location="(binder)",
            suggestion=(
                f"Mode-B scalar calls are restricted to "
                f"{sorted(SCALAR_FUNCTIONS)}."
            ),
        )
    args = tuple(
        _bind(a, scope=scope, bundle=bundle, in_filter=in_filter)
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
    """Raise ``IllegalWindowInFilterError`` if any referenced
    ``ColumnSqlKey`` has a windowed ``Column.sql`` body.

    DEV-1369 removed predicate-promotion; filters touching a windowed
    column SQL now raise.
    """
    if isinstance(scope, StageSchema):
        # StageSchema columns don't carry Column.sql in the bundle;
        # window detection is handled when the upstream stage was bound.
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
