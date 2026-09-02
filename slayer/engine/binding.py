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
  at ``source_model``. ``__``-bearing refs resolve by ordinary exact-match
  against a column on the model (DEV-1743). I2: ``source_model is not None``
  is asserted.
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

import difflib
from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.errors import (
    AggregationNotAllowedError,
    IllegalScopeReferenceError,
    IllegalWindowInFilterError,
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

    if not isinstance(bound_col, (ColumnKey, ColumnSqlKey)):
        # Defensive — the binder should never produce a non-column key
        # for an identifier ref against a ModelScope.
        raise ValueError(
            f"TimeDimension {full!r} did not resolve to a column "
            f"reference (got {type(bound_col).__name__})."
        )

    # DEV-1450 follow-up #4a: a derived (Column.sql) temporal column routes
    # through ColumnSqlKey; TimeTruncKey.column accepts both kinds, so the
    # leaf / path are read via the kind-agnostic helpers.
    terminal_model = _terminal_model_for_path(
        path=column_path(bound_col),
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
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> BoundFilter:
    """Bind a parsed filter predicate + classify its phase.

    Walks the bound tree to gather every referenced ``ValueKey`` and
    raises ``IllegalWindowInFilterError`` if any referenced
    ``Column.sql`` contains a window function (DEV-1369: no
    auto-promotion).

    ``alias_map`` maps a stage's declared-measure names (user ``name``,
    canonical alias, declared name) to their bound ``ValueKey`` so a
    filter may reference a declared measure by alias (P4 / DEV-1445:
    ``filters=["rev >= 100"]`` for a measure declared ``name="rev"``).
    A bare ref that matches an alias interns onto that exact slot rather
    than resolving against the model columns — so the colon form and the
    alias form share one slot.
    """
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


# ---------------------------------------------------------------------------
# Walk helper
# ---------------------------------------------------------------------------


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
        # DEV-1475: walk the column LHS and every literal RHS so the
        # cross-model filter router and the windowed-column rejection
        # check both see InKey-rooted predicates the same way they see
        # BetweenKey ones.
        yield from walk_value_keys(key.column)
        for v in key.values:
            yield from walk_value_keys(v)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _bind(
    parsed: ParsedExpr,
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    in_filter: bool,
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
) -> ValueKey:
    if isinstance(parsed, Literal):
        return LiteralKey(value=normalize_scalar(parsed.value))

    if isinstance(parsed, Ref):
        return _resolve_ref(
            parsed.name, scope=scope, bundle=bundle, alias_map=alias_map,
        )

    if isinstance(parsed, DottedRef):
        return _resolve_dotted(parsed.parts, scope=scope, bundle=bundle)

    if isinstance(parsed, StarSource):
        return StarKey()

    if isinstance(parsed, AggCall):
        return _bind_agg(parsed, scope=scope, bundle=bundle)

    if isinstance(parsed, TransformCall):
        return _bind_transform(
            parsed, scope=scope, bundle=bundle, alias_map=alias_map,
        )

    if isinstance(parsed, ScalarCall):
        return _bind_scalar(
            parsed, scope=scope, bundle=bundle, in_filter=in_filter,
            alias_map=alias_map,
        )

    if isinstance(parsed, Arith):
        return ArithmeticKey(
            op=parsed.op,
            operands=(
                _bind(parsed.left, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map),
                _bind(parsed.right, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map),
            ),
        )

    if isinstance(parsed, UnaryOp):
        return ArithmeticKey(
            op=parsed.op,
            operands=(_bind(parsed.operand, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map),),
        )

    if isinstance(parsed, Cmp):
        # DEV-1475: ``IN`` / ``NOT IN`` predicates fold into a single
        # ``InKey`` rather than an ``ArithmeticKey`` so the SQL generator
        # has a structured handle on the column + literal-tuple shape.
        # The parser already validated that ``parsed.right`` is a
        # ``TupleLit`` of ``Literal`` elements for these ops.
        if parsed.op in ("in", "not in"):
            return _bind_in(
                parsed,
                scope=scope, bundle=bundle, in_filter=in_filter,
                alias_map=alias_map,
            )
        return ArithmeticKey(
            op=parsed.op,
            operands=(
                _bind(parsed.left, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map),
                _bind(parsed.right, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map),
            ),
        )

    if isinstance(parsed, BoolOp):
        operands = tuple(
            _bind(v, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map)
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
) -> InKey:
    """Bind an ``IN`` / ``NOT IN`` predicate into an ``InKey`` (DEV-1475).

    The LHS is bound through the normal column-resolution path
    (``ColumnKey`` for a bare ref, ``ColumnKey`` with a non-empty
    ``path`` for a dotted join ref, ``ColumnSqlKey`` for a derived
    column, or an alias-map hit for a declared-measure name).

    The RHS is a ``TupleLit`` of ``Literal`` nodes (the parser already
    enforced that shape); every element binds to a ``LiteralKey`` after
    scalar normalization.
    """
    if not isinstance(parsed.right, TupleLit):
        # Defensive — the parser's ``ast.Compare`` branch guarantees a
        # ``TupleLit`` on the RHS for ``in`` / ``not in``. Surface a
        # clear runtime error if a future caller bypasses the parser.
        raise ValueError(
            f"_bind_in: expected TupleLit on RHS of {parsed.op!r}, got "
            f"{type(parsed.right).__name__}."
        )
    column = _bind(
        parsed.left,
        scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map,
    )
    values = tuple(
        LiteralKey(value=normalize_scalar(elt.value))
        for elt in parsed.right.elements
    )
    # SQL's three-valued logic makes a NULL in the list a trap rather than a
    # member test. ``col IN (a, NULL)`` never matches on the NULL, and
    # ``col NOT IN (a, NULL)`` evaluates to NULL for EVERY row — so the filter
    # silently returns zero rows instead of "everything except a". Neither is
    # what the author meant, and neither announces itself.
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
    """A ``Did you mean 'X'?`` clause over the model's columns + named measures.

    Unnamed measures are skipped — ``None`` can't sort against strings.
    """
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
) -> ValueKey:
    """Resolve a bare identifier against the scope.

    A name present in ``alias_map`` (a stage's declared-measure aliases,
    supplied only on the filter/order path) interns onto that declared
    slot's ``ValueKey`` before any column lookup — so a filter referencing
    a measure by its user ``name`` shares the measure's slot (P4).
    """
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

    # DEV-1743: a ``__``-bearing name is no longer special — it resolves by the
    # ordinary exact-match below. A flat query-backed column literally named
    # ``stores__name`` (D5) binds when it exists; otherwise the normal
    # unknown-reference error fires.
    col = next((c for c in model.columns if c.name == name), None)
    if col is None:
        # Try ModelMeasure as a fallback for bare measure refs.
        mm = next((m for m in model.measures if m.name == name), None)
        if mm is not None:
            # A bare saved measure reaches the binder only as the source of an
            # explicit aggregation — a plain ``aov`` is inlined by expansion first.
            raise UnknownReferenceError(
                name=name,
                scope_kind="ModelScope",
                scope_summary=f"model {model.name!r}",
                suggestion=(
                    f"{name!r} is a saved measure on {model.name!r}, not a "
                    f"column, so it takes no aggregation. Reference it by its "
                    f"bare name {name!r} without a colon aggregation."
                ),
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

    if col.sql is not None and col.sql.strip() != name:
        return ColumnSqlKey(path=(), model=model.name, column_name=col.name)
    return ColumnKey(path=(), leaf=col.name)


def _walk_join_chain(
    *,
    hop_path: Tuple[str, ...],
    host,
    bundle: ResolvedSourceBundle,
    parts: Tuple[str, ...],
):
    """Walk ``hop_path`` join hops from ``host``, validating each hop against the
    current model's joins and rejecting a hop that revisits a model (circular
    join). Returns the terminal model. ``parts`` is the full dotted ref, used
    only for error messages."""
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
        # A path that walks back to an already-visited model is a circular join
        # (``a -> b -> a``): the leaf can never resolve, so reject it here rather
        # than fail confusingly on the leaf. Legacy-compatible ValueError.
        if nxt.name in visited_models:
            raise ValueError(
                f"Circular join detected resolving {'.'.join(parts)!r}: "
                f"revisits model {nxt.name!r}."
            )
        visited_models.add(nxt.name)
        current = nxt
    return current


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
    current = _walk_join_chain(
        hop_path=hop_path, host=host, bundle=bundle, parts=parts,
    )

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


def _resolve_dotted_star(
    parts: Tuple[str, ...],
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> StarKey:
    """Resolve a dotted star (``customers.*``, trailing ``*``) to a StarKey.

    Mirrors ``_resolve_dotted``'s self-prefix strip (C14) and join-chain
    validation, but the leaf is ``*`` (no terminal column) so the result
    is a ``StarKey`` whose ``path`` is the validated hop chain. An empty
    path after stripping is the local star (``orders.*`` on ``orders``).
    """
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
    # C14: strip same-model self-prefix (``orders.*`` on ``orders``).
    if hop_path and hop_path[0] == host.name:
        hop_path = hop_path[1:]
    # Validate the hop chain (raises on a missing / circular join) — the leaf is
    # ``*`` so the terminal model is not needed, only the validated hop path.
    _walk_join_chain(hop_path=hop_path, host=host, bundle=bundle, parts=parts)
    return StarKey(path=tuple(hop_path))


def _bind_agg_partition_keys(
    value, *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> frozenset:
    """Bind an aggregation ``partition_by`` value (single ref, list/tuple, or
    ``[]``) to a frozenset of column keys (DEV-1739)."""
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
        # Cross-model star: ``customers.*:count`` → a StarKey carrying the
        # join path so the cross-model planner routes COUNT(*) through the
        # join graph, exactly like ``customers.revenue:sum`` (P3). Parity
        # with the legacy dotted-star path.
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

    # Bind args / kwargs. For aggregations, identifier args/kwargs become
    # ColumnKey via the binder; scalars normalise. ``partition_by`` is lifted
    # out of kwargs onto ``partition_keys`` (DEV-1739): a single ref, a list,
    # or ``[]`` (grand total). ``None`` means no partition.
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
    # Codex review: enforce the per-column aggregation eligibility gates.
    # Without this, ``id:sum`` (a PK) or ``status:avg`` (text) compile
    # silently in the typed pipeline. The check is best-effort against
    # the bundle — sources whose target model can't be resolved (e.g.
    # an unreferenced join target) skip the check.
    # DEV-1576 / DEV-1717: heal alias + gate, then store the EFFECTIVE
    # (healed) name on the key so the generator resolves the canonical
    # aggregation rather than the raw parser token.
    effective_agg = _validate_agg_eligibility(
        source=source, agg=parsed.agg, bundle=bundle,
    )
    # DEV-1826 expression sources: numeric-only aggregations are rejected when
    # the expression is confidently non-numeric (per-column gates don't apply).
    if isinstance(source, EXPRESSION_SOURCE_KINDS):
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
    # DEV-1503 — stamp the typed non-anchor join paths on the SqlExprKey so
    # the planner's isolation trigger reads typed data, not parsed SQL. The
    # anchor is the model the filter is bound against — the joined target for
    # cross-model aggregates, the host for filtered-local. The anchor relation
    # uses the ``__``-canonical path alias when the anchor is a joined model.
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
    eligibility gates.

    Returns the **effective** (alias-healed) aggregation name, which the
    caller stores on ``AggregateKey.agg`` (DEV-1576 / DEV-1717) — the typed
    colon parser (``syntax.py``) does not normalise, so healing must land
    here, after the owning model is resolved, or the generator later fails on
    the raw token at ``_resolve_aggregation_def``.

    Healing (:func:`normalize_aggregation_name`) is **skipped** when the raw
    token exactly matches a custom aggregation registered on the owning model,
    so a custom ``countd`` wins over the ``countd -> count_distinct`` alias.

    Gate order (the binding contract):

    0. Unknown-name-first, for EVERY source shape (column, star, expression —
       DEV-1826): a name that is neither a built-in nor a model custom
       aggregation raises ``"Unknown aggregation ..."`` **before** the
       PK / whitelist / type gates, so ``*:bogus`` / ``bogus(*)`` never
       escape to SQL generation and a misspelled agg on an aggregatable
       column is not mislabelled as a type restriction.
    1. Primary-key columns are restricted to ``count`` / ``count_distinct``.
    2. An explicit ``Column.allowed_aggregations`` whitelist overrides
       type defaults.
    3. Otherwise, built-in aggregations are gated by
       ``DEFAULT_AGGREGATIONS_BY_TYPE``; model-custom aggregations are exempt.

    Star and expression sources have no column to attach a whitelist to, so
    the per-column gates (1-3) don't apply — for an expression the value is a
    new derived quantity owned by the query author (advisory gates,
    documented). Cross-model and derived (``ColumnSqlKey``) sources are
    best-effort: if the owning model can't be resolved through the bundle (an
    unresolved join target) validation is skipped — the compile-time path
    validator would have raised earlier on a truly broken ref.
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
    "time_shift": frozenset({"periods", "granularity"}),
    "lag": frozenset({"periods"}),
    "lead": frozenset({"periods"}),
    "rank": frozenset(),
    "percent_rank": frozenset(),
    "dense_rank": frozenset(),
    "ntile": frozenset({"n"}),
    "consecutive_periods": frozenset({"period"}),
}

# Positional-parameter signature (after the value) for the transforms whose
# documented DSL form accepts positional args: ``time_shift(x, periods,
# granularity)``, ``lag(x, periods)``, ``lead(x, periods)``. Each name maps the
# i-th positional onto the matching kwarg. Transforms absent here are
# keyword-only after the value.
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
) -> TransformKey:
    # ``alias_map`` lets a transform input reference a declared-measure
    # alias inside a filter (``change(rev) > 0``); partition_by must still
    # be a real column, so it is bound without the alias map.
    inp = _bind(
        parsed.input, scope=scope, bundle=bundle, in_filter=False,
        alias_map=alias_map,
    )
    # The value to transform is the first positional (``parsed.input``).
    # A few transforms accept further POSITIONAL params per the documented
    # DSL surface (``time_shift(x, periods, granularity)``,
    # ``lag(x, periods)``, ``lead(x, periods)``); map those onto their kwarg
    # names. Every other transform (rank family, cumsum, change,
    # consecutive_periods, ...) stays keyword-only after the value.
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
    # Positional params first, then explicit kwargs; a name supplied BOTH
    # ways is an error (ambiguous, e.g. ``time_shift(x, -1, periods=-2)``).
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
    alias_map: Optional[Dict[str, "ValueKey"]] = None,
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
    # Arity for EVERY allowlisted scalar, not just like. sqlglot's own
    # handling is inconsistent — a wrong-arity round silently drops the
    # extra argument, length emits SQL the database rejects — so a
    # mistyped filter deserves a clear error here instead.
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
        _bind(a, scope=scope, bundle=bundle, in_filter=in_filter, alias_map=alias_map)
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
