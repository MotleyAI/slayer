"""The single ``ValueKey`` → sqlglot-AST renderer, parameterised by :class:`RenderContext`
rather than by call site so one key can't render two ways. Column-like leaves anchor through
``ScopeFrame.resolve``; a missing facility raises instead of degrading quietly."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import RenderContextMissingFacilityError
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
)
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.render.aggregates import (
    DISPATCH_DISTINCT,
    DISPATCH_SIMPLE,
    is_builtin_agg,
    resolve_agg_entry,
)
from slayer.sql.render.parse import (  # noqa: F401 — re-exported render surface
    rewrite_log_aliases as rewrite_log_alias,
)
# The pure operator / scalar / literal composers live in the scope-free leaf
# module ``row_expr`` (DEV-1826) so ``ScopeFrame`` can reuse them; re-imported
# here so this module keeps its full render surface.
from slayer.sql.render.row_expr import (  # noqa: F401 — re-exported render surface
    _literal,
    group_is_operands,
    group_unary_operand,
    iif_case_chain,
    render_arithmetic,
    render_scalar_call,
)
from slayer.sql.scope import ScopeFrame

_AGG_BUILDER = "composites.agg_builder"

# Stands in when a HAVING aggregate wasn't materialised in the base SELECT (so the ``filtered_rn_map`` lookup misses to the unfiltered ranked path).
_HAVING_PLACEHOLDER = "__having_ref__"


def _wrap_cast_for_type(
    expr: exp.Expression, dt: Optional[DataType],
) -> exp.Expression:
    """Wrap ``expr`` in ``CAST(expr AS <dt>)`` to enforce a declared ``DataType``. Skipped for
    ``None`` / ``TEXT`` / opaque types and a bare ``exp.Column`` (on SQLite the cast can truncate). Idempotent."""
    if dt is None or dt == DataType.TEXT or dt.is_opaque:
        return expr
    if isinstance(expr, exp.Column):
        return expr
    target = exp.DataType.Type(dt.value)
    if isinstance(expr, exp.Cast):
        existing = expr.args.get("to")
        if isinstance(existing, exp.DataType) and existing.this == target:
            return expr
    return exp.Cast(this=expr, to=exp.DataType(this=target))


def _filter_cast_type(dt: Optional[DataType]) -> Optional[DataType]:
    """CAST target for a derived column inside WHERE/HAVING; temporal types suppressed
    (``CAST(text AS TIMESTAMP)`` truncates on SQLite, and a base column here isn't cast either)."""
    if dt in (DataType.DATE, DataType.TIMESTAMP):
        return None
    return dt


def _ranked_value_cast_type(dt: Optional[DataType]) -> Optional[DataType]:
    """CAST target for a ``first`` / ``last`` value; temporal casts suppressed (the value IS
    the raw picked column, redundant and truncating on SQLite). Non-temporal types keep their cast."""
    if dt in (DataType.DATE, DataType.TIMESTAMP):
        return None
    return dt


class FilterFacilities(BaseModel):
    """What WHERE / HAVING rendering needs. ``agg_builder`` (the HAVING seam) emits the
    aggregate as an expression, not its alias — Postgres rejects aliases in HAVING;
    ``cast_column_sql`` casts derived leaves; ``paren_comparison_operands`` over-parenthesises."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    slot_by_key: Dict[Any, Any] = Field(default_factory=dict)
    aliases_by_slot_id: Dict[str, List[str]] = Field(default_factory=dict)
    agg_builder: Optional[
        Callable[[AggregateKey, Optional[Any], str], exp.Expression]
    ] = None
    cast_column_sql: bool = False
    paren_comparison_operands: bool = True


class CompositeFacilities(BaseModel):
    """What AGGREGATE-phase composite rendering needs; ``agg_builder`` is the seam to ``_build_agg`` (absent, the renderer handles simple built-ins and raises for the rest)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    agg_builder: Optional[Callable[[AggregateKey], exp.Expression]] = None
    rn_suffix_map: Optional[Dict[str, str]] = None
    default_time_col: Optional[str] = None
    filtered_rn_map: Optional[Dict[str, str]] = None
    filtered_match_map: Optional[Dict[str, str]] = None
    composite_alias_by_key: Optional[Dict[Any, str]] = None
    resolved_agg_kwargs: Optional[Dict[Any, Any]] = None
    value_alias_by_sql: Optional[Dict[str, str]] = None


class AliasFacilities(BaseModel):
    """The aliases an earlier scope projected; its presence switches the five slotted kinds to
    ALIAS-EXCLUSIVE resolution (rebuilding from source in an alias-only CTE is wrong SQL). An
    absent slot RAISES; ``table_by_slot_id`` carries the qualifier."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    slot_id_by_key: Dict[Any, str] = Field(default_factory=dict)
    available_alias_by_slot_id: Dict[str, str] = Field(default_factory=dict)
    table_by_slot_id: Dict[str, str] = Field(default_factory=dict)


class RenderContext(BaseModel):
    """Everything a render needs beyond the key. ``consumer`` is the projection-boundary seam;
    ``scope`` is Optional — a reference-anchoring key with none fails closed (not a deep ``AttributeError``)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    scope: Optional[ScopeFrame] = None
    dialect: SqlDialect
    consumer: Optional[ScopeFrame] = None
    filters: Optional[FilterFacilities] = None
    composites: Optional[CompositeFacilities] = None
    aliases: Optional[AliasFacilities] = None


def _require(*, ctx: RenderContext, facility: str, key: Any) -> Any:
    got = getattr(ctx, facility, None)
    if got is None:
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__, facility=facility,
        )
    return got


def _require_scope(ctx: RenderContext, key: Any) -> ScopeFrame:
    """Fail closed when a reference-anchoring key has no scope (alias-environment families carry ``scope=None``)."""
    if ctx.scope is None:
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility="scope",
            detail="a scope is required to anchor this reference",
        )
    return ctx.scope


# Comparison ops whose multi-term operands the filter families parenthesise unconditionally.
_COMPARISON_OPS = frozenset({"==", "=", "!=", "<>", "<", "<=", ">", ">="})


def _paren_if_binary(node: exp.Expression) -> exp.Expression:
    """Wrap an ``exp.Binary`` operand in ``(...)`` (self-delimiting); other nodes pass through."""
    return exp.Paren(this=node) if isinstance(node, exp.Binary) else node


def _render_via_alias(key: ValueKey, ctx: RenderContext) -> exp.Expression:
    """Alias-exclusive resolution: the materialised alias for ``key`` (table-qualified when
    the facility carries a qualifier); an unmaterialised slot RAISES."""
    facilities = ctx.aliases
    assert facilities is not None  # guarded by the caller
    slot_id = facilities.slot_id_by_key.get(key)
    alias = (
        facilities.available_alias_by_slot_id.get(slot_id)
        if slot_id is not None
        else None
    )
    if alias is None:
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility="aliases",
            detail=f"{type(key).__name__} is not materialised as a slot",
        )
    col_ident = exp.to_identifier(alias, quoted=True)
    table = facilities.table_by_slot_id.get(slot_id)
    if table:
        return exp.Column(this=col_ident, table=exp.to_identifier(table))
    return exp.Column(this=col_ident)


_ALIAS_SLOTTED_KINDS: Tuple[type, ...] = (
    ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey, TransformKey,
)


def _render_filter_aggregate(
    key: AggregateKey, ctx: RenderContext,
) -> exp.Expression:
    """The HAVING seam: recover the aggregate's base-SELECT alias (or placeholder) and hand it to ``_build_agg``, rendered as an expression so HAVING works where SELECT aliases are rejected."""
    filters = ctx.filters
    assert filters is not None and filters.agg_builder is not None  # caller-guarded
    slot = filters.slot_by_key.get(key)
    having_full_alias = _HAVING_PLACEHOLDER
    if slot is not None:
        aliases = filters.aliases_by_slot_id.get(slot.id)
        if aliases:
            having_full_alias = aliases[0]
    return filters.agg_builder(key, slot, having_full_alias)


def _render_aggregate(key: AggregateKey, ctx: RenderContext) -> exp.Expression:
    """Dispatch an ``AggregateKey``: filter HAVING seam, then composite builder, then built-in."""
    if ctx.filters is not None and ctx.filters.agg_builder is not None:
        return _render_filter_aggregate(key, ctx)

    facilities = _require(ctx=ctx, facility="composites", key=key)
    if facilities.agg_builder is not None:
        return facilities.agg_builder(key)

    return _render_builtin_aggregate(key, ctx)


def _render_builtin_aggregate(  # NOSONAR(S3776) — sequential fail-closed guards over the built-in agg contract (mechanism gate, then the FIELD guards that keep a filtered / parametric / cross-star aggregate from silently rendering as a plain SUM). Each guard IS a distinct wrong-number-vs-error boundary; merging them hides which one fired.
    key: AggregateKey, ctx: RenderContext,
) -> exp.Expression:
    """Render a simple / distinct built-in aggregate; anything it can't faithfully emit (custom mechanisms, filtered / parametric sources, cross-model stars) refuses."""
    if not is_builtin_agg(key.agg):
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility=_AGG_BUILDER,
            detail=f"custom aggregation {key.agg!r} needs the generator's builder",
        )
    entry = resolve_agg_entry(key.agg)
    if entry.dispatch not in (DISPATCH_SIMPLE, DISPATCH_DISTINCT):
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility=_AGG_BUILDER,
            detail=(
                f"aggregation {key.agg!r} renders via the {entry.dispatch!r} "
                f"mechanism, which needs the generator's builder"
            ),
        )
    # The next guards refuse a key whose FIELDS need the generator (else a filtered aggregate renders as a plain SUM over excluded rows).
    if key.column_filter_key is not None:
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility=_AGG_BUILDER,
            detail=(
                "the aggregate's source carries a column filter, which needs "
                "the generator's CASE-WHEN wrapper"
            ),
        )
    if key.kwargs or key.args:
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility=_AGG_BUILDER,
            detail=(
                f"aggregation {key.agg!r} carries args/kwargs, which need the "
                f"generator's parameter resolution"
            ),
        )
    if isinstance(key.source, StarKey):
        if key.source.path:
            # ``customers.*:count`` counts the JOINED relation's rows (needs the join graph); a bare ``*`` would count host rows.
            raise RenderContextMissingFacilityError(
                key_kind=type(key).__name__,
                facility=_AGG_BUILDER,
                detail=(
                    f"cross-model star over path {key.source.path!r} needs the "
                    f"generator's join-graph routing"
                ),
            )
        if key.agg != "count":
            # ``COUNT`` is the only aggregation a bare star is defined for (else ``SUM(*)`` etc.).
            raise NotImplementedError(
                f"Aggregation {key.agg!r} cannot take ``*`` as its source; "
                f"only 'count' is defined over a bare star.",
            )
        inner: exp.Expression = exp.Star()
    else:
        inner = _require_scope(ctx, key).resolve(
            key.source, consumer=ctx.consumer,
        )
    if entry.node_class is None:  # pragma: no cover — dispatch gate guarantees it
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility=_AGG_BUILDER,
            detail=f"aggregation {key.agg!r} has no direct sqlglot node",
        )
    if entry.dispatch == DISPATCH_DISTINCT:
        return entry.node_class(this=exp.Distinct(expressions=[inner]))
    return entry.node_class(this=inner)


def render_value_key(  # NOSONAR(S3776) — sequential dispatch over the closed ValueKey union; each branch IS that type's render contract, and splitting them is exactly the fragmentation this module removes.
    *, key: ValueKey, ctx: RenderContext,
) -> exp.Expression:
    """Render ``key`` to sqlglot AST in ``ctx``."""
    # ALIAS-EXCLUSIVE mode: intercepted before every scope branch so a miss RAISES.
    if ctx.aliases is not None and isinstance(key, _ALIAS_SLOTTED_KINDS):
        return _render_via_alias(key, ctx)

    if isinstance(key, ColumnKey):
        return _require_scope(ctx, key).resolve(key, consumer=ctx.consumer)

    if isinstance(key, ColumnSqlKey):
        scope = _require_scope(ctx, key)
        resolved = scope.resolve(key, consumer=ctx.consumer)
        # Filter-side CAST for a DERIVED leaf (temporal suppressed); target-scope never casts.
        if ctx.filters is not None and ctx.filters.cast_column_sql:
            col_type = scope.column_type(key)
            resolved = _wrap_cast_for_type(resolved, _filter_cast_type(col_type))
        return resolved

    if isinstance(key, StarKey):
        # Like the aggregate branch: a pathed star needs the join graph; a bare ``*`` counts the host's rows.
        if key.path:
            raise RenderContextMissingFacilityError(
                key_kind=type(key).__name__,
                facility=_AGG_BUILDER,
                detail=(
                    f"cross-model star over path {key.path!r} needs the "
                    f"generator's join-graph routing"
                ),
            )
        return exp.Star()

    if isinstance(key, LiteralKey):
        return _literal(key.value)

    if isinstance(key, TimeTruncKey):
        column = _require_scope(ctx, key).resolve(
            key.column, consumer=ctx.consumer,
        )
        # The dialect owns the per-backend wire form; a literal DATE_TRUNC would name a function SQLite lacks.
        return ctx.dialect.build_date_trunc(
            col_expr=column,
            granularity=TimeGranularity(key.granularity),
            parse=lambda sql: sqlglot.parse_one(
                sql, dialect=ctx.dialect.sqlglot_name,
            ),
        )

    if isinstance(key, ArithmeticKey):
        op = key.op.lower()
        operands = [render_value_key(key=o, ctx=ctx) for o in key.operands]
        if (
            ctx.filters is not None
            and ctx.filters.paren_comparison_operands
            and op in _COMPARISON_OPS
        ):
            operands = [_paren_if_binary(o) for o in operands]
        return render_arithmetic(op=op, operands=operands)

    if isinstance(key, ScalarCallKey):
        if key.name == "iif":
            return _render_iif_case(key=key, ctx=ctx)
        args = [
            render_value_key(key=a, ctx=ctx)
            if isinstance(a, _VALUE_KEY_TYPES)
            else _literal(a)
            for a in key.args
        ]
        return render_scalar_call(
            name=key.name, args=args, dialect=ctx.dialect,
        )

    if isinstance(key, BetweenKey):
        return exp.Between(
            this=render_value_key(key=key.column, ctx=ctx),
            low=render_value_key(key=key.low, ctx=ctx),
            high=render_value_key(key=key.high, ctx=ctx),
        )

    if isinstance(key, InKey):
        # Backstop for the bind-time rule: a NULL member breaks IN, and ``NOT IN`` with one matches no rows.
        if any(v.value is None for v in key.values):
            raise NotImplementedError(
                "NULL is not allowed inside an IN list: 'NOT IN' with a NULL "
                "matches no rows. Test for null separately with IS NULL / "
                "IS NOT NULL.",
            )
        node = exp.In(
            this=render_value_key(key=key.column, ctx=ctx),
            expressions=[_literal(v.value) for v in key.values],
        )
        return exp.Not(this=node) if key.negated else node

    if isinstance(key, AggregateKey):
        return _render_aggregate(key, ctx)

    if isinstance(key, TransformKey):
        # POST-phase: materialised by an earlier scope, referenced by alias.
        facilities = _require(ctx=ctx, facility="aliases", key=key)
        slot_id = facilities.slot_id_by_key.get(key)
        alias = (
            facilities.available_alias_by_slot_id.get(slot_id)
            if slot_id is not None
            else None
        )
        if alias is None:
            raise RenderContextMissingFacilityError(
                key_kind=type(key).__name__,
                facility="aliases",
                detail=f"transform {key.op!r} is not materialised as a slot",
            )
        return exp.column(alias, quoted=True)

    raise NotImplementedError(
        f"Unsupported ValueKey type {type(key).__name__}: {key!r}",
    )


_VALUE_KEY_TYPES: Tuple[type, ...] = (
    ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey, LiteralKey, AggregateKey,
    TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey, InKey,
)


def _render_iif_case(*, key: ScalarCallKey, ctx: "RenderContext") -> exp.Case:
    """Render an ``iif`` chain as one multi-WHEN CASE, flattening nested ``iif`` in the otherwise position."""
    def _part(a):
        return (
            render_value_key(key=a, ctx=ctx)
            if isinstance(a, _VALUE_KEY_TYPES) else _literal(a)
        )

    return iif_case_chain(key, _part)


def contains_aggregate(key: ValueKey) -> bool:
    """Whether ``key``'s tree contains an ``AggregateKey`` (decides GROUP BY / HAVING). A structural
    walk, NOT ``phase >= AGGREGATE``: every ``TransformKey`` is POST phase, which would route a transform over a raw column into HAVING."""
    if isinstance(key, AggregateKey):
        return True
    if isinstance(key, ArithmeticKey):
        return any(contains_aggregate(o) for o in key.operands)
    if isinstance(key, ScalarCallKey):
        return any(
            contains_aggregate(a)
            for a in key.args
            if isinstance(a, _VALUE_KEY_TYPES)
        )
    if isinstance(key, TransformKey):
        # partition_keys / time_key are dependencies too: cumsum(x, partition_by=revenue:sum).
        return (
            contains_aggregate(key.input)
            or any(contains_aggregate(p) for p in key.partition_keys)
            or (key.time_key is not None and contains_aggregate(key.time_key))
        )
    if isinstance(key, BetweenKey):
        return any(
            contains_aggregate(k) for k in (key.column, key.low, key.high)
        )
    if isinstance(key, InKey):
        return contains_aggregate(key.column)
    return False
