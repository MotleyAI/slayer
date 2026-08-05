"""The single ``ValueKey`` → sqlglot-AST renderer (P-G).

The generator grew five renderers that drifted apart: the same
``ScalarCallKey`` emitted ``IFNULL(...)`` from a filter (invalid on Postgres)
and ``COALESCE(...)`` from a projection. One function now renders the whole
closed union, parameterised by :class:`RenderContext` rather than by call site.

Two rules keep it from becoming a sixth copy: column-like leaves anchor through
``ScopeFrame.resolve`` (which also registers crossed joins and handles
consumer-scope materialisation), and a missing facility raises
:class:`RenderContextMissingFacilityError` instead of degrading quietly.

Migration status
----------------
The API here is complete and directly tested, but the generator's own render
paths do NOT yet route through :func:`render_value_key`. Only the ScalarCall
POLICY is shared today: all six paths call :func:`render_scalar_call`, so that
construct genuinely renders once. Everything else still runs the generator's
own per-path branches.

Deferred to the scope-assembly PR, together with the cross-scope migration,
because the two are the same piece of work. Finishing it needs:

* **Filter paths** (``_render_value_key_for_filter``, ``:9119`` host WHERE /
  HAVING and ``:7468`` the shifted-CTE WHERE) — ``FilterFacilities`` must carry
  the local-aggregate HAVING branch, which reads ``slot_by_key`` to find a
  materialised slot, the first/last ranked state, and the filter-side CAST
  policy applied per column type. Rendering an aggregate leaf inline (rather
  than by output alias) is what makes HAVING work on backends that reject
  SELECT aliases there, so that branch cannot simply be dropped.
* **Composite paths** (``_render_aggregate_composite_expr``, ``:2851`` the base
  SELECT and ``:3741`` the first/last base SELECT) — ``CompositeFacilities``
  already declares the maps these need (rn-suffix, filtered-rank and
  match-flag, composite alias-by-key, resolved agg kwargs, value alias-by-sql);
  they are threaded through but not yet consumed, because the aggregate leaf
  goes to ``agg_builder``. Wiring ``agg_builder`` to the generator's
  ``_build_agg`` is the intended seam and keeps emission byte-identical.
* **Cross-scope paths** (``_render_filter_value_key_in_target_scope``,
  ``_render_value_key_against_aliases``, ``_render_filter_for_outer_wrapper``)
  — these consume another scope's projected columns, so they are the ones that
  should set ``consumer=`` and thereby give ``ScopeFrame.resolve``'s
  materialisation branch its first production caller. An API nobody calls does
  not establish the projection-boundary principle.

Doing the reroute properly means moving that state onto the context and
re-verifying emission across the dialect matrix; doing it hastily would risk
silent SQL changes across the whole suite for no principle gained beyond what
the shared ScalarCall policy already delivers.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp

from slayer.core.errors import RenderContextMissingFacilityError
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    Phase,
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
from slayer.sql.scope import ScopeFrame

# Arithmetic / comparison / boolean operators, as sqlglot node classes. One
# table instead of the three hand-rolled composers this replaces.
_BINARY_OPS: Dict[str, Any] = {
    "+": exp.Add, "-": exp.Sub, "*": exp.Mul, "/": exp.Div,
    "%": exp.Mod,
    "=": exp.EQ, "==": exp.EQ, "!=": exp.NEQ, "<>": exp.NEQ,
    "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE,
}

_GRANULARITY_TO_SQL: Dict[str, str] = {
    "second": "SECOND", "minute": "MINUTE", "hour": "HOUR", "day": "DAY",
    "week": "WEEK", "month": "MONTH", "quarter": "QUARTER", "year": "YEAR",
}


class FilterFacilities(BaseModel):
    """What WHERE / HAVING rendering needs beyond the scope."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    slot_by_key: Dict[Any, Any] = Field(default_factory=dict)
    aliases_by_slot_id: Dict[str, List[str]] = Field(default_factory=dict)
    first_last_state: Optional[Any] = None


class CompositeFacilities(BaseModel):
    """What AGGREGATE-phase composite rendering needs beyond the scope.

    ``agg_builder`` is the seam to the generator's ``_build_agg``: building a
    real aggregate needs model columns, resolved kwargs and dialect hooks that
    only the generator holds. When it is absent the renderer still handles the
    simple built-ins directly, and raises for the rest rather than guessing.
    """

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
    """What POST-phase rendering needs: the aliases an earlier scope projected."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    slot_id_by_key: Dict[Any, str] = Field(default_factory=dict)
    available_alias_by_slot_id: Dict[str, str] = Field(default_factory=dict)


class RenderContext(BaseModel):
    """Everything a render needs that is not the key itself.

    ``consumer`` is the projection-boundary seam: when set, column-like leaves
    are materialised in ``scope`` and returned to the consumer as bare aliases.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    scope: ScopeFrame
    dialect: SqlDialect
    consumer: Optional[ScopeFrame] = None
    filters: Optional[FilterFacilities] = None
    composites: Optional[CompositeFacilities] = None
    aliases: Optional[AliasFacilities] = None


def _require(ctx: RenderContext, facility: str, key: Any) -> Any:
    got = getattr(ctx, facility, None)
    if got is None:
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__, facility=facility,
        )
    return got


def _literal(value: Any) -> exp.Expression:
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.true() if value else exp.false()
    if isinstance(value, Decimal):
        return exp.Literal.number(str(value))
    if isinstance(value, (int, float)):
        return exp.Literal.number(str(value))
    return exp.Literal.string(str(value))


def render_scalar_call(
    name: str, args: List[exp.Expression], *, dialect: SqlDialect,
) -> exp.Expression:
    """The one ScalarCall policy: typed node, dialect rewrite, log-alias fix-up.

    Every render path calls this, so one call cannot render two ways.

    The log fix-up is load-bearing: ``exp.func("LOG10", x)`` normalises to a
    generic ``Log(10, x)`` re-emitting as ``LOG(10, x)``, wrong on dialects with
    a native single-arg ``LOG10``. Transpiling alone fixes ifnull and breaks
    log10. ``like`` is the allowlist's only operator rather than function.
    """
    if name == "like":
        return exp.Like(this=args[0], expression=args[1])
    node = dialect.rewrite_target_ast(exp.func(name.upper(), *args))
    return _rewrite_log_alias(node, dialect=dialect)


def _rewrite_log_alias(node: exp.Expression, *, dialect: SqlDialect):
    if not isinstance(node, exp.Log):
        return node
    base = node.args.get("this")
    arg = node.args.get("expression")
    if arg is None or not isinstance(base, exp.Literal) or base.is_string:
        return node
    try:
        base_val = float(base.this)
    except (TypeError, ValueError):
        return node
    for candidate in (10, 2):
        if base_val == candidate and dialect.should_use_native_log(candidate):
            return exp.Anonymous(
                this=f"log{candidate}", expressions=[arg.copy()],
            )
    return node


def _render_aggregate(key: AggregateKey, ctx: RenderContext) -> exp.Expression:
    facilities = _require(ctx, "composites", key)
    if facilities.agg_builder is not None:
        return facilities.agg_builder(key)

    # No builder: handle the aggregations that need nothing beyond the source
    # expression, and refuse the rest rather than emitting something plausible.
    if not is_builtin_agg(key.agg):
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility="composites.agg_builder",
            detail=f"custom aggregation {key.agg!r} needs the generator's builder",
        )
    entry = resolve_agg_entry(key.agg)
    if entry.dispatch not in (DISPATCH_SIMPLE, DISPATCH_DISTINCT):
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility="composites.agg_builder",
            detail=(
                f"aggregation {key.agg!r} renders via the {entry.dispatch!r} "
                f"mechanism, which needs the generator's builder"
            ),
        )
    if isinstance(key.source, StarKey):
        inner: exp.Expression = exp.Star()
    else:
        inner = ctx.scope.resolve(key.source, consumer=ctx.consumer)
    assert entry.node_class is not None
    if entry.dispatch == DISPATCH_DISTINCT:
        return entry.node_class(this=exp.Distinct(expressions=[inner]))
    return entry.node_class(this=inner)


def render_value_key(  # NOSONAR(S3776) — sequential dispatch over the closed ValueKey union; each branch IS that type's render contract, and splitting them is exactly the fragmentation this module removes.
    key: ValueKey, ctx: RenderContext,
) -> exp.Expression:
    """Render ``key`` to sqlglot AST in ``ctx``."""
    if isinstance(key, (ColumnKey, ColumnSqlKey)):
        return ctx.scope.resolve(key, consumer=ctx.consumer)

    if isinstance(key, StarKey):
        return exp.Star()

    if isinstance(key, LiteralKey):
        return _literal(key.value)

    if isinstance(key, TimeTruncKey):
        column = ctx.scope.resolve(key.column, consumer=ctx.consumer)
        unit = _GRANULARITY_TO_SQL.get(
            key.granularity.lower(), key.granularity.upper(),
        )
        return ctx.dialect.rewrite_target_ast(
            exp.func("DATE_TRUNC", exp.Literal.string(unit), column),
        )

    if isinstance(key, ArithmeticKey):
        operands = [render_value_key(o, ctx) for o in key.operands]
        op = key.op.lower()
        if op == "and":
            return exp.and_(*operands)
        if op == "or":
            return exp.or_(*operands)
        node_cls = _BINARY_OPS.get(key.op)
        if node_cls is None:
            raise NotImplementedError(
                f"Unsupported arithmetic operator {key.op!r}.",
            )
        result = operands[0]
        for operand in operands[1:]:
            result = node_cls(this=result, expression=operand)
        return result

    if isinstance(key, ScalarCallKey):
        args = [
            render_value_key(a, ctx)
            if isinstance(a, _VALUE_KEY_TYPES)
            else _literal(a)
            for a in key.args
        ]
        return render_scalar_call(key.name, args, dialect=ctx.dialect)

    if isinstance(key, BetweenKey):
        return exp.Between(
            this=render_value_key(key.column, ctx),
            low=render_value_key(key.low, ctx),
            high=render_value_key(key.high, ctx),
        )

    if isinstance(key, InKey):
        node = exp.In(
            this=render_value_key(key.column, ctx),
            expressions=[_literal(v.value) for v in key.values],
        )
        return exp.Not(this=node) if key.negated else node

    if isinstance(key, AggregateKey):
        return _render_aggregate(key, ctx)

    if isinstance(key, TransformKey):
        # POST-phase: the value was materialised by an earlier scope, so it is
        # referenced by alias rather than rebuilt.
        facilities = _require(ctx, "aliases", key)
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


def contains_aggregate(key: ValueKey) -> bool:
    """Whether ``key`` contains an aggregate, structurally.

    Replaces the ``(expr, any_agg)`` tuple the old renderers threaded by hand.
    ``phase`` already propagates as the max over operands and arguments, so
    nested cases (a scalar call over an arithmetic over an aggregate) are
    covered without a second traversal.
    """
    return getattr(key, "phase", Phase.ROW) >= Phase.AGGREGATE
