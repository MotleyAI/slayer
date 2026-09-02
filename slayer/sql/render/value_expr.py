"""The single ``ValueKey`` → sqlglot-AST renderer, parameterised by :class:`RenderContext`
rather than by call site so one key can't render two ways. Column-like leaves anchor through
``ScopeFrame.resolve``; a missing facility raises instead of degrading quietly."""

from __future__ import annotations

from decimal import Decimal
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
    check_scalar_arity,
)
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.render.aggregates import (
    DISPATCH_DISTINCT,
    DISPATCH_SIMPLE,
    is_builtin_agg,
    resolve_agg_entry,
)
from slayer.sql.render.parse import rewrite_log_aliases as rewrite_log_alias
from slayer.sql.scope import ScopeFrame

_BINARY_OPS: Dict[str, Any] = {
    "+": exp.Add, "-": exp.Sub, "*": exp.Mul, "/": exp.Div,
    "%": exp.Mod,
    "=": exp.EQ, "==": exp.EQ, "!=": exp.NEQ, "<>": exp.NEQ,
    "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE,
}


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


def _literal(value: Any) -> exp.Expression:
    """Render a scalar leaf; unsupported types RAISE rather than stringify to a wrong value."""
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.true() if value else exp.false()
    if isinstance(value, Decimal):
        return exp.Literal.number(str(value))
    if isinstance(value, (int, float)):
        return exp.Literal.number(str(value))
    if isinstance(value, str):
        return exp.Literal.string(value)
    raise NotImplementedError(
        f"Unsupported literal in a ValueKey render: "
        f"type={type(value).__name__} value={value!r}",
    )


# sqlglot does NOT parenthesise by node nesting (``Mul(Add(a,b),c)`` emits ``a + b * c``), so
# precedence needs explicit ``Paren`` nodes — spanning boolean/comparison ops, since a comparison in arithmetic still parses.
_PRECEDENCE: Dict[Any, int] = {
    exp.Or: 1,
    exp.And: 2,
    exp.Not: 3,
    exp.EQ: 4, exp.NEQ: 4, exp.LT: 4, exp.LTE: 4, exp.GT: 4, exp.GTE: 4,
    exp.Is: 4, exp.In: 4, exp.Like: 4, exp.Between: 4,
    exp.Add: 5, exp.Sub: 5,
    exp.Mul: 6, exp.Div: 6, exp.Mod: 6,
    exp.Neg: 7,
}

_IS = "is"
_IS_NOT = "is not"

# Exactly-two-operand ops: left-folding ``a < b < c`` to ``(a < b) < c`` compares a
# boolean to a number, and reading only the first two would silently drop the rest.
_STRICTLY_BINARY = frozenset({
    "=", "==", "!=", "<>", "<", "<=", ">", ">=", _IS, _IS_NOT,
})

# Comparisons are NON-ASSOCIATIVE, so an equal-precedence child needs parens on EITHER side:
# ``(a = b) is null`` → ``a = (b IS NULL)``, and ``(a < b) = c`` → ``a < b = c`` Postgres rejects.
_COMPARISON_PREC = 4


def _paren_if_lower_prec(
    child: exp.Expression, *, parent_prec: int, is_right: bool,
) -> exp.Expression:
    """Parenthesise ``child`` when dropping its parens would change meaning: lower precedence than
    the parent, or an equal-precedence RIGHT child (a LEFT one too at :data:`_COMPARISON_PREC`).
    Even ``+`` / ``*`` aren't associative over floats/decimals, so the tree is preserved."""
    if isinstance(child, exp.Mod):
        # ``%`` parenthesised unconditionally: sqlglot's parser tiers it with ``+``/``-``,
        # and generated SQL is re-parsed by sqlglot, so ``a + b % c`` would regroup in flight.
        return exp.Paren(this=child)
    child_prec = _PRECEDENCE.get(type(child))
    if child_prec is None:
        return child
    if child_prec < parent_prec:
        return exp.Paren(this=child)
    if child_prec == parent_prec and (is_right or parent_prec == _COMPARISON_PREC):
        return exp.Paren(this=child)
    return child


def group_unary_operand(operand: exp.Expression, *, op: str) -> exp.Expression:
    """Parenthesise a unary operator's operand when precedence requires it (``-(a + b)`` unwrapped emits ``-a + b``)."""
    if op not in ("not", "-"):
        raise NotImplementedError(
            f"group_unary_operand only covers 'not' and '-', got {op!r}.",
        )
    parent = exp.Not if op == "not" else exp.Neg
    return _paren_if_lower_prec(
        operand, parent_prec=_PRECEDENCE[parent], is_right=False,
    )


def group_is_operands(
    *, lhs: exp.Expression, rhs: exp.Expression,
) -> Tuple[exp.Expression, exp.Expression]:
    """Parenthesise ``IS`` / ``IS NOT`` operands: ``IS`` binds tighter than ``=``, so ungrouped ``(a = 5) is null`` reads as ``a = (5 IS NULL)``."""
    is_prec = _PRECEDENCE[exp.Is]
    return (
        _paren_if_lower_prec(lhs, parent_prec=is_prec, is_right=False),
        _paren_if_lower_prec(rhs, parent_prec=is_prec, is_right=True),
    )


def _render_unary(*, op: str, operand: exp.Expression) -> exp.Expression:
    """Single-operand forms. ``-x`` is a single-operand ``ArithmeticKey``, so dropping the op would turn ``amount > -10`` into ``amount > 10``; the operand gets binary precedence grouping."""
    if op in ("not", "-"):
        grouped = group_unary_operand(operand, op=op)
        return exp.Not(this=grouped) if op == "not" else exp.Neg(this=grouped)
    if op == "+":
        return operand
    raise NotImplementedError(f"Unsupported unary operator {op!r}.")


def _fold_binary(
    *, node_cls: Any, operands: List[exp.Expression],
) -> exp.Expression:
    """Left-fold ``operands``, grouping each side by precedence as it goes."""
    parent_prec = _PRECEDENCE.get(node_cls)
    result = operands[0]
    for operand in operands[1:]:
        lhs, rhs = result, operand
        if parent_prec is not None:
            lhs = _paren_if_lower_prec(
                lhs, parent_prec=parent_prec, is_right=False,
            )
            rhs = _paren_if_lower_prec(
                rhs, parent_prec=parent_prec, is_right=True,
            )
        result = node_cls(this=lhs, expression=rhs)
    return result


def render_arithmetic(
    *, op: str, operands: List[exp.Expression],
) -> exp.Expression:
    """Compose an arithmetic / comparison / boolean operator — the single composer."""
    if not operands:
        raise NotImplementedError(f"Operator {op!r} needs at least one operand.")

    if op in ("and", "or") and len(operands) == 1:
        return operands[0]

    if len(operands) == 1:
        return _render_unary(op=op, operand=operands[0])

    if op == "and":
        return exp.and_(*operands)
    if op == "or":
        return exp.or_(*operands)

    if op in _STRICTLY_BINARY and len(operands) != 2:
        # Refuse rather than fold: a chained comparison is silently wrong either way.
        raise NotImplementedError(
            f"Operator {op!r} takes exactly two operands, got {len(operands)}.",
        )

    if op in (_IS, _IS_NOT):
        lhs, rhs = group_is_operands(lhs=operands[0], rhs=operands[1])
        node = exp.Is(this=lhs, expression=rhs)
        return exp.Not(this=node) if op == _IS_NOT else node

    node_cls = _BINARY_OPS.get(op)
    if node_cls is None:
        raise NotImplementedError(f"Unsupported arithmetic operator {op!r}.")
    return _fold_binary(node_cls=node_cls, operands=operands)


def render_scalar_call(
    *, name: str, args: List[exp.Expression], dialect: SqlDialect,
) -> exp.Expression:
    """The one ScalarCall policy. The log fix-up is load-bearing: ``exp.func("LOG10", x)`` normalises to ``LOG(10, x)``, wrong where ``LOG10`` is native single-arg."""
    arity_error = check_scalar_arity(name=name, argc=len(args))
    if arity_error is not None:
        # Checked before building: sqlglot is inconsistent (3-arg ROUND drops the third, etc.).
        raise NotImplementedError(arity_error)
    if name == "like":
        return exp.Like(this=args[0], expression=args[1])
    if name == "mod":
        # ``%`` is an operator, not a Func; the ``%`` composer parenthesises both sides correctly.
        return render_arithmetic(op="%", operands=args)
    node = dialect.rewrite_target_ast(exp.func(name.upper(), *args))
    return rewrite_log_alias(node, dialect=dialect)


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

    ifs: List[exp.If] = []
    node = key
    while isinstance(node, ScalarCallKey) and node.name == "iif":
        # Fail-closed arity backstop so a malformed key can't surface an opaque IndexError.
        arity_error = check_scalar_arity(name="iif", argc=len(node.args))
        if arity_error is not None:
            raise NotImplementedError(arity_error)
        ifs.append(exp.If(this=_part(node.args[0]), true=_part(node.args[1])))
        node = node.args[2]
    return exp.Case(ifs=ifs, default=_part(node))


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
