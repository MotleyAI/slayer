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
All five live render families route through :func:`render_value_key` (DEV-1763,
byte-identical SQL). The generator's legacy per-path renderers have been
deleted (DEV-1749). The seams the migration added:

* **Filter** (``_build_where_having_from_planned`` host WHERE / HAVING and
  ``_shifted_where_part`` the shifted-CTE WHERE) — a render-scoped host
  ``ScopeFrame`` plus :class:`FilterFacilities` carrying: the HAVING seam
  (``agg_builder`` renders the aggregate as its EXPRESSION after the renderer's
  ``slot_by_key`` lookup + ``having_full_alias`` recovery), the filter-side CAST
  policy (``cast_column_sql``), and the DEV-1539 comparison grouping
  (``paren_comparison_operands``).
* **Composite** (``_build_base_select_for_planned`` base SELECT) — the composite
  STRUCTURE renders here; only the aggregate LEAF goes to
  :class:`CompositeFacilities`'s ``agg_builder`` → the generator's ``_build_agg``
  (byte-identical). First/last aggregates render as a planned
  ``RankedAggregatePlan`` CTE, not at render time.
* **Alias-environment / Outer-wrapper** (POST-phase filters, window-transform
  inputs, consecutive-periods predicates, the cross-model transform chain, and
  the DEV-1503 outer combined WHERE) — carry :class:`AliasFacilities` and
  resolve the five slotted kinds ALIAS-EXCLUSIVELY, table-qualified via
  ``table_by_slot_id`` for the outer wrapper.
* **Target-scope** (``_collect_routed_filters`` cross-model CTE WHERE / HAVING) —
  a target-rooted ``ScopeFrame`` with a routed-key reroot to the CTE's local
  scope.

Two shared leaf policies stay: ScalarCall (:func:`render_scalar_call`, all
paths) and arithmetic / comparison / boolean composition
(:func:`render_arithmetic`, now the single arithmetic renderer).
"""

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
from slayer.sql.scope import ScopeFrame

# Arithmetic / comparison / boolean operators, as sqlglot node classes. One
# table instead of the three hand-rolled composers this replaces.
_BINARY_OPS: Dict[str, Any] = {
    "+": exp.Add, "-": exp.Sub, "*": exp.Mul, "/": exp.Div,
    "%": exp.Mod,
    "=": exp.EQ, "==": exp.EQ, "!=": exp.NEQ, "<>": exp.NEQ,
    "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE,
}


# Named once: the render context field that carries the generator's aggregate
# builder, cited by every fail-closed guard below.
_AGG_BUILDER = "composites.agg_builder"

# The HAVING placeholder full-alias (DEV-1501): the filter family looks the
# aggregate up in ``slot_by_key`` and recovers the base SELECT's projected
# alias; when the slot was not materialised there, this placeholder stands in
# and the ``filtered_rn_map`` lookup misses back to the unfiltered ranked path.
_HAVING_PLACEHOLDER = "__having_ref__"


def _wrap_cast_for_type(
    expr: exp.Expression, dt: Optional[DataType],
) -> exp.Expression:
    """Wrap ``expr`` in ``CAST(expr AS <dt>)`` so a declared SLayer ``DataType``
    is enforced in emitted SQL (DEV-1361).

    Skipped when ``dt`` is ``None`` / ``TEXT`` / opaque (no such SQL type), or
    when ``expr`` is a bare ``exp.Column`` (its runtime type already matches, and
    on SQLite ``CAST(text_timestamp AS TIMESTAMP)`` can truncate to a year).
    Idempotent against an existing CAST to the same target. The single home for
    the policy — the generator re-exports it (DEV-1763 P-G).
    """
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
    """The CAST target for a derived column inside a WHERE / HAVING predicate
    (DEV-1450 #4a).

    Temporal types (``DATE`` / ``TIMESTAMP``) are suppressed: in a filter the
    derived expression is COMPARED, not type-enforced, and
    ``CAST(text AS TIMESTAMP)`` on SQLite gives it NUMERIC affinity — truncating
    a string timestamp to its leading year and breaking ``BETWEEN``. A base
    temporal column in the same position is never cast (it renders as a bare
    ``exp.Column``), so this keeps the derived form on par. Non-temporal types
    pass through so a derived numeric / boolean column keeps its enforcing CAST.
    """
    if dt in (DataType.DATE, DataType.TIMESTAMP):
        return None
    return dt


class FilterFacilities(BaseModel):
    """What WHERE / HAVING rendering needs beyond the scope.

    ``agg_builder`` is the HAVING seam to the generator's ``_build_agg``: the
    renderer does the ``slot_by_key`` lookup and the ``having_full_alias``
    recovery itself, then hands ``(key, slot, having_full_alias)`` to the
    builder, which resolves agg kwargs and emits the aggregate as its
    *expression* (``SUM(x)``, not its SELECT alias — Postgres rejects aliases in
    HAVING). ``cast_column_sql`` applies the filter-side CAST policy to derived
    ``ColumnSqlKey`` leaves (the filter family casts; target-scope does not).
    ``paren_comparison_operands`` reproduces the legacy ``_paren_if_binary``
    grouping (every multi-term comparison operand parenthesised — strictly more
    grouping than the shared composer derives, never less; DEV-1539).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    slot_by_key: Dict[Any, Any] = Field(default_factory=dict)
    aliases_by_slot_id: Dict[str, List[str]] = Field(default_factory=dict)
    agg_builder: Optional[
        Callable[[AggregateKey, Optional[Any], str], exp.Expression]
    ] = None
    cast_column_sql: bool = False
    paren_comparison_operands: bool = True


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
    """What POST-phase rendering needs: the aliases an earlier scope projected.

    Presence of this facility switches the five slotted kinds (column, derived,
    time-trunc, aggregate, transform) to ALIAS-EXCLUSIVE resolution — they come
    back as the materialised alias, never rebuilt from source (rebuilding in a
    CTE that only projects aliases is wrong SQL). ``table_by_slot_id`` carries
    the qualifier (``_base``, a ``_cm_*`` CTE) for families whose alias is
    table-qualified; absent means a bare quoted alias. A slot that is not in the
    maps is a promotion bug and RAISES rather than falling back to the scope.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    slot_id_by_key: Dict[Any, str] = Field(default_factory=dict)
    available_alias_by_slot_id: Dict[str, str] = Field(default_factory=dict)
    table_by_slot_id: Dict[str, str] = Field(default_factory=dict)


class RenderContext(BaseModel):
    """Everything a render needs that is not the key itself.

    ``consumer`` is the projection-boundary seam: when set, column-like leaves
    are materialised in ``scope`` and returned to the consumer as bare aliases.

    ``scope`` is Optional: the alias-environment and outer-wrapper families
    resolve every leaf through the alias maps and carry no scope. A key kind
    that needs to anchor a reference (column, derived, time-trunc, or a
    no-builder aggregate) fails closed with
    :class:`RenderContextMissingFacilityError` when ``scope`` is absent, rather
    than raising an ``AttributeError`` deep in a branch.
    """

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
    """The scope-carrying door: a key that anchors a reference needs a scope.

    Fails closed rather than dereferencing ``None`` — the alias-environment and
    outer-wrapper families legitimately carry ``scope=None`` and reach every
    leaf through the alias maps instead.
    """
    if ctx.scope is None:
        raise RenderContextMissingFacilityError(
            key_kind=type(key).__name__,
            facility="scope",
            detail="a scope is required to anchor this reference",
        )
    return ctx.scope


# The comparison operators whose multi-term operands the filter families
# parenthesise unconditionally (DEV-1539). No letters, so ``.lower()`` is
# identity — matched against the already-lowered op.
_COMPARISON_OPS = frozenset({"==", "=", "!=", "<>", "<", "<=", ">", ">="})


def _paren_if_binary(node: exp.Expression) -> exp.Expression:
    """Wrap a multi-term operand in ``(...)`` when it is an ``exp.Binary``
    (arithmetic ``a + b`` or an ``AND`` / ``OR`` connector). Bare columns,
    literals, function calls, and already-enclosed ``CAST(...)`` / ``Paren``
    are not ``Binary`` and pass through. The pre-wrap makes the operand
    self-delimiting, so the shared composer's own precedence grouping is a
    no-op over it."""
    return exp.Paren(this=node) if isinstance(node, exp.Binary) else node


def _render_via_alias(key: ValueKey, ctx: RenderContext) -> exp.Expression:
    """Alias-exclusive resolution (POST-phase / outer-wrapper): return the
    materialised alias for ``key``, table-qualified when the facility carries a
    qualifier. A slot that is not materialised RAISES — rebuilding it from
    source in a scope that only projects aliases would be wrong SQL."""
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
    # A non-None ``alias`` guarantees ``slot_id`` was found (it is the lookup
    # key that produced ``alias``), so the qualifier lookup needs no re-guard.
    col_ident = exp.to_identifier(alias, quoted=True)
    table = facilities.table_by_slot_id.get(slot_id)
    if table:
        return exp.Column(this=col_ident, table=exp.to_identifier(table))
    return exp.Column(this=col_ident)


# The five slotted kinds that alias-exclusive mode resolves from the alias maps
# rather than from source (column, derived, time-trunc, aggregate, transform).
_ALIAS_SLOTTED_KINDS: Tuple[type, ...] = (
    ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey, TransformKey,
)


def _literal(value: Any) -> exp.Expression:
    """Render a scalar leaf.

    Unsupported types RAISE rather than being stringified: a ``datetime`` or a
    ``list`` silently becoming a quoted string is a wrong value, not an error,
    and the generator's equivalent already raises.
    """
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


# sqlglot does NOT parenthesise by node nesting. ``Mul(Add(a, b), c)`` emits
# ``a + b * c`` and ``Add(GT(a, b), 1)`` emits ``a > b + 1`` — both parse back
# with different meaning. Precedence must be materialised as explicit ``Paren``
# nodes, and the table has to span BOOLEAN and COMPARISON operators too, not
# just arithmetic: a comparison nested inside arithmetic is the case that bites
# hardest, because the result still parses and still returns rows.
_PRECEDENCE: Dict[Any, int] = {
    exp.Or: 1,
    exp.And: 2,
    exp.Not: 3,
    exp.EQ: 4, exp.NEQ: 4, exp.LT: 4, exp.LTE: 4, exp.GT: 4, exp.GTE: 4,
    exp.Is: 4, exp.In: 4, exp.Like: 4, exp.Between: 4,
    exp.Add: 5, exp.Sub: 5,
    exp.Mul: 6, exp.Div: 6, exp.Mod: 6,
    # Unary minus binds tighter than any binary arithmetic.
    exp.Neg: 7,
}

_IS = "is"
_IS_NOT = "is not"

# Operators taking exactly two operands. Left-folding a comparison would turn
# ``a < b < c`` into ``(a < b) < c`` — a boolean compared to a number — and
# reading only the first two would silently DROP the rest.
_STRICTLY_BINARY = frozenset({
    "=", "==", "!=", "<>", "<", "<=", ">", ">=", _IS, _IS_NOT,
})

# The comparison family's shared level. Unlike arithmetic, comparisons are
# NON-ASSOCIATIVE in SQL, so an equal-precedence child needs parens on EITHER
# side — a left child included. Without that, ``(a = b) is null`` emits
# ``a = b IS NULL``, which every dialect reads as ``a = (b IS NULL)`` because
# ``IS`` binds tighter than ``=``; and ``(a < b) = c`` emits ``a < b = c``,
# which Postgres rejects outright as a non-associative chain.
_COMPARISON_PREC = 4


def _paren_if_lower_prec(
    child: exp.Expression, *, parent_prec: int, is_right: bool,
) -> exp.Expression:
    """Parenthesise ``child`` when dropping its parens would change meaning.

    Lower precedence than the parent always needs parens. So does EVERY
    equal-precedence right child: checking the parent operator is not enough
    (``Mul(a, Mod(b, c))`` emits ``a * b % c``, regrouping to ``(a * b) % c``),
    and even ``+`` and ``*`` are not operationally associative over floats or
    fixed-precision decimals, where rounding and overflow make ``a + (b + c)``
    and ``(a + b) + c`` genuinely different. Preserving the tree the binder
    built costs a pair of parentheses; regrouping costs accuracy.

    At :data:`_COMPARISON_PREC` an equal-precedence LEFT child is parenthesised
    too, because that family is non-associative.

    A node with no precedence entry — a column, a literal, a function call —
    is already self-delimiting.
    """
    if isinstance(child, exp.Mod):
        # ``%`` is parenthesised unconditionally, because precedence alone is
        # not enough to survive our own pipeline. SQL puts ``%`` on the
        # ``*`` / ``/`` tier, and so does the Mode-B parser — but SQLGLOT's
        # parser puts it on the ``+`` / ``-`` tier, so it reads back
        # ``a + b % c`` as ``(a + b) % c``. Generated SQL IS re-parsed by
        # sqlglot downstream (reserved-word pre-quoting, the log-alias
        # transform), so an unparenthesised ``%`` would be silently regrouped
        # in flight rather than by any database.
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
    """Parenthesise a unary operator's operand when precedence requires it.

    Public because the generator's three arithmetic composers built ``exp.Not``
    / ``exp.Neg`` around a bare operand: ``-(a + b)`` emitted ``-a + b`` and
    ``not (a and b)`` emitted ``NOT a AND b``. Both parse cleanly and mean
    something else, so the policy is shared rather than re-derived (P-G).
    """
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
    """Parenthesise an ``IS`` / ``IS NOT`` operand when precedence requires it.

    ``IS`` binds tighter than ``=``, so an ungrouped ``(a = 5) is null`` emits
    ``a = 5 IS NULL`` and every dialect reads it as ``a = (5 IS NULL)`` — a
    different predicate that still returns rows. Shared with the generator's
    composers for the same reason as :func:`group_unary_operand`.
    """
    is_prec = _PRECEDENCE[exp.Is]
    return (
        _paren_if_lower_prec(lhs, parent_prec=is_prec, is_right=False),
        _paren_if_lower_prec(rhs, parent_prec=is_prec, is_right=True),
    )


def _render_unary(*, op: str, operand: exp.Expression) -> exp.Expression:
    """The single-operand forms.

    The binder represents ``-x`` as a SINGLE-operand ``ArithmeticKey``, so a
    fold that just returns the operand would turn ``amount > -10`` into
    ``amount > 10``. The operand needs the same precedence treatment as a
    binary one: without it ``-(a + b)`` emits ``-a + b`` and ``not (a and b)``
    emits ``NOT a AND b``.
    """
    if op in ("not", "-"):
        grouped = group_unary_operand(operand, op=op)
        return exp.Not(this=grouped) if op == "not" else exp.Neg(this=grouped)
    if op == "+":
        # Unary plus is a no-op; SQL never needs it spelled out.
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
    op: str, operands: List[exp.Expression],
) -> exp.Expression:
    """Compose an arithmetic / comparison / boolean operator.

    The single composer: the generator's three call sites delegate here, so
    one ``ArithmeticKey`` groups the same way wherever it is rendered (P-G).
    Their hand-rolled versions each knew a different subset of the precedence
    table and emitted predicates that parse cleanly and mean something else.
    """
    if not operands:
        raise NotImplementedError(f"Operator {op!r} needs at least one operand.")

    if op in ("and", "or") and len(operands) == 1:
        # Degenerate but well-defined: the conjunction of one term IS that term.
        return operands[0]

    if len(operands) == 1:
        return _render_unary(op=op, operand=operands[0])

    if op == "and":
        return exp.and_(*operands)
    if op == "or":
        return exp.or_(*operands)

    if op in _STRICTLY_BINARY and len(operands) != 2:
        # Refuse rather than fold. Left-folding a chained comparison compares a
        # BOOLEAN against the next operand, and reading only the first two
        # drops the rest — both silently wrong. The Mode-B parser already
        # rejects chained comparisons; this is the structural backstop.
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
    """The one ScalarCall policy: typed node, dialect rewrite, log-alias fix-up.

    Every render path calls this, so one call cannot render two ways.

    The log fix-up is load-bearing: ``exp.func("LOG10", x)`` normalises to a
    generic ``Log(10, x)`` re-emitting as ``LOG(10, x)``, wrong on dialects with
    a native single-arg ``LOG10``. Transpiling alone fixes ifnull and breaks
    log10. ``like`` is the allowlist's only operator rather than function.
    """
    arity_error = check_scalar_arity(name=name, argc=len(args))
    if arity_error is not None:
        # Checked before building, because sqlglot is inconsistent: a 3-arg
        # ROUND silently DROPS the third, a 2-arg LENGTH emits SQL the backend
        # rejects, and a 2-arg LOWER raises a raw sqlglot error.
        raise NotImplementedError(arity_error)
    if name == "like":
        return exp.Like(this=args[0], expression=args[1])
    node = dialect.rewrite_target_ast(exp.func(name.upper(), *args))
    return rewrite_log_alias(node, dialect=dialect)


def rewrite_log_alias(
    node: exp.Expression, *, dialect: SqlDialect,
) -> exp.Expression:
    """The single log-alias policy: a 2-arg ``LOG(10|2, x)`` becomes the
    dialect's native single-arg ``log10`` / ``log2`` where one exists.

    Shared with the generator, which applies it over parsed trees. Two copies
    of this rule would reintroduce exactly the drift this module removes.
    """
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


def _render_filter_aggregate(
    key: AggregateKey, ctx: RenderContext,
) -> exp.Expression:
    """The HAVING seam: look the aggregate up in ``slot_by_key``, recover the
    base SELECT's projected full alias (the placeholder when it was not
    materialised there), and hand ``(key, slot, having_full_alias)`` to the
    generator's ``_build_agg``. Rendering the aggregate as its expression rather
    than by output alias is what makes HAVING work on backends that reject
    SELECT aliases there."""
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
    """Dispatch an ``AggregateKey`` by builder precedence: the FILTER HAVING
    seam, then the COMPOSITE builder, then the built-in fallback."""
    # A filter context sets ``filters.agg_builder`` and no composites facility.
    if ctx.filters is not None and ctx.filters.agg_builder is not None:
        return _render_filter_aggregate(key, ctx)

    facilities = _require(ctx=ctx, facility="composites", key=key)
    if facilities.agg_builder is not None:
        return facilities.agg_builder(key)

    return _render_builtin_aggregate(key, ctx)


def _render_builtin_aggregate(  # NOSONAR(S3776) — sequential fail-closed guards over the built-in agg contract (mechanism gate, then the FIELD guards that keep a filtered / parametric / cross-star aggregate from silently rendering as a plain SUM). Each guard IS a distinct wrong-number-vs-error boundary; merging them hides which one fired.
    key: AggregateKey, ctx: RenderContext,
) -> exp.Expression:
    """Render an aggregate that needs no generator builder — the simple /
    distinct built-ins over a source expression. Everything the built-in path
    cannot faithfully emit (custom mechanisms, filtered / parametric sources,
    cross-model stars) refuses rather than emitting something plausible."""
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
    # The dispatch gate above refuses aggregations whose MECHANISM needs the
    # generator. These two refuse a key whose FIELDS do: without them a
    # filtered aggregate would render as a plain SUM, silently covering rows
    # the filter excludes — a wrong number rather than an error.
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
            # ``customers.*:count`` counts rows of the JOINED relation, which
            # needs the join graph. A bare ``*`` here would count host rows —
            # a wrong number, same class as the two guards above.
            raise RenderContextMissingFacilityError(
                key_kind=type(key).__name__,
                facility=_AGG_BUILDER,
                detail=(
                    f"cross-model star over path {key.source.path!r} needs the "
                    f"generator's join-graph routing"
                ),
            )
        if key.agg != "count":
            # ``COUNT`` is the only aggregation a bare star is defined for.
            # Without this the dispatch gate happily builds ``SUM(*)`` or
            # ``COUNT(DISTINCT *)`` — SQL no backend accepts, discovered at
            # execution time rather than here.
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
    key: ValueKey, ctx: RenderContext,
) -> exp.Expression:
    """Render ``key`` to sqlglot AST in ``ctx``."""
    # ALIAS-EXCLUSIVE mode: when a scope projected these earlier, the five
    # slotted kinds resolve to their materialised alias, never rebuilt from
    # source (POST-phase / outer-wrapper). Intercepted before every scope-based
    # branch so a miss RAISES rather than falling back to the scope.
    if ctx.aliases is not None and isinstance(key, _ALIAS_SLOTTED_KINDS):
        return _render_via_alias(key, ctx)

    if isinstance(key, ColumnKey):
        return _require_scope(ctx, key).resolve(key, consumer=ctx.consumer)

    if isinstance(key, ColumnSqlKey):
        scope = _require_scope(ctx, key)
        resolved = scope.resolve(key, consumer=ctx.consumer)
        # Filter-side CAST policy for a DERIVED leaf: the filter family enforces
        # the declared type (temporal suppressed); target-scope never casts. A
        # base ``ColumnKey`` above is already a bare column and never cast.
        if ctx.filters is not None and ctx.filters.cast_column_sql:
            col_type = scope.column_type(key)
            resolved = _wrap_cast_for_type(resolved, _filter_cast_type(col_type))
        return resolved

    if isinstance(key, StarKey):
        # Same rule as the aggregate branch: a pathed star names the JOINED
        # relation's rows, which needs the join graph. Emitting a bare ``*``
        # would silently count the host's.
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
        # Delegate to the dialect strategy, which owns the per-backend wire
        # form: STRFTIME on SQLite, DATETRUNC on T-SQL, native WEEK(SUNDAY) on
        # BigQuery, plus the WEEK_SUNDAY day-shift. Emitting a literal
        # DATE_TRUNC here would name a function SQLite does not have.
        return ctx.dialect.build_date_trunc(
            col_expr=column,
            granularity=TimeGranularity(key.granularity),
            parse=lambda sql: sqlglot.parse_one(
                sql, dialect=ctx.dialect.sqlglot_name,
            ),
        )

    if isinstance(key, ArithmeticKey):
        op = key.op.lower()
        operands = [render_value_key(o, ctx) for o in key.operands]
        # Filter families keep the DEV-1539 extra grouping: every multi-term
        # COMPARISON operand is parenthesised (strictly more than the composer
        # derives). Pre-wrapping makes each operand self-delimiting.
        if (
            ctx.filters is not None
            and ctx.filters.paren_comparison_operands
            and op in _COMPARISON_OPS
        ):
            operands = [_paren_if_binary(o) for o in operands]
        return render_arithmetic(op, operands)

    if isinstance(key, ScalarCallKey):
        args = [
            render_value_key(a, ctx)
            if isinstance(a, _VALUE_KEY_TYPES)
            else _literal(a)
            for a in key.args
        ]
        return render_scalar_call(
            name=key.name, args=args, dialect=ctx.dialect,
        )

    if isinstance(key, BetweenKey):
        return exp.Between(
            this=render_value_key(key.column, ctx),
            low=render_value_key(key.low, ctx),
            high=render_value_key(key.high, ctx),
        )

    if isinstance(key, InKey):
        # Backstop for the bind-time rule: SQL's three-valued logic makes a
        # NULL member a trap, and ``NOT IN`` with one matches no rows at all.
        if any(v.value is None for v in key.values):
            raise NotImplementedError(
                "NULL is not allowed inside an IN list: 'NOT IN' with a NULL "
                "matches no rows. Test for null separately with IS NULL / "
                "IS NOT NULL.",
            )
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


def contains_aggregate(key: ValueKey) -> bool:
    """Whether ``key``'s tree CONTAINS an ``AggregateKey``.

    Replaces the ``(expr, any_agg)`` tuple the old renderers threaded by hand,
    which decides GROUP BY / HAVING placement.

    A structural walk, deliberately NOT ``phase >= AGGREGATE``: phase answers
    "when is this evaluated", a different question. Every ``TransformKey`` is
    POST phase whether or not it wraps an aggregate, so the phase test reports
    True for a transform over a raw column and would route a non-aggregate
    predicate into HAVING.
    """
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
        # partition_keys and time_key are expression dependencies just as
        # input is: cumsum(x, partition_by=revenue:sum) references an
        # aggregate even though its input does not.
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
        # ``values`` are LiteralKeys by type, so only the column can carry one.
        return contains_aggregate(key.column)
    return False
