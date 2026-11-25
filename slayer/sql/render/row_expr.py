"""Scope-free row-level expression composers.

The pure ``ValueKey`` → sqlglot composers that need NO ``RenderContext`` /
``ScopeFrame``: literals, arithmetic/comparison/boolean operators, scalar
calls, and the ``iif`` → ``CASE`` chain. Extracted from ``value_expr.py`` so
``ScopeFrame`` (which ``value_expr`` imports) can also render an aggregate's
expression source without an import cycle; ``value_expr`` re-imports them.

:func:`render_row_expression` renders an aggregation-free row-level tree
(an ``AggregateKey.source`` expression), resolving field-like leaves through
a caller-supplied ``resolve_field`` callback — the one seam that differs
between the ScopeFrame and the generator's spec-builder call sites.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, DatePart, TimeGranularity
from slayer.core.time_points import date_bound, parse_temporal_value
from slayer.core.keys import (
    CLOCK_FUNCTIONS,
    DATE_ADD_COUNT_ARG,
    DATE_OPERAND_ARGS,
    NULL_SAFE_COMPARISON_OPS,
    LIST_ONLY_FUNCTIONS,
    ArithmeticKey,
    ArrayLiteralKey,
    CastKey,
    FieldElementFn,
    FieldKey,
    FieldSqlKey,
    FieldTypeFn,
    InKey,
    ListSetKey,
    LiteralKey,
    ScalarCallKey,
    TimeTruncKey,
    _FrozenKey,
    boolean_valued,
    check_scalar_arity,
    date_promotion_positions,
    integer_valued,
    is_list_call,
    list_element_type,
    mixed_nullif,
    numbers_read_list,
    numeric_boolean_positions,
    temporal_pair,
    temporal_type,
    unit_word,
    value_type,
    reads_no_rows,
)
from slayer.sql.dialects.base import (
    CONSTANT_INPUT,
    SqlDialect,
    TemporalComparisonOp,
    bare_comparands,
    case,
    cast_target,
    decimal_parts,
    either,
    func,
    is_list_fn,
    lit,
    operand_copy,
    require_library,
    temporal_type_of,
    text_length,
)
from slayer.sql.render.library import json_operands, render_library_call
from slayer.sql.render.parse import rewrite_log_aliases as rewrite_log_alias

_BINARY_OPS: Dict[str, Any] = {
    "+": exp.Add, "-": exp.Sub, "*": exp.Mul, "/": exp.Div,
    "%": exp.Mod,
    "=": exp.EQ, "==": exp.EQ, "!=": exp.NEQ, "<>": exp.NEQ,
    "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE,
}


def _literal(value: Any, *, dialect: Optional[SqlDialect] = None) -> Expression:
    """Render a scalar leaf; unsupported types RAISE rather than stringify to a wrong value."""
    if isinstance(value, date) and dialect is not None:
        return dialect.build_temporal_literal(value=value, dt=temporal_type_of(value))
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
    exp.Is: 4, exp.In: 4, exp.Like: 4, exp.ILike: 4, exp.Between: 4,
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
    child: Expression, *, parent_prec: int, is_right: bool,
) -> Expression:
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


def group_unary_operand(operand: Expression, *, op: str) -> Expression:
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
    *, lhs: Expression, rhs: Expression,
) -> Tuple[Expression, Expression]:
    """Parenthesise ``IS`` / ``IS NOT`` operands: ``IS`` binds tighter than ``=``, so ungrouped ``(a = 5) is null`` reads as ``a = (5 IS NULL)``."""
    is_prec = _PRECEDENCE[exp.Is]
    return (
        _paren_if_lower_prec(lhs, parent_prec=is_prec, is_right=False),
        _paren_if_lower_prec(rhs, parent_prec=is_prec, is_right=True),
    )


def _render_unary(*, op: str, operand: Expression) -> Expression:
    """Single-operand forms. ``-x`` is a single-operand ``ArithmeticKey``, so dropping the op would turn ``amount > -10`` into ``amount > 10``; the operand gets binary precedence grouping."""
    if op in ("not", "-"):
        grouped = group_unary_operand(operand, op=op)
        return exp.Not(this=grouped) if op == "not" else exp.Neg(this=grouped)
    if op == "+":
        return operand
    raise NotImplementedError(f"Unsupported unary operator {op!r}.")


def _nonzero_number(node: Expression) -> bool:
    try:
        return isinstance(node, exp.Literal) and node.is_number and float(node.this) != 0
    except ValueError:
        return False


def _fold_binary(
    *, node_cls: Any, operands: List[Expression],
) -> Expression:
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
        if node_cls is exp.Div and not _nonzero_number(rhs):
            rhs = exp.Nullif(this=rhs, expression=exp.Literal.number(0))  # division by zero is NULL
        result = node_cls(this=lhs, expression=rhs)
    return result


def render_arithmetic(
    *, op: str, operands: List[Expression],
) -> Expression:
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
    if op in _TEMPORAL_COMPARISON_OPS:
        operands = list(bare_comparands(operands[0], operands[1]))
    return _fold_binary(node_cls=node_cls, operands=operands)


_TEMPORAL_COMPARISON_OPS: Dict[str, TemporalComparisonOp] = {
    "=": "=", "==": "=", "!=": "!=", "<>": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">=",
}
_MIRRORED: Dict[TemporalComparisonOp, TemporalComparisonOp] = {
    "=": "=", "!=": "!=", "<": ">", "<=": ">=", ">": "<", ">=": "<=",
}


def _temporal_value(key: Any) -> Optional[date]:
    return key.value if isinstance(key, LiteralKey) and isinstance(key.value, date) else None


def temporal_comparison(key: ArithmeticKey) -> Optional[Tuple[TemporalComparisonOp, Any, date]]:
    """``(operand-left op, operand, value)`` when ``key`` compares an expression with a date/datetime literal."""
    op = _TEMPORAL_COMPARISON_OPS.get(key.op.lower())
    if op is None or len(key.operands) != 2:
        return None
    left, right = key.operands
    left_value, right_value = _temporal_value(left), _temporal_value(right)
    if right_value is not None and left_value is None:
        return op, left, right_value
    if left_value is not None and right_value is None:
        return _MIRRORED[op], right, left_value
    return None


def render_null_safe(*, op: str, operands: List[Expression], dialect: SqlDialect) -> Expression:
    """``a IS [NOT] DISTINCT FROM b`` through the dialect's null-safe equality."""
    left, right = bare_comparands(*(operand_copy(o) for o in operands))
    same = dialect.build_is_not_distinct_from(left, right)
    return same if op == "is not distinct from" else exp.Not(this=exp.Paren(this=same))


def render_cast(
    *, key: CastKey, part: Callable[[Any], Expression], dialect: SqlDialect, direct: bool = True,
) -> Expression:
    """The author's ``CAST`` / ``TRY_CAST`` through the dialect's author-cast lowering, and a ``TRY_CAST`` of
    ``json_value`` (``RETURNING``) as the dialect's typed read when ``direct`` (its inputs readable) and the type
    has no length or scale to apply."""
    to = cast_target(key.type_text, dialect=dialect)
    if key.try_:
        require_library(dialect, "try_cast")
        to = dialect.try_cast_type(to, data_type=key.data_type)
    source = key.source
    sized = text_length(to) is not None or decimal_parts(to) is not None
    if key.try_ and direct and not sized and isinstance(source, ScalarCallKey) and source.name == "json_value":
        value, path, native = json_operands(source, part=part, dialect=dialect)
        return dialect.build_json_value_as(value, path, native, to, key.data_type)
    value = part(source)
    if key.try_:
        value.meta[CONSTANT_INPUT] = reads_no_rows(source)
    return dialect.build_author_cast(value, to, source=key.numeric_source, try_=key.try_)


def render_temporal_comparison(
    *, op: TemporalComparisonOp, operand: Expression, value: date, dialect: SqlDialect,
) -> Expression:
    """A comparison against a temporal literal, through the dialect's one hook (ISO text, so a zoned column stays bare)."""
    operand, _ = bare_comparands(operand, exp.Literal.string(""))
    grouped = _paren_if_lower_prec(operand, parent_prec=_COMPARISON_PREC, is_right=False)
    return dialect.build_temporal_comparison(op=op, operand=grouped, value=value)


def render_scalar_call(
    *, name: str, args: List[Expression], dialect: SqlDialect,
) -> Expression:
    """The one ScalarCall policy: a library function through its dialect hook, else the same-named SQL call.
    The log fix-up is load-bearing (``passthrough_call``)."""
    arity_error = check_scalar_arity(name=name, argc=len(args))
    if arity_error is not None:
        # Checked before building: sqlglot is inconsistent (3-arg ROUND drops the third, etc.).
        raise NotImplementedError(f"Scalar arity check failed: {arity_error}")
    if name in DATE_FUNCTIONS:
        raise RuntimeError(f"Date function {name!r} renders only from its key (render_date_call).")
    library = render_library_call(name=name, args=args, dialect=dialect)
    if library is not None:
        return dialect.rewrite_library_ast(dialect.rewrite_target_ast(library))
    args = in_domain(name, args, dialect=dialect)
    if name == "mod":
        return dialect.build_modulo(args[0], args[1], True)
    if name == "ln":
        return dialect.natural_log(args[0])
    if name == "round" and len(args) == 2:
        rounded = dialect.build_round(args[0], args[1])
        if rounded is not None:
            return rounded
    if name == "log" and len(args) == 1:
        # Postgres's one-argument log is base 10; elsewhere bare LOG is often ln.
        return passthrough_call(name="log10", args=args, dialect=dialect)
    return passthrough_call(name=name, args=args, dialect=dialect)


def _number(node: Expression) -> Optional[Decimal]:
    """A numeric literal's value (``-2`` included), else ``None``."""
    negative = isinstance(node, exp.Neg)
    inner = node.this if negative else node
    if not (isinstance(inner, exp.Literal) and inner.is_number):
        return None
    return -Decimal(inner.this) if negative else Decimal(inner.this)


def _guarded(x: Expression, condition: Expression, *, holds: bool) -> Expression:
    return x if holds else case((condition, x.copy()))


def _positive(x: Expression) -> Expression:
    value = _number(x)
    return _guarded(x, exp.GT(this=operand_copy(x), expression=lit(0)), holds=value is not None and value > 0)


def _log_base(base: Expression) -> Expression:
    value = _number(base)
    valid = exp.And(this=exp.GT(this=operand_copy(base), expression=lit(0)),
                    expression=exp.NEQ(this=operand_copy(base), expression=lit(1)))
    return _guarded(base, valid, holds=value is not None and value > 0 and value != 1)


def _non_negative(x: Expression) -> Expression:
    value = _number(x)
    return _guarded(x, exp.GTE(this=operand_copy(x), expression=lit(0)), holds=value is not None and value >= 0)


def _power_args(base: Expression, exponent: Expression, *, dialect: SqlDialect) -> List[Expression]:
    b, e = _number(base), _number(exponent)
    if (b is not None and b > 0) or (e is not None and e >= 0 and e == e.to_integral_value()):
        return [base, exponent]
    fractional = exp.NEQ(this=operand_copy(exponent), expression=dialect.truncate(exponent))
    undefined = either(
        exp.And(this=exp.LT(this=operand_copy(base), expression=lit(0)), expression=fractional),
        exp.And(this=exp.EQ(this=operand_copy(base), expression=lit(0)),
                expression=exp.LT(this=operand_copy(exponent), expression=lit(0))),
    )
    return [case((undefined, exp.Null()), default=base.copy()), exponent]


def in_domain(name: str, args: List[Expression], *, dialect: SqlDialect) -> List[Expression]:
    """``args`` with an argument outside the function's domain read as NULL, so the call yields NULL; NULLing the
    argument (not wrapping the call) holds on an engine that evaluates every CASE branch. A literal already in
    the domain stays as written."""
    if name in ("ln", "log10", "log2") or (name == "log" and len(args) == 1):
        return [_positive(args[0])]
    if name == "log":
        return [_log_base(args[0]), _positive(args[1])]
    if name == "sqrt":
        return [_non_negative(args[0])]
    if name in ("pow", "power"):
        return _power_args(args[0], args[1], dialect=dialect)
    return args


def _shared_element(
    key: ScalarCallKey, *, field_type: FieldTypeFn, field_element: FieldElementFn,
) -> Optional[DataType]:
    """The one element type of ``contains_any`` / ``contains_all``'s two lists, or ``length``'s list, when known."""
    if key.name not in LIST_ONLY_FUNCTIONS and key.name != "length":
        return None
    types = {list_element_type(a, field_type=field_type, field_element=field_element) for a in key.args} - {None}
    return types.pop() if len(types) == 1 else None


def render_scalar_key(
    *, key: ScalarCallKey, args: List[Expression], dialect: SqlDialect, field_type: FieldTypeFn,
    part: Callable[[Any], Expression], field_element: Optional[FieldElementFn] = None,
) -> Expression:
    """A non-date scalar key: ``mod`` by its operand types, else ``render_scalar_call``."""
    if key.name == "mod":
        return render_modulo(key=key, operands=args, dialect=dialect, field_type=field_type)
    if is_list_fn(key.name) and is_list_call(key, field_type=field_type):
        numbers = numbers_read_list(key, field_type=field_type)
        if numbers is not None:
            each = DataType.STRING if numbers.try_ else DataType.FLOAT
            return dialect.rewrite_target_ast(
                dialect.build_list_call(key.name, (args[0], part(numbers.source)), each=each))
        element = _shared_element(key, field_type=field_type, field_element=field_element or _no_element)
        return dialect.rewrite_target_ast(dialect.build_list_call(key.name, tuple(args), element=element))
    return render_scalar_call(name=key.name, args=args, dialect=dialect)


def _no_element(_key: Any) -> Optional[DataType]:
    return None


def render_array_literal(*, key: ArrayLiteralKey, dialect: SqlDialect) -> Expression:
    return dialect.build_array_literal([_literal(v.value, dialect=dialect) for v in key.values])


def render_list_set(
    *, key: ListSetKey, part: Callable[[Any], Expression], dialect: SqlDialect, field_type: FieldTypeFn,
    field_element: Optional[FieldElementFn],
) -> Expression:
    """The element set through the dialect; ``[]``'s is itself, typed by the list it meets."""
    if isinstance(key.source, ArrayLiteralKey) and not key.source.values:
        return dialect.empty_list()
    element = list_element_type(key.source, field_type=field_type, field_element=field_element or _no_element)
    return dialect.build_list_set(part(key.source), element)


def list_keyed(keys: Sequence[Any], parts: List[Expression], *, dialect: SqlDialect) -> List[Expression]:
    """Each element-set operand as what a comparison reads of it."""
    return [dialect.build_list_key(p) if isinstance(k, ListSetKey) else p for k, p in zip(keys, parts, strict=True)]


def passthrough_call(*, name: str, args: List[Expression], dialect: SqlDialect) -> Expression:
    """``name(args)`` as sqlglot spells it on ``dialect``. The log fix-up is load-bearing:
    ``exp.func("LOG10", x)`` normalises to ``LOG(10, x)``, wrong where ``LOG10`` is native single-arg."""
    node = dialect.rewrite_library_ast(dialect.rewrite_target_ast(func(name.upper(), *args)))
    return rewrite_log_alias(node, dialect=dialect)


DATE_FUNCTIONS = frozenset({*DATE_OPERAND_ARGS, *CLOCK_FUNCTIONS})


def render_date_call(
    *, key: ScalarCallKey, args: List[Expression], dialect: SqlDialect, field_type: FieldTypeFn,
) -> Expression:
    """Render a date-function call through the dialect's typed hooks; ``args`` are the rendered arguments."""
    if key.name in CLOCK_FUNCTIONS and dialect.time_zone is not None and dialect.zone_now is not None:
        # Under a zone the stage's one clock reading, in its wall clock.
        now = dialect.zone_now
        if key.name == "now":
            return dialect.build_temporal_literal(value=now, dt=DataType.DATETIME)
        return dialect.build_temporal_literal(value=now.date(), dt=DataType.DATE)
    if key.name == "now":
        return dialect.build_current_timestamp()
    if key.name == "current_date":
        return dialect.build_current_date()
    types = {pos: _operand_type(key, pos, field_type=field_type) for pos in DATE_OPERAND_ARGS[key.name]}
    if key.name == "date_part":
        return dialect.build_date_part(part=DatePart(unit_word(key.args[0])), expr=args[1], operand=types[1])
    if key.name == "date_diff":
        start, end = args[1], args[2]
        operand = types[1]
        if types[1] is not types[2]:
            operand = DataType.DATETIME
            start = dialect.promote_to_timestamp(start) if types[1] is DataType.DATE else start
            end = dialect.promote_to_timestamp(end) if types[2] is DataType.DATE else end
        return dialect.build_date_diff(
            unit=TimeGranularity(unit_word(key.args[0])), start=start, end=end, operand=operand,
        )
    count = args[DATE_ADD_COUNT_ARG]
    if not isinstance(key.args[DATE_ADD_COUNT_ARG], LiteralKey):
        # One truncation toward zero, identical on every dialect.
        count = exp.Cast(
            this=passthrough_call(name="trunc", args=[count], dialect=dialect),
            to=exp.DataType.build("INT"),
        )
    return dialect.build_date_add(
        expr=args[0], count=count, unit=TimeGranularity(unit_word(key.args[2])), operand=types[0],
    )


def _operand_type(key: ScalarCallKey, pos: int, *, field_type: FieldTypeFn) -> DataType:
    dt = temporal_type(key.args[pos], field_type=field_type)
    if dt is None:
        # The checker rejects these at bind; reaching render is a wiring bug.
        raise RuntimeError(f"{key.name}() operand {pos} has no DATE/TIMESTAMP type at render.")
    return dt


def data_type_target(dt: DataType) -> exp.DataType:
    """The CAST target for SLayer type ``dt``; INT is 64-bit."""
    return exp.DataType(this=exp.DataType.Type.BIGINT if dt is DataType.INTEGER else exp.DataType.Type(dt.kind.value))


def int_value(value: Expression) -> Expression:
    """``value`` read as an integer — a boolean's 1 / 0, NULL kept."""
    return exp.Cast(this=value, to=exp.DataType.build("INT"))


def coerced_parts(
    *, key: Any, parts: List[Expression], field_type: FieldTypeFn, dialect: SqlDialect,
) -> List[Expression]:
    """``key``'s rendered children with each boolean in a numeric position read as its integer, each DATE in a
    TIMESTAMP position as its midnight, and two compared temporal values as the dialect compares them."""
    ints = numeric_boolean_positions(key, field_type=field_type)
    dates = date_promotion_positions(key, field_type=field_type)
    out = [
        int_value(p) if i in ints else dialect.promote_to_timestamp(p) if i in dates else p
        for i, p in enumerate(parts)
    ]
    out = _typed_null_safe(key=key, parts=out, field_type=field_type, dialect=dialect)
    if temporal_pair(key, field_type=field_type) is not None:
        out = list(dialect.build_temporal_comparands(out[0], out[1]))
    return out


def bucket_operand(key: TimeTruncKey, *, field_type: FieldTypeFn) -> DataType:
    """The DATE / TIMESTAMP type of a bucket's source."""
    dt = temporal_type(key.source, field_type=field_type)
    if dt is None:
        # The checker rejects these at bind; reaching render is a wiring bug.
        raise RuntimeError(f"date_trunc() source {key.source!r} has no DATE/TIMESTAMP type at render.")
    return dt


def render_bucket(
    *, key: TimeTruncKey, source: Expression, dialect: SqlDialect, field_type: FieldTypeFn,
) -> Expression:
    """A bucket through the dialect, typed by its source."""
    return dialect.build_bucket(
        col_expr=source, granularity=key.granularity, operand=bucket_operand(key, field_type=field_type),
    )


def render_mixed_nullif(*, args: List[Expression], date_position: int, dialect: SqlDialect) -> Expression:
    """``nullif`` of a DATE and a TIMESTAMP: compared at the DATE's midnight, its first argument returned as is."""
    sides = [dialect.promote_to_timestamp(a) if i == date_position else a.copy() for i, a in enumerate(args)]
    left, right = dialect.build_temporal_comparands(sides[0], sides[1])
    return case((exp.EQ(this=left, expression=right), exp.Null()), default=args[0])


def date_typed_comparison(
    comparison: Tuple[TemporalComparisonOp, Any, date], *, field_type: FieldTypeFn,
) -> Tuple[TemporalComparisonOp, Any, date, bool]:
    """A date-time literal compared with a DATE operand as the exact DATE bound, else with the operand read as its
    midnight (the trailing flag)."""
    op, operand, value = comparison
    if isinstance(value, datetime) and temporal_type(operand, field_type=field_type) is DataType.DATE:
        bound = date_bound(op, value)
        if bound is None:
            return op, operand, value, True
        return _TEMPORAL_COMPARISON_OPS[bound[0]], operand, bound[1], False
    return op, operand, value, False


def iif_case_chain(
    *, key: ScalarCallKey, part: Callable[[Any], Expression], field_type: FieldTypeFn, dialect: SqlDialect,
) -> exp.Case:
    """Render an ``iif`` chain as one multi-WHEN CASE, flattening nested ``iif``
    in the otherwise position; ``part`` renders each argument. A boolean branch of a
    numeric chain reads as its integer, a DATE branch of a TIMESTAMP chain as its midnight."""
    mixed = bool(numeric_boolean_positions(key, field_type=field_type))
    timestamp = value_type(key, field_type=field_type) is DataType.DATETIME

    def value(arg: Any) -> Expression:
        rendered = part(arg)
        if mixed and boolean_valued(arg, field_type=field_type):
            return int_value(rendered)
        if timestamp and temporal_type(arg, field_type=field_type) is DataType.DATE:
            return dialect.promote_to_timestamp(rendered)
        return rendered

    ifs: List[exp.If] = []
    node: Any = key
    while isinstance(node, ScalarCallKey) and node.name == "iif":
        # Fail-closed arity backstop so a malformed key can't surface an opaque IndexError.
        arity_error = check_scalar_arity(name="iif", argc=len(node.args))
        if arity_error is not None:
            raise NotImplementedError(f"Scalar arity check failed: {arity_error}")
        ifs.append(exp.If(this=part(node.args[0]), true=value(node.args[1])))
        node = node.args[2]
    return exp.Case(ifs=ifs, default=value(node))


def _parsed_temporal(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    return parse_temporal_value(value) if isinstance(value, str) else None


def _off_midnight(value: Any) -> bool:
    parsed = _parsed_temporal(value)
    return isinstance(parsed, datetime) and parsed.time() != datetime.min.time()


def typed_temporal_literal(value: Any, *, operand: Optional[DataType], dialect: SqlDialect) -> Any:
    """A literal compared with an ``operand`` value, typed to it; where dates live in text, a literal already of
    that type keeps its spelling. An off-midnight instant beside a DATE promotes the DATE instead."""
    parsed = _parsed_temporal(value) if operand is not None else None
    if parsed is None:
        return value
    if operand is DataType.DATETIME and not isinstance(parsed, datetime):
        return datetime.combine(parsed, datetime.min.time())
    if operand is DataType.DATE and isinstance(parsed, datetime) and parsed.time() == datetime.min.time():
        return parsed.date()
    return value if dialect.temporal_text_compatible else parsed


def _typed_null_safe(
    *, key: Any, parts: List[Expression], field_type: FieldTypeFn, dialect: SqlDialect,
) -> List[Expression]:
    """A null-safe comparison's temporal literal typed to the other side, as ``IN`` types its members."""
    if not (isinstance(key, ArithmeticKey) and key.op in NULL_SAFE_COMPARISON_OPS and len(key.operands) == 2):
        return parts
    out = list(parts)
    for i, literal in enumerate(key.operands):
        other = key.operands[1 - i]
        operand = temporal_type(other, field_type=field_type)
        if not isinstance(literal, LiteralKey) or isinstance(other, LiteralKey) or operand is None:
            continue
        if operand is DataType.DATE and _off_midnight(literal.value):
            out[1 - i], operand = dialect.promote_to_timestamp(out[1 - i]), DataType.DATETIME
        typed = typed_temporal_literal(literal.value, operand=operand, dialect=dialect)
        if typed is not literal.value:
            out[i] = _literal(typed, dialect=dialect)
    return out


def render_in_predicate(
    *, key: InKey, column: Expression, dialect: SqlDialect, field_type: FieldTypeFn,
) -> Expression:
    """``column [NOT] IN (…)`` over ``key``'s literals, an ISO member typed to a temporal column."""
    # Backstop for the bind-time rule: a NULL member breaks IN, and ``NOT IN`` with one matches no rows.
    if any(v.value is None for v in key.values):
        raise NotImplementedError(
            "NULL is not allowed inside an IN list: 'NOT IN' with a NULL "
            "matches no rows. Test for null separately with IS NULL / "
            "IS NOT NULL.",
        )
    operand = temporal_type(key.field, field_type=field_type)
    if operand is DataType.DATE and any(_off_midnight(v.value) for v in key.values):
        # No DATE equals an off-midnight instant: the column reads as its midnight instead.
        column, operand = dialect.promote_to_timestamp(column), DataType.DATETIME
    node = exp.In(this=column, expressions=[
        _literal(typed_temporal_literal(v.value, operand=operand, dialect=dialect), dialect=dialect)
        for v in key.values
    ])
    return exp.Not(this=node) if key.negated else node


def render_row_expression(
    *,
    key: Any,
    dialect: SqlDialect,
    resolve_field: Callable[[Any], Expression],
    field_type: FieldTypeFn,
    field_element: Optional[FieldElementFn] = None,
) -> Expression:
    """Render an aggregation-free row-level ``ValueKey`` tree (an
    ``AggregateKey``'s expression source) to sqlglot AST.

    Field-like leaves resolve through ``resolve_field``; composites reuse the
    shared composers so the expression renders exactly like the same tree in
    any other row-level position.
    """
    def _part(a: Any) -> Expression:
        # ANY key routes as a key (the tail raise owns unsupported kinds); only
        # true scalars render as literals.
        if isinstance(a, _FrozenKey):
            return render_row_expression(
                key=a, dialect=dialect, resolve_field=resolve_field, field_type=field_type,
                field_element=field_element,
            )
        return _literal(a, dialect=dialect)

    if isinstance(key, (FieldKey, FieldSqlKey)):
        return resolve_field(key)
    if isinstance(key, LiteralKey):
        return _literal(key.value, dialect=dialect)
    if isinstance(key, ArithmeticKey):
        return _row_arithmetic(key=key, part=_part, dialect=dialect, field_type=field_type)
    if isinstance(key, TimeTruncKey):
        return render_bucket(key=key, source=_part(key.source), dialect=dialect, field_type=field_type)
    if isinstance(key, ScalarCallKey):
        return _row_scalar_call(key=key, part=_part, dialect=dialect, field_type=field_type,
                                field_element=field_element)
    if isinstance(key, InKey):
        (column,) = coerced_parts(key=key, parts=[_part(key.field)], field_type=field_type, dialect=dialect)
        return render_in_predicate(key=key, column=column, dialect=dialect, field_type=field_type)
    if isinstance(key, CastKey):
        return render_cast(key=key, part=_part, dialect=dialect)
    if isinstance(key, ArrayLiteralKey):
        return render_array_literal(key=key, dialect=dialect)
    if isinstance(key, ListSetKey):
        return render_list_set(key=key, part=_part, dialect=dialect, field_type=field_type,
                               field_element=field_element)
    raise NotImplementedError(
        f"Row-level expression cannot contain {type(key).__name__}.",
    )


def _row_arithmetic(
    *, key: ArithmeticKey, part: Callable[[Any], Expression], dialect: SqlDialect, field_type: FieldTypeFn,
) -> Expression:
    comparison = temporal_comparison(key)
    if comparison is not None:
        op, operand, value, midnight = date_typed_comparison(comparison, field_type=field_type)
        rendered = dialect.promote_to_timestamp(part(operand)) if midnight else part(operand)
        return render_temporal_comparison(op=op, operand=rendered, value=value, dialect=dialect)
    rendered = list_keyed(key.operands, [part(o) for o in key.operands], dialect=dialect)
    operands = coerced_parts(key=key, parts=rendered, field_type=field_type, dialect=dialect)
    if key.op in NULL_SAFE_COMPARISON_OPS:
        return render_null_safe(op=key.op, operands=operands, dialect=dialect)
    if key.op == "%":
        return render_modulo(key=key, operands=operands, dialect=dialect, field_type=field_type)
    return render_arithmetic(op=key.op.lower(), operands=operands)


def render_modulo(
    *, key: ArithmeticKey | ScalarCallKey, operands: List[Expression], dialect: SqlDialect, field_type: FieldTypeFn,
) -> Expression:
    """``%`` / ``mod`` through the dialect, which knows whether its native ``%`` takes non-integers."""
    children = key.operands if isinstance(key, ArithmeticKey) else key.args
    integer = all(integer_valued(c, field_type=field_type) for c in children)
    return dialect.build_modulo(operands[0], operands[1], integer)


def _row_scalar_call(
    *, key: ScalarCallKey, part: Callable[[Any], Expression], dialect: SqlDialect, field_type: FieldTypeFn,
    field_element: Optional[FieldElementFn],
) -> Expression:
    if key.name == "iif":
        return iif_case_chain(key=key, part=part, field_type=field_type, dialect=dialect)
    args = coerced_parts(key=key, parts=[part(a) for a in key.args], field_type=field_type, dialect=dialect)
    date_position = mixed_nullif(key, field_type=field_type)
    if date_position is not None:
        return render_mixed_nullif(args=args, date_position=date_position, dialect=dialect)
    if key.name in DATE_FUNCTIONS:
        return render_date_call(key=key, args=args, dialect=dialect, field_type=field_type)
    return render_scalar_key(key=key, args=args, dialect=dialect, field_type=field_type, part=part,
                             field_element=field_element)
