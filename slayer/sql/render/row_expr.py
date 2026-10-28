"""Scope-free row-level expression composers.

The pure ``ValueKey`` → sqlglot composers that need NO ``RenderContext`` /
``ScopeFrame``: literals, arithmetic/comparison/boolean operators, scalar
calls, and the ``iif`` → ``CASE`` chain. Extracted from ``value_expr.py`` so
``ScopeFrame`` (which ``value_expr`` imports) can also render an aggregate's
expression source without an import cycle; ``value_expr`` re-imports them.

:func:`render_row_expression` renders an aggregation-free row-level tree
(an ``AggregateKey.source`` expression), resolving column-like leaves through
a caller-supplied ``resolve_column`` callback — the one seam that differs
between the ScopeFrame and the generator's spec-builder call sites.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, DatePart, TimeGranularity
from slayer.core.keys import (
    CLOCK_FUNCTIONS,
    DATE_ADD_COUNT_ARG,
    DATE_OPERAND_ARGS,
    NULL_SAFE_COMPARISON_OPS,
    ArithmeticKey,
    CastKey,
    ColumnKey,
    ColumnSqlKey,
    ColumnTypeFn,
    InKey,
    LiteralKey,
    ScalarCallKey,
    TimeTruncKey,
    _FrozenKey,
    boolean_valued,
    check_scalar_arity,
    integer_valued,
    numeric_boolean_positions,
    temporal_type,
    unit_word,
)
from slayer.sql.dialects.base import (
    SqlDialect,
    TemporalComparisonOp,
    bare_comparands,
    cast_target,
    func,
    operand_copy,
    temporal_type_of,
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
    """The database's own cast of the author's type; ``TRY_CAST`` through the dialect's lowering, and a
    ``TRY_CAST`` of ``json_value`` (``RETURNING``) as the dialect's typed read when ``direct`` (its inputs readable)."""
    to = cast_target(key.type_text, dialect=dialect)
    if key.try_:
        to = dialect.try_cast_type(to, data_type=key.data_type)
    source = key.source
    if key.try_ and direct and isinstance(source, ScalarCallKey) and source.name == "json_value":
        value, path, native = json_operands(source, part=part, dialect=dialect)
        return dialect.build_json_value_as(value, path, native, to, key.data_type)
    value = part(source)
    return dialect.build_try_cast(value, to) if key.try_ else dialect.build_cast(value, to)


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
    if name == "mod":
        return dialect.build_modulo(args[0], args[1], True)
    if name == "ln":
        return dialect.natural_log(args[0])
    if name == "log" and len(args) == 1:
        # Postgres's one-argument log is base 10; elsewhere bare LOG is often ln.
        return passthrough_call(name="log10", args=args, dialect=dialect)
    return passthrough_call(name=name, args=args, dialect=dialect)


def render_scalar_key(
    *, key: ScalarCallKey, args: List[Expression], dialect: SqlDialect, column_type: ColumnTypeFn,
) -> Expression:
    """A non-date scalar key: ``mod`` by its operand types, else ``render_scalar_call``."""
    if key.name == "mod":
        return render_modulo(key=key, operands=args, dialect=dialect, column_type=column_type)
    return render_scalar_call(name=key.name, args=args, dialect=dialect)


def passthrough_call(*, name: str, args: List[Expression], dialect: SqlDialect) -> Expression:
    """``name(args)`` as sqlglot spells it on ``dialect``. The log fix-up is load-bearing:
    ``exp.func("LOG10", x)`` normalises to ``LOG(10, x)``, wrong where ``LOG10`` is native single-arg."""
    node = dialect.rewrite_library_ast(dialect.rewrite_target_ast(func(name.upper(), *args)))
    return rewrite_log_alias(node, dialect=dialect)


DATE_FUNCTIONS = frozenset({*DATE_OPERAND_ARGS, *CLOCK_FUNCTIONS})


def render_date_call(
    *, key: ScalarCallKey, args: List[Expression], dialect: SqlDialect, column_type: ColumnTypeFn,
) -> Expression:
    """Render a date-function call through the dialect's typed hooks; ``args`` are the rendered arguments."""
    if key.name == "now":
        return dialect.build_current_timestamp()
    if key.name == "current_date":
        return dialect.build_current_date()
    types = {pos: _operand_type(key, pos, column_type=column_type) for pos in DATE_OPERAND_ARGS[key.name]}
    if key.name == "date_part":
        return dialect.build_date_part(part=DatePart(unit_word(key.args[0])), expr=args[1], operand=types[1])
    if key.name == "date_diff":
        start, end = args[1], args[2]
        operand = types[1]
        if types[1] is not types[2]:
            operand = DataType.TIMESTAMP
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


def _operand_type(key: ScalarCallKey, pos: int, *, column_type: ColumnTypeFn) -> DataType:
    dt = temporal_type(key.args[pos], column_type=column_type)
    if dt is None:
        # The checker rejects these at bind; reaching render is a wiring bug.
        raise RuntimeError(f"{key.name}() operand {pos} has no DATE/TIMESTAMP type at render.")
    return dt


def int_value(value: Expression) -> Expression:
    """``value`` read as an integer — a boolean's 1 / 0, NULL kept."""
    return exp.Cast(this=value, to=exp.DataType.build("INT"))


def numeric_parts(
    *, key: Any, parts: List[Expression], column_type: ColumnTypeFn,
) -> List[Expression]:
    """``key``'s rendered children with each boolean in a numeric position read as its integer."""
    positions = numeric_boolean_positions(key, column_type=column_type)
    return [int_value(p) if i in positions else p for i, p in enumerate(parts)]


def iif_case_chain(
    *, key: ScalarCallKey, part: Callable[[Any], Expression], column_type: ColumnTypeFn,
) -> exp.Case:
    """Render an ``iif`` chain as one multi-WHEN CASE, flattening nested ``iif``
    in the otherwise position; ``part`` renders each argument. A boolean branch of a
    numeric chain reads as its integer."""
    mixed = bool(numeric_boolean_positions(key, column_type=column_type))

    def value(arg: Any) -> Expression:
        rendered = part(arg)
        return int_value(rendered) if mixed and boolean_valued(arg, column_type=column_type) else rendered

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


def render_in_predicate(*, key: InKey, column: Expression, dialect: SqlDialect) -> Expression:
    """``column [NOT] IN (…)`` over ``key``'s literals."""
    # Backstop for the bind-time rule: a NULL member breaks IN, and ``NOT IN`` with one matches no rows.
    if any(v.value is None for v in key.values):
        raise NotImplementedError(
            "NULL is not allowed inside an IN list: 'NOT IN' with a NULL "
            "matches no rows. Test for null separately with IS NULL / "
            "IS NOT NULL.",
        )
    node = exp.In(this=column, expressions=[_literal(v.value, dialect=dialect) for v in key.values])
    return exp.Not(this=node) if key.negated else node


def render_row_expression(
    *,
    key: Any,
    dialect: SqlDialect,
    resolve_column: Callable[[Any], Expression],
    column_type: ColumnTypeFn,
) -> Expression:
    """Render an aggregation-free row-level ``ValueKey`` tree (an
    ``AggregateKey``'s expression source) to sqlglot AST.

    Column-like leaves resolve through ``resolve_column``; composites reuse the
    shared composers so the expression renders exactly like the same tree in
    any other row-level position.
    """
    def _part(a: Any) -> Expression:
        # ANY key routes as a key (the tail raise owns unsupported kinds); only
        # true scalars render as literals.
        if isinstance(a, _FrozenKey):
            return render_row_expression(
                key=a, dialect=dialect, resolve_column=resolve_column, column_type=column_type,
            )
        return _literal(a, dialect=dialect)

    if isinstance(key, (ColumnKey, ColumnSqlKey)):
        return resolve_column(key)
    if isinstance(key, LiteralKey):
        return _literal(key.value, dialect=dialect)
    if isinstance(key, ArithmeticKey):
        return _row_arithmetic(key=key, part=_part, dialect=dialect, column_type=column_type)
    if isinstance(key, TimeTruncKey):
        return dialect.build_bucket(col_expr=_part(key.source), granularity=key.granularity)
    if isinstance(key, ScalarCallKey):
        return _row_scalar_call(key=key, part=_part, dialect=dialect, column_type=column_type)
    if isinstance(key, InKey):
        (column,) = numeric_parts(key=key, parts=[_part(key.column)], column_type=column_type)
        return render_in_predicate(key=key, column=column, dialect=dialect)
    if isinstance(key, CastKey):
        return render_cast(key=key, part=_part, dialect=dialect)
    raise NotImplementedError(
        f"Row-level expression cannot contain {type(key).__name__}.",
    )


def _row_arithmetic(
    *, key: ArithmeticKey, part: Callable[[Any], Expression], dialect: SqlDialect, column_type: ColumnTypeFn,
) -> Expression:
    comparison = temporal_comparison(key)
    if comparison is not None:
        op, operand, value = comparison
        return render_temporal_comparison(op=op, operand=part(operand), value=value, dialect=dialect)
    if key.op in NULL_SAFE_COMPARISON_OPS:
        return render_null_safe(op=key.op, operands=[part(o) for o in key.operands], dialect=dialect)
    operands = numeric_parts(key=key, parts=[part(o) for o in key.operands], column_type=column_type)
    if key.op == "%":
        return render_modulo(key=key, operands=operands, dialect=dialect, column_type=column_type)
    return render_arithmetic(op=key.op.lower(), operands=operands)


def render_modulo(
    *, key: ArithmeticKey | ScalarCallKey, operands: List[Expression], dialect: SqlDialect, column_type: ColumnTypeFn,
) -> Expression:
    """``%`` / ``mod`` through the dialect, which knows whether its native ``%`` takes non-integers."""
    children = key.operands if isinstance(key, ArithmeticKey) else key.args
    integer = all(integer_valued(c, column_type=column_type) for c in children)
    return dialect.build_modulo(operands[0], operands[1], integer)


def _row_scalar_call(
    *, key: ScalarCallKey, part: Callable[[Any], Expression], dialect: SqlDialect, column_type: ColumnTypeFn,
) -> Expression:
    if key.name == "iif":
        return iif_case_chain(key=key, part=part, column_type=column_type)
    args = numeric_parts(key=key, parts=[part(a) for a in key.args], column_type=column_type)
    if key.name in DATE_FUNCTIONS:
        return render_date_call(key=key, args=args, dialect=dialect, column_type=column_type)
    return render_scalar_key(key=key, args=args, dialect=dialect, column_type=column_type)
