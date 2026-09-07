"""Scope-free row-level expression composers (DEV-1826).

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

from decimal import Decimal
from typing import Any, Callable, Dict, List, Tuple

from sqlglot import exp

from slayer.core.keys import (
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    LiteralKey,
    ScalarCallKey,
    _FrozenKey,
    check_scalar_arity,
)
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.render.parse import rewrite_log_aliases as rewrite_log_alias

_BINARY_OPS: Dict[str, Any] = {
    "+": exp.Add, "-": exp.Sub, "*": exp.Mul, "/": exp.Div,
    "%": exp.Mod,
    "=": exp.EQ, "==": exp.EQ, "!=": exp.NEQ, "<>": exp.NEQ,
    "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE,
}


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


def iif_case_chain(
    *, key: ScalarCallKey, part: Callable[[Any], exp.Expression],
) -> exp.Case:
    """Render an ``iif`` chain as one multi-WHEN CASE, flattening nested ``iif``
    in the otherwise position; ``part`` renders each argument."""
    ifs: List[exp.If] = []
    node: Any = key
    while isinstance(node, ScalarCallKey) and node.name == "iif":
        # Fail-closed arity backstop so a malformed key can't surface an opaque IndexError.
        arity_error = check_scalar_arity(name="iif", argc=len(node.args))
        if arity_error is not None:
            raise NotImplementedError(arity_error)
        ifs.append(exp.If(this=part(node.args[0]), true=part(node.args[1])))
        node = node.args[2]
    return exp.Case(ifs=ifs, default=part(node))


def render_row_expression(
    *,
    key: Any,
    dialect: SqlDialect,
    resolve_column: Callable[[Any], exp.Expression],
) -> exp.Expression:
    """Render an aggregation-free row-level ``ValueKey`` tree (an
    ``AggregateKey``'s expression source, DEV-1826) to sqlglot AST.

    Column-like leaves resolve through ``resolve_column``; composites reuse the
    shared composers so the expression renders exactly like the same tree in
    any other row-level position.
    """
    def _part(a: Any) -> exp.Expression:
        # ANY key routes as a key (the tail raise owns unsupported kinds); only
        # true scalars render as literals.
        if isinstance(a, _FrozenKey):
            return render_row_expression(
                key=a, dialect=dialect, resolve_column=resolve_column,
            )
        return _literal(a)

    if isinstance(key, (ColumnKey, ColumnSqlKey)):
        return resolve_column(key)
    if isinstance(key, LiteralKey):
        return _literal(key.value)
    if isinstance(key, ArithmeticKey):
        return render_arithmetic(
            op=key.op.lower(),
            operands=[_part(o) for o in key.operands],
        )
    if isinstance(key, ScalarCallKey):
        if key.name == "iif":
            return iif_case_chain(key=key, part=_part)
        return render_scalar_call(
            name=key.name, args=[_part(a) for a in key.args], dialect=dialect,
        )
    raise NotImplementedError(
        f"Row-level expression cannot contain {type(key).__name__}.",
    )
