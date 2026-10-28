"""OSI metric SQL expression -> SLayer formula transform.

``convert_expression`` replaces each outermost aggregate (``FILTER`` included)
with a sentinel, renders the surrounding arithmetic with sqlglot, and
substitutes each aggregate's formula text. ``agg(x) FILTER (WHERE p)`` lowers
to ``agg(CASE WHEN p THEN x END)``. A non-bare aggregate operand is written
inline in formula syntax when the formula grammar accepts it, else
materialized as a hidden derived column on its single owning dataset.
Anything inexpressible is clean-failed (``ok=False``) with a reason.

Model-awareness is injected via callbacks so the transform is unit-testable:
- ``owner_of(qualifier, column) -> model | None`` — which dataset owns a column.
- ``ref_of(model, column) -> anchor-relative dotted ref | None``.
"""

from __future__ import annotations

import math
import re
from typing import Callable, Optional

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from pydantic import BaseModel

from slayer.core.keys import SCALAR_PASSTHROUGH, check_scalar_arity
from slayer.core.refs import functional_agg_text
from slayer.engine.syntax import parse_expr

_SENTINEL_PREFIX = "SLAYERTOKEN"
_SIMPLE_AGG = {exp.Sum: "sum", exp.Avg: "avg", exp.Min: "min", exp.Max: "max"}
_AGG_NODES = (exp.Filter, exp.AggFunc, exp.WithinGroup)

_BINARY_OPS: dict[type[exp.Expr], str] = {
    exp.Add: "+", exp.Sub: "-", exp.Mul: "*", exp.Div: "/", exp.Mod: "%",
    exp.EQ: "==", exp.NEQ: "!=", exp.GT: ">", exp.GTE: ">=", exp.LT: "<", exp.LTE: "<=",
    exp.And: "and", exp.Or: "or",
}
# Passthrough scalars whose sqlglot argument order is not the formula's.
_NOT_INLINE_FUNCS = frozenset({"log"})
_FUNC_ARG_KEYS = frozenset({"this", "expression", "expressions", "decimals"})
_SAFE_STRING = re.compile(r"[^'\\]*")

OwnerOf = Callable[[Optional[str], str], Optional[str]]
RefOf = Callable[[str, str], Optional[str]]
NameTaken = Callable[[str, str], bool]  # (owning_model, name) -> already exists?


class MaterializedColumn(BaseModel):
    """A hidden derived Column the converter must create on ``owning_model``."""

    owning_model: str
    name: str
    sql: str


class ExprResult(BaseModel):
    """Outcome of converting one OSI expression to a SLayer formula."""

    ok: bool
    formula: str | None = None
    reason: str | None = None
    materialized: list[MaterializedColumn] = []
    warnings: list[str] = []


def outermost_aggregates(tree: exp.Expr) -> list[exp.Expr]:
    """Aggregates (``FILTER`` wrappers included) not nested inside another aggregate."""
    result: list[exp.Expr] = []
    for node in tree.find_all(*_AGG_NODES):
        ancestor = node.parent
        while ancestor is not None and not isinstance(ancestor, _AGG_NODES):
            ancestor = ancestor.parent
        if ancestor is None:
            result.append(node)
    return result


class _Unconvertible(Exception):
    """Internal control-flow signal carrying a clean-fail reason."""


class _NotInline(Exception):
    """The operand has no formula-syntax rendering."""


def _parens(text: str, node: exp.Expr) -> str:
    return f"({text})" if isinstance(node, (exp.Binary, exp.Not, exp.Neg, exp.Is)) else text


def _literal(node: exp.Literal) -> str:
    if not node.is_string:
        return node.this
    if not _SAFE_STRING.fullmatch(node.this):
        raise _NotInline("string literal with a quote or backslash")
    return f"'{node.this}'"


class _Converter:
    def __init__(self, entity_name: str, owner_of: OwnerOf, ref_of: RefOf,
                 percentile_unsupported: bool, name_taken: NameTaken, target: str | None) -> None:
        self.entity_name = entity_name
        self.owner_of = owner_of
        self.ref_of = ref_of
        self.percentile_unsupported = percentile_unsupported
        self.name_taken = name_taken
        self.target = target
        self.materialized: list[MaterializedColumn] = []
        self.warnings: list[str] = []
        self._dedup: dict[tuple[str, str], str] = {}
        self._counter = 0

    # ---- operands ----

    def _column_ref(self, qualifier: Optional[str], column: str) -> str:
        owner = self.owner_of(qualifier or None, column)
        if owner is None:
            raise _Unconvertible(f"cannot resolve owning dataset for column {column!r}")
        ref = self.ref_of(owner, column)
        if ref is None:
            raise _Unconvertible(
                f"column {column!r} on model {owner!r} is not reachable from the anchor"
            )
        return ref

    def _owners(self, node: exp.Expr) -> set[str]:
        """Owning datasets of every column in ``node``; an unresolvable column fails."""
        owners = {self.owner_of(c.table or None, c.name) for c in node.find_all(exp.Column)}
        if None in owners:
            raise _Unconvertible("expression references a column that cannot be resolved")
        return {o for o in owners if o is not None}

    def _materialize(self, operand: exp.Expr) -> str:
        """Create/reuse a hidden derived column for ``operand``; return its anchor-relative ref."""
        if operand.find(exp.AggFunc, exp.Window, exp.WithinGroup):
            raise _Unconvertible("nested aggregate in aggregate operand")
        owners = self._owners(operand)
        if not owners:
            raise _Unconvertible("aggregate operand references no column")
        if len(owners) != 1:
            raise _Unconvertible("aggregate operand spans multiple datasets and is not expressible inline")
        owner = owners.pop()
        operand_sql = operand.sql(dialect=self.target)
        key = (owner, operand_sql)
        name = self._dedup.get(key)
        if name is None:
            name = self._fresh_column_name(owner)
            self._dedup[key] = name
            self.materialized.append(
                MaterializedColumn(owning_model=owner, name=name, sql=operand_sql)
            )
        ref = self.ref_of(owner, name)
        if ref is None:
            raise _Unconvertible("materialized operand is not reachable from the anchor")
        return ref

    def _fresh_column_name(self, owner: str) -> str:
        """A hidden-column name that collides with no existing name on ``owner``."""
        while True:
            name = f"_{self.entity_name}_{self._counter}"
            self._counter += 1
            if not self.name_taken(owner, name):
                return name

    def _operand_ref(self, operand: exp.Expr) -> str:
        if isinstance(operand, exp.Column):
            return self._column_ref(operand.table or None, operand.name)
        try:
            text = self._inline(operand)
            parse_expr(f"count({text})")
        except (_NotInline, ValueError, SyntaxError):
            return self._materialize(operand)
        return text

    # ---- SQL AST -> formula syntax ----

    def _inline(self, node: exp.Expr) -> str:  # NOSONAR(S3776) — one flat dispatch over SQL node kinds
        if isinstance(node, exp.Column):
            return self._column_ref(node.table or None, node.name)
        if isinstance(node, exp.Literal):
            return _literal(node)
        if isinstance(node, exp.Null):
            return "None"
        if isinstance(node, exp.Boolean):
            return "True" if node.this else "False"
        if isinstance(node, exp.Paren):
            return f"({self._inline(node.this)})"
        if isinstance(node, exp.Neg):
            return f"-{_parens(self._inline(node.this), node.this)}"
        if isinstance(node, exp.Not):
            inner = node.this
            if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
                return f"{_parens(self._inline(inner.this), inner.this)} is not None"
            return f"not {_parens(self._inline(inner), inner)}"
        if isinstance(node, exp.Is):
            if not isinstance(node.expression, exp.Null):
                raise _NotInline("IS with a non-NULL operand")
            return f"{_parens(self._inline(node.this), node.this)} is None"
        op = _BINARY_OPS.get(type(node))
        if op is not None:
            left, right = node.this, node.expression
            return f"{_parens(self._inline(left), left)} {op} {_parens(self._inline(right), right)}"
        if isinstance(node, exp.In):
            if node.args.get("query") or node.args.get("unnest") or not node.expressions:
                raise _NotInline("IN without a literal list")
            items = ", ".join(self._inline(e) for e in node.expressions)
            return f"{_parens(self._inline(node.this), node.this)} in ({items})"
        if isinstance(node, exp.Case):
            return self._inline_case(node)
        if isinstance(node, exp.If):
            false = node.args.get("false")
            return (f"iif({self._inline(node.this)}, {self._inline(node.args['true'])}, "
                    f"{self._inline(false) if false is not None else 'None'})")
        if isinstance(node, exp.Func):
            return self._inline_func(node)
        raise _NotInline(f"{type(node).__name__} has no formula form")

    def _inline_case(self, node: exp.Case) -> str:
        if node.this is not None:
            raise _NotInline("simple CASE")
        whens = " ".join(
            f"WHEN {self._inline(branch.this)} THEN {self._inline(branch.args['true'])}"
            for branch in node.args.get("ifs") or []
        )
        default = node.args.get("default")
        tail = f" ELSE {self._inline(default)}" if default is not None else ""
        return f"CASE {whens}{tail} END"

    def _inline_func(self, node: exp.Func) -> str:
        if isinstance(node, (exp.AggFunc, exp.Window)):
            raise _NotInline("aggregate or window in an operand")
        name = (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()
        if name not in SCALAR_PASSTHROUGH or name in _NOT_INLINE_FUNCS:
            raise _NotInline(f"function {name!r} is not in the formula allowlist")
        args: list[exp.Expr] = []
        for key in node.arg_types:
            value = node.args.get(key)
            if not isinstance(value, (exp.Expr, list)):
                continue  # absent arg or a boolean flag
            if key not in _FUNC_ARG_KEYS:
                raise _NotInline(f"function {name!r} argument {key!r}")
            args.extend(value if isinstance(value, list) else [value])
        if check_scalar_arity(name=name, argc=len(args)) is not None:
            raise _NotInline(f"function {name!r} arity")
        return f"{name}({', '.join(self._inline(a) for a in args)})"

    # ---- one outermost aggregate ----

    def _agg_ref(self, node: exp.Expr) -> str:
        """Return the ``agg(ref)`` text for one outermost aggregate node."""
        if isinstance(node, exp.Filter):
            return self._filtered_ref(node)
        return self._plain_agg_ref(node, mask=None)

    def _filtered_ref(self, node: exp.Filter) -> str:
        where = node.expression
        predicate = where.this if isinstance(where, exp.Where) else where
        if predicate.find(exp.AggFunc, exp.Window, exp.WithinGroup, exp.Subquery, exp.Select):
            raise _Unconvertible("FILTER predicate must be row-level (no aggregate, window or subquery)")
        if len(self._owners(predicate) | self._owners(node.this)) > 1:
            raise _Unconvertible("FILTER predicate reads a dataset other than the aggregate's")
        return self._plain_agg_ref(node.this, mask=predicate)

    @staticmethod
    def _masked(operand: exp.Expr, mask: exp.Expr | None) -> exp.Expr:
        if mask is None:
            return operand
        return exp.Case(ifs=[exp.If(this=mask.copy(), true=operand.copy())])

    def _plain_agg_ref(self, node: exp.Expr, *, mask: exp.Expr | None) -> str:
        if isinstance(node, exp.WithinGroup):
            return self._percentile_ref(node, mask=mask)

        if type(node) in _SIMPLE_AGG:
            agg = _SIMPLE_AGG[type(node)]
            # SLayer supports DISTINCT only for COUNT.
            if isinstance(node.this, exp.Distinct):
                raise _Unconvertible(f"{agg.upper()}(DISTINCT ...) is not supported")
            return functional_agg_text(source=self._operand_ref(self._masked(node.this, mask)), suffix=agg)

        if isinstance(node, exp.Count):
            return self._count_ref(node, mask=mask)

        raise _Unconvertible(f"unsupported aggregation {node.sql_name()!r}")

    def _count_ref(self, node: exp.Count, *, mask: exp.Expr | None) -> str:
        inner = node.this
        if inner is None or isinstance(inner, exp.Star):
            if mask is None:
                return functional_agg_text(source="*", suffix="count")
            return functional_agg_text(
                source=self._operand_ref(self._masked(exp.Literal.number(1), mask)), suffix="count")
        if isinstance(inner, exp.Distinct):
            exprs = inner.expressions
            if len(exprs) != 1:
                raise _Unconvertible("COUNT(DISTINCT ...) with multiple columns")
            return functional_agg_text(
                source=self._operand_ref(self._masked(exprs[0], mask)), suffix="count_distinct")
        return functional_agg_text(source=self._operand_ref(self._masked(inner, mask)), suffix="count")

    def _percentile_ref(self, node: exp.WithinGroup, *, mask: exp.Expr | None) -> str:
        inner = node.this
        if not isinstance(inner, (exp.PercentileCont, exp.PercentileDisc)):
            raise _Unconvertible("unsupported WITHIN GROUP aggregate")
        p_node = inner.this
        if not (isinstance(p_node, exp.Literal) and p_node.is_number):
            raise _Unconvertible("percentile fraction must be a numeric literal")
        p_val = float(p_node.name)
        if not 0.0 <= p_val <= 1.0:
            raise _Unconvertible("percentile fraction must be between 0 and 1")

        order = node.args.get("expression")
        if not (isinstance(order, exp.Order) and order.expressions):
            raise _Unconvertible("percentile WITHIN GROUP missing ORDER BY")
        ordered = order.expressions[0]
        col_node = ordered.this if isinstance(ordered, exp.Ordered) else ordered
        if not isinstance(col_node, exp.Column):
            raise _Unconvertible("percentile ORDER BY must be a bare column")
        ref = self._operand_ref(self._masked(col_node, mask))

        if self.percentile_unsupported:
            self.warnings.append(
                "percentile/median has no GROUP BY aggregate on the target dialect; "
                "the measure imports but fails at query time there."
            )
        if isinstance(inner, exp.PercentileCont) and math.isclose(p_val, 0.5):
            return functional_agg_text(source=ref, suffix="median")
        return functional_agg_text(source=ref, suffix=f"percentile(p={p_node.name})")

    # ---- residual validation ----

    @staticmethod
    def _is_sentinel(node: exp.Expr) -> bool:
        return isinstance(node, exp.Column) and node.name.startswith(_SENTINEL_PREFIX)

    def _validate_residual(self, root: exp.Expr) -> None:
        for node in root.walk():
            reason = self._residual_violation(node)
            if reason:
                raise _Unconvertible(reason)

    def _residual_violation(self, node: exp.Expr) -> str | None:
        """Return the clean-fail reason for a single residual node, or None."""
        if isinstance(node, (exp.AggFunc, exp.Window, exp.WithinGroup)):
            return "window/aggregate function not expressible"
        if isinstance(node, exp.Case):
            return "CASE outside an aggregate is not expressible"
        if isinstance(node, exp.Column) and not self._is_sentinel(node):
            return f"bare column {node.name!r} must appear inside an aggregation"
        if isinstance(node, exp.Literal) and node.is_string:
            return "string literal is not expressible in a measure"
        if isinstance(node, exp.Func) and not isinstance(node, exp.Count):
            if node.sql_name().lower() not in SCALAR_PASSTHROUGH:
                return f"function {node.sql_name()!r} is not allowed"
        return None

    # ---- top-level ----

    def convert(self, tree: exp.Expr) -> ExprResult:
        if tree.find(exp.Window):
            return self._fail("window functions are not expressible; use a transform")

        try:
            ref_by_token: dict[str, str] = {}
            new_root = tree
            for i, node in enumerate(outermost_aggregates(tree)):
                sentinel = exp.column(f"{_SENTINEL_PREFIX}{i}")
                ref_by_token[sentinel.name] = self._agg_ref(node)
                if node is new_root:
                    new_root = sentinel
                else:
                    node.replace(sentinel)

            new_root = self._strip_redundant_parens(new_root)
            self._validate_residual(new_root)
            rendered = new_root.sql(normalize_functions="lower")
            formula = self._substitute(rendered, ref_by_token)
        except _Unconvertible as exc:
            return self._fail(str(exc))

        return ExprResult(
            ok=True, formula=formula,
            materialized=self.materialized, warnings=self.warnings,
        )

    @staticmethod
    def _strip_redundant_parens(root: exp.Expr) -> exp.Expr:
        """Drop parentheses wrapping a single atom, so ``(SUM(a)) / (COUNT(*))`` renders as ``sum(a) / count(*)``."""
        # Collect first: the tree is mutated below.
        atom_parens = [
            p for p in root.find_all(exp.Paren)
            if isinstance(p.this, (exp.Column, exp.Literal))
        ]
        for paren in atom_parens:
            if paren is root:
                root = paren.this
            else:
                paren.replace(paren.this)
        return root

    @staticmethod
    def _substitute(rendered: str, ref_by_token: dict[str, str]) -> str:
        out = rendered
        for token, ref in ref_by_token.items():
            out = re.sub(rf"\b{re.escape(token)}\b", lambda _m, r=ref: r, out)
        return out

    def _fail(self, reason: str) -> ExprResult:
        return ExprResult(ok=False, formula=None, reason=reason,
                          materialized=[], warnings=self.warnings)


def convert_expression(
    expr: str | exp.Expr,
    *,
    entity_name: str,
    owner_of: OwnerOf,
    ref_of: RefOf,
    percentile_unsupported: bool = False,
    name_taken: NameTaken = lambda model, name: False,
    target: str | None = None,
) -> ExprResult:
    """Convert an OSI SQL aggregation expression into a SLayer formula.

    ``name_taken(owning_model, name)`` reserves hidden-column names; ``target``
    is the sqlglot dialect hidden-column SQL is rendered in.
    """
    converter = _Converter(
        entity_name=entity_name, owner_of=owner_of, ref_of=ref_of,
        percentile_unsupported=percentile_unsupported, name_taken=name_taken, target=target,
    )
    if isinstance(expr, str):
        try:
            tree = sqlglot.parse_one(expr)
        except SqlglotError as exc:
            return converter._fail(f"could not parse expression: {exc}")
    else:
        tree = expr.copy()
    return converter.convert(tree)
