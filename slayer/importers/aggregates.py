"""Source SQL aggregation expression -> SLayer measure formula, shared by the importers.

``convert_expression`` replaces each outermost aggregate (``FILTER`` included)
with a sentinel, renders the surrounding arithmetic with sqlglot, and
substitutes each aggregate's formula text. How an aggregate's operand becomes a
formula reference is the importer's ``OperandPolicy``: OSI writes it inline in
formula syntax (``InlineOperands``), Cube reads it through a Mode-A column.
Anything inexpressible is clean-failed (``ok=False``) with a reason.
"""

from __future__ import annotations

import math
import re
from typing import Callable, Optional

import sqlglot
from pydantic import BaseModel
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.keys import SCALAR_PASSTHROUGH, check_scalar_arity
from slayer.core.refs import functional_agg_text
from slayer.core.syntax import parse_expr

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
_LIST_LENGTH_NAMES = frozenset({"cardinality", "array_length"})

OwnerOf = Callable[[Optional[str], str], Optional[str]]
RefOf = Callable[[str, str], Optional[str]]
NameTaken = Callable[[str, str], bool]  # (owning_model, name) -> already exists?
IsList = Callable[[Optional[str], str], bool]  # (qualifier, column) -> list-valued?


class MaterializedColumn(BaseModel):
    """A hidden helper Column the converter must create on ``owning_model``."""

    owning_model: str
    name: str
    sql: str
    filter: str | None = None
    agg: str | None = None


class ExprResult(BaseModel):
    """Outcome of converting one source expression to a SLayer formula."""

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


class Unconvertible(Exception):
    """Control-flow signal carrying a clean-fail reason."""


class _NotInline(Exception):
    """The operand has no formula-syntax rendering."""


def sql_string(value: str) -> str:
    """A single-quoted string literal, ``'`` doubled."""
    return "'" + value.replace("'", "''") + "'"


def contains_args(
    node: exp.ArrayContains, *, is_list: Callable[[exp.Expr], bool],
) -> tuple[Expression, Expression]:
    """``(list, value)`` of an ``ARRAY_CONTAINS``, whichever order the dialect wrote them in."""
    first, second = node.this, node.expression
    if is_list(second) or (isinstance(first, exp.Literal) and not is_list(first)):
        return second, first
    return first, second


class OperandPolicy:
    """How an importer turns an aggregate's operand into a formula reference."""

    #: Whether ``CASE`` and string literals may appear between aggregates (the formula grammar reads them).
    sql_residual = False

    def __init__(self) -> None:
        self.materialized: list[MaterializedColumn] = []

    def operand(self, operand: exp.Expr, *, mask: exp.Expr | None, agg: str) -> str:
        """The reference ``agg`` aggregates: ``operand`` over the rows ``mask`` keeps."""
        raise NotImplementedError

    def is_list(self, operand: exp.Expr) -> bool:  # NOSONAR(S1172) — hook default
        """Whether ``operand`` is a list-valued column."""
        return False

    def residual(self, node: exp.Column) -> str | None:  # NOSONAR(S1172) — hook default
        """The formula text of a name allowed outside an aggregate (a measure), else ``None``."""
        return None


def _parens(text: str, node: exp.Expr) -> str:
    return f"({text})" if isinstance(node, (exp.Binary, exp.Not, exp.Neg, exp.Is)) else text


class InlineOperands(OperandPolicy):
    """OSI: operands written inline in formula syntax, else a hidden column on their single owning dataset."""

    def __init__(self, *, entity_name: str, owner_of: OwnerOf, ref_of: RefOf, name_taken: NameTaken,
                 target: str | None, is_list: IsList) -> None:
        super().__init__()
        self.entity_name = entity_name
        self.owner_of = owner_of
        self.ref_of = ref_of
        self.name_taken = name_taken
        self.target = target
        self._is_list = is_list
        self._dedup: dict[tuple[str, str], str] = {}
        self._counter = 0

    def is_list(self, operand: exp.Expr) -> bool:
        return isinstance(operand, exp.Column) and self._is_list(operand.table or None, operand.name)

    def operand(self, operand: exp.Expr, *, mask: exp.Expr | None, agg: str) -> str:
        if mask is not None:
            if len(self._owners(mask) | self._owners(operand)) > 1:
                raise Unconvertible("FILTER predicate reads a dataset other than the aggregate's")
            operand = exp.Case(ifs=[exp.If(this=mask.copy(), true=operand.copy())])
        if isinstance(operand, exp.Column):
            return self._column_ref(operand.table or None, operand.name)
        try:
            text = self._inline(operand)
            parse_expr(f"count({text})")
        except (_NotInline, ValueError, SyntaxError):
            return self._materialize(operand)
        return text

    def _column_ref(self, qualifier: Optional[str], column: str) -> str:
        owner = self.owner_of(qualifier or None, column)
        if owner is None:
            raise Unconvertible(f"cannot resolve owning dataset for column {column!r}")
        ref = self.ref_of(owner, column)
        if ref is None:
            raise Unconvertible(f"column {column!r} on model {owner!r} is not reachable from the anchor")
        return ref

    def _owners(self, node: exp.Expr) -> set[str]:
        """Owning datasets of every column in ``node``; an unresolvable column fails."""
        owners = {self.owner_of(c.table or None, c.name) for c in node.find_all(exp.Column)}
        if None in owners:
            raise Unconvertible("expression references a column that cannot be resolved")
        return {o for o in owners if o is not None}

    def _materialize(self, operand: exp.Expr) -> str:
        """Create/reuse a hidden derived column for ``operand``; return its anchor-relative ref."""
        if operand.find(exp.AggFunc, exp.Window, exp.WithinGroup):
            raise Unconvertible("nested aggregate in aggregate operand")
        owners = self._owners(operand)
        if not owners:
            raise Unconvertible("aggregate operand references no column")
        if len(owners) != 1:
            raise Unconvertible("aggregate operand spans multiple datasets and is not expressible inline")
        owner = owners.pop()
        operand_sql = operand.sql(dialect=self.target)
        name = self._dedup.get((owner, operand_sql))
        if name is None:
            name = self._fresh_column_name(owner)
            self._dedup[(owner, operand_sql)] = name
            self.materialized.append(MaterializedColumn(owning_model=owner, name=name, sql=operand_sql))
        ref = self.ref_of(owner, name)
        if ref is None:
            raise Unconvertible("materialized operand is not reachable from the anchor")
        return ref

    def _fresh_column_name(self, owner: str) -> str:
        """A hidden-column name that collides with no existing name on ``owner``."""
        while True:
            name = f"_{self.entity_name}_{self._counter}"
            self._counter += 1
            if not self.name_taken(owner, name):
                return name

    # ---- SQL AST -> formula syntax ----

    def _inline(self, node: exp.Expr) -> str:  # NOSONAR(S3776) — one flat dispatch over SQL node kinds
        if isinstance(node, exp.Column):
            return self._column_ref(node.table or None, node.name)
        if isinstance(node, exp.Literal):
            return sql_string(node.this) if node.is_string else node.this
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
        if isinstance(node, exp.EQ) and isinstance(node.expression, exp.Any):
            return f"contains({self._inline(node.expression.this)}, {self._inline(node.this)})"
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
        if isinstance(node, exp.ArrayContains):
            values, value = contains_args(node, is_list=self.is_list)
            return f"contains({self._inline(values)}, {self._inline(value)})"
        name = (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()
        if isinstance(node, exp.ArraySize) or name in _LIST_LENGTH_NAMES:
            (values,) = node.expressions if isinstance(node, exp.Anonymous) else [node.this]
            return f"length({self._inline(values)})"
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


class _Translator:
    def __init__(self, policy: OperandPolicy, percentile_unsupported: bool) -> None:
        self.policy = policy
        self.percentile_unsupported = percentile_unsupported
        self.warnings: list[str] = []

    # ---- one outermost aggregate ----

    def _agg_ref(self, node: exp.Expr) -> str:
        """The ``agg(ref)`` text for one outermost aggregate node."""
        if not isinstance(node, exp.Filter):
            return self._plain_agg_ref(node, mask=None)
        where = node.expression
        predicate = where.this if isinstance(where, exp.Where) else where
        if predicate.find(exp.AggFunc, exp.Window, exp.WithinGroup, exp.Subquery, exp.Select):
            raise Unconvertible("FILTER predicate must be row-level (no aggregate, window or subquery)")
        return self._plain_agg_ref(node.this, mask=predicate)

    def _refuse_list(self, operand: exp.Expr, agg: str) -> None:
        if self.policy.is_list(operand):
            raise Unconvertible(f"{agg} over the list field {operand.sql()!r} is not supported; only COUNT counts "
                                f"a list's rows")

    def _plain_agg_ref(self, node: exp.Expr, *, mask: exp.Expr | None) -> str:
        if isinstance(node, exp.WithinGroup):
            return self._percentile_ref(node, mask=mask)
        if type(node) in _SIMPLE_AGG:
            agg = _SIMPLE_AGG[type(node)]
            # SLayer supports DISTINCT only for COUNT.
            if isinstance(node.this, exp.Distinct):
                raise Unconvertible(f"{agg.upper()}(DISTINCT ...) is not supported")
            self._refuse_list(node.this, agg.upper())
            return functional_agg_text(source=self.policy.operand(node.this, mask=mask, agg=agg), suffix=agg)
        if isinstance(node, exp.Count):
            return self._count_ref(node, mask=mask)
        if isinstance(node, exp.ArrayAgg):
            return self._collect_ref(node, mask=mask)
        name = node.sql_name() if isinstance(node, exp.Func) else node.key
        raise Unconvertible(f"unsupported aggregation {name!r}")

    def _count_ref(self, node: exp.Count, *, mask: exp.Expr | None) -> str:
        inner = node.this
        if inner is None or isinstance(inner, exp.Star):
            if mask is None:
                return functional_agg_text(source="*", suffix="count")
            return functional_agg_text(
                source=self.policy.operand(exp.Literal.number(1), mask=mask, agg="count"), suffix="count")
        if isinstance(inner, exp.Distinct):
            exprs = inner.expressions
            if len(exprs) != 1:
                raise Unconvertible("COUNT(DISTINCT ...) with multiple columns")
            self._refuse_list(exprs[0], "COUNT(DISTINCT)")
            return functional_agg_text(
                source=self.policy.operand(exprs[0], mask=mask, agg="count_distinct"), suffix="count_distinct")
        return functional_agg_text(source=self.policy.operand(inner, mask=mask, agg="count"), suffix="count")

    def _collect_ref(self, node: exp.ArrayAgg, *, mask: exp.Expr | None) -> str:
        inner = node.this
        if not (isinstance(inner, exp.Distinct) and len(inner.expressions) == 1):
            raise Unconvertible("ARRAY_AGG keeps duplicates and order; only ARRAY_AGG(DISTINCT x) maps (to collect)")
        self._refuse_list(inner.expressions[0], "ARRAY_AGG")
        return functional_agg_text(
            source=self.policy.operand(inner.expressions[0], mask=mask, agg="collect"), suffix="collect")

    def _percentile_ref(self, node: exp.WithinGroup, *, mask: exp.Expr | None) -> str:
        inner = node.this
        if not isinstance(inner, (exp.PercentileCont, exp.PercentileDisc)):
            raise Unconvertible("unsupported WITHIN GROUP aggregate")
        p_node = inner.this
        if not (isinstance(p_node, exp.Literal) and p_node.is_number):
            raise Unconvertible("percentile fraction must be a numeric literal")
        p_val = float(p_node.name)
        if not 0.0 <= p_val <= 1.0:
            raise Unconvertible("percentile fraction must be between 0 and 1")

        order = node.args.get("expression")
        if not (isinstance(order, exp.Order) and order.expressions):
            raise Unconvertible("percentile WITHIN GROUP missing ORDER BY")
        ordered = order.expressions[0]
        col_node = ordered.this if isinstance(ordered, exp.Ordered) else ordered
        if not isinstance(col_node, exp.Column):
            raise Unconvertible("percentile ORDER BY must be a bare column")
        self._refuse_list(col_node, "PERCENTILE")
        agg = "median" if isinstance(inner, exp.PercentileCont) and math.isclose(p_val, 0.5) else "percentile"
        ref = self.policy.operand(col_node, mask=mask, agg=agg)

        if self.percentile_unsupported:
            self.warnings.append(
                "percentile/median has no GROUP BY aggregate on the target dialect; "
                "the measure imports but fails at query time there."
            )
        return functional_agg_text(source=ref, suffix="median" if agg == "median" else f"percentile(p={p_node.name})")

    # ---- residual validation ----

    @staticmethod
    def _is_sentinel(node: exp.Expr) -> bool:
        return isinstance(node, exp.Column) and node.name.startswith(_SENTINEL_PREFIX)

    def _residual_violation(self, node: exp.Expr) -> str | None:
        """The clean-fail reason for a single residual node, or None."""
        if isinstance(node, (exp.AggFunc, exp.Window, exp.WithinGroup)):
            return "window/aggregate function not expressible"
        if isinstance(node, exp.Case) and not self.policy.sql_residual:
            return "CASE outside an aggregate is not expressible"
        if isinstance(node, exp.Column) and not self._is_sentinel(node):
            return f"bare column {node.name!r} must appear inside an aggregation"
        if isinstance(node, exp.Literal) and node.is_string and not self.policy.sql_residual:
            return "string literal is not expressible in a measure"
        if isinstance(node, exp.Func) and not isinstance(node, (exp.Count, exp.Case, exp.If)):
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
            for node in [*outermost_aggregates(tree), *self._residual_names(tree)]:
                sentinel = exp.column(f"{_SENTINEL_PREFIX}{len(ref_by_token)}")
                ref_by_token[sentinel.name] = (self._agg_ref(node) if not isinstance(node, exp.Column)
                                               else self.policy.residual(node) or "")
                if node is new_root:
                    new_root = sentinel
                else:
                    node.replace(sentinel)

            new_root = self._strip_redundant_parens(new_root)
            for node in new_root.walk():
                reason = self._residual_violation(node)
                if reason:
                    raise Unconvertible(reason)
            formula = self._substitute(new_root.sql(normalize_functions="lower"), ref_by_token)
        except Unconvertible as exc:
            return self._fail(str(exc))
        return ExprResult(ok=True, formula=formula, materialized=self.policy.materialized, warnings=self.warnings)

    def _residual_names(self, tree: exp.Expr) -> list[exp.Column]:
        """Columns outside every aggregate that the policy reads as measures."""
        inside = {id(c) for agg in outermost_aggregates(tree) for c in agg.find_all(exp.Column)}
        return [c for c in tree.find_all(exp.Column)
                if id(c) not in inside and self.policy.residual(c) is not None]

    @staticmethod
    def _strip_redundant_parens(root: exp.Expr) -> exp.Expr:
        """Drop parentheses wrapping a single atom, so ``(SUM(a)) / (COUNT(*))`` renders as ``sum(a) / count(*)``."""
        atom_parens = [p for p in root.find_all(exp.Paren) if isinstance(p.this, (exp.Column, exp.Literal))]
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
        return ExprResult(ok=False, formula=None, reason=reason, materialized=[], warnings=self.warnings)


def convert_aggregates(
    expr: str | exp.Expr, *, policy: OperandPolicy, percentile_unsupported: bool = False,
    dialect: str | None = None,
) -> ExprResult:
    """Convert a source SQL aggregation expression into a SLayer formula under ``policy``."""
    translator = _Translator(policy=policy, percentile_unsupported=percentile_unsupported)
    if isinstance(expr, str):
        try:
            tree = sqlglot.parse_one(expr, read=dialect)
        except SqlglotError as exc:
            return translator._fail(f"could not parse expression: {exc}")
    else:
        tree = expr.copy()
    return translator.convert(tree)


def convert_expression(
    expr: str | exp.Expr,
    *,
    entity_name: str,
    owner_of: OwnerOf,
    ref_of: RefOf,
    percentile_unsupported: bool = False,
    name_taken: NameTaken = lambda model, name: False,
    target: str | None = None,
    is_list: IsList = lambda qualifier, column: False,
) -> ExprResult:
    """Convert an OSI SQL aggregation expression into a SLayer formula, operands inline.

    ``name_taken(owning_model, name)`` reserves hidden-column names; ``target``
    is the sqlglot dialect hidden-column SQL is rendered in.
    """
    policy = InlineOperands(entity_name=entity_name, owner_of=owner_of, ref_of=ref_of, name_taken=name_taken,
                            target=target, is_list=is_list)
    return convert_aggregates(expr, policy=policy, percentile_unsupported=percentile_unsupported)
