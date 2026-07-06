"""OSI metric/field SQL expression -> SLayer formula transform (DEV-1643).

An OSI metric carries a raw SQL aggregation expression (e.g. ``SUM(amount)``,
``(SUM(a)) / (COUNT(*))``, ``SUM(quantity * amount)``). SLayer measures use
colon syntax (``amount:sum``, ``*:count``) with arithmetic over aggregated refs.

``convert_expression`` walks the sqlglot AST, replaces each *outermost*
aggregate subtree with a sentinel, lets sqlglot render the surrounding
arithmetic / scalar-function structure, then substitutes the SLayer ref for each
sentinel. Non-bare aggregate operands (arithmetic / scalar / CASE) are
materialized as hidden derived Columns on the operand's owning model. Anything
inexpressible is clean-failed (``ok=False``) with a reason.

Model-awareness is injected via two callbacks so the transform is unit-testable
in isolation:
- ``owner_of(qualifier, column) -> model | None`` — which dataset owns a column.
- ``ref_of(model, column) -> anchor-relative dotted ref | None`` — the ref to
  emit for a column on ``model`` (``None`` = unreachable from the anchor).
"""

from __future__ import annotations

import math
import re
from typing import Callable, Optional

import sqlglot
import sqlglot.expressions as exp
from pydantic import BaseModel

from slayer.core.formula import SCALAR_PASSTHROUGH

# Dialects whose expressions are SQL and can be fed to sqlglot / Column.sql.
SQL_DIALECTS = frozenset({"ANSI_SQL", "SNOWFLAKE", "DATABRICKS"})

_SENTINEL_PREFIX = "SLAYERTOKEN"
_SIMPLE_AGG = {exp.Sum: "sum", exp.Avg: "avg", exp.Min: "min", exp.Max: "max"}

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


class _Unconvertible(Exception):
    """Internal control-flow signal carrying a clean-fail reason."""


class _Converter:
    def __init__(self, entity_name: str, owner_of: OwnerOf, ref_of: RefOf,
                 percentile_unsupported: bool, name_taken: NameTaken) -> None:
        self.entity_name = entity_name
        self.owner_of = owner_of
        self.ref_of = ref_of
        self.percentile_unsupported = percentile_unsupported
        self.name_taken = name_taken
        self.materialized: list[MaterializedColumn] = []
        self.warnings: list[str] = []
        self._dedup: dict[tuple[str, str], str] = {}
        self._counter = 0

    # ---- ref building for a single aggregate ----

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

    def _materialize(self, operand: exp.Expression) -> str:
        """Create/reuse a hidden derived column for a non-bare operand; return
        its anchor-relative ref."""
        if operand.find(exp.AggFunc, exp.Window, exp.WithinGroup):
            raise _Unconvertible("nested aggregate in aggregate operand")
        columns = list(operand.find_all(exp.Column))
        resolved = [self.owner_of(c.table or None, c.name) for c in columns]
        # An unresolved column (unknown, or ambiguous) must fail the whole
        # operand — discarding it would materialize SQL referencing a column
        # that does not exist.
        if not columns or any(owner is None for owner in resolved):
            raise _Unconvertible(
                "aggregate operand references a column that cannot be resolved"
            )
        owners = set(resolved)
        if len(owners) != 1:
            raise _Unconvertible("aggregate operand spans multiple datasets")
        owner = owners.pop()
        operand_sql = operand.sql()
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
        """A hidden-column name that collides with no existing column on
        ``owner`` (the formula references this name verbatim, so a collision
        would silently aggregate the wrong column)."""
        while True:
            name = f"_{self.entity_name}_{self._counter}"
            self._counter += 1
            if not self.name_taken(owner, name):
                return name

    def _operand_ref(self, operand: exp.Expression) -> str:
        if isinstance(operand, exp.Column):
            return self._column_ref(operand.table or None, operand.name)
        return self._materialize(operand)

    def _agg_ref(self, node: exp.Expression) -> str:
        """Return the ``ref:agg`` token for one outermost aggregate node."""
        # PERCENTILE_CONT/DISC(...) WITHIN GROUP (ORDER BY col)
        if isinstance(node, exp.WithinGroup):
            return self._percentile_ref(node)

        if type(node) in _SIMPLE_AGG:
            agg = _SIMPLE_AGG[type(node)]
            return f"{self._operand_ref(node.this)}:{agg}"

        if isinstance(node, exp.Count):
            inner = node.this
            if inner is None or isinstance(inner, exp.Star):
                return "*:count"
            if isinstance(inner, exp.Distinct):
                exprs = inner.expressions
                if len(exprs) != 1:
                    raise _Unconvertible("COUNT(DISTINCT ...) with multiple columns")
                return f"{self._operand_ref(exprs[0])}:count_distinct"
            return f"{self._operand_ref(inner)}:count"

        raise _Unconvertible(f"unsupported aggregation {node.sql_name()!r}")

    def _percentile_ref(self, node: exp.WithinGroup) -> str:
        inner = node.this
        if not isinstance(inner, (exp.PercentileCont, exp.PercentileDisc)):
            raise _Unconvertible("unsupported WITHIN GROUP aggregate")
        p_node = inner.this
        if not (isinstance(p_node, exp.Literal) and p_node.is_number):
            raise _Unconvertible("percentile fraction must be a numeric literal")
        try:
            p_val = float(p_node.name)
        except ValueError as exc:  # pragma: no cover - defensive
            raise _Unconvertible("percentile fraction is not numeric") from exc
        if not 0.0 <= p_val <= 1.0:
            raise _Unconvertible("percentile fraction must be between 0 and 1")

        order = node.args.get("expression")
        if not (isinstance(order, exp.Order) and order.expressions):
            raise _Unconvertible("percentile WITHIN GROUP missing ORDER BY")
        ordered = order.expressions[0]
        col_node = ordered.this if isinstance(ordered, exp.Ordered) else ordered
        if not isinstance(col_node, exp.Column):
            raise _Unconvertible("percentile ORDER BY must be a bare column")
        ref = self._column_ref(col_node.table or None, col_node.name)

        if self.percentile_unsupported:
            self.warnings.append(
                "percentile/median has no GROUP BY aggregate on the target dialect; "
                "the measure imports but fails at query time there."
            )
        if isinstance(inner, exp.PercentileCont) and math.isclose(p_val, 0.5):
            return f"{ref}:median"
        return f"{ref}:percentile(p={p_node.name})"

    # ---- residual validation ----

    @staticmethod
    def _is_sentinel(node: exp.Expression) -> bool:
        return isinstance(node, exp.Column) and node.name.startswith(_SENTINEL_PREFIX)

    def _validate_residual(self, root: exp.Expression) -> None:
        for node in root.walk():
            reason = self._residual_violation(node)
            if reason:
                raise _Unconvertible(reason)

    def _residual_violation(self, node: exp.Expression) -> str | None:
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

    def convert(self, expr: str) -> ExprResult:
        try:
            tree = sqlglot.parse_one(expr)
        except sqlglot.errors.ParseError as exc:
            return self._fail(f"could not parse expression: {exc}")

        if tree.find(exp.Window):
            return self._fail("window functions are not expressible; use a transform")

        try:
            outermost = self._find_outermost_aggregates(tree)
            replacements: list[tuple[exp.Expression, exp.Column]] = []
            for i, node in enumerate(outermost):
                ref = self._agg_ref(node)
                sentinel = exp.column(f"{_SENTINEL_PREFIX}{i}")
                sentinel.meta["slayer_ref"] = ref
                replacements.append((node, sentinel))

            new_root = tree
            ref_by_token: dict[str, str] = {}
            for node, sentinel in replacements:
                ref_by_token[sentinel.name] = sentinel.meta["slayer_ref"]
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
    def _find_outermost_aggregates(tree: exp.Expression) -> list[exp.Expression]:
        agg_types = (exp.AggFunc, exp.WithinGroup)
        result: list[exp.Expression] = []
        for node in tree.find_all(*agg_types):
            ancestor = node.parent
            nested = False
            while ancestor is not None:
                if isinstance(ancestor, agg_types):
                    nested = True
                    break
                ancestor = ancestor.parent
            if not nested:
                result.append(node)
        return result

    @staticmethod
    def _strip_redundant_parens(root: exp.Expression) -> exp.Expression:
        """Drop parentheses that wrap a single atom (a sentinel ref or literal),
        so ``(SUM(a)) / (COUNT(*))`` renders as ``a:sum / *:count``."""
        # Materialize the matches first (a list comprehension, not list(gen)) —
        # the tree is mutated in place below, so we can't iterate it lazily.
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
            out = re.sub(rf"\b{re.escape(token)}\b", ref, out)
        return out

    def _fail(self, reason: str) -> ExprResult:
        return ExprResult(ok=False, formula=None, reason=reason,
                          materialized=[], warnings=self.warnings)


def convert_expression(
    expr: str,
    *,
    entity_name: str,
    owner_of: OwnerOf,
    ref_of: RefOf,
    percentile_unsupported: bool = False,
    name_taken: NameTaken = lambda model, name: False,
) -> ExprResult:
    """Convert an OSI SQL aggregation expression into a SLayer formula.

    ``name_taken(owning_model, name)`` lets the caller reserve hidden
    derived-column names against existing columns so a materialized operand
    never collides with a real column.
    """
    return _Converter(
        entity_name=entity_name, owner_of=owner_of, ref_of=ref_of,
        percentile_unsupported=percentile_unsupported, name_taken=name_taken,
    ).convert(expr)
