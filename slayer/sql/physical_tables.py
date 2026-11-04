"""The physical tables a statement reads: one walk for the session policy and the cache."""

from __future__ import annotations

from sqlglot import exp
from sqlglot.expressions.core import Expr
from sqlglot.optimizer.scope import Scope, traverse_scope


def _generates_numbers(table: exp.Table) -> bool:
    """A table function over numeric literals only (the time spine's integer sequence): no stored rows."""
    call = table.this
    if not isinstance(call, exp.Func):
        return False
    literals = list(call.find_all(exp.Literal))
    return bool(literals) and all(not lit.is_string for lit in literals) and not any(call.find_all(exp.Column, exp.Table))


def physical_tables(statement: Expr) -> list[exp.Table]:
    """The physical ``exp.Table`` nodes of ``statement``, snapshotted before any mutation.

    A CTE / derived source and a number-series table function are skipped; a qualified
    reference is never a CTE.
    """
    physical = []
    for scope in traverse_scope(statement):
        for table in scope.tables:
            qualified = bool(table.db or table.catalog)
            if not qualified and isinstance(scope.sources.get(table.alias_or_name), Scope):
                continue
            if _generates_numbers(table):
                continue
            physical.append(table)
    return physical
