"""Forced-filter SQL rewrite for session-policy RLS (DEV-1578).

``apply_session_policy`` is a pure sqlglot transform. Given final SQL, it
wraps every *physical* table reference whose configured column(s) apply in a
filtered ``SELECT * ... WHERE`` subquery, preserving the original alias::

    FROM orders               -->  FROM (SELECT * FROM orders
                                         WHERE organization_uuid = '7ef3') AS orders

Why the final-SQL layer: base tables, joins, every CTE, sql-mode raw tables,
and query-backed stages all compile to physical-table ``FROM``s here, so one
code path scopes every model type. Wrapping the *table* (not appending to the
outer ``WHERE``) preserves ``LEFT JOIN`` NULL-extension semantics.

Physical-vs-CTE classification is scope-aware (sqlglot ``traverse_scope``): a
table reference that resolves to a CTE/derived scope is skipped; a physical
table that happens to share a CTE's name (e.g. inside that CTE's body) is
still wrapped. Values are always ``exp.convert`` literals (injection-safe).
"""

from __future__ import annotations

from typing import Callable, Optional

import sqlglot
from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from slayer.core.errors import ForcedFilterError
from slayer.core.policy import ColumnFilterRule, SessionPolicy

# Statement roots the rewrite is willing to operate on. Anything else
# (INSERT / UPDATE / DELETE / MERGE / DDL / Command …) fails closed — the
# forced filter must never silently pass an unrecognised statement through.
_ALLOWED_ROOTS = (exp.Select, exp.SetOperation)


class ScopedTable(BaseModel):
    """A physical table reference's identity, as parsed from the SQL.

    ``schema_name`` mirrors a single-dot/two-dot qualifier in the SQL
    (``public.orders`` -> ``schema_name="public"``); ``catalog`` mirrors a
    three-part name (``proj.dataset.tbl`` -> ``catalog="proj"``). The engine's
    column-presence probe resolves the effective schema as ``schema_name`` or
    the datasource default.
    """

    model_config = ConfigDict(frozen=True)

    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    name: str


# True (column present), False (table confirmed to lack it), or None (presence
# cannot be confirmed -> fail closed).
HasColumn = Callable[[ScopedTable, str], Optional[bool]]


def _rule_label(rule: ColumnFilterRule) -> str:
    return f"'{rule.name}'" if rule.name else f"on column '{rule.column}'"


def _scoped_table(table: exp.Table) -> ScopedTable:
    return ScopedTable(
        catalog=(table.catalog or None),
        schema_name=(table.db or None),
        name=table.name,
    )


def _build_predicate(rule: ColumnFilterRule) -> exp.Expression:
    col = exp.column(rule.column)
    value = rule.value
    if isinstance(value, tuple):
        return exp.In(
            this=col, expressions=[exp.convert(v) for v in value]
        )
    return exp.EQ(this=col, expression=exp.convert(value))


def _physical_tables(ast: exp.Expression) -> list:
    """Return the physical ``exp.Table`` nodes in ``ast`` (CTE/derived
    references excluded), snapshotted before any mutation."""
    physical = []
    for scope in traverse_scope(ast):
        for table in scope.tables:
            source = scope.sources.get(table.alias_or_name)
            if isinstance(source, Scope):
                continue  # resolves to a CTE / derived table — leave alone
            physical.append(table)
    return physical


def _wrap_table(table: exp.Table, predicates: list) -> None:
    """Replace ``table`` in place with ``(SELECT * FROM <table> WHERE ...) AS
    <original_alias>``."""
    alias = table.alias_or_name
    bare = table.copy()
    bare.set("alias", None)
    inner = exp.select("*").from_(bare)
    for predicate in predicates:
        inner = inner.where(predicate)  # chained .where AND-combines
    table.replace(
        exp.Subquery(
            this=inner, alias=exp.TableAlias(this=exp.to_identifier(alias))
        )
    )


def apply_session_policy(
    sql: str,
    *,
    dialect: str,
    policy: SessionPolicy,
    has_column: HasColumn,
) -> str:
    """Wrap every physical-table ref whose rule column(s) apply.

    ``has_column(scoped_table, column)`` returns ``True`` / ``False`` /
    ``None`` (cannot confirm). Raises :class:`ForcedFilterError` on a
    fail-closed condition (unconfirmable column, ``block`` on an absent
    column, or a non-SELECT statement root).
    """
    if not policy.data_filters:
        return sql  # zero-overhead: no parse, no introspection

    ast = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(ast, _ALLOWED_ROOTS):
        raise ForcedFilterError(
            "Forced filter: refusing to rewrite a non-SELECT statement "
            f"({type(ast).__name__}); failing closed."
        )

    for table in _physical_tables(ast):
        scoped = _scoped_table(table)
        predicates = []
        for rule in policy.data_filters:
            present = has_column(scoped, rule.column)
            if present is None:
                raise ForcedFilterError(
                    f"Forced filter rule {_rule_label(rule)}: could not "
                    f"confirm column '{rule.column}' on table "
                    f"'{scoped.name}'; failing closed.",
                    table=scoped.name,
                    column=rule.column,
                    rule_name=rule.name,
                )
            if present is False:
                if rule.on_unapplicable == "block":
                    raise ForcedFilterError(
                        f"Forced filter rule {_rule_label(rule)} requires "
                        f"column '{rule.column}' on table '{scoped.name}', "
                        f"which does not have it.",
                        table=scoped.name,
                        column=rule.column,
                        rule_name=rule.name,
                    )
                continue  # "pass": skip this rule for this table
            predicates.append(_build_predicate(rule))
        if predicates:
            _wrap_table(table, predicates)

    return ast.sql(dialect=dialect)
