"""Forced-filter SQL rewrite for session-policy RLS.

``apply_session_policy`` is a pure sqlglot transform wrapping every *physical* table
reference in the final SQL per the policy's ``ruleset``. A column ruleset filters each
table having the tenant column; a join ruleset filters the anchor directly and reaches
it from other tables via a correlated ``EXISTS`` (a null-guarded ``IN`` on dialects whose
correlated subqueries need a server setting)::

    FROM orders  -->  FROM (SELECT * FROM orders AS _rls_src
                            WHERE EXISTS (
                              SELECT 1 FROM customers AS _rls_j0
                              WHERE _rls_j0.id = _rls_src.customer_id
                                AND _rls_j0.organization_uuid = '7ef3'
                            )) AS orders

Rewriting at the final-SQL layer means base tables, joins, CTEs, sql-mode raw tables and
query-backed stages all funnel through one code path. Values are always ``exp.convert``
literals and identifiers are built structurally, so the rewrite is injection-safe.
"""

from __future__ import annotations

from typing import Callable, Optional

import sqlglot
from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.expressions.core import Expression
from sqlglot.optimizer.scope import Scope, traverse_scope

from slayer.core.errors import ForcedFilterError
from slayer.core.policy import (
    ColumnFilterRuleset,
    JoinFilterRule,
    JoinFilterRuleset,
    SessionPolicy,
    _validate_join_rule_anchor,
)
from slayer.sql.dialects import SqlDialect, get_dialect

# Any other statement root (INSERT / UPDATE / DDL / …) fails closed.
_ALLOWED_ROOTS = (exp.Select, exp.SetOperation)

# Internal aliases for the join-rule rewrite. Each lives in a fresh subquery
# scope per wrap, so they never collide with the outer query or a sibling wrap.
_RLS_SRC = "_rls_src"


def _hop_alias(i: int) -> str:
    return f"_rls_j{i}"


class ScopedTable(BaseModel):
    """A physical table reference's identity, as parsed from the SQL.

    ``schema_name`` and ``catalog`` mirror the qualifiers the SQL actually states, so
    both are ``None`` for a bare name; the engine's probe falls back to the datasource.
    """

    model_config = ConfigDict(frozen=True)

    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    name: str


# True (column present), False (table confirmed to lack it), or None (presence
# cannot be confirmed -> fail closed).
HasColumn = Callable[[ScopedTable, str], Optional[bool]]


def _scoped_table(table: exp.Table) -> ScopedTable:
    return ScopedTable(
        catalog=(table.catalog or None),
        schema_name=(table.db or None),
        name=table.name,
    )


def _build_predicate(
    column: str, value, *, table: Optional[str] = None
) -> Expression:
    """``column = value`` / ``column IN (...)``, optionally qualified by ``table``."""
    col = exp.column(column, table=table) if table else exp.column(column)
    if isinstance(value, tuple):
        return exp.In(this=col, expressions=[exp.convert(v) for v in value])
    return exp.EQ(this=col, expression=exp.convert(value))


def _physical_tables(ast: Expression) -> list:
    """The physical ``exp.Table`` nodes in ``ast``, snapshotted before any mutation."""
    physical = []
    for scope in traverse_scope(ast):
        for table in scope.tables:
            source = scope.sources.get(table.alias_or_name)
            if isinstance(source, Scope):
                continue  # resolves to a CTE / derived table — leave alone
            physical.append(table)
    return physical


def _target_matches(scoped: ScopedTable, target_table: str) -> bool:
    """Whether ``scoped`` is the table a policy entry names.

    A bare target matches the table in any schema; a qualified one matches only when
    every qualifier it states matches. Case-insensitive throughout.
    """
    parsed = exp.to_table(target_table)
    if scoped.name.casefold() != parsed.name.casefold():
        return False
    if parsed.db and (scoped.schema_name or "").casefold() != parsed.db.casefold():
        return False
    if parsed.catalog and (
        (scoped.catalog or "").casefold() != parsed.catalog.casefold()
    ):
        return False
    return True


def _wrap_table(table: exp.Table, predicates: list) -> None:
    """Replace ``table`` in place with ``(SELECT * FROM <table> WHERE ...) AS <alias>``."""
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


# ---------------------------------------------------------------------------
# ColumnFilterRuleset
# ---------------------------------------------------------------------------


def _apply_column_ruleset(
    ast: Expression, ruleset: ColumnFilterRuleset, has_column: HasColumn
) -> None:
    predicate = _build_predicate(ruleset.column, ruleset.value)
    for table in _physical_tables(ast):
        scoped = _scoped_table(table)
        present = has_column(scoped, ruleset.column)
        if present is None:
            raise ForcedFilterError(
                f"Forced filter on column '{ruleset.column}': could not confirm "
                f"the column on table '{scoped.name}'; failing closed.",
                table=scoped.name,
                column=ruleset.column,
            )
        if present is False:
            if ruleset.on_unapplicable == "block":
                raise ForcedFilterError(
                    f"Forced filter requires column '{ruleset.column}' on table "
                    f"'{scoped.name}', which does not have it.",
                    table=scoped.name,
                    column=ruleset.column,
                )
            continue  # "pass": leave this table unfiltered
        _wrap_table(table, [predicate])


# ---------------------------------------------------------------------------
# JoinFilterRuleset
# ---------------------------------------------------------------------------


def _validated_hops(rule: JoinFilterRule, *, ruleset: JoinFilterRuleset) -> tuple:
    """The rule's target-first hops, re-validated at the SQL boundary (fail closed)."""
    try:
        # Same helper construction uses, so a model_copy bypassing the ruleset
        # validator fails closed here rather than emitting a mis-scoped filter.
        return _validate_join_rule_anchor(rule, ruleset.table)
    except ValueError as exc:
        raise ForcedFilterError(
            f"Forced filter join path for '{rule.target_table}' is invalid "
            "(non-chaining, target not an endpoint, does not reach the anchor, "
            "or the anchor appears more than once); failing closed.",
            table=rule.target_table,
            column=ruleset.column,
        ) from exc


def _hop_chain(hops: tuple, projection: Expression, *, global_joins: bool) -> exp.Select:
    """``SELECT <projection> FROM <hop 0> [[GLOBAL] INNER JOIN <hop i> …]`` over the hops' aliases."""
    first_to = exp.to_table(hops[0].to_table)
    first_to.set("alias", exp.TableAlias(this=exp.to_identifier(_hop_alias(0))))
    inner = exp.select(projection).from_(first_to)
    for i in range(1, len(hops)):
        hop = hops[i]
        to_tbl = exp.to_table(hop.to_table)
        to_tbl.set("alias", exp.TableAlias(this=exp.to_identifier(_hop_alias(i))))
        on = exp.EQ(
            this=exp.column(hop.to_column, table=_hop_alias(i)),
            expression=exp.column(hop.from_column, table=_hop_alias(i - 1)),
        )
        inner = inner.join(exp.Join(this=to_tbl, on=on, kind="INNER", global_=global_joins or None))
    return inner


def _tenant_predicate(hops: tuple, ruleset: JoinFilterRuleset) -> Expression:
    """The tenant predicate, on the terminal hop — the anchor."""
    return _build_predicate(ruleset.column, ruleset.value, table=_hop_alias(len(hops) - 1))


def _build_exists(rule: JoinFilterRule, *, ruleset: JoinFilterRuleset) -> exp.Exists:
    """Correlated ``EXISTS``: hop 0 correlates back to the wrapper's ``_rls_src``."""
    hops = _validated_hops(rule, ruleset=ruleset)
    correlation = exp.EQ(
        this=exp.column(hops[0].to_column, table=_hop_alias(0)),
        expression=exp.column(hops[0].from_column, table=_RLS_SRC),
    )
    inner = _hop_chain(hops, exp.Literal.number(1), global_joins=False).where(correlation)
    return exp.Exists(this=inner.where(_tenant_predicate(hops, ruleset)))


def _build_in(
    rule: JoinFilterRule, *, ruleset: JoinFilterRuleset, sql_dialect: SqlDialect
) -> exp.In:
    """Non-correlated ``_rls_src.<from0> IN (SELECT <to0> …)``; the NOT NULL guard keeps
    it equal to the ``EXISTS`` form whatever the session's NULL-in-``IN`` semantics."""
    hops = _validated_hops(rule, ruleset=ruleset)
    key = exp.column(hops[0].to_column, table=_hop_alias(0))
    global_ = sql_dialect.global_in_subqueries
    inner = _hop_chain(hops, sql_dialect.in_subquery_key(key.copy()), global_joins=global_).where(
        exp.Not(this=exp.Is(this=key.copy(), expression=exp.Null()))
    )
    return exp.In(
        this=exp.column(hops[0].from_column, table=_RLS_SRC),
        query=exp.Subquery(this=inner.where(_tenant_predicate(hops, ruleset))),
        is_global=global_ or None,
    )


def _wrap_table_join_rules(
    table: exp.Table, rules: list, *, ruleset: JoinFilterRuleset, sql_dialect: SqlDialect
) -> None:
    """Replace ``table`` in place with one AND-combined semi-join per targeting rule."""
    alias = table.alias_or_name
    bare = table.copy()
    bare.set("alias", exp.TableAlias(this=exp.to_identifier(_RLS_SRC)))
    inner = exp.select("*").from_(bare)
    for rule in rules:
        semi_join = (
            _build_in(rule, ruleset=ruleset, sql_dialect=sql_dialect)
            if sql_dialect.correlated_subqueries_gated
            else _build_exists(rule, ruleset=ruleset)
        )
        inner = inner.where(semi_join)
    table.replace(
        exp.Subquery(
            this=inner, alias=exp.TableAlias(this=exp.to_identifier(alias))
        )
    )


def _apply_join_ruleset(
    ast: Expression, ruleset: JoinFilterRuleset, *, sql_dialect: SqlDialect
) -> None:
    """Structurally scope every physical table; never probes column presence."""
    for table in _physical_tables(ast):
        scoped = _scoped_table(table)
        if _target_matches(scoped, ruleset.table):
            _wrap_table(table, [_build_predicate(ruleset.column, ruleset.value)])
            continue
        targeting = [
            r for r in ruleset.joins if _target_matches(scoped, r.target_table)
        ]
        if targeting:
            _wrap_table_join_rules(table, targeting, ruleset=ruleset, sql_dialect=sql_dialect)
            continue
        if any(_target_matches(scoped, w) for w in ruleset.whitelist):
            continue  # whitelisted: emitted unfiltered
        raise ForcedFilterError(
            f"Forced filter: table '{scoped.name}' is not covered by the policy "
            "(not the anchor, not a join target, not whitelisted); failing "
            "closed.",
            table=scoped.name,
        )


def apply_session_policy(
    sql: str,
    *,
    dialect: str,
    policy: SessionPolicy,
    has_column: HasColumn,
) -> str:
    """Wrap every physical-table ref per the policy's ``ruleset``.

    ``has_column`` returns ``True``/``False``/``None`` (cannot confirm) and is consulted
    only for a ``ColumnFilterRuleset``. Raises :class:`ForcedFilterError` on any
    fail-closed condition.
    """
    ruleset = policy.ruleset

    ast = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(ast, _ALLOWED_ROOTS):
        raise ForcedFilterError(
            "Forced filter: refusing to rewrite a non-SELECT statement "
            f"({type(ast).__name__}); failing closed."
        )

    if isinstance(ruleset, ColumnFilterRuleset):
        _apply_column_ruleset(ast, ruleset, has_column)
    else:  # JoinFilterRuleset
        _apply_join_ruleset(ast, ruleset, sql_dialect=get_dialect(dialect))
    return ast.sql(dialect=dialect)
