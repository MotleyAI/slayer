"""Forced-filter SQL rewrite for session-policy RLS (DEV-1578 / DEV-1627).

``apply_session_policy`` is a pure sqlglot transform. Given final SQL, it
wraps every *physical* table reference whose configured rule(s) apply.

**Column rules** (DEV-1578) wrap the table in a filtered ``SELECT *`` subquery,
preserving the original alias::

    FROM orders               -->  FROM (SELECT * FROM orders
                                         WHERE organization_uuid = '7ef3') AS orders

**Join rules** (DEV-1627) scope a table that lacks the tenant column via a
correlated ``EXISTS`` semi-join along an explicit, policy-authored join path
(cardinality-safe, ``LEFT JOIN``-preserving)::

    FROM orders  -->  FROM (SELECT * FROM orders AS _rls_src
                            WHERE EXISTS (
                              SELECT 1 FROM customers AS _rls_j0
                              WHERE _rls_j0.id = _rls_src.customer_id
                                AND _rls_j0.organization_uuid = '7ef3'
                            )) AS orders

Composition is **override**: if any join rule targets a table, that table is
scoped only by its join rule(s) (column rules do not touch it, and its column
presence is never probed). Otherwise the table falls under the column rules.

Why the final-SQL layer: base tables, joins, every CTE, sql-mode raw tables,
and query-backed stages all compile to physical-table ``FROM``s here, so one
code path scopes every model type. Physical-vs-CTE classification is
scope-aware (sqlglot ``traverse_scope``). Values are always ``exp.convert``
literals and identifiers are built structurally (injection-safe).
"""

from __future__ import annotations

from typing import Callable, Optional

import sqlglot
from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from slayer.core.errors import ForcedFilterError
from slayer.core.policy import (
    ColumnFilterRule,
    JoinFilterRule,
    SessionPolicy,
)

# Statement roots the rewrite is willing to operate on. Anything else
# (INSERT / UPDATE / DELETE / MERGE / DDL / Command …) fails closed — the
# forced filter must never silently pass an unrecognised statement through.
_ALLOWED_ROOTS = (exp.Select, exp.SetOperation)

# Deterministic internal aliases for the correlated-EXISTS rewrite. The inner
# base table (the wrapped physical table) is ``_rls_src``; each join-path hop
# target gets ``_rls_j{i}``. These live inside a fresh subquery scope per wrap,
# so they never collide with the outer query or with a sibling wrap.
_RLS_SRC = "_rls_src"


def _hop_alias(i: int) -> str:
    return f"_rls_j{i}"


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


def _target_matches(scoped: ScopedTable, target_table: str) -> bool:
    """Whether ``scoped`` is the table a ``JoinFilterRule`` targets.

    A bare target (``orders``) matches the table in any schema; a qualified
    target (``public.orders`` / ``proj.dataset.orders``) matches only when the
    parsed schema (and catalog, if given) match. Case-insensitive throughout.
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


def _predicates_for_table(
    *,
    scoped: ScopedTable,
    column_rules: list,
    has_column: HasColumn,
) -> list:
    """Return the predicates that apply to ``scoped`` (one per column rule
    whose column the table has). Raises :class:`ForcedFilterError` on a
    fail-closed condition (unconfirmable column, or ``block`` on a confirmed-
    absent column). A ``pass`` rule whose column is absent contributes
    nothing."""
    predicates = []
    for rule in column_rules:
        present = has_column(scoped, rule.column)
        if present is None:
            raise ForcedFilterError(
                f"Forced filter rule {_rule_label(rule)}: could not confirm "
                f"column '{rule.column}' on table '{scoped.name}'; failing "
                f"closed.",
                table=scoped.name,
                column=rule.column,
                rule_name=rule.name,
            )
        if present is False:
            if rule.on_unapplicable == "block":
                raise ForcedFilterError(
                    f"Forced filter rule {_rule_label(rule)} requires column "
                    f"'{rule.column}' on table '{scoped.name}', which does not "
                    f"have it.",
                    table=scoped.name,
                    column=rule.column,
                    rule_name=rule.name,
                )
            continue  # "pass": skip this rule for this table
        predicates.append(_build_predicate(rule))
    return predicates


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


def _terminal_predicate(rule: JoinFilterRule, *, table_alias: str) -> exp.Expression:
    """The tenant predicate on the last hop's ``to_table`` alias — scalar ``=``
    or ``IN``, always emitted, values via ``exp.convert`` (injection-safe)."""
    col = exp.column(rule.column, table=table_alias)
    value = rule.value
    if isinstance(value, tuple):
        return exp.In(this=col, expressions=[exp.convert(v) for v in value])
    return exp.EQ(this=col, expression=exp.convert(value))


def _build_exists(rule: JoinFilterRule) -> exp.Exists:
    """Build the correlated ``EXISTS`` body for one join rule.

    ``FROM`` is the first hop's ``to_table`` (alias ``_rls_j0``); each later
    hop becomes an inner ``JOIN``; the first hop correlates to the wrapper's
    inner base alias (``_rls_src``); the terminal predicate lives on the last
    hop's alias. All identifiers are structural (dotted/quoted-safe)."""
    hops = rule.join_path

    first_to = exp.to_table(hops[0].to_table)
    first_to.set("alias", exp.TableAlias(this=exp.to_identifier(_hop_alias(0))))
    inner = exp.select(exp.Literal.number(1)).from_(first_to)

    for i in range(1, len(hops)):
        hop = hops[i]
        to_tbl = exp.to_table(hop.to_table)
        to_tbl.set("alias", exp.TableAlias(this=exp.to_identifier(_hop_alias(i))))
        on = exp.EQ(
            this=exp.column(hop.to_column, table=_hop_alias(i)),
            expression=exp.column(hop.from_column, table=_hop_alias(i - 1)),
        )
        inner = inner.join(to_tbl, on=on, join_type="inner")

    # First hop correlates the inner base table to the wrapped source row.
    correlation = exp.EQ(
        this=exp.column(hops[0].to_column, table=_hop_alias(0)),
        expression=exp.column(hops[0].from_column, table=_RLS_SRC),
    )
    inner = inner.where(correlation)
    inner = inner.where(
        _terminal_predicate(rule, table_alias=_hop_alias(len(hops) - 1))
    )
    return exp.Exists(this=inner)


def _wrap_table_exists(table: exp.Table, rules: list) -> None:
    """Replace ``table`` in place with a correlated-EXISTS wrapper: one
    ``EXISTS`` per targeting rule, AND-combined, preserving the outer alias."""
    alias = table.alias_or_name
    bare = table.copy()
    bare.set("alias", exp.TableAlias(this=exp.to_identifier(_RLS_SRC)))
    inner = exp.select("*").from_(bare)
    for rule in rules:
        inner = inner.where(_build_exists(rule))  # chained .where AND-combines
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
    on_correlated_emitted: Optional[Callable[[], None]] = None,
) -> str:
    """Wrap every physical-table ref whose rule(s) apply.

    ``has_column(scoped_table, column)`` returns ``True`` / ``False`` /
    ``None`` (cannot confirm) and is consulted only for column-rule tables.
    ``on_correlated_emitted`` (if given) is invoked once when at least one
    correlated ``EXISTS`` is emitted (any dialect) — the engine uses it as the
    ClickHouse version guard. Raises :class:`ForcedFilterError` on a
    fail-closed condition (unconfirmable column, ``block`` on an absent column,
    or a non-SELECT statement root).
    """
    if not policy.data_filters:
        return sql  # zero-overhead: no parse, no introspection

    column_rules = [r for r in policy.data_filters if isinstance(r, ColumnFilterRule)]
    join_rules = [r for r in policy.data_filters if isinstance(r, JoinFilterRule)]

    ast = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(ast, _ALLOWED_ROOTS):
        raise ForcedFilterError(
            "Forced filter: refusing to rewrite a non-SELECT statement "
            f"({type(ast).__name__}); failing closed."
        )

    emitted_correlated = False
    for table in _physical_tables(ast):
        scoped = _scoped_table(table)
        # Override: a join-targeted table is scoped ONLY by its join rule(s);
        # column rules never touch it and its column presence is not probed.
        targeting = [r for r in join_rules if _target_matches(scoped, r.target_table)]
        if targeting:
            _wrap_table_exists(table, targeting)
            emitted_correlated = True
            continue
        predicates = _predicates_for_table(
            scoped=scoped, column_rules=column_rules, has_column=has_column
        )
        if predicates:
            _wrap_table(table, predicates)

    if emitted_correlated:
        if on_correlated_emitted is not None:
            on_correlated_emitted()
        if dialect == "clickhouse":
            # Correlated subqueries are experimental on ClickHouse; attach the
            # enabling setting structurally (works on Select and Union roots).
            ast.set(
                "settings",
                [exp.var("allow_experimental_correlated_subqueries").eq(1)],
            )

    return ast.sql(dialect=dialect)
