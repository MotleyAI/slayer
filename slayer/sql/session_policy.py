"""Forced-filter SQL rewrite for session-policy RLS (DEV-1578 / DEV-1627 / DEV-1718).

``apply_session_policy`` is a pure sqlglot transform. Given final SQL and a
``SessionPolicy``, it wraps every *physical* table reference according to the
policy's single ``ruleset``.

**Column ruleset** (``ColumnFilterRuleset``) wraps every table that has the
tenant column in a filtered ``SELECT *`` subquery, preserving the alias::

    FROM orders               -->  FROM (SELECT * FROM orders
                                         WHERE organization_uuid = '7ef3') AS orders

**Join ruleset** (``JoinFilterRuleset``) classifies each physical table
structurally (no column introspection):

* the anchor ``table`` is wrapped directly (``WHERE column = value``);
* a ``JoinFilterRule.target_table`` is scoped via a correlated ``EXISTS``
  semi-join to the anchor (cardinality-safe, ``LEFT JOIN``-preserving)::

    FROM orders  -->  FROM (SELECT * FROM orders AS _rls_src
                            WHERE EXISTS (
                              SELECT 1 FROM customers AS _rls_j0
                              WHERE _rls_j0.id = _rls_src.customer_id
                                AND _rls_j0.organization_uuid = '7ef3'
                            )) AS orders

* a table in the ``whitelist`` is emitted unchanged;
* anything else fails closed (``ForcedFilterError``).

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
    ColumnFilterRuleset,
    JoinFilterRule,
    JoinFilterRuleset,
    SessionPolicy,
    _table_names_match,
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


def _scoped_table(table: exp.Table) -> ScopedTable:
    return ScopedTable(
        catalog=(table.catalog or None),
        schema_name=(table.db or None),
        name=table.name,
    )


def _build_predicate(column: str, value) -> exp.Expression:
    """Unqualified ``column = value`` / ``column IN (...)`` predicate."""
    col = exp.column(column)
    if isinstance(value, tuple):
        return exp.In(this=col, expressions=[exp.convert(v) for v in value])
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
    """Whether ``scoped`` is the table a policy entry (join target / anchor /
    whitelist) names.

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


# ---------------------------------------------------------------------------
# ColumnFilterRuleset
# ---------------------------------------------------------------------------


def _apply_column_ruleset(
    ast: exp.Expression, ruleset: ColumnFilterRuleset, has_column: HasColumn
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


def _terminal_predicate(*, column: str, value, table_alias: str) -> exp.Expression:
    """The tenant predicate on the anchor alias — scalar ``=`` or ``IN``, always
    emitted, values via ``exp.convert`` (injection-safe)."""
    col = exp.column(column, table=table_alias)
    if isinstance(value, tuple):
        return exp.In(this=col, expressions=[exp.convert(v) for v in value])
    return exp.EQ(this=col, expression=exp.convert(value))


def _build_exists(rule: JoinFilterRule, *, ruleset: JoinFilterRuleset) -> exp.Exists:
    """Build the correlated ``EXISTS`` body for one join rule.

    Uses ``rule.oriented_hops()`` (target-first): ``FROM`` is the first hop's
    ``to_table`` (alias ``_rls_j0``); each later hop becomes an inner ``JOIN``;
    the first hop correlates to the wrapper's inner base alias (``_rls_src``);
    the terminal tenant predicate lives on the last hop's alias — which must be
    the anchor table. All identifiers are structural (dotted/quoted-safe).
    """
    hops = rule.oriented_hops()
    if not _table_names_match(hops[-1].to_table, ruleset.table):
        # Defensive: a bad model_copy could break the anchor reachability the
        # ruleset validator enforces — fail closed rather than land the tenant
        # predicate on a non-anchor table.
        raise ForcedFilterError(
            f"Forced filter join path for '{rule.target_table}' does not reach "
            f"the anchor table '{ruleset.table}'; failing closed.",
            table=rule.target_table,
            column=ruleset.column,
        )

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
        _terminal_predicate(
            column=ruleset.column,
            value=ruleset.value,
            table_alias=_hop_alias(len(hops) - 1),
        )
    )
    return exp.Exists(this=inner)


def _wrap_table_exists(
    table: exp.Table, rules: list, *, ruleset: JoinFilterRuleset
) -> None:
    """Replace ``table`` in place with a correlated-EXISTS wrapper: one
    ``EXISTS`` per targeting rule, AND-combined, preserving the outer alias."""
    alias = table.alias_or_name
    bare = table.copy()
    bare.set("alias", exp.TableAlias(this=exp.to_identifier(_RLS_SRC)))
    inner = exp.select("*").from_(bare)
    for rule in rules:
        inner = inner.where(_build_exists(rule, ruleset=ruleset))
    table.replace(
        exp.Subquery(
            this=inner, alias=exp.TableAlias(this=exp.to_identifier(alias))
        )
    )


def _apply_join_ruleset(ast: exp.Expression, ruleset: JoinFilterRuleset) -> bool:
    """Structurally scope every physical table under a join ruleset. Returns
    whether at least one correlated ``EXISTS`` was emitted (drives the
    ClickHouse setting/guard). Never probes column presence."""
    emitted_correlated = False
    for table in _physical_tables(ast):
        scoped = _scoped_table(table)
        if _target_matches(scoped, ruleset.table):
            _wrap_table(table, [_build_predicate(ruleset.column, ruleset.value)])
            continue
        targeting = [
            r for r in ruleset.joins if _target_matches(scoped, r.target_table)
        ]
        if targeting:
            _wrap_table_exists(table, targeting, ruleset=ruleset)
            emitted_correlated = True
            continue
        if any(_target_matches(scoped, w) for w in ruleset.whitelist):
            continue  # whitelisted: emitted unfiltered
        raise ForcedFilterError(
            f"Forced filter: table '{scoped.name}' is not covered by the policy "
            "(not the anchor, not a join target, not whitelisted); failing "
            "closed.",
            table=scoped.name,
        )
    return emitted_correlated


# ---------------------------------------------------------------------------
# ClickHouse correlated-subquery SETTINGS
# ---------------------------------------------------------------------------


_CH_CORRELATED_SETTING = "allow_experimental_correlated_subqueries"


def _settings_holder(ast: exp.Expression) -> exp.Expression:
    """Return the node carrying (or that should carry) this statement's
    ClickHouse ``SETTINGS``. sqlglot parks a trailing ``SETTINGS`` on the last
    branch ``SELECT`` of an unparenthesised ``UNION`` rather than on the
    set-operation root, so honour that placement when it exists; otherwise use
    the root (so appending never produces two ``SETTINGS`` clauses).

    Only the set-operation's **own right spine** is followed — never a nested
    subquery in a ``FROM`` clause, whose ``SETTINGS`` is local to that rowset
    and must not receive the statement-level correlated-subquery flag."""
    if ast.args.get("settings"):
        return ast
    if isinstance(ast, exp.SetOperation):
        node: exp.Expression = ast
        while isinstance(node, exp.SetOperation):
            node = node.expression  # right branch owns the trailing SETTINGS
        if node.args.get("settings"):
            return node
    return ast


def _attach_ch_correlated_setting(ast: exp.Expression) -> None:
    """Force ``allow_experimental_correlated_subqueries = 1`` onto the
    statement's SETTINGS, preserving any other settings. Drops any prior entry
    for this setting (any value, e.g. ``= 0``) so the correlated subquery is
    never emitted with the setting left disabled."""
    holder = _settings_holder(ast)
    kept = [
        s
        for s in (holder.args.get("settings") or [])
        if getattr(s.this, "name", None) != _CH_CORRELATED_SETTING
    ]
    holder.set("settings", [*kept, exp.var(_CH_CORRELATED_SETTING).eq(1)])


def apply_session_policy(
    sql: str,
    *,
    dialect: str,
    policy: SessionPolicy,
    has_column: HasColumn,
    on_correlated_emitted: Optional[Callable[[], None]] = None,
) -> str:
    """Wrap every physical-table ref per the policy's ``ruleset``.

    ``has_column(scoped_table, column)`` returns ``True`` / ``False`` /
    ``None`` (cannot confirm) and is consulted **only** for a
    ``ColumnFilterRuleset``. ``on_correlated_emitted`` (if given) is invoked
    once when at least one correlated ``EXISTS`` is emitted (any dialect) — the
    engine uses it as the ClickHouse version guard. Raises
    :class:`ForcedFilterError` on a fail-closed condition (unconfirmable column,
    ``block`` on an absent column, an unlisted table under a join ruleset, or a
    non-SELECT statement root).
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
        emitted_correlated = False
    else:  # JoinFilterRuleset
        emitted_correlated = _apply_join_ruleset(ast, ruleset)

    if emitted_correlated:
        if on_correlated_emitted is not None:
            on_correlated_emitted()
        if dialect == "clickhouse":
            # Correlated subqueries are experimental on ClickHouse; enable the
            # setting structurally, preserving any SETTINGS the statement
            # already carries and forcing our value to 1.
            _attach_ch_correlated_setting(ast)

    return ast.sql(dialect=dialect)
