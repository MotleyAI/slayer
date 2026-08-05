"""Forced-filter SQL rewrite for session-policy RLS.

``apply_session_policy`` is a pure sqlglot transform wrapping every *physical* table
reference in the final SQL per the policy's ``ruleset``. A column ruleset filters each
table having the tenant column; a join ruleset filters the anchor directly and reaches
it from other tables via a correlated ``EXISTS``::

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
from sqlglot.optimizer.scope import Scope, traverse_scope

from slayer.core.errors import ForcedFilterError
from slayer.core.policy import (
    ColumnFilterRuleset,
    JoinFilterRule,
    JoinFilterRuleset,
    SessionPolicy,
    _validate_join_rule_anchor,
)

# Any other statement root (INSERT / UPDATE / DDL / …) fails closed.
_ALLOWED_ROOTS = (exp.Select, exp.SetOperation)

# Internal aliases for the correlated-EXISTS rewrite. Each lives in a fresh subquery
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
) -> exp.Expression:
    """``column = value`` / ``column IN (...)``, optionally qualified by ``table``."""
    col = exp.column(column, table=table) if table else exp.column(column)
    if isinstance(value, tuple):
        return exp.In(this=col, expressions=[exp.convert(v) for v in value])
    return exp.EQ(this=col, expression=exp.convert(value))


def _physical_tables(ast: exp.Expression) -> list:
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


def _build_exists(rule: JoinFilterRule, *, ruleset: JoinFilterRuleset) -> exp.Exists:
    """Build the correlated ``EXISTS`` body for one join rule.

    Walks the target-first hops: the first hop's ``to_table`` is the ``FROM`` and
    correlates back to the wrapper's ``_rls_src``, later hops become inner joins, and
    the tenant predicate lands on the terminal hop — the anchor.
    """
    try:
        # Re-validated at the SQL boundary via the same helper construction uses, so a
        # model_copy bypassing the ruleset validator fails closed here rather than
        # emitting a mis-scoped EXISTS.
        hops = _validate_join_rule_anchor(rule, ruleset.table)
    except ValueError as exc:
        raise ForcedFilterError(
            f"Forced filter join path for '{rule.target_table}' is invalid "
            "(non-chaining, target not an endpoint, does not reach the anchor, "
            "or the anchor appears more than once); failing closed.",
            table=rule.target_table,
            column=ruleset.column,
        ) from exc

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

    correlation = exp.EQ(
        this=exp.column(hops[0].to_column, table=_hop_alias(0)),
        expression=exp.column(hops[0].from_column, table=_RLS_SRC),
    )
    inner = inner.where(correlation)
    inner = inner.where(
        _build_predicate(
            ruleset.column, ruleset.value, table=_hop_alias(len(hops) - 1)
        )
    )
    return exp.Exists(this=inner)


def _wrap_table_exists(
    table: exp.Table, rules: list, *, ruleset: JoinFilterRuleset
) -> None:
    """Replace ``table`` in place with one AND-combined ``EXISTS`` per targeting rule."""
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
    """Structurally scope every physical table, returning whether any ``EXISTS`` was
    emitted (which drives the ClickHouse guard). Never probes column presence."""
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
    """The node carrying (or that should carry) this statement's ClickHouse ``SETTINGS``.

    sqlglot parks a trailing ``SETTINGS`` on the last branch of an unparenthesised
    ``UNION`` rather than the root, so honour that placement to avoid emitting two
    clauses. Only the set-operation's own right spine is followed — a nested ``FROM``
    subquery's ``SETTINGS`` is local to that rowset.
    """
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
    """Force ``allow_experimental_correlated_subqueries = 1``, preserving other settings.

    Any prior entry for this setting is dropped so a caller-supplied ``= 0`` can't leave
    the correlated subquery emitted with the setting disabled.
    """
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

    ``has_column`` returns ``True``/``False``/``None`` (cannot confirm) and is consulted
    only for a ``ColumnFilterRuleset``. ``on_correlated_emitted`` fires once if any
    correlated ``EXISTS`` is emitted, on any dialect. Raises :class:`ForcedFilterError`
    on any fail-closed condition.
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
            # Correlated subqueries are experimental on ClickHouse.
            _attach_ch_correlated_setting(ast)

    return ast.sql(dialect=dialect)
