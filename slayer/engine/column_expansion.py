"""Recursive expansion of derived ``Column.sql`` references.

Closes DEV-1333. A ``Column.sql`` may reference any other column on the same
model or on a joined model — including columns that are themselves derived
(have their own ``sql`` expression rather than being a bare base-table
column). The query planner had been emitting such references verbatim, which
fails at execution because the joined table's underlying SQL knows nothing
about derived SLayer columns. This module walks the parsed AST of every
``Column.sql`` we are about to embed in a query, recursively replaces each
``<table>.<col>`` reference whose target is a derived column with the
target's own SQL (qualified to the right path alias), and lets the bare
base-column references qualify to the canonical ``__``-delimited path
alias.

The expansion runs in the enrichment phase, so the SQL generator never sees
unresolved derived references.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, FrozenSet, Optional, Tuple

import sqlglot
from sqlglot import exp

from slayer.core.models import Column, SlayerModel

ResolveModel = Callable[..., Awaitable[Optional[SlayerModel]]]


def _is_trivial_base(*, column: Column) -> bool:
    """A column is "trivial base" iff its sql is missing or is just its own
    bare name. These need no expansion — only re-qualification.
    """
    if column.sql is None:
        return True
    return column.sql.strip() == column.name


async def _walk_path_to_target(
    *,
    source_model: SlayerModel,
    source_alias: str,
    table_alias: str,
    resolve_model: ResolveModel,
    named_queries: Dict[str, Any],
    is_root: bool,
) -> Tuple[Optional[SlayerModel], Optional[str]]:
    """Resolve a ``table_alias`` (e.g. ``B`` or ``B__C``) seen inside a
    Column.sql to the terminal joined model and the canonical alias to use
    in emitted SQL.

    The ``is_root`` flag captures whether ``source_model`` is the FROM root
    of the outer query. When True, walked paths are emitted bare
    (``"__".join(parts)``); when False, they are prefixed with
    ``source_alias`` so a derived column on a joined model referencing a
    further-joined model resolves to the right ``__``-delimited path
    (e.g., walking ``C`` off source ``B`` reached from root via ``B`` →
    canonical ``B__C``, not ``C``). Closes the alias-prefix bug raised on
    PR #89.

    Returns ``(None, None)`` if the alias does not resolve as a join path —
    in that case the caller should leave the reference untouched (it is
    likely a CTE / sub-query alias the user wired up themselves).
    """
    parts = table_alias.split("__") if "__" in table_alias else [table_alias]
    # Local: the alias is the source model itself (its name or its FROM
    # alias as we already know it).
    if len(parts) == 1 and parts[0] in (source_alias, source_model.name):
        return source_model, source_alias
    current = source_model
    for hop in parts:
        join = next((j for j in current.joins if j.target_model == hop), None)
        if join is None:
            return None, None
        nxt = await resolve_model(model_name=hop, named_queries=named_queries)
        if nxt is None:
            return None, None
        current = nxt
    walked = "__".join(parts)
    canonical = walked if is_root else f"{source_alias}__{walked}"
    return current, canonical


async def expand_derived_refs(
    *,
    sql: Optional[str],
    model: SlayerModel,
    alias_path: str,
    resolve_model: ResolveModel,
    named_queries: Optional[Dict[str, Any]] = None,
    dialect: str,
    visited: Optional[FrozenSet[Tuple[str, str]]] = None,
    is_root: bool = True,
) -> Optional[str]:
    """Recursively expand cross-model and local derived-column references
    inside ``sql``.

    Args:
        sql: The Column / measure SQL to expand. May be ``None`` — returned
            unchanged.
        model: The model whose join graph is the reference frame for
            unprefixed and singly-prefixed identifiers in ``sql``.
        alias_path: The alias prefix under which bare identifiers in ``sql``
            should be qualified — typically the FROM alias used for ``model``
            in the outer query (e.g., ``"orders"`` or ``"customers__regions"``).
        resolve_model: Async callable ``(model_name=str, named_queries=...)``
            that returns a ``SlayerModel`` (or None).
        named_queries: Pass-through context for ``resolve_model``.
        dialect: sqlglot dialect for parse/emit.
        visited: Cycle-detection set of ``(model_name, column_name)``
            tuples populated during recursion. Callers leave as None.

    Raises:
        ValueError: on a circular column-reference chain.
    """
    if not sql:
        return sql
    visited = visited or frozenset()
    named_queries = named_queries or {}

    parsed = sqlglot.parse_one(sql, dialect=dialect)
    # Materialize the columns first — we may mutate them in place via .replace().
    column_nodes = list(parsed.find_all(exp.Column))

    for col in column_nodes:
        # exp.Column may carry a multi-part qualifier (catalog.db.table.col).
        # We treat anything beyond the immediate table identifier as outside
        # SLayer's contract (the Column.sql convention is `<alias>.<col>`).
        table_id = col.args.get("table")
        if col.args.get("db") or col.args.get("catalog"):
            # Multi-part — leave alone, not a SLayer alias.
            continue

        col_name = col.name

        if table_id is None:
            # Bare identifier → qualify to alias_path.
            col.set("table", exp.to_identifier(alias_path))
            continue

        table_alias = table_id.name
        target_model, canonical_alias = await _walk_path_to_target(
            source_model=model,
            source_alias=alias_path,
            table_alias=table_alias,
            resolve_model=resolve_model,
            named_queries=named_queries,
            is_root=is_root,
        )
        if target_model is None or canonical_alias is None:
            # Unknown alias — leave untouched.
            continue

        target_col = target_model.get_column(col_name)
        if target_col is None or _is_trivial_base(column=target_col):
            # Base column or unknown identifier on a known target model:
            # rewrite the table to the canonical alias and stop.
            col.set("table", exp.to_identifier(canonical_alias))
            continue

        # Derived → recurse. Recursion stays "root" only when the target
        # column lives on the same model (no alias change); a remote
        # target descended via a path is by definition non-root, so its
        # own walks must prefix the canonical alias.
        next_is_root = is_root and (target_model is model)
        key = (target_model.name, col_name)
        if key in visited:
            chain = " → ".join(f"{m}.{c}" for m, c in [*visited, key])
            raise ValueError(
                f"Circular column reference detected: {chain}"
            )
        expanded_sql = await expand_derived_refs(
            sql=target_col.sql,
            model=target_model,
            alias_path=canonical_alias,
            resolve_model=resolve_model,
            named_queries=named_queries,
            dialect=dialect,
            visited=visited | frozenset({key}),
            is_root=next_is_root,
        )
        if expanded_sql is None:
            continue
        # Splice in, parenthesized so the surrounding expression's precedence
        # is preserved.
        expanded_ast = sqlglot.parse_one(expanded_sql, dialect=dialect)
        col.replace(exp.Paren(this=expanded_ast))

    return parsed.sql(dialect=dialect)
