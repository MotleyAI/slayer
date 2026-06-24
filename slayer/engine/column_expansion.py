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

from typing import Any, Awaitable, Callable, Dict, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import ScopeType, traverse_scope

from slayer.core.errors import ColumnCycleError
from slayer.core.models import Column, SlayerModel

ResolveModel = Callable[..., Awaitable[Optional[SlayerModel]]]


def _is_trivial_base(*, column: Column) -> bool:
    """A column is "trivial base" iff its sql is missing or is just its own
    bare name. These need no expansion — only re-qualification.
    """
    if column.sql is None:
        return True
    sql = column.sql.strip()
    # A double-quoted self-identity (``"legalEntityType"`` for a column named
    # ``legalEntityType``) is still a bare base reference — required to point
    # at a mixed-case physical column on case-folding dialects. Strip the
    # surrounding identifier quotes before comparing so it is not mistaken
    # for a derived expression (which would self-recurse into a false cycle).
    if len(sql) >= 2 and sql[0] == '"' and sql[-1] == '"':
        sql = sql[1:-1].replace('""', '"')
    return sql == column.name


def _root_scope_column_ids(*, parsed: exp.Expression) -> Set[int]:
    """Return the ``id()`` set of ``exp.Column`` nodes that lexically belong
    to the root scope of ``parsed`` (DEV-1410).

    Column.sql is contractually a scalar expression, not a SELECT. To re-use
    sqlglot's scope analysis we wrap ``parsed`` in a synthetic
    ``SELECT <parsed> AS _`` so it has a real root scope. The wrapper is
    used only for scope traversal; the original ``parsed`` AST is unchanged.

    A Column is "root-scope" iff its innermost scope-defining ancestor is
    the wrapper itself. Anything nested under a ``Subquery``, CTE, set
    operation (``Union`` / ``Except`` / ``Intersect``), ``Values``, or
    other scope-producing construct returns a non-root ScopeType and is
    skipped from derived-column inlining.

    ``Window`` / ``OVER`` is NOT a new scope: columns inside
    ``PARTITION BY`` / ``ORDER BY`` remain root-scope.
    """
    if not isinstance(parsed, exp.Expression):
        return set()
    wrapper = exp.Select(expressions=[exp.Alias(this=parsed.copy(), alias="_")])
    scope_node_ids: Dict[int, ScopeType] = {}
    for scope in traverse_scope(wrapper):
        scope_node_ids[id(scope.expression)] = scope.scope_type
    if not scope_node_ids:
        # No SELECTs in the fragment at all — every column is root-scope.
        return {id(c) for c in parsed.find_all(exp.Column)}
    # Re-walk the WRAPPER (which holds copies) — but we need ids from the
    # ORIGINAL parsed tree. Pair them up positionally: find_all yields
    # nodes in document order on both wrapper.this[0] and parsed.
    wrapper_cols = list(wrapper.find_all(exp.Column))
    parsed_cols = list(parsed.find_all(exp.Column))
    if len(wrapper_cols) != len(parsed_cols):
        # Fail closed: if the positional pairing between the wrapper copy
        # and the original tree ever drifts, treat NO column as root-scope.
        # That suppresses derived-column inlining entirely for this
        # fragment, which is conservative (the compile-time guard still
        # catches cycles, and a missed inline merely shows up as the
        # historical bare-name auto-qualification — never as a silent
        # cross-scope splice). This branch is unreachable today; the
        # wrapper just wraps a deep copy and ``find_all`` walks in
        # document order.
        return set()
    root_ids: Set[int] = set()
    for w_col, p_col in zip(wrapper_cols, parsed_cols):
        node: Optional[exp.Expression] = w_col.parent
        scope_type: Optional[ScopeType] = None
        while node is not None:
            if id(node) in scope_node_ids:
                scope_type = scope_node_ids[id(node)]
                break
            node = node.parent
        if scope_type == ScopeType.ROOT:
            root_ids.add(id(p_col))
    return root_ids


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
    # DEV-1410: literal match against the host's FROM alias or model name
    # comes FIRST, before any ``__`` splitting. ``alias_path`` is already a
    # canonical ``__``-delimited path coming from the engine (e.g.
    # ``"B__C"``) — splitting it would falsely treat it as a multi-hop
    # walk and fail to resolve as the host.
    if table_alias == source_alias or table_alias == source_model.name:
        return source_model, source_alias
    parts = table_alias.split("__") if "__" in table_alias else [table_alias]
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


async def _process_column_node(
    *,
    col: exp.Column,
    model: SlayerModel,
    alias_path: str,
    resolve_model: ResolveModel,
    named_queries: Dict[str, Any],
    dialect: str,
    visited: Tuple[Tuple[str, str], ...],
    is_root: bool,
    root_scope_ids: Set[int],
) -> None:
    """Resolve one ``exp.Column`` node in the parsed AST, mutating it in
    place. Encapsulates the multi-branch decision that drives expansion:

    - multi-part qualifier (``catalog.db.table.col``) → leave alone
    - bare identifier → qualify to ``alias_path``
    - ``<table>.<col>`` where the alias doesn't resolve as a join path
      → leave alone (CTE / sub-query alias)
    - ``<table>.<col>`` where the target column is base → rewrite table
      to the canonical alias
    - ``<table>.<col>`` where the target column is derived → recurse and
      splice the expanded AST in (parenthesized for precedence safety)

    Cycle detection raises ``ValueError`` with the recursion chain.
    """
    # exp.Column may carry a multi-part qualifier (catalog.db.table.col).
    # We treat anything beyond the immediate table identifier as outside
    # SLayer's contract (the Column.sql convention is `<alias>.<col>`).
    if col.args.get("db") or col.args.get("catalog"):
        return

    table_id = col.args.get("table")
    col_name = col.name

    # DEV-1410: bare identifiers and qualified ``<alias>.<col>`` refs flow
    # through the SAME lookup. A bare ref is treated as if it had been
    # written with the host alias — ``_walk_path_to_target`` returns
    # ``(source_model, alias_path)`` for that single-part match, so the
    # downstream derived-vs-base decision (and recursion) is shared.
    table_alias = table_id.name if table_id is not None else alias_path
    target_model, canonical_alias = await _walk_path_to_target(
        source_model=model,
        source_alias=alias_path,
        table_alias=table_alias,
        resolve_model=resolve_model,
        named_queries=named_queries,
        is_root=is_root,
    )
    if target_model is None or canonical_alias is None:
        return  # unknown alias — leave untouched

    target_col = target_model.get_column(col_name)
    if target_col is None or _is_trivial_base(column=target_col):
        # Base column or unknown identifier on a known target model:
        # rewrite the table to the canonical alias and stop.
        col.set("table", exp.to_identifier(canonical_alias))
        return

    # DEV-1410 scope guard: only inline derived-column bodies when the
    # reference is in the ROOT scope of the parent fragment. Nested
    # scopes (subqueries, set-op branches, VALUES, CTEs) can legitimately
    # use the same identifier to mean an inner column of a different
    # rowset — leave them alone.
    if id(col) not in root_scope_ids:
        return

    # Derived → recurse. Recursion stays "root" only when the target
    # column lives on the same model (no alias change); a remote target
    # descended via a path is by definition non-root, so its own walks
    # must prefix the canonical alias.
    next_is_root = is_root and (target_model is model)
    key = (target_model.name, col_name)
    if key in visited:
        cycle_start = visited.index(key)
        cycle = (*visited[cycle_start:], key)
        raise ColumnCycleError(cycle=list(cycle))
    expanded_sql = await expand_derived_refs(
        sql=target_col.sql,
        model=target_model,
        alias_path=canonical_alias,
        resolve_model=resolve_model,
        named_queries=named_queries,
        dialect=dialect,
        visited=(*visited, key),
        is_root=next_is_root,
    )
    if expanded_sql is None:
        return
    # Splice in, parenthesized so the surrounding expression's precedence
    # is preserved.
    expanded_ast = sqlglot.parse_one(expanded_sql, dialect=dialect)
    col.replace(exp.Paren(this=expanded_ast))


async def expand_derived_refs(
    *,
    sql: Optional[str],
    model: SlayerModel,
    alias_path: str,
    resolve_model: ResolveModel,
    named_queries: Optional[Dict[str, Any]] = None,
    dialect: str,
    visited: Optional[Tuple[Tuple[str, str], ...]] = None,
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
        visited: Ordered cycle-detection chain of ``(model_name,
            column_name)`` tuples populated during recursion. Ordered
            (not a set) so the cycle path in error messages reflects the
            actual recursion order — frozenset iteration is randomized
            via PYTHONHASHSEED. Callers leave as None.

    Raises:
        ValueError: on a circular column-reference chain.
    """
    if not sql:
        return sql
    visited = visited or ()
    named_queries = named_queries or {}

    parsed = sqlglot.parse_one(sql, dialect=dialect)
    # Materialize the columns first — we may mutate them in place via .replace().
    column_nodes = list(parsed.find_all(exp.Column))
    # DEV-1410: compute root-scope membership once. Derived-column inlining
    # only applies to root-scope refs; nested scopes (subqueries, set ops,
    # VALUES, CTEs) are left alone.
    root_scope_ids = _root_scope_column_ids(parsed=parsed)

    for col in column_nodes:
        await _process_column_node(
            col=col,
            model=model,
            alias_path=alias_path,
            resolve_model=resolve_model,
            named_queries=named_queries,
            dialect=dialect,
            visited=visited,
            is_root=is_root,
            root_scope_ids=root_scope_ids,
        )

    return parsed.sql(dialect=dialect)
