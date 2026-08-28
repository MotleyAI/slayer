"""DEV-1410: save-time derived-column cycle detection.

A model whose derived ``Column.sql`` chain forms a cycle must be rejected at
save time so the broken model never reaches a query. The compile-time guard
in :mod:`slayer.engine.column_expansion` is the authoritative correctness
boundary; this module is the early-failure UX layer.

Wiring: :class:`slayer.storage.base.StorageBackend.save_model` calls
:func:`validate_no_column_cycles` before delegating to the backend's
``_save_model_impl``. The migration write-back path passes
``_validate=False`` so legacy cyclic models remain loadable.

Scope: same-datasource only. Cross-datasource references are invalid by
design and not attempted. Unresolved join targets (referenced model not yet
persisted) are silently skipped — best-effort. The compile-time guard
catches anything missed here.
"""
from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

import sqlglot

from slayer.core.errors import ColumnCycleError
from slayer.core.models import Column, SlayerModel
from slayer.engine.column_expansion import (
    _is_trivial_base,
    _reference_sites,
    _root_scope_column_ids,
    resolve_ref_target,
)
from slayer.sql.reserved_keywords import prequote_reserved_identifiers

if TYPE_CHECKING:
    from slayer.storage.base import StorageBackend


# Single sqlglot dialect for the dependency walk. The walk only inspects
# ``exp.Column`` identifier shape — dialect choice does not change which
# columns appear in the AST. Using sqlglot's default keeps the validator
# independent of the model's runtime datasource dialect.
_DEPENDENCY_DIALECT: str | None = None


def _column_dependencies(
    *,
    column: Column,
    host: SlayerModel,
    reachable: dict[str, SlayerModel],
) -> list[tuple[str, str]]:
    """Extract the root-scope derived-column dependencies of ``column``.

    Returns a list of ``(model_name, column_name)`` tuples — only refs
    pointing at columns that exist in ``reachable`` AND are themselves
    derived (non-trivial sql). Base, unknown, and external refs are
    silently dropped: they cannot participate in a derived-column cycle.

    DEV-1743: each reference is resolved through the shared
    :func:`slayer.engine.column_expansion.resolve_ref_target` — exact-name-first
    (a ``__``-named DIRECT join target stays whole) then a dotted chain of exact
    hops (``customers.regions.label`` walks host→customers→regions), never
    ``__``-splitting. Opaque / physical refs simply fail to resolve and drop out.
    """
    if column.sql is None or _is_trivial_base(column=column):
        return []
    try:
        # DEV-1686: prequote reserved qualifiers/leaves so a derived column
        # referencing a reserved joined model (e.g. ``grant.amount``) parses
        # cleanly here instead of falling back to a noisy ``Command`` parse.
        parsed = sqlglot.parse_one(
            prequote_reserved_identifiers(sql=column.sql, dialect=_DEPENDENCY_DIALECT),
            dialect=_DEPENDENCY_DIALECT,
        )
    except Exception:
        # Parse failure on a save attempt — let the actual save proceed so
        # the surface-level error (storage / pydantic) is what the user
        # sees, not a noisy validator complaint about unparseable SQL.
        return []
    root_ids = _root_scope_column_ids(parsed=parsed)
    deps: list[tuple[str, str]] = []
    for _node, quals, leaf in _reference_sites(parsed, root_ids):
        target = resolve_ref_target(
            qualifiers=quals, source_model=host, resolve_model=reachable.get,
        )
        if target is None:
            continue
        target_col = target.get_column(leaf)
        if target_col is None or _is_trivial_base(column=target_col):
            continue
        deps.append((target.name, target_col.name))
    return deps


def _node_dependencies(
    *,
    node: tuple[str, str],
    reachable: dict[str, SlayerModel],
) -> list[tuple[str, str]]:
    """Return the dependency edges leaving ``node = (model_name, col_name)``.
    Empty list when the model or column is missing — those are dead-ends,
    not errors.
    """
    model_name, col_name = node
    host = reachable.get(model_name)
    if host is None:
        return []
    col = host.get_column(col_name)
    if col is None:
        return []
    return _column_dependencies(column=col, host=host, reachable=reachable)


def _dfs_visit(
    *,
    node: tuple[str, str],
    reachable: dict[str, SlayerModel],
    on_stack: list[tuple[str, str]],
    on_stack_set: set[tuple[str, str]],
    visited: set[tuple[str, str]],
) -> list[tuple[str, str]] | None:
    """Recursive DFS visit. Returns the first cycle reachable from
    ``node``, or ``None``. Mutates ``on_stack`` / ``on_stack_set`` /
    ``visited`` in place — the caller initialises them empty and
    discards them on return.
    """
    if node in on_stack_set:
        idx = on_stack.index(node)
        return [*on_stack[idx:], node]
    if node in visited:
        return None
    on_stack.append(node)
    on_stack_set.add(node)
    for dep in _node_dependencies(node=node, reachable=reachable):
        found = _dfs_visit(
            node=dep, reachable=reachable,
            on_stack=on_stack, on_stack_set=on_stack_set, visited=visited,
        )
        if found is not None:
            return found
    on_stack.pop()
    on_stack_set.discard(node)
    visited.add(node)
    return None


def _detect_cycle_dfs(
    *,
    start: tuple[str, str],
    reachable: dict[str, SlayerModel],
) -> list[tuple[str, str]] | None:
    """DFS from ``start = (model_name, column_name)``. Returns the first
    cycle found as an ordered list (start may appear at both ends if the
    cycle closes through it), or ``None`` if the subgraph is acyclic.
    """
    return _dfs_visit(
        node=start, reachable=reachable,
        on_stack=[], on_stack_set=set(), visited=set(),
    )


async def _prefetch_reachable_models(
    *,
    model: SlayerModel,
    storage: "StorageBackend",
) -> dict[str, SlayerModel]:
    """BFS over ``model.joins`` (transitively), pulling each target model
    in the same ``data_source``. Returns ``{model_name: model}`` including
    ``model`` itself. Unresolvable target names (model not persisted yet)
    are silently omitted — save-time is best-effort.
    """
    out: dict[str, SlayerModel] = {model.name: model}
    queue: deque[SlayerModel] = deque([model])
    while queue:
        current = queue.popleft()
        for join in current.joins:
            target_name = join.target_model
            if target_name in out:
                continue
            try:
                target = await storage.get_model(
                    target_name, data_source=model.data_source,
                )
            except Exception:
                target = None
            if target is None:
                continue
            out[target_name] = target
            queue.append(target)
    return out


async def validate_no_column_cycles(
    *,
    model: SlayerModel,
    storage: "StorageBackend",
) -> None:
    """Raise :class:`ColumnCycleError` if any derived column on ``model``
    (or on a reachable joined model in the same ``data_source``)
    participates in a cycle.

    Best-effort: unresolved join targets are skipped; nested-scope refs
    are excluded by the same ``_root_scope_column_ids`` rule used by the
    compile-time expander. The compile-time guard remains authoritative.
    """
    reachable = await _prefetch_reachable_models(model=model, storage=storage)
    # Iterate roots in a deterministic order so the reported cycle is
    # stable across runs.
    roots: list[tuple[str, str]] = []
    for entity_name in sorted(reachable.keys()):
        entity = reachable[entity_name]
        for col in entity.columns:
            if col.sql is None or _is_trivial_base(column=col):
                continue
            roots.append((entity_name, col.name))
    for root in roots:
        cycle = _detect_cycle_dfs(start=root, reachable=reachable)
        if cycle is not None:
            raise ColumnCycleError(cycle=cycle)
