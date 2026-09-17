"""DEV-1410: save-time derived-column cycle detection.

A model whose derived ``Column.sql`` chain forms a cycle must be rejected at
save time so the broken model never reaches a query. The compile-time guard
in :mod:`slayer.sql.column_expansion` is the authoritative correctness
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

from typing import TYPE_CHECKING

import sqlglot

from slayer.core.errors import AmbiguousJoinPathError, ColumnCycleError
from slayer.core.join_walker import resolve_hop
from slayer.core.models import Column, SlayerModel
from slayer.sql.column_expansion import (
    is_trivial_base,
    reference_sites,
    root_scope_column_ids,
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


def _fragment_refs(sql: str):
    """Root-scope ``(qualifiers, leaf)`` references of a Mode-A fragment, or
    ``None`` on a parse failure (let the surface-level error surface instead)."""
    try:
        # DEV-1686: prequote reserved qualifiers/leaves so a fragment referencing
        # a reserved joined model (``grant.amount``) parses cleanly here instead
        # of falling back to a noisy ``Command`` parse.
        # prequote accepts a None dialect at runtime (sqlglot default); its str
        # annotation is too strict for the dialect-independent identifier scan.
        parsed = sqlglot.parse_one(
            prequote_reserved_identifiers(sql=sql, dialect=_DEPENDENCY_DIALECT),  # pyright: ignore[reportArgumentType]
            dialect=_DEPENDENCY_DIALECT,
        )
    except Exception:
        return None
    # sqlglot's parse_one is typed with a generic Expr TypeVar; without into= it
    # is an Expression at runtime.
    return [
        (quals, leaf)
        for _node, quals, leaf in reference_sites(
            parsed, root_scope_column_ids(parsed=parsed),  # pyright: ignore[reportArgumentType]
        )
    ]


def _column_dependencies(
    *,
    column: Column,
    host: SlayerModel,
    reachable: dict[str, SlayerModel],
) -> list[tuple[str, str]]:
    """Root-scope dependency edges ``(model_name, column_name)`` of ``column``.

    Both its derived ``Column.sql`` and its ``Column.filter`` (DEV-1832) are
    dependency sources — an edge points at any referenced column that itself
    needs expansion (derived or filtered), so a filter naming another derived
    column is an edge and one naming its own column is a cycle. A dotted filter
    reference that does not walk from ``host`` fails the save, naming the path.

    DEV-1743: each reference resolves through the shared
    :func:`slayer.sql.column_expansion.resolve_ref_target` — exact-name-first
    then a dotted chain of exact hops, never ``__``-splitting.
    """
    return [
        *_sql_dependencies(column=column, host=host, reachable=reachable),
        *_filter_dependencies(column=column, host=host, reachable=reachable),
    ]


def _sql_dependencies(
    *, column: Column, host: SlayerModel, reachable: dict[str, SlayerModel],
) -> list[tuple[str, str]]:
    """Edges from a derived ``Column.sql`` to every expanding column it names."""
    if column.sql is None or is_trivial_base(column=column):
        return []
    deps: list[tuple[str, str]] = []
    for quals, leaf in _fragment_refs(column.sql) or []:
        target = resolve_ref_target(
            qualifiers=quals, source_model=host, models_by_name=reachable,
        )
        if target is None:
            continue
        col = target.get_column(leaf)
        if col is not None and col.needs_expansion:
            deps.append((target.name, col.name))
    return deps


def _filter_dependencies(
    *, column: Column, host: SlayerModel, reachable: dict[str, SlayerModel],
) -> list[tuple[str, str]]:
    """Edges from a ``Column.filter`` to every expanding column it names; a
    trivial-base self-reference reads the physical column, not a cycle."""
    if not column.filter:
        return []
    deps: list[tuple[str, str]] = []
    for quals, leaf in _fragment_refs(column.filter) or []:
        target = _filter_ref_target(
            column=column, host=host, quals=quals, leaf=leaf, reachable=reachable,
        )
        if target is None:
            continue  # a real join hop whose target is not loaded here — skip
        col = target.get_column(leaf)
        if col is None or not col.needs_expansion:
            continue
        if (
            target.name == host.name and leaf == column.name
            and is_trivial_base(column=column)
        ):
            continue
        deps.append((target.name, col.name))
    return deps


def _filter_ref_target(
    *, column: Column, host: SlayerModel, quals, leaf: str,
    reachable: dict[str, SlayerModel],
) -> SlayerModel | None:
    """The model a filter reference resolves to; a leading hop naming no join
    edge on ``host`` is a broken path and fails the save, naming the path."""
    first = _first_hop(quals=quals, host=host)
    if first is None:
        return host
    if not _is_join_hop(host=host, token=first, reachable=reachable):
        raise ValueError(
            f"Column {column.name!r} on model {host.name!r} has a filter "
            f"referencing {'.'.join((*quals, leaf))!r}, which does not "
            f"resolve to a joined model from {host.name!r}."
        )
    return resolve_ref_target(
        qualifiers=quals, source_model=host, models_by_name=reachable,
    )


def _first_hop(*, quals, host: SlayerModel):
    """The leading join-hop token of a reference (host's own name stripped), or
    ``None`` when the reference is host-local (a bare column)."""
    q = list(quals)
    if q and q[0] == host.name:
        q = q[1:]
    return q[0] if q else None


def _is_join_hop(*, host: SlayerModel, token: str, reachable: dict[str, SlayerModel]) -> bool:
    """Whether ``token`` names a join edge incident to ``host`` (its own declared
    joins, resolvable without loading the target); ambiguous counts as a hop."""
    try:
        return resolve_hop(current=host, token=token, models_by_name=reachable) is not None
    except AmbiguousJoinPathError:
        return True


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
    """The datasource's models keyed by name, including ``model`` — the
    bidirectional closure is the connected component (DEV-1853), so refs may
    cross edges declared on either side. Unlistable datasources and
    unloadable peers are silently omitted — save-time is best-effort.
    """
    out: dict[str, SlayerModel] = {model.name: model}
    try:
        names = await storage.list_models(model.data_source)
    except Exception:
        names = [j.target_model for j in model.joins]
    for name in names:
        if name in out:
            continue
        try:
            target = await storage.get_model(
                name, data_source=model.data_source,
            )
        except Exception:
            target = None
        if target is not None:
            out[name] = target
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
    are excluded by the same ``root_scope_column_ids`` rule used by the
    compile-time expander. The compile-time guard remains authoritative.
    """
    reachable = await _prefetch_reachable_models(model=model, storage=storage)
    # Iterate roots in a deterministic order so the reported cycle is
    # stable across runs.
    roots: list[tuple[str, str]] = []
    for entity_name in sorted(reachable.keys()):
        entity = reachable[entity_name]
        for col in entity.columns:
            if not col.needs_expansion:
                continue
            roots.append((entity_name, col.name))
    for root in roots:
        cycle = _detect_cycle_dfs(start=root, reachable=reachable)
        if cycle is not None:
            raise ColumnCycleError(cycle=cycle)
