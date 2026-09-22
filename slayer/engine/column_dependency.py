"""Save-time well-formedness of a model's derived columns (arity + cycles).

A derived ``Column.sql`` / ``Column.filter`` reference must be a function of its
declaring model's row (Axiom 1): it may cross only provably to-one hops. Called
from ``StorageBackend.save_model`` (skipped under ``_validate=False``), this
early-failure layer rejects a path that provably fans (``DerivedColumnFanningError``),
warns on an unproven hop (query-time input-safety gate is the backstop), and
rejects a derived-column cycle (``ColumnCycleError``). Best-effort, same-datasource,
saved model only; unresolved/unloaded/ambiguous targets are skipped and the
compile-time/query-time guards remain authoritative.
"""
from __future__ import annotations

import warnings
from collections.abc import Iterator
from typing import TYPE_CHECKING

import sqlglot

from slayer.core.errors import (
    AmbiguousJoinPathError,
    ColumnCycleError,
    DerivedColumnFanningError,
)
from slayer.core.join_walker import resolve_hop, walk
from slayer.core.models import Column, SlayerModel
from slayer.engine.join_safety import provably_fans, provably_to_one
from slayer.sql.column_expansion import (
    is_trivial_base,
    reference_sites,
    root_scope_column_ids,
    resolve_ref_target,
)
from slayer.sql.reserved_keywords import prequote_reserved_identifiers

if TYPE_CHECKING:
    from slayer.storage.base import StorageBackend


# The walk only inspects ``exp.Column`` identifier shape, so sqlglot's default
# dialect keeps it independent of the model's runtime datasource dialect.
_DEPENDENCY_DIALECT: str | None = None


def _fragment_refs(sql: str):
    """Root-scope ``(qualifiers, leaf)`` references of a Mode-A fragment, or
    ``None`` on a parse failure (let the surface-level error surface instead)."""
    try:
        # prequote reserved qualifiers/leaves so a fragment referencing
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
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]]:
    """Root-scope dependency edges ``(model, column)`` of ``column`` — both its
    ``Column.sql`` and its ``Column.filter``, pointing at any referenced column
    that itself needs expansion (a filter naming its own column is a cycle; a
    dotted filter path that does not walk from ``host`` fails the save)."""
    return [
        *_sql_dependencies(column=column, host=host, reachable=reachable),
        *_filter_dependencies(
            column=column, host=host, reachable=reachable,
            known_but_unloaded=known_but_unloaded,
        ),
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
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]]:
    """Edges from a ``Column.filter`` to every expanding column it names; a
    trivial-base self-reference reads the physical column, not a cycle."""
    if not column.filter:
        return []
    deps: list[tuple[str, str]] = []
    for quals, leaf in _fragment_refs(column.filter) or []:
        target = _filter_ref_target(
            column=column, host=host, quals=quals, leaf=leaf, reachable=reachable,
            known_but_unloaded=known_but_unloaded,
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
    known_but_unloaded: frozenset[str] = frozenset(),
) -> SlayerModel | None:
    """The model a filter reference resolves to; a leading hop naming no join edge
    on ``host`` fails the save (a known-but-unloaded target returns ``None`` — its
    reverse join may exist but is invisible here — only an unknown token raises)."""
    first = _first_hop(quals=quals, host=host)
    if first is None:
        return host
    if not _is_join_hop(host=host, token=first, reachable=reachable):
        if first in known_but_unloaded:
            return None
        raise ValueError(
            f"Column {column.name!r} on model {host.name!r} has a filter "
            f"referencing {'.'.join((*quals, leaf))!r}, which does not "
            f"resolve to a joined model from {host.name!r}."
        )
    return resolve_ref_target(
        qualifiers=quals, source_model=host, models_by_name=reachable,
    )


def _first_hop(*, quals, host: SlayerModel):
    """The leading join-hop token of a reference, or ``None`` when host-local."""
    path = _hop_path(quals=quals, host=host)
    return path[0] if path else None


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
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]]:
    """Dependency edges leaving ``node = (model, col)``; empty when the model or
    column is missing (a dead-end, not an error)."""
    model_name, col_name = node
    host = reachable.get(model_name)
    if host is None:
        return []
    col = host.get_column(col_name)
    if col is None:
        return []
    return _column_dependencies(
        column=col, host=host, reachable=reachable,
        known_but_unloaded=known_but_unloaded,
    )


def _dfs_visit(
    *,
    node: tuple[str, str],
    reachable: dict[str, SlayerModel],
    on_stack: list[tuple[str, str]],
    on_stack_set: set[tuple[str, str]],
    visited: set[tuple[str, str]],
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]] | None:
    """First cycle reachable from ``node`` (or ``None``); mutates the
    ``on_stack``/``on_stack_set``/``visited`` accumulators in place."""
    if node in on_stack_set:
        idx = on_stack.index(node)
        return [*on_stack[idx:], node]
    if node in visited:
        return None
    on_stack.append(node)
    on_stack_set.add(node)
    for dep in _node_dependencies(
        node=node, reachable=reachable, known_but_unloaded=known_but_unloaded,
    ):
        found = _dfs_visit(
            node=dep, reachable=reachable,
            on_stack=on_stack, on_stack_set=on_stack_set, visited=visited,
            known_but_unloaded=known_but_unloaded,
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
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]] | None:
    """First cycle from ``start`` as an ordered list (``start`` may appear at
    both ends when the cycle closes through it), or ``None`` if acyclic."""
    return _dfs_visit(
        node=start, reachable=reachable,
        on_stack=[], on_stack_set=set(), visited=set(),
        known_but_unloaded=known_but_unloaded,
    )


async def _prefetch_reachable_models(
    *,
    model: SlayerModel,
    storage: "StorageBackend",
) -> tuple[dict[str, SlayerModel], frozenset[str]]:
    """The datasource's models keyed by name (including ``model``) — the
    bidirectional closure, so refs may cross edges declared on either side. Second
    element: KNOWN names that failed to load, whose reverse joins can't be proved absent."""
    out: dict[str, SlayerModel] = {model.name: model}
    try:
        names = await storage.list_models(model.data_source)
    except Exception:
        names = [j.target_model for j in model.joins]
    unloaded: set[str] = set()
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
        else:
            unloaded.add(name)
    return out, frozenset(unloaded)


def _hop_path(*, quals, host: SlayerModel) -> tuple[str, ...]:
    """A reference's join-hop tokens with ``host``'s own name stripped; empty = host-local."""
    q = list(quals)
    if q and q[0] == host.name:
        q = q[1:]
    return tuple(q)


def _classify_hop_path(
    *, host: SlayerModel, path: tuple[str, ...], reachable: dict[str, SlayerModel],
) -> tuple[str, str] | None:
    """Classify a reference's hop ``path`` from ``host``: ``("fanning", token)`` on
    the first fanning hop (beats any later hop), ``("unproven", token)`` on the first
    hop neither provably to-one nor fanning, else ``None``. Empty/unresolvable/
    ambiguous/unloaded → ``None`` (``walk`` returns ``None`` for a target absent from
    ``reachable``, so arity is never proven on topology this prefetch cannot see)."""
    if not path:
        return None
    try:
        chain = walk(root=host, path=path, models_by_name=reachable)
    except AmbiguousJoinPathError:
        return None
    if chain is None:
        return None
    unproven: str | None = None
    for token, edge in zip(path, chain):
        target = reachable.get(edge.target_model)
        if target is None:
            return None
        if provably_fans(edge=edge, target_model=target):
            return ("fanning", token)
        if unproven is None and not provably_to_one(edge=edge, target_model=target):
            unproven = token
    return ("unproven", unproven) if unproven is not None else None


def _arity_reference_sources(column: Column) -> list[tuple[str, str]]:
    """The ``(kind, fragment)`` pairs carrying arity: a derived ``Column.sql`` and
    a ``Column.filter`` (a trivial-base ``sql`` is host-local, so it is skipped)."""
    out: list[tuple[str, str]] = []
    if column.sql is not None and not is_trivial_base(column=column):
        out.append(("sql", column.sql))
    if column.filter:
        out.append(("filter", column.filter))
    return out


def _unproven_arity_message(*, column: str, model: str, hop: str, kind: str) -> str:
    return (
        f"Derived column {column!r} on model {model!r} has a {kind} reference "
        f"crossing an unproven join hop to {hop!r} (cardinality not declared "
        f"to-one and no covering unique key): it broadcasts if aggregated as a "
        f"column of {model!r}. Declare the hop's cardinality, or aggregate the "
        f"target column ({hop}.<column>:<aggregation>) instead."
    )


def _iter_arity_refs(
    *, model: SlayerModel,
) -> Iterator[tuple[Column, str, tuple[str, ...], str]]:
    """Yield ``(column, kind, hop_path, leaf)`` for every arity-bearing reference."""
    for column in model.columns:
        for kind, fragment in _arity_reference_sources(column):
            for quals, leaf in _fragment_refs(fragment) or []:
                yield column, kind, _hop_path(quals=quals, host=model), leaf


def _check_reference_arity(
    *, model: SlayerModel, reachable: dict[str, SlayerModel],
) -> None:
    """Arity gate over the saved model's columns: a fanning-crossing reference
    raises ``DerivedColumnFanningError``; an unproven hop warns once per
    ``(column, kind, hop)``; to-one/unresolvable/unloaded/ambiguous skip."""
    warned: set[tuple[str, str, str]] = set()
    for column, kind, path, leaf in _iter_arity_refs(model=model):
        verdict = _classify_hop_path(host=model, path=path, reachable=reachable)
        if verdict is None:
            continue
        status, hop = verdict
        if status == "fanning":
            raise DerivedColumnFanningError(
                column=column.name, model=model.name, hop=hop, kind=kind,
                reference=".".join((*path, leaf)),
            )
        key = (column.name, kind, hop)
        if key in warned:
            continue
        warned.add(key)
        warnings.warn(
            _unproven_arity_message(
                column=column.name, model=model.name, hop=hop, kind=kind,
            ),
            UserWarning, stacklevel=2,
        )


async def validate_derived_columns(
    *,
    model: SlayerModel,
    storage: "StorageBackend",
) -> None:
    """Reject ``model``'s ill-formed derived columns at save time: the arity gate
    (:func:`_check_reference_arity`) then a :class:`ColumnCycleError` if any derived
    column on ``model`` or a reachable same-datasource model cycles. Best-effort;
    the compile-time / query-time guards remain authoritative."""
    reachable, known_but_unloaded = await _prefetch_reachable_models(
        model=model, storage=storage,
    )
    _check_reference_arity(model=model, reachable=reachable)
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
        cycle = _detect_cycle_dfs(
            start=root, reachable=reachable, known_but_unloaded=known_but_unloaded,
        )
        if cycle is not None:
            raise ColumnCycleError(cycle=cycle)
