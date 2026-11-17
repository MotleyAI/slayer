"""Save-time well-formedness of a model's derived columns (arity + cycles) and filters.

A derived ``Column.sql`` / ``Column.filter`` reference must be a function of its
declaring model's row (Axiom 1): it may cross only provably to-one hops. Called
from ``StorageBackend.save_dataset`` (skipped under ``_validate=False``), this
early-failure layer rejects a path that revisits a model already on it
(``DerivedColumnCircularError``) or provably fans (``DerivedColumnFanningError``),
warns on an unproven hop (query-time input-safety gate is the backstop), and
rejects a derived-column cycle (``ColumnCycleError``). A reference crossing an ambiguous hop is rejected
(``DerivedColumnAmbiguousError``); a save that changes the model's edges warns
about other models' definitions it breaks. Model filters naming a measure or a
windowed column, or reading their own dataset (``ModelFilterCycleError``), are
refused here and, for every model a query reads, at query time. Best-effort, same-datasource;
unresolved/unloaded targets are skipped and the compile-time/query-time guards
remain authoritative.
"""
from __future__ import annotations

import warnings
from collections.abc import Iterator

import sqlglot

from slayer.core.errors import (
    AmbiguousRelationshipPathError,
    CircularRelationshipPathError,
    DatasetFieldCycleError,
    DerivedFieldAmbiguousError,
    DerivedFieldCircularError,
    DerivedFieldFanningError,
    DatasetFilterCycleError,
    UnreachableDefinitionError,
)
from slayer.core.relationship_search import unreachable_target
from slayer.core.relationship_walker import OrientedRelationship, provably_to_one, walk, walk_miss, walk_terminal
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.syntax import drop_root_hop
from slayer.engine.elaborate_env import validate_dataset_filter
from slayer.engine.cardinality.relationship_safety import provably_fans
from slayer.sql.field_expansion import (
    is_trivial_base,
    reference_sites,
    root_scope_field_ids,
    resolve_ref_target,
)
from slayer.sql.reserved_keywords import prequote_reserved_identifiers

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
            parsed, root_scope_field_ids(parsed=parsed),  # pyright: ignore[reportArgumentType]
        )
    ]


def definition_reads(
    *, field: DatasetField, host: SlayerDataset, reachable: dict[str, SlayerDataset],
) -> list[tuple[str, str]]:
    """Every ``(model, column)`` the ``Column.sql`` / ``Column.filter`` of ``field`` names, base columns included."""
    out: list[tuple[str, str]] = []
    texts = [] if field.sql is None or is_trivial_base(field=field) else [field.sql]
    for text in [*texts, *([field.filter] if field.filter else [])]:
        for quals, leaf in _fragment_refs(text) or []:
            target = resolve_ref_target(qualifiers=quals, source_dataset=host, datasets_by_name=reachable)
            if target is not None and target.get_field(leaf, quoted=True) is not None:
                out.append((target.name, leaf))
    return out


def _field_dependencies(
    *,
    field: DatasetField,
    host: SlayerDataset,
    reachable: dict[str, SlayerDataset],
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]]:
    """Root-scope dependency edges ``(model, column)`` of ``field`` — both its
    ``Column.sql`` and its ``Column.filter``, pointing at any referenced column
    that itself needs expansion (a filter naming its own column is a cycle; a
    dotted filter path that does not walk from ``host`` fails the save)."""
    return [
        *_sql_dependencies(field=field, host=host, reachable=reachable),
        *_filter_dependencies(
            field=field, host=host, reachable=reachable,
            known_but_unloaded=known_but_unloaded,
        ),
    ]


def _sql_dependencies(
    *, field: DatasetField, host: SlayerDataset, reachable: dict[str, SlayerDataset],
) -> list[tuple[str, str]]:
    """Edges from a derived ``Column.sql`` to every expanding column it names."""
    if field.sql is None or is_trivial_base(field=field):
        return []
    deps: list[tuple[str, str]] = []
    for quals, leaf in _fragment_refs(field.sql) or []:
        target = resolve_ref_target(
            qualifiers=quals, source_dataset=host, datasets_by_name=reachable,
        )
        if target is None:
            continue
        col = target.get_field(leaf, quoted=True)
        if col is not None and col.needs_expansion:
            deps.append((target.name, col.name))
    return deps


def _filter_dependencies(
    *, field: DatasetField, host: SlayerDataset, reachable: dict[str, SlayerDataset],
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]]:
    """Edges from a ``Column.filter`` to every expanding column it names; a
    trivial-base self-reference reads the physical column, not a cycle."""
    if not field.filter:
        return []
    deps: list[tuple[str, str]] = []
    for quals, leaf in _fragment_refs(field.filter) or []:
        target = _filter_ref_target(
            field=field, host=host, quals=quals, leaf=leaf, reachable=reachable,
            known_but_unloaded=known_but_unloaded,
        )
        if target is None:
            continue  # a real join hop whose target is not loaded here — skip
        col = target.get_field(leaf, quoted=True)
        if col is None or not col.needs_expansion:
            continue
        if (
            target.name == host.name and leaf == field.name
            and is_trivial_base(field=field)
        ):
            continue
        deps.append((target.name, col.name))
    return deps


def _filter_ref_target(
    *, field: DatasetField, host: SlayerDataset, quals, leaf: str,
    reachable: dict[str, SlayerDataset],
    known_but_unloaded: frozenset[str] = frozenset(),
) -> SlayerDataset | None:
    """The model a filter reference resolves to; a leading hop naming no join edge
    on ``host`` fails the save (a known-but-unloaded target returns ``None`` — its
    reverse join may exist but is invisible here — only an unknown token raises).
    An ambiguous or revisiting path is judged elsewhere and skipped here."""
    path = _hop_path(quals=quals, host=host)
    if not path:
        return host
    try:
        walked = walk_terminal(root=host, path=path, datasets_by_name=reachable)
        miss = walk_miss(root=host, path=path, datasets_by_name=reachable) if walked is None else None
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        return None
    if walked is not None:
        return walked[0]
    if miss is not None and miss.index == 0 and miss.kind == "no_edge" and path[0] not in known_but_unloaded:
        raise ValueError(
            f"Column {field.name!r} on model {host.name!r} has a filter "
            f"referencing {'.'.join((*quals, leaf))!r}, which does not "
            f"resolve to a joined model from {host.name!r}."
        )
    return None


def _node_dependencies(
    *,
    node: tuple[str, str],
    reachable: dict[str, SlayerDataset],
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]]:
    """Dependency edges leaving ``node = (model, col)``; empty when the model or
    column is missing (a dead-end, not an error)."""
    model_name, col_name = node
    host = reachable.get(model_name)
    if host is None:
        return []
    col = host.get_field(col_name, quoted=True)
    if col is None:
        return []
    return _field_dependencies(
        field=col, host=host, reachable=reachable,
        known_but_unloaded=known_but_unloaded,
    )


def _dfs_visit(
    *,
    node: tuple[str, str],
    reachable: dict[str, SlayerDataset],
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
    reachable: dict[str, SlayerDataset],
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]] | None:
    """First cycle from ``start`` as an ordered list (``start`` may appear at
    both ends when the cycle closes through it), or ``None`` if acyclic."""
    return _dfs_visit(
        node=start, reachable=reachable,
        on_stack=[], on_stack_set=set(), visited=set(),
        known_but_unloaded=known_but_unloaded,
    )


def _hop_path(*, quals, host: SlayerDataset) -> tuple[str, ...]:
    """A reference's join-hop tokens with ``host``'s own name dropped; empty = host-local."""
    return drop_root_hop(quals, host.spelling, head_quoted=True)


def _classify_hop_path(
    *, host: SlayerDataset, path: tuple[str, ...], reachable: dict[str, SlayerDataset],
) -> tuple[str, str] | None:
    """Classify a reference's hop ``path`` from ``host``: ``("fanning", token)`` on
    the first fanning hop (beats any later hop), ``("unproven", token)`` on the first
    hop neither provably to-one nor fanning, else ``None``. Empty/unresolvable/
    unloaded → ``None`` (``walk`` returns ``None`` for a target absent from
    ``reachable``, so arity is never proven on topology this prefetch cannot see); a
    revisit propagates its ``CircularJoinPathError`` (caught one level up)."""
    if not path:
        return None
    chain = walk(root=host, path=path, datasets_by_name=reachable)
    if chain is None:
        return None
    unproven: str | None = None
    for token, edge in zip(path, chain):
        target = reachable.get(edge.to)
        if target is None:
            return None
        if provably_fans(edge=edge, target_dataset=target):
            return ("fanning", token)
        if unproven is None and not provably_to_one(edge=edge, target_dataset=target):
            unproven = token
    return ("unproven", unproven) if unproven is not None else None


def _arity_reference_sources(field: DatasetField) -> list[str]:
    """The fragments carrying arity: a derived ``Column.sql`` and a ``Column.filter``
    (a trivial-base ``sql`` is host-local, so it is skipped)."""
    out: list[str] = []
    if field.sql is not None and not is_trivial_base(field=field):
        out.append(field.sql)
    if field.filter:
        out.append(field.filter)
    return out


def _unproven_arity_message(*, field: str, dataset: str, hop: str) -> str:
    return (
        f"Derived column {field!r} on model {dataset!r} has a reference crossing "
        f"an unproven join hop to {hop!r} (cardinality not declared to-one and no "
        f"covering unique key): aggregating it as a column of {dataset!r} fails "
        f"at query time. Declare the hop's cardinality, or aggregate the target column "
        f"(<aggregation>({hop}.<column>)) instead."
    )


def _iter_arity_refs(
    *, dataset: SlayerDataset,
) -> Iterator[tuple[DatasetField, tuple[str, ...], str, tuple[str, ...]]]:
    """Yield ``(column, hop_path, leaf, quals)`` for every arity-bearing reference —
    ``quals`` is the raw pre-strip spelling, for the error's ``reference``."""
    for column in dataset.fields:
        for fragment in _arity_reference_sources(column):
            for quals, leaf in _fragment_refs(fragment) or []:
                yield column, _hop_path(quals=quals, host=dataset), leaf, tuple(quals)


def _check_reference_arity(
    *, dataset: SlayerDataset, reachable: dict[str, SlayerDataset],
) -> None:
    """Arity gate over the saved model's columns: a revisiting reference raises
    ``DerivedColumnCircularError`` (the walk aborts before arity is judged, so it
    precedes any fanning verdict); a fanning-crossing reference raises
    ``DerivedColumnFanningError``; an unproven hop warns once per
    ``(column, hop)``; to-one/unresolvable/unloaded/ambiguous skip."""
    warned: set[tuple[str, str]] = set()
    for column, path, leaf, quals in _iter_arity_refs(dataset=dataset):
        reference = ".".join((*quals, leaf))
        try:
            verdict = _classify_hop_path(host=dataset, path=path, reachable=reachable)
        except CircularRelationshipPathError as exc:
            raise DerivedFieldCircularError(
                field=column.name, reference=reference, root_dataset=dataset.name,
                revisited=exc.revisited, hop=exc.hop, via=exc.via,
            ) from exc
        if verdict is None:
            continue
        status, hop = verdict
        if status == "fanning":
            raise DerivedFieldFanningError(
                field=column.name, dataset=dataset.name, hop=hop, reference=reference,
            )
        key = (column.name, hop)
        if key in warned:
            continue
        warned.add(key)
        warnings.warn(
            _unproven_arity_message(field=column.name, dataset=dataset.name, hop=hop),
            UserWarning, stacklevel=2,
        )


def _definitions(dataset: SlayerDataset) -> Iterator[tuple[DatasetField | None, str]]:
    """``(column or None for a model filter, fragment)`` for every Mode-A definition on ``dataset``."""
    for column in dataset.fields:
        for fragment in _arity_reference_sources(column):
            yield column, fragment
    for sql in dataset.filters or []:
        yield None, sql


def _definition_label(*, field: DatasetField | None, fragment: str) -> str:
    return f"field {field.name!r}" if field is not None else f"dataset filter {fragment!r}"


def definition_heads(datasets: list[SlayerDataset]) -> list[str]:
    """The first join hops ``datasets``' Mode-A definitions spell, deduplicated in order."""
    heads = (
        path[0]
        for model in datasets
        for _column, fragment in _definitions(model)
        for quals, _leaf in _fragment_refs(fragment) or []
        if (path := _hop_path(quals=quals, host=model))
    )
    return list(dict.fromkeys(heads))


def check_definition_reach(*, dataset: SlayerDataset, reachable: dict[str, SlayerDataset]) -> None:
    """Reject a Mode-A definition on ``dataset`` naming a model no join path reaches from it."""
    models = {**reachable, dataset.name: dataset}
    for column, fragment in _definitions(dataset):
        for quals, leaf in _fragment_refs(fragment) or []:
            path = _hop_path(quals=quals, host=dataset)
            unreached = unreachable_target(root=dataset, token=path[0], datasets_by_name=models) if path else None
            if unreached is not None:
                raise UnreachableDefinitionError(
                    dataset=dataset.name, reference=".".join((*quals, leaf)), unreached=unreached,
                    definition=_definition_label(field=column, fragment=fragment),
                )


def check_definition_ambiguity(*, dataset: SlayerDataset, reachable: dict[str, SlayerDataset]) -> None:
    """Reject a Mode-A definition on ``dataset`` whose path crosses an ambiguous hop: a column's
    as :class:`DerivedColumnAmbiguousError`, a model filter's as the base error naming it. A
    revisit met first on the path is left to the circular verdict."""
    models = {**reachable, dataset.name: dataset}
    for column, fragment in _definitions(dataset):
        for quals, leaf in _fragment_refs(fragment) or []:
            reference = ".".join((*quals, leaf))
            try:
                walk(root=dataset, path=_hop_path(quals=quals, host=dataset), datasets_by_name=models)
            except CircularRelationshipPathError:
                continue
            except AmbiguousRelationshipPathError as exc:
                if column is not None:
                    raise DerivedFieldAmbiguousError(
                        field=column.name, dataset=dataset.name, reference=reference, error=exc,
                    ) from exc
                raise AmbiguousRelationshipPathError(
                    source_dataset=exc.source_model, target_dataset=exc.target_model,
                    candidates=exc.candidates, token=exc.token,
                    context=f"Model filter {fragment!r} on model {dataset.name!r} references {reference!r}",
                ) from exc


def _ref_break(
    *, host: SlayerDataset, path: tuple[str, ...], before: dict[str, SlayerDataset], after: dict[str, SlayerDataset],
) -> str | None:
    """How a reference path that resolved under ``before`` breaks under ``after``, else ``None``."""
    try:
        if walk_terminal(root=before[host.name], path=path, datasets_by_name=before) is None:
            return None
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        return None
    try:
        if walk_terminal(root=after[host.name], path=path, datasets_by_name=after) is not None:
            return None
        miss = walk_miss(root=after[host.name], path=path, datasets_by_name=after)
    except AmbiguousRelationshipPathError as exc:
        return (f"crosses {exc.source_model!r}↔{exc.target_model!r}, which two or more edges now connect; "
                f"spell that hop by an edge name")
    except CircularRelationshipPathError:
        return None
    assert miss is not None
    return (f"is no longer resolvable: the hop {miss.token!r} no longer exists; reconnect it, "
            f"or spell the path through the remaining joins (by edge name where a pair has several)")


def warn_broken_dependents(
    *, dataset: SlayerDataset, stored: SlayerDataset | None, peers: dict[str, SlayerDataset],
) -> None:
    """When ``dataset``'s edges differ from ``stored``, warn once per other model's Mode-A
    definition reference that resolved before the save and breaks after it."""
    def edge_set(m: SlayerDataset | None) -> list[str]:
        return sorted(j.model_dump_json() for j in (m.relationships if m is not None else []))

    if edge_set(dataset) == edge_set(stored):
        return
    others = {n: p for n, p in peers.items() if n != dataset.name}
    before = {**others, **({dataset.name: stored} if stored is not None else {})}
    after = {**others, dataset.name: dataset}
    for name in sorted(others):
        for label, reference, broken in _broken_references(host=others[name], before=before, after=after):
            warnings.warn(
                f"Saving model {dataset.name!r}: {label} on model {name!r} references {reference!r}, which {broken}.",
                UserWarning, stacklevel=3,
            )


def _broken_references(
    *, host: SlayerDataset, before: dict[str, SlayerDataset], after: dict[str, SlayerDataset],
) -> Iterator[tuple[str, str, str]]:
    """``(definition label, reference, how it broke)`` for each of ``host``'s references broken by the save."""
    for column, fragment in _definitions(host):
        for quals, leaf in _fragment_refs(fragment) or []:
            path = _hop_path(quals=quals, host=host)
            broken = _ref_break(host=host, path=path, before=before, after=after) if path else None
            if broken is not None:
                yield _definition_label(field=column, fragment=fragment), ".".join((*quals, leaf)), broken


def validate_derived_fields(
    *,
    dataset: SlayerDataset,
    peers: dict[str, SlayerDataset],
    unloaded: frozenset[str],
) -> None:
    """Reject ``dataset``'s ill-formed derived columns at save time: the arity gate
    (:func:`_check_reference_arity`) then a :class:`ColumnCycleError` if any derived
    column on ``dataset`` or a same-datasource peer cycles. ``unloaded`` names peers that
    failed to load, whose reverse joins can't be proved absent. Best-effort; the
    compile-time / query-time guards remain authoritative."""
    reachable = {**peers, dataset.name: dataset}
    check_definition_reach(dataset=dataset, reachable=reachable)
    check_definition_ambiguity(dataset=dataset, reachable=reachable)
    _check_reference_arity(dataset=dataset, reachable=reachable)
    # Iterate roots in a deterministic order so the reported cycle is
    # stable across runs.
    roots: list[tuple[str, str]] = []
    for entity_name in sorted(reachable.keys()):
        entity = reachable[entity_name]
        for col in entity.fields:
            if not col.needs_expansion:
                continue
            roots.append((entity_name, col.name))
    for root in roots:
        cycle = _detect_cycle_dfs(
            start=root, reachable=reachable, known_but_unloaded=unloaded,
        )
        if cycle is not None:
            raise DatasetFieldCycleError(cycle=cycle)


def filter_references(
    fragment: str, *, host: SlayerDataset, reachable: dict[str, SlayerDataset],
) -> Iterator[tuple[SlayerDataset, str, tuple[str, ...]]]:
    """``(owner, leaf, models on its path)`` for each reference ``fragment`` makes from ``host``,
    recursing through the derived columns it names; unresolvable references are skipped."""
    yield from _references(fragment, host=host, reachable=reachable, via=(), seen=set())


def _resolve_owner(
    *, quals, host: SlayerDataset, reachable: dict[str, SlayerDataset],
) -> tuple[SlayerDataset, list[OrientedRelationship]] | None:
    """The model owning a reference and the chain reaching it, else ``None`` when unresolvable."""
    path = _hop_path(quals=quals, host=host)
    try:
        chain = walk(root=host, path=path, datasets_by_name=reachable) if path else []
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        return None
    if chain is None:
        return None
    owner = reachable.get(chain[-1].to) if chain else host
    return None if owner is None else (owner, chain)


def _references(
    fragment: str, *, host: SlayerDataset, reachable: dict[str, SlayerDataset], via: tuple[str, ...],
    seen: set[tuple[str, str]],
) -> Iterator[tuple[SlayerDataset, str, tuple[str, ...]]]:
    for quals, leaf in _fragment_refs(fragment) or []:
        resolved = _resolve_owner(quals=quals, host=host, reachable=reachable)
        if resolved is None:
            continue
        owner, chain = resolved
        on_path = (*via, *(edge.to for edge in chain))
        yield owner, leaf, on_path
        column = owner.get_field(leaf, quoted=True)
        if column is None or (owner.name, leaf) in seen:
            continue
        seen.add((owner.name, leaf))
        for inner in _arity_reference_sources(column):
            yield from _references(inner, host=owner, reachable=reachable, via=on_path, seen=seen)


def _filtered_reads(dataset: SlayerDataset, *, reachable: dict[str, SlayerDataset]) -> list[str]:
    """The models whose datasets ``dataset``'s filters read: every model on every path they cross."""
    return list(dict.fromkeys(
        name
        for fragment in dataset.filters
        for _owner, _leaf, path in filter_references(fragment, host=dataset, reachable=reachable)
        for name in path
    ))


def _filter_cycle(*, dataset: SlayerDataset, reachable: dict[str, SlayerDataset]) -> list[str] | None:
    """The first chain of filtered datasets from ``dataset`` back to itself, else ``None``."""
    stack = [dataset.name]
    done: set[str] = set()

    def visit(name: str) -> list[str] | None:
        current = reachable.get(name)
        for dep in _filtered_reads(current, reachable=reachable) if current is not None else ():
            if dep == dataset.name:
                return [*stack, dep]
            if dep in done or dep in stack:
                continue
            stack.append(dep)
            found = visit(dep)
            stack.pop()
            if found is not None:
                return found
            done.add(dep)
        return None

    return visit(dataset.name)


def check_dataset_filters(*, dataset: SlayerDataset, reachable: dict[str, SlayerDataset]) -> None:
    """Refuse ``dataset``'s filters naming a measure or windowed column (``ModelFilterError``) or
    reading its own dataset (:class:`ModelFilterCycleError`, only a cycle through ``dataset``)."""
    if not dataset.filters:
        return
    models = {**reachable, dataset.name: dataset}
    for fragment in dataset.filters:
        validate_dataset_filter(mf=fragment, references=(
            (owner, leaf) for owner, leaf, _path in filter_references(fragment, host=dataset, reachable=models)
        ))
    cycle = _filter_cycle(dataset=dataset, reachable=models)
    if cycle is not None:
        raise DatasetFilterCycleError(cycle)
