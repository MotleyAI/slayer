"""Save-time well-formedness of a dataset's derived fields (arity + cycles) and filters.

A row definition (a field's ``expression`` in every form, its ``filter``, a dataset's ``filters``) reads source
columns by bare name and fields by path; each path must be a function of its declaring dataset's row (Axiom 1): it may
cross only provably to-one hops. Called
from ``StorageBackend.save_dataset`` (skipped under ``_validate=False``), this
early-failure layer rejects a path that revisits a dataset already on it
(``DerivedFieldCircularError``) or provably fans (``DerivedFieldFanningError``),
warns on an unproven hop (query-time input-safety gate is the backstop), and
rejects a derived-field cycle (``DatasetFieldCycleError``). A reference crossing an ambiguous hop is rejected
(``DerivedFieldAmbiguousError``); a save that changes the dataset's edges warns
about other datasets' definitions it breaks. Dataset filters naming a metric or a
windowed field, or reading their own dataset (``DatasetFilterCycleError``), are
refused here and, for every dataset a query reads, at query time. Best-effort, same-datasource;
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
    UnresolvableDimensionRelationshipError,
)
from slayer.core.field_expression import portable_text
from slayer.core.relationship_search import unreachable_target
from slayer.core.relationship_walker import OrientedRelationship, provably_to_one, walk, walk_miss, walk_terminal
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.deprecation import outside_stacklevel
from slayer.core.syntax import DottedRef, Ref, drop_root_hop, parse_template, walk_parsed
from slayer.engine.elaborate_env import validate_dataset_filter
from slayer.engine.cardinality.relationship_safety import provably_fans
from slayer.sql.definitions import qualified_siblings, sibling_names
from slayer.sql.field_expansion import (
    is_trivial_base,
    reads_source_column,
    reference_sites,
    resolve_ref_target,
)
from slayer.sql.reserved_keywords import prequote_reserved_identifiers
from slayer.sql.root_scope import root_scope_field_ids

# The walk only inspects ``exp.Column`` identifier shape, so sqlglot's default
# dialect keeps it independent of the dataset's runtime datasource dialect.
_DEPENDENCY_DIALECT: str | None = None

#: A definition's text and whether it is portable (Mode B) rather than native SQL.
Fragment = tuple[str, bool]


def _native(text: str) -> Fragment:
    return (text, False)


def value_fragments(field: DatasetField, *, host: SlayerDataset) -> list[Fragment]:
    """Every form of ``field``'s value that can run: its resolved native SQL, else its portable ``sql``, each dialect
    entry, and a deprecated ``sql`` (sibling references read as paths)."""
    if field.resolved:
        text = field.definition_sql
        return [] if text is None or is_trivial_base(field=field) else [_native(text)]
    out: list[Fragment] = []
    portable = portable_text(field.expression)
    if portable is not None:
        out.append((portable, True))
    out += [_native(t) for t in field.native_texts if t != field.sql]
    if field.sql is not None:
        out.append(_native(qualified_siblings(field.sql, dataset=host.name,
                                              siblings=sibling_names(host, field=field.name), dialect="postgres")))
    return out


def _portable_refs(text: str) -> list[tuple[tuple[str, ...], str]] | None:
    try:
        parsed = parse_template(text)
    except Exception:  # noqa: BLE001 — binding reports an unparseable definition
        return None
    return [((), n.name) if isinstance(n, Ref) else (tuple(n.parts[:-1]), n.parts[-1])
            for n in walk_parsed(parsed) if isinstance(n, (Ref, DottedRef))]


def _refs(fragment: Fragment, *, host: SlayerDataset) -> list[tuple[tuple[str, ...], str]]:
    """The field paths ``fragment`` reads from ``host``: its references bar source-column reads."""
    text, portable = fragment
    found = (_portable_refs(text) if portable else _fragment_refs(text)) or []
    return [(quals, leaf) for quals, leaf in found if not reads_source_column(tuple(quals), dataset=host)]


def _fragment_refs(sql: str):
    """Root-scope ``(qualifiers, leaf)`` references of a Mode-A fragment, or
    ``None`` on a parse failure (let the surface-level error surface instead)."""
    try:
        # prequote reserved qualifiers/leaves so a fragment referencing
        # a reserved joined dataset (``grant.amount``) parses cleanly here instead
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
    """Every ``(dataset, field)`` the definition or ``filter`` of ``field`` reads by path, base fields included; a
    source column it reads counts as a read of the host's physical readers of that column."""
    out: list[tuple[str, str]] = []
    for fragment in _arity_reference_sources(field, host=host):
        text, portable = fragment
        for quals, leaf in (_portable_refs(text) if portable else _fragment_refs(text)) or []:
            if reads_source_column(tuple(quals), dataset=host):
                out += [(host.name, c.name) for c in host.fields
                        if c.name != field.name and c.is_physical_reader and c.physical_name == leaf]
        for quals, leaf in _refs(fragment, host=host):
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
    """Root-scope dependency edges ``(dataset, field)`` of ``field`` — every form of its
    definition and its ``filter``, pointing at any field a path reads that itself needs
    expansion (a path to its own field is a cycle; a dotted filter path that does not
    walk from ``host`` fails the save)."""
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
    """Edges from every form of a field's definition to each expanding field a path reads."""
    deps: list[tuple[str, str]] = []
    for fragment in value_fragments(field, host=host):
        for quals, leaf in _refs(fragment, host=host):
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
    """Edges from a ``DatasetField.filter`` to every expanding field a path reads (its own field by path
    is a cycle; a bare own name reads the source column)."""
    if not field.filter:
        return []
    deps: list[tuple[str, str]] = []
    for quals, leaf in _refs(_native(field.filter), host=host):
        target = _filter_ref_target(
            field=field, host=host, quals=quals, leaf=leaf, reachable=reachable,
            known_but_unloaded=known_but_unloaded,
        )
        if target is None:
            continue  # a real relationship hop whose target is not loaded here — skip
        col = target.get_field(leaf, quoted=True)
        if col is not None and col.needs_expansion:
            deps.append((target.name, col.name))
    return deps


def _filter_ref_target(
    *, field: DatasetField, host: SlayerDataset, quals, leaf: str,
    reachable: dict[str, SlayerDataset],
    known_but_unloaded: frozenset[str] = frozenset(),
) -> SlayerDataset | None:
    """The dataset a filter reference resolves to; a leading hop naming no relationship edge
    on ``host`` fails the save (a known-but-unloaded target returns ``None`` — its
    reverse relationship may exist but is invisible here — only an unknown token raises).
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
        raise UnresolvableDimensionRelationshipError(
            reference=".".join((*quals, leaf)), root_dataset=host.name,
            reason=f"the filter of field {field.name!r} names {path[0]!r}, which does not resolve to a joined dataset "
                   f"of {host.name!r}.",
        )
    return None


def _node_dependencies(
    *,
    node: tuple[str, str],
    reachable: dict[str, SlayerDataset],
    known_but_unloaded: frozenset[str] = frozenset(),
) -> list[tuple[str, str]]:
    """Dependency edges leaving ``node = (dataset, col)``; empty when the dataset or
    field is missing (a dead-end, not an error)."""
    dataset_name, col_name = node
    host = reachable.get(dataset_name)
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
    """A reference's relationship-hop tokens with ``host``'s own name dropped; empty = host-local."""
    return drop_root_hop(quals, host.spelling, head_quoted=True)


def _classify_hop_path(
    *, host: SlayerDataset, path: tuple[str, ...], reachable: dict[str, SlayerDataset],
) -> tuple[str, str] | None:
    """Classify a reference's hop ``path`` from ``host``: ``("fanning", token)`` on
    the first fanning hop (beats any later hop), ``("unproven", token)`` on the first
    hop neither provably to-one nor fanning, else ``None``. Empty/unresolvable/
    unloaded → ``None`` (``walk`` returns ``None`` for a target absent from
    ``reachable``, so arity is never proven on topology this prefetch cannot see); a
    revisit propagates its ``CircularRelationshipPathError`` (caught one level up)."""
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


def _arity_reference_sources(field: DatasetField, *, host: SlayerDataset) -> list[Fragment]:
    """The fragments carrying arity: every form of the field's definition and its ``filter``."""
    return [*value_fragments(field, host=host), *([_native(field.filter)] if field.filter else [])]


def _unproven_arity_message(*, field: str, dataset: str, hop: str) -> str:
    return (
        f"Derived field {field!r} on dataset {dataset!r} has a reference crossing "
        f"an unproven relationship hop to {hop!r} (cardinality not declared to-one and no "
        f"covering unique key): aggregating it as a field of {dataset!r} fails "
        f"at query time. Declare the hop's cardinality, or aggregate the target field "
        f"(<aggregation>({hop}.<field>)) instead."
    )


def _iter_arity_refs(
    *, dataset: SlayerDataset,
) -> Iterator[tuple[DatasetField, tuple[str, ...], str, tuple[str, ...]]]:
    """Yield ``(field, hop_path, leaf, quals)`` for every arity-bearing reference —
    ``quals`` is the raw pre-strip spelling, for the error's ``reference``."""
    for field in dataset.fields:
        for fragment in _arity_reference_sources(field, host=dataset):
            for quals, leaf in _refs(fragment, host=dataset):
                yield field, _hop_path(quals=quals, host=dataset), leaf, tuple(quals)


def _check_reference_arity(
    *, dataset: SlayerDataset, reachable: dict[str, SlayerDataset],
) -> None:
    """Arity gate over the saved dataset's fields: a revisiting reference raises
    ``DerivedFieldCircularError`` (the walk aborts before arity is judged, so it
    precedes any fanning verdict); a fanning-crossing reference raises
    ``DerivedFieldFanningError``; an unproven hop warns once per
    ``(field, hop)``; to-one/unresolvable/unloaded/ambiguous skip."""
    warned: set[tuple[str, str]] = set()
    for field, path, leaf, quals in _iter_arity_refs(dataset=dataset):
        reference = ".".join((*quals, leaf))
        try:
            verdict = _classify_hop_path(host=dataset, path=path, reachable=reachable)
        except CircularRelationshipPathError as exc:
            raise DerivedFieldCircularError(
                field=field.name, reference=reference, root_dataset=dataset.name,
                revisited=exc.revisited, hop=exc.hop, via=exc.via,
            ) from exc
        if verdict is None:
            continue
        status, hop = verdict
        if status == "fanning":
            raise DerivedFieldFanningError(
                field=field.name, dataset=dataset.name, hop=hop, reference=reference,
            )
        key = (field.name, hop)
        if key in warned:
            continue
        warned.add(key)
        warnings.warn(
            _unproven_arity_message(field=field.name, dataset=dataset.name, hop=hop),
            UserWarning, stacklevel=2,
        )


def _definitions(dataset: SlayerDataset) -> Iterator[tuple[DatasetField | None, Fragment]]:
    """``(field or None for a dataset filter, fragment)`` for every row definition on ``dataset``."""
    for field in dataset.fields:
        for fragment in _arity_reference_sources(field, host=dataset):
            yield field, fragment
    for sql in dataset.filters or []:
        yield None, _native(sql)


def _definition_label(*, field: DatasetField | None, fragment: Fragment) -> str:
    return f"field {field.name!r}" if field is not None else f"dataset filter {fragment[0]!r}"


def definition_heads(datasets: list[SlayerDataset]) -> list[str]:
    """The first relationship hops ``datasets``' row definitions and metrics spell, deduplicated in order."""
    heads = (
        path[0]
        for dataset in datasets
        for _field, fragment in _definitions(dataset)
        for quals, _leaf in _refs(fragment, host=dataset)
        if (path := _hop_path(quals=quals, host=dataset))
    )
    return list(dict.fromkeys(heads))


def check_definition_reach(*, dataset: SlayerDataset, reachable: dict[str, SlayerDataset]) -> None:
    """Reject a row definition on ``dataset`` naming a dataset no relationship path reaches from it."""
    datasets = {**reachable, dataset.name: dataset}
    for field, fragment in _definitions(dataset):
        for quals, leaf in _refs(fragment, host=dataset):
            path = _hop_path(quals=quals, host=dataset)
            unreached = unreachable_target(root=dataset, token=path[0], datasets_by_name=datasets) if path else None
            if unreached is not None:
                raise UnreachableDefinitionError(
                    dataset=dataset.name, reference=".".join((*quals, leaf)), unreached=unreached,
                    definition=_definition_label(field=field, fragment=fragment),
                )


def check_definition_ambiguity(*, dataset: SlayerDataset, reachable: dict[str, SlayerDataset]) -> None:
    """Reject a Mode-A definition on ``dataset`` whose path crosses an ambiguous hop: a field's
    as :class:`DerivedFieldAmbiguousError`, a dataset filter's as the base error naming it. A
    revisit met first on the path is left to the circular verdict."""
    datasets = {**reachable, dataset.name: dataset}
    for field, fragment in _definitions(dataset):
        for quals, leaf in _refs(fragment, host=dataset):
            reference = ".".join((*quals, leaf))
            try:
                walk(root=dataset, path=_hop_path(quals=quals, host=dataset), datasets_by_name=datasets)
            except CircularRelationshipPathError:
                continue
            except AmbiguousRelationshipPathError as exc:
                if field is not None:
                    raise DerivedFieldAmbiguousError(
                        field=field.name, dataset=dataset.name, reference=reference, error=exc,
                    ) from exc
                raise AmbiguousRelationshipPathError(
                    source_dataset=exc.source_dataset, target_dataset=exc.target_dataset,
                    candidates=exc.candidates, token=exc.token,
                    context=f"Dataset filter {fragment[0]!r} on dataset {dataset.name!r} references {reference!r}",
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
        return (f"crosses {exc.source_dataset!r}↔{exc.target_dataset!r}, which two or more edges now connect; "
                f"spell that hop by an edge name")
    except CircularRelationshipPathError:
        return None
    assert miss is not None
    return (f"is no longer resolvable: the hop {miss.token!r} no longer exists; reconnect it, "
            f"or spell the path through the remaining relationships (by edge name where a pair has several)")


def warn_broken_dependents(
    *, dataset: SlayerDataset, stored: SlayerDataset | None, peers: dict[str, SlayerDataset],
) -> None:
    """When ``dataset``'s edges differ from ``stored``, warn once per other dataset's Mode-A
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
                f"Saving dataset {dataset.name!r}: {label} on dataset {name!r} references {reference!r}, which {broken}.",
                UserWarning, stacklevel=outside_stacklevel(),
            )


def _broken_references(
    *, host: SlayerDataset, before: dict[str, SlayerDataset], after: dict[str, SlayerDataset],
) -> Iterator[tuple[str, str, str]]:
    """``(definition label, reference, how it broke)`` for each of ``host``'s references broken by the save."""
    for field, fragment in _definitions(host):
        for quals, leaf in _refs(fragment, host=host):
            path = _hop_path(quals=quals, host=host)
            broken = _ref_break(host=host, path=path, before=before, after=after) if path else None
            if broken is not None:
                yield _definition_label(field=field, fragment=fragment), ".".join((*quals, leaf)), broken


def validate_derived_fields(
    *,
    dataset: SlayerDataset,
    peers: dict[str, SlayerDataset],
    unloaded: frozenset[str],
) -> None:
    """Reject ``dataset``'s ill-formed derived fields at save time: the arity gate
    (:func:`_check_reference_arity`) then a :class:`DatasetFieldCycleError` if any derived
    field on ``dataset`` or a same-datasource peer cycles. ``unloaded`` names peers that
    failed to load, whose reverse relationships can't be proved absent. Best-effort; the
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
    """``(owner, leaf, datasets on its path)`` for each field path ``fragment`` reads from ``host``,
    recursing through the derived fields it names; unresolvable references are skipped."""
    yield from _references(_native(fragment), host=host, reachable=reachable, via=(), seen=set())


def _resolve_owner(
    *, quals, host: SlayerDataset, reachable: dict[str, SlayerDataset],
) -> tuple[SlayerDataset, list[OrientedRelationship]] | None:
    """The dataset owning a reference and the chain reaching it, else ``None`` when unresolvable."""
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
    fragment: Fragment, *, host: SlayerDataset, reachable: dict[str, SlayerDataset], via: tuple[str, ...],
    seen: set[tuple[str, str]],
) -> Iterator[tuple[SlayerDataset, str, tuple[str, ...]]]:
    for quals, leaf in _refs(fragment, host=host):
        resolved = _resolve_owner(quals=quals, host=host, reachable=reachable)
        if resolved is None:
            continue
        owner, chain = resolved
        on_path = (*via, *(edge.to for edge in chain))
        yield owner, leaf, on_path
        field = owner.get_field(leaf, quoted=True)
        if field is None or (owner.name, leaf) in seen:
            continue
        seen.add((owner.name, leaf))
        for inner in _arity_reference_sources(field, host=owner):
            yield from _references(inner, host=owner, reachable=reachable, via=on_path, seen=seen)


def _filtered_reads(dataset: SlayerDataset, *, reachable: dict[str, SlayerDataset]) -> list[str]:
    """The datasets ``dataset``'s filters read: every dataset on every path they cross."""
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
    """Refuse ``dataset``'s filters naming a metric or windowed field (``DatasetFilterError``) or
    reading its own dataset (:class:`DatasetFilterCycleError`, only a cycle through ``dataset``)."""
    if not dataset.filters:
        return
    datasets = {**reachable, dataset.name: dataset}
    for fragment in dataset.filters:
        validate_dataset_filter(mf=fragment, references=(
            (owner, leaf) for owner, leaf, _path in filter_references(fragment, host=dataset, reachable=datasets)
        ))
    cycle = _filter_cycle(dataset=dataset, reachable=datasets)
    if cycle is not None:
        raise DatasetFilterCycleError(cycle)
