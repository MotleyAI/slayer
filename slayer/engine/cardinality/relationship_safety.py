"""Relationship-arity safety: a hop is *provably many-to-one* iff, on its traversal
orientation, its target-side fields cover a declared PK/unique set or its
oriented cardinality is ``many_to_one``/``one_to_one``. Unknown = unsafe.
Both orientations of every declared edge participate; proof is per orientation,
so one direction may be provably to-one while the other fans out."""

from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TypeVar, cast

from pydantic import BaseModel

from slayer.core.enums import RelationshipCardinality
from slayer.core.errors import AmbiguousRelationshipPathError, UnresolvableDimensionRelationshipError
from slayer.core.relationship_search import HopFilter, routes
from slayer.core.relationship_walker import (
    OrientedRelationship,
    OrientedLike,
    canonical_path,
    edges_between,
    follow,
    is_product_edge,
    provably_to_one,
    resolve_hop,
    reverse_token,
    unique_key_sets,
)
from slayer.core.time_spine import TIME_SPINE_FIELD, TIME_SPINE_DATASET, axis_field, is_spine
from slayer.core.keys import (
    is_ranked,
    SLOT_COMPOSITE_KINDS,
    AggregateKey,
    FieldKey,
    FieldSqlKey,
    Grain,
    LiteralKey,
    SqlFragmentKey,
    StarKey,
    TimeTruncKey,
    ValueKey,
    reroot_value_key,
    source_anchor_path,
    substitute_value_keys,
    walk_value_keys,
    window_kwarg_of,
)
from slayer.core.models import DatasetRelationship, SlayerDataset, relationship_key_error
from slayer.core.refs import key_display
from slayer.core.scope import DatasetScope
from slayer.engine.reference_closure import (
    aggregate_input_closure,
    field_default_key,
    definition_refs,
    key_closure,
)
from slayer.engine.elaborate_env import check_partition_key_attributable
from slayer.engine.cardinality.unrelated import crosses_product_hop
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.ir.terms import AggregateHomes
from slayer.engine.cardinality.inference import (
    CardinalityVerdict,
    RelationshipCardinalityFinding,
    RelationshipCardinalityReport,
)

__all__ = [
    "provably_fans",
    "safe_reachable",
    "may_inline_crossing_inputs",
    "audit_relationship_safety",
    "RelationshipSafetyFinding",
]


def may_inline_crossing_inputs(crossed_paths: Sequence[tuple]) -> bool:  # NOSONAR(S1172) — crossed_paths is the documented seam; the cardinality-aware decision reads it, hardcoded False until then.
    """Whether a crossing-input local aggregate may stay inline in the host base.
    Hardcoded ``False`` (always a producer); the seam that will flip."""
    return False


def provably_fans(*, edge: OrientedLike, target_dataset: SlayerDataset) -> bool:
    """Is ``edge`` provably fanning onto ``to`` in its orientation?
    True iff it is NOT provably many-to-one and its declared cardinality is
    ``one_to_many``/``many_to_many`` — proof beats a contradictory to-many
    declaration, so a reverse-PK-covered or undeclared hop is unproven, not
    fanning (absence of a target unique key permits a fan but does not prove one)."""
    if provably_to_one(edge=edge, target_dataset=target_dataset):
        return False
    return edge.cardinality in (
        RelationshipCardinality.ONE_TO_MANY, RelationshipCardinality.MANY_TO_MANY,
    )


def safe_reachable(
    *,
    root: SlayerDataset,
    path: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
) -> bool:
    """Is the (possibly composed) ``path`` a reachable chain of provably many-to-one hops
    from ``root``? Any declared edge traverses in either orientation; an unresolvable
    hop fails the walk. Empty path is safe. An ambiguous hop raises."""
    chain = follow(root=root, path=path, datasets_by_name=datasets_by_name)
    if chain is None:
        return False
    for edge in chain:
        target_dataset = datasets_by_name.get(edge.to)
        if target_dataset is None or not provably_to_one(
            edge=edge, target_dataset=target_dataset
        ):
            return False
    return True


class RelationshipSafetyFinding(BaseModel):
    """One audit record per declared edge: the provability of both orientations
    plus, when the declared (forward) orientation is unproven, its
    consequence and remedies. Detection contradictions are separate findings."""

    data_source: str
    dataset: str
    to: str
    message: str
    severity: str = "warning"
    forward_provably_to_one: bool = False
    reverse_provably_to_one: bool = False
    contradiction: bool = False


_UNPROVEN_REMEDY = (
    "declare `cardinality` many_to_one/one_to_one, declare a covering unique "
    "key on the target, or run cardinality detection"
)


def audit_relationship_safety(
    *,
    datasets: Sequence[SlayerDataset],
    detection: Optional[RelationshipCardinalityReport] = None,
) -> list[RelationshipSafetyFinding]:
    """One finding per declared edge, recording both orientations' provability.
    The forward-unproven edges carry their consequence and remedies;
    data-contradicted declarations are appended as separate error findings."""
    # Relationships resolve within the parent dataset's datasource — no cross-datasource shadowing.
    datasets_by_key = {(m.data_source, m.name): m for m in datasets}
    findings: list[RelationshipSafetyFinding] = []
    for dataset in datasets:
        for relationship in dataset.relationships:
            target = datasets_by_key.get((dataset.data_source, relationship.to))
            if target is not None:
                findings.extend(_key_findings(dataset=dataset, relationship=relationship, target=target))
                findings.append(_edge_finding(dataset=dataset, relationship=relationship, target=target))
    if detection is not None:
        findings.extend(
            _contradiction_finding(finding)
            for finding in detection.findings
            if finding.verdict is CardinalityVerdict.CONTRADICTS_HARD
        )
    return findings


def _key_findings(
    *, dataset: SlayerDataset, relationship: DatasetRelationship, target: SlayerDataset,
) -> list[RelationshipSafetyFinding]:
    """An error finding per key that is not a declared base field on its side."""
    errors = [
        relationship_key_error(dataset=dataset.name, target=relationship.to, key=key,
                       side=side.name, fields=side.fields)
        for pair in relationship.column_pairs
        for key, side in zip(pair, (dataset, target))
    ]
    return [
        RelationshipSafetyFinding(
            data_source=dataset.data_source, dataset=dataset.name,
            to=relationship.to, message=str(err), severity="error",
        )
        for err in errors if err is not None
    ]


def _edge_finding(
    *, dataset: SlayerDataset, relationship: DatasetRelationship, target: SlayerDataset
) -> RelationshipSafetyFinding:
    """Provability of one declared edge in both orientations. Picks THIS
    declaration by position (never by pairs — parallel edges may share them)."""
    idx = next(i for i, j in enumerate(j for j in dataset.relationships if j.to == relationship.to)
               if j is relationship)
    fwd_edge = edges_between(source=dataset, target=target)[idx]
    back = edges_between(source=target, target=dataset)
    rev_edge = back[len(back) - sum(1 for j in dataset.relationships if j.to == relationship.to) + idx]
    fwd_ok = provably_to_one(edge=fwd_edge, target_dataset=target)
    rev_ok = provably_to_one(edge=rev_edge, target_dataset=dataset)
    if fwd_ok:
        message = (
            f"Relationship {dataset.name} → {relationship.to} is provably "
            f"to-one; the reverse hop "
            f"{relationship.to} → {dataset.name} "
            f"{'is provably to-one' if rev_ok else 'fans out'}."
        )
    else:
        message = (
            f"Relationship {dataset.name} → {relationship.to} is unproven: "
            f"dimensions across it are associated (cells may overlap; "
            f"partition_by=[] repeats the total instead) and "
            f"aggregation inputs across it are refused. Remedies: {_UNPROVEN_REMEDY}."
        )
    return RelationshipSafetyFinding(
        data_source=dataset.data_source,
        dataset=dataset.name,
        to=relationship.to,
        message=message,
        severity="warning" if not fwd_ok else "info",
        forward_provably_to_one=fwd_ok,
        reverse_provably_to_one=rev_ok,
    )


def _contradiction_finding(finding: RelationshipCardinalityFinding) -> RelationshipSafetyFinding:
    return RelationshipSafetyFinding(
        data_source=finding.data_source,
        dataset=finding.dataset,
        to=finding.to,
        message=(
            f"Relationship {finding.dataset} → {finding.to} declares "
            f"{finding.stored}, but cardinality detection contradicts "
            f"it (observed {finding.detected})."
        ),
        severity="error",
        contradiction=True,
    )


# ---------------------------------------------------------------------------
# Attributability / re-rooting analysis shared by binding, elaboration and compilation
# ---------------------------------------------------------------------------



def key_host_path(key: ValueKey) -> Tuple[str, ...]:
    if isinstance(key, TimeTruncKey):
        return source_anchor_path(key)
    return tuple(getattr(key, "path", ()) or ())


def _common_prefix_len(a: Tuple[str, ...], b: Tuple[str, ...]) -> int:
    """Shared prefix length; token equality is edge identity on canonical paths."""
    n = 0
    for x, y in zip(a, b):
        if x != y:
            break
        n += 1
    return n


def _route_via_common_prefix(
    *, host_name: str, target_path: Tuple[str, ...], host_path: Tuple[str, ...],
    datasets_by_name: Dict[str, SlayerDataset],
) -> Tuple[str, ...]:
    """The route from the aggregate's root (at ``target_path``) to a
    host-coordinate ``host_path`` (both canonical): step back only to the two paths'
    longest common prefix — the reversed per-hop tokens of ``target_path`` past
    it — then forward along ``host_path``'s own suffix (an empty ``host_path`` gives
    the reverse path back to the host); an ambiguous reverse hop propagates."""
    host_dataset = datasets_by_name.get(host_name)
    host_token = host_dataset.spelling if host_dataset is not None else host_name
    if host_dataset is None or not target_path:
        return (host_token, *host_path)
    chain = follow(root=host_dataset, path=target_path, datasets_by_name=datasets_by_name)
    if chain is None:
        return (host_token, *host_path)
    cp = _common_prefix_len(target_path, host_path)
    reverse_suffix = tuple(reversed([reverse_token(edge) for edge in chain[cp:]]))
    return (*reverse_suffix, *host_path[cp:])


def spine_routes(
    *, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset], cap: int = 2,
) -> list[list[OrientedRelationship]]:
    """Up to ``cap`` minimal routes ``dataset → time_spine`` over provably to-one addressable hops."""
    return [list(r.edges) for r in routes(
        root=dataset, target=TIME_SPINE_DATASET, datasets_by_name=datasets_by_name,
        hop_filter=HopFilter.TO_ONE, cap=cap, shortest_only=True,
    )]


def fanning_spine_routes(
    *, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset], cap: int = 2,
) -> list[list[OrientedRelationship]]:
    """Up to ``cap`` nearest routes ``dataset → time_spine`` crossing a fanning or unproven hop, never a product hop:
    its fanning axes (Axiom 15)."""
    # An axis is another dataset's: the route reaches it before its final hop into the spine.
    def admit(edge: OrientedRelationship) -> bool:
        return not edge.product and not (edge.source_dataset == dataset.name and edge.to == TIME_SPINE_DATASET)

    return [list(r.edges) for r in routes(root=dataset, target=TIME_SPINE_DATASET, datasets_by_name=datasets_by_name,
                                          hop_filter=HopFilter.ALL, cap=cap, shortest_only=True, admit_edge=admit)]


def fanning_route_error(*, dataset: str, routes: list[list[OrientedRelationship]]) -> UnresolvableDimensionRelationshipError:
    """The ambiguous-spine-route error for tied fanning axes, each spelled as its fact's time column."""
    spelled = [".".join((dataset, *canonical_path(r[:-1]), r[-1].column_pairs[0][0])) for r in routes]
    return UnresolvableDimensionRelationshipError(
        reference=f"{dataset}.{TIME_SPINE_FIELD}", root_dataset=TIME_SPINE_DATASET,
        reason="equal-distance fanning axes to the time spine: " + ", ".join(spelled)
        + "; query one of them as a time dimension instead of the spine",
    )


def spine_route_error(*, dataset: str, routes: list[list[OrientedRelationship]], leaf: str) -> UnresolvableDimensionRelationshipError:
    """The ambiguous-spine-route error, naming each tied canonical route."""
    return UnresolvableDimensionRelationshipError(
        reference=f"{dataset}.{leaf}", root_dataset=TIME_SPINE_DATASET,
        reason="equal-length routes to the time spine: " + ", ".join(
            ".".join((dataset, *canonical_path(r))) for r in routes
        ) + "; give the dataset a default_time_dimension, or query its facts through one of the routes",
    )


def _factor_free(datasets_by_name: Dict[str, SlayerDataset]) -> Dict[str, SlayerDataset]:
    return {n: m.as_population_factor(False) if m.population_spine else m for n, m in datasets_by_name.items()}


def _is_product_path(host_path: Tuple[str, ...], *, host_name: Optional[str], datasets_by_name: Dict[str, SlayerDataset]) -> bool:
    """``host_path`` starts at the population's product hop to the spine."""
    host = datasets_by_name.get(host_name) if host_name is not None else None
    return host is not None and host.population_spine and bool(host_path) and host_path[0] == TIME_SPINE_DATASET


def reads_population_spine(key: ValueKey, *, host: Optional[SlayerDataset]) -> bool:
    """``key`` reads a spine population's product hop (the spine as the population sees it)."""
    return host is not None and host.population_spine and any(
        isinstance(r, FieldKey) and r.path[:1] == (TIME_SPINE_DATASET,) for r in walk_value_keys(key)
    )


def home_spine_route(
    *, root_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset], leaf: str = TIME_SPINE_FIELD,
    fanning: bool = False,
) -> Optional[Tuple[str, ...]]:
    """The home's canonical route to the spine — its nearest to-one one, else (``fanning``) its nearest fanning axis;
    ``None`` without one; equal distances raise."""
    universe = {**_factor_free(datasets_by_name)}
    home = universe.get(root_dataset.name, root_dataset.as_population_factor(False))
    universe[home.name] = home
    routes = spine_routes(dataset=home, datasets_by_name=universe)
    if len(routes) > 1:
        raise spine_route_error(dataset=home.name, routes=routes, leaf=leaf)
    # Only a dataset without an axis of its own reaches the spine across a fanning axis.
    if routes or not fanning or axis_field(home) is not None:
        return canonical_path(routes[0]) if routes else None
    fanned = fanning_spine_routes(dataset=home, datasets_by_name=universe)
    if len(fanned) > 1:
        raise fanning_route_error(dataset=home.name, routes=fanned)
    return canonical_path(fanned[0]) if fanned else None


def attributable_from_root(
    *, host_path: Tuple[str, ...], target_path: Tuple[str, ...],
    root_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    host_name: Optional[str] = None,
) -> bool:
    """Is a host-coordinate path attributable from the aggregate's root over provably many-to-one hops only?"""
    tp, hp = tuple(target_path), tuple(host_path)
    if _is_product_path(hp, host_name=host_name, datasets_by_name=datasets_by_name) or (
        not tp and root_dataset.population_spine and hp[:1] == (TIME_SPINE_DATASET,)
    ):
        return home_spine_route(root_dataset=root_dataset, datasets_by_name=datasets_by_name) is not None
    if hp[: len(tp)] == tp:
        return safe_reachable(
            root=root_dataset, path=hp[len(tp):], datasets_by_name=datasets_by_name,
        )
    if host_name is None or (tp and host_name == tp[0]):
        return False
    if hp and not _common_prefix_len(tp, hp) and safe_reachable(
        root=root_dataset, path=hp, datasets_by_name=datasets_by_name,
    ):
        return True
    route = _route_via_common_prefix(
        host_name=host_name, target_path=tp, host_path=hp,
        datasets_by_name=datasets_by_name,
    )
    return safe_reachable(
        root=root_dataset, path=route, datasets_by_name=datasets_by_name,
    )


def _effective_dependency_paths(
    closure: Tuple[Tuple[str, ...], ...], own: Tuple[str, ...],
) -> List[Tuple[str, ...]]:
    """The paths a reference actually depends on, for attributability /
    determination: the MAXIMAL closure paths (a full derived crossing, not the
    intermediate relationship prefixes the join builder needs) plus the reference's OWN
    path always (a host-declared derived field's ``()`` host-locality, which a
    maximal filter would swallow as a prefix)."""
    paths = set(closure)
    maximal = [
        p for p in paths
        if not any(q != p and len(q) > len(p) and q[: len(p)] == p for q in paths)
    ]
    if own not in maximal:
        maximal.append(own)
    return maximal


def key_attributable_from_root(
    *, key: ValueKey, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    host_dataset: SlayerDataset, host_name: Optional[str] = None,
) -> bool:
    """Is every path in ``key``'s dependency closure attributable from the
    aggregate's root? A derived reference is unattributable exactly
    when its definition crosses a fanning hop; an unanalysable closure is
    unattributable (fail closed)."""
    closure = key_closure(
        key=key, anchor_dataset=host_dataset, anchor_relation=host_dataset.name,
        bundle=bundle,
    )
    if closure is None:
        return False
    return all(
        attributable_from_root(
            host_path=p, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, host_name=host_name,
        )
        for p in _effective_dependency_paths(closure, key_host_path(key))
    )


def key_broadcast_reason(
    *, key: ValueKey, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    host_dataset: SlayerDataset, host_name: Optional[str] = None,
) -> str:
    """Why ``key`` broadcasts: the first path in its dependency closure not
    attributable from the root names the fanning hop — so a derived
    fanning dimension's warning names ``region_events``, not 'unreachable'."""
    closure = key_closure(
        key=key, anchor_dataset=host_dataset, anchor_relation=host_dataset.name,
        bundle=bundle,
    )
    paths = (_effective_dependency_paths(closure, key_host_path(key))
             if closure is not None else [key_host_path(key)])
    for p in paths:
        if not attributable_from_root(
            host_path=p, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, host_name=host_name,
        ):
            return broadcast_reason(
                host_path=p, target_path=target_path, root_dataset=root_dataset,
                datasets_by_name=datasets_by_name, host_name=host_name,
            )
    return UNREACHABLE_NO_PATH


def _reads_host_spine(field: ValueKey, *, product: bool, host: Optional[SlayerDataset], root_dataset: SlayerDataset) -> bool:
    """``field`` reads the host's spine: a product hop, or a spine host's own timestamp from a non-spine root."""
    if not isinstance(field, FieldKey):
        return False
    return product or (host is not None and is_spine(host) and not is_spine(root_dataset) and not field.path
                       and field.leaf == TIME_SPINE_FIELD)


def _onto_home_spine(
    r: ValueKey, field: FieldKey, *, root_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[ValueKey]:
    """``r`` re-read through the home's own spine route: its axis, else its fanning axis (Axiom 15)."""
    route = home_spine_route(root_dataset=root_dataset, datasets_by_name=datasets_by_name, fanning=True)
    if route is None:
        return None
    moved = field.model_copy(update={"path": route})
    return moved if isinstance(r, FieldKey) else r.model_copy(update={"source": moved})


def _resolved_by_root_sibling(
    *, hp: Tuple[str, ...], via_host: Tuple[str, ...], target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset],
) -> bool:
    """The host route is unsafe, but the root reaches the sibling through its own relationship."""
    if not hp or _common_prefix_len(target_path, hp):
        return False
    return not safe_reachable(root=root_dataset, path=via_host, datasets_by_name=datasets_by_name) and safe_reachable(
        root=root_dataset, path=hp, datasets_by_name=datasets_by_name,
    )


def _reroot_leaf_via_host(
    r: ValueKey, *, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], host_name: str,
) -> Optional[ValueKey]:
    if not isinstance(r, (FieldKey, FieldSqlKey, StarKey, TimeTruncKey)):
        return None
    if isinstance(r, TimeTruncKey) and not isinstance(r.source, (FieldKey, FieldSqlKey)):
        return None  # an expression's leaves reroot one by one
    hp = key_host_path(r)
    field = r.source if isinstance(r, TimeTruncKey) else r
    product = isinstance(field, FieldKey) and _is_product_path(hp, host_name=host_name, datasets_by_name=datasets_by_name)
    if _reads_host_spine(field, product=product, host=datasets_by_name.get(host_name), root_dataset=root_dataset):
        moved = _onto_home_spine(r, cast(FieldKey, field), root_dataset=root_dataset,
                                 datasets_by_name=datasets_by_name)
        if moved is not None:
            return moved
        if product:
            return r
    if hp[: len(target_path)] == target_path:
        return None  # reroot_value_key strips the prefix
    if target_path and host_name == target_path[0]:
        return None
    via_host = _route_via_common_prefix(
        host_name=host_name, target_path=target_path, host_path=hp,
        datasets_by_name=datasets_by_name,
    )
    if _resolved_by_root_sibling(hp=hp, via_host=via_host, target_path=target_path, root_dataset=root_dataset,
                                 datasets_by_name=datasets_by_name):
        return None  # resolved through the root's own relationship to the sibling
    if isinstance(r, TimeTruncKey):
        return r.model_copy(update={
            "source": cast(FieldKey | FieldSqlKey, r.source).model_copy(update={"path": via_host}),
        })
    return r.model_copy(update={"path": via_host})


_RerootableT = TypeVar("_RerootableT", bound=ValueKey)
_PATH_LEAVES = (FieldKey, FieldSqlKey, StarKey)


def reroot_from_root(
    key: _RerootableT, *, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], host_name: str,
) -> _RerootableT:
    """Re-anchor a host-coordinate key into the root's coordinates, per leaf, by the same rules ``attributable_from_root`` proves safety with."""
    tp = tuple(target_path)
    mapping: Dict[ValueKey, ValueKey] = {}
    for r in walk_value_keys(key):
        # A field bucket reroots whole (substitute matches it pre-order, so its
        # inner-field entry is inert); an expression bucket's leaves reroot alone.
        rerooted = _reroot_leaf_via_host(
            r, target_path=tp, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, host_name=host_name,
        )
        if rerooted is not None:
            mapping[r] = rerooted
        elif isinstance(r, _PATH_LEAVES):
            mapping[r] = reroot_value_key(r, target_path=tp)
    # One pass, each host leaf mapped once: a stripped leaf equal to a host key
    # is never re-mapped, a via-host leaf never stripped.
    return lower_axis_hops(
        substitute_value_keys(key, mapping), root_dataset=root_dataset, datasets_by_name=datasets_by_name,
    )


def lower_axis_hops(
    key: _RerootableT, *, root_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> _RerootableT:
    """The spine field reached over an axis edge is the axis field itself (never a join)."""
    universe = _factor_free(datasets_by_name)
    root = universe.get(root_dataset.name, root_dataset)
    mapping: Dict[ValueKey, ValueKey] = {}
    for r in walk_value_keys(key):
        if not (isinstance(r, FieldKey) and r.path and r.leaf == TIME_SPINE_FIELD):
            continue
        try:
            chain = follow(root=root, path=r.path, datasets_by_name=universe)
        except AmbiguousRelationshipPathError:
            continue
        if not chain or chain[-1].to != TIME_SPINE_DATASET or is_product_edge(chain[-1]):
            continue
        owner = universe.get(chain[-1].source_dataset)
        axis = chain[-1].column_pairs[0][0]
        mapping[r] = (
            field_default_key(path=r.path[:-1], leaf=axis, base=owner)
            if owner is not None else FieldKey(path=r.path[:-1], leaf=axis)
        )
    return substitute_value_keys(key, mapping) if mapping else key


UNREACHABLE_NO_PATH = "unreachable from the aggregate's root (no relationship path from it)"


def _hop_walk_reason(
    *, root_dataset: SlayerDataset, path: Tuple[str, ...],
    datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[str]:
    """Walk ``path`` from ``root_dataset`` (bidirectional): the
    fanning/unproven-hop reason if the path resolves but a hop is not provably
    many-to-one, else ``None`` (an unresolvable/ambiguous path is unreachable)."""
    try:
        chain = follow(root=root_dataset, path=path, datasets_by_name=datasets_by_name)
    except AmbiguousRelationshipPathError:
        return None
    if chain is None:
        return None
    for i, edge in enumerate(chain):
        tgt = datasets_by_name.get(edge.to)
        if tgt is None:
            return None
        if not provably_to_one(edge=edge, target_dataset=tgt):
            token = path[i] if i < len(path) else edge.to
            return f"crosses a fanning or unproven relationship hop to {token}"
    return None


def broadcast_reason(
    *, host_path: Tuple[str, ...], target_path: Tuple[str, ...],
    root_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    host_name: Optional[str] = None,
) -> str:
    """Why a dimension broadcasts: crosses an unproven/fanning relationship hop (forward
    from the root, or back through the reverse hop), or unreachable if no path."""
    tp, hp = tuple(target_path), tuple(host_path)
    if _is_product_path(hp, host_name=host_name, datasets_by_name=datasets_by_name):
        return f"{root_dataset.name} has no route to the time spine"
    if hp[: len(tp)] == tp:
        reason = _hop_walk_reason(
            root_dataset=root_dataset, path=hp[len(tp):], datasets_by_name=datasets_by_name,
        )
        return reason or UNREACHABLE_NO_PATH
    # Off the forward path: reachable only back through the reverse (fanning) hop?
    if host_name is not None and not (tp and host_name == tp[0]):
        route = _route_via_common_prefix(
            host_name=host_name, target_path=tp, host_path=hp,
            datasets_by_name=datasets_by_name,
        )
        reason = _hop_walk_reason(
            root_dataset=root_dataset, path=route, datasets_by_name=datasets_by_name,
        )
        if reason is not None:
            return reason
    return UNREACHABLE_NO_PATH


def assert_partition_key_attributable(
    *, key: ValueKey, pk: ValueKey, label: str,
    scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> None:
    """A partition key whose dependency closure crosses a fanning hop is unattributable; the checker raises. Path-less keys are judged from the host, path-bearing from the aggregate's root."""
    host_m = scope.population
    if host_m is None:
        return
    datasets_by_name = bundle.datasets_by_name
    hp = key_host_path(pk)
    if crosses_product_hop(host=host_m, path=hp, datasets_by_name=datasets_by_name):
        return  # a row-level read across a product hop: the checker refuses it (Axiom 3.1)
    if not hp:
        # Path-less derived key: a fan from its own host is a mode-invariant safety
        # error; a host-safe key's cross-dataset-root concern stays the planner's.
        root, target, host_name = host_m, (), None
    else:
        target = source_anchor_path(key.source) if isinstance(key, AggregateKey) else ()
        root = walk_key_path(dataset=host_m, path=target, bundle=bundle) or host_m
        host_name = host_m.name if target else None
    attributable = key_attributable_from_root(
        key=pk, target_path=target, root_dataset=root,
        datasets_by_name=datasets_by_name, bundle=bundle, host_dataset=host_m,
        host_name=host_name,
    )
    reason = None if attributable else key_broadcast_reason(
        key=pk, target_path=target, root_dataset=root,
        datasets_by_name=datasets_by_name, bundle=bundle, host_dataset=host_m,
        host_name=host_name,
    )
    check_partition_key_attributable(
        label=label, pk=pk, attributable=attributable, reason=reason,
    )


def shared_relationship_key_reroot(
    *, key: ValueKey, target_path: Tuple[str, ...], host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[ValueKey]:
    """A host-local dimension that IS a source-side relationship key of the single hop to the root: return the root's target-side key, else ``None``."""
    leaf = _grain_leaf_name(key)
    if leaf is None or key_host_path(key) or len(target_path) != 1:
        return None
    try:
        edge = resolve_hop(
            current=host_dataset, token=target_path[0],
            datasets_by_name=datasets_by_name,
        )
    except AmbiguousRelationshipPathError:
        return None
    root_dataset = datasets_by_name.get(edge.to) if edge is not None else None
    if edge is None or root_dataset is None:
        return None
    for src, tgt in edge.column_pairs:
        if src == leaf:
            return field_default_key(path=(), leaf=tgt, base=root_dataset)
    return None


def grain_member_attributable(
    *, key: ValueKey, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    host_dataset: SlayerDataset, host_name: Optional[str] = None,
) -> bool:
    """Is a grain member attributable from the aggregate's root? Every field it
    references must be — judged on its dependency closure so a derived
    fanning reference is unattributable; every nested aggregate must be too."""
    saw = False
    for r in walk_value_keys(key):
        if isinstance(r, AggregateKey):
            saw = True
            if not attributable_from_root(
                host_path=source_anchor_path(r.source), target_path=target_path,
                root_dataset=root_dataset, datasets_by_name=datasets_by_name,
            ):
                return False
        elif isinstance(r, (FieldKey, FieldSqlKey, TimeTruncKey, StarKey)):
            saw = True
            if not key_attributable_from_root(
                key=r, target_path=target_path, root_dataset=root_dataset,
                datasets_by_name=datasets_by_name, bundle=bundle,
                host_dataset=host_dataset, host_name=host_name,
            ):
                return False
    return saw


def _grain_leaf_name(key: ValueKey) -> Optional[str]:
    """The field name a field-ish grain member / dimension names, else None."""
    if isinstance(key, FieldKey):
        return key.leaf
    if isinstance(key, FieldSqlKey):
        return key.field_name
    return None


def undetermined_witness(
    *, key: ValueKey, leaf_determined: Callable[[ValueKey], bool],
    anchor_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
    is_member: Callable[[ValueKey], bool] = lambda _k: False,
    _visiting: frozenset = frozenset(),
) -> Optional[ValueKey]:
    """The first sub-key of ``key`` not determined, else ``None`` — determination
    closed under row-level combination (Axiom 2.2); unknown kinds fail closed."""
    if is_member(key) or isinstance(key, LiteralKey):
        return None
    if isinstance(key, (FieldKey, FieldSqlKey)) and leaf_determined(key):
        return None
    visiting = _visiting
    if isinstance(key, FieldSqlKey) and key not in _visiting:
        operands = definition_refs(key=key, anchor_dataset=anchor_dataset, bundle=bundle)
        visiting = _visiting | {key}
    else:
        operands = _determination_operands(key)
    if operands is None:
        return key
    for operand in operands:
        witness = undetermined_witness(
            key=operand, leaf_determined=leaf_determined, anchor_dataset=anchor_dataset,
            bundle=bundle, is_member=is_member, _visiting=visiting,
        )
        if witness is not None:
            return witness
    return None


def _determination_operands(key: ValueKey) -> Optional[Iterable[ValueKey]]:
    """The operands whose determination decides ``key``'s, else ``None`` (opaque)."""
    if isinstance(key, TimeTruncKey):
        return (key.source,)
    if isinstance(key, AggregateKey):
        return key.partition_keys
    if isinstance(key, (*SLOT_COMPOSITE_KINDS, SqlFragmentKey)):
        return key.children()
    return None


def grain_witness(
    *, key: ValueKey, grain: Grain, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
) -> Optional[ValueKey]:
    """The first sub-key of ``key`` the dataset grain does not determine (Axiom 1), else ``None``."""
    return undetermined_witness(
        key=key, is_member=grain.__contains__, anchor_dataset=host_dataset, bundle=bundle,
        leaf_determined=lambda leaf: _field_pinned(
            key=leaf, grain=grain, host_dataset=host_dataset,
            datasets_by_name=datasets_by_name, bundle=bundle,
        ),
    )


def grain_determines(
    *, key: ValueKey, grain: Grain, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset],
    bundle: ResolvedSourceBundle,
) -> bool:
    """Does a dataset grain determine ``key``? (see ``grain_witness``)"""
    return grain_witness(
        key=key, grain=grain, host_dataset=host_dataset,
        datasets_by_name=datasets_by_name, bundle=bundle,
    ) is None


def _field_pinned(
    *, key: ValueKey, grain: Grain, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
) -> bool:
    """Every dependency-closure path of a field is pinned by the grain over
    provably to-one hops; an unanalysable closure is not."""
    closure = key_closure(
        key=key, anchor_dataset=host_dataset, anchor_relation=host_dataset.name,
        bundle=bundle,
    )
    if closure is None:
        return False
    # Each dependency is pinned at its OWN depth (reseeded per hop), so check the
    # effective (maximal + own) paths — a relationship prefix is pinned implicitly by the
    # deeper walk that passes through it, never independently.
    return all(
        _path_grain_determined(
            path=p, grain=grain, host_dataset=host_dataset, datasets_by_name=datasets_by_name,
        )
        for p in _effective_dependency_paths(closure, key_host_path(key))
    )


CANNOT_BE_ANALYSED = "cannot be analysed for determination by the operand grain"


def determination_broadcast_reason(
    *, witness: ValueKey, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
) -> str:
    """Why a dimension the operand grain does not determine broadcasts, from its witness."""
    if not isinstance(witness, (FieldKey, FieldSqlKey)) or key_closure(
        key=witness, anchor_dataset=host_dataset, anchor_relation=host_dataset.name,
        bundle=bundle,
    ) is None:
        return CANNOT_BE_ANALYSED
    if key_attributable_from_root(
        key=witness, target_path=(), root_dataset=host_dataset, datasets_by_name=datasets_by_name,
        bundle=bundle, host_dataset=host_dataset, host_name=host_dataset.name,
    ):
        return (
            f"not determined by the operand grain — add {key_display(witness)} "
            f"(or its dataset's key) to the inner partition_by="
        )
    return key_broadcast_reason(
        key=witness, target_path=(), root_dataset=host_dataset, datasets_by_name=datasets_by_name,
        bundle=bundle, host_dataset=host_dataset, host_name=host_dataset.name,
    )


def _grain_leaves(*, grain: Grain, at: Tuple[str, ...]) -> set:
    return {
        leaf for g in grain
        if key_host_path(g) == at and (leaf := _grain_leaf_name(g)) is not None
    }


def _entity_seeded(*, grain: Grain, dataset: SlayerDataset, at: Tuple[str, ...]) -> bool:
    """The grain pins ``dataset`` at ``at`` iff its leaves there cover a unique key set."""
    here = _grain_leaves(grain=grain, at=at)
    return any(ks and set(ks) <= here for ks in unique_key_sets(dataset))


def _hop_pins(
    *, edge: OrientedRelationship, tgt: SlayerDataset, grain: Grain,
    path: Tuple[str, ...], i: int, pinned_before: bool,
) -> bool:
    """The to-one target of ``edge`` stays pinned iff its own entity key is seeded,
    its host-side FK fields are grain members, or the source was pinned and the hop
    is provably to-one."""
    to_one = provably_to_one(edge=edge, target_dataset=tgt)
    src_leaves = _grain_leaves(grain=grain, at=path[:i])
    fk_seed = to_one and bool(edge.column_pairs) and all(
        src in src_leaves for src, _ in edge.column_pairs)
    return (
        _entity_seeded(grain=grain, dataset=tgt, at=path[: i + 1])
        or fk_seed
        or (pinned_before and to_one)
    )


def _path_grain_determined(
    *, path: Tuple[str, ...], grain: Grain, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset],
) -> bool:
    """One closure path of ``grain_determines``: pinned from the host over provably
    to-one hops, each hop reseeded by an entity or FK key in the grain."""
    try:
        chain = follow(root=host_dataset, path=path, datasets_by_name=datasets_by_name)
    except AmbiguousRelationshipPathError:
        return False
    if path and chain is None:
        return False
    pinned = _entity_seeded(grain=grain, dataset=host_dataset, at=())
    for i, e in enumerate(chain or []):
        tgt = datasets_by_name.get(e.to)
        if tgt is None:
            return False
        pinned = _hop_pins(
            edge=e, tgt=tgt, grain=grain,
            path=path, i=i, pinned_before=pinned,
        )
    return pinned


def crossing_local_root_predicate(
    *, scope: DatasetScope, bundle: ResolvedSourceBundle, homes: AggregateHomes,
) -> Callable[[ValueKey], bool]:
    """Predicate for a LOCAL plain aggregate whose inputs cross a relationship (desugars onto a HOST-rooted producer); windowed / ranked roots excluded."""
    host_dataset = scope.population

    def _pred(k: ValueKey) -> bool:
        return (
            isinstance(k, AggregateKey)
            and k.partition_keys is None
            and not homes.target_rooted(k)
            # A host-locus wrap already compiles inline at the producer grain; its
            # attached parameter's crossing closure must not re-route it onto a
            # host-rooted producer (as ``_local_broadcasts`` excludes).
            and k.locus != "host"
            and window_kwarg_of(k) is None
            and not is_ranked(k)
            and host_dataset is not None
            and _crosses(k, host=host_dataset)
        )

    def _crosses(k: AggregateKey, *, host: SlayerDataset) -> bool:
        crossed = aggregate_input_closure(
            key=k, anchor_dataset=host, anchor_relation=host.name, bundle=bundle,
        )
        if crossed is None:
            return True  # unanalysable input → own producer (fail closed downstream)
        return bool(crossed) and not may_inline_crossing_inputs(crossed)

    return _pred
