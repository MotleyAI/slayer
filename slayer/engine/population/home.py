"""The home dataset of an aggregation source or a row-level expression (semantics Axiom 2).

Resolved once, in elaboration: a relationship path from the query root that
determines every input over provably to-one hops. Lives here — not in
``elaborate_env`` (THE checker) — because it needs ``join_safety``, which imports
the checker; ``elaborate.py`` calls this and hands the checker a plain map.
"""

from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Optional, Tuple

from slayer.core.keys import (
    anchor_token,
    is_framed_path,
    is_ranked,
    is_reaggregation_key,
    AggregateKey,
    ArithmeticKey,
    FieldKey,
    LiteralKey,
    ScalarCallKey,
    TransformKey,
    ValueKey,
    constituent_grain,
    operand_constituents,
    parameter_row_leaves,
    prepend_value_key,
    reroot_value_key,
    source_anchor_path,
    source_leaf_paths,
    source_row_leaves,
    walk_value_keys,
)
from slayer.core.models import SlayerDataset
from slayer.core.refs import key_display
from slayer.core.relationship_walker import follow, is_unrelated_edge
from slayer.core.relationship_walker import provably_to_one
from slayer.engine.cardinality.relationship_safety import (
    attributable_from_root,
    key_host_path,
    provably_fans,
    safe_reachable,
)
from slayer.core.errors import UnresolvableDimensionRelationshipError
from slayer.engine.population.anchors import anchor_route, source_frame
from slayer.engine.population.home_reading import home_frame, home_read
from slayer.engine.reference_closure import field_read_paths, key_closure
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.ir.terms import AggregateHome, AggregateHomes, HomeKind, HomelessOperand, RowExpressionHome

Path = Tuple[str, ...]


def _longest_common_prefix(paths: List[Path]) -> Path:
    """Token-wise LCP of canonical paths."""
    if not paths:
        return ()
    common = paths[0]
    for p in paths[1:]:
        i = 0
        while i < len(common) and i < len(p) and common[i] == p[i]:
            i += 1
        common = common[:i]
    return common


Operand = Tuple[ValueKey, Path]


def _grain_member_operands(
    member: ValueKey, *, dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey], in_frame: Callable[[ValueKey], ValueKey] = lambda k: k,
) -> List[Operand]:
    """Home inputs a grain member contributes: a leaf at its own relationship path (in the aggregation's frame); an
    aggregate/transform member stands for its own grain members, recursively."""
    if isinstance(member, (AggregateKey, TransformKey)):
        return _constituent_grain_operands(
            c=member, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket, in_frame=in_frame,
        )
    return [(member, p) for p in source_leaf_paths(in_frame(member))]


def _constituent_grain_operands(
    c: ValueKey, *, dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey], in_frame: Callable[[ValueKey], ValueKey] = lambda k: k,
) -> List[Operand]:
    """An attached constituent's grain members (Axiom 2.3): its grain is the explicit ``partition_by=`` else the
    query dimensions, and a windowed inner always includes the query's time bucket."""
    members = constituent_grain(
        c=c, projected_dim_keys=dim_keys, projected_td_keys=td_keys,
        active_bucket=active_bucket,
    )
    out: List[Operand] = []
    for m in members:
        out.extend(_grain_member_operands(
            member=m, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket, in_frame=in_frame,
        ))
    return out


def _constituent_grain_paths(
    c: ValueKey, *, dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey], in_frame: Callable[[ValueKey], ValueKey] = lambda k: k,
) -> List[Path]:
    return [p for _, p in _constituent_grain_operands(
        c=c, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket, in_frame=in_frame,
    )]


def _frame_reader(
    anchor: Path, *, host_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> Callable[[ValueKey], ValueKey]:
    """A population reference as an aggregation read from its own frame (``anchor``'s anchor hop) reads it."""
    universe = {**datasets_by_name, host_dataset.name: host_dataset}
    frame = home_frame(anchor, host=host_dataset, datasets_by_name=universe) if is_framed_path(anchor) else None
    if frame is None:
        return lambda k: k
    frame_dataset = frame[0]
    token = anchor_token(frame_dataset.name)

    def read(key: ValueKey) -> ValueKey:
        found = home_read(key, home=frame_dataset, host=host_dataset, datasets_by_name=universe)
        return key if found is None else prepend_value_key(found, host_path=(token,))

    return read


def home_path_for(
    *, agg: AggregateKey, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> Path:
    """The path of :func:`aggregate_home`."""
    return aggregate_home(
        agg=agg, host_dataset=host_dataset, datasets_by_name=datasets_by_name, bundle=bundle,
        dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket,
    ).path


def aggregate_home(
    *, agg: AggregateKey, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> AggregateHome:
    """The home dataset of an aggregate (Axiom 2): the source anchor when it determines
    every input — each source leaf and each row leaf of every argument (a bound
    definition default included) — over provably to-one hops, else the deepest
    relationship path that does. Candidates are the anchor, then the input paths and
    their longest common prefix deepest first; the first from which every input is
    attributable wins, else the inputs' common prefix, where input safety names the
    offending hop."""
    anchor = source_anchor_path(agg.source)
    input_paths: List[Path] = list(source_leaf_paths(agg.source)) or [anchor]
    # A ranked aggregate's positional args are ranking keys, not value inputs;
    # they stay attributable from the source and never pull the home shallower.
    arg_values = () if is_ranked(agg) else agg.args
    for v in (*arg_values, *(val for _, val in agg.kwargs)):
        input_paths.extend(key_host_path(leaf) for leaf in parameter_row_leaves(v))
    seed_paths = list(input_paths)
    # A constituent of the SOURCE combination broadcasts onto the home's rows, so
    # the home must determine every one of its grain members (Axiom 2.3). An
    # aggregate-valued parameter is not a source operand — a query dimension it does
    # not share is associated / broadcast, never moved into the home (Axioms 2.4, 2.9).
    in_frame = _frame_reader(anchor, host_dataset=host_dataset, datasets_by_name=datasets_by_name)
    for c in operand_constituents(agg.source):
        input_paths.extend(cg := _constituent_grain_paths(
            c=c, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket, in_frame=in_frame,
        ))
        seed_paths.extend(cg)
    second_order = is_reaggregation_key(agg)
    home = _first_determining(
        input_paths, seeds=seed_paths, anchor=anchor, order=lambda p: (p != anchor, -len(p), p),
        host_dataset=host_dataset, datasets_by_name=datasets_by_name, bundle=bundle,
    )
    if home is not None:
        return AggregateHome(kind=_resolved_kind(second_order=second_order, path=home), path=home)
    # No valid home: fall to the inputs' common prefix (never deeper than the
    # source anchor) so input safety walks each input forward from there and names
    # the fanning forward hop, not a cancelled reverse hop back onto the path.
    return AggregateHome(
        kind="operand_dataset" if second_order else "unresolved", path=_longest_common_prefix(input_paths),
    )


def _first_determining(
    input_paths: List[Path], *, seeds: List[Path], anchor: Path, order: Callable[[Path], tuple],
    host_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
) -> Optional[Path]:
    """The first candidate in ``order`` determining every input over provably to-one hops (Axiom 2.2): the anchor,
    the seeds, the inputs' common prefix and their meet (Axiom 2.6)."""
    meet = _inputs_meet(input_paths, anchor=anchor, host_dataset=host_dataset, datasets_by_name=datasets_by_name,
                        bundle=bundle)
    candidates = {anchor, _longest_common_prefix(input_paths), *seeds, *((meet,) if meet is not None else ())}
    return next((p for p in sorted(candidates, key=order) if _determines_all(
        path=p, input_paths=input_paths, host_dataset=host_dataset, datasets_by_name=datasets_by_name, bundle=bundle,
    )), None)


def _inputs_meet(
    input_paths: List[Path], *, anchor: Path, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
) -> Optional[Path]:
    """The smallest dataset determining every input's dataset (Axiom 2.6), as a path through the aggregation's frame;
    ``None`` when the inputs share one dataset or none determines them all."""
    universe = {**datasets_by_name, host_dataset.name: host_dataset}
    reached = [walk_key_path(dataset=host_dataset, path=p, bundle=bundle) for p in input_paths]
    terminals = [d for d in reached if d is not None]
    if len(terminals) != len(reached) or len({d.name for d in terminals}) < 2:
        return None
    frame = home_frame(anchor, host=host_dataset, datasets_by_name=universe)
    try:
        meet = source_frame(terminals, datasets_by_name=universe, spell=lambda d: d.spelling)
        if meet is None or frame is None:
            return None
        route = anchor_route(reader=frame[0], anchor=meet, datasets_by_name=universe)
    except UnresolvableDimensionRelationshipError:
        return None
    return None if route.unrelated else (*anchor[:len(anchor) - len(frame[1])], *route.route)


def _resolved_kind(*, second_order: bool, path: Path) -> HomeKind:
    if second_order:
        return "operand_dataset"
    return "physical_target" if path else "host"


def resolve_aggregate_homes(
    *, roots: Iterable[ValueKey], host_dataset: Optional[SlayerDataset],
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> AggregateHomes:
    """The home of every aggregate reachable from ``roots``; unresolved at its anchor when the stage has no
    dataset host.

    ``dim_keys`` / ``td_keys`` are the query's projected non-time / time
    dimension keys and ``active_bucket`` its main time bucket — an ungrained
    constituent's grain (Axiom 2.3)."""
    homes: Dict[AggregateKey, AggregateHome] = {}
    for root in roots:
        for k in walk_value_keys(root):
            if not isinstance(k, AggregateKey) or k in homes:
                continue
            homes[k] = AggregateHome(kind="unresolved", path=source_anchor_path(k.source)) if host_dataset is None \
                else aggregate_home(
                    agg=k, host_dataset=host_dataset,
                    datasets_by_name=datasets_by_name, bundle=bundle,
                    dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket,
                )
    return AggregateHomes(by_key=homes)


def _determines_all(
    *, path: Path, input_paths: List[Path], host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
) -> bool:
    dataset_at_p = walk_key_path(dataset=host_dataset, path=path, bundle=bundle)
    return dataset_at_p is not None and all(
        attributable_from_root(
            host_path=q, target_path=path, root_dataset=dataset_at_p,
            datasets_by_name=datasets_by_name, host_name=host_dataset.name,
        )
        for q in input_paths
    )


def row_expression_home(
    *, key: ValueKey, host_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
    dim_keys: List[ValueKey], td_keys: List[ValueKey], active_bucket: Optional[ValueKey],
) -> RowExpressionHome:
    """A row-level expression's home (Axiom 2.2): the deepest dataset determining every operand — each row leaf at
    its declared dataset and each dataset its definition reaches across a fanning hop; an attached constituent's grain
    members — else the operands that fail."""
    universe = {**bundle.datasets_by_name, host_dataset.name: host_dataset}
    operands = _home_operands(key, host_dataset=host_dataset, bundle=bundle, universe=universe,
                              dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket)
    if isinstance(operands, RowExpressionHome):
        return operands
    return _home_of(operands, anchor=source_anchor_path(key), host_dataset=host_dataset, bundle=bundle,
                    universe=universe)


def _home_operands(
    key: ValueKey, *, host_dataset: SlayerDataset, bundle: ResolvedSourceBundle, universe: Dict[str, SlayerDataset],
    dim_keys: List[ValueKey], td_keys: List[ValueKey], active_bucket: Optional[ValueKey],
) -> List[Operand] | RowExpressionHome:
    """Each row leaf at its declared dataset or the datasets its fanning definition reaches, and each constituent's
    grain members; or the refusal of a definition crossing an unproven hop."""
    operands: List[Operand] = []
    for leaf in source_row_leaves(key):
        reach = _closure_reach(leaf, host_dataset=host_dataset, bundle=bundle, universe=universe)
        if isinstance(reach, RowExpressionHome):
            return reach
        operands.extend((leaf, p) for p in reach or [key_host_path(leaf)])
    operands.extend(_constituent_operands(key, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket))
    return operands


def _home_of(
    operands: List[Operand], *, anchor: Path, host_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
    universe: Dict[str, SlayerDataset],
) -> RowExpressionHome:
    paths = [p for _, p in operands]
    if len(set(paths)) <= 1:
        return RowExpressionHome(path=paths[0] if paths else anchor)
    home = _deepest_determining(paths, anchor=anchor, host_dataset=host_dataset, bundle=bundle)
    if home is not None:
        return RowExpressionHome(path=home)
    return _homeless(operands, anchor=anchor, host_dataset=host_dataset, bundle=bundle, universe=universe)


def _constituent_operands(
    key: ValueKey, *, dim_keys: List[ValueKey], td_keys: List[ValueKey], active_bucket: Optional[ValueKey],
) -> List[Operand]:
    return [op for c in operand_constituents(key) for op in _constituent_grain_operands(
        c=c, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket,
    )]


def as_home_field(key: ValueKey, *, host_dataset: Optional[SlayerDataset], bundle: ResolvedSourceBundle) -> ValueKey:
    """``key`` read as a field of its home (Axiom 13): NULL on a population row with no home row, when its home
    lies below another operand's dataset."""
    if host_dataset is None:
        return key
    universe = {**bundle.datasets_by_name, host_dataset.name: host_dataset}
    operands = _home_operands(key, host_dataset=host_dataset, bundle=bundle, universe=universe,
                              dim_keys=[], td_keys=[], active_bucket=None)
    if isinstance(operands, RowExpressionHome):
        return key
    path = _home_of(operands, anchor=source_anchor_path(key), host_dataset=host_dataset, bundle=bundle,
                    universe=universe).path
    reads: Dict[ValueKey, List[Path]] = {}
    for k, p in operands:
        reads.setdefault(k, []).append(p)
    # A derived leaf reading at its home keeps its own definition's reading; only another operand's dataset guards.
    if not path or all(any(p[:len(path)] == path for p in ps) for ps in reads.values()):
        return key
    chain = follow(root=host_dataset, path=path, datasets_by_name=universe)
    if not chain or not chain[-1].column_pairs:
        return key
    anchor = next((getattr(k, "anchor", None) for k, p in operands if p == path), None)
    matched = ArithmeticKey(op="is not", operands=(
        FieldKey(path=path, leaf=chain[-1].column_pairs[0][1], anchor=anchor), LiteralKey(value=None),
    ))
    if isinstance(key, ScalarCallKey) and key.name == "iif" and key.args[:1] == (matched,):
        return key
    return ScalarCallKey(name="iif", args=(matched, key, LiteralKey(value=None)))


def _deepest_determining(
    paths: List[Path], *, anchor: Path, host_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
) -> Optional[Path]:
    return _first_determining(
        paths, seeds=paths, anchor=anchor, order=lambda p: (-len(p), p),
        host_dataset=host_dataset, datasets_by_name=bundle.datasets_by_name, bundle=bundle,
    )


def _closure_reach(
    leaf: ValueKey, *, host_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
    universe: Dict[str, SlayerDataset],
) -> List[Path] | RowExpressionHome:
    """Every dataset ``leaf``'s definition reads a field on, when it reads across a fanning hop, or its refusal when it
    crosses an unproven one; an unanalysable definition is left to its resolution error."""
    path = key_host_path(leaf)
    dataset = walk_key_path(dataset=host_dataset, path=path, bundle=bundle)
    if dataset is None:
        return []
    rerooted = reroot_value_key(leaf, target_path=path)
    closure = key_closure(key=rerooted, anchor_dataset=dataset, anchor_relation=dataset.name, bundle=bundle)
    shown = _operand_display(leaf, host_dataset=host_dataset)
    fans = False
    for p in closure or ():
        fanning, unproven = _crossing(root=dataset, path=p, universe=universe)
        if unproven is not None:
            return RowExpressionHome(operands=(HomelessOperand(operand=shown, hop=unproven),), common=dataset.name)
        fans = fans or fanning
    reads = field_read_paths(rerooted, anchor_dataset=dataset, bundle=bundle) if fans else ()
    return [(*path, *p) for p in reads or ()]


def _crossing(*, root: SlayerDataset, path: Path, universe: Dict[str, SlayerDataset]) -> Tuple[bool, Optional[str]]:
    """Whether ``path`` crosses a provably fanning hop, and its first hop neither provably to-one nor fanning."""
    chain = follow(root=root, path=path, datasets_by_name=universe)
    if chain is None:
        return False, path[0] if path else None
    fans = False
    for token, edge in zip(path, chain):
        target = universe.get(edge.to)
        if target is None:
            return fans, token
        if provably_to_one(edge=edge, target_dataset=target):
            continue
        if not provably_fans(edge=edge, target_dataset=target):
            return fans, token
        fans = True
    return fans, None


def _operand_display(key: ValueKey, *, host_dataset: SlayerDataset) -> str:
    """``key`` as written from the population: a host-local leaf qualified by the host's name."""
    shown = key_display(key)
    return shown if key_host_path(key) else f"{host_dataset.name}.{shown}"


def _first_unsafe_hop(*, root: SlayerDataset, path: Path, universe: Dict[str, SlayerDataset]) -> Optional[str]:
    return next((path[i - 1] for i in range(1, len(path) + 1)
                 if not safe_reachable(root=root, path=path[:i], datasets_by_name=universe)), None)


def _homeless(
    operands: List[Operand], *, anchor: Path, host_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
    universe: Dict[str, SlayerDataset],
) -> RowExpressionHome:
    """Two operands no dataset determines together, each with its first unsafe hop from their common dataset."""
    pairs = [(a, b) for i, a in enumerate(operands) for b in operands[i + 1:] if a[1] != b[1]]
    (a, pa), (b, pb) = next(
        (pair for pair in pairs
         if _deepest_determining([pair[0][1], pair[1][1]], anchor=anchor, host_dataset=host_dataset,
                                 bundle=bundle) is None),
        pairs[0],
    )
    common = _longest_common_prefix([pa, pb])
    dataset = walk_key_path(dataset=host_dataset, path=common, bundle=bundle) or host_dataset
    return RowExpressionHome(
        operands=tuple(
            HomelessOperand(operand=_operand_display(k, host_dataset=host_dataset), hop=_first_unsafe_hop(
                root=dataset, path=p[len(common):], universe=universe,
            ))
            for k, p in ((a, pa), (b, pb))
        ),
        common=dataset.name,
    )


def _crosses_unrelated(*, root: SlayerDataset, path: Path, datasets_by_name: Dict[str, SlayerDataset]) -> bool:
    chain = follow(root=root, path=path, datasets_by_name=datasets_by_name)
    return chain is not None and any(is_unrelated_edge(e) for e in chain)


def population_crossing(
    *, source: ValueKey, population: SlayerDataset, bundle: ResolvedSourceBundle,
) -> Optional[Tuple[str, Optional[str]]]:
    """``(leaf, hop)`` of the first row leaf whose dependency closure crosses an unproven or fanning hop from
    ``population`` (Axiom 12.5; an unrelated dataset is left to its own check); ``hop`` is ``None`` when the leaf's
    definition cannot be analysed."""
    universe = bundle.datasets_by_name
    for leaf in source_row_leaves(source):
        if _crosses_unrelated(root=population, path=key_host_path(leaf), datasets_by_name=universe):
            continue
        closure = key_closure(
            key=reroot_value_key(leaf, target_path=()), anchor_dataset=population,
            anchor_relation=population.name, bundle=bundle,
        )
        if closure is None:
            return key_display(leaf), None
        hop = next((p[-1] for p in closure
                    if p and not safe_reachable(root=population, path=p, datasets_by_name=universe)
                    and not _crosses_unrelated(root=population, path=p, datasets_by_name=universe)), None)
        if hop is not None:
            return key_display(leaf), hop
    return None
