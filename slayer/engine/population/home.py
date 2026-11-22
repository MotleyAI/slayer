"""The home dataset of an aggregation source (semantics Axiom 2).

Resolved once, in elaboration: the deepest relationship path from the query root that
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
from slayer.engine.cardinality.relationship_safety import attributable_from_root, key_host_path, safe_reachable
from slayer.engine.population.home_reading import home_frame, home_read
from slayer.engine.reference_closure import key_closure
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.ir.terms import AggregateHome, AggregateHomes, HomeKind

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


def _grain_member_paths(
    member: ValueKey, *, dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey], in_frame: Callable[[ValueKey], ValueKey] = lambda k: k,
) -> List[Path]:
    """Home-input paths a grain member contributes: a leaf's own relationship path (in the aggregation's frame); an
    aggregate/transform member stands for its own grain members, recursively."""
    if isinstance(member, (AggregateKey, TransformKey)):
        return _constituent_grain_paths(
            c=member, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket, in_frame=in_frame,
        )
    return list(source_leaf_paths(in_frame(member)))


def _constituent_grain_paths(
    c: ValueKey, *, dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey], in_frame: Callable[[ValueKey], ValueKey] = lambda k: k,
) -> List[Path]:
    """Paths of an attached constituent's grain members (Axiom 2.3): its grain is
    the explicit ``partition_by=`` else the query dimensions, and a windowed inner
    always includes the query's time bucket."""
    members = constituent_grain(
        c=c, projected_dim_keys=dim_keys, projected_td_keys=td_keys,
        active_bucket=active_bucket,
    )
    out: List[Path] = []
    for m in members:
        out.extend(_grain_member_paths(
            member=m, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket, in_frame=in_frame,
        ))
    return out


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
    candidates = sorted(
        {anchor, _longest_common_prefix(input_paths), *seed_paths},
        key=lambda p: (p != anchor, -len(p), p),
    )
    second_order = is_reaggregation_key(agg)
    for p in candidates:
        if _determines_all(
            path=p, input_paths=input_paths, host_dataset=host_dataset, datasets_by_name=datasets_by_name, bundle=bundle,
        ):
            return AggregateHome(kind=_resolved_kind(second_order=second_order, path=p), path=p)
    # No valid home: fall to the inputs' common prefix (never deeper than the
    # source anchor) so input safety walks each input forward from there and names
    # the fanning forward hop, not a cancelled reverse hop back onto the path.
    return AggregateHome(
        kind="operand_dataset" if second_order else "unresolved", path=_longest_common_prefix(input_paths),
    )


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


def row_expression_crossing(
    *, source: ValueKey, host_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
) -> Optional[Tuple[str, Optional[str]]]:
    """``(leaf, hop)`` of the first row leaf whose dependency closure crosses an unproven or
    fanning hop from the expression's anchor (Axiom 2.2; ``hop`` ``None`` when the leaf's
    definition cannot be analysed), else ``None``."""
    anchor = source_anchor_path(source)
    root = walk_key_path(dataset=host_dataset, path=anchor, bundle=bundle) or host_dataset
    for leaf in source_row_leaves(source):
        closure = key_closure(
            key=reroot_value_key(leaf, target_path=anchor), anchor_dataset=root,
            anchor_relation=root.name, bundle=bundle,
        )
        if closure is None:
            return key_display(leaf), None
        hop = next((p[-1] for p in closure if p and not safe_reachable(
            root=root, path=p, datasets_by_name=bundle.datasets_by_name,
        )), None)
        if hop is not None:
            return key_display(leaf), hop
    return None
