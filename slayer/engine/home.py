"""The home dataset of an aggregation source (semantics Axiom 2).

Resolved once, in elaboration: the source anchor when it determines every input
over provably to-one hops, else the deepest join path from the query root that does. Lives here — not in
``elaborate_env`` (THE checker) — because it needs ``join_safety``, which imports
the checker; ``elaborate.py`` calls this and hands the checker the resolved homes.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from slayer.core.keys import (
    is_ranked,
    is_reaggregation_key,
    AggregateKey,
    TransformKey,
    ValueKey,
    constituent_grain,
    operand_constituents,
    parameter_row_leaves,
    source_anchor_path,
    source_leaf_paths,
    walk_value_keys,
)
from slayer.core.models import SlayerModel
from slayer.engine.join_safety import attributable_from_root, key_host_path
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
    active_bucket: Optional[ValueKey],
) -> List[Path]:
    """Home-input paths a grain member contributes: a leaf's own join path; an
    aggregate/transform member stands for its own grain members, recursively."""
    if isinstance(member, (AggregateKey, TransformKey)):
        return _constituent_grain_paths(
            c=member, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket,
        )
    return list(source_leaf_paths(member))


def _constituent_grain_paths(
    c: ValueKey, *, dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
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
            member=m, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket,
        ))
    return out


def home_path_for(
    *, agg: AggregateKey, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> Path:
    """The path of :func:`aggregate_home`."""
    return aggregate_home(
        agg=agg, host_model=host_model, models_by_name=models_by_name, bundle=bundle,
        dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket,
    ).path


def aggregate_home(
    *, agg: AggregateKey, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> AggregateHome:
    """The home dataset of an aggregate (Axiom 2): the source anchor when it determines
    every input — each source leaf and each row leaf of every argument (a bound
    definition default included) — over provably to-one hops, else the deepest
    join path that does. Candidates are the anchor, then the input paths and
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
    for c in operand_constituents(agg.source):
        input_paths.extend(cg := _constituent_grain_paths(
            c=c, dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket,
        ))
        seed_paths.extend(cg)
    candidates = sorted(
        {anchor, _longest_common_prefix(input_paths), *seed_paths},
        key=lambda p: (p != anchor, -len(p), p),
    )
    second_order = is_reaggregation_key(agg)
    for p in candidates:
        if _determines_all(p, input_paths=input_paths, host_model=host_model,
                           models_by_name=models_by_name, bundle=bundle):
            return AggregateHome(kind=_resolved_kind(p, second_order=second_order), path=p)
    # No valid home: fall to the inputs' common prefix (never deeper than the
    # source anchor) so input safety walks each input forward from there and names
    # the fanning forward hop, not a cancelled reverse hop back onto the path.
    return AggregateHome(
        kind="operand_dataset" if second_order else "unresolved", path=_longest_common_prefix(input_paths),
    )


def _determines_all(
    p: Path, *, input_paths: List[Path], host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> bool:
    """Every input path is attributable from the dataset at ``p``."""
    model_at_p = walk_key_path(model=host_model, path=p, bundle=bundle)
    return model_at_p is not None and all(
        attributable_from_root(
            host_path=q, target_path=p, root_model=model_at_p,
            models_by_name=models_by_name, host_name=host_model.name,
        )
        for q in input_paths
    )


def _resolved_kind(p: Path, *, second_order: bool) -> HomeKind:
    if second_order:
        return "operand_dataset"
    return "physical_target" if p else "host"


def resolve_aggregate_homes(
    *, roots: Iterable[ValueKey], host_model: Optional[SlayerModel],
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> AggregateHomes:
    """The home of every aggregate reachable from ``roots``; unresolved at its anchor when the stage has no
    model host.

    ``dim_keys`` / ``td_keys`` are the query's projected non-time / time
    dimension keys and ``active_bucket`` its main time bucket — an ungrained
    constituent's grain (Axiom 2.3)."""
    homes: Dict[AggregateKey, AggregateHome] = {}
    for root in roots:
        for k in walk_value_keys(root):
            if not isinstance(k, AggregateKey) or k in homes:
                continue
            homes[k] = AggregateHome(kind="unresolved", path=source_anchor_path(k.source)) if host_model is None \
                else aggregate_home(
                    agg=k, host_model=host_model,
                    models_by_name=models_by_name, bundle=bundle,
                    dim_keys=dim_keys, td_keys=td_keys, active_bucket=active_bucket,
                )
    return AggregateHomes(by_key=homes)
