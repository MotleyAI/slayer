"""The home dataset of an aggregation source (semantics Axiom 2).

Resolved once, in elaboration: the deepest join path from the query root that
determines every input over provably to-one hops. Lives here — not in
``elaborate_env`` (THE checker) — because it needs ``join_safety``, which imports
the checker; ``elaborate.py`` calls this and hands the checker a plain map.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from slayer.core.enums import RANKED_AGGREGATIONS
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    ValueKey,
    source_anchor_path,
    source_leaf_paths,
    walk_value_keys,
)
from slayer.core.models import SlayerModel
from slayer.engine.join_safety import attributable_from_root, key_host_path
from slayer.engine.reference_closure import default_param_value_key
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle

Path = Tuple[str, ...]


def _longest_common_prefix(paths: List[Path]) -> Path:
    if not paths:
        return ()
    common = paths[0]
    for p in paths[1:]:
        i = 0
        while i < len(common) and i < len(p) and common[i] == p[i]:
            i += 1
        common = common[:i]
    return common


def _default_home_candidate_paths(
    *, agg: AggregateKey, host_model: SlayerModel, bundle: ResolvedSourceBundle,
) -> List[Path]:
    """Home candidates from non-overridden definition defaults: each default
    resolved as a reference FROM THE HOST, so a default naming a shallower model
    (``customers.spend``) widens the home exactly as spelling it would. The
    definition is looked up on the source's anchor (a cross-model custom agg)."""
    owner = walk_key_path(
        model=host_model, path=source_anchor_path(agg.source), bundle=bundle,
    )
    agg_def = next(
        (a for a in (owner.aggregations or []) if a.name == agg.agg), None,
    ) if owner is not None else None
    if agg_def is None:
        return []
    explicit = {name for name, _ in agg.kwargs}
    out: List[Path] = []
    for p in agg_def.params:
        if p.name in explicit:
            continue
        vk = default_param_value_key(
            sql=p.sql, owner_path=(), owner_model=host_model, bundle=bundle,
        )
        if not isinstance(vk, (ColumnKey, ColumnSqlKey)):
            continue
        path = key_host_path(vk)
        if path and walk_key_path(model=host_model, path=path, bundle=bundle) is not None:
            out.append(path)
    return out


def home_path_for(
    *, agg: AggregateKey, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> Path:
    """The home dataset of an aggregate (Axiom 2): the deepest join path that
    determines every input — each source leaf, each column-valued arg/kwarg, and
    each definition default — over provably to-one hops. Candidates are the input
    paths and their longest common prefix, deepest first (ties prefer the source
    anchor); the first from which every input is attributable wins. Falls back to
    the anchor, where input safety then raises on an unproven hop."""
    anchor = source_anchor_path(agg.source)
    input_paths: List[Path] = list(source_leaf_paths(agg.source)) or [anchor]
    # A ranked aggregate's positional args are ranking keys, not value inputs;
    # they stay attributable from the source and never pull the home shallower.
    arg_values = () if agg.agg in RANKED_AGGREGATIONS else agg.args
    for v in (*arg_values, *(val for _, val in agg.kwargs)):
        if isinstance(v, (ColumnKey, ColumnSqlKey)):
            input_paths.append(key_host_path(v))
    input_paths.extend(_default_home_candidate_paths(
        agg=agg, host_model=host_model, bundle=bundle,
    ))
    candidates = sorted(
        {anchor, _longest_common_prefix(input_paths), *input_paths},
        key=lambda p: (-len(p), p != anchor, p),
    )
    for p in candidates:
        model_at_p = walk_key_path(model=host_model, path=p, bundle=bundle)
        if model_at_p is None:
            continue
        if all(
            attributable_from_root(
                host_path=q, target_path=p, root_model=model_at_p,
                models_by_name=models_by_name, host_name=host_model.name,
            )
            for q in input_paths
        ):
            return p
    return anchor


def resolve_aggregate_homes(
    *, roots: Iterable[ValueKey], host_model: Optional[SlayerModel],
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> Dict[AggregateKey, Path]:
    """Home path of every aggregate reachable from ``roots``; empty when the
    stage has no model host (a stage query's aggregates are all host-local)."""
    if host_model is None:
        return {}
    homes: Dict[AggregateKey, Path] = {}
    for root in roots:
        for k in walk_value_keys(root):
            if isinstance(k, AggregateKey) and k not in homes:
                homes[k] = home_path_for(
                    agg=k, host_model=host_model,
                    models_by_name=models_by_name, bundle=bundle,
                )
    return homes
