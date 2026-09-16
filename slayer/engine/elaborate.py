"""The one elaboration pass: bound query → ``ElaboratedQuery`` (D2, D8).

Public seam ``elaborate_query``; binding is its sub-phase, so a raw query and a
``prebound=`` entry produce the same environment. The environment builder and
THE checker live in ``elaborate_env``.
"""

from __future__ import annotations

from typing import Dict, Optional, Union

from slayer.core.query import SlayerQuery
from slayer.core.scope import ModelScope, StageSchema
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.elaborate_env import (
    build_environment,
    home_dataset,
    type_and_split_filters,
)
from slayer.engine.home import resolve_aggregate_homes
from slayer.engine.join_safety import crossing_local_root_predicate
from slayer.ir.elaborated import ElaboratedQuery
from slayer.ir.prebound import (
    PreboundQuery,
    StrictQueryCarrier,
    position_typing_context,
)
from slayer.ir.source_bundle import ResolvedSourceBundle, resolve_scope


def elaborate_query(
    *,
    query: Optional[Union[SlayerQuery, StrictQueryCarrier]] = None,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    prebound: Optional[PreboundQuery] = None,
    disable_host_rooted_isolation: bool = False,
) -> ElaboratedQuery:
    """Elaborate one query stage; ``prebound=`` skips the parse/bind sub-phase,
    ``disable_host_rooted_isolation`` types without splitting (recursion guard)."""
    stage_schemas = stage_schemas or {}
    if scope is None:
        if query is None:
            raise ValueError("elaborate_query needs query= or an explicit scope=.")
        scope = resolve_scope(
            query=query, bundle=bundle, stage_schemas=stage_schemas,
        )
    if prebound is None:
        if not isinstance(query, SlayerQuery):
            raise ValueError("elaborate_query needs query= or prebound=.")
        prebound = bind_query_inputs(
            query=query, bundle=bundle, scope=scope,
            stage_schemas=stage_schemas,
        )
    prebound, filter_typings = type_and_split_filters(
        prebound,
        crossing_root=(
            crossing_local_root_predicate(scope=scope, bundle=bundle)
            if not disable_host_rooted_isolation else None
        ),
        split=not disable_host_rooted_isolation,
    )
    dim_keys, row_agg_set = position_typing_context(prebound)
    model = scope.source_model if isinstance(scope, ModelScope) else None
    home_roots = [
        *(dm.bound.value_key for dm in prebound.declared_measures),
        *(bf.value_key for bf in prebound.bound_filters),
        *(spec.bound.value_key for spec in prebound.order_specs),
    ]
    home_paths = resolve_aggregate_homes(
        roots=home_roots, host_model=model,
        models_by_name=bundle.models_by_name, bundle=bundle,
    )
    env = build_environment(
        prebound=prebound,
        home=home_dataset(scope=scope, model=model),
        dim_keys=dim_keys,
        row_agg_set=row_agg_set,
        filter_typings=filter_typings,
        home_paths=home_paths,
    )
    return env.model_copy(update={
        "query": query,
        "scope": scope,
        "bundle": bundle,
        "stage_schemas": dict(stage_schemas),
        "prebound": prebound,
        "filter_typings": tuple(filter_typings),
    })
