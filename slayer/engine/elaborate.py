"""The one elaboration pass: bound query → typing environment (D2, D8).

``elaborate_query`` types a user-authored stage (binding is its sub-phase, so a
raw query and a ``prebound=`` entry produce the same environment);
``elaborate_synthesized`` types a compiler-synthesized producer. The environment
builder and THE checker live in ``elaborate_env``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from slayer.core.query import SlayerQuery
from slayer.core.scope import ModelScope, StageSchema
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.elaborate_env import (
    build_environment,
    home_dataset,
    position_typing_context,
    type_and_split_filters,
)
from slayer.engine.home import resolve_aggregate_homes
from slayer.engine.join_safety import crossing_local_root_predicate
from slayer.ir.elaborated import ElaboratedProducer, ElaboratedQuery, ElaboratedStage
from slayer.ir.prebound import (
    PreboundQuery,
    StrictQueryCarrier,
    partition_declared_measures,
)
from slayer.ir.source_bundle import ResolvedSourceBundle, resolve_scope


def elaborate_query(
    *,
    query: Optional[SlayerQuery] = None,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    prebound: Optional[PreboundQuery] = None,
) -> ElaboratedStage:
    """Elaborate one user-authored query stage; ``prebound=`` skips the parse/bind
    sub-phase. Splits host-rooted filter strings into per-conjunct masks."""
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
        prebound, crossing_root=crossing_local_root_predicate(scope=scope, bundle=bundle),
    )
    return ElaboratedStage(query=query, **_environment_fields(
        prebound, filter_typings=filter_typings, bundle=bundle, scope=scope,
        stage_schemas=stage_schemas,
    ))


def elaborate_synthesized(
    prebound: PreboundQuery,
    *,
    bundle: ResolvedSourceBundle,
    scope: Union[ModelScope, StageSchema],
    stage_schemas: Dict[str, StageSchema],
    source_model: Optional[str] = None,
) -> ElaboratedProducer:
    """Elaborate a compiler-synthesized producer, typing its filters unsplit."""
    prebound, filter_typings = type_and_split_filters(prebound, split=False)
    return ElaboratedProducer(
        query=StrictQueryCarrier(source_model=source_model, prebound=prebound),
        **_environment_fields(
            prebound, filter_typings=filter_typings, bundle=bundle, scope=scope,
            stage_schemas=stage_schemas,
        ),
    )


def _environment_fields(
    prebound: PreboundQuery,
    *,
    filter_typings,
    bundle: ResolvedSourceBundle,
    scope: Union[ModelScope, StageSchema],
    stage_schemas: Dict[str, StageSchema],
) -> Dict[str, Any]:
    dim_keys, row_agg_set = position_typing_context(prebound)
    model = scope.source_model if isinstance(scope, ModelScope) else None
    home_roots = [
        *(dm.bound.value_key for dm in prebound.declared_measures),
        *(bf.value_key for bf in prebound.bound_filters),
        *(spec.bound.value_key for spec in prebound.order_specs),
    ]
    dim_dms, td_dms, _ = partition_declared_measures(
        declared_measures=prebound.declared_measures,
        n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
    )
    home_paths = resolve_aggregate_homes(
        roots=home_roots, host_model=model,
        models_by_name=bundle.models_by_name, bundle=bundle,
        dim_keys=[dm.bound.value_key for dm in dim_dms],
        td_keys=[dm.bound.value_key for dm in td_dms],
        active_bucket=prebound.main_time_key,
    )
    env = build_environment(
        prebound=prebound,
        home=home_dataset(scope=scope, model=model),
        dim_keys=dim_keys,
        row_agg_set=row_agg_set,
        filter_typings=filter_typings,
        home_paths=home_paths,
    )
    return {
        **{name: getattr(env, name) for name in ElaboratedQuery.model_fields},
        "scope": scope,
        "bundle": bundle,
        "stage_schemas": dict(stage_schemas),
        "prebound": prebound,
        "filter_typings": tuple(filter_typings),
    }
