"""The one elaboration pass: bound query → ``ElaboratedQuery`` (D2, D8).

Public seam ``elaborate_query``; binding is its sub-phase, so a raw query and a
``prebound=`` entry produce the same environment. The environment builder lives
in ``elaborate_env`` (imported by the planner without a cycle).
"""

from __future__ import annotations

from typing import Dict, Optional, Union

from slayer.core.query import SlayerQuery
from slayer.core.scope import ModelScope, StageSchema
from slayer.engine.elaborate_env import build_environment, home_dataset
from slayer.engine.prebound import PreboundQuery
from slayer.engine.compile.stages import (
    _crossing_local_root_predicate,
    _position_typing_context,
    _resolve_scope,
    _type_and_split_filters,
    bind_query_inputs,
)
from slayer.ir.elaborated import ElaboratedQuery, ElaborationSource
from slayer.ir.source_bundle import ResolvedSourceBundle


def elaborate_query(
    *,
    query: Optional[SlayerQuery] = None,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    prebound: Optional[PreboundQuery] = None,
) -> ElaboratedQuery:
    """Elaborate one query stage; ``prebound=`` skips the parse/bind sub-phase."""
    if scope is None:
        if query is None:
            raise ValueError("elaborate_query needs query= or an explicit scope=.")
        scope = _resolve_scope(
            query=query, bundle=bundle, stage_schemas=stage_schemas or {},
        )
    if prebound is None:
        if query is None:
            raise ValueError("elaborate_query needs query= or prebound=.")
        prebound = bind_query_inputs(
            query=query, bundle=bundle, scope=scope,
            stage_schemas=stage_schemas or {},
        )
    prebound, filter_typings = _type_and_split_filters(
        prebound,
        crossing_root=_crossing_local_root_predicate(scope=scope, bundle=bundle),
    )
    dim_keys, row_agg_set = _position_typing_context(prebound)
    model = scope.source_model if isinstance(scope, ModelScope) else None
    env = build_environment(
        prebound=prebound,
        home=home_dataset(scope=scope, model=model),
        dim_keys=dim_keys,
        row_agg_set=row_agg_set,
        filter_typings=filter_typings,
    )
    return env.model_copy(update={"source": ElaborationSource(
        query=query, bundle=bundle, scope=scope,
        stage_schemas=stage_schemas, prebound=prebound,
    )})
