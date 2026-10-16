"""The one elaboration pass: bound query → typing environment (D2, D8).

``elaborate_query`` types a user-authored stage (binding is its sub-phase, so a
raw query and a ``prebound=`` entry produce the same environment);
``elaborate_synthesized`` types a compiler-synthesized producer. The environment
builder and THE checker live in ``elaborate_env``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from slayer.core.keys import ValueKey, split_top_level_and
from slayer.core.query import SlayerQuery
from slayer.core.scope import ModelScope, StageSchema
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.elaborate_env import (
    build_environment,
    check_combined_partition_keys,
    check_unrelated_filter_sides,
    check_unrelated_row_read,
    home_dataset,
    position_typing_context,
    type_and_split_filters,
)
from slayer.engine.home import resolve_aggregate_homes
from slayer.engine.join_safety import crossing_local_root_predicate
from slayer.engine.unrelated import ProductSides
from slayer.ir.elaborated import ConjunctTyping, ElaboratedProducer, ElaboratedQuery, ElaboratedStage
from slayer.ir.planned import MaskTyping
from slayer.ir.prebound import (
    PreboundQuery,
    StrictQueryCarrier,
    partition_declared_measures,
)
from slayer.ir.source_bundle import ResolvedSourceBundle, resolve_scope


def elaborate_query(
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    prebound: Optional[PreboundQuery] = None,
) -> ElaboratedStage:
    """Elaborate one user-authored query stage; ``prebound=`` skips the parse/bind
    sub-phase. Splits host-rooted filter strings into per-conjunct masks."""
    stage_schemas = stage_schemas or {}
    if scope is None:
        scope = resolve_scope(
            query=query, bundle=bundle, stage_schemas=stage_schemas,
        )
    if prebound is None:
        prebound = bind_query_inputs(
            query=query, bundle=bundle, scope=scope,
            stage_schemas=stage_schemas,
        )
    sides = ProductSides(host=scope.source_model, bundle=bundle) if isinstance(scope, ModelScope) and (
        scope.source_model is not None) else None
    crossing = crossing_local_root_predicate(scope=scope, bundle=bundle)
    prebound, filter_typings = type_and_split_filters(
        prebound,
        crossing_root=crossing if sides is None or not sides.tokens else (
            lambda k: crossing(k) or bool(sides.crossed(k))
        ),
    )
    if sides is not None:
        _check_product_hops(prebound, filter_typings=filter_typings, sides=sides)
    check_combined_partition_keys(
        prebound, filter_typings=filter_typings,
        order_texts=[o.raw_formula or o.column.full_name for o in query.order or []],
    )
    return ElaboratedStage(query=query, **_environment_fields(
        prebound, filter_typings=filter_typings, bundle=bundle, scope=scope,
        stage_schemas=stage_schemas,
    ))


def _check_product_hops(
    prebound: PreboundQuery, *, filter_typings: Sequence[ConjunctTyping], sides: ProductSides,
) -> None:
    """Only aggregates and one-sided field filters read across a product hop (semantics Axiom 3.1)."""
    if not sides.tokens:
        return
    population = sides.host.spelling
    roots: List[Tuple[ValueKey, str]] = [
        (dm.bound.value_key, f"{'dimension' if dm.is_dimension else 'measure'} {dm.public_name!r}")
        for dm in prebound.declared_measures
    ]
    roots += [(sp.bound.value_key, "order") for sp in prebound.order_specs]
    for bf, typing in zip(prebound.bound_filters, filter_typings, strict=True):
        if typing.typing == MaskTyping.FIELD:
            for cj in split_top_level_and(bf.value_key):
                check_unrelated_filter_sides(conjunct=cj, datasets=sides.mixed_sides(cj), population=population)
        else:
            roots.append((bf.value_key, "filter"))
    for root, location in roots:
        reference, dataset = next(sides.row_level_reads(root), (None, ""))
        check_unrelated_row_read(reference=reference, dataset=dataset, population=population, location=location)


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
