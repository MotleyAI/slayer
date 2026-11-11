"""Query-scoped extensions: one overlay step per stage over the models it reads."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from slayer.core.enums import JoinType
from slayer.core.errors import ModelNotFoundError, QueryExtensionError, TimeSpineError
from slayer.core.join_edges import EdgeMerge
from slayer.core.models import ModelJoin, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.core.scope import ModelScope, StageSchema
from slayer.core.time_spine import TIME_SPINE_MODEL, is_spine
from slayer.ir.source_bundle import InnerRestriction, ResolvedSourceBundle, overlay_columns_and_measures

__all__ = [
    "apply_query_extensions",
    "extension_of",
    "with_own_extension",
]


def extension_of(query: SlayerQuery, name: str) -> Optional[ModelExtension]:
    """``query``'s extension of model ``name``, if any."""
    return next((e for e in query.extensions or [] if e.source_name == name), None)


def with_own_extension(model: Optional[SlayerModel], query: SlayerQuery) -> Optional[SlayerModel]:
    """``model`` with the columns and measures ``query`` extends it with."""
    extension = extension_of(query, model.name) if model is not None else None
    return overlay_columns_and_measures(model, extension) if model is not None and extension is not None else model


def _universe(bundle: ResolvedSourceBundle) -> Dict[str, SlayerModel]:
    out: Dict[str, SlayerModel] = {}
    if bundle.source_model is not None:
        out[bundle.source_model.name] = bundle.source_model
    for model in bundle.referenced_models:
        out.setdefault(model.name, model)
    return out


def _extended(
    name: str, *, models: Dict[str, SlayerModel], bundle: ResolvedSourceBundle, where: Optional[str],
) -> SlayerModel:
    model = models.get(name)
    if name == TIME_SPINE_MODEL or (model is not None and is_spine(model)):
        raise TimeSpineError(
            summary=f"{TIME_SPINE_MODEL!r} cannot be extended.",
            suggestion="Bucket its timestamp in time_dimensions; add columns or joins to the dataset models instead.",
        )
    if model is not None:
        return model
    failure = bundle.splice_failures.get(name)
    if failure is not None:
        raise failure
    raise ModelNotFoundError(name, context=f"An extension of {where}" if where else None)


def _reserved_names(models: Dict[str, SlayerModel], bundle: ResolvedSourceBundle) -> set:
    return {
        *bundle.datasource_model_names, *(m.name for m in models.values() if m.explicit_spelling is None),
        *(m.explicit_spelling for m in models.values() if m.explicit_spelling is not None),
        *(d.name for d in bundle.stage_displays.values()),
    }


def apply_query_extensions(
    *, query: SlayerQuery, scope: "ModelScope | StageSchema | None", bundle: ResolvedSourceBundle,
) -> "Tuple[ModelScope | StageSchema | None, ResolvedSourceBundle]":
    """``query``'s extensions applied to the stage's models: columns and measures overlaid, joins merged into the
    join graph, and each INNER join recorded as a population filter."""
    if not query.extensions:
        return scope, bundle
    display = bundle.stage_displays.get(query.name) if query.name else None
    where = display.label if display is not None else None
    models = _universe(bundle)
    for extension in query.extensions:
        model = _extended(extension.source_name, models=models, bundle=bundle, where=where)
        models[model.name] = overlay_columns_and_measures(model, extension)
    merge = EdgeMerge(
        models=models, reserved_names=_reserved_names(models, bundle),
        stored_edge_names={
            *bundle.datasource_edge_names,
            *(j.name for m in models.values() for j in m.joins if j.name and not j.query_local),
        },
    )
    restrictions = _merge_joins(query=query, merge=merge, bundle=bundle, where=where)
    models = merge.models
    source = models[bundle.source_model.name] if bundle.source_model is not None else None
    bundle = bundle.model_copy(update={
        "source_model": source,
        "referenced_models": [models.get(m.name, m) for m in bundle.referenced_models],
        "inner_restrictions": (*bundle.inner_restrictions, *restrictions),
    })
    return _extended_scope(scope, query=query, models=models, source=source), bundle


def _merge_joins(
    *, query: SlayerQuery, merge: EdgeMerge, bundle: ResolvedSourceBundle, where: Optional[str],
) -> List[InnerRestriction]:
    """Merge every extension join into ``merge``; the INNER ones' population filters."""
    restrictions = [
        _merge_join(declaring=extension.source_name, join=join, merge=merge, bundle=bundle, where=where)
        for extension in query.extensions or [] for join in extension.joins or []
    ]
    return [r for r in restrictions if r is not None]


def _merge_join(
    *, declaring: str, join: ModelJoin, merge: EdgeMerge, bundle: ResolvedSourceBundle, where: Optional[str],
) -> Optional[InnerRestriction]:
    """Merge one extension join into ``merge``; its population filter when INNER."""
    if join.target_model not in merge.models:
        _extended(join.target_model, models=merge.models, bundle=bundle, where=where)
    try:
        merged = merge.add(declaring=declaring, join=join)
    except QueryExtensionError as exc:
        raise QueryExtensionError(f"{where}: {exc}" if where else str(exc)) from exc
    if join.join_type is not JoinType.INNER:
        return None
    source = merge.models[declaring]
    target = merge.models[join.target_model]
    return InnerRestriction(
        model=source.spelling, token=merged.token, key=merged.edge.join_pairs[0][1],
        label=f"INNER join from {source.spelling!r} to {target.spelling!r}" + (f" in {where}" if where else ""),
    )


def _extended_scope(
    scope: "ModelScope | StageSchema | None", *, query: SlayerQuery, models: Dict[str, SlayerModel],
    source: Optional[SlayerModel],
) -> "ModelScope | StageSchema | None":
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        return ModelScope(source_model=models.get(scope.source_model.name, scope.source_model))
    if isinstance(scope, StageSchema) and source is not None and extension_of(query, source.name) is not None:
        return ModelScope(source_model=source)
    return scope
