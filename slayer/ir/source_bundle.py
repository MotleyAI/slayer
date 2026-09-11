"""ResolvedSourceBundle: eagerly resolved query inputs (P11).
The orchestrator builds this once at execute start; the binder reads it purely.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
    _check_column_measure_namespace,
)
from slayer.core.query import ModelExtension, SlayerQuery

if TYPE_CHECKING:
    from slayer.core.scope import StageSchema


class ResolvedSourceBundle(BaseModel):
    """Eagerly resolved inputs to one query execution (P11)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_model: Optional[SlayerModel] = None
    referenced_models: List[SlayerModel] = Field(default_factory=list)
    inline_extensions: List[ModelExtension] = Field(default_factory=list)
    named_queries: Dict[str, SlayerQuery] = Field(default_factory=dict)
    # DEV-1450 stage 7b.15d — per-named-stage resolved source model, keyed by
    # stage name. Populated for siblings whose source resolves to a concrete
    # model (a stored model, an inline ``SlayerModel``, or a ``ModelExtension``
    # over a stored base). Siblings sourced FROM another sibling (chain or a
    # ``ModelExtension`` over a sibling) are omitted — the planner resolves
    # those against the upstream ``StageSchema`` at plan time. Lets each stage
    # in a heterogeneous DAG bind against its OWN source rather than the root's.
    # Per-named-stage source model (sibling-sourced stages omitted).
    stage_source_models: Dict[str, SlayerModel] = Field(default_factory=dict)
    query_variables: Dict[str, Any] = Field(default_factory=dict)
    datasource_hint: Optional[str] = None

    def get_referenced_model(self, name: str) -> Optional[SlayerModel]:
        """Linear lookup by name (list is small, O(n) scan is fine)."""
        for m in self.referenced_models:
            if m.name == name:
                return m
        return None


# Anything accepted as ``SlayerQuery.source_model``.
SourceSpec = Union[str, SlayerModel, ModelExtension, Dict[str, Any]]


def _apply_extension_overlay(
    base: SlayerModel, ext: ModelExtension
) -> SlayerModel:
    """Extend ``base`` with the extra columns / measures / joins of ``ext``."""
    extra_cols = [
        Column.model_validate(c) if isinstance(c, dict) else c
        for c in (ext.columns or [])
    ]
    extra_measures = [
        ModelMeasure.model_validate(m) if isinstance(m, dict) else m
        for m in (ext.measures or [])
    ]
    extra_joins = [
        ModelJoin.model_validate(j) if isinstance(j, dict) else j
        for j in (ext.joins or [])
    ]
    # Query-backed base defers its measure overlay to post-expansion; overlaying
    # now would build the source_queries+measures combo the validator rejects.
    overlay_measures = list(base.measures) if base.source_queries else (
        list(base.measures) + extra_measures
    )
    merged = base.model_copy(
        update={
            "columns": list(base.columns) + extra_cols,
            "measures": overlay_measures,
            "joins": list(base.joins) + extra_joins,
        }
    )
    # model_copy runs no validators; re-run the namespace check so an overlay
    # name reusing an existing one is a loud error, not a silent shadow.
    if not merged.source_queries:
        _check_column_measure_namespace(
            model_name=merged.name,
            columns=merged.columns,
            measures=merged.measures,
        )
    return merged


def _source_name_if_sibling(
    spec: SourceSpec, sibling_names: "set[str] | Dict[str, Any]"
) -> Optional[str]:
    """Return the sibling stage name a ``source_model`` spec reads from, if any.

    Covers the bare-string, ``ModelExtension``, and dict-with-``source_name``
    forms; returns ``None`` when the name is not in ``sibling_names``.
    """
    if isinstance(spec, str):
        return spec if spec in sibling_names else None
    if isinstance(spec, ModelExtension):
        return spec.source_name if spec.source_name in sibling_names else None
    if isinstance(spec, dict) and isinstance(spec.get("source_name"), str):
        nm = spec["source_name"]
        return nm if nm in sibling_names else None
    return None


def _spec_adds_measures(spec: SourceSpec) -> bool:
    """True when a ``source_model`` spec is a ``ModelExtension`` carrying measures."""
    if isinstance(spec, ModelExtension):
        return bool(spec.measures)
    if isinstance(spec, dict) and isinstance(spec.get("source_name"), str):
        return bool(spec.get("measures"))
    return False


def _follow_sibling_chain(
    spec: SourceSpec, named_queries: Dict[str, SlayerQuery]
) -> SourceSpec:
    """Resolve a sibling-pointing ``source_model`` to the real base spec (cycle raises ``ValueError``)."""
    seen: List[str] = []
    while True:
        sib = _source_name_if_sibling(spec, named_queries)
        if sib is None:
            return spec
        if sib in seen:
            chain = " -> ".join([*seen, sib])
            raise ValueError(
                f"Circular reference detected in source_queries DAG: {chain}"
            )
        seen.append(sib)
        spec = named_queries[sib].source_model


def _as_extension_over_nonsibling(
    spec: SourceSpec, sibling_names: "set[str]"
) -> Optional[ModelExtension]:
    """Return the ``ModelExtension`` if ``spec`` overlays a NON-sibling base.

    ``None`` for plain strings, inline models, and overlays over a sibling.
    """
    if isinstance(spec, ModelExtension):
        ext = spec
    elif isinstance(spec, dict) and isinstance(spec.get("source_name"), str):
        ext = ModelExtension.model_validate(spec)
    else:
        return None
    if ext.source_name in sibling_names:
        return None
    return ext


def synthetic_model_from_stage_schema(
    *, name: str, schema: "StageSchema", data_source: str
) -> SlayerModel:
    """Stand-in ``SlayerModel`` whose ``sql_table`` is a stage's CTE name and
    whose columns are that stage's flat output columns.

    Lets the planner resolve a join / cross-model ref targeting a sibling stage
    (materialised as a CTE elsewhere). ``StageColumn.name`` is already the
    ``__``-flattened bind name, so synthetic column names match downstream refs.
    """
    return SlayerModel(
        name=name,
        data_source=data_source or "_stage",
        sql_table=name,
        columns=[
            Column(name=c.name, type=c.type or DataType.DOUBLE)
            for c in schema.columns
        ],
    )


def stage_bundle_with_siblings(
    *,
    bundle: ResolvedSourceBundle,
    source_model: SlayerModel,
    sibling_schemas: Dict[str, "StageSchema"],
    data_source: str,
) -> ResolvedSourceBundle:
    """Per-stage bundle: ``source_model`` is the stage's own host; synthetic
    sibling models (one per emitted ``StageSchema``) are threaded into
    ``referenced_models`` so a join / cross-model ref to a sibling resolves.

    Order: host first, then synthetic siblings, then the original bundle's
    referenced models minus any shadowed by the host or a synthetic sibling.
    """
    synths = [
        synthetic_model_from_stage_schema(
            name=n, schema=s, data_source=data_source
        )
        for n, s in sibling_schemas.items()
    ]
    shadow = {source_model.name} | {s.name for s in synths}
    referenced = (
        [source_model]
        + synths
        + [m for m in bundle.referenced_models if m.name not in shadow]
    )
    return bundle.model_copy(
        update={"source_model": source_model, "referenced_models": referenced}
    )


