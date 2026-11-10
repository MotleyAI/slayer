"""ResolvedSourceBundle: eagerly resolved query inputs (P11).
The orchestrator builds this once at execute start; the binder reads it purely.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType
from slayer.core.granularity import CustomGranularity
from slayer.core.join_edges import EdgeMerge
from slayer.core.join_walker import with_list_children
from slayer.core.models import (
    Column,
    SlayerModel,
    _check_column_measure_namespace,
    _check_join_keys,
)
from slayer.core.query import ModelExtension, SlayerQuery, SourceSpec

from slayer.core.scope import ModelScope, StageDisplay, StageSchema

__all__ = [
    "InnerRestriction",
    "resolve_scope",
    "ResolvedSourceBundle",
    "apply_extension_overlay",
    "follow_sibling_chain",
    "model_from_stage_schema",
    "overlay_columns_and_measures",
    "source_name_if_sibling",
    "stage_bundle_with_siblings",
]


class InnerRestriction(BaseModel):
    """A query-declared INNER join: the field filter ``<model>.<token>.<key> IS NOT NULL``."""

    model_config = ConfigDict(frozen=True)

    model: str
    token: str
    key: str
    # How messages name the declaration.
    label: str


class ResolvedSourceBundle(BaseModel):
    """Eagerly resolved inputs to one query execution (P11)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_model: Optional[SlayerModel] = None
    referenced_models: List[SlayerModel] = Field(default_factory=list)
    inline_extensions: List[ModelExtension] = Field(default_factory=list)
    named_queries: Dict[str, SlayerQuery] = Field(default_factory=dict)
    # Per-named-stage source model (sibling-sourced stages omitted).
    stage_source_models: Dict[str, SlayerModel] = Field(default_factory=dict)
    query_variables: Dict[str, Any] = Field(default_factory=dict)
    datasource_hint: Optional[str] = None
    dialect: str  # sqlglot dialect the query renders in
    # The datasource's type: picks its own dialect where several share a sqlglot name (MariaDB / MySQL).
    datasource_type: Optional[str] = None
    # Minted stage identity → its user-facing spelling.
    stage_displays: Dict[str, StageDisplay] = Field(default_factory=dict)
    # Stored query-backed models the statement may splice, including result joins.
    query_backed: Dict[str, SlayerModel] = Field(default_factory=dict)
    # Query-backed models whose stages are being planned, outermost first.
    splice_chain: Tuple[str, ...] = ()
    runtime_variables: Dict[str, Any] = Field(default_factory=dict)
    # Query-backed models this stage reads that could not be spliced, with the cause.
    splice_failures: Dict[str, Exception] = Field(default_factory=dict)
    # The one clock reading every time point in this execution resolves against.
    now: datetime = Field(default_factory=datetime.now)
    # The datasource's custom granularities, keyed by ``granularity_key``.
    granularities: Dict[str, CustomGranularity] = Field(default_factory=dict)
    # A stored model shadows the built-in ``time_spine``.
    spine_clash: bool = False
    # The datasource's model names and stored edge names (what a query-local edge must not be named).
    datasource_model_names: FrozenSet[str] = frozenset()
    datasource_edge_names: FrozenSet[str] = frozenset()
    # The stage's query-declared INNER joins, read as population filters.
    inner_restrictions: Tuple[InnerRestriction, ...] = ()

    def definition_view(self) -> "ResolvedSourceBundle":
        """This bundle as stored definitions see it: without anything a query extension added."""
        models = [*([self.source_model] if self.source_model is not None else []), *self.referenced_models]
        if not any(m.extended for m in models):
            return self
        return self.model_copy(update={
            "source_model": self.source_model.definition_view() if self.source_model is not None else None,
            "referenced_models": [m.definition_view() for m in self.referenced_models],
        })

    def get_referenced_model(self, name: str) -> Optional[SlayerModel]:
        """Linear lookup by name (list is small, O(n) scan is fine); a list child resolves through its parent."""
        for m in self.referenced_models:
            if m.name == name:
                return m
        child = self.models_by_name.get(name)
        return child if child is not None and child.list_of is not None else None

    def factor_free(self, model: SlayerModel) -> SlayerModel:
        """``model`` as stored: a population without its product hops."""
        if model.unrelated_datasets:
            model = model.with_unrelated(frozenset())
        return model.as_population_factor(False) if model.population_spine else model

    def rerooted(self, new_source: SlayerModel) -> "ResolvedSourceBundle":
        """Re-root at ``new_source``, keeping the former source in the universe
        (a reverse hop can target it — the map must not shrink); a producer never
        sees a spine population's product hop."""
        new_source = self.factor_free(new_source)
        refs = self.referenced_models
        old = self.source_model
        if old is not None and self.get_referenced_model(old.name) is None:
            refs = [*refs, old]
        return self.model_copy(
            update={"source_model": new_source, "referenced_models": refs},
        )

    @property
    def models_by_name(self) -> Dict[str, SlayerModel]:
        """The host-inclusive model map every walker and scanner consumes: the
        source model first (so a source→host reverse hop resolves deterministically,
        not incidentally), then the referenced models, de-duplicated by name with
        the source winning. Production bundles already list the source among
        ``referenced_models``; a hand-built bundle that omits it still exposes it."""
        out: Dict[str, SlayerModel] = {}
        if self.source_model is not None:
            out[self.source_model.name] = self.source_model
        for m in self.referenced_models:
            out.setdefault(m.name, m)
        return with_list_children(out)


def overlay_columns_and_measures(base: SlayerModel, ext: ModelExtension) -> SlayerModel:
    """``base`` with ``ext``'s columns and measures, marked query-local."""
    columns, measures = list(ext.columns or []), list(ext.measures or [])
    if not columns and not measures:
        return base
    merged = base.model_copy(update={
        "columns": [*base.columns, *columns], "measures": [*base.measures, *measures],
    }).with_query_local(base.query_local_names | {c.name for c in columns} | {m.name for m in measures if m.name})
    if not merged.source_queries:
        _check_column_measure_namespace(model_name=merged.name, columns=merged.columns, measures=merged.measures)
    return merged


def apply_extension_overlay(base: SlayerModel, ext: ModelExtension) -> SlayerModel:
    """``base`` extended by ``ext`` alone: its joins merged with ``base``'s own edges."""
    merge = EdgeMerge(models={base.name: overlay_columns_and_measures(base, ext)})
    for join in ext.joins or []:
        merge.add(declaring=base.name, join=join)
    return merge.models[base.name]


def source_name_if_sibling(
    spec: SourceSpec | None, sibling_names: "set[str] | Dict[str, Any]"
) -> Optional[str]:
    """The sibling stage a bare-name spec reads from, else ``None``."""
    return spec if isinstance(spec, str) and spec in sibling_names else None


def follow_sibling_chain(
    spec: SourceSpec | None, named_queries: Dict[str, SlayerQuery]
) -> SourceSpec | None:
    """Resolve a sibling-pointing ``source_model`` to the real base spec (cycle raises ``ValueError``)."""
    seen: List[str] = []
    while True:
        sib = source_name_if_sibling(spec, named_queries)
        if sib is None:
            return spec
        if sib in seen:
            chain = " -> ".join([*seen, sib])
            raise ValueError(
                f"Circular reference detected in source_queries DAG: {chain}"
            )
        seen.append(sib)
        spec = named_queries[sib].source_model


def model_from_stage_schema(
    *,
    name: str,
    schema: "StageSchema",
    data_source: str,
    sql: Optional[str] = None,
    column_sql: Optional[Dict[str, str]] = None,
    default_time_dimension: Optional[str] = None,
    saved: Optional[SlayerModel] = None,
) -> SlayerModel:
    """The model a stage's output is read as: over its CTE (``sql=None``) or over ``sql``,
    with the joins and filters of ``saved``, the stored query-backed model it reads as.

    A single-column grain is stamped ``unique``; a composite grain ``primary_key`` on each member.
    """
    grain = schema.grain or []
    composite = len(grain) > 1
    column_sql = column_sql or {}
    model = SlayerModel(
        name="_stage",
        data_source=data_source,
        sql_table="_stage" if sql is None else None,
        sql=sql,
        columns=[
            Column(
                name=c.name,
                sql=column_sql.get(c.name),
                type=c.type or DataType.DOUBLE,
                element_type=(c.element_type or DataType.UNKNOWN) if c.type is DataType.ARRAY else None,
                granularity=c.granularity.name if isinstance(c.granularity, CustomGranularity) else c.granularity,
                label=c.label,
                format=c.format,
                description=c.description,
                primary_key=composite and c.name in grain,
                unique=not composite and c.name in grain,
            ).with_respellings(c.respellings)
            for c in schema.columns
        ],
    )
    joins = list(saved.joins) if saved is not None else []
    # Joins attach after the rename (validators skip model_copy), so check their keys here.
    _check_join_keys(model_name=name, columns=model.columns, joins=joins)
    # A minted stage identity carries the reserved prefix the name validator rejects.
    return model.model_copy(update={
        "name": name, "sql_table": name if sql is None else None,
        "default_time_dimension": default_time_dimension or schema.default_time_dimension,
        "joins": joins,
        "filters": list(saved.filters) if saved is not None else [],
    }).with_spelling(schema.display_name if schema.display_name != name else None)


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
    referenced models minus the host's own entry. A sibling identity meeting a
    referenced model is an invariant violation (identities are minted unique).
    """
    synths = [
        model_from_stage_schema(
            name=n, schema=s, data_source=data_source, saved=bundle.query_backed.get(n),
        )
        for n, s in sibling_schemas.items()
    ]
    clashes = sorted({s.name for s in synths} & {m.name for m in bundle.referenced_models})
    if clashes:
        raise ValueError(
            f"Stage identities {clashes} collide with models in the source bundle; "
            f"stage identities must be minted apart from model names."
        )
    referenced = (
        [source_model]
        + synths
        + [m for m in bundle.referenced_models if m.name != source_model.name]
    )
    return bundle.model_copy(
        update={"source_model": source_model, "referenced_models": referenced}
    )




def resolve_scope(
    *,
    query,
    bundle: "ResolvedSourceBundle",
    stage_schemas: Optional[Dict[str, "StageSchema"]],
):
    """The scope a query binds against: its named sibling stage's schema, else the bundle's model."""
    source = query.source_model
    if isinstance(source, str) and source in (stage_schemas or {}):
        return (stage_schemas or {})[source]
    return ModelScope(source_model=bundle.source_model)
