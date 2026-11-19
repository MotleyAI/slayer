"""ResolvedSourceBundle: eagerly resolved query inputs (P11).
The orchestrator builds this once at execute start; the binder reads it purely.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType
from slayer.core.granularity import CustomGranularity
from slayer.core.relationship_edges import EdgeMerge
from slayer.core.relationship_walker import with_list_children
from slayer.core.models import (
    DatasetField,
    SlayerDataset,
    _check_field_metric_namespace,
    _check_relationship_keys,
)
from slayer.core.query import DatasetExtension, SlayerQuery, SourceSpec
from slayer.core.scope import DatasetScope, StageDisplay, StageSchema

__all__ = [
    "InnerRestriction",
    "resolve_scope",
    "ResolvedSourceBundle",
    "apply_extension_overlay",
    "follow_sibling_chain",
    "dataset_from_stage_schema",
    "overlay_metrics",
    "source_name_if_sibling",
    "stage_bundle_with_siblings",
]


class InnerRestriction(BaseModel):
    """A query-declared INNER join: the field filter ``<dataset>.<token>.<key> IS NOT NULL``."""

    model_config = ConfigDict(frozen=True)

    dataset: str
    token: str
    key: str
    # How messages name the declaration.
    label: str


class ResolvedSourceBundle(BaseModel):
    """Eagerly resolved inputs to one query execution (P11)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    population: Optional[SlayerDataset] = None
    referenced_datasets: List[SlayerDataset] = Field(default_factory=list)
    named_queries: Dict[str, SlayerQuery] = Field(default_factory=dict)
    # The root query as resolved; with ``named_queries``, the stages the planner reads.
    query: Optional[SlayerQuery] = None
    # Per-named-stage source dataset (sibling-sourced stages omitted).
    stage_source_datasets: Dict[str, SlayerDataset] = Field(default_factory=dict)
    query_variables: Dict[str, Any] = Field(default_factory=dict)
    datasource_hint: Optional[str] = None
    dialect: str  # sqlglot dialect the query renders in
    # The datasource's type: picks its own dialect where several share a sqlglot name (MariaDB / MySQL).
    datasource_type: Optional[str] = None
    # Minted stage identity → its user-facing spelling.
    stage_displays: Dict[str, StageDisplay] = Field(default_factory=dict)
    # Stored query-backed datasets the statement may splice, including result joins.
    query_backed: Dict[str, SlayerDataset] = Field(default_factory=dict)
    # Query-backed datasets whose stages are being planned, outermost first.
    splice_chain: Tuple[str, ...] = ()
    runtime_variables: Dict[str, Any] = Field(default_factory=dict)
    # Query-backed datasets this stage reads that could not be spliced, with the cause.
    splice_failures: Dict[str, Exception] = Field(default_factory=dict)
    # The one clock reading every time point in this execution resolves against.
    now: datetime = Field(default_factory=datetime.now)
    # The datasource's custom granularities, keyed by ``granularity_key``.
    granularities: Dict[str, CustomGranularity] = Field(default_factory=dict)
    # A stored dataset shadows the built-in ``time_spine``.
    spine_clash: bool = False
    # The datasource's dataset names and stored edge names (what a query-local edge must not be named).
    datasource_dataset_names: FrozenSet[str] = frozenset()
    datasource_edge_names: FrozenSet[str] = frozenset()
    # The stage's query-declared INNER joins, read as population filters.
    inner_restrictions: Tuple[InnerRestriction, ...] = ()

    def definition_view(self) -> "ResolvedSourceBundle":
        """This bundle as stored definitions see it: without anything a query extension added."""
        datasets = [*([self.population] if self.population is not None else []), *self.referenced_datasets]
        if not any(m.extended for m in datasets):
            return self
        return self.model_copy(update={
            "population": self.population.definition_view() if self.population is not None else None,
            "referenced_datasets": [m.definition_view() for m in self.referenced_datasets],
        })

    def get_referenced_dataset(self, name: str) -> Optional[SlayerDataset]:
        """Linear lookup by name (list is small, O(n) scan is fine); a list child resolves through its parent."""
        for m in self.referenced_datasets:
            if m.name == name:
                return m
        child = self.datasets_by_name.get(name)
        return child if child is not None and child.list_of is not None else None

    def factor_free(self, dataset: SlayerDataset) -> SlayerDataset:
        """``dataset`` as stored: a population without its product hops."""
        if dataset.unrelated_datasets:
            dataset = dataset.with_unrelated(frozenset())
        return dataset.as_population_factor(False) if dataset.population_spine else dataset

    def rerooted(self, new_source: SlayerDataset) -> "ResolvedSourceBundle":
        """Re-root at ``new_source``, keeping the former source in the universe
        (a reverse hop can target it — the map must not shrink); a producer never
        sees a spine population's product hop."""
        new_source = self.factor_free(new_source)
        refs = self.referenced_datasets
        old = self.population
        if old is not None and self.get_referenced_dataset(old.name) is None:
            refs = [*refs, old]
        return self.model_copy(
            update={"population": new_source, "referenced_datasets": refs},
        )

    @property
    def datasets_by_name(self) -> Dict[str, SlayerDataset]:
        """The host-inclusive dataset map every walker and scanner consumes: the
        source dataset first (so a source→host reverse hop resolves deterministically,
        not incidentally), then the referenced datasets, de-duplicated by name with
        the source winning. Production bundles already list the source among
        ``referenced_datasets``; a hand-built bundle that omits it still exposes it."""
        out: Dict[str, SlayerDataset] = {}
        if self.population is not None:
            out[self.population.name] = self.population
        for m in self.referenced_datasets:
            out.setdefault(m.name, m)
        return with_list_children(out)


def overlay_metrics(base: SlayerDataset, ext: DatasetExtension) -> SlayerDataset:
    """``base`` with ``ext``'s metrics, marked query-local."""
    metrics = list(ext.metrics or [])
    if not metrics:
        return base
    merged = base.model_copy(update={"metrics": [*base.metrics, *metrics]}).with_query_local(
        base.query_local_names | {m.name for m in metrics if m.name})
    if not merged.source_queries:
        _check_field_metric_namespace(dataset_name=merged.name, fields=merged.fields, metrics=merged.metrics)
    return merged


def apply_extension_overlay(base: SlayerDataset, ext: DatasetExtension) -> SlayerDataset:
    """``base`` extended by ``ext`` alone: its relationships merged with ``base``'s own edges."""
    merge = EdgeMerge(datasets={base.name: overlay_metrics(base, ext)})
    for relationship in ext.relationships or []:
        merge.add(declaring=base.name, relationship=relationship)
    return merge.datasets[base.name]


def source_name_if_sibling(
    spec: SourceSpec | None, sibling_names: "set[str] | Dict[str, Any]"
) -> Optional[str]:
    """The sibling stage a bare-name spec reads from, else ``None``."""
    return spec if isinstance(spec, str) and spec in sibling_names else None


def follow_sibling_chain(
    spec: SourceSpec | None, named_queries: Dict[str, SlayerQuery]
) -> SourceSpec | None:
    """Resolve a sibling-pointing ``population`` to the real base spec (cycle raises ``ValueError``)."""
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
        spec = named_queries[sib].population


def dataset_from_stage_schema(
    *,
    name: str,
    schema: "StageSchema",
    data_source: str,
    sql: Optional[str] = None,
    field_sql: Optional[Dict[str, str]] = None,
    default_time_dimension: Optional[str] = None,
    saved: Optional[SlayerDataset] = None,
) -> SlayerDataset:
    """The dataset a stage's output is read as: over its CTE (``sql=None``) or over ``sql``,
    with the relationships and filters of ``saved``, the stored query-backed dataset it reads as.

    A single-field grain is stamped ``unique``; a composite grain ``primary_key`` on each member.
    """
    grain = schema.grain or []
    composite = len(grain) > 1
    field_sql = field_sql or {}
    dataset = SlayerDataset(
        name="_stage",
        data_source=data_source,
        sql_table="_stage" if sql is None else None,
        sql=sql,
        fields=[
            DatasetField(
                name=c.name,
                sql=field_sql.get(c.name),
                datatype=c.datatype or DataType.FLOAT,
                element_type=(c.element_type or DataType.OPAQUE) if c.datatype is DataType.ARRAY else None,
                granularity=c.granularity.name if isinstance(c.granularity, CustomGranularity) else c.granularity,
                label=c.label,
                format=c.format,
                description=c.description,
                primary_key=composite and c.name in grain,
                unique=not composite and c.name in grain,
            ).with_respellings(c.respellings)
            for c in schema.fields
        ],
    )
    relationships = list(saved.relationships) if saved is not None else []
    # Relationships attach after the rename (validators skip model_copy), so check their keys here.
    _check_relationship_keys(dataset_name=name, fields=dataset.fields, relationships=relationships)
    # A minted stage identity carries the reserved prefix the name validator rejects.
    return dataset.model_copy(update={
        "name": name, "sql_table": name if sql is None else None,
        "default_time_dimension": default_time_dimension or schema.default_time_dimension,
        "relationships": relationships,
        "filters": list(saved.filters) if saved is not None else [],
    }).with_spelling(schema.display_name if schema.display_name != name else None)


def stage_bundle_with_siblings(
    *,
    bundle: ResolvedSourceBundle,
    source_dataset: SlayerDataset,
    sibling_schemas: Dict[str, "StageSchema"],
    data_source: str,
) -> ResolvedSourceBundle:
    """Per-stage bundle: ``population`` is the stage's own host; synthetic
    sibling datasets (one per emitted ``StageSchema``) are threaded into
    ``referenced_datasets`` so a relationship / cross-dataset ref to a sibling resolves.

    Order: host first, then synthetic siblings, then the original bundle's
    referenced datasets minus the host's own entry. A sibling identity meeting a
    referenced dataset is an invariant violation (identities are minted unique).
    """
    synths = [
        dataset_from_stage_schema(
            name=n, schema=s, data_source=data_source, saved=bundle.query_backed.get(n),
        )
        for n, s in sibling_schemas.items()
    ]
    clashes = sorted({s.name for s in synths} & {m.name for m in bundle.referenced_datasets})
    if clashes:
        raise ValueError(
            f"Stage identities {clashes} collide with datasets in the source bundle; "
            f"stage identities must be minted apart from dataset names."
        )
    referenced = (
        [source_dataset]
        + synths
        + [m for m in bundle.referenced_datasets if m.name != source_dataset.name]
    )
    return bundle.model_copy(
        update={"population": source_dataset, "referenced_datasets": referenced}
    )




def resolve_scope(
    *,
    query: SlayerQuery,
    bundle: "ResolvedSourceBundle",
    stage_schemas: Optional[Dict[str, "StageSchema"]],
):
    """The scope a query binds against: its named sibling stage's schema, else the bundle's dataset."""
    source = query.population
    if isinstance(source, str) and source in (stage_schemas or {}):
        return (stage_schemas or {})[source]
    return DatasetScope(population=bundle.population)
