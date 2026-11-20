"""FacadeCatalog build (shared by Flight + Postgres facades).

Snapshots the live ``StorageBackend`` view into a wire-facade-shaped
catalog: one logical catalog (``"slayer"``), one schema per datasource,
one table per non-hidden ``SlayerDataset``, and on each table a fan-out
of metrics + dimensions derived from the dataset's fields, saved
metrics, custom aggregations, and reachable relationship paths.

No caching in Phase 1 — every handler call rebuilds the catalog
fresh (spec §7.2). The cost on small-to-mid storages is sub-
millisecond; if profiling makes the case, a follow-up adds a
``StorageBackend.serial()`` accessor + cache invalidation.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.relationship_search import paths_from, unaddressable_pairs
from slayer.core.relationship_walker import OrientedRelationship, OrientedLike, neighbors
from slayer.core.enums import (
    DEFAULT_AGGREGATIONS_BY_TYPE,
    DataType,
    JoinType,
    PRIMARY_KEY_AGGREGATIONS,
    RelationshipCardinality,
    TEMPORAL_DATATYPES,
)
from slayer.core.models import (
    Aggregation,
    DatasetField,
    SlayerDataset,
    is_identifier,
)
from slayer.core.refs import functional_agg_text
from slayer.core.syntax import metric_text_at_path
from slayer.engine.key_metadata import field_aggregate_type
from slayer.facade.datatypes import SUPPORTED_DATATYPES

logger = logging.getLogger(__name__)

# Aggregations that need named parameters beyond ``{value}`` — we cannot
# bake a defensible default into a flat-name catalog, so the field-agg
# expansion (§5.1 rule 3) and the custom-agg expansion (rule 4) both
# skip these for built-ins. Custom aggs with non-empty ``params`` are
# also skipped per rule 4 for the same reason.
_PARAMETRIC_BUILTIN_AGGS: frozenset[str] = frozenset({
    "weighted_avg", "percentile", "corr", "covar_samp", "covar_pop",
})

DEFAULT_BFS_DEPTH = 3
CATALOG_NAME = "slayer"


class FacadeMetric(BaseModel):
    name: str
    description: str | None = None
    label: str | None = None
    data_type: DataType | None = None
    metric_expression: str


class FacadeDimension(BaseModel):
    name: str
    description: str | None = None
    label: str | None = None
    data_type: DataType
    is_time: bool
    dimension_ref: str
    # False when the relationship path fans out: excluded from SELECT * expansion.
    row_preserving: bool = True


class FacadeRelationship(BaseModel):
    """A direct (single-hop) relationship from a parent dataset to a target dataset.

    Used by the wire-facade translator to recognise BI-tool-emitted
    LEFT JOIN-with-subquery shapes against the parent's configured relationships.
    Only relationships whose target is a non-hidden dataset in the same catalog are
    exposed, mirroring the BFS dim/metric filter.
    """
    to: str
    column_pairs: list[tuple[str, str]]
    join_type: JoinType = JoinType.LEFT
    cardinality: RelationshipCardinality | None = None
    name: str | None = None


class FacadeTable(BaseModel):
    # arbitrary_types_allowed lets `dataset_ref` carry the in-memory
    # SlayerDataset handle without Pydantic deep-copying / re-validating it.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    table_type: str
    description: str | None = None
    metrics: list[FacadeMetric]
    dimensions: list[FacadeDimension]
    relationships: list[FacadeRelationship] = Field(default_factory=list)
    # In-memory handle to the underlying SlayerDataset — required by the
    # translator for ON-clause field validation (hidden FK/PK fields
    # don't appear on `dimensions`) and for the dynamic-join lookup
    # materialisation. Excluded from any future serialisation
    # of FacadeCatalog.
    dataset_ref: SlayerDataset | None = Field(default=None, exclude=True)


class FacadeSchema(BaseModel):
    name: str
    tables: list[FacadeTable]


class FacadeCatalog(BaseModel):
    catalog_name: str = CATALOG_NAME
    schemas: list[FacadeSchema]


def local_metrics(table: FacadeTable) -> list[FacadeMetric]:
    """Metrics that should appear in the per-table flat-column view
    (``pg_attribute`` / ``INFORMATION_SCHEMA.COLUMNS``) — saved
    ``DatasetMetric`` entries only.

    ``_metric_expansion`` produces three kinds of entries on
    every table:

    1. **Cross-dataset entries** — names like ``customers.row_count`` /
       ``customers.regions.population_sum`` produced by joining sibling
       datasets' metrics under a dotted prefix.
    2. **Synthetic same-dataset entries** — the ``row_count`` rule-1
       metric (``metric_expression="count(*)"``), the field × built-in-
       aggregation cartesian (``<col>_<agg>``, keyed by the expression
       ``<agg>(<col>)``), and the field × custom-aggregation
       cartesian. None of these are user-authored — they're catalog
       fan-out so BI tools can pick any field × any agg.
    3. **Saved metrics** — ``DatasetMetric`` entries where the catalog
       sets ``name == metric_expression`` (the user named them, so the
       expression IS the name).

    BI tools (Metabase, dbt schema scan, pgjdbc clients) that flatten
    the catalog through ``pg_attribute`` then discover every dimension
    AND every metric as a projectable "column" of the parent table and
    emit ``SELECT *``-style queries listing them all. Metabase's MBQL
    fingerprint pass then wraps each one in ``COUNT(...)``/``MAX(...)``,
    landing dotted names in ``SlayerQuery.metrics[*].name`` (Pydantic
    rejects them) or exploding the wire response width (the C.1 e2e
    test asserts ``len(cols) == 7``, the count of ``orders``' user-
    authored fields).

    Both kinds (1) and (2) are stripped here. Saved metrics (kind 3)
    stay because the user named them — they ARE the dataset's queryable
    surface.

    The raw ``table.metrics`` list keeps all three kinds so:
      * ``INFORMATION_SCHEMA.METRICS`` still exposes them as the
        catalog-namespaced answer to "what can I aggregate?";
      * the catalog-SQL fingerprint hash still tracks them for cache
        invalidation;
      * the translator's ``metrics_by_name`` / ``metrics_by_formula``
        lookups still resolve hand-written cross-dataset SQL (rejected
        there by the translator-side guard) and same-dataset aggregate
        refs like ``MAX(total)``.

    The "dot in name" cross-dataset predicate is safe because every
    catalog-side name source forbids dots: ``DatasetField.name``,
    ``DatasetMetric.name``, and ``Aggregation.name`` all
    enforce ``[a-zA-Z_][a-zA-Z0-9_]*``.
    """
    return [
        m for m in table.metrics
        if "." not in m.name and m.metric_expression == m.name
    ]


def local_dimensions(table: FacadeTable) -> list[FacadeDimension]:
    """Mirror of :func:`local_metrics` for dimensions: drop cross-dataset
    entries (single-hop and multi-hop joined dimensions). Unlike
    metrics, dimensions don't have a synthetic-vs-user-authored split —
    every dimension IS a ``DatasetField`` the user defined on the dataset.
    See :func:`local_metrics` for the leak-path rationale."""
    return [d for d in table.dimensions if "." not in d.name]


def build_catalog(
    *,
    datasets_by_datasource: dict[str, list[SlayerDataset]],
    bfs_depth: int = DEFAULT_BFS_DEPTH,
) -> FacadeCatalog:
    """Build a ``FacadeCatalog`` snapshot.

    ``datasets_by_datasource`` maps each datasource name to its dataset list;
    the caller (typically the handlers) builds this from ``storage.
    list_datasets(data_source=...)`` so cross-datasource relationships are naturally
    constrained (SLayer doesn't auto-mirror relationships across datasources).
    """
    schemas: list[FacadeSchema] = []
    for datasource, models in datasets_by_datasource.items():
        by_name: dict[str, SlayerDataset] = {m.name: m for m in models}
        for pair in unaddressable_pairs(datasets_by_name=by_name):
            logger.warning("Facade catalog (datasource %r): %s.", datasource, pair.describe())
        tables: list[FacadeTable] = []
        for model in models:
            if model.hidden:
                continue
            if not _field_types_supported(dataset=model):
                # _field_types_supported logs the warning; skip this dataset
                # entirely so the rest of the catalog stays usable.
                continue
            tables.append(
                _build_table(
                    dataset=model,
                    datasets_by_name=by_name,
                    bfs_depth=bfs_depth,
                )
            )
        schemas.append(FacadeSchema(name=datasource, tables=tables))
    return FacadeCatalog(catalog_name=CATALOG_NAME, schemas=schemas)


DEFAULT_PG_SCHEMA = "public"


def build_catalog_grouped_by_schema(
    *,
    datasets_by_datasource: dict[str, list[SlayerDataset]],
    schema_by_datasource: dict[str, str] | None = None,
    datasource_priority: list[str] | None = None,
    default_schema: str = DEFAULT_PG_SCHEMA,
    bfs_depth: int = DEFAULT_BFS_DEPTH,
) -> FacadeCatalog:
    """Build a catalog spanning many datasources, grouped into Postgres schemas.

    Each datasource's tables are built independently (so relationship BFS stays scoped
    to one datasource — merging never fabricates cross-datasource relationships), then
    re-grouped into ``FacadeSchema``s named by each datasource's
    ``schema_by_datasource`` entry (default ``"public"``). When two datasources
    map to the same schema and share a dataset name, ``datasource_priority``
    decides the winner (earlier = higher priority); the loser is shadowed (a
    Postgres schema can only expose one table of a given name) and logged.
    """
    schema_by_datasource = schema_by_datasource or {}
    priority = datasource_priority or []

    def _priority_index(datasource: str) -> int:
        try:
            return priority.index(datasource)
        except ValueError:
            return len(priority)

    # Per-datasource build keeps relationship scoping correct (one FacadeSchema each).
    per_datasource = build_catalog(
        datasets_by_datasource=datasets_by_datasource, bfs_depth=bfs_depth,
    )

    # target schema -> table name -> (priority_index, datasource, table)
    grouped: dict[str, dict[str, tuple[int, str, FacadeTable]]] = {}
    for source_schema in per_datasource.schemas:
        datasource = source_schema.name
        target = schema_by_datasource.get(datasource, default_schema)
        bucket = grouped.setdefault(target, {})
        for table in source_schema.tables:
            _place_in_schema(
                bucket=bucket, incoming=(_priority_index(datasource), datasource, table),
                target=target)

    schemas = [
        FacadeSchema(name=name, tables=[entry[2] for entry in bucket.values()])
        for name, bucket in grouped.items()
    ]
    return FacadeCatalog(catalog_name=CATALOG_NAME, schemas=schemas)


def _place_in_schema(
    *,
    bucket: dict[str, tuple[int, str, FacadeTable]],
    incoming: tuple[int, str, FacadeTable],
    target: str,
) -> None:
    """Keep the higher-priority datasource's table (ties keep the existing one); log the shadowing."""
    name = incoming[2].name
    existing = bucket.get(name)
    if existing is None:
        bucket[name] = incoming
        return
    kept, shadowed = (incoming, existing) if incoming[0] < existing[0] else (existing, incoming)
    logger.warning(
        "Facade catalog: dataset %r exists in both datasource %r and "
        "%r under schema %r; keeping %r (higher priority), shadowing "
        "%r. Set distinct postgres_schema to expose both.",
        name, existing[1], incoming[1], target, kept[1], shadowed[1],
    )
    bucket[name] = kept


def _field_types_supported(*, dataset: SlayerDataset) -> bool:
    """Reject the whole dataset if any non-hidden field has a DatasetField.datatype
    outside ``SUPPORTED_DATATYPES`` (§12 gotcha #7). DataType is a StrEnum so
    the pydantic field is already constrained to the known values — but a
    future extension that adds a new variant would silently surface here as
    unmappable, which we'd rather catch with a clear warning than emit a
    half-typed catalog. Opaque ``UNKNOWN`` fields ARE supported: they map to
    VARCHAR and carry no aggregations (see DEFAULT_AGGREGATIONS_BY_TYPE)."""
    supported = set(SUPPORTED_DATATYPES)
    for col in dataset.fields:
        if col.hidden:
            continue
        if col.datatype not in supported:
            logger.warning(
                "Facade catalog: skipping dataset %r (datasource %r) — field "
                "%r has unsupported type %r (supported: %s).",
                dataset.name, dataset.data_source, col.name, col.datatype,
                sorted(t.value for t in supported),
            )
            return False
    return True


def _build_table(
    *,
    dataset: SlayerDataset,
    datasets_by_name: dict[str, SlayerDataset],
    bfs_depth: int,
) -> FacadeTable:
    table_type = _table_type(dataset=dataset)
    reachable = _walk_relationship_paths(
        root=dataset, datasets_by_name=datasets_by_name, max_depth=bfs_depth,
    )
    metrics = _metric_expansion(dataset=dataset, reachable=reachable)
    dimensions = _dimension_expansion(dataset=dataset, reachable=reachable)
    joins = _facade_relationships_for(dataset=dataset, datasets_by_name=datasets_by_name)
    return FacadeTable(
        name=dataset.name,
        table_type=table_type,
        description=dataset.description,
        metrics=metrics,
        dimensions=dimensions,
        relationships=joins,
        dataset_ref=dataset,
    )


def _facade_relationships_for(
    *, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset],
) -> list[FacadeRelationship]:
    """Every edge incident to ``dataset``, oriented from it, whose other end is a
    non-hidden dataset in the same catalog (the BFS filter)."""
    out: list[FacadeRelationship] = []
    for edge in neighbors(dataset=dataset, datasets_by_name=datasets_by_name):
        target = datasets_by_name.get(edge.to)
        if target is None or target.hidden or not edge.column_pairs:
            continue
        out.append(_facade_relationship_from(relationship=edge))
    return out


def _facade_relationship_from(*, relationship: OrientedLike) -> FacadeRelationship:
    return FacadeRelationship(
        to=relationship.to,
        column_pairs=list(relationship.column_pairs),
        join_type=relationship.join_type,
        cardinality=relationship.cardinality,
        name=relationship.name,
    )


def _table_type(*, dataset: SlayerDataset) -> str:
    if dataset.sql is not None:
        return "VIEW"
    return "TABLE"


def _walk_relationship_paths(
    *,
    root: SlayerDataset,
    datasets_by_name: dict[str, SlayerDataset],
    max_depth: int,
) -> list[tuple[list[str], SlayerDataset, bool]]:
    """Every addressable simple path from ``root`` up to ``max_depth`` hops into
    non-hidden datasets, as ``(canonical tokens, target dataset, row_preserving)``;
    ``row_preserving`` is True iff every hop keeps the root grain."""
    return [
        (list(r.tokens), datasets_by_name[r.edges[-1].to],
         all(_hop_preserves_grain(edge=e) for e in r.edges))
        for r in paths_from(root=root, datasets_by_name=datasets_by_name, depth=max_depth,
                            passable=lambda m: not m.hidden)
    ]


def _hop_preserves_grain(*, edge: OrientedRelationship) -> bool:
    """True when traversing ``edge`` cannot fan out the root's rows. An
    unknown cardinality passes only in the declared direction (the prior
    star-expansion surface); inverted-unknown fails safe."""
    if edge.cardinality in (RelationshipCardinality.MANY_TO_ONE, RelationshipCardinality.ONE_TO_ONE):
        return True
    if edge.cardinality is None:
        return edge.declaring_dataset == edge.source_dataset
    return False


def _path_dotted(path: list[str]) -> str:
    """Convert a relationship path to its dotted reference form.

    Used uniformly for both the catalog-facing metric / dimension ``name``
    (what BI tools see via ``INFORMATION_SCHEMA.*`` and project in SQL) and
    the engine-facing ``metric_expression`` / ``dimension_ref``. The
    consistency lets us pass user-written WHERE clauses straight through
    to ``SlayerQuery.filter`` without a name-rewrite step.
    """
    return ".".join(path)


# first / last are SLayer's "value at earliest / latest time" aggregations —
# the engine resolves them against the query's time dimension (or the
# dataset's default). Exposing the corresponding ``<col>_first`` / ``_last``
# pseudo-columns through pg_attribute on a dataset with no time dimension
# leaves BI tools (Metabase fingerprint, dbt schema scan) discovering
# them as queryable columns whose execution then fails with
# "Aggregation 'first' on metric '<col>' requires a time field".
_TIME_DEPENDENT_AGGREGATIONS: frozenset[str] = frozenset({"first", "last"})


def _dataset_has_resolvable_time_dimension(dataset: SlayerDataset) -> bool:
    """True if the engine can resolve ``first`` / ``last`` aggregations
    on this dataset without an explicit time-dimension argument.

    The engine auto-picks a time dimension only when the QUERY already
    includes one. For ``first(<col>)`` referenced as a flat metric (which
    is how every BI tool's flat ``SELECT <col>_first FROM <dataset>``
    fingerprint scan emits it), the engine falls back to
    ``dataset.default_time_dimension`` and errors if it's unset. So the
    facade only exposes ``<col>_first`` / ``<col>_last`` when the dataset
    declares ``default_time_dimension`` explicitly."""
    return bool(dataset.default_time_dimension)


def _eligible_aggregations(*, field: DatasetField, dataset: SlayerDataset) -> set[str]:
    """Per §5.1.3: default-by-type ∩ explicit whitelist, with the identifier clamp.

    Without a dataset time dimension, time-dependent aggregations (``first``,
    ``last``) are dropped — they would expose pseudo-columns the engine then
    refuses to execute against.
    """
    if is_identifier(field=field, fields=dataset.fields):
        base = set(PRIMARY_KEY_AGGREGATIONS)
    else:
        base = set(DEFAULT_AGGREGATIONS_BY_TYPE.get(field.datatype, frozenset()))
    if field.allowed_aggregations is not None:
        base &= set(field.allowed_aggregations)
    # Strip parametric built-ins — they need named args (§5.1.3).
    base -= _PARAMETRIC_BUILTIN_AGGS
    if not _dataset_has_resolvable_time_dimension(dataset):
        base -= _TIME_DEPENDENT_AGGREGATIONS
    return base


def _eligible_custom_aggregations(*, dataset: SlayerDataset) -> list[Aggregation]:
    """Per §5.1.4: custom aggs that use only ``{value}`` (no extra params)."""
    return [agg for agg in dataset.aggregations if not agg.params]


def _metric_expansion(
    *,
    dataset: SlayerDataset,
    reachable: list[tuple[list[str], SlayerDataset, bool]],
) -> list[FacadeMetric]:
    local = _local_metrics_for(dataset=dataset)
    out = list(local)
    # Apply BFS-derived joined metrics. Rules 1-4 are computed on ``J``
    # and then prefixed with the dotted relationship path; the prefix is the same
    # in both ``name`` (catalog-facing) and ``metric_expression`` (engine-
    # facing), matching SLayer's DSL convention end-to-end (§5.1.5).
    for path, joined_model, _row_preserving in reachable:
        prefix = _path_dotted(path)
        joined_local = _local_metrics_for(dataset=joined_model)
        for m in joined_local:
            formula = metric_text_at_path(m.metric_expression, path=path)
            out.append(
                FacadeMetric(
                    name=f"{prefix}.{m.name}",
                    description=m.description,
                    label=m.label,
                    data_type=m.data_type,
                    metric_expression=formula,
                )
            )
    return out


def _synthetic_row_count(dataset: SlayerDataset) -> FacadeMetric:
    """Rule 1: synthetic row-count metric (``count(*)``), renamed on collision."""
    name = "row_count"
    if any(c.name == "row_count" for c in dataset.fields):
        name = "_row_count"
        logger.warning(
            "Facade catalog: dataset %r has a field named 'row_count' which "
            "collides with the synthetic row-count metric; renaming the "
            "synthetic to '_row_count'.",
            dataset.name,
        )
    return FacadeMetric(
        name=name,
        description=f"Row count of {dataset.name}",
        data_type=DataType.INTEGER,
        metric_expression="count(*)",
    )


def _saved_dataset_metrics(dataset: SlayerDataset) -> list[FacadeMetric]:
    """Rule 2: every saved ``DatasetMetric`` with a name."""
    return [
        FacadeMetric(
            name=mm.name,
            description=mm.description,
            label=mm.label,
            data_type=mm.datatype,  # may be None; LIMIT-0 schema fills it in
            metric_expression=mm.name,
        )
        for mm in dataset.metrics
        if mm.name is not None
    ]


def _field_x_builtin_aggs(dataset: SlayerDataset) -> list[FacadeMetric]:
    """Rule 3: field × eligible-builtin-agg cartesian."""
    return [
        FacadeMetric(
            name=f"{col.name}_{agg}",
            description=_describe_field_agg(field=col, agg=agg),
            label=col.label,
            data_type=field_aggregate_type(dataset=dataset, field_name=col.name, aggregation=agg),
            metric_expression=functional_agg_text(source=col.name, suffix=agg),
        )
        for col in dataset.fields
        if not col.hidden
        for agg in sorted(_eligible_aggregations(field=col, dataset=dataset))
    ]


def _field_x_custom_aggs(dataset: SlayerDataset) -> list[FacadeMetric]:
    """Rule 4: field × parameterless custom aggs. Custom aggs are not
    gated by ``DEFAULT_AGGREGATIONS_BY_TYPE``, so we expose them on every
    non-hidden scalar field; a list field takes no custom aggregation."""
    custom = _eligible_custom_aggregations(dataset=dataset)
    return [
        FacadeMetric(
            name=f"{col.name}_{agg.name}",
            description=agg.description or _describe_field_agg(
                field=col, agg=agg.name,
            ),
            label=col.label,
            data_type=field_aggregate_type(dataset=dataset, field_name=col.name, aggregation=agg.name),
            metric_expression=functional_agg_text(source=col.name, suffix=agg.name),
        )
        for agg in custom
        for col in dataset.fields
        if not col.hidden and col.datatype is not DataType.ARRAY
    ]


def _local_metrics_for(*, dataset: SlayerDataset) -> list[FacadeMetric]:
    """Apply rules 1-4 to a single dataset in isolation (no relationship walk)."""
    return [
        _synthetic_row_count(dataset),
        *_saved_dataset_metrics(dataset),
        *_field_x_builtin_aggs(dataset),
        *_field_x_custom_aggs(dataset),
    ]


def _local_dimensions_for(*, dataset: SlayerDataset) -> list[FacadeDimension]:
    """Bare-field dims for a single dataset in isolation (no relationship walk)."""
    out: list[FacadeDimension] = []
    for col in dataset.fields:
        if col.hidden:
            continue
        out.append(FacadeDimension(
            name=col.name,
            description=col.description,
            label=col.label,
            data_type=col.datatype,
            is_time=col.datatype in TEMPORAL_DATATYPES,
            dimension_ref=col.name,
        ))
    return out


def build_local_view(
    dataset: SlayerDataset,
) -> tuple[list[FacadeDimension], list[FacadeMetric]]:
    """Build the bare-field dims + col×agg metrics for a single dataset
    in isolation (no relationship walk). Used by the translator's dynamic-join
    lookup materialisation so a join the catalog's BFS didn't
    pre-expand can still resolve `<target>.<col>` / `<agg>(<target>.<col>)`
    refs.
    """
    return _local_dimensions_for(dataset=dataset), _local_metrics_for(dataset=dataset)


def _describe_field_agg(*, field: DatasetField, agg: str) -> str | None:
    if field.description:
        return f"{field.description} ({agg})"
    return None


def _dimension_expansion(
    *,
    dataset: SlayerDataset,
    reachable: list[tuple[list[str], SlayerDataset, bool]],
) -> list[FacadeDimension]:
    out: list[FacadeDimension] = []
    for col in dataset.fields:
        if col.hidden:
            continue
        out.append(
            FacadeDimension(
                name=col.name,
                description=col.description,
                label=col.label,
                data_type=col.datatype,
                is_time=col.datatype in TEMPORAL_DATATYPES,
                dimension_ref=col.name,
            )
        )
    for path, joined_model, row_preserving in reachable:
        prefix = _path_dotted(path)
        for col in joined_model.fields:
            if col.hidden:
                continue
            ref = f"{prefix}.{col.name}"
            out.append(
                FacadeDimension(
                    name=ref,
                    description=col.description,
                    label=col.label,
                    data_type=col.datatype,
                    is_time=col.datatype in TEMPORAL_DATATYPES,
                    dimension_ref=ref,
                    row_preserving=row_preserving,
                )
            )
    return out
