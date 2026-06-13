"""FacadeCatalog build (DEV-1390 §5; shared by Flight + Postgres facades).

Snapshots the live ``StorageBackend`` view into a wire-facade-shaped
catalog: one logical catalog (``"slayer"``), one schema per datasource,
one table per non-hidden ``SlayerModel``, and on each table a fan-out
of metrics + dimensions derived from the model's columns, saved
measures, custom aggregations, and reachable join paths.

No caching in Phase 1 — every handler call rebuilds the catalog
fresh (spec §7.2). The cost on small-to-mid storages is sub-
millisecond; if profiling makes the case, a follow-up adds a
``StorageBackend.serial()`` accessor + cache invalidation.
"""

from __future__ import annotations

import logging
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from pydantic import BaseModel

from slayer.core.enums import (
    DEFAULT_AGGREGATIONS_BY_TYPE,
    PRIMARY_KEY_AGGREGATIONS,
    DataType,
)
from slayer.core.models import (
    Aggregation,
    Column,
    SlayerModel,
)
from slayer.facade.datatypes import SUPPORTED_DATATYPES

logger = logging.getLogger(__name__)

# Aggregations that need named parameters beyond ``{value}`` — we cannot
# bake a defensible default into a flat-name catalog, so the column-agg
# expansion (§5.1 rule 3) and the custom-agg expansion (rule 4) both
# skip these for built-ins. Custom aggs with non-empty ``params`` are
# also skipped per rule 4 for the same reason.
_PARAMETRIC_BUILTIN_AGGS: FrozenSet[str] = frozenset({
    "weighted_avg", "percentile", "corr", "covar_samp", "covar_pop",
})

DEFAULT_BFS_DEPTH = 3
CATALOG_NAME = "slayer"


class FacadeMetric(BaseModel):
    name: str
    description: Optional[str] = None
    label: Optional[str] = None
    data_type: Optional[DataType] = None
    measure_formula: str


class FacadeDimension(BaseModel):
    name: str
    description: Optional[str] = None
    label: Optional[str] = None
    data_type: DataType
    is_time: bool
    dimension_ref: str


class FacadeTable(BaseModel):
    name: str
    table_type: str
    description: Optional[str] = None
    metrics: List[FacadeMetric]
    dimensions: List[FacadeDimension]


class FacadeSchema(BaseModel):
    name: str
    tables: List[FacadeTable]


class FacadeCatalog(BaseModel):
    catalog_name: str = CATALOG_NAME
    schemas: List[FacadeSchema]


def build_catalog(
    *,
    models_by_datasource: Dict[str, List[SlayerModel]],
    bfs_depth: int = DEFAULT_BFS_DEPTH,
) -> FacadeCatalog:
    """Build a ``FacadeCatalog`` snapshot.

    ``models_by_datasource`` maps each datasource name to its model list;
    the caller (typically the handlers) builds this from ``storage.
    list_models(data_source=...)`` so cross-datasource joins are naturally
    constrained (SLayer doesn't auto-mirror joins across datasources).
    """
    schemas: List[FacadeSchema] = []
    for datasource, models in models_by_datasource.items():
        by_name: Dict[str, SlayerModel] = {m.name: m for m in models}
        tables: List[FacadeTable] = []
        for model in models:
            if model.hidden:
                continue
            if not _column_types_supported(model=model):
                # _column_types_supported logs the warning; skip this model
                # entirely so the rest of the catalog stays usable.
                continue
            tables.append(
                _build_table(
                    model=model,
                    models_by_name=by_name,
                    bfs_depth=bfs_depth,
                )
            )
        schemas.append(FacadeSchema(name=datasource, tables=tables))
    return FacadeCatalog(catalog_name=CATALOG_NAME, schemas=schemas)


def _column_types_supported(*, model: SlayerModel) -> bool:
    """Reject the whole model if any non-hidden column has a Column.type
    outside the six base types (§12 gotcha #7). DataType is a StrEnum so the
    pydantic field is already constrained to the six values — but a future
    extension that adds a new variant would silently surface here as
    unmappable, which we'd rather catch with a clear warning than emit a
    half-typed catalog."""
    supported = set(SUPPORTED_DATATYPES)
    for col in model.columns:
        if col.hidden:
            continue
        if col.type not in supported:
            logger.warning(
                "Facade catalog: skipping model %r (datasource %r) — column "
                "%r has unsupported type %r (supported: %s).",
                model.name, model.data_source, col.name, col.type,
                sorted(t.value for t in supported),
            )
            return False
    return True


def _build_table(
    *,
    model: SlayerModel,
    models_by_name: Dict[str, SlayerModel],
    bfs_depth: int,
) -> FacadeTable:
    table_type = _table_type(model=model)
    reachable = _walk_join_paths(
        root=model, models_by_name=models_by_name, max_depth=bfs_depth,
    )
    metrics = _metric_expansion(model=model, reachable=reachable)
    dimensions = _dimension_expansion(model=model, reachable=reachable)
    return FacadeTable(
        name=model.name,
        table_type=table_type,
        description=model.description,
        metrics=metrics,
        dimensions=dimensions,
    )


def _table_type(*, model: SlayerModel) -> str:
    if model.sql is not None:
        return "VIEW"
    return "TABLE"


def _walk_join_paths(
    *,
    root: SlayerModel,
    models_by_name: Dict[str, SlayerModel],
    max_depth: int,
) -> List[Tuple[List[str], SlayerModel]]:
    """BFS the join graph from ``root`` up to ``max_depth`` hops.

    Returns a list of (path, target_model) tuples where ``path`` is the
    sequence of join-step names (in dotted-path form, e.g.
    ``["customers", "regions"]`` for a two-hop walk). Diamond joins
    naturally produce distinct path entries for the same target.

    Cycles are bounded by depth alone — within ``max_depth``, a
    ``A→B→A`` revisit is allowed (a legitimate query shape when the
    join columns differ); past ``max_depth`` the BFS terminates.
    """
    out: List[Tuple[List[str], SlayerModel]] = []
    if max_depth <= 0:
        return out
    queue: List[Tuple[SlayerModel, List[str]]] = [(root, [])]
    while queue:
        current, path = queue.pop(0)
        if len(path) >= max_depth:
            continue
        for join in current.joins:
            target = models_by_name.get(join.target_model)
            if target is None or target.hidden:
                continue
            new_path = [*path, join.target_model]
            out.append((new_path, target))
            queue.append((target, new_path))
    return out


def _path_dotted(path: List[str]) -> str:
    """Convert a join path to its dotted reference form.

    Used uniformly for both the catalog-facing metric / dimension ``name``
    (what BI tools see via ``INFORMATION_SCHEMA.*`` and project in SQL) and
    the engine-facing ``measure_formula`` / ``dimension_ref``. The
    consistency lets us pass user-written WHERE clauses straight through
    to ``SlayerQuery.filters`` without a name-rewrite step (DEV-1390 §6.2).
    """
    return ".".join(path)


# first / last are SLayer's "value at earliest / latest time" aggregations —
# the engine resolves them against the query's time dimension (or the
# model's default). Exposing the corresponding ``<col>_first`` / ``_last``
# pseudo-columns through pg_attribute on a model with no time dimension
# leaves BI tools (Metabase fingerprint, dbt schema scan) discovering
# them as queryable columns whose execution then fails with
# "Aggregation 'first' on measure '<col>' requires a time column".
_TIME_DEPENDENT_AGGREGATIONS: FrozenSet[str] = frozenset({"first", "last"})


def _model_has_resolvable_time_dimension(model: SlayerModel) -> bool:
    """True if the engine can resolve ``first`` / ``last`` aggregations
    on this model without an explicit time-dimension argument.

    The engine auto-picks a time dimension only when the QUERY already
    includes one. For ``<col>:first`` referenced as a flat metric (which
    is how every BI tool's flat ``SELECT <col>_first FROM <model>``
    fingerprint scan emits it), the engine falls back to
    ``model.default_time_dimension`` and errors if it's unset. So the
    facade only exposes ``<col>_first`` / ``<col>_last`` when the model
    declares ``default_time_dimension`` explicitly."""
    return bool(model.default_time_dimension)


def _eligible_aggregations(
    *, column: Column, model: Optional[SlayerModel] = None,
) -> Set[str]:
    """Per §5.1.3: default-by-type ∩ explicit whitelist, with PK clamp.

    When ``model`` is given AND it has no time dimension, time-dependent
    aggregations (``first``, ``last``) are dropped — they would expose
    pseudo-columns the engine then refuses to execute against.
    """
    if column.primary_key:
        base = set(PRIMARY_KEY_AGGREGATIONS)
    else:
        base = set(DEFAULT_AGGREGATIONS_BY_TYPE.get(column.type, frozenset()))
    if column.allowed_aggregations is not None:
        base &= set(column.allowed_aggregations)
    # Strip parametric built-ins — they need named args (§5.1.3).
    base -= _PARAMETRIC_BUILTIN_AGGS
    if model is not None and not _model_has_resolvable_time_dimension(model):
        base -= _TIME_DEPENDENT_AGGREGATIONS
    return base


def _eligible_custom_aggregations(*, model: SlayerModel) -> List[Aggregation]:
    """Per §5.1.4: custom aggs that use only ``{value}`` (no extra params)."""
    return [agg for agg in model.aggregations if not agg.params]


def _metric_expansion(
    *,
    model: SlayerModel,
    reachable: List[Tuple[List[str], SlayerModel]],
) -> List[FacadeMetric]:
    local = _local_metrics_for(model=model)
    out = list(local)
    # Apply BFS-derived joined metrics. Rules 1-4 are computed on ``J``
    # and then prefixed with the dotted join path; the prefix is the same
    # in both ``name`` (catalog-facing) and ``measure_formula`` (engine-
    # facing), matching SLayer's DSL convention end-to-end (§5.1.5).
    for path, joined_model in reachable:
        prefix = _path_dotted(path)
        joined_local = _local_metrics_for(model=joined_model)
        for m in joined_local:
            if m.measure_formula == "*:count":
                # Per §5.1.5 sub-bullet: *:count keeps the literal *:count
                # but is dotted-prefixed by the joined model name.
                # E.g. orders → customers → "customers.*:count".
                formula = f"{prefix}.*:count"
            else:
                formula = f"{prefix}.{m.measure_formula}"
            out.append(
                FacadeMetric(
                    name=f"{prefix}.{m.name}",
                    description=m.description,
                    label=m.label,
                    data_type=m.data_type,
                    measure_formula=formula,
                )
            )
    return out


def _synthetic_row_count(model: SlayerModel) -> FacadeMetric:
    """Rule 1: synthetic ``*:count`` metric, renamed on collision."""
    name = "row_count"
    if any(c.name == "row_count" for c in model.columns):
        name = "_row_count"
        logger.warning(
            "Facade catalog: model %r has a Column named 'row_count' which "
            "collides with the synthetic *:count metric; renaming the "
            "synthetic to '_row_count'.",
            model.name,
        )
    return FacadeMetric(
        name=name,
        description=f"Row count of {model.name}",
        data_type=DataType.INT,
        measure_formula="*:count",
    )


def _saved_model_measures(model: SlayerModel) -> List[FacadeMetric]:
    """Rule 2: every saved ``ModelMeasure`` with a name."""
    return [
        FacadeMetric(
            name=mm.name,
            description=mm.description,
            label=mm.label,
            data_type=mm.type,  # may be None; LIMIT-0 schema fills it in
            measure_formula=mm.name,
        )
        for mm in model.measures
        if mm.name is not None
    ]


def _column_x_builtin_aggs(model: SlayerModel) -> List[FacadeMetric]:
    """Rule 3: column × eligible-builtin-agg cartesian."""
    return [
        FacadeMetric(
            name=f"{col.name}_{agg}",
            description=_describe_column_agg(column=col, agg=agg),
            label=col.label,
            data_type=_agg_output_type(column=col, agg=agg),
            measure_formula=f"{col.name}:{agg}",
        )
        for col in model.columns
        if not col.hidden
        for agg in sorted(_eligible_aggregations(column=col, model=model))
    ]


def _column_x_custom_aggs(model: SlayerModel) -> List[FacadeMetric]:
    """Rule 4: column × parameterless custom aggs. Custom aggs are not
    gated by ``DEFAULT_AGGREGATIONS_BY_TYPE``, so we expose them on every
    non-hidden column. Custom-agg output type is opaque."""
    custom = _eligible_custom_aggregations(model=model)
    return [
        FacadeMetric(
            name=f"{col.name}_{agg.name}",
            description=agg.description or _describe_column_agg(
                column=col, agg=agg.name,
            ),
            label=col.label,
            data_type=None,
            measure_formula=f"{col.name}:{agg.name}",
        )
        for agg in custom
        for col in model.columns
        if not col.hidden
    ]


def _local_metrics_for(*, model: SlayerModel) -> List[FacadeMetric]:
    """Apply rules 1-4 to a single model in isolation (no join walk)."""
    return [
        _synthetic_row_count(model),
        *_saved_model_measures(model),
        *_column_x_builtin_aggs(model),
        *_column_x_custom_aggs(model),
    ]


def _describe_column_agg(*, column: Column, agg: str) -> Optional[str]:
    if column.description:
        return f"{column.description} ({agg})"
    return None


def _agg_output_type(*, column: Column, agg: str) -> Optional[DataType]:
    """Coarse-grained output-type inference for column × agg pairs.

    Used only to populate ``INFORMATION_SCHEMA.METRICS.data_type``; the
    wire schema is always derived from the actual ``LIMIT 0`` execution
    (§5.3), so any inference here is informational.
    """
    if agg in {"count", "count_distinct"}:
        return DataType.INT
    if agg in {"sum"}:
        # SUM(INT) → INT for SQLite/Postgres; SUM(DOUBLE) → DOUBLE.
        # Boolean SUM is also INT (cast to int per DEFAULT_AGGREGATIONS_BY_TYPE).
        if column.type == DataType.BOOLEAN:
            return DataType.INT
        return column.type
    if agg in {"min", "max", "first", "last"}:
        return column.type
    if agg in {"avg", "median", "percentile", "stddev_samp", "stddev_pop",
               "var_samp", "var_pop", "weighted_avg", "corr",
               "covar_samp", "covar_pop"}:
        return DataType.DOUBLE
    return None


def _dimension_expansion(
    *,
    model: SlayerModel,
    reachable: List[Tuple[List[str], SlayerModel]],
) -> List[FacadeDimension]:
    out: List[FacadeDimension] = []
    for col in model.columns:
        if col.hidden:
            continue
        out.append(
            FacadeDimension(
                name=col.name,
                description=col.description,
                label=col.label,
                data_type=col.type,
                is_time=col.type in {DataType.DATE, DataType.TIMESTAMP},
                dimension_ref=col.name,
            )
        )
    for path, joined_model in reachable:
        prefix = _path_dotted(path)
        for col in joined_model.columns:
            if col.hidden:
                continue
            ref = f"{prefix}.{col.name}"
            out.append(
                FacadeDimension(
                    name=ref,
                    description=col.description,
                    label=col.label,
                    data_type=col.type,
                    is_time=col.type in {DataType.DATE, DataType.TIMESTAMP},
                    dimension_ref=ref,
                )
            )
    return out
