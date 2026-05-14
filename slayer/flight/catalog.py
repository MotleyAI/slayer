"""FlightCatalog build (DEV-1390 §5).

Snapshots the live ``StorageBackend`` view into a Flight-SQL-shaped
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
from slayer.flight.types import SUPPORTED_DATATYPES

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


class FlightMetric(BaseModel):
    name: str
    description: Optional[str] = None
    label: Optional[str] = None
    data_type: Optional[DataType] = None
    measure_formula: str


class FlightDimension(BaseModel):
    name: str
    description: Optional[str] = None
    label: Optional[str] = None
    data_type: DataType
    is_time: bool
    dimension_ref: str


class FlightTable(BaseModel):
    name: str
    table_type: str
    description: Optional[str] = None
    metrics: List[FlightMetric]
    dimensions: List[FlightDimension]


class FlightSchema(BaseModel):
    name: str
    tables: List[FlightTable]


class FlightCatalog(BaseModel):
    catalog_name: str = CATALOG_NAME
    schemas: List[FlightSchema]


def build_catalog(
    *,
    models_by_datasource: Dict[str, List[SlayerModel]],
    bfs_depth: int = DEFAULT_BFS_DEPTH,
) -> FlightCatalog:
    """Build a ``FlightCatalog`` snapshot.

    ``models_by_datasource`` maps each datasource name to its model list;
    the caller (typically the handlers) builds this from ``storage.
    list_models(data_source=...)`` so cross-datasource joins are naturally
    constrained (SLayer doesn't auto-mirror joins across datasources).
    """
    schemas: List[FlightSchema] = []
    for datasource, models in models_by_datasource.items():
        by_name: Dict[str, SlayerModel] = {m.name: m for m in models}
        tables: List[FlightTable] = []
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
        schemas.append(FlightSchema(name=datasource, tables=tables))
    return FlightCatalog(catalog_name=CATALOG_NAME, schemas=schemas)


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
                "Flight catalog: skipping model %r (datasource %r) — column "
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
) -> FlightTable:
    table_type = _table_type(model=model)
    reachable = _walk_join_paths(
        root=model, models_by_name=models_by_name, max_depth=bfs_depth,
    )
    metrics = _metric_expansion(model=model, reachable=reachable)
    dimensions = _dimension_expansion(model=model, reachable=reachable)
    return FlightTable(
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


def _eligible_aggregations(*, column: Column) -> Set[str]:
    """Per §5.1.3: default-by-type ∩ explicit whitelist, with PK clamp."""
    if column.primary_key:
        base = set(PRIMARY_KEY_AGGREGATIONS)
    else:
        base = set(DEFAULT_AGGREGATIONS_BY_TYPE.get(column.type, frozenset()))
    if column.allowed_aggregations is not None:
        base &= set(column.allowed_aggregations)
    # Strip parametric built-ins — they need named args (§5.1.3).
    return base - _PARAMETRIC_BUILTIN_AGGS


def _eligible_custom_aggregations(*, model: SlayerModel) -> List[Aggregation]:
    """Per §5.1.4: custom aggs that use only ``{value}`` (no extra params)."""
    return [agg for agg in model.aggregations if not agg.params]


def _metric_expansion(
    *,
    model: SlayerModel,
    reachable: List[Tuple[List[str], SlayerModel]],
) -> List[FlightMetric]:
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
                FlightMetric(
                    name=f"{prefix}.{m.name}",
                    description=m.description,
                    label=m.label,
                    data_type=m.data_type,
                    measure_formula=formula,
                )
            )
    return out


def _local_metrics_for(*, model: SlayerModel) -> List[FlightMetric]:
    """Apply rules 1-4 to a single model in isolation (no join walk)."""
    out: List[FlightMetric] = []

    # Rule 1: synthetic row_count (with collision rename to _row_count).
    row_count_name = "row_count"
    if any(c.name == "row_count" for c in model.columns):
        row_count_name = "_row_count"
        logger.warning(
            "Flight catalog: model %r has a Column named 'row_count' which "
            "collides with the synthetic *:count metric; renaming the "
            "synthetic to '_row_count'.",
            model.name,
        )
    out.append(
        FlightMetric(
            name=row_count_name,
            description=f"Row count of {model.name}",
            data_type=DataType.INT,
            measure_formula="*:count",
        )
    )

    # Rule 2: saved ModelMeasures.
    for mm in model.measures:
        if mm.name is None:
            # A nameless saved measure has no surfaceable handle — skip.
            continue
        out.append(
            FlightMetric(
                name=mm.name,
                description=mm.description,
                label=mm.label,
                data_type=mm.type,  # may be None; LIMIT-0 schema fills it in
                measure_formula=mm.name,
            )
        )

    # Rule 3: column × agg cartesian over eligible aggregations.
    for col in model.columns:
        if col.hidden:
            continue
        for agg in sorted(_eligible_aggregations(column=col)):
            out.append(
                FlightMetric(
                    name=f"{col.name}_{agg}",
                    description=_describe_column_agg(column=col, agg=agg),
                    label=col.label,
                    data_type=_agg_output_type(column=col, agg=agg),
                    measure_formula=f"{col.name}:{agg}",
                )
            )

    # Rule 4: custom aggs without ``params``.
    custom = _eligible_custom_aggregations(model=model)
    for agg in custom:
        for col in model.columns:
            if col.hidden:
                continue
            # Custom aggs aren't gated by DEFAULT_AGGREGATIONS_BY_TYPE.
            # We expose them on every non-hidden column.
            out.append(
                FlightMetric(
                    name=f"{col.name}_{agg.name}",
                    description=agg.description or _describe_column_agg(
                        column=col, agg=agg.name,
                    ),
                    label=col.label,
                    data_type=None,  # custom agg output type is opaque
                    measure_formula=f"{col.name}:{agg.name}",
                )
            )

    return out


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
) -> List[FlightDimension]:
    out: List[FlightDimension] = []
    for col in model.columns:
        if col.hidden:
            continue
        out.append(
            FlightDimension(
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
                FlightDimension(
                    name=ref,
                    description=col.description,
                    label=col.label,
                    data_type=col.type,
                    is_time=col.type in {DataType.DATE, DataType.TIMESTAMP},
                    dimension_ref=ref,
                )
            )
    return out
