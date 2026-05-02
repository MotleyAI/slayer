"""Convert a parsed DbtProject into SLayer models.

Orchestrates the full pipeline: entity resolution, dimension/measure
conversion, measure consolidation (one ``Column`` per unique expr +
one ``ModelMeasure`` per dbt measure), and folding of metric definitions
(simple-with-filter / derived / ratio / cumulative) into ``ModelMeasure``
entries on the source semantic model.

The converter never emits ``SlayerQuery`` definitions: every dbt artefact
that produces a query-shaped result is expressed as a named formula on a
model. Metrics that cannot be expressed that way (e.g. transform-name
collisions, conversion metrics) are returned in
``ConversionResult.unconverted_metrics``.
"""

import logging
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import sqlalchemy as sa
from pydantic import BaseModel, Field

from slayer.core.enums import DataType, JoinType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.dbt.entities import EntityRegistry
from slayer.dbt.filters import convert_dbt_filter
from slayer.dbt.models import (
    DbtDimension,
    DbtMeasure,
    DbtMetric,
    DbtProject,
    DbtRegularModel,
    DbtSemanticModel,
)
from slayer.dbt.sql_resolver import resolve_refs
from slayer.engine.ingestion import introspect_table_to_model

logger = logging.getLogger(__name__)

# Map dbt aggregation names to SLayer aggregation names
_AGG_MAP: Dict[str, str] = {
    "sum": "sum",
    "average": "avg",
    "avg": "avg",
    "count": "count",
    "count_distinct": "count_distinct",
    "min": "min",
    "max": "max",
    "median": "median",
    "percentile": "percentile",
    "sum_boolean": "sum",
}

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_FLOAT_FORMAT = NumberFormat(type=NumberFormatType.FLOAT)


class DbtConversionError(Exception):
    """Raised when a dbt project cannot be converted to SLayer shape.

    The message includes the offending semantic-model name and the
    colliding identifiers so the user can fix the dbt definitions.
    """


class ConversionWarning(BaseModel):
    """A warning or info message from the conversion process."""
    model_name: Optional[str] = None
    metric_name: Optional[str] = None
    message: str


class ConversionResult(BaseModel):
    """Result of converting a DbtProject to SLayer representations."""
    models: List[SlayerModel] = Field(default_factory=list)
    unconverted_metrics: List[ConversionWarning] = Field(default_factory=list)
    warnings: List[ConversionWarning] = Field(default_factory=list)


def _map_agg(dbt_agg: str) -> str:
    """Map a dbt aggregation name to a SLayer aggregation name."""
    mapped = _AGG_MAP.get(dbt_agg.lower())
    if mapped is None:
        logger.warning("Unknown dbt aggregation '%s', passing through as-is", dbt_agg)
        return dbt_agg.lower()
    return mapped


def _is_simple_identifier(s: str) -> bool:
    """A bare SQL column reference (no operators, calls, or dots)."""
    return bool(_IDENTIFIER_RE.match(s))


def _convert_dimension(dim: DbtDimension) -> Column:
    """Convert a dbt dimension to a SLayer column."""
    if dim.type == "time":
        data_type = DataType.TIMESTAMP
    else:
        data_type = DataType.STRING

    sql = dim.expr if dim.expr and dim.expr != dim.name else None

    return Column(
        name=dim.name,
        sql=sql,
        type=data_type,
        description=dim.description,
        label=dim.label,
    )


def _convert_measures(
    dbt_measures: List[DbtMeasure],
    *,
    sm_name: str,
    existing_column_names: set,
    unconverted: List[ConversionWarning],
) -> Tuple[List[Column], List[ModelMeasure]]:
    """Convert dbt measures into a (Columns, ModelMeasures) pair.

    Each unique measure expression yields a single ``Column`` whose name is
    either the bare expression (when it is a SQL identifier) or
    ``<first_dbt_measure_name>_col`` (when the expression is a SQL fragment
    like ``amount * quantity``). Each dbt measure yields one ``ModelMeasure``
    whose formula is ``<col_name>:<agg>``. label and description live on the
    ``ModelMeasure`` only — they belong to the named formula, not the raw
    column.

    Column-name collisions with already-emitted dimensions/entities or with
    any of the about-to-be-emitted ``ModelMeasure`` names are resolved by
    suffixing the Column with ``_col`` (Q-2 / Q-I in the S4 spec).

    A ``ModelMeasure`` whose name shadows a built-in transform (e.g.
    ``cumsum``) is rejected by the Pydantic validator; the dbt measure is
    routed to ``unconverted`` and skipped.
    """
    groups: Dict[str, List[DbtMeasure]] = defaultdict(list)
    for m in dbt_measures:
        key = m.expr or m.name
        groups[key].append(m)

    # All dbt-measure names, taken to be the eventual ``ModelMeasure`` names.
    measure_names = {m.name for m in dbt_measures}

    columns: List[Column] = []
    measures: List[ModelMeasure] = []
    used_column_names = set(existing_column_names)

    for expr_key, group in groups.items():
        if _is_simple_identifier(expr_key):
            base_name = expr_key
        else:
            base_name = f"{group[0].name}_col"

        col_name = base_name
        # Q-2: avoid collision with any ModelMeasure name in this model.
        # Q-I: also avoid collision with the dimensions/entities already on
        # the model, by suffixing ``_col`` until unique.
        while col_name in measure_names or col_name in used_column_names:
            col_name = f"{col_name}_col"
        used_column_names.add(col_name)

        sql = expr_key if expr_key != col_name else None
        columns.append(Column(
            name=col_name,
            sql=sql,
            type=DataType.NUMBER,
            format=_FLOAT_FORMAT,
        ))

        for m in group:
            mapped_agg = _map_agg(m.agg)
            try:
                measures.append(ModelMeasure(
                    name=m.name,
                    formula=f"{col_name}:{mapped_agg}",
                    label=m.label,
                    description=m.description,
                ))
            except ValueError as exc:
                unconverted.append(ConversionWarning(
                    model_name=sm_name,
                    metric_name=m.name,
                    message=(
                        f"dbt measure '{m.name}' could not be converted to a "
                        f"ModelMeasure: {exc}"
                    ),
                ))

    return columns, measures


class DbtToSlayerConverter:
    """Convert a DbtProject into SLayer models."""

    def __init__(
        self,
        project: DbtProject,
        data_source: str,
        sa_engine: Optional[sa.Engine] = None,
        include_hidden_models: bool = False,
    ) -> None:
        self.project = project
        self.data_source = data_source
        self.sa_engine = sa_engine
        self.include_hidden_models = include_hidden_models
        self.entity_registry = EntityRegistry()
        self._warnings: List[ConversionWarning] = []
        self._unconverted: List[ConversionWarning] = []
        # {model_name: SlayerModel} for metric resolution
        self._models_by_name: Dict[str, SlayerModel] = {}
        # {model_name: DbtSemanticModel} for looking up entities
        self._dbt_models_by_name: Dict[str, DbtSemanticModel] = {}
        # {regular_model_name: raw_code} — used to inline SQL into semantic
        # models whose underlying dbt model is a query rather than a table.
        self._regular_models_sql: Dict[str, str] = {
            rm.name: rm.raw_code
            for rm in project.regular_models
            if rm.raw_code
        }

    def convert(self) -> ConversionResult:
        """Full conversion pipeline."""
        self.entity_registry.build(self.project.semantic_models)

        for sm in self.project.semantic_models:
            self._dbt_models_by_name[sm.name] = sm

        models: List[SlayerModel] = []
        for sm in self.project.semantic_models:
            model = self._convert_semantic_model(sm)
            models.append(model)
            self._models_by_name[model.name] = model

        for metric in self.project.metrics:
            self._convert_metric(metric)

        self._mirror_inner_joins()

        if self.include_hidden_models and self.project.regular_models:
            models.extend(self._convert_regular_models(existing_names={m.name for m in models}))

        return ConversionResult(
            models=models,
            unconverted_metrics=self._unconverted,
            warnings=self._warnings,
        )

    def _mirror_inner_joins(self) -> None:
        """Ensure inner joins are symmetric: if A→B is inner, B→A should be too."""
        for model in list(self._models_by_name.values()):
            for join in model.joins:
                if join.join_type != JoinType.INNER:
                    continue
                target = self._models_by_name.get(join.target_model)
                if target is None:
                    continue
                reverse_pairs = [[tgt, src] for src, tgt in join.join_pairs]
                already_exists = any(
                    j.target_model == model.name and j.join_pairs == reverse_pairs
                    for j in target.joins
                )
                if not already_exists:
                    target.joins.append(ModelJoin(
                        target_model=model.name,
                        join_pairs=reverse_pairs,
                        join_type=JoinType.INNER,
                    ))

    def _convert_regular_models(self, existing_names: set) -> List[SlayerModel]:
        """Convert orphan dbt models (not wrapped by semantic_models) to hidden SLayer models."""
        if self.sa_engine is None:
            self._warnings.append(ConversionWarning(
                message=(
                    "include_hidden_models=True but no SQLAlchemy engine was provided; "
                    "skipping regular-model import."
                ),
            ))
            return []

        engine = self.sa_engine
        inspector = sa.inspect(engine)
        results: List[SlayerModel] = []
        for rm in self.project.regular_models:
            if rm.name in existing_names:
                continue
            converted = self._convert_regular_model(rm=rm, sa_engine=engine, inspector=inspector)
            if converted is not None:
                results.append(converted)
                existing_names.add(converted.name)
        return results

    def _convert_regular_model(
        self,
        rm: DbtRegularModel,
        sa_engine: sa.Engine,
        inspector: sa.engine.Inspector,
    ) -> Optional[SlayerModel]:
        """Introspect a regular dbt model and wrap it as a hidden SlayerModel."""
        table_name = rm.alias or rm.name
        try:
            model = introspect_table_to_model(
                sa_engine=sa_engine,
                inspector=inspector,
                table_name=table_name,
                schema=rm.schema_name,
                data_source=self.data_source,
                model_name=rm.name,
            )
        except Exception as exc:
            self._warnings.append(ConversionWarning(
                model_name=rm.name,
                message=(
                    f"Skipped hidden import of dbt model '{rm.name}' "
                    f"(table '{table_name}'): {type(exc).__name__}: {exc}"
                ),
            ))
            return None

        model.hidden = True
        if rm.description:
            model.description = rm.description

        col_descriptions = {c.name: c.description for c in rm.columns if c.description}
        if col_descriptions:
            for c in model.columns:
                desc = col_descriptions.get(c.name)
                if desc and not c.description:
                    c.description = desc

        return model

    def _convert_semantic_model(self, sm: DbtSemanticModel) -> SlayerModel:
        """Convert a single dbt semantic model to a SlayerModel.

        Hard-fails (DbtConversionError) when the same name appears as both a
        dimension and a measure on this semantic model — ambiguous, since v2
        SLayer columns and measures share a namespace per model.
        """
        # Q-G: hard-fail on dim/measure name collisions before doing any work.
        dim_names = {d.name for d in sm.dimensions}
        measure_names = {m.name for m in sm.measures}
        collisions = sorted(dim_names & measure_names)
        if collisions:
            raise DbtConversionError(
                f"Semantic model '{sm.name}': dimension and measure share name(s) "
                f"{collisions}. SLayer columns and measures occupy a single "
                f"namespace per model — rename one side in the dbt project."
            )

        ref_name = sm.model or sm.name

        sql_source: Optional[str] = None
        sql_table: Optional[str] = None
        if ref_name in self._regular_models_sql:
            resolved, warnings = resolve_refs(
                self._regular_models_sql[ref_name],
                self._regular_models_sql,
            )
            sql_source = resolved
            for message in warnings:
                self._warnings.append(ConversionWarning(
                    model_name=sm.name,
                    message=message,
                ))
        else:
            sql_table = ref_name

        default_time_dim = None
        if sm.defaults and sm.defaults.agg_time_dimension:
            default_time_dim = sm.defaults.agg_time_dimension

        cols: List[Column] = [_convert_dimension(d) for d in sm.dimensions]

        # Add primary key column for primary/unique entities.
        entity_col_names = {c.name for c in cols}
        for entity in sm.entities:
            if entity.type in ("primary", "unique"):
                col_name = entity.expr or entity.name
                if col_name not in entity_col_names:
                    cols.append(Column(
                        name=col_name,
                        type=DataType.NUMBER,
                        primary_key=True,
                        description=entity.description,
                    ))
                    entity_col_names.add(col_name)
                else:
                    for c in cols:
                        if c.name == col_name:
                            c.primary_key = True

        if sm.primary_entity:
            pe_name = sm.primary_entity
            pe_expr = pe_name
            for e in sm.entities:
                if e.name == pe_name:
                    pe_expr = e.expr or e.name
                    break
            if pe_expr not in entity_col_names:
                cols.append(Column(
                    name=pe_expr,
                    type=DataType.NUMBER,
                    primary_key=True,
                ))
                entity_col_names.add(pe_expr)

        measure_cols, measures = _convert_measures(
            dbt_measures=sm.measures,
            sm_name=sm.name,
            existing_column_names={c.name for c in cols},
            unconverted=self._unconverted,
        )
        cols.extend(measure_cols)

        joins = self.entity_registry.resolve_joins_for_model(sm)

        return SlayerModel(
            name=sm.name,
            sql_table=sql_table,
            sql=sql_source,
            data_source=self.data_source,
            description=sm.description,
            default_time_dimension=default_time_dim,
            columns=cols,
            measures=measures,
            joins=joins,
        )

    # ── Metric conversion ─────────────────────────────────────────────

    def _convert_metric(self, metric: DbtMetric) -> None:
        """Route a dbt metric to the appropriate handler.

        All handlers fold their output into a ``ModelMeasure`` on the source
        semantic model (or report ``unconverted_metrics`` on failure). No
        ``SlayerQuery`` is produced.
        """
        metric_type = metric.type.lower()

        if metric_type == "simple":
            self._convert_simple_metric(metric)
        elif metric_type == "derived":
            self._convert_derived_metric(metric)
        elif metric_type == "ratio":
            self._convert_ratio_metric(metric)
        elif metric_type == "cumulative":
            self._convert_cumulative_metric(metric)
        elif metric_type == "conversion":
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message="Conversion metrics are not supported in SLayer. Skipped.",
            ))
        else:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message=f"Unknown metric type '{metric.type}'. Skipped.",
            ))

    def _add_model_measure(
        self,
        *,
        slayer_model: SlayerModel,
        metric: DbtMetric,
        formula: str,
    ) -> None:
        """Append a ``ModelMeasure`` to ``slayer_model``.

        Routes transform-name collisions (Q-F) to ``unconverted_metrics``
        instead of raising. Skips silently with a warning if the name
        collides with an existing column or measure on the model.
        """
        existing_names = {c.name for c in slayer_model.columns}
        existing_names.update(m.name for m in slayer_model.measures if m.name is not None)
        if metric.name in existing_names:
            self._warnings.append(ConversionWarning(
                model_name=slayer_model.name,
                metric_name=metric.name,
                message=(
                    f"Metric '{metric.name}' collides with an existing column or "
                    f"measure on model '{slayer_model.name}'. Skipped."
                ),
            ))
            return
        try:
            slayer_model.measures.append(ModelMeasure(
                name=metric.name,
                formula=formula,
                label=metric.label,
                description=metric.description,
            ))
        except ValueError as exc:
            self._unconverted.append(ConversionWarning(
                model_name=slayer_model.name,
                metric_name=metric.name,
                message=(
                    f"Metric '{metric.name}' could not be converted to a "
                    f"ModelMeasure: {exc}"
                ),
            ))

    def _convert_simple_metric(self, metric: DbtMetric) -> None:
        """A simple metric is a (filtered) re-aggregation of a single measure.

        Without a filter: nothing to do — the underlying measure is already
        addressable as a ModelMeasure. With a filter: emit a Column carrying
        the CASE-WHEN ``filter`` and a ModelMeasure pointing at it.
        """
        if not metric.type_params or not metric.type_params.measure:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message="Simple metric has no measure reference. Skipped.",
            ))
            return

        measure_name = metric.type_params.measure

        if not metric.filter:
            return

        source_sm = self._find_measure_model(measure_name)
        if source_sm is None:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message=f"Cannot find measure '{measure_name}' in any semantic model. Skipped.",
            ))
            return

        dbt_measure = next((m for m in source_sm.measures if m.name == measure_name), None)
        if dbt_measure is None:
            return

        model_entities = {e.name: e.type for e in source_sm.entities}
        sm_by_name = {sm.name: sm for sm in self.project.semantic_models}
        slayer_filter = convert_dbt_filter(
            filter_str=metric.filter,
            source_model_name=source_sm.name,
            entity_registry=self.entity_registry,
            model_entity_names=model_entities,
            all_semantic_models=sm_by_name,
        )

        mapped_agg = _map_agg(dbt_measure.agg)
        slayer_model = self._models_by_name.get(source_sm.name)
        if slayer_model is None:
            return

        # Q-3: filtered simple metrics get a Column carrying the filter, with
        # NO allowed_aggregations. The metric becomes a ModelMeasure that
        # references that Column with the dbt-defined aggregation.
        existing_names = {c.name for c in slayer_model.columns}
        existing_names.update(m.name for m in slayer_model.measures if m.name is not None)
        if metric.name in existing_names:
            self._warnings.append(ConversionWarning(
                model_name=slayer_model.name,
                metric_name=metric.name,
                message=(
                    f"Filtered metric '{metric.name}' collides with an existing column "
                    f"or measure on model '{slayer_model.name}'. Skipped."
                ),
            ))
            return

        col_name = f"{metric.name}_col"
        while col_name in existing_names:
            col_name = f"{col_name}_col"

        underlying_sql = (
            dbt_measure.expr
            if dbt_measure.expr and dbt_measure.expr != dbt_measure.name
            else dbt_measure.name
        )
        slayer_model.columns.append(Column(
            name=col_name,
            sql=underlying_sql,
            type=DataType.NUMBER,
            format=_FLOAT_FORMAT,
            filter=slayer_filter,
        ))
        try:
            slayer_model.measures.append(ModelMeasure(
                name=metric.name,
                formula=f"{col_name}:{mapped_agg}",
                label=metric.label,
                description=metric.description or f"Filtered metric: {metric.name}",
            ))
        except ValueError as exc:
            # Roll back the column we just appended so the model stays consistent.
            slayer_model.columns.pop()
            self._unconverted.append(ConversionWarning(
                model_name=slayer_model.name,
                metric_name=metric.name,
                message=(
                    f"Filtered metric '{metric.name}' could not be converted: {exc}"
                ),
            ))

    def _convert_derived_metric(self, metric: DbtMetric) -> None:
        """A derived metric expresses a formula over other metrics/measures.

        The ``ModelMeasure.formula`` references inputs by **bare name** —
        either another ``ModelMeasure`` on the same model (which the formula
        parser resolves) or a column-with-aggregation when bare names cannot
        be located locally.
        """
        if not metric.type_params:
            return

        expr = metric.type_params.expr
        if not expr:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message="Derived metric has no expr. Skipped.",
            ))
            return

        formula = expr
        if metric.type_params.metrics:
            for m_input in metric.type_params.metrics:
                ref_name = m_input.alias or m_input.name
                resolved = self._resolve_metric_to_name(m_input.name)
                if resolved and resolved != ref_name:
                    formula = re.sub(
                        rf"\b{re.escape(ref_name)}\b",
                        resolved.replace("\\", r"\\"),
                        formula,
                    )

        source_model_name = self._find_metric_source_model(metric)
        if source_model_name is None:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message=f"Could not determine source model for derived metric '{metric.name}'. Skipped.",
            ))
            return

        slayer_model = self._models_by_name.get(source_model_name)
        if slayer_model is None:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message=(
                    f"Source model '{source_model_name}' for derived metric "
                    f"'{metric.name}' was not converted. Skipped."
                ),
            ))
            return

        self._add_model_measure(
            slayer_model=slayer_model,
            metric=metric,
            formula=formula,
        )

    def _convert_ratio_metric(self, metric: DbtMetric) -> None:
        """A ratio metric is numerator / denominator over two measures/metrics."""
        if not metric.type_params:
            return

        num = metric.type_params.numerator
        den = metric.type_params.denominator
        if not num or not den:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message="Ratio metric missing numerator or denominator. Skipped.",
            ))
            return

        num_formula = self._resolve_metric_to_name(num.name) or num.name
        den_formula = self._resolve_metric_to_name(den.name) or den.name

        source_model_name = self._find_metric_source_model(metric)
        if source_model_name is None:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message=f"Could not determine source model for ratio metric '{metric.name}'. Skipped.",
            ))
            return

        slayer_model = self._models_by_name.get(source_model_name)
        if slayer_model is None:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message=(
                    f"Source model '{source_model_name}' for ratio metric "
                    f"'{metric.name}' was not converted. Skipped."
                ),
            ))
            return

        self._add_model_measure(
            slayer_model=slayer_model,
            metric=metric,
            formula=f"{num_formula} / {den_formula}",
        )

    def _convert_cumulative_metric(self, metric: DbtMetric) -> None:
        """A cumulative metric is a running total of one underlying measure."""
        if not metric.type_params or not metric.type_params.measure:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message="Cumulative metric has no measure reference. Skipped.",
            ))
            return

        measure_ref = self._resolve_measure_to_name(metric.type_params.measure)
        if not measure_ref:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message=(
                    f"Cumulative metric '{metric.name}' references unknown "
                    f"measure '{metric.type_params.measure}'. Skipped."
                ),
            ))
            return

        source_model_name = self._find_metric_source_model(metric)
        if source_model_name is None:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message=f"Could not determine source model for cumulative metric '{metric.name}'. Skipped.",
            ))
            return

        slayer_model = self._models_by_name.get(source_model_name)
        if slayer_model is None:
            self._unconverted.append(ConversionWarning(
                metric_name=metric.name,
                message=(
                    f"Source model '{source_model_name}' for cumulative metric "
                    f"'{metric.name}' was not converted. Skipped."
                ),
            ))
            return

        self._add_model_measure(
            slayer_model=slayer_model,
            metric=metric,
            formula=f"cumsum({measure_ref})",
        )

    # ── Resolution helpers ────────────────────────────────────────────

    def _find_measure_model(self, measure_name: str) -> Optional[DbtSemanticModel]:
        """Find which dbt semantic model contains a given measure."""
        for sm in self.project.semantic_models:
            for m in sm.measures:
                if m.name == measure_name:
                    return sm
        return None

    def _find_metric_source_model(self, metric: DbtMetric) -> Optional[str]:
        """Determine the source model for a metric.

        Walks ``measure``, then ``metrics``, then ``numerator``/``denominator``
        to support all metric shapes that reference an underlying measure.
        """
        if metric.type_params is None:
            return None

        if metric.type_params.measure:
            sm = self._find_measure_model(metric.type_params.measure)
            if sm:
                return sm.name

        if metric.type_params.metrics:
            for m_input in metric.type_params.metrics:
                source = self._resolve_metric_source(m_input.name)
                if source:
                    return source

        # Q-E: ratio metrics carry numerator/denominator, not metrics=[…].
        for side in (metric.type_params.numerator, metric.type_params.denominator):
            if side is None:
                continue
            source = self._resolve_metric_source(side.name)
            if source:
                return source

        return None

    def _resolve_metric_source(self, metric_name: str) -> Optional[str]:
        """Recursively resolve a metric name to its source model.

        First tries the metric's own ``measure`` (simple), then descends into
        ``numerator``/``denominator`` (ratio), then ``metrics`` (derived).
        Falls back to looking up ``metric_name`` as a dbt measure directly.
        """
        for m in self.project.metrics:
            if m.name != metric_name:
                continue
            if m.type_params is None:
                return None
            if m.type_params.measure:
                sm = self._find_measure_model(m.type_params.measure)
                if sm:
                    return sm.name
            for side in (m.type_params.numerator, m.type_params.denominator):
                if side is None:
                    continue
                source = self._resolve_metric_source(side.name)
                if source:
                    return source
            if m.type_params.metrics:
                for m_input in m.type_params.metrics:
                    source = self._resolve_metric_source(m_input.name)
                    if source:
                        return source
            return None
        # ``metric_name`` may be a bare dbt measure name rather than a metric.
        sm = self._find_measure_model(metric_name)
        return sm.name if sm else None

    def _resolve_metric_to_name(self, metric_name: str) -> Optional[str]:
        """Resolve a metric name to a formula reference.

        Returns the bare ``ModelMeasure`` name when the metric was lowered
        into a ``ModelMeasure`` (simple, derived, ratio, cumulative). Falls
        back to the ``column:agg`` colon form when ``metric_name`` is a dbt
        measure rather than a metric.
        """
        for m in self.project.metrics:
            if m.name == metric_name:
                # All metric kinds are stored as ModelMeasures named after the metric.
                return metric_name
        return self._resolve_measure_to_name(metric_name)

    def _resolve_measure_to_name(self, measure_name: str) -> Optional[str]:
        """Resolve a dbt measure name to a formula reference.

        After ``_convert_measures`` has run, the dbt measure name is the
        ``ModelMeasure`` name on its semantic model. Reference it by bare
        name (Q-5) so the formula parser can resolve it relative to the
        current model.
        """
        sm = self._find_measure_model(measure_name)
        if sm is None:
            return None
        slayer_model = self._models_by_name.get(sm.name)
        if slayer_model is None:
            return None
        for m in slayer_model.measures:
            if m.name == measure_name:
                return measure_name
        # Fallback: shouldn't happen for converted measures, but if the
        # measure was routed to unconverted_metrics we have nothing to point at.
        return None
