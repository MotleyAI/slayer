"""Convert a parsed DbtProject into SLayer models and query definitions.

Orchestrates the full pipeline: entity resolution, dimension/measure conversion,
measure consolidation, metric-to-measure and metric-to-query generation.
"""

import logging
import re
from collections import defaultdict
from typing import Dict, List, Optional

import sqlalchemy as sa
from pydantic import BaseModel, Field

from slayer.core.enums import DataType, JoinType
from slayer.core.models import Column, ModelJoin, SlayerModel
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


class ConversionWarning(BaseModel):
    """A warning or info message from the conversion process."""
    model_name: Optional[str] = None
    metric_name: Optional[str] = None
    message: str


class ConversionResult(BaseModel):
    """Result of converting a DbtProject to SLayer representations."""
    models: List[SlayerModel] = Field(default_factory=list)
    queries: List[dict] = Field(default_factory=list)  # SlayerQuery dicts for metrics
    warnings: List[ConversionWarning] = Field(default_factory=list)


def _map_agg(dbt_agg: str) -> str:
    """Map a dbt aggregation name to a SLayer aggregation name."""
    mapped = _AGG_MAP.get(dbt_agg.lower())
    if mapped is None:
        logger.warning("Unknown dbt aggregation '%s', passing through as-is", dbt_agg)
        return dbt_agg.lower()
    return mapped


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
    strict_aggregations: bool,
) -> List[Column]:
    """Convert dbt measures to SLayer columns with consolidation.

    Measures with the same expr are consolidated into one SLayer column
    with multiple allowed_aggregations. Original name:agg pairs are listed
    in the description. dbt measures imply numeric data, so the resulting
    columns get DataType.NUMBER.
    """
    # Group by effective expr (expr or name if expr is None)
    groups: Dict[str, List[DbtMeasure]] = defaultdict(list)
    for m in dbt_measures:
        key = m.expr or m.name
        groups[key].append(m)

    result: List[Column] = []
    for expr_key, measures_in_group in groups.items():
        if len(measures_in_group) == 1:
            m = measures_in_group[0]
            mapped_agg = _map_agg(m.agg)
            sql = m.expr if m.expr and m.expr != m.name else None
            allowed = [mapped_agg] if strict_aggregations else None

            desc = m.description or ""
            if desc and not desc.endswith("."):
                desc += "."
            desc = f"{desc} Default aggregation: {m.agg}".strip()

            result.append(Column(
                name=m.name,
                sql=sql,
                type=DataType.NUMBER,
                description=desc,
                label=m.label,
                allowed_aggregations=allowed,
            ))
        else:
            # Consolidate: multiple dbt measures → one SLayer column
            aggs = []
            name_agg_pairs = []
            labels = []
            descriptions = []

            for m in measures_in_group:
                mapped = _map_agg(m.agg)
                if mapped not in aggs:
                    aggs.append(mapped)
                name_agg_pairs.append(f"{m.name} ({m.agg})")
                if m.label:
                    labels.append(m.label)
                if m.description:
                    descriptions.append(m.description)

            # Use the first dbt measure's name; fall back to expr_key. Keep the
            # SQL expression whenever it differs from the chosen name.
            measure_name = measures_in_group[0].name or expr_key
            sql = expr_key if expr_key != measure_name else None

            desc = f"dbt measures: {', '.join(name_agg_pairs)}"
            if descriptions:
                desc = f"{descriptions[0]}. {desc}"

            result.append(Column(
                name=measure_name,
                sql=sql,
                type=DataType.NUMBER,
                description=desc,
                label=labels[0] if labels else None,
                allowed_aggregations=aggs if strict_aggregations else None,
            ))

    return result


class DbtToSlayerConverter:
    """Convert a DbtProject into SLayer models and query definitions."""

    def __init__(
        self,
        project: DbtProject,
        data_source: str,
        strict_aggregations: bool = True,
        sa_engine: Optional[sa.Engine] = None,
        include_hidden_models: bool = False,
    ) -> None:
        self.project = project
        self.data_source = data_source
        self.strict_aggregations = strict_aggregations
        self.sa_engine = sa_engine
        self.include_hidden_models = include_hidden_models
        self.entity_registry = EntityRegistry()
        self._warnings: List[ConversionWarning] = []
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
        # 1. Build entity registry
        self.entity_registry.build(self.project.semantic_models)

        # 2. Index dbt models
        for sm in self.project.semantic_models:
            self._dbt_models_by_name[sm.name] = sm

        # 3. Convert semantic models
        models: List[SlayerModel] = []
        for sm in self.project.semantic_models:
            model = self._convert_semantic_model(sm)
            models.append(model)
            self._models_by_name[model.name] = model

        # 4. Convert metrics
        queries: List[dict] = []
        for metric in self.project.metrics:
            query = self._convert_metric(metric)
            if query is not None:
                queries.append(query)

        # 5. Mirror inner joins: if A→B is inner, ensure B→A is inner too
        self._mirror_inner_joins()

        # 6. Convert orphan regular dbt models into hidden SLayer models
        if self.include_hidden_models and self.project.regular_models:
            models.extend(self._convert_regular_models(existing_names={m.name for m in models}))

        return ConversionResult(
            models=models,
            queries=queries,
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
        """Convert orphan dbt models (not wrapped by semantic_models) to hidden SLayer models.

        Requires a live SQLAlchemy engine for SQL introspection. If no engine was
        provided, logs one warning and returns [].
        """
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
                # A semantic_model with the same name already produced a visible
                # SLayer model; don't shadow it with a hidden import.
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

        # Overlay column-level descriptions from dbt manifest onto columns.
        col_descriptions = {c.name: c.description for c in rm.columns if c.description}
        if col_descriptions:
            for c in model.columns:
                desc = col_descriptions.get(c.name)
                if desc and not c.description:
                    c.description = desc

        return model

    def _convert_semantic_model(self, sm: DbtSemanticModel) -> SlayerModel:
        """Convert a single dbt semantic model to a SlayerModel.

        If the referenced dbt model is a regular model with a ``.sql`` body
        on disk (i.e. a query, not a physical source table), inline the
        resolved SQL into ``SlayerModel.sql`` so SLayer can query it directly
        without requiring ``dbt run`` to have materialised it. Otherwise the
        ref name is used as ``sql_table``.
        """
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

        # Default time dimension
        default_time_dim = None
        if sm.defaults and sm.defaults.agg_time_dimension:
            default_time_dim = sm.defaults.agg_time_dimension

        # Convert dimensions to columns
        cols: List[Column] = [_convert_dimension(d) for d in sm.dimensions]

        # Add primary key column for primary/unique entities
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

        # Also handle primary_entity shorthand
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

        # Append converted measures (consolidated). Avoid name collisions with
        # already-added entity/dimension columns by skipping duplicates.
        existing_names = {c.name for c in cols}
        for m_col in _convert_measures(
            dbt_measures=sm.measures,
            strict_aggregations=self.strict_aggregations,
        ):
            if m_col.name in existing_names:
                continue
            cols.append(m_col)
            existing_names.add(m_col.name)

        # Resolve joins from foreign entities
        joins = self.entity_registry.resolve_joins_for_model(sm)

        return SlayerModel(
            name=sm.name,
            sql_table=sql_table,
            sql=sql_source,
            data_source=self.data_source,
            description=sm.description,
            default_time_dimension=default_time_dim,
            columns=cols,
            joins=joins,
        )

    def _convert_metric(self, metric: DbtMetric) -> Optional[dict]:
        """Convert a dbt metric. Returns a query dict, or None if handled as a measure."""
        metric_type = metric.type.lower()

        if metric_type == "simple":
            return self._convert_simple_metric(metric)
        elif metric_type == "derived":
            return self._convert_derived_metric(metric)
        elif metric_type == "ratio":
            return self._convert_ratio_metric(metric)
        elif metric_type == "cumulative":
            return self._convert_cumulative_metric(metric)
        elif metric_type == "conversion":
            self._warnings.append(ConversionWarning(
                metric_name=metric.name,
                message="Conversion metrics are not supported in SLayer. Skipped.",
            ))
            return None
        else:
            self._warnings.append(ConversionWarning(
                metric_name=metric.name,
                message=f"Unknown metric type '{metric.type}'. Skipped.",
            ))
            return None

    def _convert_simple_metric(self, metric: DbtMetric) -> Optional[dict]:
        """Convert a simple metric to a filtered measure on the base model."""
        if not metric.type_params or not metric.type_params.measure:
            self._warnings.append(ConversionWarning(
                metric_name=metric.name,
                message="Simple metric has no measure reference. Skipped.",
            ))
            return None

        measure_name = metric.type_params.measure

        if not metric.filter:
            # No filter — the measure is already queryable. Nothing to add.
            return None

        # Find which semantic model owns this measure
        source_sm = self._find_measure_model(measure_name)
        if source_sm is None:
            self._warnings.append(ConversionWarning(
                metric_name=metric.name,
                message=f"Cannot find measure '{measure_name}' in any semantic model. Skipped.",
            ))
            return None

        # Find the dbt measure to get the expr and agg
        dbt_measure = None
        for m in source_sm.measures:
            if m.name == measure_name:
                dbt_measure = m
                break

        if dbt_measure is None:
            return None

        # Build entity names dict for filter resolution
        model_entities = {e.name: e.type for e in source_sm.entities}

        # Convert the filter
        sm_by_name = {sm.name: sm for sm in self.project.semantic_models}
        slayer_filter = convert_dbt_filter(
            filter_str=metric.filter,
            source_model_name=source_sm.name,
            entity_registry=self.entity_registry,
            model_entity_names=model_entities,
            all_semantic_models=sm_by_name,
        )

        # Create a filtered measure and add to the SLayer model
        mapped_agg = _map_agg(dbt_measure.agg)
        slayer_model = self._models_by_name.get(source_sm.name)
        if slayer_model is None:
            return None

        sql = dbt_measure.expr if dbt_measure.expr and dbt_measure.expr != dbt_measure.name else None

        filtered_column = Column(
            name=metric.name,
            sql=sql or dbt_measure.name,
            type=DataType.NUMBER,
            description=metric.description or f"Filtered metric: {metric.name}",
            label=metric.label,
            allowed_aggregations=[mapped_agg] if self.strict_aggregations else None,
            filter=slayer_filter,
        )
        slayer_model.columns.append(filtered_column)
        return None  # Handled as a column, no query needed

    def _convert_derived_metric(self, metric: DbtMetric) -> Optional[dict]:
        """Convert a derived metric to a SlayerQuery dict."""
        if not metric.type_params:
            return None

        expr = metric.type_params.expr
        if not expr:
            self._warnings.append(ConversionWarning(
                metric_name=metric.name,
                message="Derived metric has no expr. Skipped.",
            ))
            return None

        # Build the formula: replace metric names with SLayer colon syntax.
        # Use word-boundary regex so a ref like "total" doesn't mutate
        # `subtotal` or `total_orders` elsewhere in the expression.
        formula = expr
        if metric.type_params.metrics:
            for m_input in metric.type_params.metrics:
                ref_name = m_input.alias or m_input.name
                resolved = self._resolve_metric_to_formula(m_input.name)
                if resolved:
                    formula = re.sub(
                        rf"\b{re.escape(ref_name)}\b",
                        # Escape backreference syntax in the replacement so
                        # any literal \1 / \g<...> in the resolved colon
                        # expression is treated as text, not a backref.
                        resolved.replace("\\", r"\\"),
                        formula,
                    )

        # Find a source model for this query
        source_model = self._find_metric_source_model(metric)

        query = {
            "name": metric.name,
            "description": metric.description or f"Derived metric: {metric.name}",
            "measures": [{"formula": formula, "name": metric.name}],
        }
        if source_model:
            query["source_model"] = source_model

        return query

    def _convert_ratio_metric(self, metric: DbtMetric) -> Optional[dict]:
        """Convert a ratio metric to a SlayerQuery dict."""
        if not metric.type_params:
            return None

        num = metric.type_params.numerator
        den = metric.type_params.denominator
        if not num or not den:
            self._warnings.append(ConversionWarning(
                metric_name=metric.name,
                message="Ratio metric missing numerator or denominator. Skipped.",
            ))
            return None

        num_formula = self._resolve_metric_to_formula(num.name) or num.name
        den_formula = self._resolve_metric_to_formula(den.name) or den.name

        source_model = self._find_metric_source_model(metric)

        query = {
            "name": metric.name,
            "description": metric.description or f"Ratio metric: {metric.name}",
            "measures": [{"formula": f"{num_formula} / {den_formula}", "name": metric.name}],
        }
        if source_model:
            query["source_model"] = source_model

        return query

    def _convert_cumulative_metric(self, metric: DbtMetric) -> Optional[dict]:
        """Convert a cumulative metric to a SlayerQuery dict."""
        if not metric.type_params or not metric.type_params.measure:
            self._warnings.append(ConversionWarning(
                metric_name=metric.name,
                message="Cumulative metric has no measure reference. Skipped.",
            ))
            return None

        measure_ref = self._resolve_measure_to_formula(metric.type_params.measure)
        if not measure_ref:
            return None

        source_model = self._find_metric_source_model(metric)

        query = {
            "name": metric.name,
            "description": metric.description or f"Cumulative metric: {metric.name}",
            "measures": [{"formula": f"cumsum({measure_ref})", "name": metric.name}],
        }
        if source_model:
            query["source_model"] = source_model

        return query

    def _find_measure_model(self, measure_name: str) -> Optional[DbtSemanticModel]:
        """Find which dbt semantic model contains a given measure."""
        for sm in self.project.semantic_models:
            for m in sm.measures:
                if m.name == measure_name:
                    return sm
        return None

    def _find_metric_source_model(self, metric: DbtMetric) -> Optional[str]:
        """Determine the source model for a metric query."""
        if metric.type_params and metric.type_params.measure:
            sm = self._find_measure_model(metric.type_params.measure)
            if sm:
                return sm.name
        # For derived metrics, try to find source through input metrics
        if metric.type_params and metric.type_params.metrics:
            for m_input in metric.type_params.metrics:
                source = self._resolve_metric_source(m_input.name)
                if source:
                    return source
        return None

    def _resolve_metric_source(self, metric_name: str) -> Optional[str]:
        """Recursively resolve a metric name to its source model."""
        for m in self.project.metrics:
            if m.name == metric_name:
                if m.type_params and m.type_params.measure:
                    sm = self._find_measure_model(m.type_params.measure)
                    return sm.name if sm else None
        return None

    def _resolve_metric_to_formula(self, metric_name: str) -> Optional[str]:
        """Resolve a metric name to a SLayer field formula (measure:agg)."""
        for m in self.project.metrics:
            if m.name == metric_name:
                return self._resolve_measure_to_formula_from_metric(m)
        # Might be a direct measure name
        return self._resolve_measure_to_formula(metric_name)

    def _resolve_measure_to_formula_from_metric(self, metric: DbtMetric) -> Optional[str]:
        """Resolve a metric to its underlying measure:agg formula."""
        if metric.type_params and metric.type_params.measure:
            return self._resolve_measure_to_formula(metric.type_params.measure)
        return f"{metric.name}:sum"  # Fallback

    def _resolve_measure_to_formula(self, measure_name: str) -> Optional[str]:
        """Resolve a dbt measure name to SLayer colon syntax (measure:agg)."""
        for sm in self.project.semantic_models:
            for m in sm.measures:
                if m.name == measure_name:
                    mapped_agg = _map_agg(m.agg)
                    # After consolidation, the SLayer column might have a
                    # different name (expr-based). Check if it was consolidated.
                    slayer_model = self._models_by_name.get(sm.name)
                    if slayer_model:
                        for slayer_c in slayer_model.columns:
                            if slayer_c.name == measure_name:
                                return f"{measure_name}:{mapped_agg}"
                            # Check if consolidated under expr name
                            if slayer_c.sql == (m.expr or m.name) and slayer_c.name != measure_name:
                                return f"{slayer_c.name}:{mapped_agg}"
                    return f"{measure_name}:{mapped_agg}"
        return None
