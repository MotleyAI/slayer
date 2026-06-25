"""Pydantic v2 models for parsed dbt semantic layer objects.

Lightweight representations of dbt's semantic_models and metrics YAML.
We don't use metricflow-semantic-interfaces because it requires a Pydantic v1
compatibility shim and has heavy transitive dependencies we don't need.

DEV-1595: parser completeness — every semantically-relevant
dbt-semantic-interfaces (DSI) field is parsed (so the converter can either
represent it or route it to a clean-fail report), never silently dropped.
Pydantic ``extra="ignore"`` is kept everywhere (DSI is forward-compatible and
adds fields over time; ``forbid`` would break on a newer manifest).
"""

import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ───────────────────────── shared helpers ─────────────────────────


# Plural → singular granularity normalization for offset/window strings
# (DSI accepts ``"2 weeks"``; SLayer's transforms want ``week``).
_PLURAL_GRANULARITY_RE = re.compile(r"s$", re.IGNORECASE)


def _clause_to_str(clause: Any) -> Optional[str]:
    """Extract one where-clause as a string from a bare string or a DSI
    ``{"where_sql_template": "..."}`` dict; ``None`` when empty."""
    if isinstance(clause, dict):
        tmpl = clause.get("where_sql_template")
        return str(tmpl) if tmpl else None
    return str(clause) if clause else None


def _normalize_filter(value: Any) -> Optional[str]:
    """Normalize a DSI ``WhereFilterIntersection`` to a single filter string.

    DSI filters are a *string*, a *list of strings*, or the structured
    ``{"where_filters": [{"where_sql_template": "..."}]}`` dict. SLayer carries
    a single ``Optional[str]`` per filter, so multiple where-clauses are
    AND-joined (each parenthesised to preserve precedence). The raw Jinja is
    preserved verbatim inside each clause — ``convert_dbt_filter`` resolves it
    downstream.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    # Reduce the intersection (dict / list / tuple) to its clause iterable.
    if isinstance(value, dict):
        clauses: Any = value.get("where_filters") or []
    elif isinstance(value, (list, tuple)):
        clauses = value
    else:
        return str(value)

    parts = [c for c in (_clause_to_str(x) for x in clauses) if c]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return " AND ".join(f"({p})" for p in parts)


class DbtConfig(BaseModel):
    """DSI ``config`` block. Only ``meta`` is semantically carried by SLayer."""
    meta: Optional[Dict[str, Any]] = None


class DbtTimeTypeParams(BaseModel):
    time_granularity: Optional[str] = None
    is_partition: Optional[bool] = None


class DbtNonAdditiveDimension(BaseModel):
    name: str
    window_choice: str = "min"
    window_groupings: List[str] = Field(default_factory=list)


class DbtValidityParams(BaseModel):
    """SCD validity-window params on a dimension (recognized, not represented)."""
    is_start: Optional[bool] = None
    is_end: Optional[bool] = None


class DbtEntity(BaseModel):
    name: str
    type: str  # "primary", "foreign", "unique", "natural"
    expr: Optional[str] = None  # defaults to name if omitted
    description: Optional[str] = None
    label: Optional[str] = None
    role: Optional[str] = None  # recognized for report/meta; not represented
    config: Optional[DbtConfig] = None


class DbtDimension(BaseModel):
    name: str
    type: str = "categorical"  # "categorical" or "time"
    expr: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    type_params: Optional[DbtTimeTypeParams] = None
    is_partition: Optional[bool] = None  # recognized for report/meta
    validity_params: Optional[DbtValidityParams] = None  # recognized for report/meta
    config: Optional[DbtConfig] = None


class DbtMeasureAggParams(BaseModel):
    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


class DbtMeasure(BaseModel):
    name: str
    agg: str  # "sum", "count", "average", "count_distinct", "min", "max", etc.
    expr: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    create_metric: Optional[bool] = None
    agg_time_dimension: Optional[str] = None
    agg_params: Optional[DbtMeasureAggParams] = None
    non_additive_dimension: Optional[DbtNonAdditiveDimension] = None
    config: Optional[DbtConfig] = None

    @field_validator("expr", mode="before")
    @classmethod
    def _coerce_expr_to_str(cls, v: object) -> Optional[str]:
        """Coerce numeric expr values to strings (e.g. dbt `expr: 1`)."""
        if v is None:
            return None
        return str(v)


class DbtDefaults(BaseModel):
    agg_time_dimension: Optional[str] = None


class DbtSemanticModel(BaseModel):
    name: str
    model: Optional[str] = None  # raw string, e.g. "ref('claim')"
    description: Optional[str] = None
    defaults: Optional[DbtDefaults] = None
    primary_entity: Optional[str] = None
    entities: List[DbtEntity] = Field(default_factory=list)
    dimensions: List[DbtDimension] = Field(default_factory=list)
    measures: List[DbtMeasure] = Field(default_factory=list)
    label: Optional[str] = None
    config: Optional[DbtConfig] = None


class DbtMetricTimeWindow(BaseModel):
    """A DSI metric time window: ``{count, granularity}`` or ``"7 days"``."""
    count: int
    granularity: str

    @field_validator("granularity", mode="before")
    @classmethod
    def _normalize_granularity(cls, v: Any) -> Any:
        """Singularize plural granularities (``weeks`` → ``week``)."""
        if isinstance(v, str):
            return _PLURAL_GRANULARITY_RE.sub("", v.strip()).lower() or v
        return v

    @classmethod
    def parse(cls, value: Any) -> Optional["DbtMetricTimeWindow"]:
        """Coerce a ``"<count> <granularity>"`` string / dict into the model."""
        if value is None:
            return None
        if isinstance(value, DbtMetricTimeWindow):
            return value
        if isinstance(value, dict):
            return cls.model_validate(value)
        if isinstance(value, str):
            parts = value.strip().split()
            if len(parts) == 2 and parts[0].lstrip("-").isdigit():
                return cls(count=int(parts[0]), granularity=parts[1])
            # Single token (e.g. "month") → count 1.
            if len(parts) == 1 and parts[0]:
                return cls(count=1, granularity=parts[0])
        return None


class DbtMetricInputMeasure(BaseModel):
    """A measure reference within a metric's type_params."""
    name: str
    filter: Optional[str] = None
    alias: Optional[str] = None
    join_to_timespine: bool = False
    fill_nulls_with: Optional[int] = None

    @field_validator("filter", mode="before")
    @classmethod
    def _normalize_filter(cls, v: Any) -> Optional[str]:
        return _normalize_filter(v)


class DbtMetricInput(BaseModel):
    """A metric reference within a derived metric's type_params."""
    name: str
    alias: Optional[str] = None
    offset_window: Optional[str] = None
    offset_to_grain: Optional[str] = None
    filter: Optional[str] = None

    @field_validator("filter", mode="before")
    @classmethod
    def _normalize_filter(cls, v: Any) -> Optional[str]:
        return _normalize_filter(v)

    @field_validator("offset_window", mode="before")
    @classmethod
    def _coerce_offset_window(cls, v: Any) -> Any:
        """Accept the DSI object form ``{count, granularity}`` as well as the
        string form (``"1 month"``); store canonically as a string so
        ``DbtMetricTimeWindow.parse`` (plural normalization, custom-grain
        clean-fail) handles it downstream in the converter."""
        if isinstance(v, dict):
            count = v.get("count")
            gran = v.get("granularity")
            if count is not None and gran:
                return f"{count} {gran}"
        return v


class DbtCumulativeTypeParams(BaseModel):
    """DSI ``cumulative_type_params`` (window / grain_to_date / period_agg)."""
    measure: Optional[str] = None
    metric: Optional[str] = None
    window: Optional[DbtMetricTimeWindow] = None
    grain_to_date: Optional[str] = None
    period_agg: Optional[str] = None  # default "first" in DSI

    @field_validator("window", mode="before")
    @classmethod
    def _coerce_window(cls, v: Any) -> Any:
        return DbtMetricTimeWindow.parse(v)


class DbtConversionTypeParams(BaseModel):
    """DSI ``conversion_type_params`` — parsed so conversion metrics fail
    cleanly (funnel SQL is unsupported), never crash."""
    base_measure: Optional[DbtMetricInputMeasure] = None
    conversion_measure: Optional[DbtMetricInputMeasure] = None
    base_metric: Optional[str] = None
    conversion_metric: Optional[str] = None
    entity: Optional[str] = None
    calculation: Optional[str] = None
    window: Optional[DbtMetricTimeWindow] = None
    constant_properties: Optional[List[Dict[str, Any]]] = None

    @field_validator("window", mode="before")
    @classmethod
    def _coerce_window(cls, v: Any) -> Any:
        return DbtMetricTimeWindow.parse(v)


class DbtMetricAggregationParams(BaseModel):
    """DSI ``metric_aggregation_params`` — a measure-less simple metric that
    aggregates a semantic-model expression directly. Unsupported shape in
    SLayer (parsed for clean-fail routing)."""
    semantic_model: Optional[str] = None
    agg: Optional[str] = None
    expr: Optional[str] = None
    agg_params: Optional[DbtMeasureAggParams] = None
    agg_time_dimension: Optional[str] = None


class DbtMetricTypeParams(BaseModel):
    measure: Optional[DbtMetricInputMeasure] = None  # simple: measure ref (str shorthand or obj)
    expr: Optional[str] = None  # derived metrics: formula expression
    metrics: Optional[List[DbtMetricInput]] = None  # derived: input metric refs
    numerator: Optional[DbtMetricInput] = None  # ratio
    denominator: Optional[DbtMetricInput] = None  # ratio
    # Cumulative — both the flat legacy fields and the nested struct.
    window: Optional[DbtMetricTimeWindow] = None
    grain_to_date: Optional[str] = None
    cumulative_type_params: Optional[DbtCumulativeTypeParams] = None
    # Conversion / measure-less / metadata.
    conversion_type_params: Optional[DbtConversionTypeParams] = None
    metric_aggregation_params: Optional[DbtMetricAggregationParams] = None
    time_granularity: Optional[str] = None
    is_private: Optional[bool] = None

    @field_validator("measure", mode="before")
    @classmethod
    def _coerce_measure(cls, v: Any) -> Any:
        """Accept the string shorthand (``measure: revenue``) or the full
        ``MetricInputMeasure`` object."""
        if isinstance(v, str):
            return {"name": v}
        return v

    @field_validator("window", mode="before")
    @classmethod
    def _coerce_window(cls, v: Any) -> Any:
        return DbtMetricTimeWindow.parse(v)

    @property
    def measure_name(self) -> Optional[str]:
        return self.measure.name if self.measure else None


class DbtMetric(BaseModel):
    name: str
    type: str  # "simple", "derived", "cumulative", "ratio", "conversion"
    description: Optional[str] = None
    label: Optional[str] = None
    type_params: Optional[DbtMetricTypeParams] = None
    filter: Optional[str] = None
    time_granularity: Optional[str] = None
    config: Optional[DbtConfig] = None

    @field_validator("filter", mode="before")
    @classmethod
    def _normalize_filter(cls, v: Any) -> Optional[str]:
        return _normalize_filter(v)


class DbtColumnMeta(BaseModel):
    """Column-level metadata from dbt's manifest for a regular (non-semantic) model."""
    name: str
    description: Optional[str] = None
    data_type: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class DbtRegularModel(BaseModel):
    """A regular dbt model — a ``.sql`` file in the dbt project.

    Populated from two sources, which may be used together:

    * ``manifest.json`` (via ``slayer.dbt.manifest``) — provides
      ``database``/``schema_name``/``alias``/``description``/``tags``/``columns``
      for orphan models (those not wrapped by a ``semantic_model``) so they can
      be introspected and surfaced as hidden SLayer models.
    * The project directory itself (via ``slayer.dbt.parser``) — provides
      ``raw_code``, the SQL body of the ``.sql`` file. That body may contain
      unresolved dbt Jinja (e.g. ``{{ ref('X') }}``, ``{{ source('s','t') }}``,
      ``{{ config(...) }}``); it is resolved by
      ``slayer.dbt.sql_resolver.resolve_refs`` when the converter inlines a
      regular model's SQL into a semantic-model-derived ``SlayerModel``.
    """
    name: str
    database: Optional[str] = None
    schema_name: Optional[str] = None  # avoids shadowing pydantic's `schema` method
    alias: Optional[str] = None  # materialized table name; falls back to `name`
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    columns: List[DbtColumnMeta] = Field(default_factory=list)
    raw_code: Optional[str] = None  # SQL body from the .sql file on disk, Jinja unresolved


class DbtProject(BaseModel):
    """Aggregated result of parsing all YAML files in a dbt project.

    ``saved_queries`` / ``exports`` (and any other top-level DSI artefacts) are
    out of scope for the importer but accepted via ``extra="ignore"`` so a full
    manifest doesn't crash the parser.
    """
    semantic_models: List[DbtSemanticModel] = Field(default_factory=list)
    metrics: List[DbtMetric] = Field(default_factory=list)
    regular_models: List[DbtRegularModel] = Field(default_factory=list)
