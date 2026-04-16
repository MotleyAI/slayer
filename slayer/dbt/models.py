"""Pydantic v2 models for parsed dbt semantic layer objects.

Lightweight representations of dbt's semantic_models and metrics YAML.
We don't use metricflow-semantic-interfaces because it requires a Pydantic v1
compatibility shim and has heavy transitive dependencies we don't need.
"""

from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class DbtTimeTypeParams(BaseModel):
    time_granularity: Optional[str] = None
    is_partition: Optional[bool] = None


class DbtNonAdditiveDimension(BaseModel):
    name: str
    window_choice: str = "min"
    window_groupings: List[str] = Field(default_factory=list)


class DbtEntity(BaseModel):
    name: str
    type: str  # "primary", "foreign", "unique", "natural"
    expr: Optional[str] = None  # defaults to name if omitted
    description: Optional[str] = None


class DbtDimension(BaseModel):
    name: str
    type: str = "categorical"  # "categorical" or "time"
    expr: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    type_params: Optional[DbtTimeTypeParams] = None


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


class DbtMetricInputMeasure(BaseModel):
    """A measure reference within a metric's type_params."""
    name: str
    filter: Optional[str] = None
    alias: Optional[str] = None


class DbtMetricInput(BaseModel):
    """A metric reference within a derived metric's type_params."""
    name: str
    alias: Optional[str] = None
    offset_window: Optional[str] = None
    offset_to_grain: Optional[str] = None
    filter: Optional[str] = None


class DbtMetricTypeParams(BaseModel):
    measure: Optional[str] = None  # simple metrics: measure name (string shorthand)
    expr: Optional[str] = None  # derived metrics: formula expression
    metrics: Optional[List[DbtMetricInput]] = None  # derived: input metric refs
    numerator: Optional[DbtMetricInput] = None  # ratio
    denominator: Optional[DbtMetricInput] = None  # ratio


class DbtMetric(BaseModel):
    name: str
    type: str  # "simple", "derived", "cumulative", "ratio", "conversion"
    description: Optional[str] = None
    label: Optional[str] = None
    type_params: Optional[DbtMetricTypeParams] = None
    filter: Optional[str] = None


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
    """Aggregated result of parsing all YAML files in a dbt project."""
    semantic_models: List[DbtSemanticModel] = Field(default_factory=list)
    metrics: List[DbtMetric] = Field(default_factory=list)
    regular_models: List[DbtRegularModel] = Field(default_factory=list)
