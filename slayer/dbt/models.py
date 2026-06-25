"""Pydantic v2 models for parsed dbt semantic layer objects.

Lightweight representations of dbt's semantic_models and metrics YAML.
We don't use metricflow-semantic-interfaces because it requires a Pydantic v1
compatibility shim and has heavy transitive dependencies we don't need.
"""


from pydantic import BaseModel, Field, field_validator


class DbtTimeTypeParams(BaseModel):
    time_granularity: str | None = None
    is_partition: bool | None = None


class DbtNonAdditiveDimension(BaseModel):
    name: str
    window_choice: str = "min"
    window_groupings: list[str] = Field(default_factory=list)


class DbtEntity(BaseModel):
    name: str
    type: str  # "primary", "foreign", "unique", "natural"
    expr: str | None = None  # defaults to name if omitted
    description: str | None = None


class DbtDimension(BaseModel):
    name: str
    type: str = "categorical"  # "categorical" or "time"
    expr: str | None = None
    description: str | None = None
    label: str | None = None
    type_params: DbtTimeTypeParams | None = None


class DbtMeasureAggParams(BaseModel):
    percentile: float | None = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


class DbtMeasure(BaseModel):
    name: str
    agg: str  # "sum", "count", "average", "count_distinct", "min", "max", etc.
    expr: str | None = None
    description: str | None = None
    label: str | None = None
    create_metric: bool | None = None
    agg_time_dimension: str | None = None
    agg_params: DbtMeasureAggParams | None = None
    non_additive_dimension: DbtNonAdditiveDimension | None = None

    @field_validator("expr", mode="before")
    @classmethod
    def _coerce_expr_to_str(cls, v: object) -> str | None:
        """Coerce numeric expr values to strings (e.g. dbt `expr: 1`)."""
        if v is None:
            return None
        return str(v)


class DbtDefaults(BaseModel):
    agg_time_dimension: str | None = None


class DbtSemanticModel(BaseModel):
    name: str
    model: str | None = None  # raw string, e.g. "ref('claim')"
    description: str | None = None
    defaults: DbtDefaults | None = None
    primary_entity: str | None = None
    entities: list[DbtEntity] = Field(default_factory=list)
    dimensions: list[DbtDimension] = Field(default_factory=list)
    measures: list[DbtMeasure] = Field(default_factory=list)
    label: str | None = None


class DbtMetricInputMeasure(BaseModel):
    """A measure reference within a metric's type_params."""
    name: str
    filter: str | None = None
    alias: str | None = None


class DbtMetricInput(BaseModel):
    """A metric reference within a derived metric's type_params."""
    name: str
    alias: str | None = None
    offset_window: str | None = None
    offset_to_grain: str | None = None
    filter: str | None = None


class DbtMetricTypeParams(BaseModel):
    measure: str | None = None  # simple metrics: measure name (string shorthand)
    expr: str | None = None  # derived metrics: formula expression
    metrics: list[DbtMetricInput] | None = None  # derived: input metric refs
    numerator: DbtMetricInput | None = None  # ratio
    denominator: DbtMetricInput | None = None  # ratio


class DbtMetric(BaseModel):
    name: str
    type: str  # "simple", "derived", "cumulative", "ratio", "conversion"
    description: str | None = None
    label: str | None = None
    type_params: DbtMetricTypeParams | None = None
    filter: str | None = None


class DbtColumnMeta(BaseModel):
    """Column-level metadata from dbt's manifest for a regular (non-semantic) model."""
    name: str
    description: str | None = None
    data_type: str | None = None
    tags: list[str] = Field(default_factory=list)


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
    database: str | None = None
    schema_name: str | None = None  # avoids shadowing pydantic's `schema` method
    alias: str | None = None  # materialized table name; falls back to `name`
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    columns: list[DbtColumnMeta] = Field(default_factory=list)
    raw_code: str | None = None  # SQL body from the .sql file on disk, Jinja unresolved


class DbtProject(BaseModel):
    """Aggregated result of parsing all YAML files in a dbt project."""
    semantic_models: list[DbtSemanticModel] = Field(default_factory=list)
    metrics: list[DbtMetric] = Field(default_factory=list)
    regular_models: list[DbtRegularModel] = Field(default_factory=list)
