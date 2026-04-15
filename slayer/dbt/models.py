"""Pydantic v2 models for parsed dbt semantic layer objects.

Lightweight representations of dbt's semantic_models and metrics YAML.
We don't use metricflow-semantic-interfaces because it requires a Pydantic v1
compatibility shim and has heavy transitive dependencies we don't need.
"""

from typing import List, Optional

from pydantic import BaseModel, Field


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


class DbtProject(BaseModel):
    """Aggregated result of parsing all YAML files in a dbt project."""
    semantic_models: List[DbtSemanticModel] = Field(default_factory=list)
    metrics: List[DbtMetric] = Field(default_factory=list)
