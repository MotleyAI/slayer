"""EnrichedQuery — fully resolved query ready for SQL generation.

Architecture:
    SlayerQuery (user-facing) → EnrichedQuery (engine-internal) → SQL

SlayerQuery is what the user/agent provides — just names and references,
no SQL expressions or model details. It's intentionally minimal.

EnrichedQuery is what the query engine produces after resolving SlayerQuery
against model definitions. Every measure and dimension carries its fully
resolved SQL expression, aggregation type, and model context. The SQL generator
works exclusively with EnrichedQuery — it never needs to look up model definitions.

This separation means:
- New datasource clients only need to translate EnrichedQuery, not understand model resolution
- Validation happens at enrichment time, not during SQL generation
- The query engine controls resolution logic (placeholder expansion, join resolution)
"""

from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.format import NumberFormat
from slayer.core.formula import ParsedFilter
from slayer.core.models import Aggregation
from slayer.core.query import OrderItem


class EnrichedDimension(BaseModel):
    """A dimension with its SQL expression fully resolved."""

    name: str
    sql: Optional[str]
    type: DataType
    alias: str = Field(description="Result column name, e.g. 'orders.status'")
    model_name: str
    label: Optional[str] = Field(default=None, description="Human-readable label")
    format: Optional[NumberFormat] = Field(default=None, description="Number format from the source dimension")


class EnrichedMeasure(BaseModel):
    """A measure with its SQL expression and aggregation fully resolved."""

    name: str
    sql: Optional[str] = Field(description="SQL expression; None for *:count (COUNT(*))")
    aggregation: str = Field(description="Aggregation name: sum, avg, count, weighted_avg, etc.")
    alias: str = Field(description="Result column name, e.g. 'orders.revenue_sum'")
    model_name: str
    aggregation_def: Optional[Aggregation] = Field(default=None, description="Full aggregation definition (formula, params)")
    agg_kwargs: Dict[str, str] = Field(default_factory=dict, description="Query-time aggregation param overrides")
    label: Optional[str] = Field(default=None, description="Human-readable label")
    time_column: Optional[str] = Field(default=None, description="Explicit time col for first/last (overrides query default)")
    source_measure_name: Optional[str] = Field(default=None, description="Original measure name before canonicalization")
    filter_sql: Optional[str] = Field(default=None, description="Resolved SQL condition for filtered measures (CASE WHEN)")
    filter_columns: List[str] = Field(
        default_factory=list,
        description="Resolved (qualified) column names referenced by the filter, for join planning",
    )


class EnrichedTimeDimension(BaseModel):
    """A time dimension with resolved SQL and granularity."""

    name: str
    sql: Optional[str]
    granularity: TimeGranularity
    date_range: Optional[List[str]]
    alias: str
    model_name: str
    label: Optional[str] = None


class EnrichedExpression(BaseModel):
    """An arithmetic expression computed from measure aliases.

    The sql references measure aliases from the base query (e.g., "revenue / count").
    Generated as an outer SELECT over a CTE containing the base query.
    """

    name: str
    sql: str = Field(description="Expression referencing measure aliases")
    alias: str = Field(description="Result column name")
    label: Optional[str] = None


class EnrichedTransform(BaseModel):
    """A window-function or subquery transform applied to a measure.

    Most transforms generate window functions in an outer SELECT.
    time_shift generates a self-join CTE.
    change and change_pct are desugared at enrichment time into
    a hidden time_shift transform + an EnrichedExpression for the arithmetic.
    """

    name: str
    transform: str = Field(description="Transform name: cumsum, lag, lead, rank, time_shift, first, last")
    measure_alias: str = Field(description="Alias of the measure in the base CTE to transform")
    alias: str = Field(description="Result column name")
    offset: int = Field(description="For time_shift: number of rows or calendar units")
    granularity: Optional[str] = Field(default=None, description="For time_shift: year, month, quarter, etc.")
    time_alias: Optional[str] = Field(default=None, description="Alias of the time dimension column for ORDER BY")
    label: Optional[str] = None


class EnrichedQuery(BaseModel):
    """Fully resolved query — everything needed to generate SQL.

    Constructed by SlayerQueryEngine._enrich() from a SlayerQuery + SlayerModel.
    Passed to SQLGenerator.generate() for SQL generation.
    """

    model_name: str
    sql_table: Optional[str] = None
    sql: Optional[str] = None

    resolved_joins: List[tuple] = Field(default_factory=list, description="[(target_table_sql, target_alias, join_condition, join_type), ...]")

    dimensions: List[EnrichedDimension] = Field(default_factory=list)
    measures: List[EnrichedMeasure] = Field(default_factory=list)
    time_dimensions: List[EnrichedTimeDimension] = Field(default_factory=list)

    expressions: List[EnrichedExpression] = Field(default_factory=list)
    transforms: List[EnrichedTransform] = Field(default_factory=list)

    cross_model_measures: List["CrossModelMeasure"] = Field(default_factory=list)

    last_agg_time_column: Optional[str] = Field(default=None, description="Time column for first/last aggregation (ORDER BY for ROW_NUMBER)")

    filters: List[ParsedFilter] = Field(default_factory=list)
    order: Optional[List[OrderItem]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None

    field_name_aliases: Dict[str, str] = Field(default_factory=dict, description="Custom field name → enriched alias mapping (for ORDER BY resolution)")


class CrossModelMeasure(BaseModel):
    """A measure from a joined model, computed as a separate sub-query.

    The sub-query aggregates the measure from the target model scoped to
    the shared dimensions, then the result is LEFT JOINed to the main query.
    """

    name: str
    alias: str = Field(description="Result column name, e.g. 'orders.customers__avg_score'")
    target_model_name: str = Field(description="The joined model name")
    target_model_sql_table: Optional[str]
    target_model_sql: Optional[str]
    measure: EnrichedMeasure = Field(description="The measure to aggregate")
    join_pairs: List[List[str]] = Field(description="[[source_dim, target_dim], ...] from ModelJoin")
    shared_dimensions: List[EnrichedDimension] = Field(description="Dimensions shared between main and target")
    shared_time_dimensions: List[EnrichedTimeDimension] = Field(description="Time dims shared between main and target")
    source_model_name: str = Field(description="The main query's model name")
    source_sql_table: Optional[str] = Field(description="Main model's table")
    source_sql: Optional[str] = Field(description="Main model's SQL")
    join_type: str = Field(default="left", description="'left' or 'inner'")
    label: Optional[str] = None
    format: Optional[NumberFormat] = Field(default=None, description="Inferred format for this cross-model measure")
    rerooted_enriched: Optional["EnrichedQuery"] = Field(default=None, description="Re-rooted subquery with target as source")


# Rebuild models with forward references
EnrichedQuery.model_rebuild()
CrossModelMeasure.model_rebuild()
