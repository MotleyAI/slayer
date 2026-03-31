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

from dataclasses import dataclass, field
from typing import List, Optional

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.formula import ParsedFilter
from slayer.core.query import OrderItem


@dataclass
class EnrichedDimension:
    """A dimension with its SQL expression fully resolved."""

    name: str
    sql: Optional[str]
    type: DataType
    alias: str  # Result column name (e.g., "orders.status")
    model_name: str
    label: Optional[str] = None  # Human-readable label


@dataclass
class EnrichedMeasure:
    """A measure with its SQL expression and aggregation type fully resolved."""

    name: str
    sql: Optional[str]  # None for COUNT(*)
    type: DataType  # Aggregation type (count, sum, avg, etc.)
    alias: str  # Result column name (e.g., "orders.count")
    model_name: str
    label: Optional[str] = None  # Human-readable label


@dataclass
class EnrichedTimeDimension:
    """A time dimension with resolved SQL and granularity."""

    name: str
    sql: Optional[str]
    granularity: TimeGranularity
    date_range: Optional[List[str]]
    alias: str
    model_name: str
    label: Optional[str] = None


@dataclass
class EnrichedExpression:
    """An arithmetic expression computed from measure aliases.

    The sql references measure aliases from the base query (e.g., "revenue / count").
    Generated as an outer SELECT over a CTE containing the base query.
    """

    name: str
    sql: str  # Expression referencing measure aliases
    alias: str  # Result column name
    label: Optional[str] = None


@dataclass
class EnrichedTransform:
    """A window-function or subquery transform applied to a measure.

    Most transforms generate window functions in an outer SELECT.
    time_shift generates a self-join CTE.
    """

    name: str
    transform: str  # cumsum, lag, lead, change, change_pct, rank, time_shift, last
    measure_alias: str  # Alias of the measure in the base CTE to transform
    alias: str  # Result column name
    offset: int  # For time_shift: number of rows or calendar units
    granularity: Optional[str]  # For time_shift: year, month, quarter, etc.
    time_alias: Optional[str]  # Alias of the time dimension column for ORDER BY
    label: Optional[str] = None


@dataclass
class EnrichedQuery:
    """Fully resolved query — everything needed to generate SQL.

    Constructed by SlayerQueryEngine._enrich() from a SlayerQuery + SlayerModel.
    Passed to SQLGenerator.generate() for SQL generation.
    """

    # Source table or subquery
    model_name: str
    sql_table: Optional[str] = None
    sql: Optional[str] = None

    # Resolved columns
    dimensions: List[EnrichedDimension] = field(default_factory=list)
    measures: List[EnrichedMeasure] = field(default_factory=list)
    time_dimensions: List[EnrichedTimeDimension] = field(default_factory=list)

    # Expressions and transforms (computed in outer query over base CTE)
    expressions: List[EnrichedExpression] = field(default_factory=list)
    transforms: List[EnrichedTransform] = field(default_factory=list)

    # Time column for `type: last` aggregation (ORDER BY for ROW_NUMBER)
    last_agg_time_column: Optional[str] = None

    # Filters, ordering, pagination
    filters: List[ParsedFilter] = field(default_factory=list)
    order: Optional[List[OrderItem]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
