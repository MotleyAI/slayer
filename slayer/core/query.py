"""Query models for SLayer.

SlayerQuery is the user-facing query object — minimal, just enough to express intent.
It is later converted into EnrichedQuery (see slayer/engine/enriched.py) which carries
fully resolved SQL expressions, model metadata, and is ready for SQL generation.
"""
from __future__ import annotations

import datetime
import re
from typing import List, Optional

from pydantic import BaseModel, field_validator

from slayer.core.enums import TimeGranularity

_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class ColumnRef(BaseModel):
    """Reference to a dimension by name.

    Supports dotted names for joined models: "status", "customers.name",
    "customers.regions.name" (multi-hop). Computed dimensions (SQL expressions)
    should be defined via ModelExtension on the query's model.
    """
    name: str
    model: Optional[str] = None
    label: Optional[str] = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        # Allow dotted names for multi-hop joined dimensions (e.g., "customers.regions.name")
        for part in v.split("."):
            if not _NAME_PATTERN.match(part):
                raise ValueError(f"Invalid name '{v}': each part must contain only letters, digits, and underscores")
        return v

    @property
    def full_name(self) -> str:
        if self.model:
            return f"{self.model}.{self.name}"
        return self.name

    @classmethod
    def from_string(cls, s: str) -> ColumnRef:
        if "." in s:
            model, name = s.split(".", 1)
            return cls(name=name, model=model)
        return cls(name=s)


class TimeDimension(BaseModel):
    dimension: ColumnRef
    granularity: TimeGranularity
    date_range: Optional[List[str]] = None
    label: Optional[str] = None


class OrderItem(BaseModel):
    column: ColumnRef
    direction: str = "asc"


class Field(BaseModel):
    """A computed field defined by a formula.

    The formula is parsed to determine the field type:
    - "count" → plain measure reference
    - "revenue / count" → arithmetic expression
    - "cumsum(revenue)" → transform function call
    - "time_shift(revenue, -1)" → previous row's value (compiles to LAG)
    - "time_shift(revenue, -1, 'year')" → year-over-year (calendar-based)
    - "last(revenue)" → most recent value
    - "change(revenue / count)" → transform wrapping an expression

    Available functions: cumsum, time_shift, change, change_pct, rank, last.
    """

    formula: str  # e.g., "count", "revenue / count", "cumsum(revenue)"
    name: Optional[str] = None  # Technical column name (auto-generated if omitted)
    label: Optional[str] = None  # Human-readable label for output
    description: Optional[str] = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not _NAME_PATTERN.match(v):
            raise ValueError(f"Invalid name '{v}': must contain only letters, digits, and underscores, and start with a letter or underscore")
        return v


class ModelExtension(BaseModel):
    """Extend an existing model with extra dimensions, measures, or joins.

    Used inline on a query to add computed dimensions (SQL expressions),
    extra joins, or additional measures without modifying the stored model.
    """
    source_name: str                    # Model/query to extend
    dimensions: Optional[List] = None   # Extra Dimension objects
    measures: Optional[List] = None     # Extra Measure objects
    joins: Optional[List] = None        # Extra ModelJoin objects


class SlayerQuery(BaseModel):
    """User-facing query object. Specifies what data to retrieve from a model.

    This is intentionally minimal — just names and references, no SQL.
    The query engine enriches it into an EnrichedQuery for execution.

    Use `fields` for data columns and `filters` for conditions:
        fields=[{"formula": "count"}, {"formula": "revenue / count", "name": "aov"}]
        filters=["status == 'completed'", "amount > 100"]
    """

    name: Optional[str] = None  # For referencing this query from other queries in a list
    model: object  # str (model name), SlayerModel (inline), or ModelExtension
    fields: Optional[List[Field]] = None
    dimensions: Optional[List[ColumnRef]] = None
    time_dimensions: Optional[List[TimeDimension]] = None
    main_time_dimension: Optional[str] = None  # Explicit time dimension for transforms (overrides auto-detection)
    filters: Optional[List[str]] = None
    order: Optional[List[OrderItem]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    whole_periods_only: bool = False

    def snap_to_whole_periods(self) -> "SlayerQuery":
        """Adjust date filters to align with period boundaries when whole_periods_only=True.

        For each time dimension with a granularity, adds a date range filter
        to exclude the current incomplete period if no date filter exists.
        """
        if not self.whole_periods_only or not self.time_dimensions:
            return self

        filters = list(self.filters or [])
        for td in self.time_dimensions:
            gran = td.granularity
            dim_name = td.dimension.name

            # Check if any filter already references this time dimension
            has_filter = any(dim_name in f for f in filters)
            if not has_filter:
                # Add filter to exclude current incomplete period
                today = datetime.date.today()
                prev_end = gran.period_end(gran.period_start(today) - datetime.timedelta(days=1))
                filters.append(f"{dim_name} <= '{prev_end.isoformat()}'")

        return self.model_copy(update={"filters": filters, "whole_periods_only": False})
