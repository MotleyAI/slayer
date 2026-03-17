"""Query models for SLayer.

SlayerQuery is the user-facing query object — minimal, just enough to express intent.
It is later converted into EnrichedQuery (see slayer/engine/enriched.py) which carries
fully resolved SQL expressions, model metadata, and is ready for SQL generation.
"""

import datetime
import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from slayer.core.enums import TimeGranularity

_VAR_PATTERN = re.compile(r"\{(\w+)\}")


class ColumnRef(BaseModel):
    name: str
    model: Optional[str] = None

    @property
    def full_name(self) -> str:
        if self.model:
            return f"{self.model}.{self.name}"
        return self.name

    @classmethod
    def from_string(cls, s: str) -> "ColumnRef":
        if "." in s:
            model, name = s.split(".", 1)
            return cls(name=name, model=model)
        return cls(name=s)


class TimeDimension(BaseModel):
    dimension: ColumnRef
    granularity: TimeGranularity
    date_range: Optional[List[str]] = None


class OrderItem(BaseModel):
    column: ColumnRef
    direction: str = "asc"


class Field(BaseModel):
    """A computed field defined by a formula.

    The formula is parsed to determine the field type:
    - "count" → plain measure reference
    - "revenue / count" → arithmetic expression
    - "cumsum(revenue)" → transform function call
    - "time_shift(revenue, -1, 'year')" → time shift transform
    - "last(revenue)" → most recent value
    - "change(revenue / count)" → transform wrapping an expression

    Available functions: cumsum, lag, lead, change, change_pct, rank, last, time_shift.
    """

    formula: str  # e.g., "count", "revenue / count", "cumsum(revenue)"
    name: Optional[str] = None  # Output column name (auto-generated if omitted)
    description: Optional[str] = None


class SlayerQuery(BaseModel):
    """User-facing query object. Specifies what data to retrieve from a model.

    This is intentionally minimal — just names and references, no SQL.
    The query engine enriches it into an EnrichedQuery for execution.

    Use `fields` for data columns and `filters` for conditions:
        fields=[{"formula": "count"}, {"formula": "revenue / count", "name": "aov"}]
        filters=["status == 'completed'", "amount > 100"]
    """

    model: str
    fields: Optional[List[Field]] = None
    dimensions: Optional[List[ColumnRef]] = None
    time_dimensions: Optional[List[TimeDimension]] = None
    filters: Optional[List[str]] = None
    order: Optional[List[OrderItem]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    variables: Optional[Dict[str, Any]] = None
    whole_periods_only: bool = False

    def resolve_variables(self) -> "SlayerQuery":
        """Return a copy with {var} placeholders in filter values resolved from self.variables."""
        if not self.variables or not self.filters:
            return self
        resolved_filters = []
        for f in self.filters:
            resolved = f
            for var_name, var_value in self.variables.items():
                resolved = resolved.replace(f"{{{var_name}}}", str(var_value))
            resolved_filters.append(resolved)
        return self.model_copy(update={"filters": resolved_filters, "variables": None})

    def snap_to_whole_periods(self) -> "SlayerQuery":
        """Adjust date filters to align with period boundaries when whole_periods_only=True.

        For each time dimension with a granularity, adds a between() filter
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
