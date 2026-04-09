"""Query models for SLayer.

SlayerQuery is the user-facing query object — minimal, just enough to express intent.
It is later converted into EnrichedQuery (see slayer/engine/enriched.py) which carries
fully resolved SQL expressions, model metadata, and is ready for SQL generation.
"""
from __future__ import annotations

import datetime
import re
from typing import Annotated, Any, List, Optional

from pydantic import BaseModel, BeforeValidator, field_validator, model_validator

from slayer.core.enums import TimeGranularity

_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class ColumnRef(BaseModel):
    """Reference to a dimension by name.

    Supports dotted paths for joined models: "status", "customers.name",
    "customers.regions.name" (multi-hop). Dots are parsed at validation time:
    everything before the last dot goes into ``model``, the leaf stays in ``name``.

    Computed dimensions (SQL expressions) should be defined via ModelExtension
    on the query's model.
    """
    name: str
    model: Optional[str] = None
    label: Optional[str] = None

    @model_validator(mode="after")
    def _parse_dotted_name(self) -> "ColumnRef":
        """Parse dotted paths into model + leaf name.

        "customers.regions.name" → model="customers.regions", name="name"
        "customers.name"         → model="customers",         name="name"
        "status"                 → model=None,                 name="status"
        """
        if self.model is None and "." in self.name:
            prefix, leaf = self.name.rsplit(".", 1)
            self.model = prefix
            self.name = leaf
        # Validate leaf name (must be a simple identifier, no dots)
        if not _NAME_PATTERN.match(self.name):
            raise ValueError(
                f"Invalid name '{self.name}': must contain only letters, "
                f"digits, and underscores, and start with a letter or underscore"
            )
        # Validate each part of the model path
        if self.model:
            for part in self.model.split("."):
                if not _NAME_PATTERN.match(part):
                    raise ValueError(
                        f"Invalid model path '{self.model}': each part must contain "
                        f"only letters, digits, and underscores"
                    )
        return self

    @property
    def full_name(self) -> str:
        if self.model:
            return f"{self.model}.{self.name}"
        return self.name

    @classmethod
    def from_string(cls, s: str) -> ColumnRef:
        """Create a ColumnRef from a string. Dots are parsed by the validator."""
        return cls(name=s)


def _coerce_column_ref(v: Any) -> Any:
    """Allow plain string where a ColumnRef is expected: "x" → {"name": "x"}."""
    if isinstance(v, str):
        return {"name": v}
    return v


class TimeDimension(BaseModel):
    dimension: Annotated[ColumnRef, BeforeValidator(_coerce_column_ref)]
    granularity: TimeGranularity
    date_range: Optional[List[str]] = None
    label: Optional[str] = None


class OrderItem(BaseModel):
    column: Annotated[ColumnRef, BeforeValidator(_coerce_column_ref)]
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


def _coerce_fields(v: Any) -> Any:
    """Allow plain strings in the fields list: "count" → {"formula": "count"}."""
    if v is None:
        return v
    return [{"formula": item} if isinstance(item, str) else item for item in v]


def _coerce_dimensions(v: Any) -> Any:
    """Allow plain strings in the dimensions list: "status" → {"name": "status"}."""
    if v is None:
        return v
    return [{"name": item} if isinstance(item, str) else item for item in v]


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
    source_model: object  # str (model name), SlayerModel (inline), or ModelExtension
    fields: Annotated[Optional[List[Field]], BeforeValidator(_coerce_fields)] = None

    @field_validator("name")
    @classmethod
    def _validate_no_dunder_in_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and "__" in v:
            raise ValueError(
                f"Query name '{v}' must not contain '__'. "
                f"Double underscores are reserved for join path aliases in generated SQL."
            )
        return v
    dimensions: Annotated[Optional[List[ColumnRef]], BeforeValidator(_coerce_dimensions)] = None
    time_dimensions: Optional[List[TimeDimension]] = None
    main_time_dimension: Optional[str] = None  # Explicit time dimension for transforms (overrides auto-detection)
    filters: Optional[List[str]] = None
    order: Optional[List[OrderItem]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    whole_periods_only: bool = False
    dry_run: bool = False  # Generate SQL without executing
    explain: bool = False  # Run EXPLAIN ANALYZE on the generated SQL

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
