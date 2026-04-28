"""Query models for SLayer.

SlayerQuery is the user-facing query object — minimal, just enough to express intent.
It is later converted into EnrichedQuery (see slayer/engine/enriched.py) which carries
fully resolved SQL expressions, model metadata, and is ready for SQL generation.
"""
from __future__ import annotations

import datetime
import logging
import re
from typing import Annotated, Any, Dict, List, Optional

from pydantic import BaseModel, BeforeValidator, field_validator, model_validator

from slayer.core.enums import TimeGranularity

logger = logging.getLogger(__name__)

_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_VAR_PATTERN = re.compile(r"\{\{|\}\}|\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\{([^}]*)\}")


def substitute_variables(filter_str: str, variables: Dict[str, Any]) -> str:
    """Substitute {variable} placeholders in a filter string.

    - {var_name} is replaced with the variable's value (str or number, inserted as-is).
    - {{ and }} are escaped to literal { and }.
    - Variable names must be alphanumeric + underscore.
    - Raises ValueError for undefined variables or invalid variable names.

    Example:
        substitute_variables("status = '{status_val}'", {"status_val": "active"})
        → "status = 'active'"

        substitute_variables("amount > {min_amount}", {"min_amount": 100})
        → "amount > 100"
    """
    def _replace(match: re.Match) -> str:
        full = match.group(0)
        if full == "{{":
            return "{"
        if full == "}}":
            return "}"
        # Group 1: valid variable name
        valid_name = match.group(1)
        if valid_name is not None:
            if valid_name not in variables:
                raise ValueError(
                    f"Undefined variable '{valid_name}' in filter: {filter_str!r}. "
                    f"Available variables: {sorted(variables.keys())}"
                )
            value = variables[valid_name]
            if not isinstance(value, (str, int, float)):
                raise ValueError(
                    f"Variable '{valid_name}' must be a string or number, got {type(value).__name__}"
                )
            return str(value)
        # Group 2: invalid variable name (matched {something} but name was invalid)
        bad_name = match.group(2)
        raise ValueError(
            f"Invalid variable name '{bad_name}' in filter: {filter_str!r}. "
            f"Variable names must contain only letters, digits, and underscores."
        )

    return _VAR_PATTERN.sub(_replace, filter_str)


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


_FUNCSTYLE_CALL_PATTERN = re.compile(r"^\w+\([^()]*\)$")


def _coerce_order_column(v: Any) -> Any:
    """Coerce ORDER BY column, normalizing aggregation syntax.

    Handles both colon syntax and function-style syntax for built-in
    aggregations. Converts to the underscore form that matches enriched
    measure names.

    Examples:
    - "revenue:sum" → "revenue_sum"
    - "*:count" → "_count"
    - "sum(revenue)" → "revenue_sum"
    - "revenue:last(ordered_at)" → "revenue_last"
    - "rolling_avg(revenue)" → placeholder, raw_formula carries the call so
      enrichment can resolve it via ``extra_agg_names``.
    """
    if isinstance(v, str):
        from slayer.core.formula import _rewrite_funcstyle_aggregations
        rewritten = _rewrite_funcstyle_aggregations(v)
        if _FUNCSTYLE_CALL_PATTERN.match(rewritten):
            # Unrewritten function-style call (custom aggregation). Enrichment
            # parses raw_formula with custom_agg_names and overwrites
            # column.name with the canonical alias, so a placeholder is fine.
            return {"name": "_funcstyle_pending"}
        if ":" in rewritten:
            base, agg = rewritten.rsplit(":", 1)
            agg_name = agg.split("(", 1)[0]  # strip arglist
            if base == "*":
                rewritten = f"_{agg_name}"
            else:
                rewritten = f"{base}_{agg_name}"
        return {"name": rewritten}
    return v


class TimeDimension(BaseModel):
    dimension: Annotated[ColumnRef, BeforeValidator(_coerce_column_ref)]
    granularity: TimeGranularity
    date_range: Optional[List[str]] = None
    label: Optional[str] = None


class OrderItem(BaseModel):
    column: Annotated[ColumnRef, BeforeValidator(_coerce_order_column)]
    direction: str = "asc"
    raw_formula: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _capture_raw_formula(cls, data: Any) -> Any:
        """Capture the raw column formula before coercion normalizes it."""
        if isinstance(data, dict):
            col = data.get("column")
            if isinstance(col, str):
                from slayer.core.formula import _rewrite_funcstyle_aggregations
                rewritten = _rewrite_funcstyle_aggregations(col)
                if ":" in rewritten or _FUNCSTYLE_CALL_PATTERN.match(rewritten):
                    data = {**data, "raw_formula": rewritten}
        return data


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
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'fields' must be a list, got {type(v).__name__}")
    return [{"formula": item} if isinstance(item, str) else item for item in v]


def _coerce_dimensions(v: Any) -> Any:
    """Allow plain strings in the dimensions list: "status" → {"name": "status"}."""
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'dimensions' must be a list, got {type(v).__name__}")
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


def _get_source_model_name(source_model: object) -> Optional[str]:
    """Extract the model name from any source_model type.

    Works before model resolution — handles str, dict, ModelExtension,
    and SlayerModel (or any object with a .name attribute).
    """
    if isinstance(source_model, str):
        return source_model
    if isinstance(source_model, dict):
        return source_model.get("source_name") or source_model.get("name")
    # ModelExtension has .source_name; SlayerModel has .name
    source_name = getattr(source_model, "source_name", None)
    if isinstance(source_name, str):
        return source_name
    name = getattr(source_model, "name", None)
    if isinstance(name, str):
        return name
    return None


def _strip_column_ref(ref: ColumnRef, model_name: str) -> ColumnRef:
    """Strip source model prefix from a ColumnRef.

    "orders.status"          on model "orders" → model=None,  name="status"
    "orders.customers.name"  on model "orders" → model="customers", name="name"
    "customers.name"         on model "orders" → unchanged
    "status"                 on model "orders" → unchanged
    """
    if ref.model is None:
        return ref
    if ref.model == model_name:
        return ref.model_copy(update={"model": None})
    prefix = model_name + "."
    if ref.model.startswith(prefix):
        return ref.model_copy(update={"model": ref.model[len(prefix):]})
    return ref


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
    variables: Optional[Dict[str, Any]] = None  # Variable values for filter substitution
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

    def strip_source_model_prefix(self) -> "SlayerQuery":
        """Strip redundant source model name prefix from all dotted references.

        LLMs frequently include the source model name as a prefix
        (e.g., "orders.revenue:sum" instead of "revenue:sum" when
        querying source_model="orders"). This normalizes all references
        by removing the redundant prefix before any other processing.
        """
        model_name = _get_source_model_name(self.source_model)
        if model_name is None:
            return self

        updates: Dict[str, Any] = {}
        pattern = re.compile(r"\b" + re.escape(model_name) + r"\.")

        # Dimensions
        if self.dimensions:
            new_dims = [_strip_column_ref(d, model_name) for d in self.dimensions]
            if any(n is not o for n, o in zip(new_dims, self.dimensions)):
                updates["dimensions"] = new_dims

        # Time dimensions
        if self.time_dimensions:
            new_tds = []
            td_changed = False
            for td in self.time_dimensions:
                stripped = _strip_column_ref(td.dimension, model_name)
                if stripped is not td.dimension:
                    new_tds.append(TimeDimension(
                        dimension=stripped,
                        granularity=td.granularity,
                        date_range=td.date_range,
                        label=td.label,
                    ))
                    td_changed = True
                else:
                    new_tds.append(td)
            if td_changed:
                updates["time_dimensions"] = new_tds

        # Order
        if self.order:
            new_order = []
            order_changed = False
            for item in self.order:
                stripped = _strip_column_ref(item.column, model_name)
                stripped_raw_formula = (
                    pattern.sub("", item.raw_formula) if item.raw_formula else None
                )
                if stripped is not item.column or stripped_raw_formula != item.raw_formula:
                    new_order.append(OrderItem(
                        column=stripped,
                        direction=item.direction,
                        raw_formula=stripped_raw_formula,
                    ))
                    order_changed = True
                else:
                    new_order.append(item)
            if order_changed:
                updates["order"] = new_order

        # Fields (formula strings)
        if self.fields:
            new_fields = []
            fields_changed = False
            for f in self.fields:
                new_formula = pattern.sub("", f.formula)
                if new_formula != f.formula:
                    new_fields.append(f.model_copy(update={"formula": new_formula}))
                    fields_changed = True
                else:
                    new_fields.append(f)
            if fields_changed:
                updates["fields"] = new_fields

        # Filters
        if self.filters:
            new_filters = [pattern.sub("", f) for f in self.filters]
            if new_filters != self.filters:
                updates["filters"] = new_filters

        # main_time_dimension
        prefix = model_name + "."
        if self.main_time_dimension and self.main_time_dimension.startswith(prefix):
            updates["main_time_dimension"] = self.main_time_dimension[len(prefix):]

        if not updates:
            return self

        logger.info(
            "Stripped source model prefix '%s.' from query references",
            model_name,
        )
        return self.model_copy(update=updates)
