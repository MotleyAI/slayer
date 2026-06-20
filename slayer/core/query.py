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

from pydantic import BaseModel, BeforeValidator, ConfigDict, field_validator, model_validator

from slayer.core.enums import TimeGranularity
from slayer.core.models import ModelMeasure
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function
from slayer.storage.migrations import migrate as _migrate_schema

logger = logging.getLogger(__name__)

_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_VAR_PATTERN = re.compile(r"\{\{|\}\}|\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\{([^}]*)\}")


def _validate_query_filter_string(formula: str) -> None:
    """Apply DEV-1369 DSL-mode construction-time rules to a single
    ``SlayerQuery.filters`` entry: reject raw ``OVER (...)`` window-function
    syntax.

    Raw SQL function calls (``json_extract``, ``coalesce``, …) and
    unknown bare names are rejected at enrichment time by
    :func:`slayer.core.formula.parse_filter` and the strict-resolution
    pass in :func:`slayer.engine.enrichment.resolve_filter_columns`.
    """
    if has_window_function(formula):
        raise ValueError(f"Filter '{formula}' {WINDOW_IN_FILTER_ERROR}")


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


def extract_placeholder_names(query: "SlayerQuery") -> set:
    """Return the set of valid {var} placeholder names referenced in
    ``query.filters``. Used to compute required-variable lists and to
    inject placeholder defaults during save-time dry-run validation.
    """
    found: set = set()
    for f in (query.filters or []):
        for match in _VAR_PATTERN.finditer(f):
            if match.group(0) in ("{{", "}}"):
                continue
            valid_name = match.group(1)
            if valid_name:
                found.add(valid_name)
    return found


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


# DEV-1575: the accepted ORDER BY direction vocabulary (case-insensitive),
# mapping every synonym to the canonical lowercase form the SQL generator
# compares against (``direction == "asc"``). Single source of truth shared by
# the shorthand-healing detector (``_process_order_item``) and the
# ``OrderItem.direction`` normalizing validator.
_DIRECTION_NORMALIZE = {
    "asc": "asc",
    "ascending": "asc",
    "desc": "desc",
    "descending": "desc",
}


def _is_direction(value: Any) -> bool:
    """True if ``value`` is a recognized direction word (case/whitespace-insensitive)."""
    return isinstance(value, str) and value.strip().lower() in _DIRECTION_NORMALIZE


class TimeDimension(BaseModel):
    dimension: Annotated[ColumnRef, BeforeValidator(_coerce_column_ref)]
    granularity: TimeGranularity
    date_range: Optional[List[str]] = None
    label: Optional[str] = None


class OrderItem(BaseModel):
    # DEV-1575: reject stray keys so a mixed canonical+shorthand item
    # (e.g. ``{"column": "x", "b": "asc"}``) raises loudly instead of silently
    # dropping the extra key.
    model_config = ConfigDict(extra="forbid")

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

    @field_validator("direction")
    @classmethod
    def _normalize_direction(cls, v: str) -> str:
        """DEV-1575: normalize direction to canonical ``asc``/``desc`` (case- and
        whitespace-insensitive, accepting ``ascending``/``descending`` synonyms),
        rejecting anything else.

        The SQL generator compares ``direction == "asc"`` strictly, so a
        non-normalized value (``"ASC"``, ``"ascending"``) would silently emit
        DESC. Normalizing here fixes that for both healed and canonical items.
        """
        if _is_direction(v):
            return _DIRECTION_NORMALIZE[v.strip().lower()]
        raise ValueError(
            "order direction must be one of asc/desc/ascending/descending "
            f"(case-insensitive), got {v!r}"
        )


def _coerce_measures(v: Any) -> Any:
    """Allow plain strings in the measures list: "count" → {"formula": "count"}."""
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'measures' must be a list, got {type(v).__name__}")
    return [{"formula": item} if isinstance(item, str) else item for item in v]


def _coerce_dimensions(v: Any) -> Any:
    """Allow plain strings in the dimensions list: "status" → {"name": "status"}."""
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'dimensions' must be a list, got {type(v).__name__}")
    return [{"name": item} if isinstance(item, str) else item for item in v]


def _process_order_item(item: Any) -> list:
    """DEV-1575: heal a single ``order`` entry, returning the list of canonical
    items it expands to.

    LLM agents frequently write a shorthand dict mapping column → direction
    instead of the canonical ``{"column", "direction"}`` shape. A dict with no
    ``column``/``direction`` key whose values are *all* direction words is
    treated as shorthand and expanded — one canonical item per key, preserving
    insertion order (so a single-key dict yields one item and a multi-key dict
    yields several). Everything else passes through unchanged: canonical dicts
    (their stray keys are policed by ``OrderItem``'s ``extra="forbid"``) and
    malformed input (rejected by ``OrderItem`` validation).
    """
    if not isinstance(item, dict):
        return [item]
    # A dict carrying a reserved key is canonical-intended; never reinterpret it
    # as shorthand. extra="forbid" on OrderItem rejects any stray keys.
    if "column" in item or "direction" in item:
        return [item]
    if (
        item
        and all(isinstance(k, str) for k in item)
        and all(_is_direction(val) for val in item.values())
    ):
        return [{"column": k, "direction": val} for k, val in item.items()]
    return [item]


def _coerce_order(v: Any) -> Any:
    """DEV-1575: heal shorthand ``order`` items before ``OrderItem`` validation.

    Accepts the canonical ``list``/``tuple`` of items, healing each shorthand
    dict (see ``_process_order_item``). A bare single item passed without the
    enclosing list (a ``dict`` or an ``OrderItem``) is wrapped into a
    one-element list; any other non-list input raises (mirrors the
    ``_coerce_measures``/``_coerce_dimensions`` convention).
    """
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        if isinstance(v, (dict, OrderItem)):
            v = [v]
        else:
            raise TypeError(f"'order' must be a list, got {type(v).__name__}")
    result: list = []
    for item in v:
        result.extend(_process_order_item(item))
    return result


class ModelExtension(BaseModel):
    """Extend an existing model with extra columns, measures, or joins.

    Used inline on a query to add computed columns (SQL expressions),
    extra joins, or additional measure formulas without modifying the
    stored model.
    """
    source_name: str                                # Model/query to extend
    columns: Optional[List] = None                  # Extra Column objects
    measures: Optional[List[ModelMeasure]] = None   # Extra ModelMeasure formulas
    joins: Optional[List] = None                    # Extra ModelJoin objects


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

    Use ``measures`` for computed/aggregated values and ``filters`` for
    conditions::

        measures=[{"formula": "*:count"}, {"formula": "revenue:sum / *:count", "name": "aov"}]
        filters=["status == 'completed'", "amount > 100"]
    """

    model_config = ConfigDict(extra="forbid")

    version: int = 3
    name: Optional[str] = None  # For referencing this query from other queries in a list
    source_model: object  # str (model name), SlayerModel (inline), or ModelExtension
    measures: Annotated[Optional[List[ModelMeasure]], BeforeValidator(_coerce_measures)] = None

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="SlayerQuery", data=data)

    @field_validator("name")
    @classmethod
    def _validate_query_name(cls, v: Optional[str]) -> Optional[str]:
        # Share the same rejection rules as SlayerModel.name —
        # SlayerQuery names occupy the same naming space when persisted
        # as query-backed models. Rejects ``__`` (join-path alias
        # separator), ``.`` (dotted reference syntax), and ``:`` (DSL
        # aggregation separator).
        if v is None:
            return v
        from slayer.core.models import _validate_model_name
        return _validate_model_name(v, "Query")
    dimensions: Annotated[Optional[List[ColumnRef]], BeforeValidator(_coerce_dimensions)] = None
    time_dimensions: Optional[List[TimeDimension]] = None
    main_time_dimension: Optional[str] = None  # Explicit time dimension for transforms (overrides auto-detection)
    filters: Optional[List[str]] = None
    variables: Optional[Dict[str, Any]] = None  # Variable values for filter substitution
    order: Annotated[Optional[List[OrderItem]], BeforeValidator(_coerce_order)] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    whole_periods_only: bool = False
    # DEV-1543: opt out of the auto "distinct dimension tuples" GROUP BY
    # for dim-only queries. Default ``True`` preserves the Cube.js-style
    # dedup that fires when ``measures`` is empty. Setting ``False`` emits
    # a flat ``SELECT <dims/td-exprs> FROM ... WHERE ... ORDER BY ...
    # LIMIT ...`` projection. Any measure reference (in ``measures``, in
    # ``filters``, or in ``order``) is rejected with
    # ``DistinctDimensionValuesError``; both ``dimensions`` and
    # ``time_dimensions`` empty is also rejected (nothing to project).
    distinct_dimension_values: bool = True

    @model_validator(mode="after")
    def _validate_dsl_user_input(self) -> "SlayerQuery":
        """DEV-1369: enforce DSL-mode rules on every user-input string field.

        Filter strings are pre-parsed in DSL mode so raw ``OVER (...)``
        is caught at construction time with an actionable error message.
        Bare-name strict resolution and raw-SQL-function rejection happen
        at enrichment, where the parser has full custom-aggregation and
        named-measure context.

        Note: ``__`` is **not** rejected here. Virtual-model columns
        produced by ``_query_as_model`` flatten join paths into single
        identifiers like ``kpis__total_amount_sum``, which downstream
        stages reference directly. Strict resolution at enrichment
        catches typos that don't resolve to any column / measure.
        """
        if self.filters:
            for f in self.filters:
                _validate_query_filter_string(f)
        self._validate_distinct_dimension_values()
        return self

    def _validate_distinct_dimension_values(self) -> None:
        """DEV-1543: structural rejection rules for ``distinct_dimension_values=False``.

        Only the cheap, model-free checks fire here:

        * ``measures`` non-empty — flag asks for raw rows, but the query
          asks for aggregations.
        * Both ``dimensions`` and ``time_dimensions`` empty — there are
          no projected columns to ``SELECT``.

        Deep filter / order measure-reference checks happen at enrichment,
        where named measures, custom aggregations, and post-substitution
        text are all available. Detecting them here would either reject
        valid ``{var}`` filters before substitution or miss model-defined
        custom aggregations.
        """
        if self.distinct_dimension_values:
            return
        from slayer.core.errors import DistinctDimensionValuesError

        if self.measures:
            n = len(self.measures)
            raise DistinctDimensionValuesError(
                f"distinct_dimension_values=False requires an empty `measures` "
                f"field, but {n} measure(s) were supplied. Either remove the "
                f"measures (and any other measure references) or set "
                f"distinct_dimension_values=True (the default) to keep the "
                f"auto-aggregating behaviour."
            )
        if not self.dimensions and not self.time_dimensions:
            raise DistinctDimensionValuesError(
                "distinct_dimension_values=False requires at least one of "
                "`dimensions` or `time_dimensions` to be non-empty — there "
                "are no columns to SELECT. Add the columns you want to "
                "project."
            )

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

        # Measures (formula strings)
        if self.measures:
            new_measures = []
            measures_changed = False
            for f in self.measures:
                new_formula = pattern.sub("", f.formula)
                if new_formula != f.formula:
                    new_measures.append(f.model_copy(update={"formula": new_formula}))
                    measures_changed = True
                else:
                    new_measures.append(f)
            if measures_changed:
                updates["measures"] = new_measures

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

        # Sanitize for log injection (S5145): model names are usually trusted
        # internal identifiers, but they originate from user input via the
        # public API, so strip CR/LF before logging.
        safe_name = model_name.replace("\r", "\\r").replace("\n", "\\n")
        logger.info(
            "Stripped source model prefix '%s.' from query references",
            safe_name,
        )
        return self.model_copy(update=updates)
