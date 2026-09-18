"""Core enums for SLayer."""

import datetime  # noqa: F401  (kept for downstream imports of TimeGranularity)
import difflib
from enum import Enum
from typing import Any, Literal, Optional


class StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class DataType(StrEnum):
    # No docstring on purpose — it would ship to agents in the MCP query schema.
    # Values match sqlglot exp.DataType.Type byte-for-byte. UNKNOWN = opaque
    # (no DB equality operator): stored/displayed, never grouped/aggregated/CAST — see is_opaque.

    TEXT = "TEXT"
    INT = "INT"
    DOUBLE = "DOUBLE"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    UNKNOWN = "UNKNOWN"

    @property
    def is_opaque(self) -> bool:
        """True when SLayer can store/display the column but not operate on it."""
        return self is DataType.UNKNOWN


# Legacy lowercase type spellings; pseudo-types drop to None (field default fires).
_LEGACY_DATATYPE_ALIASES: dict[str, str | None] = {
    # Pre-rename canonical values.
    "string": "TEXT",
    "number": "DOUBLE",
    "integer": "INT",
    "time": "TIMESTAMP",
    "date": "DATE",
    "boolean": "BOOLEAN",
    "unknown": "UNKNOWN",
    # Aggregation pseudo-types — dropped in v5 because they were unused.
    "count": None,
    "count_distinct": None,
    "sum": None,
    "avg": None,
    "min": None,
    "max": None,
    "last": None,
}


def _coerce_legacy_datatype(v: Any) -> Any:
    """Map legacy lowercase ``DataType`` strings to canonical values; pseudo-types
    -> None; everything else passes through (enum coercion raises on unknown)."""
    if isinstance(v, str):
        mapped = _LEGACY_DATATYPE_ALIASES.get(v)
        if v in _LEGACY_DATATYPE_ALIASES:
            return mapped
    return v



class TimeGranularity(StrEnum):
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    # Sunday-anchored week (Metabase convention); WEEK is Monday-anchored ISO-8601.
    WEEK_SUNDAY = "week_sunday"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"

    def period_start(self, date: datetime.date) -> datetime.date:
        if self in (TimeGranularity.SECOND, TimeGranularity.MINUTE, TimeGranularity.HOUR):
            return date
        if self == TimeGranularity.DAY:
            return date
        elif self == TimeGranularity.WEEK:
            return date - datetime.timedelta(days=date.weekday())
        elif self == TimeGranularity.WEEK_SUNDAY:
            # Round back to the Sunday at or before ``date``. weekday(): Mon=0..
            # Sun=6, so (weekday + 1) % 7 is the number of days since Sunday.
            return date - datetime.timedelta(days=(date.weekday() + 1) % 7)
        elif self == TimeGranularity.MONTH:
            return date.replace(day=1)
        elif self == TimeGranularity.QUARTER:
            quarter_month = ((date.month - 1) // 3) * 3 + 1
            return date.replace(month=quarter_month, day=1)
        elif self == TimeGranularity.YEAR:
            return date.replace(month=1, day=1)
        raise ValueError(f"Unexpected granularity: {self}")

    def period_end(self, date: datetime.date) -> datetime.date:
        if self in (TimeGranularity.SECOND, TimeGranularity.MINUTE, TimeGranularity.HOUR):
            return date
        if self == TimeGranularity.DAY:
            return date
        elif self == TimeGranularity.WEEK:
            return date + datetime.timedelta(days=6 - date.weekday())
        elif self == TimeGranularity.WEEK_SUNDAY:
            # Advance to the Saturday at or after ``date`` (last day of the
            # Sunday-anchored week). weekday(): Mon=0..Sun=6, Saturday=5.
            return date + datetime.timedelta(days=(5 - date.weekday()) % 7)
        elif self == TimeGranularity.MONTH:
            if date.month == 12:
                return date.replace(year=date.year + 1, month=1, day=1) - datetime.timedelta(days=1)
            else:
                return date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)
        elif self == TimeGranularity.QUARTER:
            quarter_end_month = ((date.month - 1) // 3) * 3 + 3
            if quarter_end_month == 12:
                return datetime.date(date.year, 12, 31)
            else:
                return datetime.date(date.year, quarter_end_month + 1, 1) - datetime.timedelta(days=1)
        elif self == TimeGranularity.YEAR:
            return date.replace(month=12, day=31)
        raise ValueError(f"Unexpected granularity: {self}")

    def nests_into(self, other: "TimeGranularity") -> bool:
        """True iff this bucket tiles ``other`` exactly (finer-or-equal, aligned): reflexive plus the transitive closure of second→minute→hour→day, day→week, day→week_sunday, day→month→quarter→year; week/week_sunday tile nothing coarser."""
        if self == other:
            return True
        seen: set["TimeGranularity"] = {self}
        frontier: list["TimeGranularity"] = [self]
        while frontier:
            for parent in _GRANULARITY_PARENTS.get(frontier.pop(), ()):
                if parent == other:
                    return True
                if parent not in seen:
                    seen.add(parent)
                    frontier.append(parent)
        return False


_GRANULARITY_PARENTS: dict[TimeGranularity, tuple[TimeGranularity, ...]] = {
    TimeGranularity.SECOND: (TimeGranularity.MINUTE,),
    TimeGranularity.MINUTE: (TimeGranularity.HOUR,),
    TimeGranularity.HOUR: (TimeGranularity.DAY,),
    TimeGranularity.DAY: (
        TimeGranularity.WEEK, TimeGranularity.WEEK_SUNDAY, TimeGranularity.MONTH,
    ),
    TimeGranularity.MONTH: (TimeGranularity.QUARTER,),
    TimeGranularity.QUARTER: (TimeGranularity.YEAR,),
}


class OrderDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


class JoinType(StrEnum):
    LEFT = "left"
    INNER = "inner"


class JoinCardinality(StrEnum):
    """Arity of a join, read source->target ("many orders -> one customer").

    Descriptive metadata, orthogonal to ``JoinType`` — it does not change the
    emitted join.
    """
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


def invert_cardinality(
    cardinality: "JoinCardinality | None",
) -> "JoinCardinality | None":
    """Cardinality of the reverse edge; one_to_one / many_to_many / None are
    self-inverse.
    """
    if cardinality is JoinCardinality.MANY_TO_ONE:
        return JoinCardinality.ONE_TO_MANY
    if cardinality is JoinCardinality.ONE_TO_MANY:
        return JoinCardinality.MANY_TO_ONE
    return cardinality


# The kind of database object a ``sql_table``-mode model points at. A plain
# ``Literal`` (not ``StrEnum``) so the values persist as bare strings in
# YAML/SQLite without an enum-serialisation round-trip.
ObjectKind = Literal["table", "view", "materialized_view"]


# ---------------------------------------------------------------------------
# Aggregation constants
# ---------------------------------------------------------------------------

# Built-in aggregation names (always available without model-level definition).
BUILTIN_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "avg", "min", "max",
    "count", "count_distinct", "count_distinct_approx",
    "first", "last",
    "weighted_avg",
    "median", "percentile",
    "stddev_samp", "stddev_pop",
    "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop",
})

# Named once so classifier, planner and renderer agree.
RANKED_AGGREGATIONS = ("first", "last")

# Default partition is the whole result set, not the group-by dimensions;
# ``partition_by=`` opts into per-partition ranking.
RANK_FAMILY_TRANSFORMS = {"rank", "percent_rank", "dense_rank", "ntile"}

# Time-ordered transforms that reduce along the axis: one value per partition
# (Axiom 11.3b). As an aggregation-source constituent they collapse to an exact
# per-partition pick (DEV-1832 D4c).
AXIS_COLLAPSING_TRANSFORMS = frozenset({"first", "last"})

# ``classify_aggregation`` buckets each aggregation by result-vs-source relation;
# slot type and display format both read the bucket, so the axes cannot drift.

# Result is always an integer count, independent of the source column.
INTEGER_AGGREGATIONS: frozenset[str] = frozenset({
    "count", "count_distinct", "count_distinct_approx",
})
# Result is a float in the SAME units as the source (display format inherited).
FLOAT_SOURCE_UNIT_AGGREGATIONS: frozenset[str] = frozenset({
    "avg", "weighted_avg", "median", "percentile",
    "stddev_samp", "stddev_pop",
})
# Result is a float in different units (dimensionless / squared / product), so it
# carries a plain FLOAT format, not the source's units.
FLOAT_PLAIN_AGGREGATIONS: frozenset[str] = frozenset({
    "corr", "var_samp", "var_pop", "covar_samp", "covar_pop",
})
# Result preserves the source column's type AND format.
PRESERVING_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "min", "max", "first", "last",
})


class AggregationValueClass(StrEnum):
    """How an aggregation's result relates to its source column, for slot-type
    and display-format inference (DEV-1788)."""

    COUNT = "count"                            # INT type, INTEGER format
    PRESERVING = "preserving"                  # source type & format
    FLOAT_SOURCE_UNITS = "float_source_units"  # DOUBLE type, source format (else FLOAT)
    FLOAT_PLAIN = "float_plain"                # DOUBLE type, plain FLOAT format


def classify_aggregation(
    *, measure_name: Optional[str], aggregation: str
) -> AggregationValueClass:
    """Bucket an aggregation for slot-type / display-format inference.

    ``measure_name == "*"`` (``*:count``) is COUNT; custom/unknown aggregations
    fall through to PRESERVING (inherit source type & format).
    """
    if measure_name == "*":
        return AggregationValueClass.COUNT
    if aggregation in INTEGER_AGGREGATIONS:
        return AggregationValueClass.COUNT
    if aggregation in FLOAT_SOURCE_UNIT_AGGREGATIONS:
        return AggregationValueClass.FLOAT_SOURCE_UNITS
    if aggregation in FLOAT_PLAIN_AGGREGATIONS:
        return AggregationValueClass.FLOAT_PLAIN
    return AggregationValueClass.PRESERVING

# Aliases agents routinely emit; ``stddev``/``var``/``variance`` map to the
# sample variants, matching Postgres defaults.
AGGREGATION_ALIASES: dict[str, str] = {
    "countd": "count_distinct",
    "countdistinct": "count_distinct",  # also matches "countDistinct" once lowercased
    # DEV-1595: approximate-distinct spellings agents / dbt-to-cube emit.
    "approx_count_distinct": "count_distinct_approx",
    "countdistinctapprox": "count_distinct_approx",  # matches "countDistinctApprox" lowercased
    "stddev": "stddev_samp",
    "var": "var_samp",
    "variance": "var_samp",
}


def normalize_aggregation_name(name: str) -> str:
    """Lowercase + alias-map to the canonical spelling; adopted only when the
    result is a builtin, else the original returns unchanged (custom names keep
    casing, unknown names still raise downstream)."""
    lowered = name.lower()
    candidate = AGGREGATION_ALIASES.get(lowered, lowered)
    return candidate if candidate in BUILTIN_AGGREGATIONS else name


def format_unknown_aggregation(name: str, known: "set[str] | frozenset[str]") -> str:
    """Shared 'Unknown aggregation' message (both binding gates): close-match
    hint + known list (builtins ∪ the model's custom aggregations)."""
    suggestion = difflib.get_close_matches(word=name, possibilities=sorted(known), n=1)
    hint = f" Did you mean '{suggestion[0]}'?" if suggestion else ""
    return f"Unknown aggregation '{name}'.{hint} Known: {sorted(known)}."


# Built-in aggregation SQL formulas (for aggregations that use a template).
# {value} = measure's SQL expression; {param_name} = parameter values.
# Note: percentile is dialect-dependent (no single template works on
# SQLite/ClickHouse/MySQL) and lives in generator._build_percentile instead.
BUILTIN_AGGREGATION_FORMULAS: dict[str, str] = {
    "weighted_avg": "SUM({value} * {weight}) / NULLIF(SUM({weight}), 0)",
}

# Built-in aggregations that require specific parameters.
# Percentile's required-param check lives in generator._build_percentile.
BUILTIN_AGGREGATION_REQUIRED_PARAMS: dict[str, list[str]] = {
    "weighted_avg": ["weight"],
    "corr": ["other"],
    "covar_samp": ["other"],
    "covar_pop": ["other"],
}

# Declared parameter order for built-in parametric aggregations: positional
# call values fold onto these names at binding (`percentile(x, 0.9)` ≡ `p=0.9`).
# first/last are absent on purpose — their positional arg is the ranking column.
BUILTIN_AGGREGATION_PARAM_ORDER: dict[str, list[str]] = {
    "percentile": ["p"],
    **BUILTIN_AGGREGATION_REQUIRED_PARAMS,
}

# Rejected at binding on non-numeric measures; min/max/count*/first/last
# work on any type and are deliberately absent.
NUMERIC_ONLY_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "avg", "median", "weighted_avg", "percentile",
    "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop",
})


# Per-type default whitelist, used when a column declares no explicit
# ``allowed_aggregations``.
_NUMERIC_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "avg", "min", "max", "count", "count_distinct", "count_distinct_approx",
    "median", "weighted_avg", "percentile", "first", "last",
    "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop",
})

DEFAULT_AGGREGATIONS_BY_TYPE: dict[DataType, frozenset[str]] = {
    # INT and DOUBLE share the same numeric aggregation set — the type
    # narrowing is for CAST emission, not for what's aggregable. (DEV-1361.)
    DataType.INT: _NUMERIC_AGGREGATIONS,
    DataType.DOUBLE: _NUMERIC_AGGREGATIONS,
    DataType.TEXT: frozenset({
        "count", "count_distinct", "count_distinct_approx", "first", "last", "min", "max",
    }),
    DataType.BOOLEAN: frozenset({
        "count", "count_distinct", "count_distinct_approx", "sum", "min", "max", "first", "last",
    }),
    DataType.DATE: frozenset({
        "count", "count_distinct", "count_distinct_approx", "first", "last", "min", "max",
    }),
    DataType.TIMESTAMP: frozenset({
        "count", "count_distinct", "count_distinct_approx", "first", "last", "min", "max",
    }),
}

# Primary-key columns are always restricted to row-counting aggregations,
# regardless of data type. (You can ``count`` customer_ids, but not ``sum`` them.)
PRIMARY_KEY_AGGREGATIONS: frozenset[str] = frozenset({
    "count", "count_distinct", "count_distinct_approx",
})
