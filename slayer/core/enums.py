"""Core enums for SLayer."""

import datetime  # noqa: F401  (kept for downstream imports of TimeGranularity)
from enum import Enum
from typing import Any


class StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class DataType(StrEnum):
    """SLayer data types — values match sqlglot's ``exp.DataType.Type``
    byte-for-byte so SQL generation can ``CAST`` to the declared type without
    a translation map. (DEV-1361.)"""

    TEXT = "TEXT"
    INT = "INT"
    DOUBLE = "DOUBLE"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"


# DEV-1361: lenient before-validator absorbs legacy lowercase type spellings
# from older agent input (MCP/REST/CLI), pseudo-types (count/sum/...) drop to
# None so the field falls through to its default. Used by both Column and
# ModelMeasure validators in slayer/core/models.py.
_LEGACY_DATATYPE_ALIASES: dict[str, str | None] = {
    # Pre-rename canonical values.
    "string": "TEXT",
    "number": "DOUBLE",
    "integer": "INT",
    "time": "TIMESTAMP",
    "date": "DATE",
    "boolean": "BOOLEAN",
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
    """Map legacy lowercase ``DataType`` strings to current canonical values.

    Pseudo-types resolve to ``None`` so the calling validator can drop them
    and let the field default fire. Already-canonical values, enum instances,
    and unknown strings pass through untouched (Pydantic's enum coercion will
    raise on unknown).
    """
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
    # DEV-1572: Sunday-anchored week (weeks start on Sunday, end on Saturday).
    # WEEK is Monday-anchored (ISO-8601); WEEK_SUNDAY exists so Metabase week
    # breakouts — which use Sunday weeks — bucket the way Metabase asked for.
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


class OrderDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


class JoinType(StrEnum):
    LEFT = "left"
    INNER = "inner"


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

# DEV-1576: unambiguous aggregation-name aliases that LLM agents routinely
# emit. ``normalize_aggregation_name`` lowercases the incoming token and maps
# it through this table; the result is only adopted when it lands in
# ``BUILTIN_AGGREGATIONS``. ``stddev``/``var``/``variance`` map to the *sample*
# variants, matching Postgres' bare ``stddev``/``variance`` defaults.
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
    """Coerce an aggregation token to its canonical SLayer spelling.

    Lowercases the token and applies :data:`AGGREGATION_ALIASES`. The
    normalized form is only adopted when it is a real built-in aggregation;
    otherwise the **original** string is returned unchanged so genuinely
    unknown names still raise downstream (and custom aggregation names keep
    their exact casing). Examples::

        "countd"        -> "count_distinct"
        "countDistinct" -> "count_distinct"
        "stddev"        -> "stddev_samp"
        "SUM"           -> "sum"
        "myCustomAgg"   -> "myCustomAgg"  (unchanged — not a builtin)
        "bogus"         -> "bogus"        (unchanged — still raises later)
    """
    lowered = name.lower()
    candidate = AGGREGATION_ALIASES.get(lowered, lowered)
    return candidate if candidate in BUILTIN_AGGREGATIONS else name


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

# Aggregations that only make sense on numeric-valued measures. Applying them
# to a non-numeric measure (e.g. AVG on a VARCHAR column) is always invalid
# and is rejected during query enrichment rather than at SQL execution time.
# min, max, count, count_distinct, first, last work on any type and are NOT
# in this set.
NUMERIC_ONLY_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "avg", "median", "weighted_avg", "percentile",
    "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop",
})


# Default aggregations applicable to a column based on its data type, when the
# column has no explicit ``allowed_aggregations`` whitelist. Used by the engine
# to gate ``column:agg`` expressions (e.g., ``revenue:sum`` requires ``sum`` to
# be eligible for the ``revenue`` column's data type).
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
