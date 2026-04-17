"""Core enums for SLayer."""

import datetime
from enum import Enum


class StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class DataType(StrEnum):
    STRING = "string"
    TIMESTAMP = "time"
    DATE = "date"
    BOOLEAN = "boolean"
    NUMBER = "number"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    AVERAGE = "avg"
    MIN = "min"
    MAX = "max"
    LAST = "last"

    @property
    def is_aggregation(self) -> bool:
        return self in (
            DataType.COUNT,
            DataType.COUNT_DISTINCT,
            DataType.SUM,
            DataType.AVERAGE,
            DataType.MIN,
            DataType.MAX,
            DataType.LAST,
        )

    @property
    def python_type(self) -> type:
        return {
            DataType.STRING: str,
            DataType.TIMESTAMP: datetime.datetime,
            DataType.DATE: datetime.date,
            DataType.BOOLEAN: bool,
            DataType.NUMBER: float,
            DataType.COUNT: int,
            DataType.COUNT_DISTINCT: int,
            DataType.SUM: float,
            DataType.AVERAGE: float,
            DataType.MIN: float,
            DataType.MAX: float,
            DataType.LAST: float,
        }[self]



class TimeGranularity(StrEnum):
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
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


# ---------------------------------------------------------------------------
# Aggregation constants
# ---------------------------------------------------------------------------

# Built-in aggregation names (always available without model-level definition).
BUILTIN_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "avg", "min", "max",
    "count", "count_distinct",
    "first", "last",
    "weighted_avg",
    "median", "percentile",
})

# Built-in aggregation SQL formulas (for aggregations that use a template).
# {value} = measure's SQL expression; {param_name} = parameter values.
BUILTIN_AGGREGATION_FORMULAS: dict[str, str] = {
    "weighted_avg": "SUM({value} * {weight}) / NULLIF(SUM({weight}), 0)",
    "percentile": "PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {value})",
}

# Built-in aggregations that require specific parameters.
BUILTIN_AGGREGATION_REQUIRED_PARAMS: dict[str, list[str]] = {
    "weighted_avg": ["weight"],
    "percentile": ["p"],
}

# Aggregations that only make sense on numeric-valued measures. Applying them
# to a non-numeric measure (e.g. AVG on a VARCHAR column) is always invalid
# and is rejected during query enrichment rather than at SQL execution time.
# min, max, count, count_distinct, first, last work on any type and are NOT
# in this set.
NUMERIC_ONLY_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "avg", "median", "weighted_avg", "percentile",
})
