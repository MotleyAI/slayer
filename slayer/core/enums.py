"""Core enums for SLayer."""

import datetime
import re
from enum import Enum
from typing import Any, Literal, Optional


class StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class TypeKind(StrEnum):
    """The engine's type kinds, reached only through ``DataType.kind``; values match sqlglot's type names.

    ``UNKNOWN`` is opaque (no database equality): stored and displayed, never grouped, aggregated or cast.
    """

    TEXT = "TEXT"
    INT = "INT"
    DOUBLE = "DOUBLE"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    UNKNOWN = "UNKNOWN"
    ARRAY = "ARRAY"


class DataType(StrEnum):
    # No docstring on purpose — it would ship to agents in the MCP query schema.
    # Ossie's datatype names plus Array (a list); the engine reads each through ``kind``.

    STRING = "String"
    INTEGER = "Integer"
    DECIMAL = "Decimal"
    FLOAT = "Float"
    BOOLEAN = "Boolean"
    DATE = "Date"
    TIME = "Time"
    DATETIME = "DateTime"
    DATETIME_TZ = "DateTimeTz"
    OPAQUE = "Opaque"
    ARRAY = "Array"

    @property
    def kind(self) -> TypeKind:
        """The engine kind this value behaves as."""
        return _KINDS[self]

    @property
    def is_opaque(self) -> bool:
        """True when SLayer can store/display the field but not operate on it."""
        return _KINDS[self] is TypeKind.UNKNOWN

    @property
    def is_list(self) -> bool:
        return self is DataType.ARRAY

    @property
    def is_numeric(self) -> bool:
        return _KINDS[self] in (TypeKind.INT, TypeKind.DOUBLE)

    @property
    def is_temporal(self) -> bool:
        return _KINDS[self] in (TypeKind.DATE, TypeKind.TIMESTAMP)


#: The one wire → kind table.
_KINDS: dict[DataType, TypeKind] = {
    DataType.STRING: TypeKind.TEXT, DataType.INTEGER: TypeKind.INT, DataType.DECIMAL: TypeKind.DOUBLE,
    DataType.FLOAT: TypeKind.DOUBLE, DataType.BOOLEAN: TypeKind.BOOLEAN, DataType.DATE: TypeKind.DATE,
    DataType.TIME: TypeKind.UNKNOWN, DataType.DATETIME: TypeKind.TIMESTAMP, DataType.DATETIME_TZ: TypeKind.TIMESTAMP,
    DataType.OPAQUE: TypeKind.UNKNOWN, DataType.ARRAY: TypeKind.ARRAY,
}

#: The values of each engine kind.
NUMERIC_DATATYPES: frozenset[DataType] = frozenset(t for t in DataType if t.is_numeric)
TEMPORAL_DATATYPES: frozenset[DataType] = frozenset(t for t in DataType if t.is_temporal)


def canonical_datatype(datatype: DataType) -> DataType:
    """The value SLayer itself produces for ``datatype``'s kind (``Decimal`` → ``Float``, ``Time`` → ``Opaque``)."""
    return _CANONICAL[_KINDS[datatype]]


_CANONICAL: dict[TypeKind, DataType] = {
    TypeKind.TEXT: DataType.STRING, TypeKind.INT: DataType.INTEGER, TypeKind.DOUBLE: DataType.FLOAT,
    TypeKind.BOOLEAN: DataType.BOOLEAN, TypeKind.DATE: DataType.DATE, TypeKind.TIMESTAMP: DataType.DATETIME,
    TypeKind.UNKNOWN: DataType.OPAQUE, TypeKind.ARRAY: DataType.ARRAY,
}


def kind_of(datatype: Optional[DataType]) -> Optional[TypeKind]:
    """``datatype``'s engine kind, ``None`` for none."""
    return None if datatype is None else _KINDS[datatype]


#: The types a list's elements may have (``Opaque``: opaque elements).
ELEMENT_TYPES: frozenset[DataType] = frozenset(t for t in DataType if t is not DataType.ARRAY)


def type_display(data_type: Optional[DataType], element_type: Optional[DataType] = None) -> str:
    """``Array<String>`` for a list, else the type's value (``?`` when unknown)."""
    if data_type is None:
        return "?"
    if data_type is DataType.ARRAY:
        return f"Array<{element_type.value if element_type is not None else DataType.OPAQUE.value}>"
    return data_type.value


_EXACT_NUMERIC = re.compile(r"^\s*(DECIMAL|NUMERIC|NUMBER|DEC|MONEY|SMALLMONEY|BIGNUMERIC|BIGDECIMAL)\b", re.I)
_WITH_TZ = re.compile(r"WITH\s+(LOCAL\s+)?TIME\s+ZONE|TIMESTAMPTZ|TIMESTAMP_TZ|TIMESTAMP_LTZ|DATETIMEOFFSET", re.I)
_WITHOUT_TZ = re.compile(r"WITHOUT\s+TIME\s+ZONE", re.I)


def finer_datatype(datatype: DataType, db_type: Optional[str]) -> DataType:
    """``datatype`` narrowed by the database type: ``Float`` → ``Decimal`` for an exact numeric, ``DateTime`` →
    ``DateTimeTz`` for a zoned timestamp."""
    text = db_type or ""
    if datatype is DataType.FLOAT and _EXACT_NUMERIC.match(text):
        return DataType.DECIMAL
    if datatype is DataType.DATETIME and _WITH_TZ.search(text) and not _WITHOUT_TZ.search(text):
        return DataType.DATETIME_TZ
    return datatype


#: Pre-2.0 datatype values (and their lowercase legacy spellings); ``None``: a pseudo-type, dropped.
PRE_2_0_DATATYPES: dict[str, DataType | None] = {
    "TEXT": DataType.STRING, "INT": DataType.INTEGER, "DOUBLE": DataType.FLOAT, "BOOLEAN": DataType.BOOLEAN,
    "DATE": DataType.DATE, "TIMESTAMP": DataType.DATETIME, "UNKNOWN": DataType.OPAQUE, "ARRAY": DataType.ARRAY,
    "string": DataType.STRING, "number": DataType.FLOAT, "integer": DataType.INTEGER, "time": DataType.DATETIME,
    "date": DataType.DATE, "boolean": DataType.BOOLEAN, "unknown": DataType.OPAQUE,
    # Aggregation pseudo-types, dropped in v5 because they were unused.
    "count": None, "count_distinct": None, "sum": None, "avg": None, "min": None, "max": None, "last": None,
}


def stored_datatype(value: Any) -> DataType | None:
    """A stored datatype ``value`` in either vocabulary (a pre-2.0 one unsplit), ``None`` when it names none."""
    if not isinstance(value, str):
        return None
    if value in _WIRE_VALUES:
        return DataType(value)
    return PRE_2_0_DATATYPES.get(value)


_WIRE_VALUES = frozenset(t.value for t in DataType)


def pre_2_0_datatype(value: str, *, db_type: Optional[str] = None) -> DataType | None:
    """A pre-2.0 datatype ``value`` as its Ossie value, split by ``db_type``; ``None`` for a pseudo-type."""
    mapped = PRE_2_0_DATATYPES[value]
    return None if mapped is None else finer_datatype(mapped, db_type)



class Conformance(StrEnum):
    """The rules a query runs under: SLayer's own language, or the strict Ossie Layer 3 binding."""

    SLAYER = "slayer"
    OSSIE_L3 = "ossie-l3"


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
        """True iff every ``other`` boundary is a boundary of this bucket."""
        return boundaries_nest(
            fine=(self, 1, natural_origin(self)), coarse=(other, 1, natural_origin(other)),
        )


GRANULARITY_NAMES: frozenset[str] = frozenset(g.value for g in TimeGranularity)


class DatePart(StrEnum):
    """A ``date_part`` extraction; ``week``/``iso_year``/``day_of_week`` follow ISO-8601 (Mon=1…Sun=7)."""

    YEAR = "year"
    ISO_YEAR = "iso_year"
    QUARTER = "quarter"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    DAY_OF_WEEK = "day_of_week"
    DAY_OF_YEAR = "day_of_year"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"


# Lowercased spellings meaning one ``date_part`` part, as ``<alias>(x)`` and as a part word.
DATE_PART_ALIASES: dict[str, DatePart] = {
    "year": DatePart.YEAR, "quarter": DatePart.QUARTER, "month": DatePart.MONTH, "week": DatePart.WEEK,
    "day": DatePart.DAY, "hour": DatePart.HOUR, "minute": DatePart.MINUTE, "second": DatePart.SECOND,
    "isodow": DatePart.DAY_OF_WEEK, "day_of_week": DatePart.DAY_OF_WEEK,
    "doy": DatePart.DAY_OF_YEAR, "dayofyear": DatePart.DAY_OF_YEAR, "day_of_year": DatePart.DAY_OF_YEAR,
    "isoyear": DatePart.ISO_YEAR, "iso_year": DatePart.ISO_YEAR,
    "isoweek": DatePart.WEEK, "iso_week": DatePart.WEEK, "weekofyear": DatePart.WEEK,
    "dayofmonth": DatePart.DAY, "day_of_month": DatePart.DAY,
}
# Weekday names whose numbering differs across databases.
AMBIGUOUS_WEEKDAY_NAMES: frozenset[str] = frozenset({"dow", "dayofweek", "weekday"})
# Date spellings a custom aggregation or granularity may not take (lowercased).
RESERVED_DATE_SYNTAX_NAMES: frozenset[str] = frozenset({*DATE_PART_ALIASES, "extract"})
# Function names reserved for evaluation (Ossie's ``AGG``); a field may still take one.
RESERVED_NAMES: frozenset[str] = frozenset({"agg"})


def ambiguous_weekday_message(name: str) -> str:
    return (
        f"{name!r} has conflicting weekday numberings across databases; write "
        f"date_part('day_of_week', x) (ISO, Monday = 1 … Sunday = 7) or "
        f"date_part('day_of_week', x) % 7 (Sunday = 0 … Saturday = 6)."
    )


SUB_DAY_GRANULARITIES: frozenset[TimeGranularity] = frozenset({
    TimeGranularity.SECOND, TimeGranularity.MINUTE, TimeGranularity.HOUR,
})


_EPOCH = datetime.datetime(2000, 1, 1)
_NATURAL_ORIGINS: dict[TimeGranularity, datetime.datetime] = {
    TimeGranularity.WEEK: datetime.datetime(2000, 1, 3),         # a Monday
    TimeGranularity.WEEK_SUNDAY: datetime.datetime(2000, 1, 2),  # a Sunday
}
UNIT_SECONDS: dict[TimeGranularity, int] = {
    TimeGranularity.SECOND: 1, TimeGranularity.MINUTE: 60, TimeGranularity.HOUR: 3600,
    TimeGranularity.DAY: 86400, TimeGranularity.WEEK: 604800, TimeGranularity.WEEK_SUNDAY: 604800,
}
UNIT_MONTHS: dict[TimeGranularity, int] = {
    TimeGranularity.MONTH: 1, TimeGranularity.QUARTER: 3, TimeGranularity.YEAR: 12,
}

# (base, multiple, origin): boundaries at origin + k × multiple × base.
GranularityParts = tuple[TimeGranularity, int, datetime.datetime]


def natural_origin(unit: TimeGranularity) -> datetime.datetime:
    """A boundary of ``unit``'s natural alignment."""
    return _NATURAL_ORIGINS.get(unit, _EPOCH)


def boundaries_nest(*, fine: GranularityParts, coarse: GranularityParts) -> bool:
    """Every ``coarse`` boundary is a ``fine`` boundary, decided arithmetically."""
    b1, m1, o1 = fine
    b2, m2, o2 = coarse
    if b1 in UNIT_MONTHS and b2 in UNIT_MONTHS:
        l1, l2 = UNIT_MONTHS[b1] * m1, UNIT_MONTHS[b2] * m2
        months = (o2.year * 12 + o2.month) - (o1.year * 12 + o1.month)
        same_day = (o1.day, o1.time()) == (o2.day, o2.time())
        return l2 % l1 == 0 and same_day and months % l1 == 0
    if b1 not in UNIT_SECONDS:
        return False
    l1 = UNIT_SECONDS[b1] * m1
    offset = int((o2 - o1).total_seconds())
    if b2 in UNIT_SECONDS:
        return (UNIT_SECONDS[b2] * m2) % l1 == 0 and offset % l1 == 0
    return UNIT_SECONDS[TimeGranularity.DAY] % l1 == 0 and offset % l1 == 0


class OrderDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


class JoinType(StrEnum):
    LEFT = "left"
    INNER = "inner"


class RelationshipCardinality(StrEnum):
    """Arity of a relationship, read source->target ("many orders -> one customer").

    Descriptive metadata, orthogonal to ``JoinType`` — it does not change the
    emitted join.
    """
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


def invert_cardinality(
    cardinality: "RelationshipCardinality | None",
) -> "RelationshipCardinality | None":
    """Cardinality of the reverse edge; one_to_one / many_to_many / None are
    self-inverse.
    """
    if cardinality is RelationshipCardinality.MANY_TO_ONE:
        return RelationshipCardinality.ONE_TO_MANY
    if cardinality is RelationshipCardinality.ONE_TO_MANY:
        return RelationshipCardinality.MANY_TO_ONE
    return cardinality


# The kind of database object a ``sql_table``-mode dataset points at. A plain
# ``Literal`` (not ``StrEnum``) so the values persist as bare strings in
# YAML/SQLite without an enum-serialisation round-trip.
ObjectKind = Literal["table", "view", "materialized_view"]


# ---------------------------------------------------------------------------
# Aggregation constants
# ---------------------------------------------------------------------------

# Built-in aggregation names (always available without dataset-level definition).
BUILTIN_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "avg", "min", "max",
    "count", "count_distinct", "count_distinct_approx",
    "first", "last",
    "weighted_avg",
    "median", "percentile",
    "stddev_samp", "stddev_pop",
    "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop",
    "collect",
})

# Aggregations whose result is a list of their input's type.
LIST_AGGREGATIONS: frozenset[str] = frozenset({"collect"})

# Named once so classifier, planner and renderer agree.
RANKED_AGGREGATIONS = ("first", "last")

# Transforms that require a time dimension for ORDER BY
TIME_TRANSFORMS = {
    "cumsum",
    "change",
    "change_pct",
    "time_shift",
    "first",
    "last",
    "lag",
    "lead",
    "consecutive_periods",
}

# Transforms that don't need time ordering
TIMELESS_TRANSFORMS = {"rank", "percent_rank", "dense_rank", "ntile"}

ALL_TRANSFORMS = TIME_TRANSFORMS | TIMELESS_TRANSFORMS

# Default partition is the whole result set, not the group-by dimensions;
# ``partition_by=`` opts into per-partition ranking.
RANK_FAMILY_TRANSFORMS = {"rank", "percent_rank", "dense_rank", "ntile"}

# Time-ordered transforms that reduce along the axis: one value per partition
# (Axiom 11.3b). As an aggregation-source constituent they collapse to an exact
# per-partition pick.
AXIS_COLLAPSING_TRANSFORMS = frozenset({"first", "last"})

# ``classify_aggregation`` buckets each aggregation by result-vs-source relation;
# slot type and display format both read the bucket, so the axes cannot drift.

# Result is always an integer count, independent of the source field.
INTEGER_AGGREGATIONS: frozenset[str] = frozenset({
    "count", "count_distinct", "count_distinct_approx",
})


def builtin_empty_value(aggregation: str) -> Optional[int]:
    """A built-in aggregation's value over no rows: 0 for the count family, else NULL."""
    return 0 if aggregation in INTEGER_AGGREGATIONS else None
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
# Result preserves the source field's type AND format.
PRESERVING_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "min", "max", "first", "last",
})


class AggregationValueClass(StrEnum):
    """How an aggregation's result relates to its source field, for slot-type
    and display-format inference."""

    COUNT = "count"                            # INT type, INTEGER format
    PRESERVING = "preserving"                  # source type & format
    FLOAT_SOURCE_UNITS = "float_source_units"  # DOUBLE type, source format (else FLOAT)
    FLOAT_PLAIN = "float_plain"                # DOUBLE type, plain FLOAT format
    LIST = "list"                              # ARRAY of the source type, no format


def classify_aggregation(
    *, metric_name: Optional[str], aggregation: str, source_type: Optional[DataType],
) -> AggregationValueClass:
    """Bucket an aggregation for slot-type / display-format inference.

    ``measure_name == "*"`` (``count(*)``) and a boolean ``sum`` (a count of trues) are
    COUNT; custom/unknown aggregations fall through to PRESERVING (inherit source type & format).
    """
    if metric_name == "*":
        return AggregationValueClass.COUNT
    if aggregation in INTEGER_AGGREGATIONS:
        return AggregationValueClass.COUNT
    if aggregation in LIST_AGGREGATIONS:
        return AggregationValueClass.LIST
    if source_type is DataType.BOOLEAN and aggregation == "sum":
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
    # Approximate-distinct spellings agents / dbt-to-cube emit.
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


# Built-in aggregation SQL expressions (for aggregations that use a template).
# {value} = metric's SQL expression; {param_name} = parameter values.
# Note: percentile is dialect-dependent (no single template works on
# SQLite/ClickHouse/MySQL) and lives in render_aggregate instead.
BUILTIN_AGGREGATION_EXPRESSIONS: dict[str, str] = {
    # A NULL value's weight is skipped like the value; ``* 1.0`` keeps integer inputs from dividing as integers.
    "weighted_avg": "SUM({value} * {weight}) * 1.0 / NULLIF(SUM(CASE WHEN {value} IS NOT NULL THEN {weight} END), 0)",
}

# Built-in aggregations that require specific parameters.
# Percentile's required-param check lives in render_aggregate.
BUILTIN_AGGREGATION_REQUIRED_PARAMS: dict[str, list[str]] = {
    "weighted_avg": ["weight"],
    "corr": ["other"],
    "covar_samp": ["other"],
    "covar_pop": ["other"],
}

# Declared parameter order for built-in parametric aggregations: positional
# call values fold onto these names at binding (`percentile(x, 0.9)` ≡ `p=0.9`).
# first/last are absent on purpose — their positional arg is the ranking field.
BUILTIN_AGGREGATION_PARAM_ORDER: dict[str, list[str]] = {
    "percentile": ["p"],
    **BUILTIN_AGGREGATION_REQUIRED_PARAMS,
}

# Rejected at binding on non-numeric metrics; min/max/count*/first/last
# work on any type and are deliberately absent.
NUMERIC_ONLY_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "avg", "median", "weighted_avg", "percentile",
    "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop",
})

# Aggregations that read a boolean input as its integer (true 1, false 0); min / max
# convert the result back to BOOLEAN.
BOOLEAN_RESTORED_AGGREGATIONS: frozenset[str] = frozenset({"min", "max"})
BOOLEAN_LOWERED_AGGREGATIONS: frozenset[str] = NUMERIC_ONLY_AGGREGATIONS | BOOLEAN_RESTORED_AGGREGATIONS


# Per-type default whitelist, used when a field declares no explicit
# ``allowed_aggregations``.
_NUMERIC_AGGREGATIONS: frozenset[str] = frozenset({
    "sum", "avg", "min", "max", "count", "count_distinct", "count_distinct_approx",
    "median", "weighted_avg", "percentile", "first", "last",
    "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop", "collect",
})
_ORDERED_AGGREGATIONS: frozenset[str] = frozenset({
    "count", "count_distinct", "count_distinct_approx", "first", "last", "min", "max", "collect",
})

_AGGREGATIONS_BY_KIND: dict[TypeKind, frozenset[str]] = {
    # INT and DOUBLE share the same numeric aggregation set — the type
    # narrowing is for CAST emission, not for what's aggregable.
    TypeKind.INT: _NUMERIC_AGGREGATIONS,
    TypeKind.DOUBLE: _NUMERIC_AGGREGATIONS,
    TypeKind.TEXT: _ORDERED_AGGREGATIONS,
    # A boolean is its integer inside a numeric aggregation.
    TypeKind.BOOLEAN: _NUMERIC_AGGREGATIONS,
    TypeKind.DATE: _ORDERED_AGGREGATIONS,
    TypeKind.TIMESTAMP: _ORDERED_AGGREGATIONS,
    # A list counts as a whole or by distinct element sets; its elements aggregate through ``<field>.value``.
    TypeKind.ARRAY: frozenset({"count", "count_distinct"}),
}


#: Per-type default whitelist, by each value's kind.
DEFAULT_AGGREGATIONS_BY_TYPE: dict[DataType, frozenset[str]] = {
    t: _AGGREGATIONS_BY_KIND[t.kind] for t in DataType if t.kind in _AGGREGATIONS_BY_KIND
}

# A sole primary key is restricted to counting, min/max and collect, regardless of data type.
PRIMARY_KEY_AGGREGATIONS: frozenset[str] = frozenset({
    "count", "count_distinct", "count_distinct_approx", "min", "max", "collect",
})
