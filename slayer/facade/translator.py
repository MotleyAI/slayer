"""SQL → SlayerQuery translator (DEV-1390 §6; DEV-1486 shared layer).

Shared pipeline for every SQL string entering a wire facade (Arrow Flight
SQL or Postgres), whether through a simple-query, a ``CommandStatementQuery``,
or the prepared-statement triplet. Returns a tagged-union ``TranslatorResult``
whose subclass tells the handler which kind of response to send; raises
``TranslationError`` on user-visible failures (parse error, unknown table,
``SELECT *``, DML/DDL, etc.).

The pipeline (see §6 of DEV-1390; DEV-1558 swapped catalog-matchers for a
DuckDB-backed executor on the Postgres path):

1. Parse with sqlglot (optionally with a dialect).
2. Probe-query whitelist → canned ``RowBatch``.
3. Classify AST root → reject DML/DDL, no-op SET/SHOW/BEGIN/COMMIT
   (carrying a ``command_tag`` so the Postgres facade can drive its
   transaction state machine), continue on SELECT.
4. If ``catalog_sql_executor`` is provided (Postgres facade) and
   ``is_catalog_only(parsed)`` is True, execute the SQL against the
   in-memory DuckDB and return a ``PgCatalogResult``. Otherwise
   (Flight facade) dispatch INFORMATION_SCHEMA queries to the canned
   ``match_info_schema`` builder.
5. ``SELECT *`` rejection (on real models).
6. SLayer-table translation → ``SlayerQuery`` + column-name mapping.

The translator never touches the engine or storage — it produces a
``SlayerQuery`` description and lets the handler decide when to call
``engine.execute()``.

``dialect`` affects ONLY the parse step. Predicate execution coverage is
unchanged: WHERE conjuncts are emitted verbatim into ``SlayerQuery.filters``,
which the engine parses as Mode B DSL.
"""

from __future__ import annotations

import logging
import re
from contextvars import ContextVar
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, JoinType, TimeGranularity
from slayer.core.models import ModelJoin, SlayerModel
from slayer.core.query import (
    ColumnRef,
    ModelExtension,
    OrderItem,
    SlayerQuery,
    TimeDimension,
)
from slayer.facade.catalog import (
    CATALOG_NAME,
    FacadeCatalog,
    FacadeDimension,
    FacadeMetric,
    FacadeTable,
    build_local_view,
)
from slayer.facade.info_schema import match_info_schema
from slayer.facade.probe_queries import match_probe
from slayer.facade.rows import RowBatch

logger = logging.getLogger(__name__)

_IN_FACADE_PARSE: ContextVar[bool] = ContextVar("slayer_facade_parse", default=False)
_COMMAND_FALLBACK_MARKER = "Falling back to parsing as a 'Command'"


class _SuppressCommandFallbackWarning(logging.Filter):
    """sqlglot warns whenever a statement parses to the generic ``Command``
    node. For facade traffic that fallback is the expected, handled path
    (``SHOW TRANSACTION ISOLATION LEVEL`` etc. — one warning per BI
    connection), so it is suppressed while ``translate`` is parsing.
    Engine/user parse paths keep the warning."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not (
            _IN_FACADE_PARSE.get() and _COMMAND_FALLBACK_MARKER in record.getMessage()
        )


logging.getLogger("sqlglot").addFilter(_SuppressCommandFallbackWarning())


# --- session-setting capture (DEV-1569) -------------------------------------


class SetSettingOp(BaseModel):
    """A single ``SET <name> = <value>`` capture or ``set_config(name, value, ...)``
    mutation. Names are lowercased on capture. Consumed by the Postgres
    facade to mutate its per-connection session-settings map. The Flight
    facade ignores these.
    """

    name: str
    value: str


class ResetSettingOp(BaseModel):
    """``RESET <name>`` (``name`` set) or ``RESET ALL`` (``reset_all=True``)."""

    name: Optional[str] = None
    reset_all: bool = False


# A probe matcher takes the parsed statement and returns either a canned
# ``RowBatch`` (Flight default) or a ``ProbeMatcherOutcome`` (Postgres facade,
# used to tunnel ``set_config`` mutation hints through to the connection). The
# translator unwraps both into a ``ProbeResult``.
class ProbeMatcherOutcome(BaseModel):
    """Postgres-facade-only wrapper carrying a probe's row batch alongside
    any pending session-setting mutation (e.g. from ``set_config(...)``).
    The Flight facade's default matcher returns the bare ``RowBatch``; the
    translator accepts either."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    batch: RowBatch
    settings_mutation: Optional[SetSettingOp] = None


ProbeMatcher = Callable[
    [exp.Expression], Optional[Union[RowBatch, ProbeMatcherOutcome]],
]


# --- result types (tagged union via subclassing) -----------------------------


class TranslatorResult(BaseModel):
    """Base for every translator outcome. Handlers ``isinstance``-dispatch."""

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ProbeResult(TranslatorResult):
    """One of the whitelisted connection probes matched.

    ``settings_mutation`` carries a ``set_config(name, value, ...)``
    mutation hint when the matcher returned a ``ProbeMatcherOutcome``.
    The Postgres facade applies it on Execute (but not Describe). Flight
    matchers return ``None`` here."""

    batch: RowBatch
    settings_mutation: Optional[SetSettingOp] = None


class InfoSchemaResult(TranslatorResult):
    """``SELECT ... FROM INFORMATION_SCHEMA.<TABLE>`` matched."""

    batch: RowBatch


class PgCatalogResult(TranslatorResult):
    """An injected catalog matcher (e.g. ``pg_catalog.*``) matched."""

    batch: RowBatch


class NoOpResult(TranslatorResult):
    """``BEGIN`` / ``COMMIT`` / ``ROLLBACK`` / ``SET`` / ``SHOW`` — empty success.

    ``command_tag`` is the facade-neutral verb (``"BEGIN"``, ``"COMMIT"``,
    ``"ROLLBACK"``, ``"SET"``, ``"SHOW"``, ``"USE"``, ``"RESET"``,
    ``"START TRANSACTION"``). The Postgres facade uses it to drive its
    transaction state machine and pick the ``CommandComplete`` tag; the
    Flight facade ignores it.

    ``set_setting`` carries the captured ``(name, value)`` pair when the
    root parsed as ``exp.Set`` with a single ``name = value`` SetItem.
    ``reset_setting`` carries the parsed ``RESET <name>`` or ``RESET ALL``
    intent. Both are PG-facade-only; the Flight facade ignores them.
    """

    command_tag: Optional[str] = None
    set_setting: Optional[SetSettingOp] = None
    reset_setting: Optional[ResetSettingOp] = None


class QueryResult(TranslatorResult):
    """Translated SlayerQuery for engine execution.

    ``column_name_mapping`` is ordered to match the user's projection
    list; each tuple is ``(engine_alias, bi_tool_projected_name)``.
    Server uses this to rewrite the SLayer response's column keys
    (``orders.revenue_sum``) back into the BI-tool's flat names
    (``revenue_sum``) before emitting the result set.

    ``projection_types`` is the catalog-declared ``DataType`` for each
    projected item, in the same order. ``None`` entries fall back to a
    text type at wire-schema build time (custom aggs, measures with
    unknown declared type, …).
    """

    query: SlayerQuery
    column_name_mapping: List[Tuple[str, str]]
    facade_table: FacadeTable
    schema_name: str
    projection_types: "List[Optional['DataType']]"

    @property
    def flight_table(self) -> FacadeTable:
        """Back-compat alias — this field was ``flight_table`` before the
        facade extraction (DEV-1486). Kept so ``slayer.flight.translator``
        callers reading ``result.flight_table`` keep working."""
        return self.facade_table


# --- error types -------------------------------------------------------------


class TranslationError(Exception):
    """User-visible translation failure; carries a status hint."""

    def __init__(self, message: str, *, status: str = "INVALID_ARGUMENT") -> None:
        super().__init__(message)
        self.status = status


READ_ONLY_MESSAGE = "SLayer wire facade is read-only"
SELECT_STAR_MESSAGE = (
    "SELECT * not supported; project specific metric or dimension names. "
    "Use 'SELECT * FROM INFORMATION_SCHEMA.METRICS WHERE table_name=...' "
    "to discover available names."
)
# DEV-1493: aggregating over a saved measure or a non-column expression needs
# a multi-stage rewrite, out of scope for the current facade aggregate mapping.
AGG_OVER_MEASURE_MESSAGE = (
    "Aggregating over a saved measure or a non-column expression is not "
    "supported yet (only aggregates over base/joined columns are mapped). "
    "See DEV-1493. Project the saved measure by its name instead."
)


# --- AST helpers -------------------------------------------------------------


_TIME_GRAIN_NAMES: Dict[str, TimeGranularity] = {
    "year": TimeGranularity.YEAR,
    "quarter": TimeGranularity.QUARTER,
    "month": TimeGranularity.MONTH,
    "week": TimeGranularity.WEEK,
    "day": TimeGranularity.DAY,
    "hour": TimeGranularity.HOUR,
    "minute": TimeGranularity.MINUTE,
    "second": TimeGranularity.SECOND,
}

# sqlglot represents the unwrapped one-arg time functions as dedicated nodes
# (exp.Month, exp.Year, …). date_trunc is exp.DateTrunc with a literal unit.
_TIME_GRAIN_CLASSES: Dict[type, TimeGranularity] = {
    exp.Year: TimeGranularity.YEAR,
    exp.Quarter: TimeGranularity.QUARTER,
    exp.Month: TimeGranularity.MONTH,
    exp.Week: TimeGranularity.WEEK,
    exp.Day: TimeGranularity.DAY,
    # Hour/Minute/Second don't all have dedicated AST classes; we also accept
    # them via exp.Anonymous below.
}

# sqlglot aggregate-function AST classes → SLayer aggregation names. COUNT is
# handled separately (it has *-arg and DISTINCT variants).
_AGG_CLASS_TO_NAME: Dict[type, str] = {
    exp.Sum: "sum",
    exp.Avg: "avg",
    exp.Min: "min",
    exp.Max: "max",
}

_COMPARATOR_SQL: Dict[type, str] = {
    exp.GT: ">",
    exp.GTE: ">=",
    exp.LT: "<",
    exp.LTE: "<=",
    exp.EQ: "=",
    exp.NEQ: "<>",
}

# DEV-1566: sqlglot DataType.Type → SLayer DataType for CAST(<col> AS <type>)
# projection support. Aliases sqlglot canonicalises at parse time (STRING→TEXT,
# INTEGER→INT, NUMERIC→DECIMAL, BOOL→BOOLEAN, FLOAT→DOUBLE) need no entry;
# REAL parses to exp.DataType.Type.FLOAT (not normalised), so DOUBLE coverage
# routes through here. Parameterised forms (VARCHAR(255)) collapse onto the
# same base member at parse time — precision is dropped at the SLayer
# boundary because SLayer wire types don't carry it.
#
# Codex round 8 — silently-coercive targets DROPPED:
#   * DECIMAL/NUMERIC: PostgreSQL exposes these as OID 1700 (`numeric`,
#     exact-precision). DataType.DOUBLE would advertise OID 701 (`float8`)
#     and round-trip via the float encoder — wrong wire type, potential
#     precision loss. Until SLayer adds a NUMERIC DataType + OID 1700 +
#     decimal encoder, the only safe answer is to reject.
#   * TIMESTAMPTZ / TIMESTAMP WITH TIME ZONE / TIMESTAMPLTZ: OID 1184 with
#     timezone-aware client decoding. DataType.TIMESTAMP would advertise
#     OID 1114 (`timestamp`, no-TZ) — wrong wire type AND wrong timezone
#     semantics. Reject until proper TIMESTAMPTZ support lands.
# These pairs now fall through to the "Unsupported projection expression"
# error, same as UUID/JSON/ARRAY/STRUCT.
_SQLGLOT_TYPE_TO_DATATYPE: Dict[exp.DataType.Type, DataType] = {
    exp.DataType.Type.TEXT: DataType.TEXT,
    exp.DataType.Type.VARCHAR: DataType.TEXT,
    exp.DataType.Type.CHAR: DataType.TEXT,
    exp.DataType.Type.NCHAR: DataType.TEXT,
    exp.DataType.Type.NVARCHAR: DataType.TEXT,
    exp.DataType.Type.INT: DataType.INT,
    exp.DataType.Type.BIGINT: DataType.INT,
    exp.DataType.Type.SMALLINT: DataType.INT,
    exp.DataType.Type.TINYINT: DataType.INT,
    exp.DataType.Type.MEDIUMINT: DataType.INT,
    exp.DataType.Type.DOUBLE: DataType.DOUBLE,
    exp.DataType.Type.FLOAT: DataType.DOUBLE,
    exp.DataType.Type.BOOLEAN: DataType.BOOLEAN,
    exp.DataType.Type.DATE: DataType.DATE,
    exp.DataType.Type.TIMESTAMP: DataType.TIMESTAMP,
    exp.DataType.Type.DATETIME: DataType.TIMESTAMP,
}

# DEV-1566: per-pair allowlist of CAST coercions admitted in projection.
# Each pair is one the wire encoders in slayer/pg_facade/types.py handle
# losslessly. The unknown-source (None) case admits ONLY TEXT — for every
# other target we'd push the failure into value_to_text / value_to_binary at
# response time, surfacing as an opaque connection error instead of a clean
# translation error. Identity (X→X) is always admitted; computed implicitly.
_CAST_ADMITTED_TARGETS: Dict[DataType, frozenset] = {
    DataType.DATE: frozenset({DataType.DATE, DataType.TIMESTAMP, DataType.TEXT}),
    DataType.TIMESTAMP: frozenset({DataType.TIMESTAMP, DataType.DATE, DataType.TEXT}),
    DataType.INT: frozenset({DataType.INT, DataType.DOUBLE, DataType.TEXT}),
    # DOUBLE → INT intentionally dropped (Python int(x) truncates toward zero;
    # Postgres rounds half-to-even). Round-2 Codex review.
    DataType.DOUBLE: frozenset({DataType.DOUBLE, DataType.TEXT}),
    DataType.BOOLEAN: frozenset({DataType.BOOLEAN, DataType.TEXT}),
    DataType.TEXT: frozenset({DataType.TEXT}),
}

# DEV-1566 — Codex round 3: pairs whose ORDER BY / GROUP BY semantics under
# Postgres differ from what the bare-column engine projection would produce.
# A query that references a CAST-projected item (via alias or canonical form)
# in ORDER BY / GROUP BY is rejected when the pair is in the corresponding
# set — workaround: order/group by the bare column, or wait for a follow-up
# ticket that pushes CAST into the engine SQL.
#
# ORDER BY lossy: every X→TEXT pair (lex sort ≠ engine's natural sort).
# Identity X→X, INT→DOUBLE, DATE↔TIMESTAMP all preserve relative order so
# they stay admitted.
_LOSSY_ORDER_BY_CAST_PAIRS: frozenset = frozenset({
    (DataType.INT, DataType.TEXT),
    (DataType.DOUBLE, DataType.TEXT),
    (DataType.BOOLEAN, DataType.TEXT),
    (DataType.DATE, DataType.TEXT),
    (DataType.TIMESTAMP, DataType.TEXT),
})
# GROUP BY lossy: many-to-one casts where the engine-column grouping
# returns MORE groups than the casted column would.
#   - TIMESTAMP → DATE: multiple timestamps per date.
#   - INT → DOUBLE: int64 has precise range ±2^53 in IEEE 754 float64;
#     larger bigints lose precision so distinct ints can collapse to the
#     same double under Postgres's GROUP BY semantics, but the facade
#     groups by the bare int and over-reports groups.
# Every other admitted pair is a 1:1 / identity mapping within the
# supported value range.
_LOSSY_GROUP_BY_CAST_PAIRS: frozenset = frozenset({
    (DataType.TIMESTAMP, DataType.DATE),
    (DataType.INT, DataType.DOUBLE),
})


def _sqlglot_type_to_datatype(node: exp.DataType) -> Optional[DataType]:
    """Map a sqlglot ``DataType`` node to a SLayer ``DataType``.

    Returns ``None`` when the type member isn't in the mapping (UUID, JSON,
    ARRAY, STRUCT, …) — caller falls through to the existing
    'Unsupported projection expression' error.
    """
    return _SQLGLOT_TYPE_TO_DATATYPE.get(node.this)


def _is_admitted_cast(source: Optional[DataType], target: DataType) -> bool:
    """Strict per-pair admission. ``None`` source admits only ``TEXT``."""
    if source is None:
        return target is DataType.TEXT
    if source == target:
        return True
    return target in _CAST_ADMITTED_TARGETS.get(source, frozenset())


def _column_to_dotted(
    col: exp.Column,
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> str:
    """Reconstruct the dotted reference from a sqlglot ``Column``.

    ``customers.regions.name`` (3-part) → ``"customers.regions.name"``
    ``customers.row_count`` (2-part)    → ``"customers.row_count"``
    ``revenue_sum``         (bare)      → ``"revenue_sum"``

    DEV-1558 B5: when ``strip_prefix=(schema, table)`` is given and the
    leading qualifiers on the column reference are exactly
    ``schema.table.`` (case-insensitive, or with ``schema='public'`` since
    the pg facade always exposes models under ``public``), drop them so
    three-part refs like ``"public"."orders"."customer_id"`` resolve to the
    bare ``customer_id`` dimension.

    DEV-1565: when ``alias_map`` is given and the column's leading table
    qualifier matches an alias entry (case-insensitive), the alias is
    rewritten to the target SLayer model name BEFORE the dotted form is
    built — so ``"Stores"."name"`` with ``alias_map={"Stores": "stores"}``
    yields ``"stores.name"`` (the SLayer cross-model dotted form).
    """
    parts: List[str] = []
    for key in ("catalog", "db", "table"):
        node = col.args.get(key)
        if node is None:
            continue
        parts.append(str(node.this) if hasattr(node, "this") else str(node))
    leaf = col.this
    parts.append(str(leaf.this) if hasattr(leaf, "this") else str(leaf))
    parts = _apply_alias_remap(parts, alias_map)
    return ".".join(_apply_strip_prefix(parts, strip_prefix))


def _apply_alias_remap(
    parts: List[str], alias_map: Optional[Dict[str, str]],
) -> List[str]:
    """If ``parts`` is at least 2-part and the table-qualifier (second-to-
    last element) matches an entry in ``alias_map`` (case-insensitive),
    rewrite it to the target model name. Otherwise return ``parts``
    unchanged.

    DEV-1565: maps ``<JoinAlias>.<col>`` refs (e.g. Metabase's
    ``"Stores"."name"``) to ``<model_name>.<col>`` (``"stores"."name"``).
    """
    if not alias_map or len(parts) < 2:
        return parts
    qual = parts[-2]
    target = alias_map.get(qual)
    if target is None:
        # case-insensitive fallback for quoted-identifier mismatches
        lower = qual.lower()
        for k, v in alias_map.items():
            if k.lower() == lower:
                target = v
                break
    if target is None:
        return parts
    new = list(parts)
    new[-2] = target
    return new


def _apply_strip_prefix(
    parts: List[str], strip_prefix: Optional[Tuple[str, str]],
) -> List[str]:
    """Drop the leading ``schema.table.`` qualifier from ``parts`` when it
    matches ``strip_prefix``. Four-part catalog-qualified refs drop the
    leading 3; three-part drops the leading 2; two-part drops the
    leading 1. Bare and unrelated refs pass through unchanged.

    For 4-part refs, the leading catalog must match the SLayer catalog
    name (``slayer``) — otherwise we leave the ref alone (a foreign
    catalog reference is not addressable here).
    """
    if strip_prefix is None:
        return parts
    schema_p, table_p = strip_prefix
    if len(parts) >= 4:
        c = parts[-4].lower()
        s = parts[-3].lower()
        t = parts[-2].lower()
        if (c == CATALOG_NAME.lower()
                and t == table_p.lower()
                and s in {"public", schema_p.lower()}):
            return parts[:-4] + parts[-1:]
    if len(parts) >= 3:
        s = parts[-3].lower()
        t = parts[-2].lower()
        if t == table_p.lower() and s in {"public", schema_p.lower()}:
            return parts[:-3] + parts[-1:]
    if len(parts) == 2 and parts[0].lower() == table_p.lower():
        return parts[1:]
    return parts


def _detect_time_grain_date_trunc(
    node: exp.Expression,
) -> Optional[Tuple[TimeGranularity, exp.Column]]:
    """Plain ``DATE_TRUNC(<unit>, <col>)`` detector.

    Does NOT unwrap any day offsets on the column side — a bare
    ``DATE_TRUNC('week', col + INTERVAL '1 day')`` is a user-written
    shifted bucket, not the Metabase Sunday-week wrapper, and must
    fall through to the regular "unsupported projection" error.
    The full Sunday-week pattern is handled by
    ``_detect_sunday_week_wrapper`` which requires BOTH the outer ``-1
    day`` shift and the inner ``+1 day`` shift to be present together.
    """
    unit = node.args.get("unit")
    col = node.this
    if unit is None:
        return None
    # The unit is a string literal under the dialect-less parse and a bare
    # identifier (``exp.Var``) under the Postgres dialect's TIMESTAMP_TRUNC.
    if isinstance(unit, (exp.Literal, exp.Var)):
        unit_str = str(unit.this).lower()
    else:
        unit_str = str(unit).lower()
    grain = _TIME_GRAIN_NAMES.get(unit_str)
    if grain is None:
        return None
    if isinstance(col, exp.Cast):
        col = col.this
    if not isinstance(col, exp.Column):
        return None
    return grain, col


def _date_trunc_unit(node: exp.Expression) -> Optional[str]:
    """Return the lowercase unit string of a ``DATE_TRUNC``/``TIMESTAMP_TRUNC``
    node, or ``None`` if not a trunc call. Used by the Sunday-week wrapper
    detector below."""
    if not isinstance(node, (exp.DateTrunc, exp.TimestampTrunc)):
        return None
    unit = node.args.get("unit")
    if unit is None:
        return None
    if isinstance(unit, (exp.Literal, exp.Var)):
        return str(unit.this).lower()
    return str(unit).lower()


def _detect_sunday_week_wrapper(
    node: exp.Expression,
) -> Optional[Tuple[TimeGranularity, exp.Column]]:
    """Recognise the complete Metabase Sunday-week wrapper as a single shape.

    Full pattern (the one Metabase emits for a week breakout on a DATE
    column)::

        (CAST(DATE_TRUNC('week', <col> + INTERVAL '1 day') AS DATE)
         + INTERVAL '-1 day')

    Metabase rounds to Sunday by shifting the input forward one day,
    applying Postgres' Monday-based ``DATE_TRUNC('week', ...)``, then
    shifting the result back one day. SLayer models this as
    ``TimeGranularity.WEEK_SUNDAY`` (DEV-1572), whose per-dialect SQL
    generation reproduces the same Sunday-anchored bucketing — so the
    result matches what Metabase asked for instead of being rejected.

    Both halves must be present together — a bare ``DATE_TRUNC`` with a
    ``+1``-day inner offset and no outer wrapper, OR an outer ``-1``-day
    wrapper around a grain other than WEEK, are NOT Sunday-week and must
    NOT be silently collapsed (they're either user intent we must preserve
    or other translator gaps that should keep raising). Each shift leg must
    be exactly one day. Returns ``(WEEK_SUNDAY, col)`` on match, ``None``
    otherwise. The outer ``CAST`` is already peeled by ``_detect_time_grain``.
    """
    inner = _unwrap_signed_day_offset(node, expected_sign=-1)
    if inner is node:
        return None  # no outer -1-day shift
    if isinstance(inner, exp.Cast):
        inner = inner.this
    if isinstance(inner, exp.Paren):
        inner = inner.this
    if _TIME_GRAIN_NAMES.get(_date_trunc_unit(inner) or "") != TimeGranularity.WEEK:
        return None
    col = inner.this
    if isinstance(col, exp.Cast):
        col = col.this
    unwrapped_col = _unwrap_signed_day_offset(col, expected_sign=1)
    if unwrapped_col is col:
        return None  # no inner +1-day shift — partial wrapper, not Sunday-week
    if not isinstance(unwrapped_col, exp.Column):
        return None
    return TimeGranularity.WEEK_SUNDAY, unwrapped_col


def _day_interval_sign(node: exp.Expression) -> Optional[int]:
    """Return ``+1`` for ``INTERVAL '1 day'``, ``-1`` for ``INTERVAL '-1 day'``,
    or ``None`` if ``node`` isn't a one-day interval at all.

    Recognised forms:
    * Dialect-less parse: ``INTERVAL '1 day'`` / ``INTERVAL '-1 day'`` — the
      literal carries the unit string.
    * Postgres dialect: ``INTERVAL '1' DAY`` — literal is the magnitude,
      unit is a separate ``DAY`` node.
    """
    if not isinstance(node, exp.Interval):
        return None
    val = node.this
    if not isinstance(val, exp.Literal):
        return None
    s = str(val.this).strip().lower().replace("'", "")
    unit = node.args.get("unit")
    unit_str = ""
    if unit is not None:
        unit_str = str(unit.this if hasattr(unit, "this") else unit).lower()
    if s in {"1", "-1"}:
        if not unit_str.startswith("day"):
            return None
        return 1 if s == "1" else -1
    if s == "1 day":
        return 1
    if s == "-1 day":
        return -1
    return None


def _unwrap_signed_day_offset(
    node: exp.Expression, *, expected_sign: int,
) -> exp.Expression:
    """If ``node`` shifts by exactly ``expected_sign`` days (``+1`` or ``-1``)
    via a single ADD/SUB of ``INTERVAL '1 day'`` / ``INTERVAL '-1 day'``,
    return the inner expression. Otherwise return ``node`` unchanged.

    Direction matters: ``expected_sign=-1`` matches Metabase's outer
    Sunday-week wrapper (``<expr> + INTERVAL '-1 day'`` or
    ``<expr> - INTERVAL '1 day'``) but NOT the inverse, so a legitimate
    user-written ``DATE_TRUNC('week', x + INTERVAL '1 day')`` outside the
    Sunday-week wrapper stays preserved (would not match
    ``expected_sign=-1``). ``expected_sign=+1`` matches the inner
    column-side shift (``<col> + INTERVAL '1 day'`` or
    ``<col> - INTERVAL '-1 day'``).
    """
    if isinstance(node, exp.Paren):
        inner = _unwrap_signed_day_offset(node.this, expected_sign=expected_sign)
        if inner is not node.this:
            return inner
    if isinstance(node, exp.Add):
        # Adding +1 day → net +1; adding -1 day → net -1.
        right_sign = _day_interval_sign(node.expression)
        if right_sign is not None and right_sign == expected_sign:
            return node.this
        left_sign = _day_interval_sign(node.this)
        if left_sign is not None and left_sign == expected_sign:
            return node.expression
    if isinstance(node, exp.Sub):
        # Subtracting +1 day → net -1; subtracting -1 day → net +1.
        right_sign = _day_interval_sign(node.expression)
        if right_sign is not None and -right_sign == expected_sign:
            return node.this
    return node


def _detect_time_grain_single_arg(
    node: exp.Expression,
) -> Optional[Tuple[TimeGranularity, exp.Column]]:
    """Dedicated AST classes like ``exp.Month`` / ``exp.Year``."""
    for cls, grain in _TIME_GRAIN_CLASSES.items():
        if isinstance(node, cls):
            target = node.this
            if isinstance(target, exp.Column):
                return grain, target
            return None
    return None


def _detect_time_grain_anonymous(
    node: exp.Anonymous,
) -> Optional[Tuple[TimeGranularity, exp.Column]]:
    """``hour(col)`` / ``minute(col)`` / ``second(col)`` come through here."""
    grain = _TIME_GRAIN_NAMES.get(str(node.this).lower())
    if grain is None:
        return None
    args = node.args.get("expressions") or []
    if len(args) == 1 and isinstance(args[0], exp.Column):
        return grain, args[0]
    return None


def _detect_time_grain(node: exp.Expression) -> Optional[Tuple[TimeGranularity, exp.Column]]:
    """If ``node`` is ``<grain>(<column>)`` or ``date_trunc('<grain>', <column>)``,
    return ``(granularity, column)``. Otherwise ``None``.

    Also unwraps an outer ``CAST(...)`` — Metabase emits
    ``CAST(TIMESTAMP_TRUNC(col, MONTH) AS DATE)`` when the column is
    DATE-typed (the truncation function widens to TIMESTAMP and Metabase
    casts back). The cast is irrelevant to the semantic time-grain
    classification.
    """
    if isinstance(node, exp.Cast):
        # Unwrap the cast and recurse — the inner expression is what
        # carries the time-grain semantics.
        unwrapped = _detect_time_grain(node.this)
        if unwrapped is not None:
            return unwrapped
    # DEV-1562 / DEV-1558 round-20 follow-up: Metabase emits Sunday-based
    # week truncation as the full wrapper
    # ``CAST((CAST(DATE_TRUNC('week', col + INTERVAL '1 day') AS DATE) +
    # INTERVAL '-1 day') AS DATE)``. Detect the complete pattern as a single
    # match — partial wrappers (just the outer -1d, or just the inner +1d)
    # are NOT Sunday-week and must keep raising as unsupported projections.
    sunday_week = _detect_sunday_week_wrapper(node)
    if sunday_week is not None:
        return sunday_week
    if isinstance(node, exp.Paren):
        recur = _detect_time_grain(node.this)
        if recur is not None:
            return recur
    if isinstance(node, (exp.DateTrunc, exp.TimestampTrunc)):
        match = _detect_time_grain_date_trunc(node)
        if match is not None:
            return match
    single = _detect_time_grain_single_arg(node)
    if single is not None:
        return single
    if isinstance(node, exp.Anonymous):
        return _detect_time_grain_anonymous(node)
    return None


def _alias_for_time_grain(
    grain: TimeGranularity, col: exp.Column,
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> str:
    """The flat projection name we expose for ``month(ordered_at)`` etc.

    Format: ``"<grain>(<column-ref>)"`` lowercased so it round-trips
    cleanly through GROUP BY / ORDER BY equality checks.
    """
    return f"{grain.value}({_column_to_dotted(col, strip_prefix=strip_prefix, alias_map=alias_map)})"


# --- aggregate-call detection (DEV-1486 decision 21) -------------------------


class _AggCall(BaseModel):
    """A recognised SQL aggregate function call in a projection / HAVING."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    agg: str  # SLayer aggregation name: sum/avg/min/max/count/count_distinct
    inner_ref: Optional[str] = None  # dotted column ref, or None for COUNT(*)
    inner_is_column: bool = False  # False → COUNT(*) or non-column arg
    is_count_star: bool = False  # True only for the literal COUNT(*)


def _detect_aggregate(  # NOSONAR(S3776) — flat per-aggregate-kind dispatch; splitting hides the shape
    node: exp.Expression,
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> Optional[_AggCall]:
    """If ``node`` is a SQL aggregate call, classify it; else ``None``.

    ``COUNT(*)`` → ``count`` / ``inner_ref=None``. ``COUNT(DISTINCT col)`` →
    ``count_distinct``. ``COUNT(col)`` → ``count``. ``SUM/AVG/MIN/MAX(col)`` →
    the matching agg. An aggregate over a non-column argument sets
    ``inner_is_column=False`` so the caller can raise the DEV-1493 error.

    DEV-1558 B5: ``strip_prefix`` drops the FROM-table's ``schema.table.``
    qualifier from the inner column ref so ``SUM("public"."orders"."revenue")``
    resolves to the metric ``revenue:sum`` instead of
    ``public.orders.revenue:sum``.

    DEV-1565: ``alias_map`` rewrites join-alias qualifiers (e.g.
    ``AVG("Stores"."tax_rate")``) to the SLayer cross-model dotted form
    (``stores.tax_rate:avg``).
    """
    def _dot(col: exp.Column) -> str:
        return _column_to_dotted(col, strip_prefix=strip_prefix, alias_map=alias_map)
    if isinstance(node, exp.Count):
        inner = node.this
        if isinstance(inner, exp.Star):
            return _AggCall(agg="count", inner_is_column=False, is_count_star=True)
        if isinstance(inner, exp.Distinct):
            exprs = inner.expressions
            if len(exprs) == 1 and isinstance(exprs[0], exp.Column):
                return _AggCall(
                    agg="count_distinct",
                    inner_ref=_dot(exprs[0]),
                    inner_is_column=True,
                )
            return _AggCall(agg="count_distinct", inner_is_column=False)
        if isinstance(inner, exp.Column):
            return _AggCall(
                agg="count", inner_ref=_dot(inner), inner_is_column=True,
            )
        # COUNT(<non-column expression>) — not the row-count star.
        return _AggCall(agg="count", inner_is_column=False)
    for cls, name in _AGG_CLASS_TO_NAME.items():
        if isinstance(node, cls):
            inner = node.this
            if isinstance(inner, exp.Column):
                return _AggCall(
                    agg=name, inner_ref=_dot(inner), inner_is_column=True,
                )
            return _AggCall(agg=name, inner_ref=None, inner_is_column=False)
    return None


def _agg_formula(call: _AggCall) -> str:
    """The SLayer colon-form measure formula for a recognised aggregate."""
    if call.is_count_star:
        return "*:count"
    return f"{call.inner_ref}:{call.agg}"


def _saved_measure_names(table: FacadeTable) -> set[str]:
    """Names of saved ``ModelMeasure`` metrics (formula == name)."""
    return {m.name for m in table.metrics if m.measure_formula == m.name}


def _assert_local_metric(metric: FacadeMetric) -> None:
    """DEV-1567: reject cross-model metric projection at the resolver.

    The catalog pre-expands cross-model metrics into entries with dotted
    names (``customers.row_count``, ``customers.regions.population_sum``).
    The flat-column probes (``pg_attribute`` /
    ``INFORMATION_SCHEMA.COLUMNS``) hide them so BI tools don't discover
    them as projectable, but a hand-written SQL ref still resolves
    through ``metrics_by_name`` / ``metrics_by_formula``. Letting it
    flow into ``SlayerQuery.measures[*].name`` would either trip the
    Pydantic validator (29 errors per query) or — with a non-dotted user
    alias — produce a SlayerQuery whose engine alias mis-keys against
    the engine result (DEV-1448 preserves the cross-model hop path on
    the engine side, so ``orders.cr`` against ``orders.customers.cr``).

    Guard predicate matches the catalog-flatten filter
    (``slayer/facade/catalog.py:local_metrics``); see DEV-1493 for the
    multi-stage rewrite path.
    """
    if "." in metric.name:
        raise TranslationError(
            f"Cross-model metric {metric.name!r} cannot be projected in "
            f"a flat SELECT. Use a saved metric or rewrite as a multi-"
            f"stage query (DEV-1493)."
        )


def _metric_for_aggregate(
    call: _AggCall, table: FacadeTable, metrics_by_formula: Dict[str, FacadeMetric],
) -> FacadeMetric:
    """Resolve an aggregate call to its catalog ``FacadeMetric``.

    The catalog pre-expands every *eligible* column × aggregation into a
    metric, so a successful lookup by colon-form formula simultaneously
    validates column existence + aggregation eligibility. Failures raise
    a clear ``TranslationError`` (DEV-1493 for the measure/expression case).
    """
    # Only the literal COUNT(*) is the row-count star. Any other non-column
    # argument (COUNT(<expr>), SUM(a+b), COUNT(DISTINCT <expr>)) needs a
    # multi-stage rewrite — DEV-1493.
    if not call.is_count_star and not call.inner_is_column:
        raise TranslationError(AGG_OVER_MEASURE_MESSAGE)
    formula = _agg_formula(call)
    metric = metrics_by_formula.get(formula)
    if metric is not None:
        _assert_local_metric(metric)
        return metric
    # Not eligible / unknown. Distinguish the saved-measure case for a
    # clearer message pointing at the follow-up ticket.
    if call.inner_ref is not None and call.inner_ref in _saved_measure_names(table):
        raise TranslationError(AGG_OVER_MEASURE_MESSAGE)
    raise TranslationError(
        f"Aggregate {formula!r} is not available on table {table.name!r} — "
        f"the column may not exist or the aggregation may not be allowed for it."
    )


# --- table resolution --------------------------------------------------------


def _flatten_catalog(catalog: FacadeCatalog) -> Dict[str, List[Tuple[str, FacadeTable]]]:
    """Build a (model_name → [(schema, table), …]) index for bare-name lookup."""
    by_name: Dict[str, List[Tuple[str, FacadeTable]]] = {}
    for sch in catalog.schemas:
        for tbl in sch.tables:
            by_name.setdefault(tbl.name, []).append((sch.name, tbl))
    return by_name


def _unwrap_identifier(node: Optional[exp.Expression]) -> Optional[str]:
    """Pull the string value out of a sqlglot identifier-ish node."""
    if node is None:
        return None
    return str(node.this) if hasattr(node, "this") else str(node)


def _resolve_qualified_table(
    *, schema_str: str, table_name: str, catalog: FacadeCatalog,
) -> Tuple[str, FacadeTable]:
    # Try the exact schema match first — if a catalog actually carries a
    # ``public`` schema (or whatever name the user passed), honour the
    # explicit qualifier.
    for sch in catalog.schemas:
        if sch.name != schema_str:
            continue
        for tbl in sch.tables:
            if tbl.name == table_name:
                return sch.name, tbl
        raise TranslationError(
            f"Unknown table {table_name!r} in schema {schema_str!r}"
        )
    # Pg-facade alias fall-back. The Postgres facade always advertises
    # ``public`` as its single schema (cf. ``pg_namespace`` row); when no
    # real ``public`` schema exists in the catalog (e.g. when the catalog
    # is keyed by the actual datasource name), accept ``public`` as a
    # synonym so Metabase's ``"public"."orders"`` keeps working.
    if schema_str.lower() == "public":
        return _resolve_bare_table(table_name=table_name, catalog=catalog)
    raise TranslationError(f"Unknown schema: {schema_str!r}")


def _resolve_bare_table(
    *, table_name: str, catalog: FacadeCatalog,
) -> Tuple[str, FacadeTable]:
    matches = _flatten_catalog(catalog).get(table_name, [])
    if not matches:
        raise TranslationError(f"Unknown table: {table_name!r}")
    if len(matches) > 1:
        candidates = ", ".join(f"{s}.{t.name}" for s, t in matches)
        raise TranslationError(
            f"Ambiguous table name {table_name!r}; qualify with one of: "
            f"{candidates}"
        )
    return matches[0]


def _resolve_table(
    from_clause: exp.From, catalog: FacadeCatalog,
) -> Tuple[str, FacadeTable]:
    """Resolve a SELECT's FROM into ``(schema_name, FacadeTable)``.

    Handles the three qualification forms (§6.1):

    * ``<catalog>.<schema>.<table>`` — must match ``slayer.<ds>.<model>``.
    * ``<schema>.<table>`` — direct schema lookup.
    * ``<table>`` — searches every schema; unique match → use, multiple →
      error naming the candidates, zero → "Unknown table".
    """
    inner = from_clause.this
    if not isinstance(inner, exp.Table):
        raise TranslationError(
            f"FROM clause must reference a table, got "
            f"{type(inner).__name__}"
        )
    table_name = _unwrap_identifier(inner.this)
    if not table_name:
        raise TranslationError("FROM clause is missing a table name")
    schema_str = _unwrap_identifier(inner.args.get("db"))
    catalog_str = _unwrap_identifier(inner.args.get("catalog"))

    if catalog_str is not None and catalog_str.lower() != CATALOG_NAME.lower():
        raise TranslationError(
            f"Unknown catalog: {catalog_str!r} (only {CATALOG_NAME!r} is exposed)"
        )

    if schema_str is not None:
        return _resolve_qualified_table(
            schema_str=schema_str, table_name=table_name, catalog=catalog,
        )
    return _resolve_bare_table(table_name=table_name, catalog=catalog)


# --- projection translation --------------------------------------------------


class _ProjectionItem(BaseModel):
    """One resolved projection entry."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    projected_name: str  # what the BI tool sees (alias or natural name)
    metric: Optional[FacadeMetric] = None
    dimension: Optional[FacadeDimension] = None
    time_grain: Optional[TimeGranularity] = None
    time_grain_underlying: Optional[FacadeDimension] = None
    # DEV-1566: target type when the projection was CAST(<column> AS <type>).
    # Overrides projection_types[i] at _record_metric / _record_dimension time,
    # leaving engine_alias and the SlayerQuery measure/dimension untouched.
    cast_target: Optional[DataType] = None


def _resolve_time_grain_projection(
    *,
    grain: TimeGranularity,
    col: exp.Column,
    alias_name: Optional[str],
    table: FacadeTable,
    dims_by_name: Dict[str, FacadeDimension],
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> _ProjectionItem:
    dotted = _column_to_dotted(col, strip_prefix=strip_prefix, alias_map=alias_map)
    dim = dims_by_name.get(dotted)
    if dim is None:
        raise TranslationError(
            f"Unknown dimension {dotted!r} inside time-grain "
            f"{grain.value}() on table {table.name!r}"
        )
    if not dim.is_time:
        raise TranslationError(
            f"Dimension {dotted!r} is not a time column; cannot wrap "
            f"in {grain.value}()"
        )
    return _ProjectionItem(
        projected_name=alias_name or _alias_for_time_grain(
            grain, col, strip_prefix=strip_prefix, alias_map=alias_map,
        ),
        dimension=dim,
        time_grain=grain,
        time_grain_underlying=dim,
    )


def _resolve_aggregate_projection(
    *,
    call: _AggCall,
    alias_name: Optional[str],
    table: FacadeTable,
    metrics_by_formula: Dict[str, FacadeMetric],
) -> _ProjectionItem:
    """Map a SQL aggregate call to the same projection item a bare metric
    name would produce (DEV-1486 decision 21)."""
    metric = _metric_for_aggregate(
        call=call, table=table, metrics_by_formula=metrics_by_formula,
    )
    return _ProjectionItem(
        projected_name=alias_name or metric.name,
        metric=metric,
    )


def _resolve_column_projection(
    *,
    body: exp.Column,
    alias_name: Optional[str],
    table: FacadeTable,
    metrics_by_name: Dict[str, FacadeMetric],
    dims_by_name: Dict[str, FacadeDimension],
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> _ProjectionItem:
    dotted = _column_to_dotted(body, strip_prefix=strip_prefix, alias_map=alias_map)
    if dotted in metrics_by_name:
        metric = metrics_by_name[dotted]
        _assert_local_metric(metric)
        return _ProjectionItem(
            projected_name=alias_name or dotted,
            metric=metric,
        )
    if dotted in dims_by_name:
        return _ProjectionItem(
            projected_name=alias_name or dotted,
            dimension=dims_by_name[dotted],
        )
    raise TranslationError(
        f"Unknown projection item {dotted!r} on table {table.name!r}"
    )


def _detect_column_cast(
    body: exp.Expression,
) -> Optional[Tuple[exp.Column, DataType]]:
    """If ``body`` is ``CAST(<bare or qualified Column> AS <supported_type>)``,
    return ``(inner_col, target_data_type)``. Otherwise ``None``.

    The detector REQUIRES ``body.this`` to be an ``exp.Column`` — aggregates,
    hygiene wrappers, function calls, and arithmetic stay outside scope.
    ``exp.TryCast`` is rejected (Postgres has no native TRY_CAST equivalent).
    """
    if not isinstance(body, exp.Cast) or isinstance(body, exp.TryCast):
        return None
    inner = body.this
    if not isinstance(inner, exp.Column):
        return None
    target = _sqlglot_type_to_datatype(body.to)
    if target is None:
        return None
    return inner, target


def _resolve_column_cast_projection(
    *,
    body: exp.Column,
    cast_target: DataType,
    alias_name: Optional[str],
    table: FacadeTable,
    metrics_by_name: Dict[str, FacadeMetric],
    dims_by_name: Dict[str, FacadeDimension],
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> _ProjectionItem:
    """Resolve ``CAST(<column> AS <type>)`` to a projection item that runs
    the bare column through the engine and overrides the wire OID via
    ``cast_target``. Enforces the strict per-pair coercion allowlist.

    DEV-1565: ``alias_map`` carries the LEFT-JOIN subquery's
    ``"Stores"."name"`` → catalog ``stores.name`` rewrite so Metabase-style
    joined CAST refs resolve through the same dimension-lookup path as
    non-casted joined refs.
    """
    inner = _resolve_column_projection(
        body=body, alias_name=alias_name, table=table,
        metrics_by_name=metrics_by_name, dims_by_name=dims_by_name,
        strip_prefix=strip_prefix, alias_map=alias_map,
    )
    source = _item_cast_source_type(inner)
    if not _is_admitted_cast(source=source, target=cast_target):
        offending = exp.Cast(this=body, to=exp.DataType.build(cast_target.value)).sql()
        raise TranslationError(
            f"Unsupported CAST: cannot project {source!s} column as "
            f"{cast_target!s} ({offending!r}). Admitted coercions: see "
            f"docs/interfaces/pg-facade.md."
        )
    return inner.model_copy(update={"cast_target": cast_target})


# DEV-1558 B5: hygiene-scalar wrappers Metabase uses for fingerprint queries.
# Each maps a sqlglot AST class to a human-readable name for the WARNING log.
_HYGIENE_FUNC_CLASSES: Dict[type, str] = {
    exp.Substring: "SUBSTRING",
    exp.Upper: "UPPER",
    exp.Lower: "LOWER",
    exp.Trim: "TRIM",
    exp.Length: "LENGTH",
    # exp.Left / exp.Right exist for some dialects; check at runtime.
}
for _cls_name in ("Left", "Right"):
    _cls = getattr(exp, _cls_name, None)
    if _cls is not None:
        _HYGIENE_FUNC_CLASSES[_cls] = _cls_name.upper()

# Anonymous-form hygiene calls (sqlglot doesn't always lift them to a Func
# subclass) — names match `_function_name_lower` output.
_HYGIENE_ANONYMOUS_NAMES = {"substr", "substring", "left", "right",
                            "upper", "lower", "trim", "length"}


def _detect_hygiene_wrapper(body: exp.Expression) -> Optional[Tuple[str, exp.Column]]:
    """If ``body`` is a hygiene-scalar wrapper around exactly one column
    reference, return ``(printable_func_name, inner_col)``. Otherwise None.
    """
    for cls, name in _HYGIENE_FUNC_CLASSES.items():
        if isinstance(body, cls):
            inner = body.this
            if isinstance(inner, exp.Column):
                return name, inner
            return None
    if isinstance(body, exp.Anonymous):
        fname = str(body.this).lower()
        if fname in _HYGIENE_ANONYMOUS_NAMES:
            args = body.args.get("expressions") or []
            if args and isinstance(args[0], exp.Column):
                return fname.upper(), args[0]
    return None


def _is_fingerprint_shape_wrap(
    inner_col: exp.Column, *, strip_prefix: Optional[Tuple[str, str]],
) -> bool:
    """True iff ``inner_col`` is shaped like Metabase's fingerprint
    projection — a 3-part qualified column reference whose
    ``<schema>.<table>.`` prefix matches the FROM-table prefix.

    The full pattern (e.g. ``SUBSTRING("public"."customers"."name", 1,
    1234)``) is exclusive to Metabase's field-value rescan: hand-written
    SQL like ``LENGTH(name)`` or ``UPPER(orders.status)`` does NOT match
    and stays an error so the user notices the unsupported projection
    instead of silently getting bare column values back.
    """
    if strip_prefix is None:
        return False
    # Count qualifier parts on the column ref: catalog + db + table + leaf.
    qualifier_parts = sum(
        1 for k in ("catalog", "db", "table") if inner_col.args.get(k) is not None
    )
    if qualifier_parts < 2:
        return False  # not a 3-part (or deeper) qualified ref
    # Confirm the leading schema.table prefix matches the FROM table by
    # checking that strip_prefix would actually drop something.
    raw = _raw_column_parts(inner_col)
    stripped = _apply_strip_prefix(list(raw), strip_prefix)
    return len(stripped) < len(raw)


def _raw_column_parts(col: exp.Column) -> List[str]:
    """The qualifier parts plus leaf identifier of ``col``, in order."""
    parts: List[str] = []
    for key in ("catalog", "db", "table"):
        node = col.args.get(key)
        if node is None:
            continue
        parts.append(str(node.this) if hasattr(node, "this") else str(node))
    leaf = col.this
    parts.append(str(leaf.this) if hasattr(leaf, "this") else str(leaf))
    return parts


def _build_projection_lookups(
    table: FacadeTable,
    *,
    extra_dims_by_name: Optional[Dict[str, FacadeDimension]] = None,
    extra_metrics_by_name: Optional[Dict[str, FacadeMetric]] = None,
    extra_metrics_by_formula: Optional[Dict[str, FacadeMetric]] = None,
) -> Tuple[Dict[str, FacadeMetric], Dict[str, FacadeMetric], Dict[str, FacadeDimension]]:
    """Assemble the (metrics_by_name, metrics_by_formula, dims_by_name)
    lookup dicts from the catalog's table metadata, overlaid with the
    DEV-1565 dynamic-join extras when supplied."""
    metrics_by_name = {m.name: m for m in table.metrics}
    metrics_by_formula = {m.measure_formula: m for m in table.metrics}
    dims_by_name = {d.name: d for d in table.dimensions}
    if extra_dims_by_name:
        dims_by_name.update(extra_dims_by_name)
    if extra_metrics_by_name:
        metrics_by_name.update(extra_metrics_by_name)
    if extra_metrics_by_formula:
        metrics_by_formula.update(extra_metrics_by_formula)
    return metrics_by_name, metrics_by_formula, dims_by_name


def _resolve_projection(  # NOSONAR(S3776) — flat dispatch over projection-expression shapes; one branch per shape by design
    expressions: Sequence[exp.Expression], table: FacadeTable,
    *,
    schema_name: Optional[str] = None,
    alias_map: Optional[Dict[str, str]] = None,
    extra_dims_by_name: Optional[Dict[str, FacadeDimension]] = None,
    extra_metrics_by_name: Optional[Dict[str, FacadeMetric]] = None,
    extra_metrics_by_formula: Optional[Dict[str, FacadeMetric]] = None,
    allow_column_cast: bool = True,
) -> List[_ProjectionItem]:
    """Walk the projection list, classifying each item against the table.

    DEV-1565: when a LEFT JOIN to a dynamic-fallback target is in play,
    ``extra_dims_by_name`` / ``extra_metrics_by_*`` carry the materialised
    ``<target>.<col>`` lookups so the joined dotted refs resolve without
    needing a configured catalog join.

    DEV-1566: ``allow_column_cast`` gates the ``CAST(<column> AS <type>)``
    projection branch. The pg-facade passes ``True`` (default) — its wire
    encoders coerce values to the declared OID. The Flight facade passes
    ``False`` because its ``pa.Table.from_pylist`` schema-binding rejects
    values whose Python type doesn't match the declared Arrow type and the
    facade has no value-coercion pass.
    """
    metrics_by_name, metrics_by_formula, dims_by_name = _build_projection_lookups(
        table,
        extra_dims_by_name=extra_dims_by_name,
        extra_metrics_by_name=extra_metrics_by_name,
        extra_metrics_by_formula=extra_metrics_by_formula,
    )
    # DEV-1558 B5: when the FROM was schema-qualified (e.g. "public"."orders"),
    # let column refs that lead with the same `schema.table.` prefix resolve
    # to the bare column.
    strip_prefix: Optional[Tuple[str, str]] = (
        (schema_name, table.name) if schema_name else None
    )

    out: List[_ProjectionItem] = []
    for expr in expressions:
        if isinstance(expr, exp.Star):
            raise TranslationError(SELECT_STAR_MESSAGE)

        alias_name: Optional[str] = None
        body: exp.Expression = expr
        if isinstance(expr, exp.Alias):
            alias_name = str(expr.alias)
            body = expr.this

        grain_match = _detect_time_grain(body)
        if grain_match is not None:
            grain, col = grain_match
            out.append(_resolve_time_grain_projection(
                grain=grain, col=col, alias_name=alias_name,
                table=table, dims_by_name=dims_by_name,
                strip_prefix=strip_prefix, alias_map=alias_map,
            ))
            continue

        agg_call = _detect_aggregate(body, strip_prefix=strip_prefix, alias_map=alias_map)
        if agg_call is not None:
            out.append(_resolve_aggregate_projection(
                call=agg_call, alias_name=alias_name, table=table,
                metrics_by_formula=metrics_by_formula,
            ))
            continue

        # DEV-1566: CAST(<column> AS <type>) projection. Runs AFTER the
        # time-grain unwrap (preserves Metabase's CAST(DATE_TRUNC(...) AS DATE))
        # and AFTER the aggregate detector (CAST(SUM(...) AS T) is out of
        # scope — the detector requires the inner to be a bare Column).
        # Gated by allow_column_cast so the Flight facade falls through to
        # the "Unsupported projection expression" error instead of producing
        # a wire schema its row materialiser can't fulfil.
        if allow_column_cast:
            cast_match = _detect_column_cast(body)
            if cast_match is not None:
                inner_col, cast_target = cast_match
                out.append(_resolve_column_cast_projection(
                    body=inner_col, cast_target=cast_target,
                    alias_name=alias_name, table=table,
                    metrics_by_name=metrics_by_name, dims_by_name=dims_by_name,
                    strip_prefix=strip_prefix, alias_map=alias_map,
                ))
                continue

        if isinstance(body, exp.Column):
            out.append(_resolve_column_projection(
                body=body, alias_name=alias_name, table=table,
                metrics_by_name=metrics_by_name, dims_by_name=dims_by_name,
                strip_prefix=strip_prefix, alias_map=alias_map,
            ))
            continue

        # DEV-1558 B5: hygiene-scalar projection wrappers. Metabase's
        # field-value rescan emits SUBSTRING("public"."customers"."name",
        # 1, 1234) AS "..." — we drop the wrapper and project the bare
        # column under the user's alias.
        hygiene = _detect_hygiene_wrapper(body)
        if hygiene is not None and _is_fingerprint_shape_wrap(
            hygiene[1], strip_prefix=strip_prefix,
        ):
            func_name, inner_col = hygiene
            logger.debug(
                "hygiene-scalar wrapper %r dropped for fingerprint projection "
                "(column %r)",
                func_name, _column_to_dotted(inner_col, strip_prefix=strip_prefix),
            )
            out.append(_resolve_column_projection(
                body=inner_col, alias_name=alias_name, table=table,
                metrics_by_name=metrics_by_name, dims_by_name=dims_by_name,
                strip_prefix=strip_prefix, alias_map=alias_map,
            ))
            continue

        raise TranslationError(
            f"Unsupported projection expression: {body.sql()!r}"
        )
    return out


# --- WHERE translation -------------------------------------------------------


def _split_and_chain(node: exp.Expression) -> List[exp.Expression]:
    """Flatten a top-level AND chain into its conjuncts."""
    out: List[exp.Expression] = []
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, exp.And):
            stack.append(cur.expression)
            stack.append(cur.this)
        else:
            out.append(cur)
    return out


def _lift_time_between(
    conj: exp.Between, time_dim_names: set[str],
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> Optional[Tuple[str, Optional[str], Optional[str]]]:
    col = conj.this
    if not isinstance(col, exp.Column):
        return None
    dotted = _column_to_dotted(col, strip_prefix=strip_prefix, alias_map=alias_map)
    if dotted not in time_dim_names:
        return None
    lo = _literal_str(conj.args.get("low"))
    hi = _literal_str(conj.args.get("high"))
    if lo and hi:
        return dotted, lo, hi
    return None


def _lift_time_comparator(
    conj: exp.Expression, time_dim_names: set[str],
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> Optional[Tuple[str, Optional[str], Optional[str]]]:
    col = conj.this
    if not isinstance(col, exp.Column):
        return None
    dotted = _column_to_dotted(col, strip_prefix=strip_prefix, alias_map=alias_map)
    if dotted not in time_dim_names:
        return None
    val = _literal_str(conj.expression)
    if val is None:
        return None
    if isinstance(conj, (exp.GTE, exp.GT)):
        return dotted, val, None
    return dotted, None, val


def _aggregate_alias_for_column(
    side: exp.Expression,
    items_by_projected_name: Dict[str, "_ProjectionItem"],
    *, strip_prefix: Optional[Tuple[str, str]] = None,
) -> Optional["_ProjectionItem"]:
    """If ``side`` is a column reference whose dotted (strip_prefix-aware)
    name matches an aggregate projection's ``projected_name``, return that
    projection item; else ``None``.

    Used by ``_try_aggregate_alias_filter`` to route Metabase-style
    ``WHERE "<alias>" > <literal>`` and ``HAVING "<alias>" > <literal>``
    conjuncts to colon-form filters (DEV-1568). The ``item.metric`` guard
    keeps dimension projections that happen to share a name with what
    *would* be an aggregate alias from being treated as aggregates.
    """
    if not isinstance(side, exp.Column):
        return None
    name = _column_to_dotted(side, strip_prefix=strip_prefix)
    item = items_by_projected_name.get(name)
    if item is None or item.metric is None:
        return None
    return item


def _try_aggregate_alias_filter(
    conj: exp.Expression,
    items_by_projected_name: Dict[str, "_ProjectionItem"],
    *, strip_prefix: Optional[Tuple[str, str]] = None,
) -> Optional[str]:
    """If ``conj`` is ``<aggregate-alias-col> <cmp> <literal>`` (in either
    order), return the colon-form filter string. If one side matches an
    aggregate alias but the other side is not a literal, raise. Return
    ``None`` if neither side matches an aggregate-alias column ref.

    DEV-1568: Metabase compiles MBQL ``["aggregation", N]`` post-aggregation
    references to alias-bearing SQL — ``WHERE "count" > 1`` for filters and
    ``ORDER BY "count" DESC`` for sorts. This detector rewrites the filter
    shapes to colon-form (``*:count > 1``) so the engine classifies them
    correctly as HAVING; ``_translate_order_by`` handles the ORDER BY side.
    """
    op_sql = _COMPARATOR_SQL.get(type(conj))
    if op_sql is None:
        return None
    lhs, rhs = conj.this, conj.expression
    alias_lhs = _aggregate_alias_for_column(lhs, items_by_projected_name, strip_prefix=strip_prefix)
    alias_rhs = _aggregate_alias_for_column(rhs, items_by_projected_name, strip_prefix=strip_prefix)
    if alias_lhs is None and alias_rhs is None:
        return None
    if alias_lhs is not None and alias_rhs is None and isinstance(rhs, exp.Literal):
        assert alias_lhs.metric is not None
        return f"{alias_lhs.metric.measure_formula} {op_sql} {rhs.sql()}"
    if alias_rhs is not None and alias_lhs is None and isinstance(lhs, exp.Literal):
        assert alias_rhs.metric is not None
        return f"{alias_rhs.metric.measure_formula} {_flip_comparator(op_sql)} {lhs.sql()}"
    raise TranslationError(
        f"Aggregate-alias filter expects a literal on the other side; "
        f"got: {conj.sql()!r}"
    )


def _classify_where_conjunct(
    conj: exp.Expression, time_dim_names: set[str],
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> Tuple[Optional[Tuple[str, Optional[str], Optional[str]]], Optional[str]]:
    """Classify a single conjunct.

    Returns ``((time_dim, date_range_lo, date_range_hi), None)`` if this is
    a time-dim filter that should lift to ``time_dimensions[*].date_range``.
    Returns ``(None, verbatim_sql)`` for the everything-else case.

    DEV-1558 B5: the engine's Mode-B DSL parses ``SlayerQuery.filters``
    and only accepts single-dot dotted paths. Before serialising the
    verbatim fallback, normalise any ``exp.Column`` nodes in the
    predicate via ``strip_prefix`` so a WHERE clause like
    ``"public"."orders"."total" > 0`` lands as ``"total" > 0`` in
    SlayerQuery.filters, not as the original 3-part-qualified form that
    the DSL parser would reject.
    """
    if isinstance(conj, exp.Between):
        lifted = _lift_time_between(
            conj, time_dim_names, strip_prefix=strip_prefix, alias_map=alias_map,
        )
        if lifted is not None:
            return lifted, None
    if isinstance(conj, (exp.GTE, exp.GT, exp.LTE, exp.LT)):
        lifted = _lift_time_comparator(
            conj, time_dim_names, strip_prefix=strip_prefix, alias_map=alias_map,
        )
        if lifted is not None:
            return lifted, None
    normalised = _normalise_predicate_columns(
        conj, strip_prefix=strip_prefix, alias_map=alias_map,
    )
    return None, _rewrite_neq(normalised.sql())


def _normalise_predicate_columns(
    node: exp.Expression,
    *,
    strip_prefix: Optional[Tuple[str, str]],
    alias_map: Optional[Dict[str, str]] = None,
) -> exp.Expression:
    """Walk ``node`` and rewrite every ``exp.Column``: apply ``alias_map``
    first (so ``<JoinAlias>.<col>`` becomes ``<model>.<col>``), then
    ``strip_prefix`` (so the parent table's ``schema.table.`` qualifier
    drops). The reverse order would leave alias-qualified refs untouched
    on parent strip_prefix matches that incidentally collide with the
    alias text."""
    if strip_prefix is None and not alias_map:
        return node

    def rewrite(child: exp.Expression) -> exp.Expression:
        if not isinstance(child, exp.Column):
            return child
        original = _raw_column_parts(child)
        parts = _apply_alias_remap(list(original), alias_map)
        parts = _apply_strip_prefix(parts, strip_prefix)
        if parts == original:
            return child
        # Rebuild the Column from the rewritten parts.
        new = exp.Column(this=exp.Identifier(this=parts[-1], quoted=False))
        if len(parts) >= 2:
            new.set("table", exp.Identifier(this=parts[-2], quoted=False))
        if len(parts) >= 3:
            new.set("db", exp.Identifier(this=parts[-3], quoted=False))
        if len(parts) >= 4:
            new.set("catalog", exp.Identifier(this=parts[-4], quoted=False))
        return new

    return node.transform(rewrite)


def _literal_str(node: Optional[exp.Expression]) -> Optional[str]:
    if node is None:
        return None
    if isinstance(node, exp.Literal):
        return str(node.this)
    return None


def _rewrite_neq(sql: str) -> str:
    """SQL ``!=`` → SLayer DSL ``<>`` (DSL preference per §6.2)."""
    return sql.replace("!=", "<>")


# SQL reserved words Metabase v0.62 uses as unquoted column aliases in its
# captured corpus (e.g. ``AS select``, ``AS update``, ``AS delete`` in the
# table-privileges CTE). sqlglot rejects these without quotes; we
# preprocess by quoting them on a parse-fail retry.
_KEYWORD_ALIAS_QUOTE = re.compile(
    r"\b(AS)\s+(select|update|insert|delete|create|drop|alter|grant|revoke)\b",
    re.IGNORECASE,
)


def _quote_keyword_aliases(sql: str) -> str:
    """Quote unquoted SQL-keyword aliases. ``AS select`` → ``AS "select"``."""
    return _KEYWORD_ALIAS_QUOTE.sub(
        lambda m: f'{m.group(1)} "{m.group(2)}"', sql,
    )


def _parse_with_keyword_alias_fallback(
    sql: str, *, dialect: Optional[str],
) -> exp.Expression:
    """Parse ``sql`` with sqlglot. On failure, retry once with unquoted
    SQL keyword aliases auto-quoted — Metabase's table-privileges CTE
    (corpus #8) uses ``AS select`` / ``AS update`` / ``AS delete`` which
    sqlglot rejects, but Postgres accepts. Raises ``TranslationError``
    with the original parse error if both attempts fail."""
    try:
        return sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.ParseError as primary:
        retry_sql = _quote_keyword_aliases(sql)
        if retry_sql == sql:
            raise TranslationError(f"SQL parse error: {primary}") from primary
        try:
            return sqlglot.parse_one(retry_sql, dialect=dialect)
        except sqlglot.errors.ParseError:
            raise TranslationError(f"SQL parse error: {primary}") from primary


def _apply_where(
    where: Optional[exp.Where],
    time_dims_built: Dict[str, TimeDimension],
    items_by_projected_name: Dict[str, "_ProjectionItem"],
    filters_out: List[str],
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> None:
    """Walk the WHERE chain; route aggregate-alias refs to colon-form
    filters (DEV-1568), lift time-dim filters, append verbatim rest."""
    if where is None:
        return
    time_dim_names = set(time_dims_built.keys())
    for conj in _split_and_chain(where.this):
        colon_form = _try_aggregate_alias_filter(
            conj, items_by_projected_name, strip_prefix=strip_prefix,
        )
        if colon_form is not None:
            filters_out.append(colon_form)
            continue
        lifted, verbatim = _classify_where_conjunct(
            conj, time_dim_names, strip_prefix=strip_prefix, alias_map=alias_map,
        )
        if lifted is not None:
            name, lo, hi = lifted
            td = time_dims_built[name]
            existing = list(td.date_range or [None, None])
            if lo is not None:
                existing[0] = lo
            if hi is not None:
                existing[1] = hi
            td.date_range = existing  # type: ignore[assignment]
        elif verbatim is not None:
            filters_out.append(verbatim)


def _apply_having(
    having: Optional[exp.Having],
    table: FacadeTable,
    items_by_projected_name: Dict[str, "_ProjectionItem"],
    filters_out: List[str],
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
    extra_metrics_by_formula: Optional[Dict[str, FacadeMetric]] = None,
) -> None:
    """Map ``HAVING <agg(col)> <cmp> <literal>`` (DEV-1486 decision 21) and
    ``HAVING <agg-alias-col> <cmp> <literal>`` (DEV-1568) conjuncts to
    colon-form filters. The engine classifies colon-form aggregate filters
    as HAVING.

    DEV-1565: ``extra_metrics_by_formula`` overlay covers dynamic-join
    targets whose metrics aren't yet in the catalog's projection.
    """
    if having is None:
        return
    metrics_by_formula = {m.measure_formula: m for m in table.metrics}
    if extra_metrics_by_formula:
        metrics_by_formula.update(extra_metrics_by_formula)
    for conj in _split_and_chain(having.this):
        # DEV-1568: alias-ref form (``HAVING "rev" > 1000``). Symmetric with
        # the WHERE pre-pass; tries first so it wins before the agg-call form.
        colon_form = _try_aggregate_alias_filter(
            conj, items_by_projected_name, strip_prefix=strip_prefix,
        )
        if colon_form is not None:
            filters_out.append(colon_form)
            continue
        op_sql = _COMPARATOR_SQL.get(type(conj))
        if op_sql is None:
            raise TranslationError(
                f"Unsupported HAVING expression: {conj.sql()!r}; expected "
                f"<aggregate> <comparison> <literal>."
            )
        lhs, rhs = conj.this, conj.expression
        agg_lhs = _detect_aggregate(lhs, strip_prefix=strip_prefix, alias_map=alias_map)
        agg_rhs = _detect_aggregate(rhs, strip_prefix=strip_prefix, alias_map=alias_map)
        if agg_lhs is not None and agg_rhs is None and isinstance(rhs, exp.Literal):
            _metric_for_aggregate(call=agg_lhs, table=table, metrics_by_formula=metrics_by_formula)
            filters_out.append(f"{_agg_formula(agg_lhs)} {op_sql} {rhs.sql()}")
        elif agg_rhs is not None and agg_lhs is None and isinstance(lhs, exp.Literal):
            _metric_for_aggregate(call=agg_rhs, table=table, metrics_by_formula=metrics_by_formula)
            flipped = _flip_comparator(op_sql)
            filters_out.append(f"{_agg_formula(agg_rhs)} {flipped} {lhs.sql()}")
        else:
            raise TranslationError(
                f"Unsupported HAVING expression: {conj.sql()!r}; expected "
                f"exactly one aggregate compared to a literal."
            )


def _flip_comparator(op_sql: str) -> str:
    """Flip a comparator so ``100 < SUM(x)`` becomes ``SUM(x) > 100``."""
    return {">": "<", "<": ">", ">=": "<=", "<=": ">=", "=": "=", "<>": "<>"}[op_sql]


# --- ORDER BY / GROUP BY -----------------------------------------------------


def _item_cast_source_type(item: _ProjectionItem) -> Optional[DataType]:
    """Return the underlying source DataType of a CAST-projected item, or
    None when the source type is unknown (custom metric without a declared
    ``data_type``). Caller MUST gate on ``item.cast_target is not None``
    before calling this — the helper's None return is intentionally
    overloaded with the unknown-source case so the lossy-pair check can
    treat unknown-source casts as lossy by default."""
    if item.metric is not None:
        return item.metric.data_type
    if item.dimension is not None:
        return item.dimension.data_type
    return None


def _cast_pair_is_lossy(
    *, source: Optional[DataType], target: DataType,
    lossy_pairs: frozenset,
) -> bool:
    """A cast is lossy when either: (a) the source is known and (source,
    target) is in ``lossy_pairs``, or (b) the source is unknown — the only
    admitted unknown-source target is TEXT (see ``_is_admitted_cast``)
    which is always lossy for ORDER BY (lex vs engine's natural sort).
    For GROUP BY the unknown-source path is admitted because the engine's
    bare-value grouping preserves whatever per-value distinction TEXT
    would expose."""
    if source is None:
        return target is DataType.TEXT
    return (source, target) in lossy_pairs


def _lossy_cast_error_message(
    *, kind: str, name: str, source: Optional[DataType], target: DataType,
) -> str:
    """One-line user-facing message for the lossy-CAST-pair rejection. The
    ``kind`` is ``ORDER BY`` or ``GROUP BY``; ``name`` is the user-visible
    identifier the message points at (typically the alias). ``source`` is
    None when the underlying metric/dimension has no declared data_type
    (custom metric path), which is treated as lossy by default."""
    source_str = str(source) if source is not None else "unknown"
    return (
        f"{kind} on CAST projection {name!r} with lossy pair "
        f"{source_str}→{target!s} is unsupported: the engine query projects "
        f"the bare column, so the underlying sort/group semantics don't "
        f"match the casted type. Use the bare column, or wait for engine-"
        f"side CAST pushdown."
    )


def _reject_lossy_cast_or_pass(
    *, kind: str, name: str, item: _ProjectionItem, lossy_pairs: frozenset,
) -> None:
    """If ``item`` is a CAST projection AND its (source, target) pair is
    lossy under bare-column ORDER BY / GROUP BY semantics, raise. Otherwise
    no-op. Unknown-source casts (custom metrics without a declared
    data_type) are admitted as None→TEXT only; ``_cast_pair_is_lossy``
    treats that case as lossy for ORDER BY by default."""
    if item.cast_target is None:
        return
    source = _item_cast_source_type(item)
    if not _cast_pair_is_lossy(
        source=source, target=item.cast_target, lossy_pairs=lossy_pairs,
    ):
        return
    raise TranslationError(_lossy_cast_error_message(
        kind=kind, name=name, source=source, target=item.cast_target,
    ))


def _resolve_order_by_item(
    body: exp.Expression,
    item_by_projected_name: Dict[str, _ProjectionItem],
    metric_item_by_formula: Dict[str, _ProjectionItem],
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> Tuple[_ProjectionItem, str]:
    """Look up the projection item an ORDER BY term resolves to.

    Tries the bare-name lookup first (alias or canonical metric name from
    ``_order_by_name``); on miss, DEV-1568 fallback maps a literal aggregate
    call (``ORDER BY SUM(revenue)``) to the projection registered under a
    different alias (``SELECT SUM(revenue) AS "rev"``) by matching on the
    aggregate's canonical ``measure_formula``. Returns ``(item, name)`` so
    the DEV-1566 lossy-CAST check downstream has both the resolved item and
    the user-visible name for error messaging. Raises ``TranslationError``
    if neither path resolves.
    """
    name = _order_by_name(body, strip_prefix=strip_prefix, alias_map=alias_map)
    item = item_by_projected_name.get(name)
    if item is None:
        agg = _detect_aggregate(body, strip_prefix=strip_prefix, alias_map=alias_map)
        if agg is not None:
            item = metric_item_by_formula.get(_agg_formula(agg))
    if item is None:
        raise TranslationError(
            f"ORDER BY column {name!r} is not in the projection list"
        )
    return item, name


def _translate_order_by(
    order: Optional[exp.Order],
    item_by_projected_name: Dict[str, _ProjectionItem],
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> List[OrderItem]:
    if order is None:
        return []
    # DEV-1568: when a SELECT aliases an aggregate (``SUM(revenue) AS "rev"``)
    # and the ORDER BY repeats the call literally (``ORDER BY SUM(revenue)``),
    # ``_order_by_name`` returns the canonical metric name (``"revenue_sum"``)
    # — but ``item_by_projected_name`` is keyed on the user alias (``"rev"``).
    # Build a fallback map keyed by ``metric.measure_formula`` so a repeated
    # aggregate call resolves to the same projection the alias does.
    metric_item_by_formula: Dict[str, _ProjectionItem] = {
        item.metric.measure_formula: item
        for item in item_by_projected_name.values()
        if item.metric is not None
    }
    out: List[OrderItem] = []
    for ord_expr in order.args.get("expressions") or []:
        if not isinstance(ord_expr, exp.Ordered):
            continue
        direction = "desc" if ord_expr.args.get("desc") else "asc"
        item, name = _resolve_order_by_item(
            ord_expr.this, item_by_projected_name, metric_item_by_formula,
            strip_prefix=strip_prefix, alias_map=alias_map,
        )
        _reject_lossy_cast_or_pass(
            kind="ORDER BY", name=name, item=item,
            lossy_pairs=_LOSSY_ORDER_BY_CAST_PAIRS,
        )
        if item.metric is not None:
            # DEV-1568: use the projection's SELECT alias, not the catalog
            # ``FacadeMetric.name``. ``_record_metric`` registered the SLayer
            # measure under ``projected_name``, so the OrderItem must point
            # at the same name. Pre-fix the engine saw ``ColumnRef("row_count")``
            # against a measure named ``"count"`` and DuckDB rejected with
            # ``Referenced column "orders.row_count" not found``.
            ref = ColumnRef(name=item.projected_name)
        else:
            assert item.dimension is not None
            ref = ColumnRef.from_string(item.dimension.dimension_ref)
        out.append(OrderItem(column=ref, direction=direction))
    return out


def _order_by_name(
    body: exp.Expression,
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> str:
    """Resolve an ORDER BY term to its projected name.

    A bare column / alias resolves by name. An aggregate term
    (``ORDER BY SUM(amount)``) resolves to the same canonical metric name
    a bare ``amount_sum`` projection would, so it matches the projection
    item registered for ``SUM(amount)``.
    """
    if isinstance(body, exp.Column):
        return _column_to_dotted(body, strip_prefix=strip_prefix, alias_map=alias_map)
    agg = _detect_aggregate(body, strip_prefix=strip_prefix, alias_map=alias_map)
    if agg is not None and agg.inner_is_column:
        return f"{agg.inner_ref}_{agg.agg}"
    if agg is not None and agg.is_count_star:
        return "row_count"
    grain_match = _detect_time_grain(body)
    if grain_match is not None:
        grain, col = grain_match
        return _alias_for_time_grain(grain, col, strip_prefix=strip_prefix, alias_map=alias_map)
    return body.sql()


def _validate_group_by(  # NOSONAR(S3776) — single GROUP BY validation pass; extraction adds indirection
    group: Optional[exp.Group],
    derived: List[str],
    item_by_projected_name: Dict[str, _ProjectionItem],
    *,
    strip_prefix: Optional[Tuple[str, str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> None:
    """Apply the strict-on-extras / lenient-on-omissions policy (§6.1).

    ``item_by_projected_name`` is consulted only to look up the projection
    item behind a user-facing name so the DEV-1566 lossy-CAST-pair rejection
    can fire. Membership validation still runs against the ``derived`` list
    (which carries time-grain canonical forms in addition to the projected
    names) to preserve the existing strict-on-extras behaviour.
    """
    if group is None:
        return
    derived_set = set(derived)
    user_items: List[str] = []
    for g in group.args.get("expressions") or []:
        if isinstance(g, exp.Column):
            user_items.append(_column_to_dotted(g, strip_prefix=strip_prefix, alias_map=alias_map))
        else:
            grain_match = _detect_time_grain(g)
            if grain_match is not None:
                grain, col = grain_match
                user_items.append(_alias_for_time_grain(
                    grain, col, strip_prefix=strip_prefix, alias_map=alias_map,
                ))
                continue
            user_items.append(g.sql())
    for u in user_items:
        if u not in derived_set:
            # GROUP BY positional refs (GROUP BY 1) and aggregate terms are
            # tolerated — the dimension set is derived from the projection,
            # so a positional / aggregate GROUP BY adds nothing to validate.
            if _is_ignorable_group_item(u):
                continue
            raise TranslationError(
                f"GROUP BY item {u!r} is not in the projection's derived "
                f"dimension set ({sorted(derived_set)})"
            )
        # DEV-1566 — Codex round 3: reject GROUP BY on CAST projections
        # whose (source, target) pair is lossy under bare-column grouping.
        # Unknown-source casts (only admitted as None→TEXT) are admitted
        # here — the engine's per-value grouping already collapses to the
        # same set of TEXT representations.
        item = item_by_projected_name.get(u)
        if item is None:
            continue
        _reject_lossy_cast_or_pass(
            kind="GROUP BY", name=u, item=item,
            lossy_pairs=_LOSSY_GROUP_BY_CAST_PAIRS,
        )


def _is_ignorable_group_item(item: str) -> bool:
    """``GROUP BY 1`` (positional) is ignorable; the derived dim set already
    captures the grouping."""
    return item.isdigit()


# --- main entry point --------------------------------------------------------


def _is_start_transaction(node: exp.Expression) -> bool:
    """`START TRANSACTION` parses oddly: sqlglot sees `START` as a column and
    `TRANSACTION` as an alias. Match that pattern explicitly."""
    if not isinstance(node, exp.Alias):
        return False
    body = node.this
    if not isinstance(body, exp.Column):
        return False
    body_name = (
        str(body.this.this) if hasattr(body.this, "this") else str(body.this)
    ).upper()
    alias_name = str(node.alias).upper()
    return body_name == "START" and alias_name == "TRANSACTION"


def _extract_set_setting(parsed: exp.Set) -> Optional[SetSettingOp]:
    """Extract a ``SetSettingOp`` from a clean ``SET <name> = <value>`` /
    ``SET <name> TO <value>`` AST. Returns ``None`` for multi-item SETs or
    SetItem shapes whose body isn't a single ``EQ(Column(Identifier), rhs)``
    pair — those are silently no-op'd by the caller (DEV-1569).

    DEV-1569 / Codex round 2 F2: dotted custom names (``SET myapp.user_id
    = '42'``) parse as ``Column(table='myapp', name='user_id')``;
    reconstruct the dotted form so ``SHOW myapp.user_id`` round-trips.
    """
    items = parsed.args.get("expressions") or []
    if len(items) != 1:
        return None
    item = items[0]
    body = item.this if hasattr(item, "this") else None
    if not isinstance(body, exp.EQ):
        return None
    lhs = body.this
    rhs = body.expression
    if not isinstance(lhs, exp.Column):
        return None
    # LHS is the setting name — Postgres GUC names are case-insensitive; we
    # normalise to lowercase. For multi-part `a.b.c.name` shapes (custom
    # GUCs like `myapp.user_id` or `my.app.user_id`) reconstruct the full
    # dotted form via lhs.parts (Codex round 4 F2). Bare names have a
    # single part and produce the same string as `lhs.name`.
    name = ".".join(part.name for part in lhs.parts).lower()
    value = _extract_setting_value(rhs)
    if value is None:
        return None
    return SetSettingOp(name=name, value=value)


def _extract_setting_value(rhs: Optional[exp.Expression]) -> Optional[str]:
    """Return the string value of a ``SET`` rhs node, robust across the
    parser's literal vs. identifier vs. variable encodings:

    - ``Literal(this='UTF8', is_string=True)`` → ``'UTF8'``
    - ``Var(this='UTF8')``                      → ``'UTF8'`` (unquoted form
       pgjdbc emits for `SET client_encoding TO UTF8`)
    - ``Var(this='DEFAULT')``                   → ``'DEFAULT'`` (treated as
       literal string; users wanting reset semantics use ``RESET``)
    - ``Column`` / ``Identifier``               → name
    - ``Cast(this=<literal-like>)``             → recurse one level (Codex
       round 4 F3: after extended-protocol bind substitution the rhs may
       arrive as ``'foo'::text``)

    Returns ``None`` for shapes we don't recognise so the caller can decide
    to silently no-op the SET rather than store a malformed value.
    """
    if rhs is None:
        return None
    if isinstance(rhs, exp.Cast):
        return _extract_setting_value(rhs.this)
    # Signed numeric literals — `SET extra_float_digits = -1` parses as
    # `Neg(Literal(1))` (Codex round 5 F2). Recurse and prefix the sign.
    if isinstance(rhs, exp.Neg):
        inner = _extract_setting_value(rhs.this)
        return f"-{inner}" if inner is not None else None
    if isinstance(rhs, exp.Literal):
        return str(rhs.this)
    if isinstance(rhs, exp.Var):
        return str(rhs.this)
    if isinstance(rhs, exp.Identifier):
        return str(rhs.this)
    if isinstance(rhs, exp.Column):
        return rhs.name
    return None


def _extract_reset_setting(parsed: exp.Command) -> Optional[ResetSettingOp]:
    """Extract ``RESET <name>`` or ``RESET ALL`` intent from an ``exp.Command``
    root. Returns ``None`` for the bare ``RESET`` spelling — defensive
    fallback (drivers don't emit this in practice).

    Multi-word names (``RESET TIME ZONE`` / ``RESET SESSION AUTHORIZATION``)
    are returned with internal whitespace preserved and lowercased; the
    Postgres facade's ``_apply_reset_setting`` alias-resolves them via the
    same ``_SHOW_ALIASES`` table that ``SHOW`` consults.
    """
    expr = parsed.expression
    if expr is None:
        return None
    # Mirror the pattern used by ``_show_setting_name`` for robustness across
    # sqlglot's various expression shapes.
    raw = expr.this if hasattr(expr, "this") else expr
    name = str(raw).strip().strip("'\"")
    if name.upper() == "ALL":
        return ResetSettingOp(reset_all=True)
    # Collapse multi-word names into a single-space lowercase form so the
    # facade can apply the alias table (`"TIME ZONE"` → `"time zone"` →
    # `"timezone"`).
    name = re.sub(r"\s+", " ", name).lower()
    return ResetSettingOp(name=name)


# Single-identifier-or-dotted-chain validation regex (single quantifier on
# identifier chars; the optional dotted-tail uses one bounded `*` segment —
# safe from ReDoS / S5852). Used below to validate the name half of a
# Command-form SET after the value has been peeled off by plain string ops.
# Accepts both `application_name` and `myapp.user_id` shapes to stay
# consistent with the exp.Set path's lhs.parts reconstruction (Codex
# round 5 F3).
_COMMAND_SET_NAME_RE = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*")


def _command_expression_text(expr) -> str:
    """Extract raw text from a Command's ``expression`` slot (which sqlglot
    sometimes carries as a bare ``str`` and sometimes as a node)."""
    if isinstance(expr, str):
        return expr
    if hasattr(expr, "this"):
        return str(expr.this)
    return str(expr)


def _extract_command_form_set(parsed: exp.Command) -> Optional[SetSettingOp]:
    """For an ``exp.Command(this='SET', expression='<raw>')`` root, try to
    parse the raw expression text into a ``(name, value)`` pair.

    Returns ``None`` for shapes the spec cuts: multi-word names
    (``SET TIME ZONE 'UTC'``), ``SET SESSION CHARACTERISTICS …``, no
    value, etc. Implementation uses plain string operations rather than
    a multi-segment regex to keep clear of catastrophic backtracking
    (Sonar S5852 / DEV-1569 round 2).
    """
    expr = parsed.expression
    if expr is None:
        return None
    raw = _command_expression_text(expr).strip().rstrip(";").strip()
    if not raw:
        return None
    # Find the first separator on the original raw so internal whitespace
    # inside captured values is preserved (Codex round 4 F1). `=` wins over
    # ` TO ` so `SET x = TO_DATE(...)` never accidentally splits on TO.
    # The TO match uses `\sTO\s` with case-insensitive matching: each `\s`
    # is a single-char (bounded) match — no ReDoS risk under S5852, and it
    # accepts tab / newline separators the round-2 regex did.
    eq_idx = raw.find("=")
    if eq_idx != -1:
        name = raw[:eq_idx].strip()
        value = raw[eq_idx + 1:].strip()
    else:
        to_match = re.search(r"\sTO\s", raw, flags=re.IGNORECASE)
        if to_match is None:
            return None
        name = raw[:to_match.start()].strip()
        value = raw[to_match.end():].strip()
    if not name or not value:
        return None
    if not _COMMAND_SET_NAME_RE.fullmatch(name):
        return None
    return SetSettingOp(name=name.lower(), value=value)


def _classify_noop_root(parsed: exp.Expression) -> Optional[NoOpResult]:
    """Classify SET/SHOW/BEGIN/COMMIT/ROLLBACK roots into a NoOpResult with a
    facade-neutral ``command_tag``; ``None`` if not a no-op root.

    DEV-1569: also extracts the parsed-out ``SET <name> = <value>`` and
    ``RESET <name>`` / ``RESET ALL`` shapes onto ``set_setting`` /
    ``reset_setting`` for the Postgres facade to apply per-connection.
    """
    if isinstance(parsed, exp.Transaction):
        return NoOpResult(command_tag="BEGIN")
    if isinstance(parsed, exp.Commit):
        return NoOpResult(command_tag="COMMIT")
    if isinstance(parsed, exp.Rollback):
        return NoOpResult(command_tag="ROLLBACK")
    if isinstance(parsed, exp.Set):
        return NoOpResult(
            command_tag="SET", set_setting=_extract_set_setting(parsed),
        )
    if _is_start_transaction(parsed):
        return NoOpResult(command_tag="START TRANSACTION")
    if isinstance(parsed, exp.Command):
        verb = str(parsed.this).upper() if parsed.this else ""
        # "SET" covers spellings sqlglot cannot parse into exp.Set, e.g.
        # pgjdbc's setTransactionIsolation() emitting `SET SESSION
        # CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...`. We
        # acknowledge but do not capture (set_setting stays None) since
        # the rhs isn't a clean (name, value) pair.
        if verb == "RESET":
            return NoOpResult(
                command_tag="RESET",
                reset_setting=_extract_reset_setting(parsed),
            )
        if verb == "SET":
            # DEV-1569 / Codex F1: also try to extract a (name, value) pair
            # from the Command-form fallback so spellings like
            # `SET search_path = public, extensions` round-trip through
            # SHOW. Multi-word names (`SET TIME ZONE 'UTC'`) and
            # session-characteristics spellings deliberately don't match
            # the regex and remain silent no-ops.
            return NoOpResult(
                command_tag="SET",
                set_setting=_extract_command_form_set(parsed),
            )
        if verb in {"SHOW", "USE"}:
            return NoOpResult(command_tag=verb)
    return None


def _unwrap_probe(
    probe: "Union[RowBatch, ProbeMatcherOutcome]",
) -> ProbeResult:
    """Normalise a probe matcher's return value into a ``ProbeResult``.

    ``ProbeMatcher`` may return either a bare ``RowBatch`` (Flight default)
    or a ``ProbeMatcherOutcome`` (Postgres facade, carrying an optional
    session-setting mutation hint from ``set_config``). Both shapes
    collapse into ``ProbeResult`` here so the dispatcher's branch count
    stays low (DEV-1569 / Sonar S3776).
    """
    if isinstance(probe, ProbeMatcherOutcome):
        return ProbeResult(
            batch=probe.batch, settings_mutation=probe.settings_mutation,
        )
    return ProbeResult(batch=probe)


def translate(
    sql: str,
    catalog: FacadeCatalog,
    *,
    dialect: Optional[str] = None,
    probe_matcher: Optional[ProbeMatcher] = None,
    catalog_sql_executor: (
        "Optional[CatalogSqlExecutorProtocol | Callable[[], CatalogSqlExecutorProtocol]]"
    ) = None,
    allow_column_cast: bool = True,
) -> TranslatorResult:
    """Translate a SQL string into a TranslatorResult.

    ``dialect`` is passed to the sqlglot parser only. ``probe_matcher``
    overrides the default Flight probe whitelist (the Postgres facade
    injects a datasource-aware one). ``catalog_sql_executor`` routes
    catalog SQL through an in-memory DuckDB (the Postgres facade does
    this; Flight passes ``None`` and falls back to ``match_info_schema``).

    Raises ``TranslationError`` on user-visible failures.
    """
    token = _IN_FACADE_PARSE.set(True)
    try:
        parsed = _parse_with_keyword_alias_fallback(sql, dialect=dialect)
    finally:
        _IN_FACADE_PARSE.reset(token)

    # Step 2 — probe-query whitelist (runs first so facade-specific probes,
    # e.g. SHOW for Postgres, win before generic root classification).
    pm = probe_matcher or match_probe
    probe = pm(parsed)
    if probe is not None:
        return _unwrap_probe(probe)

    # Step 3 — AST root classification.
    if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete, exp.Merge,
                            exp.TruncateTable)):
        raise TranslationError(READ_ONLY_MESSAGE)
    if isinstance(parsed, (exp.Create, exp.Drop, exp.Alter)):
        raise TranslationError(READ_ONLY_MESSAGE)
    noop = _classify_noop_root(parsed)
    if noop is not None:
        return noop

    # Step 4 — DuckDB catalog executor (Postgres facade) OR info-schema
    # dispatch (Flight facade). The executor handles BOTH pg_catalog AND
    # information_schema queries via materialised tables; when it's not
    # provided we keep the canned info-schema answer for Flight.
    #
    # This check runs BEFORE the "must be exp.Select" gate so catalog
    # queries that aren't a plain Select — UNION/UNION ALL (Metabase
    # corpus #12), set-ops, WITH-only constructs — route to the
    # executor when every Table node resolves to a catalog relation.
    # Non-catalog Selects continue to the SLayer-table translation
    # below; non-catalog UNIONs etc. surface the unsupported-statement
    # error from the gate.
    if catalog_sql_executor is not None:
        from slayer.facade.catalog_sql import is_catalog_only
        if is_catalog_only(parsed):
            # ``catalog_sql_executor`` accepts either the executor itself
            # or a zero-arg factory — lazy construction lets the pg
            # facade skip the DuckDB materialisation cost on
            # non-catalog (model) queries (Codex round 16). Resolve the
            # factory only inside this branch.
            executor = (
                catalog_sql_executor()
                if callable(catalog_sql_executor)
                else catalog_sql_executor
            )
            return PgCatalogResult(batch=executor.execute(parsed=parsed, sql=sql))
    elif isinstance(parsed, exp.Select):
        info = match_info_schema(parsed=parsed, catalog=catalog)
        if info is not None:
            return InfoSchemaResult(batch=info)

    if not isinstance(parsed, exp.Select):
        raise TranslationError(
            f"Unsupported statement: {type(parsed).__name__}"
        )

    # Step 5 / 6 — SLayer-table translation.
    return _translate_slayer_select(
        parsed, catalog, allow_column_cast=allow_column_cast,
    )


# Lightweight Protocol so the translator doesn't pull catalog_sql at import
# time (which would create a duckdb-at-import dependency for Flight).
class CatalogSqlExecutorProtocol:
    """Protocol the catalog SQL executor must satisfy. Defined here so
    ``translate`` can type-annotate its parameter without importing
    catalog_sql (which imports duckdb)."""

    def execute(self, *, parsed: exp.Expression, sql: str) -> RowBatch:
        raise NotImplementedError


class _ProjectionPlan(BaseModel):
    """Pieces of a SlayerQuery derived from the SELECT projection."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    measures: List[dict]
    dimension_refs: List[ColumnRef]
    time_dims: List[TimeDimension]
    time_dim_by_name: Dict[str, TimeDimension]
    derived_dims: List[str]
    column_name_mapping: List[Tuple[str, str]]
    projection_types: List[Optional[DataType]]


def _record_metric(
    *, plan: _ProjectionPlan, item: _ProjectionItem, table: FacadeTable,
) -> None:
    assert item.metric is not None
    plan.measures.append({
        "formula": item.metric.measure_formula,
        "name": item.projected_name,
    })
    engine_alias = f"{table.name}.{item.projected_name}"
    plan.column_name_mapping.append((engine_alias, item.projected_name))
    # DEV-1566: CAST(<metric_ref> AS T) overrides the declared metric type.
    plan.projection_types.append(item.cast_target or item.metric.data_type)


def _record_time_grain(
    *, plan: _ProjectionPlan, item: _ProjectionItem, table: FacadeTable,
) -> None:
    assert item.time_grain is not None and item.time_grain_underlying is not None
    dotted = item.time_grain_underlying.dimension_ref
    td = TimeDimension(
        dimension={"name": dotted},
        granularity=item.time_grain,
    )
    plan.time_dims.append(td)
    plan.time_dim_by_name[dotted] = td
    plan.derived_dims.append(item.projected_name)
    # When the projection aliases the time-grain expression (Metabase emits
    # ``SELECT CAST(DATE_TRUNC('month', ordered_at) AS DATE) AS "ordered_at"``
    # together with ``GROUP BY CAST(DATE_TRUNC('month', ordered_at) AS DATE)``)
    # the GROUP BY validator computes the canonical ``month(ordered_at)`` form
    # for the unaliased GROUP BY expression. Register both forms so either one
    # validates against the projection's derived dimension set.
    canonical = f"{item.time_grain.value}({dotted})"
    if canonical != item.projected_name:
        plan.derived_dims.append(canonical)
    engine_alias = f"{table.name}.{dotted}"
    plan.column_name_mapping.append((engine_alias, item.projected_name))
    plan.projection_types.append(item.time_grain_underlying.data_type)


def _record_dimension(
    *, plan: _ProjectionPlan, item: _ProjectionItem, table: FacadeTable,
) -> None:
    assert item.dimension is not None
    plan.dimension_refs.append(ColumnRef.from_string(item.dimension.dimension_ref))
    plan.derived_dims.append(item.projected_name)
    # DEV-1565 (mirrors the time-grain canonical-alias trick): when the
    # projection aliases a joined-col dim (e.g. ``"Stores"."name" AS
    # "Stores__name"``) the GROUP BY / ORDER BY may reference the alias-
    # qualified form, which alias-remap rewrites to the dotted SLayer
    # form (``stores.name``). Register that form too so validation finds it.
    if item.dimension.dimension_ref != item.projected_name:
        plan.derived_dims.append(item.dimension.dimension_ref)
    engine_alias = f"{table.name}.{item.dimension.dimension_ref}"
    plan.column_name_mapping.append((engine_alias, item.projected_name))
    # DEV-1566: CAST(<dim_ref> AS T) overrides the declared dim type.
    plan.projection_types.append(item.cast_target or item.dimension.data_type)


def _build_projection_plan(
    items: Sequence[_ProjectionItem], table: FacadeTable,
) -> _ProjectionPlan:
    plan = _ProjectionPlan(
        measures=[], dimension_refs=[], time_dims=[], time_dim_by_name={},
        derived_dims=[], column_name_mapping=[], projection_types=[],
    )
    for item in items:
        if item.metric is not None:
            _record_metric(plan=plan, item=item, table=table)
        elif item.time_grain is not None:
            _record_time_grain(plan=plan, item=item, table=table)
        else:
            _record_dimension(plan=plan, item=item, table=table)
    return plan


def _index_items_by_canonical_form(
    items: Sequence[_ProjectionItem],
) -> Dict[str, _ProjectionItem]:
    """Index projection items by alias plus canonical forms (time-grain only).

    Time-grain items are also keyed by ``grain(col)`` so an unaliased
    Metabase-style GROUP BY / ORDER BY (``ORDER BY CAST(DATE_TRUNC('month',
    ordered_at) AS DATE)``) resolves against the aliased projection.

    Column-CAST items are intentionally NOT registered here: DEV-1566's CAST
    projection is a wire-type override that leaves the engine query
    projecting the bare column, so ``ORDER BY CAST(col AS T)`` would sort by
    the engine column's natural type (numeric/temporal) instead of the
    casted type's semantics (e.g. lex for TEXT), and
    ``GROUP BY CAST(ts AS DATE)`` would group per-timestamp instead of
    per-date. Both are silently wrong. We reject these queries with the
    existing "ORDER BY column not in projection list" / GROUP BY
    strict-on-extras error and the user aliases the CAST projection to
    reference it (``SELECT CAST(c AS T) AS x ... ORDER BY x``).
    """
    by_name: Dict[str, _ProjectionItem] = {item.projected_name: item for item in items}
    for item in items:
        if item.time_grain is not None and item.time_grain_underlying is not None:
            canonical = (
                f"{item.time_grain.value}({item.time_grain_underlying.dimension_ref})"
            )
            by_name.setdefault(canonical, item)
    return by_name


def _parse_int_literal(node: Optional[exp.Expression]) -> Optional[int]:
    """Pull an int out of ``LIMIT N`` / ``OFFSET N`` style nodes."""
    if node is None or not isinstance(node.expression, exp.Literal):
        return None
    try:
        return int(str(node.expression.this))
    except ValueError:
        return None


# --- DEV-1565: LEFT JOIN-with-subquery recognition ---------------------------


class _JoinPlan(BaseModel):
    """Result of recognising Metabase's single LEFT-JOIN-with-subquery shape."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    alias: str
    target_table: FacadeTable
    target_schema: str
    source_col: str
    target_col: str
    is_dynamic: bool
    warn_cardinality: bool = False


def _parse_left_join(
    *,
    parsed_joins: List[exp.Join],
    parent_table: FacadeTable,
    catalog: FacadeCatalog,
) -> Optional[_JoinPlan]:
    """Validate and parse the single LEFT JOIN on the SELECT, if any.

    Returns ``None`` when no joins are present. Raises ``TranslationError``
    for any shape outside the Phase-1 scope (multiple joins, non-LEFT type,
    bare-table right side, exotic subquery body, malformed ON clause)."""
    if not parsed_joins:
        return None
    if len(parsed_joins) > 1:
        raise TranslationError(
            f"Multiple JOINs in one query are not supported (Phase 1 — "
            f"DEV-1565); got {len(parsed_joins)}. Use one LEFT JOIN."
        )
    join = parsed_joins[0]
    _reject_non_left_join(join)
    target_table, target_schema, alias = _resolve_join_subquery_target(join, catalog)
    source_col, target_col = _parse_on_clause(
        on=join.args.get("on"),
        parent_table=parent_table,
        target_table=target_table,
        alias=alias,
    )
    is_dynamic, warn_cardinality = _classify_against_parent_joins(
        parent_table=parent_table,
        target_name=target_table.name,
        source_col=source_col,
        target_col=target_col,
    )
    return _JoinPlan(
        alias=alias,
        target_table=target_table,
        target_schema=target_schema,
        source_col=source_col,
        target_col=target_col,
        is_dynamic=is_dynamic,
        warn_cardinality=warn_cardinality,
    )


def _reject_non_left_join(join: exp.Join) -> None:
    """Phase 1 accepts only LEFT JOIN (sqlglot: side='LEFT', kind in (None,
    'OUTER')). Every other shape — INNER / RIGHT / FULL / CROSS / plain
    JOIN — is rejected with a single error surface."""
    side = join.args.get("side")
    kind = join.args.get("kind")
    side_upper = (side or "").upper()
    kind_upper = (kind or "").upper()
    if side_upper == "LEFT" and kind_upper in ("", "OUTER"):
        return
    raise TranslationError(
        f"Only LEFT JOIN is supported (Phase 1 — DEV-1565); got "
        f"{(side_upper + ' ' + kind_upper).strip() or 'JOIN'}."
    )


def _resolve_join_subquery_target(
    join: exp.Join, catalog: FacadeCatalog,
) -> Tuple[FacadeTable, str, str]:
    """Validate the right-side subquery shape and resolve its FROM table.

    Returns ``(target_facade_table, target_schema, join_alias)``. Raises
    on bare-table right side, non-Select subquery body, missing FROM,
    inner WHERE/HAVING/GROUP/JOIN/CTE."""
    right = join.this
    if not isinstance(right, exp.Subquery):
        raise TranslationError(
            "LEFT JOIN right side must be a subquery '(SELECT … FROM "
            "<table>) AS <alias>' (Phase 1 — DEV-1565); got a bare table "
            "reference."
        )
    alias = right.alias
    if not alias:
        raise TranslationError(
            "LEFT JOIN subquery must have an alias: '(SELECT … FROM "
            "<table>) AS <alias>'."
        )
    inner = right.this
    if not isinstance(inner, exp.Select):
        raise TranslationError(
            "LEFT JOIN subquery body must be a SELECT statement (Phase 1 "
            "— DEV-1565); set-ops (UNION/INTERSECT/EXCEPT) not accepted."
        )
    for forbidden, label in (
        ("where", "WHERE"),
        ("group", "GROUP BY"),
        ("having", "HAVING"),
        ("joins", "JOIN"),
        ("with_", "WITH (CTE)"),
        # DEV-1565 (Codex round 1+2): DISTINCT / LIMIT / OFFSET inside
        # the subquery change the joined row set in ways the translator
        # would silently drop (the configured/dynamic join treats the
        # right side as the full target table). Postgres also accepts
        # OFFSET without LIMIT — handle both so the cardinality change
        # always surfaces.
        ("distinct", "DISTINCT"),
        ("limit", "LIMIT"),
        ("offset", "OFFSET"),
    ):
        if inner.args.get(forbidden):
            raise TranslationError(
                f"LEFT JOIN subquery body must be 'SELECT … FROM <single "
                f"table>' (Phase 1 — DEV-1565); inner {label} not accepted."
            )
    inner_from = inner.args.get("from_")
    if inner_from is None:
        raise TranslationError(
            "LEFT JOIN subquery body must have a FROM clause naming a "
            "single SLayer model (Phase 1 — DEV-1565)."
        )
    # Reject inner comma-join shape (FROM a, b).
    inner_table = inner_from.this
    if not isinstance(inner_table, exp.Table):
        raise TranslationError(
            "LEFT JOIN subquery body must reference exactly one table in "
            "its FROM (Phase 1 — DEV-1565); got "
            f"{type(inner_table).__name__}."
        )
    # _resolve_table accepts an exp.From wrapper, so feed the inner FROM.
    schema_name, target_table = _resolve_table(inner_from, catalog)
    return target_table, schema_name, alias


def _parse_on_clause(
    *,
    on: Optional[exp.Expression],
    parent_table: FacadeTable,
    target_table: FacadeTable,
    alias: str,
) -> Tuple[str, str]:
    """Parse the ON clause as exactly one equality between a parent-table-
    qualified column and a join-alias-qualified column.

    Returns ``(source_col, target_col)`` in the canonical case used by
    the underlying ``SlayerModel.columns[]`` (case-insensitive ON-column
    lookup catches hand-written UPPERCASE refs that Postgres folds to
    lowercase). Hidden columns counted.
    """
    if on is None or not isinstance(on, exp.EQ):
        raise TranslationError(
            "LEFT JOIN ON clause must be a single equality "
            "'<parent>.<col> = <alias>.<col>' (Phase 1 — DEV-1565); got "
            f"{type(on).__name__ if on is not None else 'no ON clause'}."
        )
    lhs, rhs = on.this, on.expression
    if not isinstance(lhs, exp.Column) or not isinstance(rhs, exp.Column):
        raise TranslationError(
            "LEFT JOIN ON clause must compare two simple column refs "
            "(Phase 1 — DEV-1565); function calls and expressions not "
            "accepted."
        )
    lhs_qual = _on_qualifier(lhs)
    rhs_qual = _on_qualifier(rhs)
    parent_match_l = _matches_parent_qualifier(lhs_qual, parent_table.name)
    parent_match_r = _matches_parent_qualifier(rhs_qual, parent_table.name)
    alias_match_l = _ci_eq(lhs_qual, alias)
    alias_match_r = _ci_eq(rhs_qual, alias)
    if parent_match_l and alias_match_r and not (parent_match_r and alias_match_l):
        source_raw, target_raw = _leaf(lhs), _leaf(rhs)
    elif parent_match_r and alias_match_l and not (parent_match_l and alias_match_r):
        source_raw, target_raw = _leaf(rhs), _leaf(lhs)
    else:
        raise TranslationError(
            f"LEFT JOIN ON clause must have one side qualified by the "
            f"parent table ({parent_table.name!r}) and the other by the "
            f"join alias ({alias!r}); got {on.sql()!r}."
        )
    return (
        _canonical_column_or_raise(parent_table, source_raw, role="parent"),
        _canonical_column_or_raise(target_table, target_raw, role="target"),
    )


def _on_qualifier(col: exp.Column) -> Optional[str]:
    """The table-qualifier of an ON-clause column ref, or None if bare."""
    table_node = col.args.get("table")
    if table_node is None:
        return None
    return str(table_node.this) if hasattr(table_node, "this") else str(table_node)


def _matches_parent_qualifier(
    qual: Optional[str], parent_name: str,
) -> bool:
    """True if the qualifier names the parent table (case-insensitive).
    Schema-prefix form is handled by ``_column_to_dotted``'s strip path,
    so this only matches the table-name segment."""
    return qual is not None and _ci_eq(qual, parent_name)


def _ci_eq(a: Optional[str], b: Optional[str]) -> bool:
    return a is not None and b is not None and a.lower() == b.lower()


def _leaf(col: exp.Column) -> str:
    leaf = col.this
    return str(leaf.this) if hasattr(leaf, "this") else str(leaf)


def _canonical_column_or_raise(
    table: FacadeTable, col_name: str, *, role: str,
) -> str:
    """Lookup ``col_name`` against the underlying ``SlayerModel.columns[]``
    (hidden cols counted). Returns the canonical column name; raises
    ``TranslationError`` on no match OR on case-insensitive ambiguity.

    Resolution order (Codex round 2):
      1. Exact (case-sensitive) match — preferred so user intent wins
         when the model has two columns differing only by case
         (``store_id`` and ``Store_ID``).
      2. Case-insensitive match — used when no exact match exists.
         Raises if multiple columns match case-insensitively (Postgres
         folds unquoted identifiers; sqlglot preserves what's written,
         so a hand-written ``ON ... = STORE_ID`` should pick the lower-
         case canonical column when that's the only match, but error
         loudly if the model carries both cases).

    DEV-1565: Postgres folds unquoted identifiers to lowercase but
    sqlglot preserves what was written, so hand-written SQL like
    ``ON ORDERS.STORE_ID = STORES.ID`` would otherwise fail to match
    against a model whose ``Column.name`` is ``store_id``. The canonical
    return propagates the model-side casing to every downstream
    comparison (the configured-join existence check, the dynamic-join
    ``ModelExtension`` build).
    """
    if table.model_ref is None:
        return col_name
    columns = table.model_ref.columns
    for c in columns:
        if c.name == col_name:
            return c.name
    needle = col_name.lower()
    matches = [c.name for c in columns if c.name.lower() == needle]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise TranslationError(
            f"LEFT JOIN ON {role}-side column {col_name!r} is "
            f"case-insensitively ambiguous on table {table.name!r} "
            f"(matches: {sorted(matches)}); quote the exact column name."
        )
    raise TranslationError(
        f"LEFT JOIN ON {role}-side column {col_name!r} does not exist "
        f"on table {table.name!r}."
    )


def _classify_against_parent_joins(
    *,
    parent_table: FacadeTable,
    target_name: str,
    source_col: str,
    target_col: str,
) -> Tuple[bool, bool]:
    """Match the emitted (target_model, join_pairs) against the parent's
    configured joins.

    Returns ``(is_dynamic, warn_cardinality)``:
      - existing LEFT match → (False, False).
      - existing INNER match on same pairs → (False, True) (warn about
        SQL LEFT vs configured INNER cardinality divergence).
      - no entry for target_model → (True, False) (dynamic fallback).
      - entry for target_model but DIFFERENT join_pairs → raise (an
        additive ModelExtension would produce a duplicate join).
    """
    same_target = [j for j in parent_table.joins if j.target_model == target_name]
    if not same_target:
        return True, False
    matching_pairs = [
        j for j in same_target if list(j.join_pairs) == [[source_col, target_col]]
    ]
    if not matching_pairs:
        configured = same_target[0].join_pairs
        raise TranslationError(
            f"LEFT JOIN to {target_name!r} uses ON columns "
            f"({source_col!r}, {target_col!r}) which do not match the "
            f"configured join_pairs ({configured}) on {parent_table.name!r}. "
            f"ModelExtension cannot override an existing join — adjust the "
            f"emitted SQL to match the configured join, or update the model "
            f"join_pairs."
        )
    j = matching_pairs[0]
    warn_cardinality = j.join_type != JoinType.LEFT
    return False, warn_cardinality


def _materialise_dynamic_join_lookups(
    *,
    target_model: SlayerModel,
    extra_dims_by_name: Dict[str, FacadeDimension],
    extra_metrics_by_name: Dict[str, FacadeMetric],
    extra_metrics_by_formula: Dict[str, FacadeMetric],
) -> None:
    """For a dynamic-join target (no configured BFS expansion in the
    catalog), build the bare-col dims and col×agg metrics keyed by
    ``<target>.<col>`` / ``<target>.<col>:<agg>`` so the projection /
    aggregate / WHERE / HAVING resolution paths find the joined refs.
    """
    local_dims, local_metrics = build_local_view(target_model)
    prefix = target_model.name
    for dim in local_dims:
        ref = f"{prefix}.{dim.dimension_ref}"
        extra_dims_by_name[ref] = FacadeDimension(
            name=ref,
            description=dim.description,
            label=dim.label,
            data_type=dim.data_type,
            is_time=dim.is_time,
            dimension_ref=ref,
        )
    for m in local_metrics:
        if m.measure_formula == "*:count":
            formula = f"{prefix}.*:count"
        else:
            formula = f"{prefix}.{m.measure_formula}"
        new_metric = FacadeMetric(
            name=f"{prefix}.{m.name}",
            description=m.description,
            label=m.label,
            data_type=m.data_type,
            measure_formula=formula,
        )
        extra_metrics_by_name[f"{prefix}.{m.name}"] = new_metric
        extra_metrics_by_formula[formula] = new_metric


def _build_source_model_from_join(
    *, parent_name: str, plan: _JoinPlan,
) -> object:
    """``SlayerQuery.source_model`` value derived from the join plan:
    the parent's bare name when an existing configured join matched, or a
    ``ModelExtension`` carrying the dynamically-built ``ModelJoin`` when
    not.
    """
    if not plan.is_dynamic:
        return parent_name
    return ModelExtension(
        source_name=parent_name,
        joins=[ModelJoin(
            target_model=plan.target_table.name,
            join_pairs=[[plan.source_col, plan.target_col]],
            join_type=JoinType.LEFT,
        )],
    )


def _emit_join_warnings(plan: _JoinPlan, parent_name: str) -> None:
    if plan.is_dynamic:
        logger.warning(
            "pg-facade: dynamic join from %r to %r on %r=%r — no configured "
            "join matched in parent.joins[]; using a ModelExtension to honor "
            "the emitted ON clause (DEV-1565).",
            parent_name, plan.target_table.name, plan.source_col, plan.target_col,
        )
    elif plan.warn_cardinality:
        logger.warning(
            "pg-facade: LEFT JOIN to %r matched a configured non-LEFT join "
            "(join_type=INNER on %r.joins) — using the configured join_type "
            "but cardinality semantics differ from the emitted SQL (DEV-1565).",
            plan.target_table.name, parent_name,
        )


class _JoinOverlays(BaseModel):
    """Translator-state derived from a parsed ``_JoinPlan``: the alias
    rewrite map and the on-demand projection-lookup overlays for the
    dynamic-fallback case."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    alias_map: Optional[Dict[str, str]] = None
    extra_dims_by_name: Dict[str, FacadeDimension] = {}
    extra_metrics_by_name: Dict[str, FacadeMetric] = {}
    extra_metrics_by_formula: Dict[str, FacadeMetric] = {}


def _prepare_join_overlays(
    join_plan: Optional[_JoinPlan], parent_name: str,
) -> _JoinOverlays:
    """Build the alias map + (for dynamic joins) the materialised lookup
    overlays, and emit the join-related warnings. Extracted from
    ``_translate_slayer_select`` to keep its cognitive complexity in check."""
    if join_plan is None:
        return _JoinOverlays()
    overlays = _JoinOverlays(
        alias_map={join_plan.alias: join_plan.target_table.name},
    )
    if join_plan.is_dynamic and join_plan.target_table.model_ref is not None:
        _materialise_dynamic_join_lookups(
            target_model=join_plan.target_table.model_ref,
            extra_dims_by_name=overlays.extra_dims_by_name,
            extra_metrics_by_name=overlays.extra_metrics_by_name,
            extra_metrics_by_formula=overlays.extra_metrics_by_formula,
        )
    _emit_join_warnings(join_plan, parent_name)
    return overlays


def _build_item_index(items: List[_ProjectionItem]) -> Dict[str, _ProjectionItem]:
    """Map every projection item by its user-facing name PLUS its canonical
    secondary key (time-grain canonical form for `CAST(date_trunc(...))`
    GROUP BY matches; dimension_ref dotted form for joined-col aliased
    projections — see DEV-1565). ``setdefault`` preserves the primary
    projected_name entry when the secondary key collides with it.

    DEV-1566: CAST-projected items are NOT registered under their
    ``dimension_ref`` secondary key — that would route ``ORDER BY <bare col>``
    on a query like ``SELECT CAST(id AS TEXT) AS x ... ORDER BY id`` to the
    cast item and trip the lossy-pair rejection, even though the user
    explicitly named the bare column (and the engine ordering already
    matches that intent).
    """
    out: Dict[str, _ProjectionItem] = {item.projected_name: item for item in items}
    for item in items:
        if item.time_grain is not None and item.time_grain_underlying is not None:
            canonical = (
                f"{item.time_grain.value}({item.time_grain_underlying.dimension_ref})"
            )
            out.setdefault(canonical, item)
        elif (
            item.dimension is not None
            and item.cast_target is None
            and item.dimension.dimension_ref != item.projected_name
        ):
            out.setdefault(item.dimension.dimension_ref, item)
    return out


def _translate_slayer_select(
    parsed: exp.Select, catalog: FacadeCatalog,
    *, allow_column_cast: bool = True,
) -> QueryResult:
    from_clause = parsed.args.get("from_")
    if from_clause is None:
        raise TranslationError(
            "No FROM clause; expected one of the registered Flight tables "
            "or INFORMATION_SCHEMA.*"
        )
    schema_name, table = _resolve_table(from_clause, catalog)

    proj_exprs = parsed.args.get("expressions") or []
    # Reject SELECT * before catalog lookup so we get the named error
    # instead of "Unknown projection item '*'".
    if any(isinstance(e, exp.Star) for e in proj_exprs):
        raise TranslationError(SELECT_STAR_MESSAGE)

    # DEV-1558 B5: every helper that resolves a column ref needs the same
    # ``(schema, table)`` prefix-strip context as ``_resolve_projection``.
    strip_prefix: Optional[Tuple[str, str]] = (
        (schema_name, table.name) if schema_name else None
    )

    join_plan = _parse_left_join(
        parsed_joins=parsed.args.get("joins") or [],
        parent_table=table,
        catalog=catalog,
    )
    overlays = _prepare_join_overlays(join_plan, table.name)

    items = _resolve_projection(
        proj_exprs, table,
        schema_name=schema_name,
        alias_map=overlays.alias_map,
        extra_dims_by_name=overlays.extra_dims_by_name or None,
        extra_metrics_by_name=overlays.extra_metrics_by_name or None,
        extra_metrics_by_formula=overlays.extra_metrics_by_formula or None,
        allow_column_cast=allow_column_cast,
    )
    plan = _build_projection_plan(items, table)
    item_by_projected_name = _index_items_by_canonical_form(items)

    _validate_group_by(
        parsed.args.get("group"), plan.derived_dims, item_by_projected_name,
        strip_prefix=strip_prefix, alias_map=overlays.alias_map,
    )

    filters: List[str] = []
    _apply_where(
        parsed.args.get("where"), plan.time_dim_by_name,
        item_by_projected_name, filters,
        strip_prefix=strip_prefix, alias_map=overlays.alias_map,
    )
    _apply_having(
        parsed.args.get("having"), table,
        item_by_projected_name, filters,
        strip_prefix=strip_prefix, alias_map=overlays.alias_map,
        extra_metrics_by_formula=overlays.extra_metrics_by_formula or None,
    )

    order_items = _translate_order_by(
        parsed.args.get("order"), _build_item_index(items),
        strip_prefix=strip_prefix, alias_map=overlays.alias_map,
    )

    source_model: object = (
        _build_source_model_from_join(parent_name=table.name, plan=join_plan)
        if join_plan is not None
        else table.name
    )

    query = SlayerQuery(
        source_model=source_model,
        measures=plan.measures or None,
        dimensions=plan.dimension_refs or None,
        time_dimensions=plan.time_dims or None,
        filters=filters or None,
        order=order_items or None,
        limit=_parse_int_literal(parsed.args.get("limit")),
        offset=_parse_int_literal(parsed.args.get("offset")),
    )

    return QueryResult(
        query=query,
        column_name_mapping=plan.column_name_mapping,
        facade_table=table,
        schema_name=schema_name,
        projection_types=plan.projection_types,
    )
