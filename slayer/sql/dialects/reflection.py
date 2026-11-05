"""Table-structure reflection: typed results, type-string mapping and catalog-query helpers."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Annotated, Any, Protocol, TypeVar

import sqlalchemy as sa
import sqlalchemy.dialects.mssql as _sqla_mssql
from pydantic import BaseModel, ConfigDict, Field, ValidatorFunctionWrapHandler, WrapValidator
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class CatalogEngine(Protocol):
    """What a catalog read needs from an engine: connections that run statements verbatim."""

    def connect(self) -> AbstractContextManager[Any]: ...


def _as_given(value: Any, handler: ValidatorFunctionWrapHandler) -> Any:
    return value if isinstance(value, str) else handler(value)


#: A live name in SQLAlchemy's spelling: a ``quoted_name`` (a lower-case Snowflake / Oracle name) keeps its flag.
LiveName = Annotated[str, WrapValidator(_as_given)]


class TableLocation(BaseModel):
    """One live table, its names in SQLAlchemy's spelling; an unset schema or catalog is the connection's current one."""

    model_config = ConfigDict(frozen=True)

    name: LiveName
    schema_name: LiveName | None = None
    catalog: LiveName | None = None


class ReflectedColumn(BaseModel):
    """One live column; ``db_type`` is set only where ``type`` is lossy (opaque, a list, exact-numeric, JSON or
    a zoned timestamp); ``element_type`` only for a list."""

    model_config = ConfigDict(frozen=True)

    name: str
    type: DataType
    element_type: DataType | None = None
    is_float: bool = False
    db_type: str | None = None
    comment: str | None = None
    # SQLAlchemy's ``quoted_name`` flag on ``name`` (a lower-case Snowflake / Oracle name).
    quoted: bool = False


class ResultColumnType(BaseModel):
    """One result column's SLayer type and the database's name for it (``None`` when the driver names none)."""

    model_config = ConfigDict(frozen=True)

    type: DataType
    element_type: DataType | None = None
    db_type: str | None = None

    @property
    def lossy_db_type(self) -> str | None:
        """The type name a stored ``Column.db_type`` keeps: only where ``type`` is lossy (opaque, a list or
        exact-numeric)."""
        if self.db_type is None:
            return None
        name = unwrap_clickhouse_wrapper_str(self.db_type)
        return name if self.type.is_opaque or self.type.is_list or is_exact_numeric_db_type(name) else None


class ReflectedForeignKey(BaseModel):
    columns: list[str]
    referred_schema: str | None = None
    referred_table: str
    referred_columns: list[str]


class TableKeys(BaseModel):
    """A table's declared keys; ``unique_key_sets`` never repeats the primary key."""

    primary_key: list[str] = Field(default_factory=list)
    foreign_keys: list[ReflectedForeignKey] = Field(default_factory=list)
    unique_key_sets: list[list[str]] = Field(default_factory=list)


# --- Type names → DataType (one table for catalog strings, SQLAlchemy types and probes) ---

_TYPE_NAME_MAP = {
    "INTEGER": DataType.INT,
    "INT": DataType.INT,
    "BIGINT": DataType.INT,
    "SMALLINT": DataType.INT,
    "TINYINT": DataType.INT,
    "MEDIUMINT": DataType.INT,
    "HUGEINT": DataType.INT,
    "SERIAL": DataType.INT,
    "BIGSERIAL": DataType.INT,
    "OID": DataType.INT,
    "YEAR": DataType.INT,
    "FLOAT": DataType.DOUBLE,
    "REAL": DataType.DOUBLE,
    "DOUBLE": DataType.DOUBLE,
    "DOUBLE_PRECISION": DataType.DOUBLE,
    "DOUBLE PRECISION": DataType.DOUBLE,
    "MONEY": DataType.DOUBLE,
    "SMALLMONEY": DataType.DOUBLE,
    "VARCHAR": DataType.TEXT,
    "CHAR": DataType.TEXT,
    "TEXT": DataType.TEXT,
    "STRING": DataType.TEXT,
    "BOOLEAN": DataType.BOOLEAN,
    "BOOL": DataType.BOOLEAN,
    "BIT": DataType.BOOLEAN,  # T-SQL boolean
    "TIMESTAMP": DataType.TIMESTAMP,
    "DATETIME": DataType.TIMESTAMP,
    "TIMESTAMP WITHOUT TIME ZONE": DataType.TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": DataType.TIMESTAMP,
    "TIMESTAMP_NTZ": DataType.TIMESTAMP,
    "TIMESTAMP_LTZ": DataType.TIMESTAMP,
    "TIMESTAMP_TZ": DataType.TIMESTAMP,
    "DATE": DataType.DATE,
    "TIME": DataType.TIMESTAMP,
    "INTERVAL": DataType.TEXT,  # no interval DataType; groupable as TEXT
    # ClickHouse
    "INT8": DataType.INT,
    "INT16": DataType.INT,
    "INT32": DataType.INT,
    "INT64": DataType.INT,
    "INT128": DataType.INT,
    "INT256": DataType.INT,
    "UINT8": DataType.INT,
    "UINT16": DataType.INT,
    "UINT32": DataType.INT,
    "UINT64": DataType.INT,
    "UINT128": DataType.INT,
    "UINT256": DataType.INT,
    "FLOAT32": DataType.DOUBLE,
    "FLOAT64": DataType.DOUBLE,
    "DATETIME64": DataType.TIMESTAMP,
    "DATE32": DataType.DATE,
    # T-SQL
    "DATETIME2": DataType.TIMESTAMP,
    "SMALLDATETIME": DataType.TIMESTAMP,
    "DATETIMEOFFSET": DataType.TIMESTAMP,
    "NVARCHAR": DataType.TEXT,
    "NCHAR": DataType.TEXT,
    "NTEXT": DataType.TEXT,
    "ROWVERSION": DataType.TEXT,  # 8-byte binary counter, not temporal
    "UUID": DataType.TEXT,
    "JSONB": DataType.TEXT,  # comparable, unlike generic JSON
}

# NUMERIC/DECIMAL are float-like only by scale.
_FLOAT_LIKE_TYPE_NAMES = frozenset({
    "FLOAT", "REAL", "DOUBLE", "DOUBLE_PRECISION", "DOUBLE PRECISION", "FLOAT32", "FLOAT64", "MONEY", "SMALLMONEY",
})

# Types with no default btree/hash opclass (no GROUP BY / DISTINCT) → UNKNOWN.
# A small allow-list on purpose: unmapped-but-comparable types stay TEXT.
_OPAQUE_TYPE_NAMES = frozenset({
    "JSON",  # ``jsonb`` is groupable and deliberately absent
    "XML",
    "TXID_SNAPSHOT",
    # Geometric / spatial — none have a default btree/hash opclass
    "POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE",
    "GEOMETRY", "GEOGRAPHY", "RASTER",
    # Range types
    "INT4RANGE", "INT8RANGE", "NUMRANGE", "TSRANGE", "TSTZRANGE", "DATERANGE",
})

# List / struct / map spellings across DuckDB, Trino, ClickHouse, BigQuery and Postgres.
_CONTAINER_PREFIXES = ("ARRAY", "LIST", "STRUCT", "ROW", "MAP", "TUPLE", "NESTED")

CLICKHOUSE_WRAPPER_NAMES = frozenset({"NULLABLE", "LOWCARDINALITY"})
CLICKHOUSE_WRAPPER_MAX_DEPTH = 8


def clean_comment(value: str | None) -> str | None:
    """Normalize a DB comment: strip whitespace, empty → None."""
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def unwrap_clickhouse_wrapper_str(db_type: str) -> str:
    """Peel nested ``Nullable(...)``/``LowCardinality(...)`` wrapper text."""
    current = db_type.strip()
    for _ in range(CLICKHOUSE_WRAPPER_MAX_DEPTH):
        head, sep, _rest = current.partition("(")
        if not sep or head.strip().upper() not in CLICKHOUSE_WRAPPER_NAMES or not current.endswith(")"):
            return current
        current = current[len(head) + 1 : -1].strip()
    return current


def is_exact_numeric_db_type(db_type: str | None) -> bool:
    """Whether a raw DB type string is exact-numeric (DECIMAL/NUMERIC family, wrappers and lists excluded)."""
    if not db_type or info_schema_list_element(db_type) is not None:
        return False
    token = db_type.split("(")[0].upper().strip()
    return "DECIMAL" in token or "NUMERIC" in token


def parse_info_schema_is_float(data_type_str: str) -> bool:
    """Whether a NUMERIC/DECIMAL type string is float-like: scale > 0, or no scale given."""
    if "(" in data_type_str and "," in data_type_str:
        try:
            return int(data_type_str.split(",")[-1].rstrip(")").strip()) > 0
        except (ValueError, IndexError):
            return True
    return True


def _is_container_name(name: str) -> bool:
    if name.endswith("[]"):
        return True
    return re.split(r"[(<\s]", name, maxsplit=1)[0].upper() in _CONTAINER_PREFIXES


def info_schema_type(
    data_type_str: str, *, overrides: Mapping[str, DataType] | None = None,
) -> tuple[DataType, bool]:
    """``(DataType, is_float)`` of a type name; ``overrides`` (upper-case base names) win over the table."""
    name = unwrap_clickhouse_wrapper_str(data_type_str)
    base = name.split("(")[0].upper().strip()
    if overrides and base in overrides:
        return overrides[base], False
    if info_schema_list_element(name) is not None:
        return DataType.ARRAY, False
    if _is_container_name(name):
        return DataType.TEXT, False
    if base in _OPAQUE_TYPE_NAMES:
        return DataType.UNKNOWN, False
    if is_exact_numeric_db_type(base):
        is_float = parse_info_schema_is_float(name)
        return (DataType.DOUBLE if is_float else DataType.INT), is_float
    mapped = _TYPE_NAME_MAP.get(base)
    if mapped is not None:
        return mapped, base in _FLOAT_LIKE_TYPE_NAMES
    if "DOUBLE" in base or "FLOAT" in base or "REAL" in base:
        return DataType.DOUBLE, True
    if "TIMESTAMP" in base or "DATETIME" in base:
        return DataType.TIMESTAMP, False
    if "INTERVAL" in base:
        return DataType.TEXT, False
    if "INT" in base:
        return DataType.INT, False
    return DataType.TEXT, False


def info_schema_list_element(data_type_str: str | None) -> DataType | None:
    """The element type of a list type string (``T[]``, ``LIST(T)``, ``ARRAY(T)``), else ``None``; a nested list
    or record has ``UNKNOWN`` elements."""
    text = (data_type_str or "").strip()
    upper = text.upper()
    if upper == "ARRAY":  # an untyped list (Snowflake), or a catalog that names no element type
        return DataType.UNKNOWN
    if upper.endswith("[]"):
        inner = text[:-2].strip()
    elif upper.startswith(("LIST(", "ARRAY(", "ARRAY<")) and upper.endswith((")", ">")):
        inner = text[min(i for i in (text.find("("), text.find("<")) if i >= 0) + 1:-1].strip()
    else:
        return None
    if info_schema_list_element(inner) is not None or inner.upper().startswith(("STRUCT", "MAP", "UNION", "ROW")):
        return DataType.UNKNOWN
    return info_schema_type(unwrap_clickhouse_wrapper_str(inner))[0]


def result_column_type_of_name(
    name: str, *, overrides: Mapping[str, DataType] | None = None,
) -> ResultColumnType:
    """A probed column the database names ``name``."""
    element = info_schema_list_element(unwrap_clickhouse_wrapper_str(name))
    if element is not None:
        return ResultColumnType(type=DataType.ARRAY, element_type=element, db_type=name)
    return ResultColumnType(type=info_schema_type(name, overrides=overrides)[0], db_type=name)


def column_from_type_string(
    *, name: str, data_type: str | None, comment: str | None = None, overrides: Mapping[str, DataType] | None = None,
    zoned_types: frozenset[str] = frozenset(), json_types: frozenset[str] = frozenset(),
) -> ReflectedColumn:
    """A column read from a catalog type string; ``db_type`` kept where ``DataType`` is lossy (a list, exact
    numerics, a JSON type in ``json_types``, a zoned timestamp in ``zoned_types``)."""
    unwrapped = unwrap_clickhouse_wrapper_str(data_type) if data_type else ""
    element = info_schema_list_element(unwrapped)
    if element is not None:
        return ReflectedColumn(
            name=name, type=DataType.ARRAY, element_type=element, db_type=unwrapped, comment=clean_comment(comment),
        )
    mapped, is_float = info_schema_type(unwrapped, overrides=overrides)
    word = unwrapped.split("(")[0].upper().strip()
    lossy = is_exact_numeric_db_type(unwrapped) or word in json_types or unwrapped.upper() in zoned_types
    return ReflectedColumn(
        name=name, type=mapped, is_float=is_float, db_type=unwrapped if lossy else None,
        comment=clean_comment(comment),
    )


# --- SQLAlchemy types → DataType ---

# Dedup set for unrecognized SA type warnings, keyed by upper-cased class name.
_logged_unmapped_sa_types: set[str] = set()

def _unwrap_clickhouse_wrappers(sa_type: sa.types.TypeEngine) -> sa.types.TypeEngine:
    """Peel nested ClickHouse Nullable/LowCardinality wrappers (as-is if no ``nested_type``)."""
    current = sa_type
    for _ in range(CLICKHOUSE_WRAPPER_MAX_DEPTH):
        if type(current).__name__.upper() not in CLICKHOUSE_WRAPPER_NAMES:
            return current
        inner = getattr(current, "nested_type", None)
        if inner is None:
            return current
        current = inner
    return current


#: Element types that are records or containers: a list of them has opaque elements.
_OPAQUE_ELEMENT_TYPE_NAMES = frozenset({"STRUCT", "RECORD", "TUPLE", "MAP", "OBJECT", "VARIANT", "ROW", "JSON"})


def _sa_list_item(sa_type: sa.types.TypeEngine) -> tuple[bool, sa.types.TypeEngine | None]:
    """``(is a list, its item type)``; the item is ``None`` for an untyped (Snowflake semi-structured) list."""
    sa_type = _unwrap_clickhouse_wrappers(sa_type)
    if isinstance(sa_type, sa.ARRAY):
        dimensions = getattr(sa_type, "dimensions", None)
        return True, None if dimensions is not None and dimensions > 1 else sa_type.item_type
    if type(sa_type).__name__.upper() != "ARRAY":
        return False, None
    item = getattr(sa_type, "item_type", None)
    if item is not None:  # ClickHouse Array(T)
        return True, item
    return True, getattr(sa_type, "value_type", None)  # Snowflake ARRAY / ARRAY(T)


def _sa_element_type(item: sa.types.TypeEngine | None) -> DataType:
    """A list item's SLayer type: a scalar's own, ``UNKNOWN`` for a nested list, a record or no item type."""
    if item is None or _sa_list_item(item)[0]:
        return DataType.UNKNOWN
    if type(_unwrap_clickhouse_wrappers(item)).__name__.upper() in _OPAQUE_ELEMENT_TYPE_NAMES:
        return DataType.UNKNOWN
    return _sa_type_to_data_type(item)


def sa_data_types(sa_type: sa.types.TypeEngine) -> tuple[DataType, DataType | None]:
    """``(type, element type)`` of a SQLAlchemy type; the element type is set only for a list."""
    is_list, item = _sa_list_item(sa_type)
    if is_list:
        return DataType.ARRAY, _sa_element_type(item)
    return _sa_type_to_data_type(sa_type), None


def list_db_type(sa_type: sa.types.TypeEngine, *, sa_dialect: Any = None) -> str:
    """A list's raw database type with its element (``INTEGER[]``, ``ARRAY<STRING>``)."""
    unwrapped = _unwrap_clickhouse_wrappers(sa_type)
    if sa_dialect is not None:
        try:
            return unwrapped.compile(dialect=sa_dialect)
        except Exception:  # noqa: BLE001 — a type its dialect cannot spell keeps the generic form
            logger.debug("list type %r does not compile on %s", unwrapped, sa_dialect.name)
    item = _sa_list_item(unwrapped)[1]
    if item is None:
        return _raw_db_type_str(unwrapped) or "ARRAY"
    return f"{type(_unwrap_clickhouse_wrappers(item)).__name__.upper()}[]"


def _sa_type_to_data_type(sa_type: sa.types.TypeEngine) -> DataType:
    sa_type = _unwrap_clickhouse_wrappers(sa_type)
    if _sa_list_item(sa_type)[0]:
        return DataType.ARRAY
    # mssql.TIMESTAMP is rowversion (binary), and its name collides with sa.TIMESTAMP.
    if isinstance(sa_type, _sqla_mssql.TIMESTAMP):
        return DataType.TEXT
    # Any dialect's UUID, e.g. SQL Server UNIQUEIDENTIFIER.
    if isinstance(sa_type, sa.Uuid):
        return DataType.TEXT
    type_name = type(sa_type).__name__.upper()
    type_str = str(sa_type).split("(")[0].upper().strip()
    if type_name in _CONTAINER_PREFIXES or _is_container_name(str(sa_type)):
        return DataType.TEXT
    if type_name in _OPAQUE_TYPE_NAMES or type_str in _OPAQUE_TYPE_NAMES:
        return DataType.UNKNOWN
    if is_exact_numeric_db_type(type_name) or is_exact_numeric_db_type(type_str):
        return DataType.DOUBLE if _sa_type_is_float(sa_type) else DataType.INT
    if type_name in _TYPE_NAME_MAP:
        return _TYPE_NAME_MAP[type_name]
    if type_str in _TYPE_NAME_MAP:
        return _TYPE_NAME_MAP[type_str]
    if type_name not in _logged_unmapped_sa_types:
        _logged_unmapped_sa_types.add(type_name)
        logger.warning(
            "Unrecognized SQLAlchemy type %r (str=%r); falling back to "
            "DataType.TEXT. Most unmapped types (bytea) are comparable and "
            "work as TEXT; add genuinely non-comparable ones to "
            "_OPAQUE_TYPE_NAMES and the rest to _TYPE_NAME_MAP.",
            type_name,
            str(sa_type),
        )
    return DataType.TEXT


def _raw_db_type_str(sa_type: sa.types.TypeEngine) -> str | None:
    """Best-effort raw database type string for ``Column.db_type`` (never raises)."""
    try:
        text = str(sa_type).strip()
    except Exception:
        text = ""
    if text:
        return text
    name = type(sa_type).__name__
    return name or None


def _sa_type_is_exact_numeric(sa_type: sa.types.TypeEngine) -> bool:
    """Whether ``sa_type`` is an exact NUMERIC/DECIMAL database type."""
    sa_type = _unwrap_clickhouse_wrappers(sa_type)
    return is_exact_numeric_db_type(type(sa_type).__name__) or is_exact_numeric_db_type(str(sa_type))


def _sa_type_is_float(sa_type: sa.types.TypeEngine) -> bool:
    """Whether the type is float-like; NUMERIC/DECIMAL only when scale > 0 or unknown."""
    sa_type = _unwrap_clickhouse_wrappers(sa_type)
    type_name = type(sa_type).__name__.upper()
    if type_name in _FLOAT_LIKE_TYPE_NAMES:
        return True
    if is_exact_numeric_db_type(type_name):
        scale = getattr(sa_type, "scale", None)
        return scale is None or scale > 0
    type_str = str(sa_type).split("(")[0].upper().strip()
    if type_str in _FLOAT_LIKE_TYPE_NAMES:
        return True
    if is_exact_numeric_db_type(type_str):
        scale = getattr(sa_type, "scale", None)
        return scale is None or scale > 0
    return False


def _sa_type_is_json(sa_type: sa.types.TypeEngine, *, json_types: frozenset[str]) -> bool:
    """A type in the dialect's ``json_types`` (``json_value`` reads it natively)."""
    sa_type = _unwrap_clickhouse_wrappers(sa_type)
    return any(n.split("(")[0].strip().upper() in json_types for n in (type(sa_type).__name__, str(sa_type)))


def column_from_sa_type(
    *, name: str, sa_type: sa.types.TypeEngine, comment: str | None = None,
    zoned_types: frozenset[str] = frozenset(), json_types: frozenset[str] = frozenset(), sa_dialect: Any = None,
) -> ReflectedColumn:
    """A column read through the Inspector; ``db_type`` kept only where ``DataType`` is lossy (opaque, a list,
    exact NUMERIC/DECIMAL, a JSON type in ``json_types``, a zoned timestamp in ``zoned_types``)."""
    data_type, element_type = sa_data_types(sa_type)
    db_type: str | None = None
    if data_type is DataType.ARRAY:
        db_type = list_db_type(sa_type, sa_dialect=sa_dialect)
    elif data_type.is_opaque or _sa_type_is_exact_numeric(sa_type) or _sa_type_is_json(sa_type, json_types=json_types) or (
        type(sa_type).__name__.upper() in zoned_types
    ):
        db_type = _raw_db_type_str(_unwrap_clickhouse_wrappers(sa_type))
    return ReflectedColumn(
        name=name, type=data_type, element_type=element_type, is_float=_sa_type_is_float(sa_type), db_type=db_type,
        comment=clean_comment(comment), quoted=getattr(name, "quote", None) is True,
    )


# --- Catalog queries ---


def schema_token(*, schema: str | None, catalog: str | None) -> str | None:
    """The catalog-qualified schema token Inspector methods take."""
    if catalog and schema:
        return f"{catalog}.{schema}"
    return schema


def col(name: str, owner: str | None = None) -> exp.Column:
    return exp.column(exp.to_identifier(name), table=exp.to_identifier(owner) if owner else None)


def eq_literal(target: Expression, value: str) -> exp.EQ:
    """``target = '<value>'`` with ``value`` as an escaped string literal."""
    return exp.EQ(this=target, expression=exp.Literal.string(value))


def call(name: str, *args: Expression) -> exp.Anonymous:
    return exp.Anonymous(this=name, expressions=list(args))


def relation(
    name: str, *, db: str | None = None, catalog: exp.Identifier | None = None, alias: str | None = None,
) -> exp.Table:
    return exp.Table(
        this=exp.to_identifier(name),
        db=exp.to_identifier(db) if db else None,
        catalog=catalog,
        alias=exp.TableAlias(this=exp.to_identifier(alias)) if alias else None,
    )


def table_function(name: str) -> exp.Table:
    """``FROM name()``."""
    return exp.Table(this=call(name))


def all_of(conditions: Iterable[Expression]) -> exp.Condition:
    return exp.and_(*conditions)


def info_schema_comments_query(
    *, comment_column: str, default_schema: Expression, location: TableLocation,
) -> exp.Select:
    """``(column_name, <comment_column>)`` from INFORMATION_SCHEMA; no schema scopes to ``default_schema``."""
    conditions = [
        eq_literal(col("table_name"), location.name),
        eq_literal(col("table_schema"), location.schema_name) if location.schema_name
        else exp.EQ(this=col("table_schema"), expression=default_schema),
    ]
    if location.catalog:
        conditions.append(eq_literal(col("table_catalog"), location.catalog))
    return (
        exp.select(col("column_name"), col(comment_column))
        .from_(relation("columns", db="information_schema"))
        .where(all_of(conditions))
    )


def fetch(sa_engine: CatalogEngine | None, statement: Expression, *, dialect: str) -> list[tuple[Any, ...]]:
    """Run one catalog query verbatim (no bind parameters) and return its rows."""
    if sa_engine is None:
        raise ValueError("no engine to read the catalog with")
    sql = statement.sql(dialect=dialect, normalize_functions=False)
    with sa_engine.connect() as conn:
        return [tuple(row) for row in conn.exec_driver_sql(sql).fetchall()]


def best_effort(read: Callable[[], _T], *, default: _T, table: str, what: str) -> _T:
    """One key read; a failure is logged at WARNING and yields ``default`` (unsupported reads stay quiet)."""
    try:
        return read()
    except NotImplementedError:
        logger.debug("Reading the %s of table %r is not supported here", what, table)
        return default
    except Exception as exc:  # noqa: BLE001 — a key read never sinks the table
        logger.warning("Reading the %s of table %r failed: %s", what, table, exc)
        return default


def without_primary_key(primary_key: list[str], key_sets: list[list[str]]) -> list[list[str]]:
    """``key_sets`` without the primary key, each set once (a constraint and its backing index repeat it)."""
    seen = {frozenset(primary_key)}
    out: list[list[str]] = []
    for ks in key_sets:
        if frozenset(ks) not in seen:
            seen.add(frozenset(ks))
            out.append(ks)
    return out


# --- Inspector reads (the base hooks) ---


def inspector_column_names(columns: object) -> list[str] | None:
    """A reflected column list when every member is a non-empty name, else ``None``."""
    if isinstance(columns, list) and columns and all(isinstance(c, str) and c for c in columns):
        return list(columns)
    return None


def is_partial_index(index: dict) -> bool:
    """Whether a reflected index carries a filter predicate."""
    for key, value in (index.get("dialect_options") or {}).items():
        if not key.endswith("_where") or value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return True
            continue
        # Never bool() a non-string: ColumnElement.__bool__ raises.
        return True
    return False


def inspector_foreign_keys(inspector: Any, *, table: str, token: str | None) -> list[ReflectedForeignKey]:
    out: list[ReflectedForeignKey] = []
    for fk in inspector.get_foreign_keys(table, schema=token):
        columns = inspector_column_names(fk.get("constrained_columns"))
        referred = inspector_column_names(fk.get("referred_columns"))
        if columns and referred and fk.get("referred_table"):
            out.append(ReflectedForeignKey(
                columns=columns, referred_schema=fk.get("referred_schema"),
                referred_table=fk["referred_table"], referred_columns=referred,
            ))
    return out


def inspector_unique_constraints(inspector: Any, *, table: str, token: str | None) -> list[list[str]]:
    sets = (inspector_column_names(uc.get("column_names")) for uc in inspector.get_unique_constraints(table, schema=token))
    return [s for s in sets if s]


def inspector_unique_indexes(inspector: Any, *, table: str, token: str | None) -> list[list[str]]:
    """Unique indexes constraining the whole table; an expression member reflects as None and rejects the set."""
    sets = (
        inspector_column_names(idx.get("column_names"))
        for idx in inspector.get_indexes(table, schema=token)
        if idx.get("unique") and not is_partial_index(idx)
    )
    return [s for s in sets if s]
