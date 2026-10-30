"""Table-structure reflection: typed results, type-string mapping and catalog-query helpers."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from typing import Any, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.types import TypeEngine
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class CatalogEngine(Protocol):
    """What a catalog read needs from an engine: connections that run statements verbatim."""

    def connect(self) -> AbstractContextManager[Any]: ...


class TableLocation(BaseModel):
    """One live table; an unset schema or catalog is the connection's current one."""

    model_config = ConfigDict(frozen=True)

    name: str
    schema_name: str | None = None
    catalog: str | None = None


class ReflectedColumn(BaseModel):
    """One live column; ``type`` is an Inspector ``TypeEngine`` or an already-mapped ``DataType``."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    name: str
    type: DataType | TypeEngine
    element_type: DataType | None = None
    is_float: bool = False
    db_type: str | None = None
    comment: str | None = None


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


# --- Type strings → DataType ---

_FLOAT_LIKE_INFO_SCHEMA_TYPES = frozenset({"FLOAT", "DOUBLE", "REAL"})

_INFO_SCHEMA_TYPE_MAP = {
    "INTEGER": DataType.INT,
    "BIGINT": DataType.INT,
    "SMALLINT": DataType.INT,
    "TINYINT": DataType.INT,
    "HUGEINT": DataType.INT,
    "FLOAT": DataType.DOUBLE,
    "DOUBLE": DataType.DOUBLE,
    "REAL": DataType.DOUBLE,
    "VARCHAR": DataType.TEXT,
    "CHAR": DataType.TEXT,
    "TEXT": DataType.TEXT,
    "BOOLEAN": DataType.BOOLEAN,
    "TIMESTAMP": DataType.TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": DataType.TIMESTAMP,
    "DATETIME": DataType.TIMESTAMP,
    "DATE": DataType.DATE,
    "TIME": DataType.TIMESTAMP,
}

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
    """Whether a raw DB type string is exact-numeric (DECIMAL/NUMERIC family, wrappers excluded)."""
    if not db_type:
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


def info_schema_type(data_type_str: str) -> tuple[DataType, bool]:
    """``(DataType, is_float)`` of a catalog type string; substring checks run widest-match-last."""
    base = data_type_str.split("(")[0].upper().strip()
    mapped = _INFO_SCHEMA_TYPE_MAP.get(base)
    if mapped is not None:
        return mapped, base in _FLOAT_LIKE_INFO_SCHEMA_TYPES
    if is_exact_numeric_db_type(base):
        return DataType.DOUBLE, parse_info_schema_is_float(data_type_str)
    if "DOUBLE" in base or "FLOAT" in base or "REAL" in base:
        return DataType.DOUBLE, True
    if "TIMESTAMP" in base or "DATETIME" in base:
        return DataType.TIMESTAMP, False
    if "INTERVAL" in base:
        return DataType.TEXT, False  # no interval DataType; groupable as TEXT
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


def column_from_type_string(
    *, name: str, data_type: str | None, comment: str | None = None, zoned_types: frozenset[str] = frozenset(),
    json_types: frozenset[str] = frozenset(),
) -> ReflectedColumn:
    """A column read from a catalog type string; ``db_type`` kept where ``DataType`` is lossy (a list, exact
    numerics, a JSON type in ``json_types``, a zoned timestamp in ``zoned_types``)."""
    unwrapped = unwrap_clickhouse_wrapper_str(data_type) if data_type else ""
    element = info_schema_list_element(unwrapped)
    if element is not None:
        return ReflectedColumn(
            name=name, type=DataType.ARRAY, element_type=element, db_type=unwrapped, comment=clean_comment(comment),
        )
    mapped, is_float = info_schema_type(unwrapped)
    word = unwrapped.split("(")[0].upper().strip()
    lossy = is_exact_numeric_db_type(unwrapped) or word in json_types or unwrapped.upper() in zoned_types
    return ReflectedColumn(
        name=name, type=mapped, is_float=is_float, db_type=unwrapped if lossy else None,
        comment=clean_comment(comment),
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
    pk = set(primary_key)
    return [ks for ks in key_sets if set(ks) != pk]


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
