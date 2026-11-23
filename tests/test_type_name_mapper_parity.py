"""One type name maps to one ``DataType`` through catalog strings, SQLAlchemy types and the probe."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql, postgresql

from slayer.core.enums import DataType
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.reflection import (
    ResultColumnType,
    column_from_sa_type,
    column_from_type_string,
    info_schema_type,
)

_CASES = [
    ("INTEGER", sa.INTEGER(), DataType.INTEGER),
    ("BIGINT", sa.BIGINT(), DataType.INTEGER),
    ("UInt8", None, DataType.INTEGER),
    ("NUMERIC(10,0)", sa.NUMERIC(10, 0), DataType.INTEGER),
    ("NUMERIC(10,2)", sa.NUMERIC(10, 2), DataType.FLOAT),
    ("NUMERIC", sa.NUMERIC(), DataType.FLOAT),
    ("DOUBLE PRECISION", postgresql.DOUBLE_PRECISION(), DataType.FLOAT),
    ("MONEY", postgresql.MONEY(), DataType.FLOAT),
    ("VARCHAR(10)", sa.VARCHAR(10), DataType.STRING),
    ("BOOLEAN", sa.BOOLEAN(), DataType.BOOLEAN),
    ("DATE", sa.DATE(), DataType.DATE),
    ("DATE32", None, DataType.DATE),
    ("TIMESTAMP", sa.TIMESTAMP(), DataType.DATETIME),
    ("INTERVAL", postgresql.INTERVAL(), DataType.STRING),
    ("INTEGER[]", postgresql.ARRAY(sa.INTEGER()), DataType.ARRAY),
    ("ARRAY(INTEGER)", None, DataType.ARRAY),
    ("Array(Int32)", None, DataType.ARRAY),
    ("STRUCT(a INTEGER)", None, DataType.STRING),
    ("ROW(a INTEGER)", None, DataType.STRING),
    ("MAP(VARCHAR, INTEGER)", None, DataType.STRING),
    ("JSON", sa.JSON(), DataType.OPAQUE),
    ("XML", None, DataType.OPAQUE),
    ("GEOMETRY", None, DataType.OPAQUE),
    ("JSONB", postgresql.JSONB(), DataType.STRING),
    ("UUID", sa.Uuid(), DataType.STRING),
    ("NOT_A_KNOWN_TYPE", None, DataType.STRING),
]


@pytest.mark.parametrize(("name", "sa_type", "expected"), _CASES, ids=[c[0] for c in _CASES])
def test_same_name_same_type(name: str, sa_type: sa.types.TypeEngine | None, expected: DataType) -> None:
    assert info_schema_type(name)[0] == expected
    assert column_from_type_string(name="c", data_type=name).type == expected
    described = dialect_for_ds_type("trino").describe_cursor_column(("c", name, None, None, None, None, None))
    assert described is not None
    assert described.type == expected
    if sa_type is not None:
        assert column_from_sa_type(name="c", sa_type=sa_type).type == expected


@pytest.mark.parametrize("sa_type", [mssql.TIMESTAMP(), mssql.ROWVERSION()])
def test_sql_server_timestamp_is_rowversion(sa_type: sa.types.TypeEngine) -> None:
    assert column_from_sa_type(name="c", sa_type=sa_type).type == DataType.STRING


def test_duckdb_json_is_text() -> None:
    assert dialect_for_ds_type("duckdb").describe_cursor_column(("c", "JSON", None, None, None, None, None)) == (
        ResultColumnType(type=DataType.STRING, db_type="JSON")
    )
