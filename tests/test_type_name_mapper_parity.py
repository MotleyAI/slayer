"""One type name maps to one ``DataType`` through catalog strings, SQLAlchemy types and the probe."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql, postgresql

from slayer.core.enums import DataType
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.reflection import column_from_sa_type, column_from_type_string, info_schema_type

_CASES = [
    ("INTEGER", sa.INTEGER(), DataType.INT),
    ("BIGINT", sa.BIGINT(), DataType.INT),
    ("UInt8", None, DataType.INT),
    ("NUMERIC(10,0)", sa.NUMERIC(10, 0), DataType.INT),
    ("NUMERIC(10,2)", sa.NUMERIC(10, 2), DataType.DOUBLE),
    ("NUMERIC", sa.NUMERIC(), DataType.DOUBLE),
    ("DOUBLE PRECISION", postgresql.DOUBLE_PRECISION(), DataType.DOUBLE),
    ("MONEY", postgresql.MONEY(), DataType.DOUBLE),
    ("VARCHAR(10)", sa.VARCHAR(10), DataType.TEXT),
    ("BOOLEAN", sa.BOOLEAN(), DataType.BOOLEAN),
    ("DATE", sa.DATE(), DataType.DATE),
    ("DATE32", None, DataType.DATE),
    ("TIMESTAMP", sa.TIMESTAMP(), DataType.TIMESTAMP),
    ("INTERVAL", postgresql.INTERVAL(), DataType.TEXT),
    ("INTEGER[]", postgresql.ARRAY(sa.INTEGER()), DataType.TEXT),
    ("ARRAY(INTEGER)", None, DataType.TEXT),
    ("Array(Int32)", None, DataType.TEXT),
    ("STRUCT(a INTEGER)", None, DataType.TEXT),
    ("ROW(a INTEGER)", None, DataType.TEXT),
    ("MAP(VARCHAR, INTEGER)", None, DataType.TEXT),
    ("JSON", sa.JSON(), DataType.UNKNOWN),
    ("XML", None, DataType.UNKNOWN),
    ("GEOMETRY", None, DataType.UNKNOWN),
    ("JSONB", postgresql.JSONB(), DataType.TEXT),
    ("UUID", sa.Uuid(), DataType.TEXT),
    ("NOT_A_KNOWN_TYPE", None, DataType.TEXT),
]


@pytest.mark.parametrize(("name", "sa_type", "expected"), _CASES, ids=[c[0] for c in _CASES])
def test_same_name_same_type(name: str, sa_type: sa.types.TypeEngine | None, expected: DataType) -> None:
    assert info_schema_type(name)[0] == expected
    assert column_from_type_string(name="c", data_type=name).type == expected
    described = dialect_for_ds_type("duckdb").describe_cursor_column(("c", name, None, None, None, None, None))
    assert described is not None
    assert described.type == expected
    if sa_type is not None:
        assert column_from_sa_type(name="c", sa_type=sa_type).type == expected


@pytest.mark.parametrize("sa_type", [mssql.TIMESTAMP(), mssql.ROWVERSION()])
def test_sql_server_timestamp_is_rowversion(sa_type: sa.types.TypeEngine) -> None:
    assert column_from_sa_type(name="c", sa_type=sa_type).type == DataType.TEXT
