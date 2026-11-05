"""The deprecated category probes answer the category SLayer 1.6.0 answered for every input it classified.

The 1.6.0 classifier is copied verbatim below as the oracle. Spec: sql/client-contract.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Sequence
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.sql.client import (
    DelegatingSqlClient,
    SlayerSQLClient,
    _types_from_cursor,
    get_column_types_sync,
    type_categories,
)
from slayer.sql.dialects import dialect_for_ds_type
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import disposable_engine

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

DEPRECATED = pytest.mark.filterwarnings("ignore:.*get_column_types.*:FutureWarning")

# --- SLayer 1.6.0's classifier (slayer/sql/client.py, slayer/sql/dialects/snowflake.py), verbatim ---

_PG_OID_MAP: dict[int, str] = {
    16: "boolean", 20: "number", 21: "number", 23: "number", 26: "number", 700: "number", 701: "number",
    1700: "number", 790: "number", 18: "string", 25: "string", 1042: "string", 1043: "string", 1082: "time",
    1083: "time", 1114: "time", 1184: "time", 1186: "time",
}
_MYSQL_TYPE_MAP: dict[int, str] = {
    0: "number", 1: "boolean", 2: "number", 3: "number", 4: "number", 5: "number", 8: "number", 9: "number",
    16: "number", 246: "number", 7: "time", 10: "time", 11: "time", 12: "time", 13: "time", 14: "time",
    15: "string", 253: "string", 254: "string",
}
_ODBC_SQL_TYPE_MAP: dict[int, str] = {
    4: "number", 5: "number", -6: "number", -5: "number", 2: "number", 3: "number", 6: "number", 7: "number",
    8: "number", 1: "string", 12: "string", -1: "string", -8: "string", -9: "string", -10: "string",
    -152: "string", -11: "string", -7: "boolean", -2: "string", -3: "string", -4: "string", 91: "time",
    92: "time", 93: "time", -154: "time", -155: "time",
}
_SNOWFLAKE_TYPE_MAP: dict[int, str] = {
    0: "number", 1: "number", 2: "string", 3: "time", 4: "time", 5: "string", 6: "time", 7: "time", 8: "time",
    9: "string", 10: "string", 11: "string", 12: "time", 13: "boolean",
}


def _map_type_code_160(type_code: Any, db_type: str | None = None) -> str:
    if isinstance(type_code, int) and db_type == "snowflake":
        dialect_category = _SNOWFLAKE_TYPE_MAP.get(type_code)
        if dialect_category is not None:
            return dialect_category
    if db_type and "duckdb" in db_type.lower():
        type_code = str(type_code)
    if isinstance(type_code, str):
        tc = type_code.upper()
        if any(t in tc for t in ("TIMESTAMP", "DATE", "TIME", "INTERVAL")):
            return "time"
        if any(t in tc for t in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")):
            return "number"
        if any(t in tc for t in ("VARCHAR", "TEXT", "CHAR", "STRING", "BLOB", "ENUM")):
            return "string"
        if "BOOL" in tc:
            return "boolean"
        return "string"
    if isinstance(type_code, type):
        if issubclass(type_code, bool):
            return "boolean"
        if issubclass(type_code, (int, float)):
            return "number"
        if issubclass(type_code, str):
            return "string"
        return "string"
    if isinstance(type_code, int):
        if db_type and "mysql" in db_type.lower():
            return _MYSQL_TYPE_MAP.get(type_code, "string")
        if db_type and any(t in db_type.lower() for t in ("mssql", "sqlserver", "tsql")):
            return _ODBC_SQL_TYPE_MAP.get(type_code, "string")
        if db_type and "snowflake" in db_type.lower():
            return "string"
        return _PG_OID_MAP.get(type_code, "string")
    return "string"


def _extract_types_from_cursor_160(result: Any, db_type: str | None = None) -> dict[str, str]:
    columns = list(result.keys())
    cursor_desc = result.cursor.description
    if cursor_desc is not None:
        type_codes = [desc[1] for desc in cursor_desc]
        if any(tc is not None for tc in type_codes):
            return {col: _map_type_code_160(tc, db_type=db_type) for col, tc in zip(columns, type_codes)}
    rows = result.fetchall()
    if not rows:
        return dict.fromkeys(columns, "string")
    types = {}
    for col, val in zip(columns, rows[0]):
        if val is None:
            types[col] = "string"
        elif isinstance(val, bool):
            types[col] = "boolean"
        elif isinstance(val, (int, float)):
            types[col] = "number"
        elif isinstance(val, str):
            types[col] = "string"
        elif hasattr(val, "isoformat"):
            types[col] = "time"
        else:
            types[col] = "string"
    return types


# --- Description rows as each driver reports them ---


class _Cursor:
    def __init__(self, description: list[tuple[Any, ...]] | None) -> None:
        self.description = description


class _Result:
    """The slice of a ``CursorResult`` both probes read."""

    def __init__(self, *, columns: list[str], description: list[tuple[Any, ...]] | None,
                 rows: Sequence[tuple[Any, ...]] = ()) -> None:
        self._columns = columns
        self.cursor = _Cursor(description)
        self._rows = list(rows)

    def keys(self) -> list[str]:
        return self._columns

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


def _row(code: Any, *, length: int | None = None, precision: int | None = None,
         scale: int | None = None) -> tuple[Any, ...]:
    return ("c", code, None, length, precision, scale, True)


def _result(*rows: tuple[Any, ...]) -> _Result:
    return _Result(columns=[f"c{i}" for i in range(len(rows))], description=list(rows))


def _head(result: _Result, *, db_type: str) -> dict[str, str]:
    dialect = dialect_for_ds_type(db_type)
    return type_categories(_types_from_cursor(result, dialect=dialect), dialect=dialect)


def _case(db_type: str, label: str, row: tuple[Any, ...]) -> Any:
    return pytest.param(db_type, row, id=f"{db_type}-{label}")


_PG_EXTRA_OIDS = {
    17: "bytea", 114: "json", 142: "xml", 650: "cidr", 869: "inet", 1000: "bool[]", 1007: "int4[]",
    1009: "text[]", 1015: "varchar[]", 1016: "int8[]", 1022: "float8[]", 1231: "numeric[]", 1266: "timetz",
    1560: "bit", 1562: "varbit", 2950: "uuid", 3802: "jsonb", 99999: "unknown",
}
POSTGRES = [
    *(_case("postgres", f"oid{oid}", _row(oid)) for oid in sorted(_PG_OID_MAP)),
    *(_case("postgres", name, _row(oid)) for oid, name in sorted(_PG_EXTRA_OIDS.items())),
    _case("postgres", "numeric(10,2)", _row(1700, precision=10, scale=2)),
    _case("postgres", "numeric(38,0)", _row(1700, precision=38, scale=0)),
]

_MYSQL_EXTRA_CODES = {6: "null", 245: "json", 247: "enum", 248: "set", 249: "tinyblob", 250: "mediumblob",
                      251: "longblob", 252: "blob", 255: "geometry", 200: "unknown"}
MYSQL = [
    *(_case("mysql", f"field{code}", _row(code, length=11)) for code in sorted(_MYSQL_TYPE_MAP)),
    *(_case("mysql", name, _row(code, length=11)) for code, name in sorted(_MYSQL_EXTRA_CODES.items())),
    _case("mysql", "tinyint(1)", _row(1, length=1)),
    _case("mysql", "tinyint(4)", _row(1, length=4)),
    _case("mysql", "bit(1)", _row(16, length=1)),
    _case("mysql", "bit(8)", _row(16, length=8)),
    _case("mysql", "year", _row(13, length=4)),
    _case("mysql", "decimal(10,2)", _row(246, length=12, scale=2)),
    _case("mysql", "decimal(10,0)", _row(246, length=11, scale=0)),
]

MSSQL = [
    *(_case("mssql", f"odbc{code}", _row(code)) for code in sorted(_ODBC_SQL_TYPE_MAP)),
    _case("mssql", "odbc-unknown", _row(999)),
    *(_case("mssql", f"pyodbc-{t.__name__}", _row(t))
      for t in (bool, int, float, str, bytes, bytearray, dt.datetime, dt.date, dt.time, uuid.UUID)),
    _case("mssql", "pyodbc-Decimal(10,2)", _row(Decimal, precision=10, scale=2)),
    _case("mssql", "pyodbc-Decimal(19,0)", _row(Decimal, precision=19, scale=0)),
]

SNOWFLAKE = [
    *(_case("snowflake", f"field{code}", _row(code)) for code in sorted(_SNOWFLAKE_TYPE_MAP)),
    *(_case("snowflake", f"field{code}", _row(code)) for code in (14, 15, 99)),
    _case("snowflake", "number(38,0)", _row(0, precision=38, scale=0)),
    _case("snowflake", "number(10,2)", _row(0, precision=10, scale=2)),
]

# DuckDB spellings whose live description rows are read below.
DUCKDB_TYPES = (
    "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UBIGINT", "FLOAT", "DOUBLE",
    "DECIMAL(18,2)", "DECIMAL(38,0)", "VARCHAR", "BLOB", "BIT", "UUID", "JSON", "DATE", "TIME",
    "TIME WITH TIME ZONE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS", "INTERVAL",
    "INTEGER[]", "VARCHAR[]", "TIMESTAMP[]", "DOUBLE[3]", "STRUCT(a INTEGER)", "STRUCT(s VARCHAR)",
    "MAP(VARCHAR, INTEGER)", "ENUM('a', 'b')", "UNION(n INTEGER, s VARCHAR)",
)


def _duckdb_row(type_sql: str) -> tuple[Any, ...]:
    with closing(duckdb.connect()) as con:
        description = con.execute(f"SELECT CAST(NULL AS {type_sql}) AS c LIMIT 0").description
    assert description is not None
    return tuple(description[0])


@pytest.mark.parametrize(("db_type", "row"), [*POSTGRES, *MYSQL, *MSSQL, *SNOWFLAKE])
def test_a_described_column_gets_its_1_6_0_category(db_type: str, row: tuple[Any, ...]) -> None:
    assert _head(_result(row), db_type=db_type) == _extract_types_from_cursor_160(_result(row), db_type=db_type)


@pytest.mark.parametrize("type_sql", DUCKDB_TYPES)
def test_a_duckdb_column_gets_its_1_6_0_category(type_sql: str) -> None:
    result = _result(_duckdb_row(type_sql))
    assert _head(result, db_type="duckdb") == _extract_types_from_cursor_160(result, db_type="duckdb")


# pyodbc's ``type_code`` per SQL Server system type, which 1.6.0 classified.
_PYODBC_TYPE_CODES: dict[str, type] = {
    "bit": bool, "tinyint": int, "smallint": int, "int": int, "bigint": int, "decimal(10,2)": Decimal,
    "numeric(19,0)": Decimal, "money": Decimal, "smallmoney": Decimal, "float": float, "real": float,
    "char(3)": str, "varchar(50)": str, "nvarchar(max)": str, "text": str, "ntext": str, "xml": str,
    "uniqueidentifier": str, "binary(8)": bytes, "varbinary(max)": bytes, "image": bytes, "timestamp": bytes,
    "date": dt.date, "time(7)": dt.time, "datetime": dt.datetime, "datetime2(7)": dt.datetime,
    "smalldatetime": dt.datetime,
}


@pytest.mark.parametrize(("system_type", "type_code"), sorted(_PYODBC_TYPE_CODES.items()),
                         ids=sorted(_PYODBC_TYPE_CODES))
def test_a_sql_server_described_result_gets_its_1_6_0_category(system_type: str, type_code: type) -> None:
    dialect = dialect_for_ds_type("mssql")
    types = dialect.read_result_description([{"name": "c", "system_type_name": system_type, "is_hidden": False}])
    assert type_categories(types, dialect=dialect) == {"c": _map_type_code_160(type_code, db_type="mssql")}


_VALUES: list[Any] = [None, True, 7, 2.5, "x", b"x", Decimal("1.5"), dt.date(2024, 1, 2),
                      dt.datetime(2024, 1, 2, 3, 4), dt.time(3, 4), [1, 2]]


@pytest.mark.parametrize("db_type", ["postgres", "mysql", "duckdb", "sqlite"])
@pytest.mark.parametrize("description", ["none", "all-absent"])
def test_a_value_typed_result_gets_its_1_6_0_categories(db_type: str, description: str) -> None:
    columns = [f"c{i}" for i in range(len(_VALUES))]
    rows = None if description == "none" else [("c", None, None, None, None, None, True)] * len(_VALUES)

    def result() -> _Result:
        return _Result(columns=columns, description=rows, rows=[tuple(_VALUES)])

    assert _head(result(), db_type=db_type) == _extract_types_from_cursor_160(result(), db_type=db_type)


@pytest.mark.parametrize("db_type", ["postgres", "duckdb", "sqlite"])
def test_an_empty_value_typed_result_is_all_strings(db_type: str) -> None:
    result = _Result(columns=["a", "b"], description=None)
    assert _head(result, db_type=db_type) == {"a": "string", "b": "string"}


def test_an_absent_type_code_beside_a_typed_one_is_a_string() -> None:
    def result() -> _Result:
        return _Result(columns=["a", "b"], description=[_row(23), _row(None)], rows=[(1, 2)])

    assert _head(result(), db_type="postgres") == _extract_types_from_cursor_160(result(), db_type="postgres")


# --- Executed through the deprecated probes ---

DUCKDB_PROBE = (
    "SELECT INTERVAL 1 DAY AS iv, '10:00:00'::TIMETZ AS ttz, [1, 2]::INTEGER[] AS ints, ['a']::VARCHAR[] AS strs, "
    "{'a': 1} AS st, '{}'::JSON AS j, '101'::BIT AS bt, 1.5::DECIMAL(18,2) AS d, 1 AS n, 'x' AS s, "
    "TRUE AS b, DATE '2024-01-02' AS day"
)


def _duckdb_expected(path: str, sql: str) -> dict[str, str]:
    with closing(duckdb.connect(path)) as con:
        description = con.execute(f"SELECT * FROM ({sql}) AS _types LIMIT 0").description
    assert description is not None
    return {d[0]: _map_type_code_160(d[1], db_type="duckdb") for d in description}


@pytest.fixture
def duckdb_ds(tmp_path: Path):
    path = str(tmp_path / "live.duckdb")
    with closing(duckdb.connect(path)) as con:
        con.execute("CREATE TABLE spans (id INTEGER, took INTERVAL, tz TIMETZ, ints INTEGER[])")
        con.execute("INSERT INTO spans VALUES (1, INTERVAL 2 HOUR, '10:00:00', [1])")
    ds = DatasourceConfig(name="ddb", type="duckdb", database=path)
    yield ds
    engine_factory.invalidate_engine(ds)


@DEPRECATED
async def test_the_client_probe_on_duckdb(duckdb_ds: DatasourceConfig) -> None:
    client = SlayerSQLClient(datasource=duckdb_ds)
    try:
        with pytest.warns(FutureWarning, match=r"SlayerSQLClient\.get_column_types\(\)"):
            categories = await client.get_column_types(DUCKDB_PROBE)
    finally:
        await client.aclose()
    expected = _duckdb_expected(str(duckdb_ds.database), DUCKDB_PROBE)
    assert (categories["iv"], categories["ttz"], categories["ints"], categories["st"]) == (
        "time", "time", "number", "number")
    assert categories == expected


@DEPRECATED
async def test_the_delegating_probe_on_duckdb(duckdb_ds: DatasourceConfig) -> None:
    client = DelegatingSqlClient(SlayerSQLClient(datasource=duckdb_ds))
    try:
        with pytest.warns(FutureWarning, match=r"DelegatingSqlClient\.get_column_types\(\)"):
            categories = await client.get_column_types("SELECT took, tz FROM spans")
    finally:
        await client.aclose()
    assert categories == {"took": "time", "tz": "time"}


@DEPRECATED
def test_the_sync_probe_on_duckdb(duckdb_ds: DatasourceConfig) -> None:
    with disposable_engine(f"duckdb:///{duckdb_ds.database}") as engine, \
            pytest.warns(FutureWarning, match=r"get_column_types_sync\(\)"):
        categories = get_column_types_sync("SELECT took, tz, ints FROM spans", engine=engine, db_type="duckdb")
    assert categories == {"took": "time", "tz": "time", "ints": "number"}


@DEPRECATED
async def test_the_engine_probe_on_duckdb(duckdb_ds: DatasourceConfig, tmp_path: Path) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(duckdb_ds)
    await storage.save_model(SlayerModel(name="spans", sql_table="spans", data_source="ddb", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="took", type=DataType.TEXT),
        Column(name="tz", type=DataType.TEXT),
    ]))
    engine = SlayerQueryEngine(storage=storage)
    try:
        with pytest.warns(FutureWarning, match=r"SlayerQueryEngine\.get_column_types\(\)"):
            categories = await engine.get_column_types("spans", data_source="ddb")
    finally:
        await engine.aclose()
    assert (categories["took"], categories["tz"]) == ("time", "time")


SQLITE_SEED = (
    "CREATE TABLE t (i INTEGER, f REAL, s TEXT, b BLOB, late INTEGER, mixed)",
    "INSERT INTO t VALUES (1, 1.5, 'x', x'00', NULL, 7)",
    "INSERT INTO t VALUES (2, 2.5, 'y', x'01', 3, 'seven')",
    "CREATE TABLE empty (i INTEGER, s TEXT)",
)


@DEPRECATED
@pytest.mark.parametrize("sql", ["SELECT * FROM t", "SELECT * FROM empty"], ids=["rows", "empty"])
def test_the_sync_probe_on_sqlite(tmp_path: Path, sql: str) -> None:
    path = str(tmp_path / "live.db")
    with transaction(path) as con:
        for statement in SQLITE_SEED:
            con.execute(statement)
        con.commit()
        cursor = con.execute(f"SELECT * FROM ({sql}) AS _types LIMIT 1")
        columns = [d[0] for d in cursor.description]
        expected = _extract_types_from_cursor_160(
            _Result(columns=columns, description=None, rows=cursor.fetchall()), db_type="sqlite")
    with disposable_engine(f"sqlite:///{path}") as engine:
        categories = get_column_types_sync(sql, engine=engine, db_type="sqlite")
    assert categories == expected


PG_PROBE = (
    "SELECT INTERVAL '1 day' AS iv, TIMETZ '10:00+02' AS ttz, '{}'::json AS j, 1 AS n, 'x'::text AS s, "
    "1.5::numeric(10,2) AS d, 2::money AS m, TRUE AS b, DATE '2024-01-02' AS day, 1::oid AS o, "
    "B'101' AS bt, ARRAY[1] AS ints"
)


@DEPRECATED
async def test_the_client_probe_on_postgres(postgresql) -> None:
    info = postgresql.info
    ds = DatasourceConfig(name="pg", type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    with postgresql.cursor() as cur:
        cur.execute(f"SELECT * FROM ({PG_PROBE}) AS _types LIMIT 0")
        expected = {d.name: _map_type_code_160(d.type_code, db_type="postgres") for d in cur.description}
    client = SlayerSQLClient(datasource=ds)
    try:
        with pytest.warns(FutureWarning, match=r"SlayerSQLClient\.get_column_types\(\)"):
            categories = await client.get_column_types(PG_PROBE)
    finally:
        await client.aclose()
        engine_factory.invalidate_engine(ds)
    assert (categories["iv"], categories["n"], categories["s"]) == ("time", "number", "string")
    assert categories == expected
