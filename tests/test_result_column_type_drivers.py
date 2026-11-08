"""Per-driver cursor descriptions of the typed column-type probe (spec: database/client-contract)."""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Callable
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy.exc

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig
from slayer.sql import client as sql_client
from slayer.sql.client import ResultColumnType, SlayerSQLClient, get_result_column_types_sync
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.reflection import info_schema_type
from slayer.sql.render.value_expr import keeps_native_type
from tests._ch_fake_http import route_engine

_DESCRIBE_COLUMNS = (
    "is_hidden", "column_ordinal", "name", "is_nullable", "system_type_id",
    "system_type_name", "max_length", "precision", "scale",
)


def _row(name: str, code: Any, *, size: int | None = None, precision: int | None = None,
         scale: int | None = None) -> tuple[Any, ...]:
    """One DB-API ``cursor.description`` row."""
    return (name, code, None, size, precision, scale, True)


def _describe(ds_type: str, row: tuple[Any, ...]) -> ResultColumnType | None:
    return dialect_for_ds_type(ds_type).describe_cursor_column(row)


# ─────────────────────────── Postgres / Redshift ────────────────────────────

_PG_ROWS = [
    (_row("c", 21), ResultColumnType(type=DataType.INT, db_type="smallint")),
    (_row("c", 23), ResultColumnType(type=DataType.INT, db_type="integer")),
    (_row("c", 20), ResultColumnType(type=DataType.INT, db_type="bigint")),
    (_row("c", 26), ResultColumnType(type=DataType.INT, db_type="oid")),
    (_row("c", 700), ResultColumnType(type=DataType.DOUBLE, db_type="real")),
    (_row("c", 701), ResultColumnType(type=DataType.DOUBLE, db_type="double precision")),
    (_row("c", 790), ResultColumnType(type=DataType.DOUBLE, db_type="money")),
    (_row("c", 1700, precision=10, scale=0), ResultColumnType(type=DataType.INT, db_type="numeric(10,0)")),
    (_row("c", 1700, precision=10, scale=2), ResultColumnType(type=DataType.DOUBLE, db_type="numeric(10,2)")),
    (_row("c", 1700), ResultColumnType(type=DataType.DOUBLE, db_type="numeric")),
    (_row("c", 16), ResultColumnType(type=DataType.BOOLEAN, db_type="boolean")),
    (_row("c", 25), ResultColumnType(type=DataType.TEXT, db_type="text")),
    (_row("c", 1043), ResultColumnType(type=DataType.TEXT, db_type="character varying")),
    (_row("c", 1042), ResultColumnType(type=DataType.TEXT, db_type="character")),
    (_row("c", 18), ResultColumnType(type=DataType.TEXT, db_type='"char"')),
    (_row("c", 1082), ResultColumnType(type=DataType.DATE, db_type="date")),
    (_row("c", 1114), ResultColumnType(type=DataType.TIMESTAMP, db_type="timestamp without time zone")),
    (_row("c", 1184), ResultColumnType(type=DataType.TIMESTAMP, db_type="timestamp with time zone")),
    (_row("c", 114), ResultColumnType(type=DataType.UNKNOWN, db_type="json")),
    (_row("c", 3802), ResultColumnType(type=DataType.TEXT, db_type="jsonb")),
    (_row("c", 2950), ResultColumnType(type=DataType.TEXT, db_type="uuid")),
    (_row("c", 99999), ResultColumnType(type=DataType.TEXT)),
]


class TestPostgresDescriptions:
    @pytest.mark.parametrize("ds_type", ["postgres", "redshift"])
    @pytest.mark.parametrize(("row", "expected"), _PG_ROWS)
    def test_oid(self, ds_type: str, row: tuple[Any, ...], expected: ResultColumnType) -> None:
        assert _describe(ds_type, row) == expected

    @pytest.mark.parametrize("oid", [1083, 1186])
    def test_time_of_day_and_interval_as_reflection_maps_them(self, oid: int) -> None:
        described = _describe("postgres", _row("c", oid))
        assert described is not None
        assert described.db_type is not None
        assert described.type == info_schema_type(described.db_type)[0]


# ─────────────────────────────── MySQL / MariaDB ────────────────────────────

def _mysql_row(code: int, length: int, scale: int = 0) -> tuple[Any, ...]:
    """A PyMySQL / aiomysql description row: the column length is both internal size and precision."""
    return _row("c", code, size=length, precision=length, scale=scale)


_MYSQL_ROWS = [
    pytest.param(_mysql_row(1, 1), DataType.BOOLEAN, id="bool"),
    pytest.param(_mysql_row(1, 4), DataType.INT, id="tinyint"),
    pytest.param(_mysql_row(16, 1), DataType.BOOLEAN, id="bit-1"),
    pytest.param(_mysql_row(16, 8), DataType.TEXT, id="bit-8"),
    pytest.param(_mysql_row(2, 6), DataType.INT, id="smallint"),
    pytest.param(_mysql_row(3, 11), DataType.INT, id="int"),
    pytest.param(_mysql_row(8, 20), DataType.INT, id="bigint"),
    pytest.param(_mysql_row(9, 9), DataType.INT, id="mediumint"),
    pytest.param(_mysql_row(13, 4), DataType.INT, id="year"),
    pytest.param(_mysql_row(4, 12, 31), DataType.DOUBLE, id="float"),
    pytest.param(_mysql_row(5, 22, 31), DataType.DOUBLE, id="double"),
    pytest.param(_mysql_row(246, 11), DataType.INT, id="decimal-scale-0"),
    pytest.param(_mysql_row(246, 12, 2), DataType.DOUBLE, id="decimal-scale-2"),
    pytest.param(_mysql_row(10, 10), DataType.DATE, id="date"),
    pytest.param(_mysql_row(12, 19), DataType.TIMESTAMP, id="datetime"),
    pytest.param(_mysql_row(7, 19), DataType.TIMESTAMP, id="timestamp"),
    pytest.param(_mysql_row(253, 50), DataType.TEXT, id="varchar"),
]


class TestMysqlDescriptions:
    @pytest.mark.parametrize("ds_type", ["mysql", "mariadb"])
    @pytest.mark.parametrize(("row", "expected"), _MYSQL_ROWS)
    def test_field_type(self, ds_type: str, row: tuple[Any, ...], expected: DataType) -> None:
        described = _describe(ds_type, row)
        assert described is not None
        assert described.type == expected
        assert described.db_type

    def test_decimal_name_carries_its_scale(self) -> None:
        described = _describe("mysql", _mysql_row(246, 12, 2))
        assert described is not None
        assert described.db_type is not None
        assert described.db_type.upper().startswith("DECIMAL(")
        assert described.db_type.replace(" ", "").endswith(",2)")


# ───────────────────────────────── Snowflake ────────────────────────────────

class TestSnowflakeDescriptions:
    @pytest.mark.parametrize(("row", "expected"), [
        (_row("c", 0, precision=38, scale=0), ResultColumnType(type=DataType.INT, db_type="NUMBER(38,0)")),
        (_row("c", 0, precision=10, scale=2), ResultColumnType(type=DataType.DOUBLE, db_type="NUMBER(10,2)")),
        (_row("c", 1), ResultColumnType(type=DataType.DOUBLE, db_type="FLOAT")),
    ])
    def test_numbers(self, row: tuple[Any, ...], expected: ResultColumnType) -> None:
        assert _describe("snowflake", row) == expected

    @pytest.mark.parametrize(("code", "expected"), [
        (2, DataType.TEXT), (3, DataType.DATE), (4, DataType.TIMESTAMP), (6, DataType.TIMESTAMP),
        (7, DataType.TIMESTAMP), (8, DataType.TIMESTAMP), (13, DataType.BOOLEAN),
    ])
    def test_scalar_codes(self, code: int, expected: DataType) -> None:
        described = _describe("snowflake", _row("c", code))
        assert described is not None
        assert described.type == expected
        assert described.db_type

    @pytest.mark.parametrize("code", [5, 9, 10, 11, 12])
    def test_time_semi_structured_and_binary_as_reflection_maps_them(self, code: int) -> None:
        described = _describe("snowflake", _row("c", code))
        assert described is not None
        assert described.db_type is not None
        assert described.type == info_schema_type(described.db_type)[0]


# ───────────────────────── type-name codes (base mapper) ────────────────────

class TestTypeNameCodes:
    @pytest.mark.parametrize(("ds_type", "name", "expected"), [
        ("duckdb", "INTEGER", DataType.INT),
        ("duckdb", "HUGEINT", DataType.INT),
        ("duckdb", "DECIMAL(18,0)", DataType.INT),
        ("duckdb", "DECIMAL(18,2)", DataType.DOUBLE),
        ("duckdb", "DOUBLE", DataType.DOUBLE),
        ("duckdb", "VARCHAR", DataType.TEXT),
        ("duckdb", "BOOLEAN", DataType.BOOLEAN),
        ("duckdb", "DATE", DataType.DATE),
        ("duckdb", "TIMESTAMP WITH TIME ZONE", DataType.TIMESTAMP),
        ("duckdb", "INTERVAL", DataType.TEXT),
        ("duckdb", "INTEGER[]", DataType.ARRAY),
        ("duckdb", "MAP(VARCHAR, INTEGER)", DataType.TEXT),
        ("duckdb", "JSON", DataType.TEXT),
        ("bigquery", "INTEGER", DataType.INT),
        ("bigquery", "FLOAT", DataType.DOUBLE),
        ("bigquery", "NUMERIC", DataType.DOUBLE),
        ("bigquery", "STRING", DataType.TEXT),
        ("bigquery", "RECORD", DataType.TEXT),
        ("trino", "bigint", DataType.INT),
        ("trino", "decimal(10,2)", DataType.DOUBLE),
        ("trino", "array(integer)", DataType.ARRAY),
        ("trino", "row(a integer)", DataType.TEXT),
        ("clickhouse", "Nullable(Int32)", DataType.INT),
        ("clickhouse", "UInt8", DataType.INT),
        ("clickhouse", "Date32", DataType.DATE),
        ("clickhouse", "Array(Int32)", DataType.ARRAY),
    ])
    def test_name(self, ds_type: str, name: str, expected: DataType) -> None:
        described = _describe(ds_type, _row("c", name))
        assert described is not None
        assert described.type == expected
        assert described.db_type == name


# ───────────────────────────── fake engines ─────────────────────────────────

class _Result:
    """A row-returning result as SQLAlchemy hands it over."""

    def __init__(self, *, keys: list[str], rows: list[tuple[Any, ...]],
                 description: list[tuple[Any, ...]] | None = None) -> None:
        self._keys = keys
        self._rows = rows
        self.returns_rows = True
        self.cursor = SimpleNamespace(
            description=description if description is not None else [_row(k, None) for k in keys],
        )

    def keys(self) -> list[str]:
        return list(self._keys)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def first(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)

    def mappings(self) -> Any:
        dicts = [dict(zip(self._keys, row)) for row in self._rows]
        return SimpleNamespace(all=lambda: dicts, fetchall=lambda: dicts, __iter__=lambda: iter(dicts))


Responder = Callable[[str], _Result]


class _Engines:
    """A fake sync engine and a fake async engine sharing one statement log."""

    def __init__(self, respond: Responder) -> None:
        self.log: list[str] = []
        self.async_connects = 0
        self._respond = respond

    def _run(self, sql: str) -> _Result:
        self.log.append(sql)
        return self._respond(sql)

    def _sync_conn(self) -> MagicMock:
        conn = MagicMock()
        conn.exec_driver_sql.side_effect = lambda sql, **_kw: self._run(sql)
        return conn

    def sync(self) -> MagicMock:
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = self._sync_conn()
        return engine

    def async_(self) -> MagicMock:
        async def _exec(sql: str, **_kw: Any) -> _Result:
            return self._run(sql)

        def _connect() -> MagicMock:
            self.async_connects += 1
            sync_conn = self._sync_conn()
            conn = MagicMock()
            conn.exec_driver_sql = AsyncMock(side_effect=_exec)
            conn.run_sync = AsyncMock(side_effect=lambda fn, *a, **kw: fn(sync_conn, *a, **kw))
            conn.rollback = AsyncMock()
            conn.get_raw_connection = AsyncMock()
            ctx = MagicMock()
            ctx.__aenter__ = AsyncMock(return_value=conn)
            ctx.__aexit__ = AsyncMock(return_value=False)
            return ctx

        engine = MagicMock()
        engine.connect.side_effect = _connect
        return engine


def _probe_sync(engines: _Engines, sql: str, *, db_type: str) -> dict[str, ResultColumnType]:
    return get_result_column_types_sync(sql, engine=engines.sync(), db_type=db_type)  # pyright: ignore[reportArgumentType]


# ─────────────────────────── generic first-row fallback ─────────────────────

class TestFirstRowFallback:
    def test_untyped_description_types_from_the_first_row(self) -> None:
        keys = ["b", "i", "f", "n", "ts", "d", "s", "z", "x"]
        row = (True, 3, 1.5, Decimal("2.5"), dt.datetime(2024, 1, 1, 1), dt.date(2024, 1, 1), "x", None, b"\x00")
        engines = _Engines(lambda _sql: _Result(keys=keys, rows=[row]))
        result = _probe_sync(engines, "SELECT 1", db_type="duckdb")
        assert result == {
            "b": ResultColumnType(type=DataType.BOOLEAN),
            "i": ResultColumnType(type=DataType.DOUBLE),
            "f": ResultColumnType(type=DataType.DOUBLE),
            "n": ResultColumnType(type=DataType.DOUBLE),
            "ts": ResultColumnType(type=DataType.TIMESTAMP),
            "d": ResultColumnType(type=DataType.DATE),
            "s": ResultColumnType(type=DataType.TEXT),
            "z": ResultColumnType(type=DataType.TEXT),
            "x": ResultColumnType(type=DataType.TEXT),
        }

    def test_fallback_is_per_column(self) -> None:
        description = [_row("a", "INTEGER"), _row("b", None)]
        engines = _Engines(lambda _sql: _Result(keys=["a", "b"], rows=[(1, 2.5)], description=description))
        assert _probe_sync(engines, "SELECT 1", db_type="duckdb") == {
            "a": ResultColumnType(type=DataType.INT, db_type="INTEGER"),
            "b": ResultColumnType(type=DataType.DOUBLE),
        }


# ───────────────────────── Postgres probes on the client's one driver ─────────

async def test_postgres_probe_runs_on_the_async_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    engines = _Engines(lambda _sql: _Result(
        keys=["x", "n"], rows=[],
        description=[_row("x", 23), _row("n", 1700, precision=12, scale=0)],
    ))
    monkeypatch.setattr(sql_client.engine_factory, "get_engine", MagicMock(side_effect=AssertionError("factory engine")))
    async_engine = engines.async_()
    monkeypatch.setattr(sql_client, "create_async_engine", lambda *_a, **_k: async_engine)
    client = SlayerSQLClient(datasource=DatasourceConfig(
        name="pg", type="postgres", connection_string="postgresql://u@db.invalid/d",
    ))
    result = await client.get_result_column_types("SELECT 1 AS x, 1::numeric(12,0) AS n")
    assert engines.async_connects == 1
    assert any("SELECT 1 AS x" in s for s in engines.log)
    assert result == {
        "x": ResultColumnType(type=DataType.INT, db_type="integer"),
        "n": ResultColumnType(type=DataType.INT, db_type="numeric(12,0)"),
    }


# ─────────────────────────────── SQL Server ─────────────────────────────────

_TSQL_SQL = "SELECT i, m, t, s, rv FROM dbo.t WHERE s <> 'x'"


def _odbc_error(code: int, message: str) -> sqlalchemy.exc.ProgrammingError:
    orig = Exception("42000", f"[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]{message} ({code}) "
                              "(SQLExecDirectW)")
    return sqlalchemy.exc.ProgrammingError("EXEC sp_describe_first_result_set", {}, orig)


def _is_describe(sql: str) -> bool:
    return sql.startswith("EXEC sp_describe_first_result_set @tsql = N'")


class TestSqlServerDescribe:
    @pytest.mark.parametrize("scale", [0, 2])
    def test_a_fallback_decimal_keeps_its_native_type(self, scale: int) -> None:
        described = _describe("mssql", _row("m", Decimal, precision=10, scale=scale))
        assert described is not None
        derived = Column(name="d", sql="m * 2", type=described.type, db_type=described.db_type)
        assert keeps_native_type(col=derived, dialect=dialect_for_ds_type("mssql")), described

    def test_declared_type_names(self) -> None:
        rows = [
            (False, 1, "i", True, 56, "int", 4, 10, 0),
            (False, 2, "m", True, 106, "decimal(10,2)", 9, 10, 2),
            (False, 3, "t", True, 42, "datetime2(7)", 8, 27, 7),
            (False, 4, "s", True, 231, "nvarchar(50)", 100, 0, 0),
            (False, 5, "rv", False, 189, "timestamp", 8, 0, 0),
        ]
        engines = _Engines(lambda sql: _Result(keys=list(_DESCRIBE_COLUMNS), rows=rows if _is_describe(sql) else []))
        result = _probe_sync(engines, _TSQL_SQL, db_type="mssql")
        assert result == {
            "i": ResultColumnType(type=DataType.INT, db_type="int"),
            "m": ResultColumnType(type=DataType.DOUBLE, db_type="decimal(10,2)"),
            "t": ResultColumnType(type=DataType.TIMESTAMP, db_type="datetime2(7)"),
            "s": ResultColumnType(type=DataType.TEXT, db_type="nvarchar(50)"),
            "rv": ResultColumnType(type=DataType.TEXT, db_type="timestamp"),
        }
        [describe] = [s for s in engines.log if _is_describe(s)]
        assert "<> ''x''" in describe
        assert not any("TOP 0" in s and not _is_describe(s) for s in engines.log)

    def test_a_clr_type_without_a_system_type_name_is_unnamed_text(self) -> None:
        rows = [(False, 1, "g", True, 240, None, -1, 0, 0), (False, 2, "i", True, 56, "int", 4, 10, 0)]
        engines = _Engines(lambda sql: _Result(keys=list(_DESCRIBE_COLUMNS), rows=rows if _is_describe(sql) else []))
        assert _probe_sync(engines, "SELECT g, i FROM dbo.t", db_type="mssql") == {
            "g": ResultColumnType(type=DataType.TEXT),
            "i": ResultColumnType(type=DataType.INT, db_type="int"),
        }

    @pytest.mark.parametrize("code", [11500, 11526, 11541])
    def test_falls_back_when_the_result_cannot_be_described(self, code: int) -> None:
        description = [
            _row("i", int, precision=10, scale=0),
            _row("m", Decimal, precision=10, scale=2),
            _row("m0", Decimal, precision=10, scale=0),
            _row("t", dt.datetime),
            _row("d", dt.date),
            _row("s", str, size=50),
            _row("b", bool),
            _row("f", float),
        ]

        def respond(sql: str) -> _Result:
            if _is_describe(sql):
                raise _odbc_error(code, "The metadata could not be determined because statement uses a temp table.")
            return _Result(keys=[d[0] for d in description], rows=[], description=description)

        engines = _Engines(respond)
        result = _probe_sync(engines, "SELECT * FROM #t", db_type="mssql")
        assert result == {
            "i": ResultColumnType(type=DataType.INT),
            "m": ResultColumnType(type=DataType.DOUBLE, db_type="decimal(10,2)"),
            "m0": ResultColumnType(type=DataType.INT, db_type="decimal(10,0)"),
            "t": ResultColumnType(type=DataType.TIMESTAMP),
            "d": ResultColumnType(type=DataType.DATE),
            "s": ResultColumnType(type=DataType.TEXT),
            "b": ResultColumnType(type=DataType.BOOLEAN),
            "f": ResultColumnType(type=DataType.DOUBLE),
        }
        assert any("TOP 0" in s and not _is_describe(s) for s in engines.log)

    @pytest.mark.parametrize("code", [208, 11499, 11542])
    def test_any_other_error_reaches_the_caller_unchanged(self, code: int) -> None:
        error = _odbc_error(code, "Invalid object name 'dbo.nope'.")

        def respond(sql: str) -> _Result:
            if _is_describe(sql):
                raise error
            return _Result(keys=["x"], rows=[])

        engines = _Engines(respond)
        with pytest.raises(sqlalchemy.exc.ProgrammingError) as info:
            _probe_sync(engines, "SELECT x FROM dbo.nope", db_type="mssql")
        assert info.value is error
        assert all(_is_describe(s) for s in engines.log if "dbo.nope" in s)

    def test_async_client_describes_too(self, monkeypatch: pytest.MonkeyPatch) -> None:
        rows = [(False, 1, "i", True, 56, "int", 4, 10, 0)]
        engines = _Engines(lambda sql: _Result(keys=list(_DESCRIBE_COLUMNS), rows=rows if _is_describe(sql) else []))
        route_engine(monkeypatch, engines.sync())  # pyright: ignore[reportArgumentType]
        client = SlayerSQLClient(datasource=DatasourceConfig(
            name="ms", type="mssql", connection_string="mssql+pyodbc://u@db.invalid/d",
        ))
        result = asyncio.run(client.get_result_column_types("SELECT 1 AS i"))
        assert result == {"i": ResultColumnType(type=DataType.INT, db_type="int")}
