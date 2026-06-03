"""Tests for SQL client helpers (type code mapping, retry-warning formatting)."""

import logging
import sqlite3

import pytest
import sqlalchemy.exc

from slayer.sql import client as sql_client
from slayer.sql.client import (
    _build_type_probe_sql,
    _execute_with_retry_async,
    _execute_with_retry_sync,
    _execute_with_retry_threaded,
    _is_transient_db_error,
    _map_type_code,
)


class TestMapTypeCode:
    """_map_type_code must correctly classify type codes from all driver families."""

    # --- Python type branch (SQLite/some drivers) ---

    def test_python_bool_type_is_boolean(self) -> None:
        """bool is a subclass of int; must be classified as boolean, not number."""
        assert _map_type_code(bool) == "boolean"

    def test_python_int_type_is_number(self) -> None:
        assert _map_type_code(int) == "number"

    def test_python_float_type_is_number(self) -> None:
        assert _map_type_code(float) == "number"

    def test_python_str_type_is_string(self) -> None:
        assert _map_type_code(str) == "string"

    # --- asyncpg OID integers (Postgres) ---

    def test_asyncpg_bool_oid(self) -> None:
        assert _map_type_code(16) == "boolean"

    def test_asyncpg_int4_oid(self) -> None:
        assert _map_type_code(23) == "number"

    def test_asyncpg_int8_oid(self) -> None:
        assert _map_type_code(20) == "number"

    def test_asyncpg_float8_oid(self) -> None:
        assert _map_type_code(701) == "number"

    def test_asyncpg_numeric_oid(self) -> None:
        assert _map_type_code(1700) == "number"

    def test_asyncpg_text_oid(self) -> None:
        assert _map_type_code(25) == "string"

    def test_asyncpg_varchar_oid(self) -> None:
        assert _map_type_code(1043) == "string"

    def test_asyncpg_timestamp_oid(self) -> None:
        assert _map_type_code(1114) == "time"

    def test_asyncpg_timestamptz_oid(self) -> None:
        assert _map_type_code(1184) == "time"

    def test_asyncpg_date_oid(self) -> None:
        assert _map_type_code(1082) == "time"

    # --- String branch (DuckDB) ---

    def test_duckdb_integer(self) -> None:
        assert _map_type_code("INTEGER") == "number"

    def test_duckdb_varchar(self) -> None:
        assert _map_type_code("VARCHAR") == "string"

    def test_duckdb_boolean(self) -> None:
        assert _map_type_code("BOOLEAN") == "boolean"

    def test_duckdb_timestamp(self) -> None:
        assert _map_type_code("TIMESTAMP") == "time"

    # --- Dialect-aware OID mapping ---

    def test_pg_oid_16_is_boolean(self) -> None:
        """Postgres OID 16 = bool."""
        assert _map_type_code(16, db_type="postgresql") == "boolean"

    def test_mysql_type_16_is_number(self) -> None:
        """MySQL field type 16 = BIT (not boolean)."""
        assert _map_type_code(16, db_type="mysql") == "number"

    def test_mysql_float_oid(self) -> None:
        """MySQL MYSQL_TYPE_FLOAT = 4."""
        assert _map_type_code(4, db_type="mysql") == "number"

    def test_mysql_decimal_oid(self) -> None:
        """MySQL MYSQL_TYPE_DECIMAL = 0."""
        assert _map_type_code(0, db_type="mysql") == "number"

    # --- SQL Server / pyodbc ODBC SQL type codes ---

    @pytest.mark.parametrize("db_type", ["mssql", "sqlserver", "tsql"])
    def test_tsql_integer_odbc_code_is_number(self, db_type: str) -> None:
        # SQL_INTEGER
        assert _map_type_code(4, db_type=db_type) == "number"

    @pytest.mark.parametrize("db_type", ["mssql", "sqlserver", "tsql"])
    def test_tsql_bigint_odbc_code_is_number(self, db_type: str) -> None:
        # SQL_BIGINT
        assert _map_type_code(-5, db_type=db_type) == "number"

    @pytest.mark.parametrize("db_type", ["mssql", "sqlserver", "tsql"])
    def test_tsql_varchar_odbc_code_is_string(self, db_type: str) -> None:
        # SQL_VARCHAR
        assert _map_type_code(12, db_type=db_type) == "string"

    @pytest.mark.parametrize("db_type", ["mssql", "sqlserver", "tsql"])
    def test_tsql_timestamp_odbc_code_is_time(self, db_type: str) -> None:
        # SQL_TYPE_TIMESTAMP
        assert _map_type_code(93, db_type=db_type) == "time"

    @pytest.mark.parametrize("db_type", ["mssql", "sqlserver", "tsql"])
    def test_tsql_bit_odbc_code_is_boolean(self, db_type: str) -> None:
        # SQL_BIT
        assert _map_type_code(-7, db_type=db_type) == "boolean"

    @pytest.mark.parametrize("db_type", ["mssql", "sqlserver", "tsql"])
    def test_tsql_datetimeoffset_odbc_code_is_time(self, db_type: str) -> None:
        # SQL_SS_TIMESTAMPOFFSET (datetimeoffset)
        assert _map_type_code(-154, db_type=db_type) == "time"

    @pytest.mark.parametrize("db_type", ["mssql", "sqlserver", "tsql"])
    def test_tsql_time2_odbc_code_is_time(self, db_type: str) -> None:
        # SQL_SS_TIME2 (time with fractional seconds)
        assert _map_type_code(-155, db_type=db_type) == "time"

    @pytest.mark.parametrize("db_type", ["mssql", "sqlserver", "tsql"])
    def test_tsql_xml_odbc_code_is_string(self, db_type: str) -> None:
        # SQL_SS_XML
        assert _map_type_code(-152, db_type=db_type) == "string"

    @pytest.mark.parametrize("db_type", ["mssql", "sqlserver", "tsql"])
    def test_tsql_guid_odbc_code_is_string(self, db_type: str) -> None:
        # SQL_GUID (uniqueidentifier)
        assert _map_type_code(-11, db_type=db_type) == "string"

    def test_tsql_does_not_fall_through_to_pg_oid_map(self) -> None:
        # Postgres OID 4 maps to nothing in PG map — it's SQL_INTEGER in ODBC.
        # Without the tsql branch it would return "string" (PG fallback).
        # With the tsql branch it correctly returns "number".
        assert _map_type_code(4, db_type="mssql") == "number"
        assert _map_type_code(4) == "string"  # Postgres fallback (OID 4 not in PG map)


def _make_op_error(orig_message: str = "database is locked") -> sqlalchemy.exc.OperationalError:
    """An OperationalError carrying a chosen DBAPI message in ``exc.orig``.

    Uses ``sqlite3.OperationalError`` as the wrapped DBAPI exception — that's
    the actual class SQLAlchemy puts in ``.orig`` when the SQLite driver
    raises an OperationalError, so the fake mirrors production semantics
    while satisfying Sonar's ``python:S112`` (no bare ``Exception``).
    """
    return sqlalchemy.exc.OperationalError(
        "SELECT 1", {}, sqlite3.OperationalError(orig_message),
    )


class TestIsTransientDbError:
    """``_is_transient_db_error`` separates retry-worthy from deterministic errors.

    Schema-level OperationalErrors (no such table, syntax error) used to
    burn 1s + 2s of retry sleep for nothing — a real UX hit on inspect_model
    and (massively) on the unit suite, where ~75 tests intentionally query
    a non-existent in-memory table.
    """

    @pytest.mark.parametrize("orig_message", [
        "database is locked",
        "deadlock detected",
        "lost connection to MySQL server during query",
        "BrokenPipeError: Broken pipe",
        "could not connect to server: Connection refused",
        "server closed the connection unexpectedly",
        "Connection refused",
        "Connection reset by peer",
        "Connection was killed",
        # Case-insensitive: upper-cased input still matches.
        "DATABASE IS LOCKED",
    ])
    def test_transient_messages_are_retried(self, orig_message: str) -> None:
        assert _is_transient_db_error(_make_op_error(orig_message)) is True

    @pytest.mark.parametrize("orig_message", [
        "no such table: orders",
        "no such column: revenue",
        "syntax error at or near \"FROM\"",
        "permission denied for table orders",
        "duplicate key value violates unique constraint",
        "relation \"orders\" does not exist",
    ])
    def test_deterministic_messages_are_not_retried(self, orig_message: str) -> None:
        assert _is_transient_db_error(_make_op_error(orig_message)) is False

    def test_disconnection_error_always_transient(self) -> None:
        """``DisconnectionError`` is by definition a connection drop — retry."""
        exc = sqlalchemy.exc.DisconnectionError("connection went away")
        assert _is_transient_db_error(exc) is True


class TestRetryFiltersDeterministicErrors:
    """Retry helpers must re-raise deterministic errors immediately.

    Before this filter was added, all three retry paths slept 1s+2s before
    finally raising — turning ~75 unit tests that intentionally hit a
    non-existent ``:memory:`` table into 3-15 s timeouts each.
    """

    async def test_async_no_such_table_raises_immediately(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls = {"n": 0}

        async def fake_execute(**_kwargs: object) -> list:  # NOSONAR(S7503) — must be async to replace _execute_sql_async (called via `await do_call()`)
            calls["n"] += 1
            raise _make_op_error("no such table: orders")

        monkeypatch.setattr(sql_client, "_execute_sql_async", fake_execute)

        with pytest.raises(sqlalchemy.exc.OperationalError, match="no such table"):
            await _execute_with_retry_async(
                sql="SELECT 1", engine=None, db_type="postgres",
                # Non-zero delays prove we don't sleep — if the filter regressed,
                # the test would still pass but get noticeably slower.
                initial_delay=10.0, max_delay=10.0,
            )

        assert calls["n"] == 1, "deterministic error must not retry"

    async def test_threaded_no_such_table_raises_immediately(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls = {"n": 0}

        def fake_execute(*_args: object, **_kwargs: object) -> list:
            calls["n"] += 1
            raise _make_op_error("no such table: orders")

        monkeypatch.setattr(sql_client, "_execute_sql_sync", fake_execute)

        with pytest.raises(sqlalchemy.exc.OperationalError, match="no such table"):
            await _execute_with_retry_threaded(
                sql="SELECT 1",
                connection_string="sqlite:///:memory:",
                db_type="sqlite",
                initial_delay=10.0, max_delay=10.0,
            )

        assert calls["n"] == 1

    def test_sync_no_such_table_raises_immediately(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls = {"n": 0}

        def fake_execute(*_args: object, **_kwargs: object) -> list:
            calls["n"] += 1
            raise _make_op_error("no such table: orders")

        monkeypatch.setattr(sql_client, "_execute_sql_sync", fake_execute)

        with pytest.raises(sqlalchemy.exc.OperationalError, match="no such table"):
            _execute_with_retry_sync(
                sql="SELECT 1",
                connection_string="sqlite:///:memory:",
                db_type="sqlite",
                initial_delay=10.0, max_delay=10.0,
            )

        assert calls["n"] == 1

    async def test_async_transient_still_retries(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Locking errors should still go through the retry path so the
        production behaviour for genuine flakes is unchanged."""
        calls = {"n": 0}

        async def fake_execute(**_kwargs: object) -> list:  # NOSONAR(S7503) — must be async to replace _execute_sql_async (called via `await do_call()`)
            calls["n"] += 1
            if calls["n"] == 1:
                raise _make_op_error("database is locked")
            return [{"ok": 1}]

        monkeypatch.setattr(sql_client, "_execute_sql_async", fake_execute)

        result = await _execute_with_retry_async(
            sql="SELECT 1", engine=None, db_type="postgres",
            initial_delay=0.0, max_delay=0.0,
        )

        assert result == [{"ok": 1}]
        assert calls["n"] == 2


class TestRetryEmptySqlExcerpt:
    """Empty/whitespace SQL must not raise IndexError when the retry warning fires.

    Regression test for the bug where `(sql or "").strip().splitlines()[0]`
    crashed inside the except handler, masking the real transient DB error.
    """

    @pytest.mark.parametrize("sql", ["", "   \n  "])
    async def test_async_empty_sql_logs_placeholder_and_retries(
        self,
        sql: str,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        calls = {"n": 0}

        async def fake_execute(**_kwargs: object) -> list:  # NOSONAR(S7503) — must be async to replace _execute_sql_async (called via `await do_call()`)
            calls["n"] += 1
            if calls["n"] == 1:
                raise _make_op_error()
            return [{"ok": 1}]

        monkeypatch.setattr(sql_client, "_execute_sql_async", fake_execute)

        with caplog.at_level(logging.WARNING, logger="slayer.sql.client"):
            result = await _execute_with_retry_async(
                sql=sql,
                engine=None,
                db_type="postgres",
                initial_delay=0.0,
                max_delay=0.0,
            )

        assert result == [{"ok": 1}]
        assert calls["n"] == 2
        assert any(
            "Transient DB error" in rec.getMessage() and "<empty sql>" in rec.getMessage()
            for rec in caplog.records
        )

    @pytest.mark.parametrize("sql", ["", "   \n  "])
    async def test_threaded_empty_sql_logs_placeholder_and_retries(
        self,
        sql: str,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        calls = {"n": 0}

        def fake_execute(*_args: object, **_kwargs: object) -> list:
            calls["n"] += 1
            if calls["n"] == 1:
                raise _make_op_error()
            return [{"ok": 1}]

        monkeypatch.setattr(sql_client, "_execute_sql_sync", fake_execute)

        with caplog.at_level(logging.WARNING, logger="slayer.sql.client"):
            result = await _execute_with_retry_threaded(
                sql=sql,
                connection_string="sqlite:///:memory:",
                db_type="sqlite",
                initial_delay=0.0,
                max_delay=0.0,
            )

        assert result == [{"ok": 1}]
        assert calls["n"] == 2
        assert any(
            "Transient DB error" in rec.getMessage() and "<empty sql>" in rec.getMessage()
            for rec in caplog.records
        )

    @pytest.mark.parametrize("sql", ["", "   \n  "])
    def test_sync_empty_sql_logs_placeholder_and_retries(
        self,
        sql: str,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        calls = {"n": 0}

        def fake_execute(*_args: object, **_kwargs: object) -> list:
            calls["n"] += 1
            if calls["n"] == 1:
                raise _make_op_error()
            return [{"ok": 1}]

        monkeypatch.setattr(sql_client, "_execute_sql_sync", fake_execute)

        with caplog.at_level(logging.WARNING, logger="slayer.sql.client"):
            result = _execute_with_retry_sync(
                sql=sql,
                connection_string="sqlite:///:memory:",
                db_type="sqlite",
                initial_delay=0.0,
                max_delay=0.0,
            )

        assert result == [{"ok": 1}]
        assert calls["n"] == 2
        assert any(
            "Transient DB error" in rec.getMessage() and "<empty sql>" in rec.getMessage()
            for rec in caplog.records
        )


class TestBuildTypeProbeSQL:
    """_build_type_probe_sql must emit dialect-appropriate row-limiting syntax."""

    BASE = "SELECT id, name FROM orders"

    def test_standard_dialect_uses_limit_0(self) -> None:
        sql = _build_type_probe_sql(self.BASE, db_type="postgres")
        assert "LIMIT 0" in sql
        assert "TOP" not in sql

    def test_sqlite_uses_limit_1(self) -> None:
        sql = _build_type_probe_sql(self.BASE, db_type="sqlite")
        assert "LIMIT 1" in sql
        assert "TOP" not in sql

    def test_mssql_uses_top_0(self) -> None:
        sql = _build_type_probe_sql(self.BASE, db_type="mssql")
        assert "SELECT TOP 0" in sql
        assert "LIMIT" not in sql

    def test_sqlserver_alias_uses_top_0(self) -> None:
        sql = _build_type_probe_sql(self.BASE, db_type="sqlserver")
        assert "SELECT TOP 0" in sql
        assert "LIMIT" not in sql

    def test_tsql_alias_uses_top_0(self) -> None:
        sql = _build_type_probe_sql(self.BASE, db_type="tsql")
        assert "SELECT TOP 0" in sql
        assert "LIMIT" not in sql

    def test_none_db_type_uses_limit(self) -> None:
        sql = _build_type_probe_sql(self.BASE, db_type=None)
        assert "LIMIT 0" in sql
