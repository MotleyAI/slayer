"""Tests for SQL client helpers (type code mapping, retry-warning formatting)."""

import logging
import sqlite3
from unittest.mock import AsyncMock, patch

import pytest
import sqlalchemy as sa
import sqlalchemy.exc

from slayer.core.models import DatasourceConfig
from slayer.sql import client as sql_client
from slayer.sql.client import (
    _build_type_probe_sql,
    _execute_with_retry_async,
    _execute_with_retry_sync,
    _execute_with_retry_threaded,
    _get_column_types_sync,
    _is_auth_failure,
    _is_transient_db_error,
    _is_unreachable_db_error,
    _map_type_code,
    _read_only_transaction_sql,
    build_sql_model_trial_query,
    classify_model_sql,
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

    def test_duckdb_driver_type_objects(self) -> None:
        duckdb = pytest.importorskip("duckdb")
        connection = duckdb.connect()
        try:
            description = connection.execute(
                "SELECT 1::INTEGER AS i, TRUE AS b, CURRENT_TIMESTAMP AS t, INTERVAL 1 DAY AS iv"
            ).description
        finally:
            connection.close()

        assert {column[0]: _map_type_code(type_code=column[1], db_type="duckdb") for column in description} == {
            "i": "number",
            "b": "boolean",
            "t": "time",
            "iv": "time",
        }

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


class TestIsAuthFailure:
    """Credential rejection is classified apart from transient errors: retrying
    is pointless, so the engine gets thrown away instead."""

    def test_oauth_invalid_grant_is_auth_failure(self) -> None:
        assert _is_auth_failure(Exception("('invalid_grant: Token has been expired or revoked.')"))

    def test_libpq_password_failure_is_auth_failure(self) -> None:
        assert _is_auth_failure(Exception('FATAL:  password authentication failed for user "svc"'))

    def test_signal_found_through_sqlalchemy_orig(self) -> None:
        """Drivers surface wrapped; the signal sits a layer or two down."""
        inner = Exception("invalid_grant")
        wrapped = sqlalchemy.exc.OperationalError("SELECT 1", {}, inner)
        assert _is_auth_failure(wrapped)

    def test_signal_found_through_cause_chain(self) -> None:
        inner = Exception("Reauthentication is needed")
        outer = RuntimeError("query failed")
        outer.__cause__ = inner
        assert _is_auth_failure(outer)

    def test_google_refresh_error_matched_by_type_name(self) -> None:
        """google-auth ships only with the optional extra, so the classifier
        matches on class name."""
        class RefreshError(Exception):
            pass
        assert _is_auth_failure(RefreshError("bad news"))

    def test_transient_errors_are_not_auth_failures(self) -> None:
        for message in ("database is locked", "deadlock detected", "server closed the connection"):
            assert not _is_auth_failure(Exception(message)), message

    def test_table_permission_denied_is_not_an_auth_failure(self) -> None:
        """The credentials worked; the grant didn't. Evicting is pool churn."""
        assert not _is_auth_failure(Exception("permission denied for table orders"))

    def test_cyclic_cause_chain_terminates(self) -> None:
        first, second = Exception("a"), Exception("b")
        first.__cause__ = second
        second.__cause__ = first
        assert _is_auth_failure(first) is False


class TestClientDiscardsEngineOnAuthFailure:

    @staticmethod
    def _client():
        return sql_client.SlayerSQLClient(
            datasource=DatasourceConfig(name="bq", type="bigquery", connection_string="bigquery://p/d"),
        )

    async def test_auth_failure_invalidates_cached_engine(self) -> None:
        client = self._client()
        client._sync_engine = object()
        boom = Exception("invalid_grant: Token has been expired or revoked.")
        with (
            patch.object(sql_client.SlayerSQLClient, "_execute", side_effect=boom),
            patch("slayer.sql.engine_factory.invalidate_engine") as invalidate,
        ):
            with pytest.raises(Exception, match="invalid_grant"):
                await client.execute("SELECT 1")
        invalidate.assert_called_once_with(client.datasource)
        assert client._sync_engine is None

    async def test_non_auth_failure_keeps_the_engine(self) -> None:
        client = self._client()
        engine = object()
        client._sync_engine = engine
        with (
            patch.object(sql_client.SlayerSQLClient, "_execute", side_effect=Exception("no such table: orders")),
            patch("slayer.sql.engine_factory.invalidate_engine") as invalidate,
        ):
            with pytest.raises(Exception, match="no such table"):
                await client.execute("SELECT 1")
        invalidate.assert_not_called()
        assert client._sync_engine is engine

    async def test_async_engine_is_disposed_too(self) -> None:
        """Native-async dialects hold a second pool ``invalidate_engine`` knows
        nothing about."""
        client = self._client()
        async_engine = AsyncMock()
        client._async_engine = async_engine
        with (
            patch.object(sql_client.SlayerSQLClient, "_execute", side_effect=Exception("invalid_grant")),
            patch("slayer.sql.engine_factory.invalidate_engine"),
        ):
            with pytest.raises(Exception, match="invalid_grant"):
                await client.execute("SELECT 1")
        async_engine.dispose.assert_awaited_once()
        assert client._async_engine is None

    async def test_get_column_types_also_discards(self) -> None:
        """Runs its own SQL on the same cached engine, so it needs the same
        cleanup ``execute`` gets."""
        client = self._client()
        client._sync_engine = object()
        with (
            patch.object(
                sql_client.SlayerSQLClient, "_get_column_types",
                side_effect=Exception("invalid_grant"),
            ),
            patch("slayer.sql.engine_factory.invalidate_engine") as invalidate,
        ):
            with pytest.raises(Exception, match="invalid_grant"):
                await client.get_column_types("SELECT 1")
        invalidate.assert_called_once_with(client.datasource)
        assert client._sync_engine is None

    def test_execute_sync_also_discards(self) -> None:
        """Shares the factory-cached engine, but has no loop to dispose an async
        pool on — so it drops only the sync one."""
        client = self._client()
        client._sync_engine = object()
        with (
            patch("slayer.sql.client._execute_with_retry_sync", side_effect=Exception("invalid_grant")),
            patch.object(sql_client.SlayerSQLClient, "_get_sync_engine_for_client", return_value=None),
            patch("slayer.sql.engine_factory.invalidate_engine") as invalidate,
        ):
            with pytest.raises(Exception, match="invalid_grant"):
                client.execute_sync("SELECT 1")
        invalidate.assert_called_once_with(client.datasource)
        assert client._sync_engine is None

    def test_execute_sync_keeps_engine_on_non_auth_failure(self) -> None:
        client = self._client()
        engine = object()
        client._sync_engine = engine
        with (
            patch("slayer.sql.client._execute_with_retry_sync", side_effect=Exception("no such table")),
            patch.object(sql_client.SlayerSQLClient, "_get_sync_engine_for_client", return_value=None),
            patch("slayer.sql.engine_factory.invalidate_engine") as invalidate,
        ):
            with pytest.raises(Exception, match="no such table"):
                client.execute_sync("SELECT 1")
        invalidate.assert_not_called()
        assert client._sync_engine is engine

    async def test_cleanup_failure_does_not_mask_the_original_error(self) -> None:
        """A failed eviction must not displace the auth error."""
        client = self._client()
        with (
            patch.object(sql_client.SlayerSQLClient, "_execute", side_effect=Exception("invalid_grant")),
            patch("slayer.sql.engine_factory.invalidate_engine", side_effect=RuntimeError("cache busted")),
        ):
            with pytest.raises(Exception, match="invalid_grant"):
                await client.execute("SELECT 1")


class TestBuildSqlModelTrialQuery:
    """``build_sql_model_trial_query`` wraps model SQL in the zero-row probe
    schema drift already ships, stripping a single trailing terminator."""

    def test_wraps_plain_sql(self) -> None:
        assert build_sql_model_trial_query("SELECT a FROM t") == (
            "SELECT * FROM (\nSELECT a FROM t\n) AS _sd_validate WHERE 1=0"
        )

    def test_strips_trailing_semicolon(self) -> None:
        assert build_sql_model_trial_query("SELECT 1;") == (
            "SELECT * FROM (\nSELECT 1\n) AS _sd_validate WHERE 1=0"
        )

    def test_strips_trailing_whitespace_and_semicolon(self) -> None:
        assert build_sql_model_trial_query("SELECT 1 ;  \n") == (
            "SELECT * FROM (\nSELECT 1\n) AS _sd_validate WHERE 1=0"
        )

    def test_strips_only_one_terminator(self) -> None:
        # A second ';' is left inside the wrapper — only one is stripped.
        assert build_sql_model_trial_query("SELECT 1;;") == (
            "SELECT * FROM (\nSELECT 1;\n) AS _sd_validate WHERE 1=0"
        )

    def test_trailing_line_comment_does_not_absorb_wrapper(self) -> None:
        # A terminal ``-- comment`` must not swallow the closing paren/guard;
        # the newline before ``)`` keeps the probe valid.
        wrapped = build_sql_model_trial_query("SELECT a FROM t -- note")
        assert wrapped == (
            "SELECT * FROM (\nSELECT a FROM t -- note\n) AS _sd_validate WHERE 1=0"
        )
        assert wrapped.rstrip().endswith("WHERE 1=0")


def _typed_exc(type_name: str, message: str = "boom") -> Exception:
    """An exception whose class NAME is ``type_name`` (classifier matches on
    the unqualified name to stay dependency-free)."""
    return type(type_name, (Exception,), {})(message)


class TestIsUnreachableDbError:
    """``_is_unreachable_db_error`` matches connection-establishment failures
    only — real rejections (missing object, syntax, permission, 4xx) stay
    rejectable so the save-time check still blocks them."""

    @pytest.mark.parametrize("orig_message", [
        "could not connect to server: Connection refused",
        "connection refused",
        "could not translate host name \"db\": getaddrinfo failed",
        "unable to open database file",
        "connection to server at \"db\" (1.2.3.4), port 5432 failed",
        "Login timeout expired",
        "(2003, \"Can't connect to MySQL server on 'db:3306' (timed out)\")",
        "Can't connect to local MySQL server through socket '/tmp/mysql.sock'",
    ])
    def test_connection_messages_are_unreachable(self, orig_message: str) -> None:
        assert _is_unreachable_db_error(_make_op_error(orig_message)) is True

    def test_disconnection_error_is_unreachable(self) -> None:
        assert _is_unreachable_db_error(
            sqlalchemy.exc.DisconnectionError("server closed")
        ) is True

    def test_interface_error_wrapping_connect_failure_is_unreachable(self) -> None:
        exc = sqlalchemy.exc.InterfaceError(
            statement="connect",
            params={},
            orig=sqlite3.OperationalError("could not connect to server"),
        )
        assert _is_unreachable_db_error(exc) is True

    @pytest.mark.parametrize("type_name", [
        "ServiceUnavailable",
        "GatewayTimeout",
        "TooManyRequests",
        "DeadlineExceeded",
        "ConnectionError",
    ])
    def test_cloud_type_names_are_unreachable(self, type_name: str) -> None:
        assert _is_unreachable_db_error(_typed_exc(type_name)) is True

    @pytest.mark.parametrize("orig_message", [
        "no such table: orders",
        "syntax error at or near \"FROM\"",
        "permission denied for table orders",
    ])
    def test_reachable_rejections_are_not_unreachable(self, orig_message: str) -> None:
        assert _is_unreachable_db_error(_make_op_error(orig_message)) is False

    @pytest.mark.parametrize("type_name", ["BadRequest", "Forbidden"])
    def test_client_error_type_names_are_not_unreachable(self, type_name: str) -> None:
        assert _is_unreachable_db_error(_typed_exc(type_name)) is False

    def test_bare_timed_out_is_not_unreachable(self) -> None:
        # A bare "timed out" can be a real slow rejection, so it stays
        # rejectable — only "login timeout" (connect phase) is unreachable.
        assert _is_unreachable_db_error(_make_op_error("query timed out")) is False

    def test_plain_value_error_is_not_unreachable(self) -> None:
        assert _is_unreachable_db_error(ValueError("boom")) is False


class TestIsUnreachableDbErrorBoundaries:
    """Pin the deliberate exclusions: a blanket InterfaceError (by type) and
    HTTP-status-shaped messages must stay rejectable."""

    def test_bare_interface_error_is_not_unreachable(self) -> None:
        exc = sqlalchemy.exc.InterfaceError(
            statement="stmt",
            params={},
            orig=sqlite3.ProgrammingError("cursor already closed"),
        )
        assert _is_unreachable_db_error(exc) is False

    @pytest.mark.parametrize("orig_message", ["400 Bad Request", "403 Forbidden"])
    def test_http_status_messages_are_not_unreachable(self, orig_message: str) -> None:
        assert _is_unreachable_db_error(_make_op_error(orig_message)) is False


class TestUnreachableSeparateFromTransient:
    """The unreachable classifier is kept apart from the retry classifier so
    ``execute()`` retry semantics are untouched (design decision)."""

    def test_unreachable_only_signal_is_not_transient(self) -> None:
        exc = _make_op_error("unable to open database file")
        assert _is_unreachable_db_error(exc) is True
        assert _is_transient_db_error(exc) is False

    @pytest.mark.parametrize("orig_message", ["database is locked", "deadlock detected"])
    def test_transient_only_signal_is_not_unreachable(self, orig_message: str) -> None:
        exc = _make_op_error(orig_message)
        assert _is_transient_db_error(exc) is True
        assert _is_unreachable_db_error(exc) is False


class TestClassifyModelSql:
    """Static classification gating the save-time trial-execute."""

    @pytest.mark.parametrize("sql", [
        "DELETE FROM orders",
        "UPDATE orders SET amount = 0",
        "INSERT INTO orders (id) VALUES (1)",
        "MERGE INTO orders USING src ON orders.id = src.id "
        "WHEN MATCHED THEN UPDATE SET amount = 0",
        "DROP TABLE orders",
        "TRUNCATE TABLE orders",
        "CREATE TABLE t (a INT)",
        "ALTER TABLE orders ADD COLUMN x INT",
        "WITH x AS (DELETE FROM orders RETURNING *) SELECT * FROM x",
        "COPY orders FROM '/tmp/x.csv'",       # non-query root the allowlist rejects
        "GRANT SELECT ON orders TO bob",
        "CALL do_stuff()",
        "VACUUM",
        "SELECT id INTO archived FROM orders",  # SELECT ... INTO creates a table
    ])
    def test_not_read_only(self, sql: str) -> None:
        assert classify_model_sql(sql, dialect="postgres") == "modifying"

    @pytest.mark.parametrize("sql", [
        "SELECT 1",
        "SELECT id, amount FROM orders WHERE id IN (SELECT id FROM other)",
        "WITH c AS (SELECT 1 AS x) SELECT * FROM c",
        "VALUES (1, 'a'), (2, 'b')",           # standalone constant table is read-only
    ])
    def test_read_only(self, sql: str) -> None:
        assert classify_model_sql(sql, dialect="postgres") == "read_only"

    @pytest.mark.parametrize("sql", ["SELECT (((", "totally not sql", ""])
    def test_unparseable(self, sql: str) -> None:
        # Unparseable SQL is rejected without a DB call — the generator can't
        # parse it either, so the model would be non-functional anyway.
        assert classify_model_sql(sql, dialect="postgres") == "unparseable"


class TestReadOnlyTransactionSql:
    """Dialects whose ``SET TRANSACTION READ ONLY`` binds the current txn."""

    @pytest.mark.parametrize("ds_type", ["postgres", "postgresql", "redshift", "oracle"])
    def test_read_only_capable_dialects(self, ds_type: str) -> None:
        assert _read_only_transaction_sql(ds_type) == "SET TRANSACTION READ ONLY"

    @pytest.mark.parametrize("ds_type", ["sqlite", "mysql", "duckdb", "bigquery", None])
    def test_dialects_without_current_txn_read_only(self, ds_type: str) -> None:
        assert _read_only_transaction_sql(ds_type) is None


class TestProbeRollsBack:
    """The type probe never commits — it rolls back so a slipped-through
    mutation cannot persist (SQLite backstop for the read-only guard)."""

    def test_probe_rolls_back_and_still_infers_types(self, tmp_path) -> None:
        db = str(tmp_path / "probe.db")
        eng = sa.create_engine(f"sqlite:///{db}")
        with eng.connect() as conn:
            conn.execute(sa.text("CREATE TABLE t (a INTEGER, b TEXT)"))
            conn.execute(sa.text("INSERT INTO t VALUES (1, 'x')"))
            conn.commit()
        seen: list[str] = []
        real_rollback = sa.Connection.rollback

        def _spy_rollback(self):  # noqa: ANN001
            seen.append("rollback")
            return real_rollback(self)

        with patch.object(sa.Connection, "rollback", _spy_rollback):
            types = _get_column_types_sync(
                "SELECT a, b FROM t", connection_string="", db_type="sqlite", engine=eng,
            )
        assert types == {"a": "number", "b": "string"}
        assert "rollback" in seen
