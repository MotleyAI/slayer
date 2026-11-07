"""Tests for SQL client helpers (cursor type descriptions, retry-warning formatting)."""

import logging
import sqlite3
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sqlalchemy as sa
import sqlalchemy.exc
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.sql import client as sql_client
from slayer.sql.client import (
    _execute_with_retry_async,
    _execute_with_retry_sync,
    _execute_with_retry_threaded,
    _is_auth_failure,
    _is_transient_db_error,
    _is_unreachable_db_error,
    _read_only_transaction_sql,
    ResultColumnType,
    classify_model_sql,
    get_result_column_types_sync,
    render_type_probe,
)
from slayer.sql.dialects import SqliteDialect, dialect_for_ds_type

from tests._engine_helpers import disposable_engine, plant_async_engine


class TestDuckdbDriverTypeObjects:
    """DuckDB describes its columns with ``DuckDBPyType`` objects, read by name."""

    def test_driver_type_objects(self) -> None:
        duckdb = pytest.importorskip("duckdb")
        connection = duckdb.connect()
        try:
            description = connection.execute(
                "SELECT 1::INTEGER AS i, TRUE AS b, CURRENT_TIMESTAMP AS t, INTERVAL 1 DAY AS iv"
            ).description
        finally:
            connection.close()

        dialect = dialect_for_ds_type("duckdb")
        assert {column[0]: dialect.describe_cursor_column(column) for column in description} == {
            "i": ResultColumnType(type=DataType.INT, db_type="INTEGER"),
            "b": ResultColumnType(type=DataType.BOOLEAN, db_type="BOOLEAN"),
            "t": ResultColumnType(type=DataType.TIMESTAMP, db_type="TIMESTAMP WITH TIME ZONE"),
            "iv": ResultColumnType(type=DataType.TEXT, db_type="INTERVAL"),
        }


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

        engine = MagicMock()
        with pytest.raises(sqlalchemy.exc.OperationalError, match="no such table"):
            await _execute_with_retry_threaded(
                sql="SELECT 1",
                db_type="sqlite",
                engine=engine,
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

        engine = MagicMock()
        with pytest.raises(sqlalchemy.exc.OperationalError, match="no such table"):
            _execute_with_retry_sync(
                sql="SELECT 1",
                db_type="sqlite",
                engine=engine,
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
                db_type="sqlite",
                engine=MagicMock(),
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
                db_type="sqlite",
                engine=MagicMock(),
                initial_delay=0.0,
                max_delay=0.0,
            )

        assert result == [{"ok": 1}]
        assert calls["n"] == 2
        assert any(
            "Transient DB error" in rec.getMessage() and "<empty sql>" in rec.getMessage()
            for rec in caplog.records
        )


class TestRetryRejectsNonPositiveAttempts:
    """``max_attempts < 1`` raises on every retry path instead of silently returning ``None``."""

    def test_sync(self, monkeypatch: pytest.MonkeyPatch) -> None:
        called = MagicMock()
        monkeypatch.setattr(sql_client, "_execute_sql_sync", called)
        engine = MagicMock()
        with pytest.raises(ValueError, match="max_attempts"):
            _execute_with_retry_sync(sql="SELECT 1", db_type="sqlite", engine=engine, max_attempts=0)
        called.assert_not_called()

    async def test_threaded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        called = MagicMock()
        monkeypatch.setattr(sql_client, "_execute_sql_sync", called)
        engine = MagicMock()
        with pytest.raises(ValueError, match="max_attempts"):
            await _execute_with_retry_threaded(sql="SELECT 1", db_type="sqlite", engine=engine, max_attempts=0)
        called.assert_not_called()


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

    async def test_async_engine_is_disposed_too(self, monkeypatch) -> None:
        """An async driver's pool is one ``invalidate_engine`` knows nothing about."""
        client = sql_client.SlayerSQLClient(
            datasource=DatasourceConfig(name="pg", type="postgres", connection_string="postgresql://u@db.invalid/d"),
        )
        async_engine = AsyncMock()
        plant_async_engine(monkeypatch, client, async_engine)
        with (
            patch.object(sql_client.SlayerSQLClient, "_execute", side_effect=Exception("invalid_grant")),
            patch("slayer.sql.engine_factory.invalidate_engine"),
        ):
            with pytest.raises(Exception, match="invalid_grant"):
                await client.execute("SELECT 1")
        async_engine.dispose.assert_awaited_once()
        plant_async_engine(monkeypatch, client, AsyncMock())

    async def test_get_result_column_types_also_discards(self) -> None:
        """Runs its own SQL on the same cached engine, so it needs the same
        cleanup ``execute`` gets."""
        client = self._client()
        client._sync_engine = object()
        probe = render_type_probe(exp.select("1"), dialect=SqliteDialect())
        with (
            patch.object(
                sql_client.SlayerSQLClient, "_get_result_column_types",
                side_effect=Exception("invalid_grant"),
            ),
            patch("slayer.sql.engine_factory.invalidate_engine") as invalidate,
        ):
            with pytest.raises(Exception, match="invalid_grant"):
                await client.get_result_column_types(probe)
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
        "SELECT id FROM orders FOR UPDATE",     # locking read (write-intent locks)
    ])
    def test_not_read_only(self, sql: str) -> None:
        assert classify_model_sql(sql, dialect="postgres") == "modifying"

    def test_next_value_for_is_not_read_only(self) -> None:
        # SELECT NEXT VALUE FOR seq advances sequence state (T-SQL).
        assert classify_model_sql("SELECT NEXT VALUE FOR s", dialect="tsql") == "modifying"

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

    @pytest.mark.parametrize("ds_type", ["postgres", "postgresql", "redshift", "oracle", None])
    def test_read_only_capable_dialects(self, ds_type: str) -> None:
        assert _read_only_transaction_sql(ds_type) == "SET TRANSACTION READ ONLY"

    @pytest.mark.parametrize("ds_type", ["sqlite", "mysql", "duckdb", "bigquery"])
    def test_dialects_without_current_txn_read_only(self, ds_type: str) -> None:
        assert _read_only_transaction_sql(ds_type) is None


class TestProbeRollsBack:
    """The type probe never commits — it rolls back so a slipped-through
    mutation cannot persist (SQLite backstop for the read-only guard)."""

    def test_probe_rolls_back_and_still_infers_types(self, tmp_path) -> None:
        db = str(tmp_path / "probe.db")
        with disposable_engine(f"sqlite:///{db}") as eng:
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
                types = get_result_column_types_sync(
                    render_type_probe(exp.select("a", "b").from_("t"), dialect=SqliteDialect()),
                    db_type="sqlite", engine=eng,
                )
            assert types == {"a": ResultColumnType(type=DataType.INT), "b": ResultColumnType(type=DataType.TEXT)}
            assert "rollback" in seen
