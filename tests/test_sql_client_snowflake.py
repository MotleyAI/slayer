"""Client-side tests for Snowflake (DEV-1551).

Where the **dialect-class methods** are tested in
``tests/dialects/test_snowflake.py``, this module covers
``slayer.sql.client``'s **delegation** to those methods:

* ``_map_type_code`` consults
  ``SqlDialect.map_cursor_type_code`` before falling through to
  Postgres OIDs.
* ``_execute_sql_sync`` / ``_execute_sql_async`` emit the dialect's
  ``statement_timeout_sql`` when the standard if/elif chain doesn't
  cover the dialect.
* The type-probe path (``_get_column_types_sync`` /
  ``_get_column_types_async``) also applies the dialect's timeout SQL.
* Snowflake stays out of ``_ASYNC_DRIVERS`` (sync driver only).
* The ``LIMIT 0`` type probe is preserved on Snowflake (no LIMIT 1 fallback).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from slayer.sql import client


# ---------------------------------------------------------------------------
# Type-code dispatch delegates to the dialect
# ---------------------------------------------------------------------------


class TestMapTypeCodeDelegatesToDialect:
    """``_map_type_code(int, db_type="snowflake")`` must consult
    ``SnowflakeDialect.map_cursor_type_code`` before falling through to
    the Postgres OID map. The dispatch order matters — code 16 is
    "boolean" in PG-OID land but undefined in Snowflake, so a Snowflake
    call MUST NOT return "boolean" for code 16.
    """

    @pytest.mark.parametrize(
        "code,expected",
        [
            (0, "number"),   # FIXED
            (2, "string"),   # TEXT
            (8, "time"),     # TIMESTAMP_NTZ
            (13, "boolean"), # BOOLEAN
        ],
    )
    def test_known_snowflake_codes_routed_to_dialect(self, code: int, expected: str) -> None:
        assert client._map_type_code(code, db_type="snowflake") == expected

    def test_unknown_snowflake_code_defaults_to_string(self) -> None:
        """Unknown Snowflake codes return None from the dialect; the
        ``snowflake``-specific branch in ``_map_type_code`` then defaults
        to ``"string"`` rather than falling through to ``_PG_OID_MAP``.
        Code 16 is ``"boolean"`` in PG-OID but undefined on Snowflake —
        a fall-through would mis-classify."""
        assert client._map_type_code(16, db_type="snowflake") == "string"

    def test_unknown_snowflake_code_with_no_pg_match_defaults_to_string(self) -> None:
        """A code that's unknown to both Snowflake AND Postgres OIDs
        falls through to the existing string default."""
        assert client._map_type_code(999, db_type="snowflake") == "string"


# ---------------------------------------------------------------------------
# Statement timeout — sync, async, type-probe
# ---------------------------------------------------------------------------


def _extract_text(executed_call) -> str:
    """``conn.execute(sa.text(...))`` — pull the rendered SQL text.

    SQLAlchemy ``TextClause`` exposes ``.text``; bare ``str`` passes through.
    """
    arg = executed_call.args[0]
    return getattr(arg, "text", str(arg))


class TestSnowflakeStatementTimeout:

    def test_execute_sync_emits_dialect_timeout_sql(self) -> None:
        """Snowflake falls past the postgres/mysql/clickhouse if-elif
        and uses the dialect's ``statement_timeout_sql``."""
        fake_engine = MagicMock()
        fake_conn = MagicMock()
        fake_engine.connect.return_value.__enter__.return_value = fake_conn
        fake_result = MagicMock()
        fake_result.keys.return_value = ["col"]
        fake_result.fetchall.return_value = []
        fake_conn.execute.return_value = fake_result

        client._execute_sql_sync(
            sql="SELECT 1",
            connection_string="snowflake://?connection_name=default",
            db_type="snowflake",
            timeout_seconds=42,
            engine=fake_engine,
        )

        assert fake_conn.execute.call_count == 2, "expected ALTER SESSION + user query"
        timeout_sql = _extract_text(fake_conn.execute.call_args_list[0])
        # Exact shape — substring "42" would silently pass for "420" etc.
        assert timeout_sql == "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 42"

    def test_execute_async_emits_dialect_timeout_sql(self) -> None:
        """Async sibling — even though Snowflake isn't in _ASYNC_DRIVERS,
        ``_execute_sql_async`` MUST emit the dialect timeout when called
        directly with db_type='snowflake', so a future native-async
        driver Just Works."""
        async def _run():
            fake_async_engine = MagicMock()
            ctx = MagicMock()
            fake_conn = MagicMock()
            fake_conn.execute = AsyncMock()
            fake_result = MagicMock()
            fake_result.keys.return_value = ["col"]
            fake_result.fetchall.return_value = []
            fake_conn.execute.return_value = fake_result
            ctx.__aenter__ = AsyncMock(return_value=fake_conn)
            ctx.__aexit__ = AsyncMock(return_value=False)
            fake_async_engine.connect.return_value = ctx

            await client._execute_sql_async(
                sql="SELECT 1",
                engine=fake_async_engine,
                db_type="snowflake",
                timeout_seconds=7,
            )
            calls = fake_conn.execute.await_args_list
            timeout_sql = _extract_text(calls[0])
            assert timeout_sql == "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 7"

        asyncio.run(_run())

    def test_get_column_types_sync_emits_dialect_timeout_sql(self) -> None:
        """Type-probe path (``LIMIT 0``) compiles a Snowflake query and
        consumes warehouse compute. The ALTER SESSION must precede it."""
        fake_engine = MagicMock()
        fake_conn = MagicMock()
        fake_engine.connect.return_value.__enter__.return_value = fake_conn
        fake_cursor_desc = [("col", 0, None, None, None, None, None)]  # FIXED → number
        fake_result = MagicMock()
        fake_result.keys.return_value = ["col"]
        fake_result.cursor.description = fake_cursor_desc
        fake_conn.execute.return_value = fake_result

        with patch.object(client, "_resolve_sync_engine", return_value=fake_engine):
            client._get_column_types_sync(
                sql="SELECT 1 AS col",
                connection_string="snowflake://?connection_name=default",
                db_type="snowflake",
                engine=fake_engine,
            )

        assert fake_conn.execute.call_count >= 2
        first_sql = _extract_text(fake_conn.execute.call_args_list[0])
        assert first_sql.startswith("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = ")


# ---------------------------------------------------------------------------
# Async routing
# ---------------------------------------------------------------------------


class TestSnowflakeAsyncRouting:
    """snowflake-connector-python is sync only; SlayerSQLClient must NOT
    treat it as an async-capable driver."""

    def test_snowflake_not_in_async_drivers(self) -> None:
        assert "snowflake" not in client._ASYNC_DRIVERS

    def test_async_connection_string_returns_none_for_snowflake(self) -> None:
        cs = "snowflake://?connection_name=default"
        assert client._async_connection_string(connection_string=cs, db_type="snowflake") is None


# ---------------------------------------------------------------------------
# LIMIT 0 type probe preserved
# ---------------------------------------------------------------------------


class TestTypeProbeUsesLimitZero:
    """Snowflake uses ``LIMIT 0`` (not ``LIMIT 1`` like SQLite, not
    ``SELECT TOP N`` like T-SQL).

    NOTE: ``LIMIT 0`` still causes Snowflake to COMPILE the query and
    consume a small amount of warehouse compute. A future
    DESCRIBE-QUERY-based probe (deferred) would skip this. Documented
    as a known limitation in docs/database-support.md.
    """

    def test_snowflake_uses_limit_zero(self) -> None:
        probe = client._build_type_probe_sql("SELECT * FROM orders", db_type="snowflake")
        assert "LIMIT 0" in probe
        assert "LIMIT 1" not in probe
        assert "SELECT TOP" not in probe.upper()
