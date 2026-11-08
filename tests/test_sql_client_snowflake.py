"""Client-side tests for Snowflake.

Where the **dialect-class methods** are tested in
``tests/dialects/test_snowflake.py``, this module covers
``slayer.sql.client``'s **delegation** to those methods:

* The typed probe reads cursor descriptions through
  ``SqlDialect.describe_cursor_column``, never through Postgres OIDs.
* ``_execute_sql_sync`` / ``_execute_sql_async`` emit the dialect's
  ``statement_timeout`` when the standard if/elif chain doesn't
  cover the dialect.
* The type-probe door also applies the dialect's timeout SQL.
* Snowflake has no async driver (sync driver only).
* The ``LIMIT 0`` type probe is preserved on Snowflake (no LIMIT 1 fallback).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.engine.url import make_url
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.database import client
from slayer.database.client import ResultColumnType
from slayer.sql.dialects import SnowflakeDialect


# ---------------------------------------------------------------------------
# Type-code dispatch delegates to the dialect
# ---------------------------------------------------------------------------


def _probe_code(code: int) -> ResultColumnType:
    """The typed probe's answer for one Snowflake column described with ``code``."""
    fake_engine = MagicMock()
    fake_conn = MagicMock()
    fake_engine.connect.return_value.__enter__.return_value = fake_conn
    fake_result = MagicMock()
    fake_result.keys.return_value = ["col"]
    fake_result.cursor.description = [("col", code, None, None, 38, 0, True)]
    fake_conn.exec_driver_sql.return_value = fake_result
    return client.get_result_column_types_sync(sql="SELECT 1 AS col", db_type="snowflake", engine=fake_engine)["col"]


class TestCursorDescriptionDelegatesToDialect:
    """Snowflake codes are read by ``SnowflakeDialect.describe_cursor_column``,
    never through the Postgres OID table — code 16 is "boolean" in PG-OID land
    but undefined in Snowflake, so a Snowflake column MUST NOT read as boolean.
    """

    @pytest.mark.parametrize(
        "code,expected",
        [
            (0, DataType.INT),        # FIXED
            (2, DataType.TEXT),       # TEXT
            (8, DataType.TIMESTAMP),  # TIMESTAMP_NTZ
            (13, DataType.BOOLEAN),   # BOOLEAN
        ],
    )
    def test_known_snowflake_codes_routed_to_dialect(self, code: int, expected: DataType) -> None:
        assert _probe_code(code).type == expected

    @pytest.mark.parametrize("code", [16, 999])
    def test_unknown_snowflake_code_defaults_to_text(self, code: int) -> None:
        assert _probe_code(code) == ResultColumnType(type=DataType.TEXT)


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
        and uses the dialect's ``statement_timeout``."""
        fake_engine = MagicMock()
        fake_conn = MagicMock()
        fake_engine.connect.return_value.__enter__.return_value = fake_conn
        fake_result = MagicMock()
        fake_result.keys.return_value = ["col"]
        fake_result.fetchall.return_value = []
        fake_conn.exec_driver_sql.return_value = fake_result

        client._execute_sql_sync(
            sql="SELECT 1",
            db_type="snowflake",
            timeout_seconds=42,
            engine=fake_engine,
        )

        assert fake_conn.exec_driver_sql.call_count == 2, "expected ALTER SESSION + user query"
        timeout_sql = _extract_text(fake_conn.exec_driver_sql.call_args_list[0])
        # Exact shape — substring "42" would silently pass for "420" etc.
        assert timeout_sql == "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 42"

    def test_execute_async_emits_dialect_timeout_sql(self) -> None:
        """Async sibling — even though Snowflake has no async driver,
        ``_execute_sql_async`` MUST emit the dialect timeout when called
        directly with db_type='snowflake', so a future native-async
        driver Just Works."""
        async def _run():
            fake_async_engine = MagicMock()
            ctx = MagicMock()
            fake_conn = MagicMock()
            fake_conn.exec_driver_sql = AsyncMock()
            fake_conn.get_raw_connection = AsyncMock()
            fake_result = MagicMock()
            fake_result.keys.return_value = ["col"]
            fake_result.fetchall.return_value = []
            fake_conn.exec_driver_sql.return_value = fake_result
            ctx.__aenter__ = AsyncMock(return_value=fake_conn)
            ctx.__aexit__ = AsyncMock(return_value=False)
            fake_async_engine.connect.return_value = ctx

            await client._execute_sql_async(
                sql="SELECT 1",
                engine=fake_async_engine,
                db_type="snowflake",
                timeout_seconds=7,
            )
            calls = fake_conn.exec_driver_sql.await_args_list
            timeout_sql = _extract_text(calls[0])
            assert timeout_sql == "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 7"

        asyncio.run(_run())

    def test_get_result_column_types_sync_emits_dialect_timeout_sql(self) -> None:
        """Type-probe path (``LIMIT 0``) compiles a Snowflake query and
        consumes warehouse compute. The ALTER SESSION must precede it."""
        fake_engine = MagicMock()
        fake_conn = MagicMock()
        fake_engine.connect.return_value.__enter__.return_value = fake_conn
        fake_cursor_desc = [("col", 0, None, None, None, None, None)]  # FIXED
        fake_result = MagicMock()
        fake_result.keys.return_value = ["col"]
        fake_result.cursor.description = fake_cursor_desc
        fake_conn.exec_driver_sql.return_value = fake_result

        client.get_result_column_types_sync(
            client.render_type_probe(exp.select(exp.Literal.number(1).as_("col")), dialect=SnowflakeDialect()),
            db_type="snowflake",
            engine=fake_engine,
        )

        assert fake_conn.exec_driver_sql.call_count >= 2
        first_sql = _extract_text(fake_conn.exec_driver_sql.call_args_list[0])
        assert first_sql.startswith("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = ")


# ---------------------------------------------------------------------------
# Async routing
# ---------------------------------------------------------------------------


class TestSnowflakeAsyncRouting:
    """snowflake-connector-python is sync only; SlayerSQLClient must NOT
    treat it as an async-capable driver."""

    def test_snowflake_url_is_not_async_capable(self) -> None:
        pytest.importorskip("snowflake.sqlalchemy")
        assert not make_url("snowflake://?connection_name=default").get_dialect(_is_async=True).is_async


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
        probe = client.render_type_probe(exp.select("*").from_("orders"), dialect=SnowflakeDialect()).sql
        assert "LIMIT 0" in probe
        assert "LIMIT 1" not in probe
        assert "SELECT TOP" not in probe.upper()
