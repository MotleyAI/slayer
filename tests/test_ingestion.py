"""Unit tests for ingestion fallback functions (SQL injection prevention)."""

from unittest.mock import MagicMock

import sqlalchemy as sa

from slayer.engine.ingestion import (
    _get_columns_fallback,
    _get_pk_constraint_fallback,
)


def _setup_mock_engine(rows):
    """Create a mock SQLAlchemy engine with stubbed connection/execute."""
    engine = MagicMock(spec=sa.Engine)
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = rows
    return engine, conn


class TestGetColumnsFallback:
    """Tests for _get_columns_fallback parameterized queries."""

    def test_without_schema(self):
        engine, conn = _setup_mock_engine([("id", "INTEGER"), ("name", "VARCHAR")])
        result = _get_columns_fallback(sa_engine=engine, table_name="orders", schema=None)

        assert len(result) == 2
        assert result[0]["name"] == "id"
        assert result[1]["name"] == "name"

        # Verify parameterized query was used
        args, kwargs = conn.execute.call_args
        sql_text = args[0]
        assert isinstance(sql_text, sa.TextClause)
        sql_str = str(sql_text)
        assert ":table_name" in sql_str
        assert "table_schema" not in sql_str

        params = args[1] if len(args) > 1 else kwargs
        assert params == {"table_name": "orders"}

    def test_with_schema(self):
        engine, conn = _setup_mock_engine([("id", "INTEGER")])
        result = _get_columns_fallback(sa_engine=engine, table_name="orders", schema="public")

        assert len(result) == 1
        assert result[0]["name"] == "id"

        args, kwargs = conn.execute.call_args
        sql_text = args[0]
        assert isinstance(sql_text, sa.TextClause)
        sql_str = str(sql_text)
        assert ":table_name" in sql_str
        assert ":schema" in sql_str

        params = args[1] if len(args) > 1 else kwargs
        assert params == {"table_name": "orders", "schema": "public"}

    def test_no_fstring_interpolation(self):
        """Ensure table_name/schema values never appear literally in the SQL text."""
        engine, conn = _setup_mock_engine([])
        _get_columns_fallback(sa_engine=engine, table_name="'; DROP TABLE users;--", schema="'; DROP TABLE users;--")

        args, _ = conn.execute.call_args
        sql_str = str(args[0])
        assert "DROP TABLE" not in sql_str
        assert "'; DROP TABLE" not in sql_str


class TestGetPkConstraintFallback:
    """Tests for _get_pk_constraint_fallback parameterized queries."""

    def test_without_schema(self):
        engine, conn = _setup_mock_engine([("id",)])
        result = _get_pk_constraint_fallback(sa_engine=engine, table_name="orders", schema=None)

        assert result == {"constrained_columns": ["id"]}

        args, kwargs = conn.execute.call_args
        sql_text = args[0]
        assert isinstance(sql_text, sa.TextClause)
        sql_str = str(sql_text)
        assert ":table_name" in sql_str
        assert "tc.table_schema = :schema" not in sql_str

        params = args[1] if len(args) > 1 else kwargs
        assert params == {"table_name": "orders"}

    def test_with_schema(self):
        engine, conn = _setup_mock_engine([("id",), ("tenant_id",)])
        result = _get_pk_constraint_fallback(sa_engine=engine, table_name="orders", schema="public")

        assert result == {"constrained_columns": ["id", "tenant_id"]}

        args, kwargs = conn.execute.call_args
        sql_text = args[0]
        assert isinstance(sql_text, sa.TextClause)
        sql_str = str(sql_text)
        assert ":table_name" in sql_str
        assert ":schema" in sql_str

        params = args[1] if len(args) > 1 else kwargs
        assert params == {"table_name": "orders", "schema": "public"}

    def test_empty_result(self):
        engine, conn = _setup_mock_engine([])
        result = _get_pk_constraint_fallback(sa_engine=engine, table_name="no_pk_table", schema=None)
        assert result == {"constrained_columns": []}

    def test_no_fstring_interpolation(self):
        """Ensure table_name/schema values never appear literally in the SQL text."""
        engine, conn = _setup_mock_engine([])
        _get_pk_constraint_fallback(sa_engine=engine, table_name="'; DROP TABLE users;--", schema="'; DROP TABLE users;--")

        args, _ = conn.execute.call_args
        sql_str = str(args[0])
        assert "DROP TABLE" not in sql_str
        assert "'; DROP TABLE" not in sql_str
