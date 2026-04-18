"""Unit tests for ingestion fallback functions (SQL injection prevention)."""

from unittest.mock import MagicMock, patch

import sqlalchemy as sa

from slayer.engine.ingestion import (
    _generate_joins,
    _get_columns_fallback,
    _get_pk_constraint_fallback,
    _parse_info_schema_is_float,
    _sa_type_is_float,
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
        _get_pk_constraint_fallback(
            sa_engine=engine, table_name="'; DROP TABLE users;--", schema="'; DROP TABLE users;--"
        )

        args, _ = conn.execute.call_args
        sql_str = str(args[0])
        assert "DROP TABLE" not in sql_str
        assert "'; DROP TABLE" not in sql_str


class TestSaTypeIsFloat:
    """Tests for _sa_type_is_float scale-aware NUMERIC/DECIMAL detection."""

    def test_float_types_are_float(self):
        assert _sa_type_is_float(sa.Float()) is True
        assert _sa_type_is_float(sa.types.REAL()) is True

    def test_numeric_with_scale_zero_is_not_float(self):
        assert _sa_type_is_float(sa.Numeric(precision=10, scale=0)) is False

    def test_numeric_with_positive_scale_is_float(self):
        assert _sa_type_is_float(sa.Numeric(precision=10, scale=2)) is True

    def test_numeric_with_no_scale_is_float(self):
        """NUMERIC without explicit scale defaults to float-like."""
        assert _sa_type_is_float(sa.Numeric()) is True

    def test_decimal_with_scale_zero_is_not_float(self):
        assert _sa_type_is_float(sa.DECIMAL(precision=20, scale=0)) is False

    def test_decimal_with_positive_scale_is_float(self):
        assert _sa_type_is_float(sa.DECIMAL(precision=20, scale=4)) is True

    def test_integer_is_not_float(self):
        assert _sa_type_is_float(sa.Integer()) is False


class TestParseInfoSchemaIsFloat:
    """Tests for _parse_info_schema_is_float scale parsing from type strings."""

    def test_decimal_with_scale(self):
        assert _parse_info_schema_is_float("DECIMAL(10,2)") is True

    def test_decimal_with_zero_scale(self):
        assert _parse_info_schema_is_float("DECIMAL(10,0)") is False

    def test_numeric_with_scale(self):
        assert _parse_info_schema_is_float("NUMERIC(18,4)") is True

    def test_numeric_with_zero_scale(self):
        assert _parse_info_schema_is_float("NUMERIC(18,0)") is False

    def test_no_precision_info(self):
        """Bare 'DECIMAL' without parens defaults to float."""
        assert _parse_info_schema_is_float("DECIMAL") is True

    def test_no_scale_in_parens(self):
        """DECIMAL(10) with only precision defaults to float."""
        assert _parse_info_schema_is_float("DECIMAL(10)") is True


class TestGenerateJoinsDedup:
    """Tests for _generate_joins FK deduplication logic."""

    def test_multiple_fks_to_same_target_preserved(self):
        """Two distinct FKs to the same target table should both produce joins."""
        inspector = MagicMock(spec=sa.engine.Inspector)
        fk_rels = [
            ("buyer_id", "users", "id"),
            ("seller_id", "users", "id"),
        ]
        with patch(
            "slayer.engine.ingestion._get_fk_relationships", return_value=fk_rels,
        ):
            joins = _generate_joins(
                inspector=inspector,
                source_table="orders",
                referenced_tables={"users"},
                schema=None,
                table_set={"orders", "users"},
            )
        assert len(joins) == 2
        pairs = [j.join_pairs for j in joins]
        assert [["buyer_id", "id"]] in pairs
        assert [["seller_id", "id"]] in pairs

    def test_exact_duplicate_fk_deduplicated(self):
        """Identical FK pair to the same target should be deduplicated."""
        inspector = MagicMock(spec=sa.engine.Inspector)
        fk_rels = [
            ("buyer_id", "users", "id"),
            ("buyer_id", "users", "id"),
        ]
        with patch(
            "slayer.engine.ingestion._get_fk_relationships", return_value=fk_rels,
        ):
            joins = _generate_joins(
                inspector=inspector,
                source_table="orders",
                referenced_tables={"users"},
                schema=None,
                table_set={"orders", "users"},
            )
        assert len(joins) == 1
