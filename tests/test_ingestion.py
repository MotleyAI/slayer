"""Unit tests for ingestion fallback functions (SQL injection prevention)."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.mssql import (
    DATETIME2,
    DATETIMEOFFSET,
    MONEY,
    NCHAR,
    NTEXT,
    NVARCHAR,
    SMALLDATETIME,
    SMALLMONEY,
    TINYINT,
)

from slayer.core.enums import DataType
from slayer.engine.ingestion import (
    _generate_joins,
    _get_columns_fallback,
    _get_pk_constraint_fallback,
    _parse_info_schema_is_float,
    _safe_get_pk_constraint,
    _sa_type_is_float,
    _sa_type_to_data_type,
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
        assert result.get("constrained_columns") == []

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


class TestSqliteSafeGetters:
    """Regression tests: SQLite engines must not hit information_schema fallback.

    Reproduces an issue found when ingesting a SQLite DB whose tables had
    foreign keys but no explicit PRIMARY KEY (e.g. mini-interact's robot DB).
    The inspector returns empty PK info, the legacy code fell through to a
    Postgres information_schema query, and crashed with
    'no such table: information_schema.table_constraints'.
    """

    def test_safe_get_pk_constraint_sqlite_no_pk(self):
        """Empty inspector PK on SQLite returns empty without info_schema query."""
        engine = sa.create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE t (a TEXT, b INTEGER)"))
            conn.commit()
        insp = sa.inspect(engine)
        result = _safe_get_pk_constraint(
            inspector=insp, sa_engine=engine, table_name="t", schema=None
        )
        assert result.get("constrained_columns") == []

    def test_safe_get_pk_constraint_sqlite_fk_only(self):
        """Tables with FK but no PK (the robot DB pattern) — must not crash."""
        engine = sa.create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE parent (id INTEGER PRIMARY KEY)"))
            conn.execute(
                sa.text(
                    "CREATE TABLE child (parent_ref INTEGER, "
                    "FOREIGN KEY (parent_ref) REFERENCES parent(id))"
                )
            )
            conn.commit()
        insp = sa.inspect(engine)
        result = _safe_get_pk_constraint(
            inspector=insp, sa_engine=engine, table_name="child", schema=None
        )
        assert result.get("constrained_columns") == []

    def test_safe_get_pk_constraint_sqlite_real_pk(self):
        """SQLite tables with declared PK still report it correctly."""
        engine = sa.create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(
                sa.text("CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT)")
            )
            conn.commit()
        insp = sa.inspect(engine)
        result = _safe_get_pk_constraint(
            inspector=insp, sa_engine=engine, table_name="u", schema=None
        )
        assert result.get("constrained_columns") == ["id"]


# ---------------------------------------------------------------------------
# DEV-1361: auto-ingestion INT vs DOUBLE distinction.
# ---------------------------------------------------------------------------


class TestSaTypeToDataTypeIntDouble:
    """``_sa_type_to_data_type`` distinguishes INT from DOUBLE so the ingested
    ``Column.type`` carries the precise type instead of a coarse ``NUMBER``."""

    @pytest.mark.parametrize("sa_type", [
        sa.Integer(),
        sa.BigInteger(),
        sa.SmallInteger(),
    ])
    def test_integer_family_maps_to_int(self, sa_type) -> None:
        assert _sa_type_to_data_type(sa_type) is DataType.INT

    @pytest.mark.parametrize("sa_type", [
        sa.Float(),
        sa.types.REAL(),
        sa.Float(precision=53),
    ])
    def test_float_family_maps_to_double(self, sa_type) -> None:
        assert _sa_type_to_data_type(sa_type) is DataType.DOUBLE

    def test_numeric_with_scale_zero_maps_to_int(self) -> None:
        # NUMERIC(10, 0) is integer-shaped — should land on INT, mirroring
        # the existing _sa_type_is_float scale-aware logic.
        assert _sa_type_to_data_type(sa.Numeric(precision=10, scale=0)) is DataType.INT

    def test_numeric_with_positive_scale_maps_to_double(self) -> None:
        assert _sa_type_to_data_type(sa.Numeric(precision=10, scale=2)) is DataType.DOUBLE

    def test_decimal_with_scale_zero_maps_to_int(self) -> None:
        assert _sa_type_to_data_type(sa.DECIMAL(precision=18, scale=0)) is DataType.INT

    def test_decimal_with_positive_scale_maps_to_double(self) -> None:
        assert _sa_type_to_data_type(sa.DECIMAL(precision=18, scale=4)) is DataType.DOUBLE

    def test_varchar_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(sa.VARCHAR(255)) is DataType.TEXT

    def test_text_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(sa.Text()) is DataType.TEXT

    def test_boolean_maps_to_boolean(self) -> None:
        assert _sa_type_to_data_type(sa.Boolean()) is DataType.BOOLEAN

    def test_date_maps_to_date(self) -> None:
        assert _sa_type_to_data_type(sa.Date()) is DataType.DATE

    def test_timestamp_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(sa.TIMESTAMP()) is DataType.TIMESTAMP

    def test_datetime_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(sa.DateTime()) is DataType.TIMESTAMP

    # --- T-SQL (SQL Server) specific types ---

    def test_tsql_tinyint_maps_to_int(self) -> None:
        assert _sa_type_to_data_type(TINYINT()) is DataType.INT

    def test_tsql_datetime2_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(DATETIME2()) is DataType.TIMESTAMP

    def test_tsql_smalldatetime_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(SMALLDATETIME()) is DataType.TIMESTAMP

    def test_tsql_datetimeoffset_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(DATETIMEOFFSET()) is DataType.TIMESTAMP

    def test_tsql_nvarchar_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(NVARCHAR()) is DataType.TEXT

    def test_tsql_nchar_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(NCHAR()) is DataType.TEXT

    def test_tsql_ntext_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(NTEXT()) is DataType.TEXT

    def test_tsql_money_maps_to_double(self) -> None:
        assert _sa_type_to_data_type(MONEY()) is DataType.DOUBLE

    def test_tsql_smallmoney_maps_to_double(self) -> None:
        assert _sa_type_to_data_type(SMALLMONEY()) is DataType.DOUBLE


class TestSqliteIngestionRoundTrip:
    """End-to-end: introspect a real SQLite table and confirm narrow types."""

    def test_int_double_text_distinction_via_inspector(self) -> None:
        from slayer.core.models import DatasourceConfig
        from slayer.engine.schema_drift import _live_schema_for_datasource

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "live.db")
            conn = sa.create_engine(f"sqlite:///{db_path}")
            with conn.connect() as c:
                c.execute(sa.text(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, amt REAL, "
                    "n VARCHAR(64), q INTEGER, ts TIMESTAMP, d DATE, b BOOLEAN)"
                ))
                c.commit()
            ds = DatasourceConfig(name="live", type="sqlite", database=db_path)
            schema = _live_schema_for_datasource(datasource=ds)
            cols = schema["t"].columns
            assert cols["id"] is DataType.INT
            assert cols["q"] is DataType.INT
            assert cols["amt"] is DataType.DOUBLE
            assert cols["n"] is DataType.TEXT
            assert cols["ts"] is DataType.TIMESTAMP
            assert cols["d"] is DataType.DATE
            assert cols["b"] is DataType.BOOLEAN
