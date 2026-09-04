"""Unit tests for exact-numeric detection and native-type preservation."""

import sqlite3

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.introspect_utils import (
    is_exact_numeric_db_type,
    unwrap_clickhouse_wrapper_str,
)
from slayer.engine.prebound import measure_key_preserves_native_type
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.storage.yaml_storage import YAMLStorage


class TestIsExactNumericDbType:
    @pytest.mark.parametrize(
        "db_type",
        [
            "DECIMAL(18,2)",
            "DECIMAL",
            "NUMERIC",
            "NUMERIC(10, 0)",
            "Decimal64(4)",
            "Decimal(18, 2)",
            "BIGNUMERIC",
            "decimal(18,2)",
            "bignumeric",
        ],
    )
    def test_exact_numeric_types_match(self, db_type):
        assert is_exact_numeric_db_type(db_type) is True

    @pytest.mark.parametrize(
        "db_type",
        [
            None,
            "",
            "DOUBLE",
            "INT",
            "MONEY",
            "SMALLMONEY",
            "TEXT",
            # Wrapped strings deliberately do NOT match — unwrapping is the
            # caller's job; the helper stays a pure string predicate.
            "Nullable(Decimal(18, 2))",
            "LowCardinality(Nullable(Decimal(10, 4)))",
        ],
    )
    def test_non_exact_numeric_types_do_not_match(self, db_type):
        assert is_exact_numeric_db_type(db_type) is False


class TestUnwrapClickhouseWrapperStr:
    @pytest.mark.parametrize(
        ("wrapped", "expected"),
        [
            ("Nullable(Decimal(18, 2))", "Decimal(18, 2)"),
            ("LowCardinality(Nullable(Decimal(10, 4)))", "Decimal(10, 4)"),
            ("nullable(String)", "String"),
            ("Decimal(18, 2)", "Decimal(18, 2)"),
            ("String", "String"),
            # Malformed wrapper text is returned as-is.
            ("Nullable(", "Nullable("),
        ],
    )
    def test_unwrap(self, wrapped, expected):
        assert unwrap_clickhouse_wrapper_str(wrapped) == expected


class TestSqliteKeepsInferredCast:
    """SQLite's numeric affinity has no exact decimal to preserve (Codex)."""

    def test_dialect_flags(self):
        assert get_dialect("sqlite").exact_decimal_native is False
        assert get_dialect("duckdb").exact_decimal_native is True

    async def test_sqlite_decimal_sum_keeps_double_cast(self, tmp_path):
        db_path = str(tmp_path / "decimal.db")
        conn = sqlite3.connect(db_path)
        with conn:
            conn.execute(
                "CREATE TABLE orders (id INT PRIMARY KEY, amount DECIMAL(18,2))"
            )
            # Integral values get INTEGER affinity — an un-cast SUM returns int.
            conn.execute("INSERT INTO orders VALUES (1, 1.00), (2, 2.00)")
        conn.close()

        datasource = DatasourceConfig(name="test", type="sqlite", database=db_path)
        model = next(
            m for m in ingest_datasource(datasource=datasource) if m.name == "orders"
        )
        amount = model.get_column("amount")
        assert amount is not None
        assert amount.db_type is not None
        assert "DECIMAL" in amount.db_type

        storage = YAMLStorage(base_dir=str(tmp_path / "models"))
        await storage.save_datasource(datasource)
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)

        query = {"source_model": "orders", "measures": ["amount:sum"]}
        result = await engine.execute(query=query)
        sql = (await engine.execute(query=query, dry_run=True)).sql

        assert "CAST(SUM(orders.amount) AS REAL)" in sql, sql
        assert isinstance(result.data[0]["orders.amount_sum"], float)


def _model_with_amount(db_type):
    return SlayerModel(
        name="orders",
        data_source="ds",
        sql_table="orders",
        columns=[Column(name="amount", type=DataType.DOUBLE, db_type=db_type)],
    )


_SUM_AMOUNT = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")


class TestMeasureKeyPreservesNativeType:
    @pytest.mark.parametrize(
        "db_type",
        ["DECIMAL(18,2)", "NUMERIC", "Decimal(18, 2)", "Decimal64(4)", "BIGNUMERIC"],
    )
    def test_exact_numeric_db_type_preserves(self, db_type):
        model = _model_with_amount(db_type)
        assert measure_key_preserves_native_type(model=model, key=_SUM_AMOUNT) is True

    @pytest.mark.parametrize(
        "db_type",
        [None, "Nullable(Decimal(18, 2))", "MONEY", "DOUBLE", "JSON"],
    )
    def test_other_db_types_do_not_preserve(self, db_type):
        model = _model_with_amount(db_type)
        assert measure_key_preserves_native_type(model=model, key=_SUM_AMOUNT) is False
