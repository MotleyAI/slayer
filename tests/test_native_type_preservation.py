"""Unit tests for exact-numeric detection and native-type preservation."""

from decimal import Decimal

import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.live_schema import ingestion
from slayer.sql.dialects.reflection import (
    is_exact_numeric_db_type,
    unwrap_clickhouse_wrapper_str,
)
from slayer.engine.key_metadata import measure_key_preserves_native_type
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database.client import SlayerSQLClient
from slayer.sql.dialects import get_dialect
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import _engine_generate
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)


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
        with transaction(db_path) as conn:
            conn.execute(
                "CREATE TABLE orders (id INT PRIMARY KEY, amount DECIMAL(18,2))"
            )
            # Integral values get INTEGER affinity — an un-cast SUM returns int.
            conn.execute("INSERT INTO orders VALUES (1, 1.00), (2, 2.00)")

        datasource = DatasourceConfig(name="test", type="sqlite", database=db_path)
        client = SlayerSQLClient(datasource=datasource)
        try:
            models = await ingestion.ingest_datasource_async(datasource, client=client)
        finally:
            await client.aclose()
            client.close()
        model = next(m for m in models if m.name == "orders")
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

        assert sql is not None
        assert "CAST(SUM(orders.amount) AS REAL)" in sql, sql
        assert isinstance(result.data[0]["orders.amount_sum"], float)
        assert result.data[0]["orders.amount_sum"] == 3.0


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
        [None, "Nullable(Decimal(18, 2))", "DOUBLE", "JSON"],
    )
    def test_other_db_types_do_not_preserve(self, db_type):
        model = _model_with_amount(db_type)
        assert measure_key_preserves_native_type(model=model, key=_SUM_AMOUNT) is False


def _wide_model(data_source: str = "ds") -> SlayerModel:
    return SlayerModel(name="t", sql_table="t", data_source=data_source, columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="amount", type=DataType.INT, db_type="NUMERIC(38,0)"),
        Column(name="wide", sql="amount * 10", type=DataType.INT, db_type="NUMERIC(38,0)"),
    ])


_WIDE_QUERY = SlayerQuery.model_validate({"source_model": "t", "dimensions": ["wide"]})


class TestDerivedColumnPreservesNativeType:
    async def test_wide_exact_integer_keeps_its_value(self, tmp_path, postgresql_proc):
        with pg_database(postgresql_proc, statements=[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, amount NUMERIC(38,0))",
            "INSERT INTO t VALUES (1, 10000000000000000000)",
        ], name="wide_pg") as datasource:
            storage = YAMLStorage(base_dir=str(tmp_path / "models"))
            await storage.save_datasource(datasource)
            await storage.save_model(_wide_model(data_source=datasource.name))
            engine = SlayerQueryEngine(storage=storage)
            try:
                sql = (await engine.execute(query=_WIDE_QUERY, dry_run=True)).sql
                result = await engine.execute(query=_WIDE_QUERY)
            finally:
                engine.close()
        assert sql is not None
        assert "CAST(" not in sql, sql
        assert result.data == [{"t.wide": Decimal("100000000000000000000")}]

    async def test_sqlite_keeps_the_inferred_cast(self):
        sql = await _engine_generate(query=_WIDE_QUERY, model=_wide_model(), dialect="sqlite")
        assert "CAST(t.amount * 10 AS INTEGER)" in sql, sql

    async def test_filter_on_a_wide_exact_integer_keeps_its_value(self, tmp_path, postgresql_proc):
        query = SlayerQuery.model_validate({"source_model": "t", "dimensions": ["wide"], "filters": ["wide > 0"]})
        with pg_database(postgresql_proc, statements=[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, amount NUMERIC(38,0))",
            "INSERT INTO t VALUES (1, 10000000000000000000)",
        ], name="wide_filter_pg") as datasource:
            storage = YAMLStorage(base_dir=str(tmp_path / "models"))
            await storage.save_datasource(datasource)
            await storage.save_model(_wide_model(data_source=datasource.name))
            engine = SlayerQueryEngine(storage=storage)
            try:
                result = await engine.execute(query=query)
            finally:
                engine.close()
        assert result.data == [{"t.wide": Decimal("100000000000000000000")}]

    async def test_a_declared_type_other_than_the_physical_one_still_casts(self):
        model = SlayerModel(name="t", sql_table="t", data_source="ds", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE, db_type="NUMERIC(18,2)"),
            Column(name="whole", sql="amount / 3", type=DataType.INT, db_type="NUMERIC(18,2)"),
        ])
        query = SlayerQuery.model_validate({"source_model": "t", "dimensions": ["whole"]})
        sql = await _engine_generate(query=query, model=model, dialect="postgres")
        assert "CAST(t.amount / 3 AS BIGINT)" in sql, sql
