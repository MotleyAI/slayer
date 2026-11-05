"""MONEY and SMALLMONEY are exact numerics (spec: aggregations/native-type-preservation)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
from pytest_postgresql import factories
from sqlalchemy.dialects import mssql, postgresql

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine import ingestion
from slayer.engine.key_metadata import measure_key_preserves_native_type
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.postgres import describe_pg_cursor_column
from slayer.sql.dialects.reflection import (
    ResultColumnType,
    column_from_sa_type,
    column_from_type_string,
    is_exact_numeric_db_type,
)
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import _engine_generate
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)

MONEY_NAMES = ["money", "MONEY", "SMALLMONEY", "smallmoney"]
_PG_MONEY_OID = 790
_SUM_AMOUNT = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")


def _ledger(*, data_source: str = "ds", db_type: str = "money") -> SlayerModel:
    return SlayerModel(name="ledger", sql_table="ledger", data_source=data_source, columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="amount", type=DataType.DOUBLE, db_type=db_type),
        Column(name="doubled", sql="amount * 2", type=DataType.DOUBLE, db_type=db_type),
    ])


_SUM_QUERY = SlayerQuery.model_validate({"source_model": "ledger", "measures": ["sum(amount)"]})
_DOUBLED_QUERY = SlayerQuery.model_validate({"source_model": "ledger", "dimensions": ["id", "doubled"]})


class TestRawNames:
    @pytest.mark.parametrize("name", MONEY_NAMES)
    def test_exact_numeric(self, name: str) -> None:
        assert is_exact_numeric_db_type(name) is True

    @pytest.mark.parametrize("name", MONEY_NAMES)
    def test_probed_type_keeps_the_name(self, name: str) -> None:
        assert ResultColumnType(type=DataType.DOUBLE, db_type=name).lossy_db_type == name

    @pytest.mark.parametrize("name", MONEY_NAMES)
    def test_a_stored_money_column_preserves_its_aggregate(self, name: str) -> None:
        model = SlayerModel(name="orders", data_source="ds", sql_table="orders",
                            columns=[Column(name="amount", type=DataType.DOUBLE, db_type=name)])
        assert measure_key_preserves_native_type(model=model, key=_SUM_AMOUNT) is True


class TestInformationSchemaStrings:
    @pytest.mark.parametrize("name", MONEY_NAMES)
    def test_column_keeps_its_raw_type(self, name: str) -> None:
        column = column_from_type_string(name="amount", data_type=name)
        assert (column.type, column.db_type) == (DataType.DOUBLE, name)


class TestSqlAlchemyTypes:
    @pytest.mark.parametrize(("sa_type", "name"), [
        (postgresql.MONEY(), "MONEY"), (mssql.MONEY(), "MONEY"), (mssql.SMALLMONEY(), "SMALLMONEY"),
    ], ids=["postgres-money", "mssql-money", "mssql-smallmoney"])
    def test_column_keeps_its_raw_type(self, sa_type: Any, name: str) -> None:
        column = column_from_sa_type(name="amount", sa_type=sa_type)
        assert column.type is DataType.DOUBLE
        assert column.db_type is not None
        assert column.db_type.upper() == name
        assert is_exact_numeric_db_type(column.db_type)


class TestCursorDescriptions:
    def test_postgres_oid(self) -> None:
        described = describe_pg_cursor_column(("amount", _PG_MONEY_OID, None, None, None, None, None))
        assert described is not None
        assert (described.type, described.lossy_db_type) == (DataType.DOUBLE, "money")

    @pytest.mark.parametrize("name", ["money", "smallmoney"])
    def test_sql_server_result_description(self, name: str) -> None:
        (described,) = get_dialect("tsql").read_result_description(
            [{"name": "amount", "system_type_name": name, "is_hidden": False}]).values()
        assert (described.type, described.lossy_db_type) == (DataType.DOUBLE, name)


class TestEmission:
    @pytest.mark.parametrize(("dialect", "db_type"), [
        ("postgres", "money"), ("tsql", "money"), ("tsql", "smallmoney"),
    ])
    async def test_sum_carries_no_float_cast(self, dialect: str, db_type: str) -> None:
        sql = await _engine_generate(query=_SUM_QUERY, model=_ledger(db_type=db_type), dialect=dialect)
        assert "CAST(" not in sql.upper(), sql

    @pytest.mark.parametrize(("dialect", "db_type"), [
        ("postgres", "money"), ("tsql", "money"), ("tsql", "smallmoney"),
    ])
    async def test_derived_column_carries_no_float_cast(self, dialect: str, db_type: str) -> None:
        sql = await _engine_generate(query=_DOUBLED_QUERY, model=_ledger(db_type=db_type), dialect=dialect)
        assert "CAST(" not in sql.upper(), sql


_LEDGER = (
    "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount MONEY)",
    "INSERT INTO ledger VALUES (1, 0.10), (2, 0.20)",
)


async def _ingested(datasource: DatasourceConfig) -> SlayerModel:
    client = SlayerSQLClient(datasource=datasource)
    try:
        models = await ingestion.ingest_datasource_async(datasource, client=client)
    finally:
        await client.aclose()
        client.close()
    return next(m for m in models if m.name == "ledger")


async def _probe(datasource: DatasourceConfig, sql: str) -> dict[str, ResultColumnType]:
    client = SlayerSQLClient(datasource=datasource)
    try:
        return await client.get_result_column_types(sql)
    finally:
        await client.aclose()
        client.close()


def _money(value: Any) -> Decimal:
    """psycopg reads ``money`` as its locale-formatted text."""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value).replace("$", "").replace(",", ""))


class TestExecutedPostgres:
    async def test_reflected_money_keeps_its_raw_type(self, postgresql_proc: Any) -> None:
        with pg_database(postgresql_proc, statements=_LEDGER, name="money_reflect") as datasource:
            model = await _ingested(datasource)
        amount = model.get_column("amount")
        assert amount is not None
        assert amount.type is DataType.DOUBLE
        assert amount.db_type is not None
        assert amount.db_type.lower() == "money"

    async def test_probed_money_keeps_its_raw_type(self, postgresql_proc: Any) -> None:
        with pg_database(postgresql_proc, statements=_LEDGER, name="money_probe") as datasource:
            types = await _probe(datasource, "SELECT amount, amount * 2 AS doubled FROM ledger")
        assert {name: (t.type, t.lossy_db_type) for name, t in types.items()} == {
            "amount": (DataType.DOUBLE, "money"), "doubled": (DataType.DOUBLE, "money"),
        }

    async def test_sum_and_derived_column_are_exact(self, tmp_path: Any, postgresql_proc: Any) -> None:
        with pg_database(postgresql_proc, statements=_LEDGER, name="money_exec") as datasource:
            storage = YAMLStorage(base_dir=str(tmp_path / "models"))
            await storage.save_datasource(datasource)
            await storage.save_model(_ledger(data_source=datasource.name))
            engine = SlayerQueryEngine(storage=storage)
            try:
                sum_sql = (await engine.execute(query=_SUM_QUERY, dry_run=True)).sql
                doubled_sql = (await engine.execute(query=_DOUBLED_QUERY, dry_run=True)).sql
                total = await engine.execute(query=_SUM_QUERY)
                doubled = await engine.execute(query=_DOUBLED_QUERY)
            finally:
                await engine.aclose()
                engine.close()
        assert sum_sql is not None
        assert "CAST(" not in sum_sql.upper(), sum_sql
        assert doubled_sql is not None
        assert "CAST(" not in doubled_sql.upper(), doubled_sql
        (row,) = total.data
        assert not isinstance(row["ledger.amount_sum"], float), row
        assert _money(row["ledger.amount_sum"]) == Decimal("0.30")
        values = {r["ledger.id"]: r["ledger.doubled"] for r in doubled.data}
        assert not any(isinstance(v, float) for v in values.values()), values
        assert {k: _money(v) for k, v in values.items()} == {1: Decimal("0.20"), 2: Decimal("0.40")}
