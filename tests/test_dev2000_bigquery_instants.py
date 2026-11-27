"""BigQuery: a ``DateTimeTz`` field (an ingested ``TIMESTAMP``) reads as its UTC wall clock and compares bare, without a
query zone (spec: queries/mixed-type-coercion)."""

from __future__ import annotations

from typing import Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from tests._dev2015_fixtures import JAN_MAR, TWO_FACTS, spine_datasets, spine_td
from tests._dev2120_fixtures import emit_lib, lib_dataset, lib_query
from tests._engine_helpers import _engine_generate

#: A ``DateTimeTz`` field is the instant on BigQuery whether or not ingestion kept its ``db_type``.
INSTANT_DB_TYPES = ["TIMESTAMP", None]


def _lib(*, datatype: DataType, db_type: Optional[str]) -> SlayerDataset:
    base = lib_dataset()
    return base.model_copy(update={"fields": [
        c.model_copy(update={"datatype": datatype, "db_type": db_type}) if c.name == "ts" else c for c in base.fields]})


async def _where(expr: str, *, model: SlayerDataset) -> str:
    sql = await emit_lib(lib_query(filter=[expr]), dialect="bigquery", datasets=[model])
    return " ".join(sql.upper().split())


@pytest.mark.parametrize("db_type", INSTANT_DB_TYPES)
async def test_now_against_a_timestamp_column_compares_the_column(db_type: Optional[str]) -> None:
    sql = await _where("lib.ts < now()", model=_lib(datatype=DataType.DATETIME_TZ, db_type=db_type))
    assert "WHERE LIB.TS < CAST(CURRENT_DATETIME() AS TIMESTAMP)" in sql, sql


async def test_now_against_a_datetime_column_compares_directly() -> None:
    sql = await _where("lib.ts < now()", model=_lib(datatype=DataType.DATETIME, db_type=None))
    assert "WHERE LIB.TS < CURRENT_DATETIME()" in sql, sql


@pytest.mark.parametrize("db_type", INSTANT_DB_TYPES)
async def test_timestamp_literal_keeps_its_iso_text(db_type: Optional[str]) -> None:
    sql = await _where("lib.ts < TIMESTAMP '2025-01-01 00:00:00'",
                       model=_lib(datatype=DataType.DATETIME_TZ, db_type=db_type))
    assert "WHERE LIB.TS < '2025-01-01 00:00:00'" in sql, sql


@pytest.mark.parametrize("db_type", INSTANT_DB_TYPES)
async def test_month_bucket_reads_the_utc_wall_clock_on_the_spine(db_type: Optional[str]) -> None:
    models = [m.model_copy(update={"fields": [
        c.model_copy(update={"datatype": DataType.DATETIME_TZ, "db_type": db_type})
        if m.name in ("orders", "returns") and c.datatype in (DataType.DATE, DataType.DATETIME) else c
        for c in m.fields]}) for m in spine_datasets()]
    query = SlayerQuery.model_validate({"population": "customers", "metrics": TWO_FACTS,
                                        "dimensions": ["customers.region"],
                                        "time_dimensions": [spine_td(date_range=JAN_MAR)]})
    sql = " ".join((await _engine_generate(query=query, dataset=models[0], extra_datasets=models[1:],
                                           dialect="bigquery", validate=False)).split())
    assert "DATE_TRUNC(CAST(orders.order_date AS DATETIME), MONTH)" in sql, sql
    assert "GENERATE_ARRAY(" in sql.upper(), sql
