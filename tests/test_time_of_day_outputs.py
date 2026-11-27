"""A time-of-day column on the typed outputs: Arrow Flight, the Postgres facade and the facade catalog (spec:
queries/time-of-day › A time-of-day result is a time of day)."""

from __future__ import annotations

import datetime as dt
import struct
from typing import Any

import pyarrow as pa
import pytest
import sqlglot
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.engine.query_engine import SlayerResponse
from slayer.facade.catalog import build_catalog, build_catalog_grouped_by_schema
from slayer.facade.catalog_sql import build_catalog_relations, stable_oid
from slayer.facade.datatypes import datatype_to_jdbc
from slayer.facade.info_schema import match_info_schema
from slayer.facade.translator import QueryResult, translate
from slayer.flight.handlers import FlightHandlers
from slayer.flight.types import arrow_to_datatype, datatype_to_arrow
from slayer.pg_facade import protocol as proto
from slayer.pg_facade import types as pg
from tests._arrow_types import ArrowTable
from tests.flight.test_handlers import _FakeEngine as _FlightEngine
from tests.flight.test_handlers import _FakeStorage as _FlightStorage

DS = "shop"
OID_TIME = 1083
TEN_THIRTY = dt.time(10, 30)
FRACTION = dt.time(10, 30, 0, 123456)
MICROS = 37_800_000_000  # 10:30:00 since midnight


def shifts() -> SlayerDataset:
    return SlayerDataset(name="shifts", sql_table="shifts", data_source=DS, fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="starts_at", datatype=DataType.TIME),
        DatasetField(name="label", datatype=DataType.STRING),
    ])


def _parsed(sql: str) -> Expression:
    tree = sqlglot.parse_one(sql)
    assert isinstance(tree, Expression)
    return tree


def flight_table(value: Any) -> ArrowTable:
    """``SELECT starts_at FROM shifts`` over Flight, the engine answering one row holding ``value``."""
    response = SlayerResponse(data=[{"shifts.starts_at": value}], columns=["shifts.starts_at"])
    handlers = FlightHandlers(
        engine=_FlightEngine(response=response, storage=_FlightStorage({DS: [shifts()]})),  # type: ignore[arg-type]
    )
    result = handlers._translate("SELECT starts_at FROM shifts")
    assert isinstance(result, QueryResult), result
    return handlers._execute_full(result)


class TestFlight:
    def test_forward(self) -> None:
        assert datatype_to_arrow(DataType.TIME) == pa.time64("us")

    @pytest.mark.parametrize("arrow_type", [pa.time64("us"), pa.time64("ns"), pa.time32("s"), pa.time32("ms")])
    def test_reverse(self, arrow_type: pa.DataType) -> None:
        assert arrow_to_datatype(arrow_type) is DataType.TIME

    def test_round_trip(self) -> None:
        assert arrow_to_datatype(datatype_to_arrow(DataType.TIME)) is DataType.TIME

    @pytest.mark.parametrize("value", [TEN_THIRTY, FRACTION, dt.time(0, 0), dt.time(23, 59, 59, 999999), None])
    def test_a_row_carries_a_microsecond_time(self, value: dt.time | None) -> None:
        table = flight_table(value)
        assert table.schema.field(0).type == pa.time64("us")
        assert table.column(0).to_pylist() == [value]


class TestPostgresFacade:
    def test_the_type_is_time(self) -> None:
        assert getattr(proto, "OID_TIME") == OID_TIME  # noqa: B009 — not there before the type exists
        assert pg.datatype_to_oid(DataType.TIME) == OID_TIME

    def test_a_projection_is_described_as_time(self) -> None:
        catalog = build_catalog(datasets_by_datasource={DS: [shifts()]})
        result = translate(sql="SELECT starts_at FROM shifts", catalog=catalog)
        assert isinstance(result, QueryResult), result
        assert pg.datatype_to_oid(result.projection_types[0]) == OID_TIME

    @pytest.mark.parametrize(("value", "text"), [
        (TEN_THIRTY, b"10:30:00"), (FRACTION, b"10:30:00.123456"), (dt.time(0, 0), b"00:00:00"),
        (dt.time(9, 5, 7), b"09:05:07"),
    ])
    def test_text_form(self, value: dt.time, text: bytes) -> None:
        assert pg.value_to_text(value, OID_TIME) == text

    @pytest.mark.parametrize(("value", "micros"), [
        (TEN_THIRTY, MICROS), (FRACTION, MICROS + 123456), (dt.time(0, 0), 0),
        (dt.time(23, 59, 59, 999999), 86_399_999_999),
    ])
    def test_binary_form(self, value: dt.time, micros: int) -> None:
        encoded = pg.value_to_binary(value, OID_TIME)
        assert encoded is not None
        assert encoded == struct.pack(">q", micros)
        assert pg.value_from_binary(encoded, OID_TIME) == value

    def test_null(self) -> None:
        assert pg.value_to_text(None, OID_TIME) is None
        assert pg.value_to_binary(None, OID_TIME) is None

    @pytest.mark.parametrize(("text", "value"), [
        (b"10:30:00", TEN_THIRTY), (b"10:30", TEN_THIRTY), (b"10:30:00.123456", FRACTION),
    ])
    def test_a_text_parameter(self, text: bytes, value: dt.time) -> None:
        assert pg.value_from_text(text, OID_TIME) == value

    def test_a_binary_parameter(self) -> None:
        assert pg.value_from_binary(struct.pack(">q", MICROS), OID_TIME) == TEN_THIRTY

    @pytest.mark.parametrize("micros", [86_400_000_000, 90_000_000_000, -1])
    def test_a_binary_parameter_outside_the_day_is_refused(self, micros: int) -> None:
        encoded = struct.pack(">q", micros)
        with pytest.raises(ValueError, match="not a time of day"):
            pg.value_from_binary(encoded, OID_TIME)

    def test_the_last_microsecond_is_a_binary_parameter(self) -> None:
        assert pg.value_from_binary(struct.pack(">q", 86_399_999_999), OID_TIME) == dt.time(23, 59, 59, 999999)

    def test_a_parameter_is_substituted_quoted(self) -> None:
        assert pg.literal_for_substitution(TEN_THIRTY) == "'10:30:00'"
        assert pg.literal_for_substitution(FRACTION) == "'10:30:00.123456'"


class TestCatalog:
    def test_jdbc_name(self) -> None:
        assert datatype_to_jdbc(DataType.TIME) == "TIME"

    def test_information_schema_dimensions(self) -> None:
        catalog = build_catalog(datasets_by_datasource={DS: [shifts()]})
        batch = match_info_schema(parsed=_parsed("SELECT * FROM INFORMATION_SCHEMA.DIMENSIONS"), catalog=catalog)
        assert batch is not None
        types = {r["dimension_name"]: r["data_type"] for r in batch.rows if r["table_name"] == "shifts"}
        assert types["starts_at"] == "TIME"

    def test_information_schema_columns(self) -> None:
        catalog = build_catalog(datasets_by_datasource={DS: [shifts()]})
        batch = match_info_schema(parsed=_parsed("SELECT * FROM INFORMATION_SCHEMA.COLUMNS"), catalog=catalog)
        assert batch is not None
        types = {r["column_name"]: r["data_type"] for r in batch.rows if r["table_name"] == "shifts"}
        assert types["starts_at"] == "TIME"

    def test_pg_catalog_relations(self) -> None:
        catalog = build_catalog_grouped_by_schema(datasets_by_datasource={DS: [shifts()]})
        relations = {r.name: r for r in build_catalog_relations(catalog)}
        columns = {r["column_name"]: r for r in relations["_is_columns"].rows if r["table_name"] == "shifts"}
        assert columns["starts_at"]["udt_name"] == "time"
        assert columns["starts_at"]["data_type"] == "time"
        pg_type = {r["oid"]: r for r in relations["pg_type"].rows}
        assert pg_type[OID_TIME]["typname"] == "time"
        assert pg_type[OID_TIME]["typlen"] == 8
        attributes = {r["attname"]: r for r in relations["pg_attribute"].rows
                      if r["attrelid"] == stable_oid("public", "shifts")}
        assert attributes["starts_at"]["atttypid"] == OID_TIME
        assert attributes["label"]["atttypid"] == proto.OID_TEXT
