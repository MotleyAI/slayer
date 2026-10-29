"""List columns and list results in the BI facades: catalogs keep them, values travel as JSON text
(spec: facade/list-values)."""

from __future__ import annotations

import json
import struct
from datetime import date, datetime
from typing import Any

import pyarrow as pa
import pytest

from slayer.core.enums import DataType
from slayer.core.models import ModelMeasure, SlayerModel
from slayer.engine.query_engine import SlayerResponse
from slayer.facade.catalog import build_catalog, build_catalog_grouped_by_schema
from slayer.facade.datatypes import datatype_to_jdbc
from slayer.facade.translator import QueryResult
from slayer.flight.handlers import FlightHandlers
from slayer.flight.types import datatype_to_arrow
from slayer.pg_facade import protocol as proto
from slayer.pg_facade.types import datatype_to_oid, value_to_binary, value_to_text
from tests._dev2120_fixtures import contractors_model
from tests.flight.test_handlers import _FakeEngine as _FlightEngine
from tests.flight.test_handlers import _FakeStorage as _FlightStorage
from tests.pg_facade.test_connection import _FakeEngine as _PgEngine
from tests.pg_facade.test_connection import _FakeStorage as _PgStorage
from tests.pg_facade.test_connection import _messages, _parse_row_description, _query, _run, _startup, _terminate

DS = "jaffle"


def _model() -> SlayerModel:
    return contractors_model(data_source=DS).model_copy(update={
        "measures": [ModelMeasure(name="cities", formula="collect(city)")]})


class TestCatalogs:
    def test_catalog_keeps_list_models(self) -> None:
        catalog = build_catalog(models_by_datasource={DS: [_model()]})
        (schema,) = catalog.schemas
        table = next(t for t in schema.tables if t.name == "contractors")
        assert "skills" in {d.name for d in table.dimensions}

    def test_grouped_catalog_keeps_list_models(self) -> None:
        catalog = build_catalog_grouped_by_schema(models_by_datasource={DS: [_model()]})
        tables = {t.name for s in catalog.schemas for t in s.tables}
        assert "contractors" in tables

    def test_lists_are_text_on_every_wire(self) -> None:
        assert datatype_to_jdbc(DataType.ARRAY) == "VARCHAR"
        assert datatype_to_oid(DataType.ARRAY) == proto.OID_TEXT
        assert datatype_to_arrow(DataType.ARRAY) == pa.utf8()


#: A list value → the JSON array it must become.
SAMPLES: list[tuple[list[Any], list[Any]]] = [
    (["Oslo", "Rome"], ["Oslo", "Rome"]),
    (['a "b" \\c'], ['a "b" \\c']),
    ([True, False], [True, False]),
    ([1, 2], [1, 2]),
    ([date(2024, 2, 29)], ["2024-02-29"]),
    ([datetime(2024, 2, 29, 10, 30)], ["2024-02-29T10:30:00"]),
    ([], []),
]


class TestPgWireValues:
    @pytest.mark.parametrize(("value", "expected"), SAMPLES)
    def test_text_is_json(self, value: list[Any], expected: list[Any]) -> None:
        raw = value_to_text(value, proto.OID_TEXT)
        assert raw is not None
        assert json.loads(raw.decode()) == expected

    def test_escaping(self) -> None:
        raw = value_to_text(['a "b" \\c'], proto.OID_TEXT)
        assert raw is not None
        assert '\\"b\\"' in raw.decode(), raw
        assert "\\\\c" in raw.decode(), raw

    def test_booleans_are_json_literals(self) -> None:
        raw = value_to_text([True, False], proto.OID_TEXT)
        assert raw is not None
        assert raw.decode().replace(" ", "") == "[true,false]"

    def test_null_list_is_sql_null(self) -> None:
        assert value_to_text(None, proto.OID_TEXT) is None

    def test_binary_format_carries_the_same_text(self) -> None:
        assert value_to_binary(["Oslo", "Rome"], proto.OID_TEXT) == value_to_text(["Oslo", "Rome"], proto.OID_TEXT)

    async def test_a_collected_list_over_the_postgres_wire(self) -> None:
        writer = await _run(_startup(user="u", database=DS) + _query("SELECT cities FROM contractors") + _terminate(),
                            storage=_PgStorage({DS: [_model()]}),
                            engine=_PgEngine([{"contractors.cities": ["Oslo", "Rome"]}]))
        msgs = _messages(writer.buffer)
        (_name, oid, _fmt), = _parse_row_description(next(b for t, b in msgs if t == "T"))
        assert oid == proto.OID_TEXT
        body = next(b for t, b in msgs if t == "D")
        length = struct.unpack_from(">i", body, 2)[0]
        text = body[6:6 + length].decode()
        assert text.startswith("["), text
        assert json.loads(text) == ["Oslo", "Rome"]


def _flight_text(value: Any) -> Any:
    handlers = FlightHandlers(
        engine=_FlightEngine(response=SlayerResponse(data=[{"contractors.cities": value}])),  # type: ignore[arg-type]
        storage=_FlightStorage({DS: [_model()]}),  # type: ignore[arg-type]
    )
    result = handlers._translate("SELECT cities FROM contractors")
    assert isinstance(result, QueryResult), result
    table = handlers._execute_full(result)
    assert table.schema.field("cities").type == pa.utf8()
    return table.to_pylist()[0]["cities"]


class TestFlightValues:
    def test_flight_parity(self) -> None:
        text = _flight_text(["Oslo", "Rome"])
        assert json.loads(text) == ["Oslo", "Rome"]

    @pytest.mark.parametrize(("value", "expected"), SAMPLES)
    def test_same_text_as_the_postgres_wire(self, value: list[Any], expected: list[Any]) -> None:
        pg = value_to_text(value, proto.OID_TEXT)
        assert pg is not None
        assert _flight_text(value) == pg.decode()
        assert json.loads(pg.decode()) == expected

    def test_null_list_is_null(self) -> None:
        assert _flight_text(None) is None
