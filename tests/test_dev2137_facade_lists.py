"""List columns and list results in the BI facades: catalogs keep them, values travel as JSON text
(spec: facade/list-values)."""

from __future__ import annotations

import json
import struct
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, ModelMeasure, SlayerModel
from slayer.engine.query_engine import SlayerResponse
from slayer.facade.catalog import build_catalog, build_catalog_grouped_by_schema
from slayer.facade.datatypes import datatype_to_jdbc
from slayer.facade.translator import QueryResult, translate
from slayer.flight.handlers import FlightHandlers
from slayer.flight.types import datatype_to_arrow
from slayer.pg_facade import protocol as proto
from slayer.pg_facade.types import datatype_to_oid, value_to_binary, value_to_text
from tests._dev2120_fixtures import contractors_model
from tests._dev2137_fixtures import assignments_model
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
        assert "skills" not in {d.name for d in table.dimensions}
        assert "skills_count" in {m.name for m in table.metrics}

    def test_list_columns_offer_only_count(self) -> None:
        model = _model().model_copy(update={"aggregations": [Aggregation(name="my_agg", formula="SUM({value})")]})
        (schema,) = build_catalog(models_by_datasource={DS: [model]}).schemas
        table = next(t for t in schema.tables if t.name == "contractors")
        assert {m.name for m in table.metrics if m.name.startswith("skills_")} == {"skills_count"}

    def test_grouped_catalog_keeps_list_models(self) -> None:
        catalog = build_catalog_grouped_by_schema(models_by_datasource={DS: [_model()]})
        tables = {t.name for s in catalog.schemas for t in s.tables}
        assert "contractors" in tables

    def test_joined_list_columns_are_not_dimensions(self) -> None:
        models = [_model(), assignments_model(data_source=DS)]
        (schema,) = build_catalog(models_by_datasource={DS: models}).schemas
        table = next(t for t in schema.tables if t.name == "assignments")
        names = {d.name for d in table.dimensions}
        assert "contractors.name" in names
        assert "contractors.skills" not in names

    def test_browse_mode_star_skips_list_columns(self) -> None:
        catalog = build_catalog(models_by_datasource={DS: [_model()]})
        result = translate("SELECT * FROM contractors", catalog, expand_star_in_browse_mode=True)
        assert isinstance(result, QueryResult), result
        projected = {flat for flat, _ in result.column_name_mapping}
        assert "contractors.name" in projected
        assert "contractors.skills" not in projected

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

    def test_decimals_are_exact(self) -> None:
        raw = value_to_text([Decimal("12345678901234567890.123"), Decimal("5.00")], proto.OID_TEXT)
        assert raw is not None
        assert raw.decode() == "[12345678901234567890.123, 5.00]"

    def test_non_finite_numbers_are_null(self) -> None:
        raw = value_to_text([float("nan"), float("inf"), Decimal("NaN"), 1.5], proto.OID_TEXT)
        assert raw is not None
        assert json.loads(raw.decode()) == [None, None, None, 1.5]

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
        engine=_FlightEngine(response=SlayerResponse(data=[{"contractors.cities": value}]),  # type: ignore[arg-type]
                             storage=_FlightStorage({DS: [_model()]})),
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
