"""One encoding of each result value on every output path (spec: results/value-encoding; principle 19)."""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
import math
import uuid
from collections.abc import Callable, Iterator
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict

from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.facade.translator import QueryResult
from slayer.flight.handlers import FlightHandlers
from slayer.mcp.server import create_mcp_server
from slayer.pg_facade import protocol as proto
from slayer.pg_facade.types import value_to_binary, value_to_text
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process
from tests._dev2172_fixtures import strict_json
from tests.flight.test_handlers import _FakeEngine as _FlightEngine
from tests.flight.test_handlers import _FakeStorage as _FlightStorage

DS = "vals_ds"
COL = "vals.v"
QUERY = {"source_model": "vals", "dimensions": ["v"], "limit": 5}
UID = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
BIG = Decimal("12345678901234567890.12")
PLUS_TWO = dt.timezone(dt.timedelta(hours=2))


class Case(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    id: str
    value: Any
    #: The JSON form, parsed by ``strict_json``.
    json_form: Any
    #: The text form; ``None`` → compared through ``read_text``.
    text: str | None = None
    #: Raw text the JSON output must contain verbatim (exact number digits).
    raw: str | None = None
    #: The value lands in a text-typed column on the Postgres wire and Arrow Flight.
    wire: bool = False
    #: Reads a text form back for comparison when ``text`` is ``None``.
    read_text: Callable[[str], Any] | None = None


def _ts(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text)


def _offset(text: str) -> tuple[dt.datetime, dt.timedelta | None]:
    parsed = dt.datetime.fromisoformat(text)
    return parsed, parsed.utcoffset()


CASES: list[Case] = [
    Case(id="list", value=["Oslo", 'a "b" \\c', True, None, 1], json_form=["Oslo", 'a "b" \\c', True, None, 1],
         read_text=strict_json, wire=True),
    Case(id="dict", value={"a": [1, "x"], "b": None}, json_form={"a": [1, "x"], "b": None}, read_text=strict_json,
         wire=True),
    Case(id="dict-non-string-keys", value={1: "a", UID: "b"}, json_form={"1": "a", str(UID): "b"},
         read_text=strict_json, wire=True),
    Case(id="uuid", value=UID, json_form=str(UID), text=str(UID), wire=True),
    Case(id="bytes", value=b"\xde\xad\xbe\xef", json_form="\\xdeadbeef", text="\\xdeadbeef", wire=True),
    Case(id="interval", value=dt.timedelta(days=1, hours=2), json_form="P1DT2H", text="P1DT2H", wire=True),
    Case(id="interval-fraction", value=dt.timedelta(days=1, hours=2, minutes=3, seconds=4.5),
         json_form="P1DT2H3M4.5S", text="P1DT2H3M4.5S", wire=True),
    Case(id="interval-negative", value=-dt.timedelta(hours=1, minutes=30), json_form="-PT1H30M", text="-PT1H30M",
         wire=True),
    Case(id="decimal", value=BIG, json_form=BIG, text=str(BIG), raw=str(BIG)),
    Case(id="decimal-exponent", value=Decimal("1E+3"), json_form=Decimal("1E+3"), text="1E+3", raw="1E+3"),
    Case(id="decimal-nan", value=Decimal("NaN"), json_form=None, text="NaN"),
    Case(id="decimal-infinity", value=Decimal("-Infinity"), json_form=None, text="-Infinity"),
    Case(id="float-nan", value=math.nan, json_form=None, text="NaN"),
    Case(id="float-infinity", value=math.inf, json_form=None, text="Infinity"),
    Case(id="float-negative-infinity", value=-math.inf, json_form=None, text="-Infinity"),
    Case(id="list-non-finite", value=[1.5, math.nan, math.inf, Decimal("NaN")],
         json_form=[Decimal("1.5"), None, None, None], read_text=strict_json, wire=True),
    Case(id="list-decimal", value=[BIG, Decimal("1E+3")], json_form=[BIG, Decimal("1E+3")], read_text=strict_json,
         raw=str(BIG), wire=True),
    Case(id="date", value=dt.date(2024, 2, 29), json_form="2024-02-29", text="2024-02-29"),
    Case(id="timestamp", value=dt.datetime(2024, 2, 29, 10, 30), json_form="2024-02-29T10:30:00", read_text=_ts),
    Case(id="timestamp-tz", value=dt.datetime(2024, 2, 29, 10, 30, tzinfo=PLUS_TWO),
         json_form="2024-02-29T10:30:00+02:00", read_text=_offset),
    Case(id="list-temporal", value=[dt.date(2024, 2, 29), dt.datetime(2024, 2, 29, 10, 30, tzinfo=PLUS_TWO)],
         json_form=["2024-02-29", "2024-02-29T10:30:00+02:00"], read_text=strict_json, wire=True),
    Case(id="nested", value=[{"id": UID, "raw": b"\x01", "span": dt.timedelta(hours=2),
                              "n": [Decimal("1.10"), math.nan], "on": dt.date(2024, 1, 5)}],
         json_form=[{"id": str(UID), "raw": "\\x01", "span": "PT2H", "n": [Decimal("1.10"), None],
                "on": "2024-01-05"}], read_text=strict_json, wire=True),
    Case(id="dict-nested", value={"k": [UID, {"d": dt.date(2024, 1, 5), "b": b"\xff"}]},
         json_form={"k": [str(UID), {"d": "2024-01-05", "b": "\\xff"}]}, read_text=strict_json, wire=True),
    Case(id="text", value="plain", json_form="plain", text="plain", wire=True),
    Case(id="integer", value=7, json_form=7, text="7"),
    Case(id="boolean", value=True, json_form=True),
]
WIRE_CASES = [c for c in CASES if c.wire]


def _expected_text(case: Case) -> Any:
    """What ``_read_text`` must give back: the text, the parsed JSON, or the temporal JSON string read back."""
    if case.text is not None:
        return case.text
    assert case.read_text is not None, case.id
    return case.json_form if case.read_text is strict_json else case.read_text(case.json_form)


def _read_text(case: Case, text: str) -> Any:
    if case.text is not None:
        return text
    assert case.read_text is not None, case.id
    return case.read_text(text)


def _response(value: Any) -> SlayerResponse:
    return SlayerResponse(data=[{COL: value}], columns=[COL])


@pytest.fixture
def serve(monkeypatch: pytest.MonkeyPatch) -> Callable[[Any], None]:
    """Make every engine execution return one row whose ``vals.v`` cell is the given value."""
    def install(value: Any) -> None:
        async def fake_execute(self: SlayerQueryEngine, *args: Any, **kwargs: Any) -> SlayerResponse:
            return _response(value)
        monkeypatch.setattr(SlayerQueryEngine, "execute", fake_execute)
    return install


@pytest.fixture(scope="module")
def store_dir(tmp_path_factory: pytest.TempPathFactory) -> str:
    return str(tmp_path_factory.mktemp("dev2172-codec-store"))


@pytest.fixture(scope="module")
def mcp_server(store_dir: str) -> Any:
    return create_mcp_server(storage=YAMLStorage(base_dir=store_dir))


@pytest.fixture(scope="module")
def rest_client(store_dir: str) -> Iterator[TestClient]:
    with TestClient(create_app(storage=YAMLStorage(base_dir=store_dir))) as client:
        yield client


# --------------------------------------------------------------------------- #
# Output paths
# --------------------------------------------------------------------------- #
async def _mcp(server: Any, fmt: str) -> str:
    blocks, _ = await server.call_tool(name="query", arguments={"query": QUERY, "format": fmt})
    return blocks[0].text


async def mcp_json(server: Any) -> tuple[str, Any]:
    text = await _mcp(server, "json")
    payload = strict_json(text)
    rows = payload["data"] if isinstance(payload, dict) else payload
    return text, rows[0][COL]


async def mcp_csv(server: Any) -> str:
    text = await _mcp(server, "csv")
    body = "\n".join(line for line in text.splitlines() if not line.startswith("#"))
    header, row = list(csv.reader(io.StringIO(body)))[:2]
    assert header == [COL]
    return row[0]


async def mcp_markdown(server: Any) -> str:
    text = await _mcp(server, "markdown")
    row = text.splitlines()[2]
    assert row.startswith("| ") and row.endswith(" |"), row
    return row[2:-2]


def rest(client: TestClient) -> tuple[str, Any]:
    resp = client.post("/query", json=QUERY)
    assert resp.status_code == 200, resp.text
    return resp.text, strict_json(resp.text)["data"][0][COL]


def cli_json(store: str) -> tuple[str, Any]:
    result = run_cli_in_process(["query", json.dumps(QUERY), "--storage", store, "--format", "json"])
    assert result.returncode == 0, result.stderr
    return result.stdout, strict_json(result.stdout)[0][COL]


def cli_table(store: str) -> str:
    result = run_cli_in_process(["query", json.dumps(QUERY), "--storage", store])
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[0] == COL
    return lines[2]


def pg_text(value: Any) -> str:
    raw = value_to_text(value, proto.OID_TEXT)
    assert raw is not None
    return raw.decode("utf-8")


def pg_binary(value: Any) -> str:
    raw = value_to_binary(value, proto.OID_TEXT)
    assert raw is not None
    return raw.decode("utf-8")


def _vals_model(column_type: DataType = DataType.TEXT) -> SlayerModel:
    return SlayerModel(name="vals", sql_table="vals", data_source=DS,
                       columns=[Column(name="id", type=DataType.INT, primary_key=True),
                                Column(name="v", type=column_type)])


def flight_cell(value: Any, *, column_type: DataType = DataType.TEXT) -> Any:
    handlers = FlightHandlers(
        engine=_FlightEngine(response=_response(value)),  # type: ignore[arg-type]
        storage=_FlightStorage({DS: [_vals_model(column_type)]}),  # type: ignore[arg-type]
    )
    result = handlers._translate("SELECT v FROM vals")
    assert isinstance(result, QueryResult), result
    table = handlers._execute_full(result)
    if column_type in (DataType.TEXT, DataType.UNKNOWN, DataType.ARRAY):
        assert table.schema.field("v").type == pa.utf8()
    return table.to_pylist()[0]["v"]


# --------------------------------------------------------------------------- #
# JSON form: MCP json, REST /query, CLI json
# --------------------------------------------------------------------------- #
JSON_PATHS = ("mcp-json", "rest", "cli-json")


async def _json_output(path: str, *, mcp_server: Any, rest_client: TestClient, store_dir: str) -> tuple[str, Any]:
    if path == "mcp-json":
        return await mcp_json(mcp_server)
    if path == "rest":
        return rest(rest_client)
    return cli_json(store_dir)


@pytest.mark.parametrize("path", JSON_PATHS)
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.id)
async def test_json_form(case: Case, path: str, serve: Callable[[Any], None], mcp_server: Any,
                         rest_client: TestClient, store_dir: str) -> None:
    serve(case.value)
    raw, cell = await _json_output(path, mcp_server=mcp_server, rest_client=rest_client, store_dir=store_dir)
    assert cell == case.json_form
    assert type(cell) is type(case.json_form) or case.json_form is None
    if case.raw is not None:
        assert case.raw in raw and f'"{case.raw}"' not in raw, raw


# --------------------------------------------------------------------------- #
# Text form: MCP csv / markdown, CLI table, Postgres wire, Arrow Flight
# --------------------------------------------------------------------------- #
TEXT_PATHS = ("mcp-csv", "mcp-markdown", "cli-table")
WIRE_PATHS = ("pg-text", "pg-binary", "flight")


async def _text_output(path: str, value: Any, *, mcp_server: Any, store_dir: str) -> str:
    if path == "mcp-csv":
        return await mcp_csv(mcp_server)
    if path == "mcp-markdown":
        return await mcp_markdown(mcp_server)
    if path == "cli-table":
        return cli_table(store_dir)
    if path == "pg-text":
        return pg_text(value)
    if path == "pg-binary":
        return pg_binary(value)
    return flight_cell(value)


TEXT_MATRIX = [(c, p) for c in CASES if c.id != "boolean" for p in TEXT_PATHS] + \
    [(c, p) for c in WIRE_CASES for p in WIRE_PATHS]


@pytest.mark.parametrize(("case", "path"), TEXT_MATRIX, ids=lambda x: x if isinstance(x, str) else x.id)
async def test_text_form(case: Case, path: str, serve: Callable[[Any], None], mcp_server: Any,
                         store_dir: str) -> None:
    serve(case.value)
    text = await _text_output(path, case.value, mcp_server=mcp_server, store_dir=store_dir)
    assert _read_text(case, text) == _expected_text(case)


@pytest.mark.parametrize("case", [c for c in CASES if c.id != "boolean"], ids=lambda c: c.id)
async def test_same_text_on_every_text_path(case: Case, serve: Callable[[Any], None], mcp_server: Any,
                                            store_dir: str) -> None:
    serve(case.value)
    paths = TEXT_PATHS + (WIRE_PATHS if case.wire else ())
    texts = {p: await _text_output(p, case.value, mcp_server=mcp_server, store_dir=store_dir) for p in paths}
    assert len(set(texts.values())) == 1, texts


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.id)
async def test_same_json_on_every_json_path(case: Case, serve: Callable[[Any], None], mcp_server: Any,
                                            rest_client: TestClient, store_dir: str) -> None:
    serve(case.value)
    cells = [(await _json_output(p, mcp_server=mcp_server, rest_client=rest_client, store_dir=store_dir))[1]
             for p in JSON_PATHS]
    assert all(c == cells[0] for c in cells), cells


@pytest.mark.parametrize("case", [c for c in CASES if isinstance(c.value, (list, dict))], ids=lambda c: c.id)
async def test_list_and_json_text_is_the_json_form(case: Case, serve: Callable[[Any], None],
                                                   mcp_server: Any) -> None:
    """Inside a list or JSON text a value takes its JSON form, the same as MCP json returns."""
    serve(case.value)
    _raw, cell = await mcp_json(mcp_server)
    assert strict_json(pg_text(case.value)) == cell


# --------------------------------------------------------------------------- #
# Scenario pins
# --------------------------------------------------------------------------- #
class TestScenarios:
    def test_non_finite_scalar_is_native_on_the_postgres_wire(self) -> None:
        assert value_to_text(math.nan, proto.OID_FLOAT8) == b"NaN"
        assert value_to_text(math.inf, proto.OID_FLOAT8) == b"Infinity"
        assert value_to_text(-math.inf, proto.OID_FLOAT8) == b"-Infinity"

    def test_exact_decimal_on_the_postgres_wire(self) -> None:
        assert value_to_text(BIG, proto.OID_FLOAT8) == str(BIG).encode()

    def test_timezone_aware_timestamp_keeps_its_utc_conversion_on_the_postgres_wire(self) -> None:
        value = dt.datetime(2024, 2, 29, 10, 30, tzinfo=PLUS_TWO)
        assert value_to_text(value, proto.OID_TIMESTAMP) == b"2024-02-29 08:30:00"

    def test_null_stays_null_on_the_wire(self) -> None:
        assert value_to_text(None, proto.OID_TEXT) is None
        assert flight_cell(None) is None

    def test_flight_double_column_reads_a_decimal_as_a_float(self) -> None:
        assert flight_cell(Decimal("2.5"), column_type=DataType.DOUBLE) == pytest.approx(2.5)

    def test_flight_int_column_reads_a_decimal_as_an_int(self) -> None:
        cell = flight_cell(Decimal("3"), column_type=DataType.INT)
        assert cell == 3 and isinstance(cell, int)

    def test_flight_double_column_keeps_a_native_nan(self) -> None:
        assert math.isnan(flight_cell(math.nan, column_type=DataType.DOUBLE))

    async def test_rest_body_keeps_the_declared_response_shape(self, serve: Callable[[Any], None],
                                                               rest_client: TestClient) -> None:
        serve(BIG)
        body = strict_json(rest_client.post("/query", json=QUERY).text)
        assert {"data", "row_count", "columns"} <= set(body)
        assert body["row_count"] == 1 and body["columns"] == [COL]

    def test_rest_openapi_still_declares_query_response(self, rest_client: TestClient) -> None:
        schema = rest_client.get("/openapi.json").json()
        ok = schema["paths"]["/query"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]
        assert ok == {"$ref": "#/components/schemas/QueryResponse"}
