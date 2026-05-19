"""Tests for slayer.flight.handlers — Flight SQL command dispatch.

Covers:

* Catalog commands (GetCatalogs / GetDbSchemas / GetTables / GetTableTypes)
  return correctly-shaped pa.Tables built from a real ``FlightCatalog``.
* Stubbed commands return well-typed empty pa.Tables.
* The Any-wrapped command/ticket decoder round-trips against the
  capture-corpus fixtures (DEV-1390 §1.1).
* Prepared-statement creation produces a ``dataset_schema`` derived
  from the LIMIT-0 schema (with an in-memory mock engine).
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as fl
import pytest
from google.protobuf.any_pb2 import Any as PbAny

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.engine.query_engine import SlayerResponse
from slayer.flight import _flight_sql_pb2 as fsql_pb
from slayer.flight.catalog import CATALOG_NAME
from slayer.flight.handlers import (
    FlightHandlers,
    _COMMAND_BY_TYPE_URL,
    _TYPE_URL_PREFIX,
    _pack_any,
    decode_command,
    decode_ticket,
)
from slayer.flight.translator import InfoSchemaResult, ProbeResult, TranslationError


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "capture-latest.jsonl"


# --- in-memory storage / engine fakes ---------------------------------------


class _FakeStorage:
    """Minimal async StorageBackend stand-in for tests."""

    def __init__(self, models_by_ds: dict[str, list[SlayerModel]]) -> None:
        self._by_ds = models_by_ds

    async def list_datasources(self) -> list[str]:  # NOSONAR(S7503) — must match async interface (called via await in production)
        return list(self._by_ds.keys())

    async def list_models(self, *, data_source: str | None = None) -> list[str]:  # NOSONAR(S7503) — must match async interface (called via await in production)
        return [m.name for m in self._by_ds.get(data_source or "", [])]

    async def get_model(self, *, name: str, data_source: str | None = None):  # NOSONAR(S7503) — must match async interface (called via await in production)
        for m in self._by_ds.get(data_source or "", []):
            if m.name == name:
                return m
        return None


class _FakeEngine:
    """Returns a fixed response — enough for LIMIT-0 schema derivation."""

    def __init__(self, *, response: SlayerResponse) -> None:
        self._response = response

    async def execute(self, *, query):  # noqa: ARG002  # NOSONAR(S7503) — must match async interface (called via await in production)
        return self._response


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
    )


def _make_handlers(*, response: SlayerResponse | None = None) -> FlightHandlers:
    storage = _FakeStorage({"jaffle": [_orders_model()]})
    if response is None:
        response = SlayerResponse(data=[], columns=[])
    engine = _FakeEngine(response=response)
    return FlightHandlers(engine=engine, storage=storage)  # type: ignore[arg-type]


# --- catalog handlers --------------------------------------------------------


def test_get_catalogs_returns_one_row_named_slayer() -> None:
    handlers = _make_handlers()
    table = handlers.handle_get_catalogs()
    assert table.to_pylist() == [{"catalog_name": CATALOG_NAME}]


def test_get_db_schemas_returns_one_row_per_datasource() -> None:
    handlers = _make_handlers()
    table = handlers.handle_get_db_schemas(fsql_pb.CommandGetDbSchemas())
    assert table.to_pylist() == [{"catalog_name": "slayer", "db_schema_name": "jaffle"}]


def test_get_tables_returns_models_with_table_type() -> None:
    handlers = _make_handlers()
    table = handlers.handle_get_tables(fsql_pb.CommandGetTables())
    rows = table.to_pylist()
    assert rows == [{
        "catalog_name": "slayer",
        "db_schema_name": "jaffle",
        "table_name": "orders",
        "table_type": "TABLE",
    }]


def test_get_table_types_returns_three_rows() -> None:
    handlers = _make_handlers()
    table = handlers.handle_get_table_types()
    assert table.to_pylist() == [
        {"table_type": "TABLE"},
        {"table_type": "VIEW"},
        {"table_type": "SEMANTIC_MODEL"},
    ]


# --- stubbed handlers --------------------------------------------------------


def test_get_primary_keys_empty_well_typed() -> None:
    handlers = _make_handlers()
    table = handlers.handle_get_primary_keys()
    assert table.num_rows == 0
    assert "column_name" in table.schema.names
    assert table.schema.field("key_sequence").type == pa.int32()


def test_keys_handlers_all_empty_with_consistent_schema() -> None:
    handlers = _make_handlers()
    for tbl in (
        handlers.handle_get_exported_keys(),
        handlers.handle_get_imported_keys(),
        handlers.handle_get_cross_reference(),
    ):
        assert tbl.num_rows == 0
        assert "pk_table_name" in tbl.schema.names
        assert "fk_column_name" in tbl.schema.names


def test_get_xdbc_type_info_lists_arrow_types() -> None:
    handlers = _make_handlers()
    table = handlers.handle_get_xdbc_type_info()
    type_names = {r["type_name"] for r in table.to_pylist()}
    assert {"VARCHAR", "BIGINT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"} <= type_names


def test_get_sql_info_includes_server_name_and_version() -> None:
    handlers = _make_handlers()
    table = handlers.handle_get_sql_info()
    rows = table.to_pylist()
    by_info = {r["info_name"]: r["value"] for r in rows}
    assert by_info[int(fsql_pb.SqlInfo.FLIGHT_SQL_SERVER_NAME)] == "SLayer"
    # Version comes from slayer.__version__ — non-empty.
    assert by_info[int(fsql_pb.SqlInfo.FLIGHT_SQL_SERVER_VERSION)]


# --- prepared-statement creation --------------------------------------------


def _unpack_prepared_statement_result(
    bytes_out: bytes,
) -> "fsql_pb.ActionCreatePreparedStatementResult":
    """Helper: the handler's response is ``Any``-wrapped per the Flight SQL
    spec (the Apache JDBC driver refuses bare ``ActionCreatePreparedStatementResult``
    bytes). Unwrap and return the inner message."""
    any_msg = PbAny()
    any_msg.ParseFromString(bytes_out)
    assert any_msg.type_url.endswith("ActionCreatePreparedStatementResult"), (
        f"unexpected response type_url: {any_msg.type_url!r}"
    )
    response = fsql_pb.ActionCreatePreparedStatementResult()
    response.ParseFromString(any_msg.value)
    return response


def test_create_prepared_statement_returns_handle_and_dataset_schema() -> None:
    handlers = _make_handlers(
        response=SlayerResponse(
            data=[],
            columns=["orders.revenue_sum"],
        ),
    )
    cmd = fsql_pb.ActionCreatePreparedStatementRequest()
    cmd.query = "SELECT revenue_sum FROM jaffle.orders"
    bytes_out = handlers.handle_create_prepared_statement(cmd)
    response = _unpack_prepared_statement_result(bytes_out)
    assert response.prepared_statement_handle == cmd.query.encode("utf-8")
    # dataset_schema is Arrow-IPC bytes — round-trip back to a pa.Schema.
    reader = pa.ipc.open_stream(pa.BufferReader(response.dataset_schema))
    schema = reader.schema
    assert "revenue_sum" in schema.names


def test_create_prepared_statement_for_probe_returns_canned_schema() -> None:
    handlers = _make_handlers()
    cmd = fsql_pb.ActionCreatePreparedStatementRequest()
    cmd.query = "SELECT 1"
    bytes_out = handlers.handle_create_prepared_statement(cmd)
    response = _unpack_prepared_statement_result(bytes_out)
    reader = pa.ipc.open_stream(pa.BufferReader(response.dataset_schema))
    schema = reader.schema
    assert schema.field("1").type == pa.int64()


def test_close_prepared_statement_is_a_no_op() -> None:
    handlers = _make_handlers()
    cmd = fsql_pb.ActionClosePreparedStatementRequest()
    cmd.prepared_statement_handle = b"SELECT 1"
    assert handlers.handle_close_prepared_statement(cmd) is None


# --- protobuf Any decoder against captured fixtures -------------------------


def _load_capture() -> list[dict]:
    if not FIXTURE_PATH.exists():
        pytest.skip("capture-latest.jsonl not present")
    return [json.loads(line) for line in FIXTURE_PATH.read_text().splitlines()]


def test_every_captured_command_type_url_is_recognised() -> None:
    """Sanity: the type_urls observed in the capture all map to a generated
    protobuf class in ``_COMMAND_BY_TYPE_URL``."""
    captured_urls: set[str] = set()
    for rec in _load_capture():
        for key in ("cmd_b64", "body_b64", "ticket_b64"):
            v = rec.get(key)
            if not v:
                continue
            type_url, _ = decode_command(base64.b64decode(v))
            captured_urls.add(type_url)
    # Every captured URL must be in our dispatch table.
    unknown = captured_urls - set(_COMMAND_BY_TYPE_URL)
    assert not unknown, f"unrecognised captured type_urls: {unknown}"
    # And every Apache-Arrow URL we model is captured at least once OR is
    # one of the spec's "[unobserved]" entries.
    # Per CAPTURE-FINDINGS.md, these messages are not in the first-pass
    # capture either because the driver doesn't issue them during
    # DatabaseMetaData introspection (CommandStatementQuery / GetSqlInfo /
    # GetXdbcTypeInfo) or because our capture stub returned empty results
    # which aborted the prepared-statement flow before the second/close
    # legs could fire (CommandPreparedStatementQuery / ActionClose). A
    # follow-up capture against the real Phase-1 server fills these in.
    expected_unobserved = {
        f"{_TYPE_URL_PREFIX}CommandStatementQuery",
        f"{_TYPE_URL_PREFIX}CommandGetSqlInfo",
        f"{_TYPE_URL_PREFIX}CommandGetXdbcTypeInfo",
        f"{_TYPE_URL_PREFIX}TicketStatementQuery",
        f"{_TYPE_URL_PREFIX}CommandPreparedStatementQuery",
        f"{_TYPE_URL_PREFIX}ActionClosePreparedStatementRequest",
    }
    not_seen = set(_COMMAND_BY_TYPE_URL) - captured_urls - expected_unobserved
    assert not not_seen, f"modelled but not captured: {not_seen}"


def test_decode_get_catalogs_captured() -> None:
    """Decode the `CommandGetCatalogs` payload from the capture corpus."""
    records = _load_capture()
    for rec in records:
        cmd_b64 = rec.get("cmd_b64")
        if not cmd_b64:
            continue
        type_url, msg = decode_command(base64.b64decode(cmd_b64))
        if type_url.endswith("CommandGetCatalogs"):
            assert isinstance(msg, fsql_pb.CommandGetCatalogs)
            return
    pytest.fail("no CommandGetCatalogs found in fixtures")


def test_decode_action_create_prepared_statement_captured() -> None:
    records = _load_capture()
    for rec in records:
        body_b64 = rec.get("body_b64")
        if not body_b64:
            continue
        type_url, msg = decode_command(base64.b64decode(body_b64))
        if type_url.endswith("ActionCreatePreparedStatementRequest"):
            assert isinstance(msg, fsql_pb.ActionCreatePreparedStatementRequest)
            # Capture exercised many SQL statements — query is non-empty.
            assert msg.query
            return
    pytest.fail("no ActionCreatePreparedStatementRequest found in fixtures")


def test_pack_any_round_trips() -> None:
    inner = fsql_pb.CommandStatementQuery()
    inner.query = "SELECT 1"
    packed = _pack_any(inner, "CommandStatementQuery")
    type_url, recovered = decode_command(packed)
    assert type_url.endswith("CommandStatementQuery")
    assert recovered.query == "SELECT 1"


# --- get_flight_info / do_get for SQL ---------------------------------------


def test_get_flight_info_for_probe_builds_canned_schema() -> None:
    handlers = _make_handlers()
    descriptor = fl.FlightDescriptor.for_command(b"")
    info = handlers.get_flight_info_for_sql(descriptor, "SELECT 1")
    assert info.schema.field("1").type == pa.int64()
    # Ticket is Any-wrapped TicketStatementQuery containing the original SQL.
    endpoint = info.endpoints[0]
    ticket_bytes = endpoint.ticket.ticket
    type_url, msg = decode_ticket(ticket_bytes)
    assert type_url.endswith("TicketStatementQuery")
    assert msg.statement_handle == b"SELECT 1"


def test_do_get_for_probe_returns_canned_table() -> None:
    handlers = _make_handlers()
    stream = handlers.do_get_for_sql("SELECT 1")
    # RecordBatchStream is a server-side return-type marker with no public
    # read API; assert the wrapper shape and re-translate to read the bytes.
    assert isinstance(stream, fl.RecordBatchStream)
    result = handlers._translate("SELECT 1")
    assert isinstance(result, ProbeResult)
    assert result.table.to_pylist() == [{"1": 1}]


def test_do_get_for_information_schema_returns_canned_table() -> None:
    handlers = _make_handlers()
    stream = handlers.do_get_for_sql("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA")
    assert isinstance(stream, fl.RecordBatchStream)
    result = handlers._translate("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA")
    assert isinstance(result, InfoSchemaResult)
    assert result.table.to_pylist() == [
        {"catalog_name": "slayer", "schema_name": "jaffle"},
    ]


def test_do_get_for_dml_raises_translation_error_propagating() -> None:
    """Unknown / forbidden SQL surfaces as a TranslationError from translate(),
    which the handler propagates (server.py maps to FlightServerError)."""
    handlers = _make_handlers()
    with pytest.raises(TranslationError):
        handlers.do_get_for_sql("INSERT INTO orders VALUES (1)")
