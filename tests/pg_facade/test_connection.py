"""Tests for slayer.pg_facade.connection — the PgConnection state machine.

Driven over an in-memory asyncio stream pair with a fake storage + engine.
"""

from __future__ import annotations

import asyncio
import struct
import types
from typing import List, Optional, Tuple

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.pg_facade import protocol as proto
from slayer.pg_facade.connection import PgConnection


# --- fakes -------------------------------------------------------------------


class _FakeWriter:
    def __init__(self) -> None:
        self.buffer = bytearray()
        self.transport = types.SimpleNamespace()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


class _FakeStorage:
    def __init__(self, models_by_ds) -> None:
        self._models_by_ds = models_by_ds

    async def list_datasources(self) -> List[str]:
        return list(self._models_by_ds)

    async def list_models(self, *, data_source: str) -> List[str]:
        return [m.name for m in self._models_by_ds.get(data_source, [])]

    async def get_model(self, *, name: str, data_source: str):
        for m in self._models_by_ds.get(data_source, []):
            if m.name == name:
                return m
        return None


class _FakeEngine:
    def __init__(self, data) -> None:
        self.data = data

    async def execute(self, *, query=None, data_source=None):
        return types.SimpleNamespace(data=self.data)


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="order_date", type=DataType.DATE),
        ],
    )


def _storage() -> _FakeStorage:
    return _FakeStorage({"jaffle": [_orders_model()]})


class _CapturingEngine:
    """Records the last executed query so tests can assert substitution."""

    def __init__(self, data) -> None:
        self.data = data
        self.last_query = None

    async def execute(self, *, query=None, data_source=None):
        self.last_query = query
        return types.SimpleNamespace(data=self.data)


# --- client-message builders -------------------------------------------------


def _frame(type_char: bytes, body: bytes) -> bytes:
    return type_char + struct.pack(">i", len(body) + 4) + body


def _startup(**params: str) -> bytes:
    body = struct.pack(">i", proto.PROTOCOL_VERSION_3)
    for k, v in params.items():
        body += k.encode() + b"\x00" + v.encode() + b"\x00"
    body += b"\x00"
    return struct.pack(">i", len(body) + 4) + body


def _ssl_request() -> bytes:
    return struct.pack(">ii", 8, proto.SSL_REQUEST_CODE)


def _gssenc_request() -> bytes:
    return struct.pack(">ii", 8, proto.GSSENC_REQUEST_CODE)


def _cancel_request() -> bytes:
    return struct.pack(">iiii", 16, proto.CANCEL_REQUEST_CODE, 1, 0)


def _bad_version() -> bytes:
    body = struct.pack(">i", 12345) + b"\x00"
    return struct.pack(">i", len(body) + 4) + body


def _query(sql: str) -> bytes:
    return _frame(b"Q", sql.encode() + b"\x00")


def _password(pw: str) -> bytes:
    return _frame(b"p", pw.encode() + b"\x00")


def _terminate() -> bytes:
    return _frame(b"X", b"")


def _parse(name: str, sql: str, oids: Tuple[int, ...] = ()) -> bytes:
    body = name.encode() + b"\x00" + sql.encode() + b"\x00" + struct.pack(">h", len(oids))
    for o in oids:
        body += struct.pack(">i", o)
    return _frame(b"P", body)


def _bind(
    portal: str, stmt: str, *,
    values: Tuple[Optional[bytes], ...] = (),
    param_formats: Tuple[int, ...] = (),
    result_formats: Tuple[int, ...] = (),
) -> bytes:
    body = portal.encode() + b"\x00" + stmt.encode() + b"\x00"
    body += struct.pack(">h", len(param_formats))
    for f in param_formats:
        body += struct.pack(">h", f)
    body += struct.pack(">h", len(values))
    for v in values:
        if v is None:
            body += struct.pack(">i", -1)
        else:
            body += struct.pack(">i", len(v)) + v
    body += struct.pack(">h", len(result_formats))
    for f in result_formats:
        body += struct.pack(">h", f)
    return _frame(b"B", body)


def _describe(kind: str, name: str) -> bytes:
    return _frame(b"D", kind.encode() + name.encode() + b"\x00")


def _execute(portal: str, max_rows: int = 0) -> bytes:
    return _frame(b"E", portal.encode() + b"\x00" + struct.pack(">i", max_rows))


def _sync() -> bytes:
    return _frame(b"S", b"")


def _close(kind: str, name: str) -> bytes:
    return _frame(b"C", kind.encode() + name.encode() + b"\x00")


# --- session driver + output parsing -----------------------------------------


async def _run(
    input_bytes: bytes, *, token: Optional[str] = None, storage=None, engine=None,
    tls_ctx=None,
) -> _FakeWriter:
    reader = asyncio.StreamReader()
    reader.feed_data(input_bytes)
    reader.feed_eof()
    writer = _FakeWriter()
    conn = PgConnection(
        reader, writer,
        engine=engine or _FakeEngine([]),
        storage=storage or _storage(),
        token=token,
        tls_ctx=tls_ctx,
    )
    await conn.run()
    return writer


def _messages(buf: bytes, *, leading_raw: int = 0) -> List[Tuple[str, bytes]]:
    return proto.split_messages(bytes(buf[leading_raw:]))


def _types(msgs: List[Tuple[str, bytes]]) -> List[str]:
    return [t for t, _ in msgs]


def _ready_statuses(msgs: List[Tuple[str, bytes]]) -> List[bytes]:
    return [body for t, body in msgs if t == "Z"]


def _error_sqlstate(body: bytes) -> Optional[str]:
    i = 0
    while i < len(body) and body[i:i + 1] != b"\x00":
        ftype = body[i:i + 1]
        i += 1
        end = body.index(b"\x00", i)
        val = body[i:end].decode("utf-8")
        i = end + 1
        if ftype == b"C":
            return val
    return None


# --- startup / SSL -----------------------------------------------------------


async def test_ssl_request_without_tls_gets_N() -> None:
    writer = await _run(_ssl_request() + _startup(user="u", database="jaffle") + _terminate())
    assert writer.buffer[0:1] == b"N"


async def test_ssl_request_with_tls_gets_S(monkeypatch) -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(_ssl_request() + _startup(user="u", database="jaffle") + _terminate())
    reader.feed_eof()
    writer = _FakeWriter()
    conn = PgConnection(
        reader, writer, engine=_FakeEngine([]), storage=_storage(),
        token=None, tls_ctx=object(),
    )
    upgraded = []

    async def _fake_upgrade() -> None:
        upgraded.append(True)

    conn._perform_tls_upgrade = _fake_upgrade  # type: ignore[method-assign]
    await conn.run()
    assert writer.buffer[0:1] == b"S"
    assert upgraded == [True]


async def test_bad_protocol_version_errors_and_closes() -> None:
    writer = await _run(_bad_version())
    msgs = _messages(writer.buffer)
    assert _types(msgs) == ["E"]
    assert _error_sqlstate(msgs[0][1]) == proto.SQLSTATE_FEATURE_NOT_SUPPORTED


# --- auth --------------------------------------------------------------------


async def test_no_token_completes_startup() -> None:
    writer = await _run(_startup(user="u", database="jaffle") + _terminate())
    msgs = _messages(writer.buffer)
    type_seq = _types(msgs)
    assert type_seq[0] == "R"  # AuthenticationOk
    assert struct.unpack(">i", msgs[0][1])[0] == 0
    assert "S" in type_seq  # ParameterStatus burst
    assert "K" in type_seq  # BackendKeyData
    assert _ready_statuses(msgs)[0] == proto.TX_IDLE


async def test_token_correct_password_succeeds() -> None:
    inp = _startup(user="u", database="jaffle") + _password("s3cret") + _terminate()
    writer = await _run(inp, token="s3cret")
    msgs = _messages(writer.buffer)
    # First R is the cleartext-password request (int32 3), then AuthenticationOk (0).
    auth_msgs = [body for t, body in msgs if t == "R"]
    assert struct.unpack(">i", auth_msgs[0])[0] == 3
    assert struct.unpack(">i", auth_msgs[1])[0] == 0


async def test_token_wrong_password_errors() -> None:
    inp = _startup(user="u", database="jaffle") + _password("wrong") + _terminate()
    writer = await _run(inp, token="s3cret")
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_INVALID_PASSWORD


async def test_token_empty_password_errors() -> None:
    inp = _startup(user="u", database="jaffle") + _password("") + _terminate()
    writer = await _run(inp, token="s3cret")
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_INVALID_PASSWORD


async def test_unknown_database_errors_3d000() -> None:
    writer = await _run(_startup(user="u", database="nope") + _terminate())
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_UNDEFINED_DATABASE


async def test_missing_database_errors_3d000() -> None:
    writer = await _run(_startup(user="u") + _terminate())
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_UNDEFINED_DATABASE


# --- simple query ------------------------------------------------------------


async def test_simple_select_one_returns_probe_row() -> None:
    writer = await _run(_startup(user="u", database="jaffle") + _query("SELECT 1") + _terminate())
    msgs = _messages(writer.buffer)
    type_seq = _types(msgs)
    assert "T" in type_seq  # RowDescription
    assert "D" in type_seq  # DataRow
    assert any(t == "C" and b.startswith(b"SELECT 1") for t, b in msgs)


async def test_multi_statement_begin_select_commit_tx_cycle() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _query("BEGIN; SELECT 1; COMMIT;")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    # Per-statement CommandComplete tags.
    tags = [b for t, b in msgs if t == "C"]
    assert any(b.startswith(b"BEGIN") for b in tags)
    assert any(b.startswith(b"SELECT 1") for b in tags)
    assert any(b.startswith(b"COMMIT") for b in tags)
    # EXACTLY one ReadyForQuery for the whole multi-statement Q message
    # (plus the one from startup), back to idle after COMMIT.
    statuses = _ready_statuses(msgs)
    assert len(statuses) == 2  # startup + one for the simple-query message
    assert statuses[-1] == proto.TX_IDLE


async def test_error_in_transaction_then_blocked_until_end() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _query("BEGIN")
        + _query("INSERT INTO orders VALUES (1)")  # read-only → error, tx → E
        + _query("SELECT 1")  # blocked: 25P02
        + _query("ROLLBACK")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    statuses = _ready_statuses(msgs)
    # startup(I), BEGIN→T, failed INSERT→E, blocked SELECT→E, ROLLBACK→I.
    assert statuses == [
        proto.TX_IDLE, proto.TX_IN_TRANSACTION, proto.TX_FAILED,
        proto.TX_FAILED, proto.TX_IDLE,
    ]
    sqlstates = [_error_sqlstate(b) for t, b in msgs if t == "E"]
    assert proto.SQLSTATE_IN_FAILED_SQL_TRANSACTION in sqlstates
    # The failed INSERT and the blocked SELECT produce no DataRow.
    assert "D" not in _types(msgs)


async def test_empty_query_returns_empty_query_response() -> None:
    writer = await _run(_startup(user="u", database="jaffle") + _query("") + _terminate())
    msgs = _messages(writer.buffer)
    assert "I" in _types(msgs)  # EmptyQueryResponse


# --- extended query ----------------------------------------------------------


async def test_extended_select_one_flow() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT 1")
        + _describe("S", "")
        + _bind("", "")
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    type_seq = _types(_messages(writer.buffer))
    assert "1" in type_seq  # ParseComplete
    assert "t" in type_seq  # ParameterDescription
    assert "T" in type_seq  # RowDescription (from Describe)
    assert "2" in type_seq  # BindComplete
    assert "D" in type_seq  # DataRow (from Execute)
    assert "Z" in type_seq  # ReadyForQuery (from Sync)


async def test_extended_query_with_bound_param_substitutes() -> None:
    # `SELECT $1` → the bound literal becomes the projection. The probe path
    # won't match, but INFORMATION_SCHEMA filtering with a param is the real
    # use; here we assert the bind succeeds and a row is produced.
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE catalog_name = $1")
        + _bind("", "", values=(b"slayer",), result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    type_seq = _types(_messages(writer.buffer))
    assert "2" in type_seq  # BindComplete (substitution succeeded)
    assert "D" in type_seq  # DataRow produced
    assert "E" not in type_seq


async def test_extended_binary_result_format_encodes_binary() -> None:
    engine = _FakeEngine([{"orders.revenue_sum": 100.0}])
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT revenue_sum FROM orders")
        + _describe("S", "")
        + _bind("", "", result_formats=(proto.FORMAT_BINARY,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp, engine=engine)
    msgs = _messages(writer.buffer)
    data_rows = [b for t, b in msgs if t == "D"]
    assert len(data_rows) == 1
    # One column, binary float8 → int16 count + int32 len(8) + 8 IEEE bytes.
    body = data_rows[0]
    count = struct.unpack_from(">h", body, 0)[0]
    assert count == 1
    length = struct.unpack_from(">i", body, 2)[0]
    assert length == 8
    value = struct.unpack_from(">d", body, 6)[0]
    assert value == 100.0  # NOSONAR(S1244) — exact binary roundtrip of a representable value


async def test_extended_text_result_format_encodes_text() -> None:
    engine = _FakeEngine([{"orders.revenue_sum": 100.0}])
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT revenue_sum FROM orders")
        + _describe("S", "")
        + _bind("", "", result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp, engine=engine)
    msgs = _messages(writer.buffer)
    body = next(b for t, b in msgs if t == "D")
    length = struct.unpack_from(">i", body, 2)[0]
    assert body[6:6 + length] == b"100.0"


async def test_describe_unknown_statement_errors() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _describe("S", "ghost")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_INTERNAL_ERROR


async def test_execute_unknown_portal_errors() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _execute("ghost")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_INTERNAL_ERROR


async def test_close_statement_then_complete() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("st", "SELECT 1")
        + _close("S", "st")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    assert "3" in _types(_messages(writer.buffer))  # CloseComplete


async def test_malformed_message_body_is_protocol_violation_not_crash() -> None:
    # A truncated Parse body must yield a protocol-violation error, not tear
    # down the session (the subsequent Sync still gets a ReadyForQuery).
    inp = (
        _startup(user="u", database="jaffle")
        + _frame(b"P", b"\xff\xff")  # garbage Parse body (no null terminators)
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_PROTOCOL_VIOLATION
    assert "Z" in _types(msgs)  # session survived to ReadyForQuery


async def test_invalid_bind_result_format_code_rejected() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT 1")
        + _bind("", "", result_formats=(7,))  # 7 is neither text(0) nor binary(1)
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_FEATURE_NOT_SUPPORTED


async def test_bind_parameter_count_mismatch_errors() -> None:
    # Statement has one placeholder but Bind supplies zero values.
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT revenue_sum FROM orders WHERE id = $1", oids=(proto.OID_INT8,))
        + _bind("", "")  # no values
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_FEATURE_NOT_SUPPORTED


async def test_extended_execute_blocked_in_failed_transaction() -> None:
    # After an error inside BEGIN, an extended-protocol SELECT must be blocked
    # with 25P02 until ROLLBACK — not executed.
    inp = (
        _startup(user="u", database="jaffle")
        + _query("BEGIN")
        + _query("INSERT INTO orders VALUES (1)")  # fails → tx state E
        + _parse("", "SELECT 1")
        + _bind("", "")
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    sqlstates = [_error_sqlstate(b) for t, b in msgs if t == "E"]
    assert proto.SQLSTATE_IN_FAILED_SQL_TRANSACTION in sqlstates


@pytest.mark.parametrize("msg_type", [b"F", b"d", b"c", b"f"])
async def test_unsupported_message_type_errors_0a000(msg_type: bytes) -> None:
    # FunctionCall / CopyData / CopyDone / CopyFail are not supported.
    inp = _startup(user="u", database="jaffle") + _frame(msg_type, b"") + _terminate()
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_FEATURE_NOT_SUPPORTED


# --- startup edge cases ------------------------------------------------------


async def test_gssenc_request_gets_N() -> None:
    writer = await _run(_gssenc_request() + _startup(user="u", database="jaffle") + _terminate())
    assert writer.buffer[0:1] == b"N"


async def test_cancel_request_closes_without_startup() -> None:
    writer = await _run(_cancel_request())
    # Stateless server — a cancel request just closes; nothing meaningful sent.
    assert _messages(writer.buffer) == []


# --- binary wire format (asyncpg-critical) -----------------------------------


async def _binary_value_bytes(sql: str, engine_data) -> bytes:
    """Run an extended binary-format query returning one column; return the
    single DataRow's value bytes."""
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _describe("S", "")
        + _bind("", "", result_formats=(proto.FORMAT_BINARY,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp, engine=_FakeEngine(engine_data))
    body = next(b for t, b in _messages(writer.buffer) if t == "D")
    length = struct.unpack_from(">i", body, 2)[0]
    return body[6:6 + length]


async def test_binary_int8_wire() -> None:
    raw = await _binary_value_bytes("SELECT row_count FROM orders", [{"orders.row_count": 42}])
    assert raw == struct.pack(">q", 42)


async def test_binary_date_wire() -> None:
    import datetime as dt

    raw = await _binary_value_bytes(
        "SELECT order_date FROM orders", [{"orders.order_date": dt.date(2000, 1, 2)}],
    )
    assert raw == struct.pack(">i", 1)  # 1 day after the 2000-01-01 epoch


async def test_binary_timestamp_wire() -> None:
    import datetime as dt

    raw = await _binary_value_bytes(
        "SELECT ordered_at FROM orders",
        [{"orders.ordered_at": dt.datetime(2000, 1, 1, 0, 0, 1)}],
    )
    assert raw == struct.pack(">q", 1_000_000)  # 1 second = 1e6 micros after epoch


# --- parameter inference + substitution --------------------------------------


async def test_parameter_description_infers_count_from_placeholders() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT row_count FROM orders WHERE status = $1 AND status = $2")
        + _describe("S", "")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    desc = next(b for t, b in _messages(writer.buffer) if t == "t")
    count = struct.unpack_from(">h", desc, 0)[0]
    assert count == 2
    oids = [struct.unpack_from(">i", desc, 2 + 4 * i)[0] for i in range(count)]
    assert oids == [proto.OID_TEXT, proto.OID_TEXT]


async def test_text_param_substituted_into_filter() -> None:
    engine = _CapturingEngine([])
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT revenue_sum FROM orders WHERE status = $1")
        + _bind("", "", values=(b"completed",), result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    await _run(inp, engine=engine)
    assert engine.last_query is not None
    assert engine.last_query.filters == ["status = 'completed'"]


async def test_binary_int_param_substituted_into_filter() -> None:
    engine = _CapturingEngine([])
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT revenue_sum FROM orders WHERE id = $1", oids=(proto.OID_INT8,))
        + _bind(
            "", "",
            values=(struct.pack(">q", 5),),
            param_formats=(proto.FORMAT_BINARY,),
            result_formats=(proto.FORMAT_TEXT,),
        )
        + _execute("")
        + _sync()
        + _terminate()
    )
    await _run(inp, engine=engine)
    assert engine.last_query is not None
    assert engine.last_query.filters == ["id = 5"]


async def test_string_param_is_quote_escaped() -> None:
    engine = _CapturingEngine([])
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT revenue_sum FROM orders WHERE status = $1")
        + _bind("", "", values=(b"O'Brien",), result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    await _run(inp, engine=engine)
    assert engine.last_query.filters == ["status = 'O''Brien'"]


# --- portal close / flush / max_rows -----------------------------------------


async def test_close_portal_completes() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("st", "SELECT 1")
        + _bind("po", "st")
        + _close("P", "po")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    assert "3" in _types(_messages(writer.buffer))  # CloseComplete


async def test_flush_does_not_break_session() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _frame(b"H", b"")  # Flush
        + _query("SELECT 1")
        + _terminate()
    )
    writer = await _run(inp)
    type_seq = _types(_messages(writer.buffer))
    assert "D" in type_seq  # the subsequent simple query still works


async def test_execute_with_max_rows_returns_all_rows_no_suspend() -> None:
    engine = _FakeEngine([{"orders.revenue_sum": 1.0}, {"orders.revenue_sum": 2.0}])
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT revenue_sum FROM orders")
        + _describe("S", "")
        + _bind("", "", result_formats=(proto.FORMAT_TEXT,))
        + _execute("", max_rows=1)  # cap requested
        + _sync()
        + _terminate()
    )
    writer = await _run(inp, engine=engine)
    type_seq = _types(_messages(writer.buffer))
    # All rows returned despite max_rows=1; no PortalSuspended ('s').
    assert type_seq.count("D") == 2
    assert "s" not in type_seq
