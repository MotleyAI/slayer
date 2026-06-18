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

    async def drain(self) -> None:  # NOSONAR(S7503) — async to satisfy the awaited interface
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:  # NOSONAR(S7503) — async to satisfy the awaited interface
        return None


class _FakeStorage:
    def __init__(self, models_by_ds) -> None:
        self._models_by_ds = models_by_ds

    async def list_datasources(self) -> List[str]:  # NOSONAR(S7503) — async to satisfy the awaited interface
        return list(self._models_by_ds)

    async def list_models(self, *, data_source: str) -> List[str]:  # NOSONAR(S7503) — async to satisfy the awaited interface
        return [m.name for m in self._models_by_ds.get(data_source, [])]

    async def get_model(self, *, name: str, data_source: str):  # NOSONAR(S7503) — async to satisfy the awaited interface
        for m in self._models_by_ds.get(data_source, []):
            if m.name == name:
                return m
        return None


class _FakeEngine:
    def __init__(self, data) -> None:
        self.data = data

    async def execute(self, *, query=None, data_source=None):  # NOSONAR(S7503) — async to satisfy the awaited interface
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

    async def execute(self, *, query=None, data_source=None):  # NOSONAR(S7503) — async to satisfy the awaited interface
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


async def test_ssl_request_without_tls_gets_n() -> None:
    writer = await _run(_ssl_request() + _startup(user="u", database="jaffle") + _terminate())
    assert writer.buffer[0:1] == b"N"


async def test_ssl_request_with_tls_gets_s(monkeypatch) -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(_ssl_request() + _startup(user="u", database="jaffle") + _terminate())
    reader.feed_eof()
    writer = _FakeWriter()
    conn = PgConnection(
        reader, writer, engine=_FakeEngine([]), storage=_storage(),
        token=None, tls_ctx=object(),
    )
    upgraded = []

    async def _fake_upgrade() -> None:  # NOSONAR(S7503) — async to satisfy the awaited interface
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
    # DEV-1558: `catalog_name` in INFORMATION_SCHEMA rows is the
    # connection's datasource (jaffle here), not the static `slayer`
    # name — Postgres-compatible semantics for `current_database()`.
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE catalog_name = $1")
        + _bind("", "", values=(b"jaffle",), result_formats=(proto.FORMAT_TEXT,))
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


async def test_extended_error_skips_until_sync() -> None:
    # An error on an Execute must put the connection in skip-until-Sync mode:
    # a second Execute before Sync is discarded (no second error), and Sync
    # resynchronises with exactly one ReadyForQuery.
    inp = (
        _startup(user="u", database="jaffle")
        + _execute("ghost")        # unknown portal → error, enter skip mode
        + _execute("ghost2")       # discarded (no error emitted)
        + _sync()                  # resync → ReadyForQuery
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    errors = [b for t, b in msgs if t == "E"]
    assert len(errors) == 1  # only the first Execute errored; the second was skipped
    statuses = _ready_statuses(msgs)
    assert statuses[-1] == proto.TX_IDLE  # Sync emitted ReadyForQuery


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


async def test_gssenc_request_gets_n() -> None:
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


# --- DEV-1558: catalog SQL via the DuckDB executor over the extended protocol


def _parse_row_description(body: bytes):
    """Decode a RowDescription frame body into a list of (name, oid, format_code)."""
    out = []
    count = struct.unpack_from(">h", body, 0)[0]
    i = 2
    for _ in range(count):
        end = body.index(b"\x00", i)
        name = body[i:end].decode("utf-8")
        i = end + 1
        # tableoid(4) + colno(2) + typoid(4) + typsize(2) + typmod(4) + format(2)
        i += 4  # tableoid
        i += 2  # colno
        type_oid = struct.unpack_from(">i", body, i)[0]
        i += 4
        i += 2  # typsize
        i += 4  # typmod
        format_code = struct.unpack_from(">h", body, i)[0]
        i += 2
        out.append((name, type_oid, format_code))
    return out


async def test_extended_protocol_catalog_query_text_format() -> None:
    """A pgjdbc-style getSchemas query over the extended protocol returns
    a RowDescription carrying the quoted aliases (case preserved) and
    text-encoded DataRows, all routed through the DuckDB catalog executor."""
    sql = (
        'SELECT nspname AS "TABLE_SCHEM", current_database() AS "TABLE_CATALOG" '
        "FROM pg_catalog.pg_namespace "
        'ORDER BY "TABLE_SCHEM"'
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _describe("S", "")
        + _bind("", "", result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    type_seq = _types(msgs)
    assert "T" in type_seq  # RowDescription
    assert "D" in type_seq  # at least one DataRow
    assert "E" not in type_seq

    rd = next(body for t, body in msgs if t == "T")
    fields = _parse_row_description(rd)
    names = [n for n, _oid, _fmt in fields]
    assert names == ["TABLE_SCHEM", "TABLE_CATALOG"]
    # All OIDs are within the 6 the facade knows how to encode.
    allowed_oids = {proto.OID_BOOL, proto.OID_INT8, proto.OID_TEXT,
                    proto.OID_FLOAT8, proto.OID_DATE, proto.OID_TIMESTAMP}
    for _name, oid, _fmt in fields:
        assert oid in allowed_oids
    # Format-code is text.
    assert all(fmt == proto.FORMAT_TEXT for _n, _o, fmt in fields)
    # DataRow payload contains the actual text values returned by the executor.
    # `public` (one of the two pg_namespace rows) must appear in some DataRow.
    data_bodies = [body for t, body in msgs if t == "D"]
    payloads = b"".join(data_bodies)
    assert b"public" in payloads
    assert b"jaffle" in payloads  # current_database() resolved to the datasource name


async def test_extended_protocol_catalog_query_binary_format() -> None:
    """Same query, but binary result format. Verifies that the catalog
    executor's results encode via the binary path of _encode_value."""
    sql = (
        'SELECT nspname AS "TABLE_SCHEM" FROM pg_catalog.pg_namespace '
        'ORDER BY "TABLE_SCHEM"'
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _describe("S", "")
        + _bind("", "", result_formats=(proto.FORMAT_BINARY,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    type_seq = _types(msgs)
    assert "T" in type_seq
    assert "D" in type_seq
    assert "E" not in type_seq

    rd = next(body for t, body in msgs if t == "T")
    fields = _parse_row_description(rd)
    assert fields[0][0] == "TABLE_SCHEM"
    # The RowDescription from Describe-Statement runs before Bind, so its
    # format codes are always text (0) per the extended-protocol spec; the
    # bound format codes only apply to DataRow encoding. Verify the
    # DataRow payload uses the binary encoding of `public` (raw 6-byte
    # length prefix + UTF-8 bytes — text-encoded `public` would be the
    # literal ASCII; binary OID_TEXT also emits the raw bytes, but we
    # confirm no error and DataRow exists).
    data_bodies = [body for t, body in msgs if t == "D"]
    assert data_bodies
    # `public` appears in some DataRow payload.
    assert any(b"public" in body for body in data_bodies)


async def test_extended_protocol_catalog_query_with_bound_parameter() -> None:
    """DEV-1558 regression: asyncpg sends Parse + Describe-Statement BEFORE
    Bind. With a $N parameter in the SQL, the catalog executor would
    previously fail on Describe (unsubstituted $N → DuckDB bind error),
    emit NoData, then Execute would emit a populated RowDescription —
    causing asyncpg to raise ``ProtocolError: columns vs described``.

    Fix: ``_describe_sql`` substitutes ``$N → NULL`` for the
    describe-only translation so the executor produces a valid column
    description; ``_handle_bind`` then substitutes real values for
    Execute. The wire sequence:

        Parse(stmt, "... WHERE catalog_name = $1") →
        ParameterDescription + RowDescription →
        Bind(stmt, [b'slayer']) →
        BindComplete →
        Execute → DataRow(s) → CommandComplete →
        Sync → ReadyForQuery
    """
    sql = ('SELECT * FROM information_schema.schemata '
           'WHERE catalog_name = $1')
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _describe("S", "")
        + _bind("", "", values=(b"jaffle",),
                result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    type_seq = _types(msgs)
    # No protocol error.
    assert "E" not in type_seq
    # RowDescription (from Describe) AND DataRow (from Execute) — and they
    # MUST be consistent (catalog query, multi-column result).
    assert "T" in type_seq  # RowDescription
    assert "2" in type_seq  # BindComplete — substitution succeeded
    rd = next(body for t, body in msgs if t == "T")
    rd_fields = _parse_row_description(rd)
    n_cols = len(rd_fields)
    # CR/Codex round 13: require at least one DataRow so the column-count
    # consistency loop is actually exercised. Binding the datasource
    # ("jaffle") matches the row in `_is_schemata` whose `catalog_name`
    # column is set to the datasource (the round-6 fix). A vacuously-
    # passing test (0 rows) would silently hide a protocol regression.
    data_bodies = [b for t, b in msgs if t == "D"]
    assert data_bodies, "Expected at least one DataRow from the catalog query"
    for body in data_bodies:
        n_data = struct.unpack_from(">h", body, 0)[0]
        assert n_data == n_cols, f"DataRow {n_data} cols vs RowDescription {n_cols}"


async def test_describe_int_param_against_int_column_no_conversion_error() -> None:
    """DEV-1558 live-Metabase repro (round 19): when pgjdbc binds a
    parameter against an INT column (e.g. ``WHERE objsubid = $1``) but
    didn't declare the param OID, ``_resolve_param_oids`` defaults to
    ``OID_TEXT``. The round-13 literal sentinel ``''`` then made the
    describe-time SQL ``objsubid = ''`` which DuckDB rejected with
    ``Conversion Error: Could not convert string '' to INT64``.

    Fix: the typed-NULL sentinel
    (``CAST(NULL AS VARCHAR)``) is universally comparable, so the
    describe step succeeds regardless of how the parameter is used
    downstream."""
    sql = "SELECT objoid FROM pg_catalog.pg_description WHERE objsubid = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _describe("S", "")
        + _bind("", "", values=(b"0",),
                result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    type_seq = _types(msgs)
    # Critical: no ErrorResponse during Describe (would surface as 'E').
    assert "E" not in type_seq, (
        "Describe with unannounced $N against an INT column "
        "must not produce an ErrorResponse"
    )
    # The full extended sequence still completes through Sync.
    assert "T" in type_seq  # RowDescription
    assert "2" in type_seq  # BindComplete
    assert "Z" in type_seq  # ReadyForQuery


async def test_simple_query_catalog_union_routes_to_executor() -> None:
    """DEV-1558 round 19: Metabase corpus #12 is a top-level
    ``UNION ALL`` between info-schema and pg_catalog branches. Before
    the fix the translator rejected ``exp.Union`` as ``Unsupported
    statement: Union`` before the catalog-executor branch fired.
    Verify a simple-query round-trip lands DataRows and no error."""
    sql = (
        "SELECT n.nspname FROM pg_catalog.pg_namespace n "
        "WHERE n.nspname = 'public' "
        "UNION ALL "
        "SELECT 'public' AS nspname"
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _query(sql)
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    type_seq = _types(msgs)
    assert "E" not in type_seq, "UNION ALL must route to executor, not error"
    assert "T" in type_seq  # RowDescription
    assert "D" in type_seq  # at least one DataRow


# --- DEV-1570: empty-string-vs-non-text Bind rewrite -----------------------


async def _run_capturing(
    input_bytes: bytes, *, token: Optional[str] = None, storage=None, engine=None,
) -> Tuple[_FakeWriter, PgConnection]:
    """Variant of ``_run`` that returns both the writer and the connection
    so tests can introspect ``conn._portals`` post-Bind."""
    reader = asyncio.StreamReader()
    reader.feed_data(input_bytes)
    reader.feed_eof()
    writer = _FakeWriter()
    conn = PgConnection(
        reader, writer,
        engine=engine or _FakeEngine([]),
        storage=storage or _storage(),
        token=token,
        tls_ctx=None,
    )
    await conn.run()
    return writer, conn


async def test_bind_empty_string_to_int_column_in_pg_catalog_no_error() -> None:
    """DEV-1570: pgjdbc's empty-string-for-null-text convention against an
    INT column previously produced ``WHERE objsubid = ''`` → DuckDB
    ``Conversion Error: Could not convert string '' to INT64`` at Execute.
    The Bind-time rewrite swaps the literal to ``NULL`` so the query runs."""
    sql = "SELECT objoid FROM pg_catalog.pg_description WHERE objsubid = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _describe("S", "")
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    type_seq = _types(msgs)
    assert "E" not in type_seq, (
        "Bind empty-string against INT column must rewrite to NULL — "
        f"got error sequence {type_seq}"
    )
    assert "T" in type_seq  # RowDescription
    assert "2" in type_seq  # BindComplete
    assert "Z" in type_seq  # ReadyForQuery


async def test_bind_empty_string_to_int_column_substitutes_null_literal() -> None:
    """Portal-level introspection: the substituted SQL must contain `NULL`
    (and NOT the literal `''`) at the parameter position."""
    sql = "SELECT objoid FROM pg_catalog.pg_description WHERE objsubid = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "NULL" in portal_sql, f"expected NULL substitution, got: {portal_sql!r}"
    assert "''" not in portal_sql, f"empty-string literal must not survive: {portal_sql!r}"


async def test_bind_empty_string_to_text_column_keeps_empty_literal() -> None:
    """Empty string against TEXT column is a legal predicate; rewrite must
    NOT fire — the `''` literal is preserved."""
    sql = "SELECT relname FROM pg_catalog.pg_class WHERE relname = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "''" in portal_sql, f"text column comparison must keep '': {portal_sql!r}"


async def test_bind_empty_string_to_boolean_column_substitutes_null() -> None:
    sql = "SELECT relname FROM pg_catalog.pg_class WHERE relhasindex = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert "E" not in _types(msgs)


async def test_bind_empty_string_to_date_column_user_model_substitutes_null() -> None:
    """DATE-typed user-model column. Verifies the user-model branch of the
    column-type index resolves under PUBLIC_SCHEMA."""
    sql = "SELECT id FROM orders WHERE order_date = $1"
    engine = _CapturingEngine([])
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp, engine=engine)
    portal_sql = conn._portals[""].sql
    assert "NULL" in portal_sql


async def test_bind_empty_string_to_timestamp_column_user_model_substitutes_null() -> None:
    sql = "SELECT id FROM orders WHERE ordered_at = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "NULL" in portal_sql


async def test_bind_empty_string_binary_format_substitutes_null() -> None:
    """Codex finding #8: text-OID parameter sent in BINARY format (0-byte
    payload) — same rule applies (raw == b'')."""
    sql = "SELECT objoid FROM pg_catalog.pg_description WHERE objsubid = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), param_formats=(proto.FORMAT_BINARY,),
                result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "NULL" in portal_sql, f"binary-format empty bind must rewrite: {portal_sql!r}"


async def test_bind_empty_string_in_between_rewrites_only_empty_bound() -> None:
    """BETWEEN $1 AND $2 with bind (b'', b'5') — $1 → NULL, $2 stays quoted '5'.
    Codex finding #6: $2 must be the QUOTED text literal `'5'`, not bare `5`,
    since the declared OID is text."""
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE objsubid BETWEEN $1 AND $2"
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"", b"5"), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "NULL" in portal_sql
    assert "'5'" in portal_sql, f"expected quoted '5' for $2: {portal_sql!r}"


async def test_bind_empty_string_in_list_rewrites_only_empty_bound() -> None:
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE objsubid IN ($1, $2)"
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"", b"5"), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "NULL" in portal_sql
    assert "'5'" in portal_sql


async def test_bind_empty_string_mixed_use_param_whole_swap() -> None:
    """$1 appears against both INT and TEXT columns. Whole-param swap:
    every occurrence of $1 substitutes to NULL — the text-column branch
    too. The empty-string `''` literal must NOT appear in the portal SQL."""
    sql = (
        "SELECT * FROM pg_catalog.pg_description AS d "
        "INNER JOIN pg_catalog.pg_class AS c ON c.oid = d.objoid "
        "WHERE d.objsubid = $1 OR c.relname = $1"
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    # Both predicate positions take NULL.
    assert portal_sql.count("NULL") >= 2, (
        f"expected NULL in both predicates for whole-param swap: {portal_sql!r}"
    )
    # No surviving '' literal anywhere in the portal SQL.
    assert "''" not in portal_sql, f"whole-param swap missed an occurrence: {portal_sql!r}"


async def test_bind_nonempty_string_against_int_column_preserves_literal() -> None:
    """The rewrite is precise: only empty-string text-OID binds get the
    NULL substitution. A non-empty string against an INT column must hit
    ``literal_for_substitution`` like any other value, so the portal SQL
    carries the literal verbatim (``'abc'``) rather than ``NULL``.

    We assert on the portal SQL rather than on DuckDB's downstream
    behaviour — DuckDB's per-version coercion of ``'abc'`` against an INT
    column varies between wheels (Python 3.11's DuckDB accepts it
    silently; 3.12's rejects with a conversion error) and is out of
    scope for this PR. The SLayer invariant under test is the rewrite's
    precision, not DuckDB's strictness."""
    sql = "SELECT objoid FROM pg_catalog.pg_description WHERE objsubid = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"abc",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "'abc'" in portal_sql, (
        f"non-empty string must reach the portal as a quoted literal: {portal_sql!r}"
    )
    assert "NULL" not in portal_sql, (
        f"non-empty string must NOT be rewritten to NULL: {portal_sql!r}"
    )


async def test_bind_empty_string_declared_oid_int8_is_bind_error() -> None:
    """When the client explicitly declares OID_INT8 in Parse and binds
    `b""`, the existing `value_from_text` path raises ValueError → bind
    error. The DEV-1570 rewrite is gated on OID_TEXT only."""
    sql = "SELECT objoid FROM pg_catalog.pg_description WHERE objsubid = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql, oids=(proto.OID_INT8,))
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    type_seq = _types(msgs)
    # The existing bind-error path fires.
    assert "E" in type_seq


async def test_bind_empty_string_with_cast_wrapped_column_falls_through() -> None:
    """Codex finding #3 / spec scope limit: expression-wrapped column refs
    (CAST(...), arithmetic, function calls) are out of scope. The
    rewrite does NOT fire; `''` survives in the portal SQL and the
    DuckDB conversion error surfaces at Execute."""
    sql = (
        "SELECT objoid FROM pg_catalog.pg_description "
        "WHERE CAST(objsubid AS BIGINT) = $1"
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "''" in portal_sql, (
        "expression-wrapped column ref is out of scope; '' literal must "
        f"survive in: {portal_sql!r}"
    )


async def test_bind_empty_string_in_subquery_predicate_rewrites() -> None:
    sql = (
        "SELECT * FROM pg_catalog.pg_class "
        "WHERE oid IN ("
        "  SELECT objoid FROM pg_catalog.pg_description WHERE objsubid = $1"
        ")"
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "NULL" in portal_sql, f"subquery predicate must rewrite: {portal_sql!r}"


async def test_bind_empty_string_via_cte_alias_falls_through() -> None:
    """Codex finding #3: CTE-derived column refs lose physical-column
    lineage. The classifier returns empty set; the `''` literal survives."""
    sql = (
        "WITH d AS (SELECT objsubid AS x FROM pg_catalog.pg_description) "
        "SELECT * FROM d WHERE x = $1"
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "''" in portal_sql, (
        f"CTE-aliased column ref is out of scope; '' must survive: {portal_sql!r}"
    )


async def test_bind_empty_string_user_model_int_column() -> None:
    """User-model branch via PUBLIC_SCHEMA — `orders.id` is INT primary key."""
    sql = "SELECT id FROM orders WHERE id = $1"
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("", sql)
        + _bind("", "", values=(b"",), result_formats=(proto.FORMAT_TEXT,))
        + _sync()
        + _terminate()
    )
    _writer, conn = await _run_capturing(inp)
    portal_sql = conn._portals[""].sql
    assert "NULL" in portal_sql, f"user-model INT column must rewrite: {portal_sql!r}"
    assert "''" not in portal_sql
