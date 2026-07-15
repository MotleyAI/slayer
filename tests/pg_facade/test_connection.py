"""Tests for slayer.pg_facade.connection — the PgConnection state machine.

Driven over an in-memory asyncio stream pair with a fake storage + engine.
"""

from __future__ import annotations

import asyncio
import struct
import types

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.pg_facade import protocol as proto
from slayer.pg_facade.connection import PgConnection
from slayer.pg_facade.probes import SESSION_SETTING_SEED


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
    def __init__(self, models_by_ds, *, schema_by_ds=None, priority=None) -> None:
        self._models_by_ds = models_by_ds
        self._schema_by_ds = schema_by_ds or {}
        self._priority = priority or []

    async def list_datasources(self) -> list[str]:  # NOSONAR(S7503) — async to satisfy the awaited interface
        return list(self._models_by_ds)

    async def list_models(self, *, data_source: str) -> list[str]:  # NOSONAR(S7503) — async to satisfy the awaited interface
        return [m.name for m in self._models_by_ds.get(data_source, [])]

    async def get_model(self, *, name: str, data_source: str):  # NOSONAR(S7503) — async to satisfy the awaited interface
        for m in self._models_by_ds.get(data_source, []):
            if m.name == name:
                return m
        return None

    async def get_datasource(self, datasource: str):  # NOSONAR(S7503) — async to satisfy the awaited interface
        schema = self._schema_by_ds.get(datasource)
        if schema is None:
            return None
        return DatasourceConfig(name=datasource, postgres_schema=schema)

    async def get_datasource_priority(self) -> list[str]:  # NOSONAR(S7503) — async to satisfy the awaited interface
        return list(self._priority)


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
        self.last_data_source = None

    async def execute(self, *, query=None, data_source=None):  # NOSONAR(S7503) — async to satisfy the awaited interface
        self.last_query = query
        self.last_data_source = data_source
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


def _parse(name: str, sql: str, oids: tuple[int, ...] = ()) -> bytes:
    body = name.encode() + b"\x00" + sql.encode() + b"\x00" + struct.pack(">h", len(oids))
    for o in oids:
        body += struct.pack(">i", o)
    return _frame(b"P", body)


def _bind(
    portal: str, stmt: str, *,
    values: tuple[bytes | None, ...] = (),
    param_formats: tuple[int, ...] = (),
    result_formats: tuple[int, ...] = (),
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
    input_bytes: bytes, *, token: str | None = None, storage=None, engine=None,
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


def _messages(buf: bytes, *, leading_raw: int = 0) -> list[tuple[str, bytes]]:
    return proto.split_messages(bytes(buf[leading_raw:]))


def _types(msgs: list[tuple[str, bytes]]) -> list[str]:
    return [t for t, _ in msgs]


def _ready_statuses(msgs: list[tuple[str, bytes]]) -> list[bytes]:
    return [body for t, body in msgs if t == "Z"]


def _error_field(body: bytes, ftype_wanted: bytes) -> str | None:
    i = 0
    while i < len(body) and body[i:i + 1] != b"\x00":
        ftype = body[i:i + 1]
        i += 1
        end = body.index(b"\x00", i)
        val = body[i:end].decode("utf-8")
        i = end + 1
        if ftype == ftype_wanted:
            return val
    return None


def _error_sqlstate(body: bytes) -> str | None:
    return _error_field(body, b"C")


def _error_message(body: bytes) -> str | None:
    return _error_field(body, b"M")


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


async def test_any_database_name_accepted_as_logical_db() -> None:
    # DEV-1594: the database parameter is a logical DB name, not a datasource
    # selector — an arbitrary name connects successfully (no 3D000).
    writer = await _run(_startup(user="u", database="nope") + _query("SELECT 1") + _terminate())
    msgs = _messages(writer.buffer)
    assert not any(t == "E" for t, _ in msgs)
    assert any(t == "Z" for t, _ in msgs)  # ReadyForQuery — startup completed


async def test_missing_database_defaults_and_connects() -> None:
    writer = await _run(_startup(user="u") + _query("SELECT 1") + _terminate())
    msgs = _messages(writer.buffer)
    assert not any(t == "E" for t, _ in msgs)
    assert any(t == "Z" for t, _ in msgs)


async def test_current_database_reflects_logical_name() -> None:
    writer = await _run(
        _startup(user="u", database="acme") + _query("SELECT current_database()") + _terminate()
    )
    body = next(b for t, b in _messages(writer.buffer) if t == "D")
    length = struct.unpack_from(">i", body, 2)[0]
    assert body[6:6 + length] == b"acme"


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


async def test_begin_read_only_tx_cycle() -> None:
    # Metabase wraps reads in BEGIN READ ONLY; sqlglot can't parse that, so the
    # facade recognises it pre-parse (DEV-1594). Whole cycle must succeed.
    inp = (
        _startup(user="u", database="jaffle")
        + _query("BEGIN READ ONLY; SELECT 1; COMMIT;")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert not any(t == "E" for t, _ in msgs)  # no error
    tags = [b for t, b in msgs if t == "C"]
    assert any(b.startswith(b"BEGIN") for b in tags)
    assert any(b.startswith(b"SELECT 1") for b in tags)
    assert any(b.startswith(b"COMMIT") for b in tags)
    assert _ready_statuses(msgs)[-1] == proto.TX_IDLE


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


class _RaisingEngine:
    """Engine double that raises a SQLAlchemy-shaped error: a wrapper whose
    ``.orig`` carries the driver's ``sqlstate`` + bare message (mirrors how
    asyncpg/psycopg errors reach the facade via SQLAlchemy's DBAPIError)."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def execute(self, *, query=None, data_source=None):  # NOSONAR(S7503)
        raise self._exc


class _DriverError(Exception):
    """asyncpg/psycopg-shaped driver error carrying a SQLSTATE + message."""

    def __init__(self, *, sqlstate=None, pgcode=None, message="") -> None:
        super().__init__(message)
        if sqlstate is not None:
            self.sqlstate = sqlstate
        if pgcode is not None:
            self.pgcode = pgcode
        self.message = message

    def __str__(self) -> str:
        return self.message


def _dbapi_error(*, sqlstate: str, message: str) -> BaseException:
    """A DBAPIError-like wrapper (``.orig`` holds the real driver error),
    mirroring how SQLAlchemy nests the driver exception."""
    wrapper = RuntimeError("(sqlalchemy wrapper) [SQL: SELECT ...]")
    wrapper.orig = _DriverError(sqlstate=sqlstate, message=message)  # type: ignore[attr-defined]
    return wrapper


async def test_run_query_surfaces_driver_sqlstate_and_message() -> None:
    """The ``_run_query`` error path must pass the driver's SQLSTATE +
    bare server message straight through (via ``_engine_error_fields``),
    NOT the generic XX000 + SQLAlchemy repr. Covers e.g. a client seeing
    ``permission denied for table Item`` (42501)."""
    engine = _RaisingEngine(
        _dbapi_error(sqlstate="42501", message="permission denied for table Item")
    )
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SELECT revenue_sum FROM orders")
        + _terminate()
    )
    writer = await _run(inp, engine=engine)
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == "42501"
    assert _error_message(err) == "permission denied for table Item"


async def test_run_query_falls_back_to_internal_error_without_sqlstate() -> None:
    """A plain exception with no driver SQLSTATE in its chain falls back to
    XX000 + ``str(exc)`` — the ``_engine_error_fields`` default branch."""
    engine = _RaisingEngine(ValueError("boom"))
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SELECT revenue_sum FROM orders")
        + _terminate()
    )
    writer = await _run(inp, engine=engine)
    msgs = _messages(writer.buffer)
    err = next(body for t, body in msgs if t == "E")
    assert _error_sqlstate(err) == proto.SQLSTATE_INTERNAL_ERROR
    assert _error_message(err) == "boom"


def test_engine_error_fields_walks_exception_chain() -> None:
    """Unit: ``_engine_error_fields`` finds a driver SQLSTATE through
    ``.orig`` / ``__cause__`` / ``__context__`` and returns its message;
    accepts both ``sqlstate`` and ``pgcode`` attributes; is cycle-safe."""
    from slayer.pg_facade.connection import _engine_error_fields

    # via .orig
    assert _engine_error_fields(
        _dbapi_error(sqlstate="42501", message="denied")
    ) == ("42501", "denied")

    # via __cause__ chain, using pgcode (psycopg spelling)
    outer = RuntimeError("wrapper")
    outer.__cause__ = _DriverError(pgcode="23505", message="duplicate key")
    code, msg = _engine_error_fields(outer)
    assert code == "23505"
    assert msg == "duplicate key"

    # No SQLSTATE anywhere → XX000 + str(exc)
    assert _engine_error_fields(ValueError("plain")) == (
        proto.SQLSTATE_INTERNAL_ERROR, "plain",
    )

    # Cyclic chain must terminate (seen-set guard), not hang.
    a = RuntimeError("a")
    b = RuntimeError("b")
    a.__context__ = b
    b.__context__ = a
    assert _engine_error_fields(a) == (proto.SQLSTATE_INTERNAL_ERROR, "a")


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


# --- DEV-1569: per-connection SET state ---


def _show_value(msgs: list[tuple[str, bytes]]) -> str:
    """Pull the single-text-column value out of the last DataRow in `msgs`.

    The format is: ``count(i16) | length(i32) | bytes``. SHOW always returns
    one column of TEXT.
    """
    body = next(b for t, b in reversed(msgs) if t == "D")
    length = struct.unpack_from(">i", body, 2)[0]
    return body[6:6 + length].decode()


def _parameter_status(msgs: list[tuple[str, bytes]]) -> list[tuple[str, str]]:
    """Decode every ParameterStatus message (``S``) in the stream into
    ``[(name, value), …]``. Note that startup ParameterStatus burst messages
    are included too — callers filter by name."""
    out: list[tuple[str, str]] = []
    for t, body in msgs:
        if t != "S":
            continue
        # Two C-strings: name\x00 value\x00
        parts = body.split(b"\x00")
        if len(parts) >= 2:
            out.append((parts[0].decode(), parts[1].decode()))
    return out


def _command_complete_tags(msgs: list[tuple[str, bytes]]) -> list[str]:
    out: list[str] = []
    for t, body in msgs:
        if t != "C":
            continue
        # Body is the tag as a C-string.
        out.append(body.rstrip(b"\x00").decode())
    return out


async def test_set_show_round_trip_returns_set_value() -> None:
    """A single connection: SET app_name='foo' then SHOW app_name returns 'foo'.

    Pins the core fix: the per-connection map must persist the SET value.
    """
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'foo'")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "foo"


async def test_set_to_spelling_round_trip() -> None:
    """`SET name TO 'value'` (Postgres docs spelling)."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name TO 'bar'")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "bar"


async def test_set_unquoted_var_round_trip() -> None:
    """`SET client_encoding TO UTF8` (unquoted rhs — pgjdbc default)."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET client_encoding TO UTF16")
        + _query("SHOW client_encoding")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "UTF16"


async def test_show_returns_seeded_default_on_fresh_connection() -> None:
    """Without any SET, SHOW returns the seeded default (search_path here)."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SHOW search_path")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == '"$user", public'


async def test_show_unknown_setting_returns_empty_string() -> None:
    """Cut: unknown settings return '' (current behavior preserved)."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SHOW some_made_up_thing")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == ""


async def test_set_followed_by_show_in_one_simple_query_burst() -> None:
    """A single Q message containing `SET ...; SHOW ...;` — the second
    statement must see the first's write."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'one-q'; SHOW application_name;")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "one-q"


async def test_set_then_set_overwrites() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'first'")
        + _query("SET application_name = 'second'")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "second"


async def test_set_then_current_setting_through_connection() -> None:
    """current_setting() consults the per-connection map, not the seed."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'metabase'")
        + _query("SELECT current_setting('application_name')")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "metabase"


async def test_two_connections_have_isolated_session_settings() -> None:
    """In-process xfail-flip target: two PgConnections set distinct values,
    each reads its own back. The integration test pins the cross-asyncio
    version of this."""
    inp_a = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'conn-A'")
        + _query("SHOW application_name")
        + _terminate()
    )
    inp_b = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'conn-B'")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer_a, writer_b = await asyncio.gather(_run(inp_a), _run(inp_b))
    assert _show_value(_messages(writer_a.buffer)) == "conn-A"
    assert _show_value(_messages(writer_b.buffer)) == "conn-B"


async def test_two_connections_isolated_for_current_setting() -> None:
    inp_a = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'cs-A'")
        + _query("SELECT current_setting('application_name')")
        + _terminate()
    )
    inp_b = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'cs-B'")
        + _query("SELECT current_setting('application_name')")
        + _terminate()
    )
    writer_a, writer_b = await asyncio.gather(_run(inp_a), _run(inp_b))
    assert _show_value(_messages(writer_a.buffer)) == "cs-A"
    assert _show_value(_messages(writer_b.buffer)) == "cs-B"


async def test_reset_named_setting_restores_seed() -> None:
    """RESET <name> for a seeded setting reverts to the seeded value."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET search_path = 'mything'")
        + _query("RESET search_path")
        + _query("SHOW search_path")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == '"$user", public'


async def test_reset_unseeded_setting_clears() -> None:
    """RESET <name> for a custom-non-seeded name clears (SHOW returns '')."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET my_custom = 'x'")
        + _query("RESET my_custom")
        + _query("SHOW my_custom")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == ""


async def test_reset_all_restores_all_seeds() -> None:
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'x'")
        + _query("SET search_path = 'y'")
        + _query("RESET ALL")
        + _query("SHOW application_name")
        + _query("SHOW search_path")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    data_rows = [b for t, b in msgs if t == "D"]
    # Two DataRow messages after RESET ALL.
    assert len(data_rows) >= 2
    # First SHOW (application_name) → seeded "".
    app_body = data_rows[-2]
    app_length = struct.unpack_from(">i", app_body, 2)[0]
    assert app_body[6:6 + app_length].decode() == ""
    # Second SHOW (search_path) → seeded '"$user", public'.
    sp_body = data_rows[-1]
    sp_length = struct.unpack_from(">i", sp_body, 2)[0]
    assert sp_body[6:6 + sp_length].decode() == '"$user", public'


async def test_set_pushes_parameter_status_for_application_name() -> None:
    """After SET of a GUC_REPORT setting, a ParameterStatus message must be
    pushed to the client. Real-Postgres behaviour; asyncpg / pgjdbc latch
    onto this for connection identity."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'datadog-tagger'")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    pushes = _parameter_status(msgs)
    # Filter out startup-burst ParameterStatus messages (which DON'T include
    # `application_name` since the seed is empty).
    post_set = [(n, v) for (n, v) in pushes if n == "application_name"]
    assert post_set == [("application_name", "datadog-tagger")]


async def test_set_does_not_push_parameter_status_for_non_reportable() -> None:
    """`work_mem` is NOT in the GUC_REPORT class — no ParameterStatus push."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET work_mem = '64MB'")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    pushes = _parameter_status(msgs)
    post_set = [(n, v) for (n, v) in pushes if n == "work_mem"]
    assert post_set == []


async def test_set_datestyle_pushes_canonical_wire_case() -> None:
    """`SET datestyle = ...` pushes ParameterStatus with the canonical
    Postgres wire-case name `DateStyle` (not the lowercased internal key)."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET datestyle = 'ISO, DMY'")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    pushes = _parameter_status(msgs)
    # The wire name must be the canonical `DateStyle`, value the just-SET value.
    # Filter out the startup burst (which already includes DateStyle=ISO, MDY).
    post = [(n, v) for (n, v) in pushes if n == "DateStyle"]
    # Two: the startup burst + the post-SET push.
    assert post[-1] == ("DateStyle", "ISO, DMY")


async def test_set_timezone_pushes_canonical_wire_case() -> None:
    """`SET timezone = ...` pushes ParameterStatus with the canonical
    Postgres wire-case name `TimeZone`."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET timezone = 'America/New_York'")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    pushes = _parameter_status(msgs)
    post = [(n, v) for (n, v) in pushes if n == "TimeZone"]
    assert post[-1] == ("TimeZone", "America/New_York")


async def test_command_form_set_time_zone_is_silent_noop() -> None:
    """`SET TIME ZONE 'X'` (Command-form fallback) acknowledges with
    CommandComplete but does NOT capture; subsequent SHOW returns the
    seeded default."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET TIME ZONE 'America/New_York'")
        + _query("SHOW timezone")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    # SET acknowledged.
    assert any(tag.startswith("SET") for tag in _command_complete_tags(msgs))
    # No ParameterStatus push for TimeZone (because Command-form not captured).
    pushes = _parameter_status(msgs)
    post = [(n, v) for (n, v) in pushes if n == "TimeZone"]
    # Only the startup burst — no post-SET push.
    assert len(post) == 1
    # SHOW returns the seeded value (UTC), not 'America/New_York'.
    assert _show_value(msgs) == "UTC"


async def test_set_session_qualifier_round_trip() -> None:
    """`SET SESSION name = value` round-trips identically to bare SET."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET SESSION application_name = 'sess'")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "sess"


async def test_set_local_treated_as_session_scope() -> None:
    """Scope cut: `SET LOCAL` is NOT restored at transaction end. Pins the
    current behavior — DEV-1569 doesn't implement transaction-bound SET."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET LOCAL application_name = 'localval'")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "localval"


async def test_set_then_rollback_does_not_revert() -> None:
    """Scope cut: real Postgres rolls back `SET` inside BEGIN; ROLLBACK; we don't.
    Pins the cut so a future change here is intentional."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("BEGIN")
        + _query("SET application_name = 'in_tx'")
        + _query("ROLLBACK")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "in_tx"


async def test_show_all_returns_empty_for_unknown_setting() -> None:
    """Scope cut: `SHOW ALL` falls into the unknown-setting silent-empty path
    (key 'all' → ''). Documents the current behaviour."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SHOW ALL")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == ""


async def test_session_settings_isolated_after_first_use() -> None:
    """Two fresh connections both seed from SESSION_SETTING_SEED. Setting on
    one doesn't pollute the other's view, even later in the same test."""
    seed_snapshot = dict(SESSION_SETTING_SEED)

    inp_a = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'A'")
        + _terminate()
    )
    inp_b = (
        _startup(user="u", database="jaffle")
        + _query("SHOW application_name")
        + _terminate()
    )
    _, writer_b = await asyncio.gather(_run(inp_a), _run(inp_b))
    # B's SHOW returns seeded "" (NOT 'A').
    assert _show_value(_messages(writer_b.buffer)) == ""
    # Module-level seed unchanged.
    assert SESSION_SETTING_SEED == seed_snapshot


async def test_extended_query_set_round_trip() -> None:
    """SET via the extended-query protocol (Parse/Bind/Execute) must update
    the connection's session settings just like a simple Q SET."""
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("st", "SET application_name = 'extp'")
        + _bind("", "st")
        + _execute("")
        + _sync()
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "extp"


async def test_extended_query_describe_does_not_mutate_session_settings() -> None:
    """The riskiest extended-protocol path: Parse + Describe for a
    `SELECT set_config(...)` triggers a translator pass that could
    in-principle mutate the session_settings map. The spec is: Describe
    is pure; mutation only happens on Execute.
    """
    # Parse + Describe-Statement runs translate() → match_pg_probe; the
    # mutation hint MUST NOT be applied until Execute.
    inp = (
        _startup(user="u", database="jaffle")
        + _parse(
            "st",
            "SELECT set_config('application_name', 'via_set_config', false)",
        )
        + _describe("S", "st")
        + _sync()
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    # After Describe (no Execute), SHOW returns the seeded default "".
    assert _show_value(msgs) == ""


async def test_extended_query_set_config_round_trip() -> None:
    """`SELECT set_config(...)` via Parse/Bind/Execute mutates the connection's
    session settings AND returns the new value."""
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("sc", "SELECT set_config('application_name', 'via_exec', false)")
        + _bind("", "sc")
        + _execute("")
        + _sync()
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "via_exec"


async def test_simple_query_set_config_then_show_through_connection() -> None:
    """`SELECT set_config(...)` in simple-Q mode mutates the per-connection
    map AND pushes ParameterStatus for reportable settings."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SELECT set_config('application_name', 'sq_cfg', false)")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "sq_cfg"
    # ParameterStatus push since application_name is GUC_REPORT-class.
    pushes = _parameter_status(msgs)
    post = [(n, v) for (n, v) in pushes if n == "application_name"]
    assert post == [("application_name", "sq_cfg")]


async def test_two_connections_isolated_under_set_config() -> None:
    """set_config-driven mutation is per-connection."""
    inp_a = (
        _startup(user="u", database="jaffle")
        + _query("SELECT set_config('application_name', 'sc-A', false)")
        + _query("SHOW application_name")
        + _terminate()
    )
    inp_b = (
        _startup(user="u", database="jaffle")
        + _query("SELECT set_config('application_name', 'sc-B', false)")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer_a, writer_b = await asyncio.gather(_run(inp_a), _run(inp_b))
    assert _show_value(_messages(writer_a.buffer)) == "sc-A"
    assert _show_value(_messages(writer_b.buffer)) == "sc-B"


# --- DEV-1569 Codex round 2 follow-ups ---


def _index_of_first(msgs: list[tuple[str, bytes]], pred) -> int:
    for i, (t, body) in enumerate(msgs):
        if pred(t, body):
            return i
    return -1


async def test_set_wire_order_command_complete_then_param_status_then_ready() -> None:
    """Pins the wire order for a simple-Q `SET application_name = 'x'`:

      ... CommandComplete -> ParameterStatus(application_name) -> ReadyForQuery

    (Real Postgres allows ParameterStatus at any time, but we pin the
    specific position so future refactors don't accidentally drop the push
    before ReadyForQuery.)
    """
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'wire'")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    # Find the indices of the post-SET CC, the post-SET ParameterStatus, and
    # the trailing ReadyForQuery for this simple-Q message.
    cc_idx = _index_of_first(
        msgs, lambda t, b: t == "C" and b.startswith(b"SET"),
    )
    ps_idx = _index_of_first(
        msgs, lambda t, b: t == "S" and b.split(b"\x00")[0] == b"application_name"
                                     and b.split(b"\x00")[1] == b"wire",
    )
    z_idx = max(i for i, (t, _b) in enumerate(msgs) if t == "Z")
    assert cc_idx != -1, "missing CommandComplete for SET"
    assert ps_idx != -1, "missing ParameterStatus(application_name=wire)"
    assert cc_idx < ps_idx < z_idx, (
        f"wire order wrong: CC@{cc_idx} PS@{ps_idx} Z@{z_idx}"
    )


async def test_reset_named_pushes_parameter_status_to_seed() -> None:
    """RESET <reportable_name> pushes ParameterStatus(name, seeded_value).

    After SET application_name = 'x' then RESET application_name, the
    second push reports the seeded "" value.
    """
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'x'")
        + _query("RESET application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    pushes = _parameter_status(msgs)
    post = [(n, v) for (n, v) in pushes if n == "application_name"]
    # Expect TWO post-startup pushes: SET → 'x', then RESET → '' (seed).
    assert post == [("application_name", "x"), ("application_name", "")]


async def test_reset_all_pushes_parameter_status_for_each_reportable() -> None:
    """RESET ALL pushes ParameterStatus for every reportable name with its
    seeded value. Drivers latch onto these for cache invalidation."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'x'")
        + _query("SET timezone = 'America/New_York'")
        + _query("RESET ALL")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    # After RESET ALL, look at the messages that follow it.
    reset_idx = _index_of_first(
        msgs, lambda t, b: t == "C" and b.startswith(b"RESET"),
    )
    assert reset_idx != -1, "missing CommandComplete for RESET ALL"
    after_reset = msgs[reset_idx:]
    pushes_after = []
    for t, body in after_reset:
        if t != "S":
            continue
        parts = body.split(b"\x00")
        if len(parts) >= 2:
            pushes_after.append((parts[0].decode(), parts[1].decode()))
    # Among them, both application_name and TimeZone should be reset-pushed
    # back to their seeded values.
    names_after = [n for (n, _v) in pushes_after]
    assert "application_name" in names_after
    assert "TimeZone" in names_after
    # Values: application_name → "", TimeZone → "UTC".
    by_name = dict(pushes_after)
    assert by_name["application_name"] == ""
    assert by_name["TimeZone"] == "UTC"


async def test_extended_query_describe_does_not_apply_set_setting() -> None:
    """Describe-statement for a SET must NOT apply the mutation. The plan
    requires _describe_sql to be pure for both NoOpResult.set_setting and
    ProbeResult.settings_mutation."""
    inp = (
        _startup(user="u", database="jaffle")
        + _parse("st", "SET application_name = 'via_describe'")
        + _describe("S", "st")
        + _sync()
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    # Subsequent SHOW returns the SEED, not 'via_describe' — because Describe
    # alone (no Execute) must not apply.
    assert _show_value(msgs) == ""


async def test_extended_query_execute_set_config_pushes_param_status() -> None:
    """Mirror simple-Q set_config-pushes-ParameterStatus through the
    extended-protocol path (Parse/Bind/Execute)."""
    inp = (
        _startup(user="u", database="jaffle")
        + _parse(
            "sc", "SELECT set_config('application_name', 'extp_cfg', false)",
        )
        + _bind("", "sc")
        + _execute("")
        + _sync()
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    pushes = _parameter_status(msgs)
    post = [(n, v) for (n, v) in pushes if n == "application_name"]
    assert post == [("application_name", "extp_cfg")]


async def test_extended_query_reset_round_trip() -> None:
    """RESET via Parse/Bind/Execute reverts to seed."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET application_name = 'pre_reset'")
        + _parse("rs", "RESET application_name")
        + _bind("", "rs")
        + _execute("")
        + _sync()
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == ""


async def test_set_server_version_silently_accepted() -> None:
    """Scope cut: real Postgres rejects `SET server_version`. We silently
    accept it (the facade is read-only-to-SLayer; `server_version` ends up
    in the per-connection map and SHOW returns the new value). Pins the
    cut."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET server_version = '99.0'")
        + _query("SHOW server_version")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    # No ERROR; CommandComplete for SET emitted.
    assert "E" not in _types(msgs), (
        f"unexpected error: {[ _error_sqlstate(b) for t, b in msgs if t == 'E' ]}"
    )
    # SHOW reflects the mutated value (since server_version IS in the seed).
    assert _show_value(msgs) == "99.0"


async def test_set_search_path_comma_values_round_trip() -> None:
    """`SET search_path = public, extensions` (Command-form, comma-separated
    values — pgjdbc / Metabase default) round-trips through SHOW correctly.
    Codex round 1 finding F1."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET search_path = public, extensions")
        + _query("SHOW search_path")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "public, extensions"


async def test_reset_multi_word_alias_resolves_to_seed_key() -> None:
    """`RESET TIME ZONE` must alias-resolve to the seeded `timezone` key
    before restoring. Codex round 1 finding F2."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET timezone = 'America/New_York'")
        + _query("RESET TIME ZONE")
        + _query("SHOW timezone")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    # After RESET TIME ZONE, SHOW timezone returns the seeded UTC.
    assert _show_value(msgs) == "UTC"


async def test_reset_session_authorization_alias_resolves() -> None:
    """`RESET SESSION AUTHORIZATION` aliases to `session_authorization`."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SET session_authorization = 'someuser'")
        + _query("RESET SESSION AUTHORIZATION")
        + _query("SHOW session_authorization")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "slayer"


async def test_set_config_with_cast_value_mutates_session() -> None:
    """`SELECT set_config('app', $1::text, false)` (asyncpg cast form) must
    still mutate per-connection state. Codex round 1 finding F3."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SELECT set_config('application_name', 'cast_value'::text, false)")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    assert _show_value(msgs) == "cast_value"


async def test_set_config_with_is_local_true_does_not_mutate() -> None:
    """`set_config('app', 'x', true)` must NOT persist — `is_local=true` is
    out of scope for DEV-1569."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query("SELECT set_config('application_name', 'transient', true)")
        + _query("SHOW application_name")
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    # SHOW returns the seeded "" — the transient set_config didn't persist.
    assert _show_value(msgs) == ""


@pytest.mark.parametrize(
    ("set_sql", "expected_name", "expected_value"),
    [
        ("SET client_encoding = 'UTF16'", "client_encoding", "UTF16"),
        ("SET server_encoding = 'UTF16'", "server_encoding", "UTF16"),
        ("SET server_version = '15.0'", "server_version", "15.0"),
        ("SET session_authorization = 'newuser'", "session_authorization", "newuser"),
        ("SET standard_conforming_strings = 'off'", "standard_conforming_strings", "off"),
        ("SET intervalstyle = 'iso_8601'", "IntervalStyle", "iso_8601"),
    ],
)
async def test_set_pushes_parameter_status_for_all_reportable_names(
    set_sql: str, expected_name: str, expected_value: str,
) -> None:
    """Sweep over the rest of the _GUC_REPORT_NAMES mapping (the application_name
    / datestyle / timezone cases live in dedicated tests above)."""
    inp = (
        _startup(user="u", database="jaffle")
        + _query(set_sql)
        + _terminate()
    )
    writer = await _run(inp)
    msgs = _messages(writer.buffer)
    pushes = _parameter_status(msgs)
    post = [(n, v) for (n, v) in pushes if n == expected_name]
    # The startup ParameterStatus burst may have pushed the same name with
    # the seed value; only the LAST push for this name should be the SET.
    assert len(post) >= 1
    assert post[-1] == (expected_name, expected_value)
# --- DEV-1570: empty-string-vs-non-text Bind rewrite -----------------------


async def _run_capturing(
    input_bytes: bytes, *, token: str | None = None, storage=None, engine=None,
) -> tuple[_FakeWriter, PgConnection]:
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


# --- DEV-1594: multi-datasource catalog + per-connection scoping -------------


def _employees_model():
    return SlayerModel(
        name="employees", data_source="hr", sql_table="employees",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


async def _run_conn(input_bytes, *, storage, engine, storage_provider=None,
                    engine_factory=None):
    reader = asyncio.StreamReader()
    reader.feed_data(input_bytes)
    reader.feed_eof()
    writer = _FakeWriter()
    conn = PgConnection(
        reader, writer, engine=engine, storage=storage,
        storage_provider=storage_provider, engine_factory=engine_factory,
    )
    await conn.run()
    return writer, conn


async def test_multi_datasource_query_routes_to_owning_datasource() -> None:
    storage = _FakeStorage({"jaffle": [_orders_model()], "hr": [_employees_model()]})
    engine = _CapturingEngine([])
    inp = _startup(user="u", database="acme") + _query("SELECT id FROM employees") + _terminate()
    writer, _ = await _run_conn(inp, storage=storage, engine=engine)
    assert not any(t == "E" for t, _ in _messages(writer.buffer))
    assert engine.last_data_source == "hr"


async def test_custom_postgres_schema_separates_and_routes() -> None:
    storage = _FakeStorage(
        {"jaffle": [_orders_model()], "hr": [_employees_model()]},
        schema_by_ds={"hr": "people"},
    )
    engine = _CapturingEngine([])
    inp = (
        _startup(user="u", database="acme")
        + _query("SELECT id FROM people.employees")
        + _terminate()
    )
    writer, _ = await _run_conn(inp, storage=storage, engine=engine)
    assert not any(t == "E" for t, _ in _messages(writer.buffer))
    assert engine.last_data_source == "hr"


async def test_storage_provider_scopes_connection_after_auth() -> None:
    # Static storage has only jaffle; the provider swaps in an hr-only store
    # post-auth. Querying employees succeeds only if the provider's store won.
    static_storage = _FakeStorage({"jaffle": [_orders_model()]})
    scoped_storage = _FakeStorage({"hr": [_employees_model()]})
    engine = _CapturingEngine([])

    async def provider(principal):
        return scoped_storage

    inp = _startup(user="u", database="acme") + _query("SELECT id FROM employees") + _terminate()
    writer, conn = await _run_conn(
        inp, storage=static_storage, engine=engine,
        storage_provider=provider, engine_factory=lambda storage: engine,
    )
    assert not any(t == "E" for t, _ in _messages(writer.buffer))
    assert engine.last_data_source == "hr"
    assert conn._storage is scoped_storage


async def test_storage_provider_storage_closed_on_connection_end() -> None:
    scoped_storage = _FakeStorage({"hr": [_employees_model()]})
    closed = {"called": False}

    async def _aclose():
        closed["called"] = True

    scoped_storage.aclose = _aclose  # type: ignore[attr-defined]
    engine = _CapturingEngine([])

    async def provider(principal):
        return scoped_storage

    inp = _startup(user="u", database="acme") + _query("SELECT 1") + _terminate()
    await _run_conn(
        inp, storage=_storage(), engine=engine,
        storage_provider=provider, engine_factory=lambda storage: engine,
    )
    assert closed["called"] is True


async def test_static_storage_not_closed() -> None:
    # No storage_provider → the static storage's aclose (if any) is never called.
    storage = _storage()
    closed = {"called": False}

    async def _aclose():
        closed["called"] = True

    storage.aclose = _aclose  # type: ignore[attr-defined]
    inp = _startup(user="u", database="acme") + _query("SELECT 1") + _terminate()
    await _run_conn(inp, storage=storage, engine=_CapturingEngine([]))
    assert closed["called"] is False


# --- Review #1: scoped engine is also disposed on connection end ------------


async def test_storage_provider_engine_disposed_on_connection_end() -> None:
    """``_close_scoped_storage`` must dispose the engine ``_resolve_scope``
    built — without this, a long-lived facade with ``storage_provider``
    leaks one engine's worth of async SQL-client pools per session."""
    scoped_storage = _FakeStorage({"hr": [_employees_model()]})
    aclosed = {"engine": False, "storage": False}

    async def _storage_aclose():
        aclosed["storage"] = True

    async def _engine_aclose():
        aclosed["engine"] = True

    scoped_storage.aclose = _storage_aclose  # type: ignore[attr-defined]
    engine = _CapturingEngine([])
    engine.aclose = _engine_aclose  # type: ignore[attr-defined]

    async def provider(principal):
        return scoped_storage

    inp = _startup(user="u", database="acme") + _query("SELECT 1") + _terminate()
    await _run_conn(
        inp, storage=_storage(), engine=_CapturingEngine([]),
        storage_provider=provider, engine_factory=lambda storage: engine,
    )
    assert aclosed["engine"] is True, "engine.aclose must run on connection end"
    assert aclosed["storage"] is True, "storage.aclose must run on connection end"


# --- Review #2: static aclose-able storage not touched on early auth failure


async def test_static_storage_aclose_not_called_when_scope_swap_did_not_run() -> None:
    """When ``storage_provider`` is configured but ``_resolve_scope`` never
    runs, the originally-passed static storage's ``aclose`` must NOT be
    called — the host owns its lifecycle. Asserts the ``_owns_scoped_resources``
    gate by constructing the connection directly and checking the flag
    stays False until the swap actually happens."""
    static_storage = _storage()
    static_aclosed = {"called": False}

    async def _storage_aclose():
        static_aclosed["called"] = True

    static_storage.aclose = _storage_aclose  # type: ignore[attr-defined]

    async def provider(principal):
        raise AssertionError("provider must not run before scope-resolve")

    reader = asyncio.StreamReader()
    writer = _FakeWriter()
    conn = PgConnection(
        reader, writer, engine=_CapturingEngine([]), storage=static_storage,
        storage_provider=provider, engine_factory=lambda storage: None,
    )
    # Flag starts False; teardown is a no-op until the swap.
    assert conn._owns_scoped_resources is False
    await conn._close_scoped_storage()
    assert static_aclosed["called"] is False, (
        "static storage's aclose must not be called when scope-swap never ran"
    )


# --- on-demand catalog refresh (TTL) ----------------------------------------


class _RefreshableStorage(_FakeStorage):
    """Fake storage whose content and fingerprint can be mutated mid-test."""

    def __init__(self, models_by_ds, *, fingerprint: str) -> None:
        super().__init__(models_by_ds)
        self.fingerprint = fingerprint

    async def graph_fingerprint(self) -> str:  # NOSONAR(S7503) — async to satisfy the awaited interface
        return self.fingerprint


def _refresh_conn(storage) -> PgConnection:
    # ttl 0.0 => the window is always "elapsed", so every call re-checks the
    # cheap fingerprint (the change-gate is what decides whether to rebuild).
    conn = PgConnection(
        asyncio.StreamReader(), _FakeWriter(),
        engine=_FakeEngine([]), storage=storage,
        catalog_ttl_seconds=0.0,
    )
    conn._storage = storage
    return conn


async def _seed_catalog(conn: PgConnection, storage) -> None:
    conn._catalog = await conn._build_catalog()
    conn._catalog_fingerprint = storage.fingerprint
    conn._catalog_checked_at = 0.0


async def test_refresh_rebuilds_catalog_when_fingerprint_changes() -> None:
    storage = _RefreshableStorage({"jaffle": [_orders_model()]}, fingerprint="v1")
    conn = _refresh_conn(storage)
    await _seed_catalog(conn, storage)
    before = conn._catalog

    # A model edit that also bumps the storage fingerprint.
    storage._models_by_ds["jaffle"][0].description = "edited"
    storage.fingerprint = "v2"

    await conn._maybe_refresh_catalog()

    assert conn._catalog is not before  # rebuilt
    assert conn._catalog_fingerprint == "v2"
    assert conn._column_type_index is None  # derived index dropped


async def test_refresh_skips_when_fingerprint_unchanged() -> None:
    storage = _RefreshableStorage({"jaffle": [_orders_model()]}, fingerprint="v1")
    conn = _refresh_conn(storage)
    await _seed_catalog(conn, storage)
    before = conn._catalog

    # Content changes but the fingerprint doesn't -> the cheap check
    # short-circuits and we never pay for a rebuild.
    storage._models_by_ds["jaffle"].append(_orders_model())

    await conn._maybe_refresh_catalog()

    assert conn._catalog is before  # NOT rebuilt


async def test_refresh_skips_mid_transaction() -> None:
    storage = _RefreshableStorage({"jaffle": [_orders_model()]}, fingerprint="v1")
    conn = _refresh_conn(storage)
    await _seed_catalog(conn, storage)
    before = conn._catalog

    storage.fingerprint = "v2"  # changed, but...
    conn._tx_state = proto.TX_IN_TRANSACTION  # ...we're inside a transaction

    await conn._maybe_refresh_catalog()

    assert conn._catalog is before  # never shift the catalog mid-transaction


async def test_refresh_disabled_by_default_never_rebuilds() -> None:
    storage = _RefreshableStorage({"jaffle": [_orders_model()]}, fingerprint="v1")
    conn = PgConnection(
        asyncio.StreamReader(), _FakeWriter(),
        engine=_FakeEngine([]), storage=storage,
    )  # no catalog_ttl_seconds -> static catalog (historical behavior)
    conn._storage = storage
    conn._catalog = await conn._build_catalog()
    before = conn._catalog
    storage.fingerprint = "v2"

    await conn._maybe_refresh_catalog()

    assert conn._catalog is before


async def test_refresh_swallows_fingerprint_error_and_keeps_catalog() -> None:
    class _BoomFingerprint(_RefreshableStorage):
        async def graph_fingerprint(self) -> str:
            raise RuntimeError("storage unreachable")  # not an OSError

    storage = _BoomFingerprint({"jaffle": [_orders_model()]}, fingerprint="v1")
    conn = _refresh_conn(storage)
    await _seed_catalog(conn, storage)
    before = conn._catalog

    # Best-effort: the error must be swallowed, not propagate to _run_statement.
    await conn._maybe_refresh_catalog()

    assert conn._catalog is before  # current catalog retained


async def test_refresh_swallows_build_error_and_keeps_catalog() -> None:
    storage = _RefreshableStorage({"jaffle": [_orders_model()]}, fingerprint="v1")
    conn = _refresh_conn(storage)
    await _seed_catalog(conn, storage)
    before = conn._catalog

    # Fingerprint moves (forces a rebuild attempt), but the rebuild itself fails.
    storage.fingerprint = "v2"

    async def _boom():
        raise RuntimeError("rebuild failed")

    conn._build_catalog = _boom  # type: ignore[assignment]

    await conn._maybe_refresh_catalog()  # must not raise

    assert conn._catalog is before  # half-built/None never swapped in
    # Fingerprint not advanced -> the next TTL window retries the rebuild.
    assert conn._catalog_fingerprint == "v1"
