"""Postgres wire protocol v3 — message encode/decode (DEV-1486).

A flat module of pure byte encoders/decoders plus the OID, SQLSTATE, and
startup-int constants. The connection layer (``connection.py``) drives an
``asyncio`` stream through ``read_startup`` / ``read_message`` and writes the
``encode_*`` outputs. Everything here is sync and stream-free so it is trivially
unit-testable.

Framing:
* Regular messages: ``<type:1><length:int32><payload>`` where ``length``
  counts the 4 length bytes + payload but NOT the type byte. Big-endian.
* The startup / SSLRequest / CancelRequest messages have NO type byte:
  ``<length:int32><code:int32><payload>``.
"""

from __future__ import annotations

import struct

from pydantic import BaseModel

# --- type OIDs (Postgres pg_type.oid) ----------------------------------------

OID_BOOL = 16
OID_INT8 = 20
OID_TEXT = 25
OID_FLOAT8 = 701
OID_NUMERIC = 1700
OID_DATE = 1082
OID_TIMESTAMP = 1114
OID_VOID = 2278

# --- SQLSTATE codes ----------------------------------------------------------

SQLSTATE_SYNTAX_ERROR = "42601"
SQLSTATE_UNDEFINED_TABLE = "42P01"
SQLSTATE_UNDEFINED_FUNCTION = "42883"
SQLSTATE_UNDEFINED_DATABASE = "3D000"
SQLSTATE_FEATURE_NOT_SUPPORTED = "0A000"
SQLSTATE_INVALID_AUTHORIZATION = "28000"
SQLSTATE_INVALID_PASSWORD = "28P01"  # NOSONAR(S2068) — SQLSTATE code, not a credential
SQLSTATE_READ_ONLY_SQL_TRANSACTION = "25006"
SQLSTATE_IN_FAILED_SQL_TRANSACTION = "25P02"
SQLSTATE_PROTOCOL_VIOLATION = "08P01"
SQLSTATE_INTERNAL_ERROR = "XX000"

# --- startup magic ints ------------------------------------------------------

SSL_REQUEST_CODE = 80877103
CANCEL_REQUEST_CODE = 80877102
GSSENC_REQUEST_CODE = 80877104
PROTOCOL_VERSION_3 = 196608  # 3.0 << 16

# Transaction-status indicators reported on ReadyForQuery.
TX_IDLE = b"I"
TX_IN_TRANSACTION = b"T"
TX_FAILED = b"E"

# Format codes carried by Bind / RowDescription.
FORMAT_TEXT = 0
FORMAT_BINARY = 1


# --- low-level packing helpers ----------------------------------------------


def _cstr(s: str) -> bytes:
    """A null-terminated UTF-8 string."""
    return s.encode("utf-8") + b"\x00"


def _msg(type_char: bytes, payload: bytes) -> bytes:
    """Frame a regular (tagged) message: type + int32 length + payload."""
    return type_char + struct.pack(">i", len(payload) + 4) + payload


# --- server → client encoders ------------------------------------------------


def encode_authentication_ok() -> bytes:
    return _msg(b"R", struct.pack(">i", 0))


def encode_authentication_cleartext_password() -> bytes:
    return _msg(b"R", struct.pack(">i", 3))


def encode_backend_key_data(pid: int, secret_key: int) -> bytes:
    return _msg(b"K", struct.pack(">ii", pid, secret_key))


def encode_parameter_status(name: str, value: str) -> bytes:
    return _msg(b"S", _cstr(name) + _cstr(value))


def encode_ready_for_query(tx_status: bytes) -> bytes:
    if tx_status not in (TX_IDLE, TX_IN_TRANSACTION, TX_FAILED):
        raise ValueError(f"invalid tx status: {tx_status!r}")
    return _msg(b"Z", tx_status)


class FieldDescription(BaseModel):
    name: str
    type_oid: int
    type_size: int = -1
    type_modifier: int = -1
    format_code: int = FORMAT_TEXT
    table_oid: int = 0
    column_attr: int = 0


def encode_row_description(fields: list[FieldDescription]) -> bytes:
    payload = struct.pack(">h", len(fields))
    for f in fields:
        payload += _cstr(f.name)
        payload += struct.pack(
            ">ihihih",
            f.table_oid,
            f.column_attr,
            f.type_oid,
            f.type_size,
            f.type_modifier,
            f.format_code,
        )
    return _msg(b"T", payload)


def encode_data_row(values: list[bytes | None]) -> bytes:
    payload = struct.pack(">h", len(values))
    for v in values:
        if v is None:
            payload += struct.pack(">i", -1)
        else:
            payload += struct.pack(">i", len(v)) + v
    return _msg(b"D", payload)


def encode_command_complete(tag: str) -> bytes:
    return _msg(b"C", _cstr(tag))


def encode_empty_query_response() -> bytes:
    return _msg(b"I", b"")


def encode_no_data() -> bytes:
    return _msg(b"n", b"")


def encode_parse_complete() -> bytes:
    return _msg(b"1", b"")


def encode_bind_complete() -> bytes:
    return _msg(b"2", b"")


def encode_close_complete() -> bytes:
    return _msg(b"3", b"")


def encode_portal_suspended() -> bytes:
    return _msg(b"s", b"")


def encode_parameter_description(oids: list[int]) -> bytes:
    payload = struct.pack(">h", len(oids))
    for oid in oids:
        payload += struct.pack(">i", oid)
    return _msg(b"t", payload)


def _error_fields(severity: str, code: str, message: str) -> bytes:
    # Field types: S=severity, V=non-localized severity, C=SQLSTATE, M=message.
    return (
        b"S" + _cstr(severity)
        + b"V" + _cstr(severity)
        + b"C" + _cstr(code)
        + b"M" + _cstr(message)
        + b"\x00"
    )


def encode_error_response(*, code: str, message: str, severity: str = "ERROR") -> bytes:
    return _msg(b"E", _error_fields(severity, code, message))


def encode_notice_response(*, code: str, message: str, severity: str = "NOTICE") -> bytes:
    return _msg(b"N", _error_fields(severity, code, message))


# --- client → server decoders (operate on the message BODY, no type byte) ----


class StartupMessage(BaseModel):
    protocol_version: int
    parameters: dict[str, str]


class ParseMessage(BaseModel):
    name: str
    query: str
    parameter_oids: list[int]


class BindMessage(BaseModel):
    portal: str
    statement: str
    parameter_format_codes: list[int]
    parameter_values: list[bytes | None]
    result_format_codes: list[int]


class DescribeMessage(BaseModel):
    kind: str  # "S" (prepared statement) or "P" (portal)
    name: str


class ExecuteMessage(BaseModel):
    portal: str
    max_rows: int


class CloseMessage(BaseModel):
    kind: str  # "S" or "P"
    name: str


class _Reader(BaseModel):
    """A tiny cursor over a bytes body for the decoders."""

    model_config = {"arbitrary_types_allowed": True}

    buf: bytes
    pos: int = 0

    def int16(self) -> int:
        (v,) = struct.unpack_from(">h", self.buf, self.pos)
        self.pos += 2
        return v

    def int32(self) -> int:
        (v,) = struct.unpack_from(">i", self.buf, self.pos)
        self.pos += 4
        return v

    def byte(self) -> bytes:
        b = self.buf[self.pos:self.pos + 1]
        self.pos += 1
        return b

    def cstr(self) -> str:
        end = self.buf.index(b"\x00", self.pos)
        s = self.buf[self.pos:end].decode("utf-8")
        self.pos = end + 1
        return s

    def take(self, n: int) -> bytes:
        b = self.buf[self.pos:self.pos + n]
        self.pos += n
        return b


def _nonneg_count(n: int, what: str) -> int:
    """Reject a negative array-count field (int16) as a protocol violation."""
    if n < 0:
        raise ValueError(f"negative {what} count {n}")
    return n


def decode_startup(body: bytes) -> StartupMessage:
    """Decode a startup-message body (after the 4-byte length). Begins with the
    int32 protocol version, then null-terminated key/value pairs, then a final
    null terminator."""
    r = _Reader(buf=body)
    version = r.int32()
    params: dict[str, str] = {}
    while r.pos < len(body):
        if body[r.pos:r.pos + 1] == b"\x00":
            break
        key = r.cstr()
        value = r.cstr()
        params[key] = value
    return StartupMessage(protocol_version=version, parameters=params)


def decode_query(body: bytes) -> str:
    """Decode a Query (``Q``) body — a single null-terminated SQL string."""
    return _Reader(buf=body).cstr()


def decode_password(body: bytes) -> str:
    """Decode a PasswordMessage (``p``) body — a null-terminated password."""
    return _Reader(buf=body).cstr()


def decode_parse(body: bytes) -> ParseMessage:
    r = _Reader(buf=body)
    name = r.cstr()
    query = r.cstr()
    n = _nonneg_count(r.int16(), "parameter OID")
    oids = [r.int32() for _ in range(n)]
    return ParseMessage(name=name, query=query, parameter_oids=oids)


def decode_bind(body: bytes) -> BindMessage:
    r = _Reader(buf=body)
    portal = r.cstr()
    statement = r.cstr()
    n_fmt = _nonneg_count(r.int16(), "parameter format code")
    fmt_codes = [r.int16() for _ in range(n_fmt)]
    n_params = _nonneg_count(r.int16(), "parameter")
    values: list[bytes | None] = []
    for _ in range(n_params):
        length = r.int32()
        if length == -1:
            values.append(None)
        elif length < -1:
            raise ValueError(f"invalid parameter length {length}")
        else:
            values.append(r.take(length))
    n_res = _nonneg_count(r.int16(), "result format code")
    res_codes = [r.int16() for _ in range(n_res)]
    return BindMessage(
        portal=portal,
        statement=statement,
        parameter_format_codes=fmt_codes,
        parameter_values=values,
        result_format_codes=res_codes,
    )


def decode_describe(body: bytes) -> DescribeMessage:
    r = _Reader(buf=body)
    kind = r.byte().decode("ascii")
    name = r.cstr()
    return DescribeMessage(kind=kind, name=name)


def decode_execute(body: bytes) -> ExecuteMessage:
    r = _Reader(buf=body)
    portal = r.cstr()
    max_rows = r.int32()
    return ExecuteMessage(portal=portal, max_rows=max_rows)


def decode_close(body: bytes) -> CloseMessage:
    r = _Reader(buf=body)
    kind = r.byte().decode("ascii")
    name = r.cstr()
    return CloseMessage(kind=kind, name=name)


# --- generic message splitting (used by the connection layer + tests) --------


def split_messages(buf: bytes) -> list[tuple[str, bytes]]:
    """Split a byte stream of tagged messages into ``[(type_char, body), …]``.

    Used by tests to verify encoded server messages and by any client-side
    helper. The body excludes the type byte and the length prefix.
    """
    out: list[tuple[str, bytes]] = []
    pos = 0
    while pos < len(buf):
        type_char = buf[pos:pos + 1].decode("ascii")
        (length,) = struct.unpack_from(">i", buf, pos + 1)
        body = buf[pos + 5:pos + 1 + length]
        out.append((type_char, body))
        pos += 1 + length
    return out


def validate_format_codes(codes: list[int]) -> None:
    """Reject format codes outside ``{text, binary}`` (protocol violation)."""
    for c in codes:
        if c not in (FORMAT_TEXT, FORMAT_BINARY):
            raise ValueError(f"invalid format code {c!r} (must be 0=text or 1=binary)")


def parse_result_format_codes(codes: list[int], column_count: int) -> list[int]:
    """Resolve Bind result-format codes to one entry per result column.

    Per the protocol: 0 codes → all text; 1 code → applies to every column;
    N codes → one per column. Any other length, or a code outside ``{0, 1}``,
    is a protocol violation (raises ``ValueError``).
    """
    validate_format_codes(codes)
    if not codes:
        return [FORMAT_TEXT] * column_count
    if len(codes) == 1:
        return [codes[0]] * column_count
    if len(codes) != column_count:
        raise ValueError(
            f"result format code count {len(codes)} does not match column "
            f"count {column_count}"
        )
    return list(codes)
