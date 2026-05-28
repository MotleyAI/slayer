"""Tests for slayer.pg_facade.protocol — PG wire v3 encode/decode (DEV-1486)."""

from __future__ import annotations

import struct

import pytest

from slayer.pg_facade import protocol as p


# --- server-message encoders: frame + field roundtrip ------------------------


def _single(buf: bytes):
    msgs = p.split_messages(buf)
    assert len(msgs) == 1
    return msgs[0]


def test_authentication_ok_frame() -> None:
    type_char, body = _single(p.encode_authentication_ok())
    assert type_char == "R"
    assert struct.unpack(">i", body)[0] == 0


def test_authentication_cleartext_password_frame() -> None:
    type_char, body = _single(p.encode_authentication_cleartext_password())
    assert type_char == "R"
    assert struct.unpack(">i", body)[0] == 3


def test_backend_key_data_frame() -> None:
    type_char, body = _single(p.encode_backend_key_data(4242, 9999))
    assert type_char == "K"
    pid, key = struct.unpack(">ii", body)
    assert (pid, key) == (4242, 9999)


def test_parameter_status_frame() -> None:
    type_char, body = _single(p.encode_parameter_status("server_version", "14.0"))
    assert type_char == "S"
    assert body == b"server_version\x00" + b"14.0\x00"


@pytest.mark.parametrize("status", [p.TX_IDLE, p.TX_IN_TRANSACTION, p.TX_FAILED])
def test_ready_for_query_frame(status: bytes) -> None:
    type_char, body = _single(p.encode_ready_for_query(status))
    assert type_char == "Z"
    assert body == status


def test_ready_for_query_rejects_bad_status() -> None:
    with pytest.raises(ValueError):
        p.encode_ready_for_query(b"X")


def test_row_description_frame() -> None:
    fields = [
        p.FieldDescription(name="a", type_oid=p.OID_INT8, format_code=p.FORMAT_BINARY),
        p.FieldDescription(name="b", type_oid=p.OID_TEXT, format_code=p.FORMAT_TEXT),
    ]
    type_char, body = _single(p.encode_row_description(fields))
    assert type_char == "T"
    count = struct.unpack_from(">h", body, 0)[0]
    assert count == 2
    # First field name is null-terminated right after the count.
    assert body[2:4] == b"a\x00"


def test_data_row_frame_with_null() -> None:
    type_char, body = _single(p.encode_data_row([b"42", None, b"x"]))
    assert type_char == "D"
    count = struct.unpack_from(">h", body, 0)[0]
    assert count == 3
    # value 1: len 2 + "42"
    assert struct.unpack_from(">i", body, 2)[0] == 2
    assert body[6:8] == b"42"
    # value 2: -1 (NULL)
    assert struct.unpack_from(">i", body, 8)[0] == -1


def test_command_complete_frame() -> None:
    type_char, body = _single(p.encode_command_complete("SELECT 3"))
    assert type_char == "C"
    assert body == b"SELECT 3\x00"


def test_empty_query_no_data_parse_bind_close_complete_frames() -> None:
    assert _single(p.encode_empty_query_response()) == ("I", b"")
    assert _single(p.encode_no_data()) == ("n", b"")
    assert _single(p.encode_parse_complete()) == ("1", b"")
    assert _single(p.encode_bind_complete()) == ("2", b"")
    assert _single(p.encode_close_complete()) == ("3", b"")
    assert _single(p.encode_portal_suspended()) == ("s", b"")


def test_parameter_description_frame() -> None:
    type_char, body = _single(p.encode_parameter_description([p.OID_TEXT, p.OID_INT8]))
    assert type_char == "t"
    count = struct.unpack_from(">h", body, 0)[0]
    assert count == 2
    assert struct.unpack_from(">i", body, 2)[0] == p.OID_TEXT
    assert struct.unpack_from(">i", body, 6)[0] == p.OID_INT8


def test_error_response_fields() -> None:
    type_char, body = _single(
        p.encode_error_response(code=p.SQLSTATE_UNDEFINED_TABLE, message="nope")
    )
    assert type_char == "E"
    assert b"C" + b"42P01\x00" in body
    assert b"M" + b"nope\x00" in body
    assert b"SERROR\x00" in body
    assert body.endswith(b"\x00")


def test_notice_response_fields() -> None:
    type_char, body = _single(
        p.encode_notice_response(code=p.SQLSTATE_FEATURE_NOT_SUPPORTED, message="hi")
    )
    assert type_char == "N"
    assert b"M" + b"hi\x00" in body


# --- client-message decoders -------------------------------------------------


def _startup_body(version: int, params: dict) -> bytes:
    body = struct.pack(">i", version)
    for k, v in params.items():
        body += k.encode() + b"\x00" + v.encode() + b"\x00"
    body += b"\x00"
    return body


def test_decode_startup() -> None:
    body = _startup_body(p.PROTOCOL_VERSION_3, {"user": "egor", "database": "jaffle"})
    msg = p.decode_startup(body)
    assert msg.protocol_version == p.PROTOCOL_VERSION_3
    assert msg.parameters == {"user": "egor", "database": "jaffle"}


def test_decode_query() -> None:
    assert p.decode_query(b"SELECT 1\x00") == "SELECT 1"


def test_decode_password() -> None:
    assert p.decode_password(b"hunter2\x00") == "hunter2"


def test_decode_parse_no_params() -> None:
    body = b"stmt1\x00" + b"SELECT 1\x00" + struct.pack(">h", 0)
    msg = p.decode_parse(body)
    assert msg.name == "stmt1"
    assert msg.query == "SELECT 1"
    assert msg.parameter_oids == []


def test_decode_parse_with_param_oids() -> None:
    body = (
        b"\x00" + b"SELECT $1\x00"
        + struct.pack(">h", 1) + struct.pack(">i", p.OID_TEXT)
    )
    msg = p.decode_parse(body)
    assert msg.parameter_oids == [p.OID_TEXT]


def test_decode_bind_text_param() -> None:
    body = (
        b"portal\x00" + b"stmt1\x00"
        + struct.pack(">h", 0)  # zero param format codes → all text
        + struct.pack(">h", 1)  # one parameter
        + struct.pack(">i", 5) + b"hello"
        + struct.pack(">h", 1) + struct.pack(">h", p.FORMAT_BINARY)  # result format
    )
    msg = p.decode_bind(body)
    assert msg.portal == "portal"
    assert msg.statement == "stmt1"
    assert msg.parameter_values == [b"hello"]
    assert msg.result_format_codes == [p.FORMAT_BINARY]


def test_decode_bind_rejects_invalid_negative_length() -> None:
    # Only -1 (NULL) is a valid negative length; -2 is a protocol violation.
    body = (
        b"\x00" + b"\x00"
        + struct.pack(">h", 0)
        + struct.pack(">h", 1)
        + struct.pack(">i", -2)
        + struct.pack(">h", 0)
    )
    with pytest.raises(ValueError):
        p.decode_bind(body)


def test_decode_bind_null_param() -> None:
    body = (
        b"\x00" + b"\x00"
        + struct.pack(">h", 0)
        + struct.pack(">h", 1)
        + struct.pack(">i", -1)  # NULL
        + struct.pack(">h", 0)
    )
    msg = p.decode_bind(body)
    assert msg.parameter_values == [None]


def test_decode_describe_and_close() -> None:
    d = p.decode_describe(b"S" + b"stmt1\x00")
    assert d.kind == "S"
    assert d.name == "stmt1"
    c = p.decode_close(b"P" + b"portal\x00")
    assert c.kind == "P"
    assert c.name == "portal"


def test_decode_execute() -> None:
    body = b"portal\x00" + struct.pack(">i", 100)
    msg = p.decode_execute(body)
    assert msg.portal == "portal"
    assert msg.max_rows == 100


# --- framing + constants -----------------------------------------------------


def test_split_messages_handles_back_to_back() -> None:
    buf = p.encode_parse_complete() + p.encode_bind_complete() + p.encode_command_complete("SELECT 1")
    msgs = p.split_messages(buf)
    assert [m[0] for m in msgs] == ["1", "2", "C"]


def test_special_startup_codes() -> None:
    assert p.SSL_REQUEST_CODE == 80877103
    assert p.CANCEL_REQUEST_CODE == 80877102
    assert p.PROTOCOL_VERSION_3 == 196608


def test_oid_constants() -> None:
    assert (p.OID_BOOL, p.OID_INT8, p.OID_TEXT, p.OID_FLOAT8, p.OID_DATE, p.OID_TIMESTAMP) == (
        16, 20, 25, 701, 1082, 1114,
    )


@pytest.mark.parametrize(
    "codes,count,expected",
    [
        ([], 3, [0, 0, 0]),
        ([1], 3, [1, 1, 1]),
        ([0, 1], 2, [0, 1]),
    ],
)
def test_parse_result_format_codes(codes, count, expected) -> None:
    assert p.parse_result_format_codes(codes, count) == expected
