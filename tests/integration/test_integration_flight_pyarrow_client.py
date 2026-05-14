"""Live integration tests for the Flight SQL facade via pyarrow.flight (DEV-1390 Task 15b).

Java-free integration suite: drives the production Flight SQL server
through pyarrow's native Flight client (gRPC over Python). Always runs
in CI since there is no JDK requirement.

Covers the same surface as the JayDeBeAPI tests in
``test_integration_flight.py``, but using a direct gRPC client:
catalog commands, prepared-statement round-trips, probe queries, real
metric/dim SELECTs, and bearer-token auth (which works here because the
pyarrow client honours ``Authorization`` headers without a server-side
handshake handler — see Task 15a's xfail note for the JDBC token gap).
"""

from __future__ import annotations

import threading
from typing import Tuple

import pyarrow.flight as fl
import pytest
from google.protobuf.any_pb2 import Any as PbAny

from slayer.flight import _flight_sql_pb2 as fsql_pb

pytestmark = pytest.mark.integration


_TYPE_URL_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql."


def _pack_command(msg, suffix: str) -> bytes:
    """Wrap a Flight SQL command in an ``Any`` for ``FlightDescriptor.cmd``."""
    any_msg = PbAny()
    any_msg.type_url = f"{_TYPE_URL_PREFIX}{suffix}"
    any_msg.value = msg.SerializeToString()
    return any_msg.SerializeToString()


def _descriptor_for(msg, suffix: str) -> fl.FlightDescriptor:
    return fl.FlightDescriptor.for_command(_pack_command(msg, suffix))


def _client(host: str, port: int, *, token: str | None = None) -> fl.FlightClient:
    """Construct a pyarrow Flight client, optionally with a bearer token header."""
    return fl.FlightClient(f"grpc://{host}:{port}")


def _bearer_options(token: str | None) -> fl.FlightCallOptions:
    """Build call-options that carry an ``Authorization: Bearer X`` header.

    Our middleware validates this on every RPC, so we set it per call rather
    than via a one-shot handshake handler.
    """
    headers: list[tuple[bytes, bytes]] = []
    if token is not None:
        headers.append((b"authorization", f"Bearer {token}".encode("utf-8")))
    return fl.FlightCallOptions(headers=headers)


# ----- catalog commands ------------------------------------------------------


def test_get_catalogs(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    descriptor = _descriptor_for(
        fsql_pb.CommandGetCatalogs(), "CommandGetCatalogs",
    )
    info = client.get_flight_info(descriptor)
    table = client.do_get(info.endpoints[0].ticket).read_all()
    rows = table.to_pylist()
    assert any(r["catalog_name"] == "slayer" for r in rows)


def test_get_db_schemas(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    descriptor = _descriptor_for(
        fsql_pb.CommandGetDbSchemas(), "CommandGetDbSchemas",
    )
    info = client.get_flight_info(descriptor)
    table = client.do_get(info.endpoints[0].ticket).read_all()
    names = {r["db_schema_name"] for r in table.to_pylist()}
    assert "jaffle_shop" in names


def test_get_tables(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    descriptor = _descriptor_for(fsql_pb.CommandGetTables(), "CommandGetTables")
    info = client.get_flight_info(descriptor)
    table = client.do_get(info.endpoints[0].ticket).read_all()
    rows = table.to_pylist()
    pairs = {(r["db_schema_name"], r["table_name"]) for r in rows}
    assert ("jaffle_shop", "orders") in pairs
    assert ("jaffle_shop", "customers") in pairs


def test_get_table_types(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    descriptor = _descriptor_for(
        fsql_pb.CommandGetTableTypes(), "CommandGetTableTypes",
    )
    info = client.get_flight_info(descriptor)
    table = client.do_get(info.endpoints[0].ticket).read_all()
    rows = {r["table_type"] for r in table.to_pylist()}
    assert {"TABLE", "VIEW"} <= rows


def test_get_primary_keys_empty(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    cmd = fsql_pb.CommandGetPrimaryKeys()
    cmd.table = "orders"
    descriptor = _descriptor_for(cmd, "CommandGetPrimaryKeys")
    info = client.get_flight_info(descriptor)
    table = client.do_get(info.endpoints[0].ticket).read_all()
    assert table.num_rows == 0
    # Verify the wire schema still carries the JDBC-standard column names.
    assert "table_name" in table.schema.names
    assert "column_name" in table.schema.names


def test_get_sql_info(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    descriptor = _descriptor_for(fsql_pb.CommandGetSqlInfo(), "CommandGetSqlInfo")
    info = client.get_flight_info(descriptor)
    table = client.do_get(info.endpoints[0].ticket).read_all()
    rows = table.to_pylist()
    info_by_id = {r["info_name"]: r["value"] for r in rows}
    assert info_by_id[int(fsql_pb.SqlInfo.FLIGHT_SQL_SERVER_NAME)] == "SLayer"


# ----- prepared-statement round-trips ----------------------------------------


def _create_prepared(client: fl.FlightClient, sql: str, *, token: str | None = None):
    """Helper: issue ``CreatePreparedStatement`` and parse the Any-wrapped result."""
    req = fsql_pb.ActionCreatePreparedStatementRequest()
    req.query = sql
    action = fl.Action("CreatePreparedStatement", req.SerializeToString())
    results = list(client.do_action(action, options=_bearer_options(token)))
    assert len(results) == 1
    any_msg = PbAny()
    any_msg.ParseFromString(results[0].body.to_pybytes())
    assert any_msg.type_url.endswith("ActionCreatePreparedStatementResult"), (
        f"unexpected response type_url: {any_msg.type_url!r}"
    )
    resp = fsql_pb.ActionCreatePreparedStatementResult()
    resp.ParseFromString(any_msg.value)
    return resp


def _execute_prepared(client: fl.FlightClient, handle: bytes):
    """Helper: run ``CommandPreparedStatementQuery{handle}`` end-to-end."""
    cmd = fsql_pb.CommandPreparedStatementQuery()
    cmd.prepared_statement_handle = handle
    descriptor = _descriptor_for(cmd, "CommandPreparedStatementQuery")
    info = client.get_flight_info(descriptor)
    return client.do_get(info.endpoints[0].ticket).read_all()


def test_prepared_statement_row_count(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    resp = _create_prepared(client, "SELECT row_count FROM orders")
    assert resp.prepared_statement_handle == b"SELECT row_count FROM orders"
    table = _execute_prepared(client, resp.prepared_statement_handle)
    assert table.column_names == ["row_count"]
    assert table.num_rows == 1
    assert int(table.to_pylist()[0]["row_count"]) > 0


def test_prepared_statement_time_grain(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    sql = (
        "SELECT month(ordered_at) AS m, row_count FROM orders "
        "WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31' "
        "ORDER BY m"
    )
    resp = _create_prepared(client, sql)
    table = _execute_prepared(client, resp.prepared_statement_handle)
    assert table.column_names == ["m", "row_count"]
    assert 1 <= table.num_rows <= 12
    for row in table.to_pylist():
        assert int(row["row_count"]) > 0


def test_prepared_statement_cross_model_dim(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    resp = _create_prepared(
        client, "SELECT customers.name, row_count FROM orders",
    )
    table = _execute_prepared(client, resp.prepared_statement_handle)
    assert table.column_names == ["customers.name", "row_count"]
    assert table.num_rows > 0


def test_prepared_statement_info_schema_metrics(
    flight_demo_server: Tuple[str, int],
) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    resp = _create_prepared(client, "SELECT * FROM INFORMATION_SCHEMA.METRICS")
    table = _execute_prepared(client, resp.prepared_statement_handle)
    rows = table.to_pylist()
    assert any(r["table_name"] == "orders" for r in rows)


@pytest.mark.parametrize(
    "probe_sql",
    [
        "SELECT 1",
        "SELECT NULL WHERE 1=0",
        "SELECT version()",
        "SELECT current_database()",
    ],
)
def test_prepared_statement_probe_queries(
    flight_demo_server: Tuple[str, int], probe_sql: str,
) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    resp = _create_prepared(client, probe_sql)
    table = _execute_prepared(client, resp.prepared_statement_handle)
    if "1=0" in probe_sql:
        assert table.num_rows == 0
    else:
        assert table.num_rows == 1
    assert table.num_columns >= 1


# ----- error paths -----------------------------------------------------------


def test_select_star_rejected(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    with pytest.raises(fl.FlightServerError) as excinfo:
        _create_prepared(client, "SELECT * FROM orders")
    assert "SELECT * not supported" in str(excinfo.value)


def test_dml_rejected(flight_demo_server: Tuple[str, int]) -> None:
    host, port = flight_demo_server
    client = _client(host, port)
    with pytest.raises(fl.FlightServerError) as excinfo:
        _create_prepared(client, "INSERT INTO orders VALUES (1)")
    assert "read-only" in str(excinfo.value).lower()


def test_close_prepared_statement(flight_demo_server: Tuple[str, int]) -> None:
    """``ActionClosePreparedStatementRequest`` is a no-op; it must complete cleanly."""
    host, port = flight_demo_server
    client = _client(host, port)
    resp = _create_prepared(client, "SELECT 1")
    close_req = fsql_pb.ActionClosePreparedStatementRequest()
    close_req.prepared_statement_handle = resp.prepared_statement_handle
    list(client.do_action(
        fl.Action("ClosePreparedStatement", close_req.SerializeToString()),
    ))


# ----- bearer-token auth -----------------------------------------------------


def test_auth_positive(
    flight_demo_server_with_token: Tuple[str, int, str],
) -> None:
    """With the correct bearer token attached on every RPC, the server accepts."""
    host, port, token = flight_demo_server_with_token
    client = _client(host, port)
    descriptor = _descriptor_for(fsql_pb.CommandGetCatalogs(), "CommandGetCatalogs")
    info = client.get_flight_info(descriptor, options=_bearer_options(token))
    table = client.do_get(
        info.endpoints[0].ticket, options=_bearer_options(token),
    ).read_all()
    rows = table.to_pylist()
    assert any(r["catalog_name"] == "slayer" for r in rows)


def test_auth_negative_missing_token(
    flight_demo_server_with_token: Tuple[str, int, str],
) -> None:
    """Without an Authorization header the server rejects with UNAUTHENTICATED."""
    host, port, _token = flight_demo_server_with_token
    client = _client(host, port)
    descriptor = _descriptor_for(fsql_pb.CommandGetCatalogs(), "CommandGetCatalogs")
    with pytest.raises(fl.FlightUnauthenticatedError):
        client.get_flight_info(descriptor)


def test_auth_negative_wrong_token(
    flight_demo_server_with_token: Tuple[str, int, str],
) -> None:
    host, port, _token = flight_demo_server_with_token
    client = _client(host, port)
    descriptor = _descriptor_for(fsql_pb.CommandGetCatalogs(), "CommandGetCatalogs")
    with pytest.raises(fl.FlightUnauthenticatedError):
        client.get_flight_info(descriptor, options=_bearer_options("wrong"))


# ----- concurrency -----------------------------------------------------------


def test_n10_concurrent_prepared_statements(flight_demo_server: Tuple[str, int]) -> None:
    """Ten parallel prepared-statement round-trips against the same server."""
    host, port = flight_demo_server

    results: list[int] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            client = _client(host, port)
            resp = _create_prepared(client, "SELECT row_count FROM orders")
            table = _execute_prepared(client, resp.prepared_statement_handle)
            with lock:
                results.append(int(table.to_pylist()[0]["row_count"]))
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, f"concurrent workers raised: {errors!r}"
    assert len(results) == 10
    assert len(set(results)) == 1
    assert results[0] > 0
