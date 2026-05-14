"""``FlightSqlServer`` — the FlightServerBase subclass that ties everything
together (DEV-1390 §13 item 8).

Decodes each incoming Flight SQL command from ``descriptor.cmd`` / ticket
bytes / ``action.body``, dispatches to ``FlightHandlers``, and serialises
the response into the right wire shape. Authentication is enforced by
the ``BearerTokenMiddlewareFactory`` registered at construction.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, Optional

import pyarrow.flight as fl

from slayer.flight import _flight_sql_pb2 as fsql_pb
from slayer.flight.auth import (
    BearerTokenMiddlewareFactory,
    validate_bind_address,
    validate_tls_pair,
)
from slayer.flight.handlers import (
    FlightHandlers,
    _TYPE_URL_PREFIX,
    decode_command,
    decode_ticket,
)
from slayer.flight.translator import TranslationError

logger = logging.getLogger(__name__)


_ACTION_CREATE_PREPARED = "CreatePreparedStatement"
_ACTION_CLOSE_PREPARED = "ClosePreparedStatement"


def _parse_action_body(body_bytes: bytes, msg_cls: type):
    """Decode a do_action body, transparently unwrapping an ``Any`` wrapper.

    The Apache flight-sql-jdbc-driver sends every action body wrapped in a
    ``google.protobuf.Any`` whose ``type_url`` points at the action class
    (per the Flight SQL spec); pyarrow-flight's Python client sends the
    raw protobuf bytes without an ``Any`` wrapper. We try to parse as
    Any first and look for the Flight SQL type-URL prefix — if present,
    use the Any-wrapped decode; otherwise treat as raw.
    """
    from google.protobuf.any_pb2 import Any as PbAny

    probe = PbAny()
    try:
        probe.ParseFromString(body_bytes)
        if probe.type_url.startswith(_TYPE_URL_PREFIX):
            type_url, decoded = decode_command(body_bytes)
            if not isinstance(decoded, msg_cls):
                raise fl.FlightServerError(
                    f"Expected action body type {msg_cls.__name__!r}, got "
                    f"{type_url!r}"
                )
            return decoded
    except Exception:
        pass

    msg = msg_cls()
    msg.ParseFromString(body_bytes)
    return msg


def _translation_error_to_flight(exc: TranslationError) -> fl.FlightServerError:
    """Translate a translator-level error into a Flight gRPC error.

    Flight SQL clients render the message back to the user as a connection
    or query error; we tag all of these as ``INVALID_ARGUMENT`` per §11.
    """
    return fl.FlightServerError(str(exc))


class FlightSqlServer(fl.FlightServerBase):
    """Pyarrow Flight server implementing the Flight SQL protocol for SLayer.

    Construct with a pre-built ``FlightHandlers``; the server stays thin
    and protocol-focused — all real logic lives in handlers, translator,
    and catalog.
    """

    def __init__(
        self,
        *,
        location: str,
        handlers: FlightHandlers,
        token: Optional[str] = None,
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
    ) -> None:
        tls_certificates = []
        if tls_cert is not None and tls_key is not None:
            cert_bytes = Path(tls_cert).read_bytes()
            key_bytes = Path(tls_key).read_bytes()
            tls_certificates = [(cert_bytes, key_bytes)]
        middleware = {"auth": BearerTokenMiddlewareFactory(token=token)}
        super().__init__(
            location=location,
            tls_certificates=tls_certificates,
            middleware=middleware,
        )
        self._handlers = handlers

    # ----- get_flight_info dispatch -----------------------------------------

    def get_flight_info(
        self, context: fl.ServerCallContext, descriptor: fl.FlightDescriptor,
    ) -> fl.FlightInfo:
        try:
            type_url, msg = decode_command(descriptor.command)
        except fl.FlightServerError:
            raise
        try:
            return self._dispatch_get_flight_info(descriptor, type_url, msg)
        except TranslationError as exc:
            raise _translation_error_to_flight(exc) from exc

    def _dispatch_get_flight_info(
        self,
        descriptor: fl.FlightDescriptor,
        type_url: str,
        msg: object,
    ) -> fl.FlightInfo:
        h = self._handlers
        suffix = type_url.removeprefix(_TYPE_URL_PREFIX)
        if suffix == "CommandStatementQuery":
            sql = msg.query  # type: ignore[attr-defined]
            return h.get_flight_info_for_sql(descriptor, sql)
        if suffix == "CommandPreparedStatementQuery":
            sql_bytes: bytes = msg.prepared_statement_handle  # type: ignore[attr-defined]
            return h.get_flight_info_for_sql(descriptor, sql_bytes.decode("utf-8"))
        if suffix == "CommandGetCatalogs":
            return self._catalog_flight_info(descriptor, h.handle_get_catalogs())
        if suffix == "CommandGetDbSchemas":
            return self._catalog_flight_info(
                descriptor, h.handle_get_db_schemas(msg),  # type: ignore[arg-type]
            )
        if suffix == "CommandGetTables":
            return self._catalog_flight_info(
                descriptor, h.handle_get_tables(msg),  # type: ignore[arg-type]
            )
        if suffix == "CommandGetTableTypes":
            return self._catalog_flight_info(descriptor, h.handle_get_table_types())
        if suffix == "CommandGetPrimaryKeys":
            return self._catalog_flight_info(descriptor, h.handle_get_primary_keys())
        if suffix == "CommandGetExportedKeys":
            return self._catalog_flight_info(descriptor, h.handle_get_exported_keys())
        if suffix == "CommandGetImportedKeys":
            return self._catalog_flight_info(descriptor, h.handle_get_imported_keys())
        if suffix == "CommandGetCrossReference":
            return self._catalog_flight_info(descriptor, h.handle_get_cross_reference())
        if suffix == "CommandGetXdbcTypeInfo":
            return self._catalog_flight_info(descriptor, h.handle_get_xdbc_type_info())
        if suffix == "CommandGetSqlInfo":
            return self._catalog_flight_info(descriptor, h.handle_get_sql_info())
        raise fl.FlightServerError(f"Unhandled Flight SQL command: {suffix}")

    @staticmethod
    def _catalog_flight_info(
        descriptor: fl.FlightDescriptor, table,
    ) -> fl.FlightInfo:
        """Build a FlightInfo for a catalog command whose ticket re-packs
        the original descriptor.cmd bytes (so ``do_get`` knows what to
        serve)."""
        ticket = fl.Ticket(descriptor.command)
        endpoints = [fl.FlightEndpoint(ticket, [])]
        return fl.FlightInfo(table.schema, descriptor, endpoints, -1, -1)

    # ----- do_get dispatch ---------------------------------------------------

    def do_get(self, context: fl.ServerCallContext, ticket: fl.Ticket):
        try:
            type_url, msg = decode_ticket(ticket.ticket)
        except fl.FlightServerError:
            raise
        try:
            return self._dispatch_do_get(type_url, msg)
        except TranslationError as exc:
            raise _translation_error_to_flight(exc) from exc

    def _dispatch_do_get(self, type_url: str, msg: object):
        h = self._handlers
        suffix = type_url.removeprefix(_TYPE_URL_PREFIX)
        if suffix == "TicketStatementQuery":
            sql_bytes: bytes = msg.statement_handle  # type: ignore[attr-defined]
            return h.do_get_for_sql(sql_bytes.decode("utf-8"))
        if suffix == "CommandPreparedStatementQuery":
            sql_bytes = msg.prepared_statement_handle  # type: ignore[attr-defined]
            return h.do_get_for_sql(sql_bytes.decode("utf-8"))
        # Catalog commands — the ticket bytes ARE the original descriptor.cmd,
        # so we dispatch the same way we did in get_flight_info.
        if suffix == "CommandGetCatalogs":
            return fl.RecordBatchStream(h.handle_get_catalogs())
        if suffix == "CommandGetDbSchemas":
            return fl.RecordBatchStream(h.handle_get_db_schemas(msg))  # type: ignore[arg-type]
        if suffix == "CommandGetTables":
            return fl.RecordBatchStream(h.handle_get_tables(msg))  # type: ignore[arg-type]
        if suffix == "CommandGetTableTypes":
            return fl.RecordBatchStream(h.handle_get_table_types())
        if suffix == "CommandGetPrimaryKeys":
            return fl.RecordBatchStream(h.handle_get_primary_keys())
        if suffix == "CommandGetExportedKeys":
            return fl.RecordBatchStream(h.handle_get_exported_keys())
        if suffix == "CommandGetImportedKeys":
            return fl.RecordBatchStream(h.handle_get_imported_keys())
        if suffix == "CommandGetCrossReference":
            return fl.RecordBatchStream(h.handle_get_cross_reference())
        if suffix == "CommandGetXdbcTypeInfo":
            return fl.RecordBatchStream(h.handle_get_xdbc_type_info())
        if suffix == "CommandGetSqlInfo":
            return fl.RecordBatchStream(h.handle_get_sql_info())
        raise fl.FlightServerError(f"Unhandled ticket type: {suffix}")

    # ----- do_action dispatch ------------------------------------------------

    def list_actions(
        self, context: fl.ServerCallContext,
    ) -> list[fl.ActionType]:
        return [
            fl.ActionType(_ACTION_CREATE_PREPARED,
                          "Create a prepared statement from a SQL string"),
            fl.ActionType(_ACTION_CLOSE_PREPARED,
                          "Close a prepared statement (no-op; server is stateless)"),
        ]

    def do_action(
        self, context: fl.ServerCallContext, action: fl.Action,
    ) -> Iterator[fl.Result]:
        action_type = action.type
        body_bytes = action.body.to_pybytes() if action.body is not None else b""
        try:
            if action_type == _ACTION_CREATE_PREPARED:
                cmd = _parse_action_body(
                    body_bytes, fsql_pb.ActionCreatePreparedStatementRequest,
                )
                response_bytes = self._handlers.handle_create_prepared_statement(cmd)
                yield fl.Result(response_bytes)
                return
            if action_type == _ACTION_CLOSE_PREPARED:
                cmd = _parse_action_body(
                    body_bytes, fsql_pb.ActionClosePreparedStatementRequest,
                )
                self._handlers.handle_close_prepared_statement(cmd)
                return
        except TranslationError as exc:
            raise _translation_error_to_flight(exc) from exc
        raise fl.FlightServerError(f"Unsupported action type: {action_type!r}")


def build_server(
    *,
    host: str,
    port: int,
    handlers: FlightHandlers,
    token: Optional[str] = None,
    tls_cert: Optional[str] = None,
    tls_key: Optional[str] = None,
) -> FlightSqlServer:
    """Factory wrapping ``FlightSqlServer`` with startup-time validation.

    Verifies the bind-address / token combination and the TLS pair before
    instantiating the server (§4.3 / §4.4 / §7.1).
    """
    validate_bind_address(host=host, token=token)
    validate_tls_pair(cert=tls_cert, key=tls_key)
    scheme = "grpc+tls" if tls_cert is not None else "grpc"
    location = f"{scheme}://{host}:{port}"
    return FlightSqlServer(
        location=location, handlers=handlers, token=token,
        tls_cert=tls_cert, tls_key=tls_key,
    )
