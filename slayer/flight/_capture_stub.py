"""Test-only Flight servers that log every incoming RPC to a JSONL file.

Two implementations:

* :class:`CaptureFlightServer` — minimal stub that always returns empty /
  well-typed responses. Used by ``tests/flight/capture_dbt_jdbc.py`` for
  the initial Phase 1.0 capture against the upstream Apache JDBC driver.
* :class:`RecordingFlightSqlServer` — subclass of the production
  :class:`slayer.flight.server.FlightSqlServer` that mirrors every RPC
  to a JSONL log before delegating to the real handlers. Used for the
  Phase 1 refresh capture: the JDBC driver completes the
  prepared-statement triplet (which the stub couldn't satisfy), so the
  JSONL trace fills in ``CommandPreparedStatementQuery`` + the close
  request.

This module is intentionally not exported from ``slayer.flight`` — it's
test-infrastructure, not part of the shipped surface.
"""

import base64
import json
import time
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.flight as fl

from slayer.flight.handlers import FlightHandlers
from slayer.flight.server import FlightSqlServer


class CaptureFlightServer(fl.FlightServerBase):
    """Flight server stub that JSON-logs every RPC name, descriptor, ticket,
    and gRPC metadata header to a single JSONL file (one record per line).
    """

    def __init__(self, location: str, log_path: Path) -> None:
        super().__init__(location)
        self._log_path = Path(log_path)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_path.write_text("")

    def _log(self, *, rpc: str, **payload: Any) -> None:
        record = {"ts": time.time(), "rpc": rpc, **payload}
        with self._log_path.open("a") as f:
            f.write(json.dumps(record, default=str) + "\n")

    @staticmethod
    def _b64(b: Optional[bytes]) -> Optional[str]:
        return base64.b64encode(b).decode("ascii") if b else None

    @staticmethod
    def _metadata(context: fl.ServerCallContext) -> dict:
        try:
            raw = context.headers() or []
        except Exception:
            return {}
        out: dict = {}
        for k, v in raw:
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode(errors="replace") if isinstance(v, bytes) else v
            out[key] = val
        return out

    @staticmethod
    def _descriptor_payload(descriptor: fl.FlightDescriptor) -> dict:
        return {
            "descriptor_type": str(descriptor.descriptor_type),
            "cmd_b64": CaptureFlightServer._b64(descriptor.command),
            "path": [
                p.decode("utf-8", errors="replace") if isinstance(p, bytes) else p
                for p in (descriptor.path or [])
            ],
        }

    def list_flights(self, context: fl.ServerCallContext, criteria: bytes):
        self._log(
            rpc="list_flights",
            criteria_b64=self._b64(criteria),
            metadata=self._metadata(context),
        )
        return iter([])

    def get_flight_info(
        self, context: fl.ServerCallContext, descriptor: fl.FlightDescriptor
    ) -> fl.FlightInfo:
        self._log(
            rpc="get_flight_info",
            **self._descriptor_payload(descriptor),
            metadata=self._metadata(context),
        )
        schema = pa.schema([])
        ticket = fl.Ticket(descriptor.command or b"capture-stub")
        endpoints = [fl.FlightEndpoint(ticket, [])]
        return fl.FlightInfo(schema, descriptor, endpoints, -1, -1)

    def get_schema(
        self, context: fl.ServerCallContext, descriptor: fl.FlightDescriptor
    ) -> fl.SchemaResult:
        self._log(
            rpc="get_schema",
            **self._descriptor_payload(descriptor),
            metadata=self._metadata(context),
        )
        return fl.SchemaResult(pa.schema([]))

    def do_get(self, context: fl.ServerCallContext, ticket: fl.Ticket):
        ticket_bytes = ticket.ticket if isinstance(ticket.ticket, bytes) else bytes(ticket.ticket)
        self._log(
            rpc="do_get",
            ticket_b64=self._b64(ticket_bytes),
            ticket_str=ticket_bytes.decode("utf-8", errors="replace"),
            metadata=self._metadata(context),
        )
        return fl.RecordBatchStream(pa.Table.from_pylist([]))

    def do_put(
        self,
        context: fl.ServerCallContext,
        descriptor: fl.FlightDescriptor,
        reader: fl.MetadataRecordBatchReader,
        writer: fl.FlightMetadataWriter,
    ) -> None:
        self._log(
            rpc="do_put",
            **self._descriptor_payload(descriptor),
            metadata=self._metadata(context),
        )

    def do_exchange(
        self,
        context: fl.ServerCallContext,
        descriptor: fl.FlightDescriptor,
        reader: fl.MetadataRecordBatchReader,
        writer: fl.MetadataRecordBatchWriter,
    ) -> None:
        self._log(
            rpc="do_exchange",
            **self._descriptor_payload(descriptor),
            metadata=self._metadata(context),
        )

    def list_actions(self, context: fl.ServerCallContext):
        self._log(rpc="list_actions", metadata=self._metadata(context))
        return []

    def do_action(self, context: fl.ServerCallContext, action: fl.Action):
        body_bytes: Optional[bytes] = None
        if action.body is not None:
            body_bytes = action.body.to_pybytes()
        self._log(
            rpc="do_action",
            action_type=action.type,
            body_b64=self._b64(body_bytes),
            metadata=self._metadata(context),
        )
        return iter([])


class _RpcLogger:
    """Mixin helper that writes JSONL records to a path."""

    def __init__(self, log_path: Path) -> None:
        self._log_path = Path(log_path)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_path.write_text("")

    def _log(self, *, rpc: str, **payload: Any) -> None:
        record = {"ts": time.time(), "rpc": rpc, **payload}
        with self._log_path.open("a") as f:
            f.write(json.dumps(record, default=str) + "\n")

    @staticmethod
    def _b64(b: Optional[bytes]) -> Optional[str]:
        return base64.b64encode(b).decode("ascii") if b else None

    @staticmethod
    def _metadata(context: fl.ServerCallContext) -> dict:
        try:
            raw = context.headers() or []
        except Exception:
            return {}
        out: dict = {}
        for k, v in raw:
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode(errors="replace") if isinstance(v, bytes) else v
            out[key] = val
        return out


class RecordingFlightSqlServer(FlightSqlServer):
    """Production FlightSqlServer that mirrors every RPC to a JSONL log.

    Drop-in subclass — adds a per-RPC ``_log`` call before delegating to
    the real handler chain. Used by ``tests/flight/capture_dbt_jdbc.py``
    to refresh the wire-capture corpus against a working server (the
    Phase 1.0 ``CaptureFlightServer`` returned empties, so the JDBC
    driver couldn't complete the prepared-statement triplet).
    """

    def __init__(
        self,
        *,
        location: str,
        handlers: FlightHandlers,
        log_path: Path,
        token: Optional[str] = None,
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
    ) -> None:
        super().__init__(
            location=location,
            handlers=handlers,
            token=token,
            tls_cert=tls_cert,
            tls_key=tls_key,
        )
        self._recorder = _RpcLogger(log_path)

    def get_flight_info(
        self, context: fl.ServerCallContext, descriptor: fl.FlightDescriptor,
    ) -> fl.FlightInfo:
        self._recorder._log(
            rpc="get_flight_info",
            descriptor_type=str(descriptor.descriptor_type),
            cmd_b64=self._recorder._b64(descriptor.command),
            path=[
                p.decode("utf-8", errors="replace") if isinstance(p, bytes) else p
                for p in (descriptor.path or [])
            ],
            metadata=self._recorder._metadata(context),
        )
        return super().get_flight_info(context, descriptor)

    def do_get(self, context: fl.ServerCallContext, ticket: fl.Ticket):
        ticket_bytes = ticket.ticket if isinstance(ticket.ticket, bytes) else bytes(ticket.ticket)
        self._recorder._log(
            rpc="do_get",
            ticket_b64=self._recorder._b64(ticket_bytes),
            ticket_str=ticket_bytes.decode("utf-8", errors="replace"),
            metadata=self._recorder._metadata(context),
        )
        return super().do_get(context, ticket)

    def do_action(self, context: fl.ServerCallContext, action: fl.Action):
        body_bytes: Optional[bytes] = (
            action.body.to_pybytes() if action.body is not None else None
        )
        self._recorder._log(
            rpc="do_action",
            action_type=action.type,
            body_b64=self._recorder._b64(body_bytes),
            metadata=self._recorder._metadata(context),
        )
        return super().do_action(context, action)
