"""Flight SQL command handlers (DEV-1390 §4.2, §6.4).

Decodes incoming Flight SQL commands from ``descriptor.cmd``, ``action.body``
and ticket bytes; dispatches to per-command logic; serialises responses.

All public methods are synchronous because pyarrow's ``FlightServerBase``
dispatches each RPC on its own gRPC thread. SLayer's storage / engine
are async; we bridge through :func:`slayer.async_utils.run_sync`.
"""

from __future__ import annotations

import decimal
import logging
from collections import defaultdict
from typing import Dict, List, Tuple

import pyarrow as pa
import pyarrow.flight as fl
from google.protobuf.any_pb2 import Any as PbAny

from slayer.async_utils import run_sync
from slayer.core.models import SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.flight import _flight_sql_pb2 as fsql_pb
from slayer.flight.catalog import (
    CATALOG_NAME,
    FlightCatalog,
    build_catalog,
)
from slayer.flight.translator import (
    InfoSchemaResult,
    NoOpResult,
    ProbeResult,
    QueryResult,
    translate,
)
from slayer.flight.types import datatype_to_arrow
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


# Type URL prefix that Flight SQL uses for its ``Any``-wrapped commands.
_TYPE_URL_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql."


_COMMAND_BY_TYPE_URL: Dict[str, type] = {
    f"{_TYPE_URL_PREFIX}CommandStatementQuery": fsql_pb.CommandStatementQuery,
    f"{_TYPE_URL_PREFIX}CommandPreparedStatementQuery": fsql_pb.CommandPreparedStatementQuery,
    f"{_TYPE_URL_PREFIX}CommandGetCatalogs": fsql_pb.CommandGetCatalogs,
    f"{_TYPE_URL_PREFIX}CommandGetDbSchemas": fsql_pb.CommandGetDbSchemas,
    f"{_TYPE_URL_PREFIX}CommandGetTables": fsql_pb.CommandGetTables,
    f"{_TYPE_URL_PREFIX}CommandGetTableTypes": fsql_pb.CommandGetTableTypes,
    f"{_TYPE_URL_PREFIX}CommandGetPrimaryKeys": fsql_pb.CommandGetPrimaryKeys,
    f"{_TYPE_URL_PREFIX}CommandGetExportedKeys": fsql_pb.CommandGetExportedKeys,
    f"{_TYPE_URL_PREFIX}CommandGetImportedKeys": fsql_pb.CommandGetImportedKeys,
    f"{_TYPE_URL_PREFIX}CommandGetCrossReference": fsql_pb.CommandGetCrossReference,
    f"{_TYPE_URL_PREFIX}CommandGetXdbcTypeInfo": fsql_pb.CommandGetXdbcTypeInfo,
    f"{_TYPE_URL_PREFIX}CommandGetSqlInfo": fsql_pb.CommandGetSqlInfo,
    f"{_TYPE_URL_PREFIX}TicketStatementQuery": fsql_pb.TicketStatementQuery,
    # Prepared-statement actions arrive Any-wrapped via do_action's body.
    f"{_TYPE_URL_PREFIX}ActionCreatePreparedStatementRequest":
        fsql_pb.ActionCreatePreparedStatementRequest,
    f"{_TYPE_URL_PREFIX}ActionClosePreparedStatementRequest":
        fsql_pb.ActionClosePreparedStatementRequest,
}


def _decode_any(buf: bytes) -> Tuple[str, object]:
    """Decode an Any-wrapped Flight SQL command. Returns ``(type_url, message)``."""
    any_msg = PbAny()
    any_msg.ParseFromString(buf)
    msg_cls = _COMMAND_BY_TYPE_URL.get(any_msg.type_url)
    if msg_cls is None:
        raise fl.FlightServerError(
            f"Unknown Flight SQL command type_url: {any_msg.type_url!r}"
        )
    msg = msg_cls()
    msg.ParseFromString(any_msg.value)
    return any_msg.type_url, msg


def _pack_any(msg: object, type_url_suffix: str) -> bytes:
    """Wrap a message in an Any with the standard Flight SQL type_url prefix."""
    any_msg = PbAny()
    any_msg.type_url = f"{_TYPE_URL_PREFIX}{type_url_suffix}"
    any_msg.value = msg.SerializeToString()  # type: ignore[attr-defined]
    return any_msg.SerializeToString()


# --- result-set shapes for the catalog commands ------------------------------


def _empty_table(schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist([], schema=schema)


def _table_to_record_batch_stream(table: pa.Table) -> fl.RecordBatchStream:
    return fl.RecordBatchStream(table)


# Flight SQL fixed result-set schemas (from the Apache Arrow Flight SQL spec).

_SCHEMA_GET_CATALOGS = pa.schema([pa.field("catalog_name", pa.utf8())])

_SCHEMA_GET_DB_SCHEMAS = pa.schema([
    pa.field("catalog_name", pa.utf8()),
    pa.field("db_schema_name", pa.utf8()),
])

_SCHEMA_GET_TABLES = pa.schema([
    pa.field("catalog_name", pa.utf8()),
    pa.field("db_schema_name", pa.utf8()),
    pa.field("table_name", pa.utf8()),
    pa.field("table_type", pa.utf8()),
])

_SCHEMA_GET_TABLE_TYPES = pa.schema([pa.field("table_type", pa.utf8())])

_SCHEMA_GET_PRIMARY_KEYS = pa.schema([
    pa.field("catalog_name", pa.utf8()),
    pa.field("db_schema_name", pa.utf8()),
    pa.field("table_name", pa.utf8()),
    pa.field("column_name", pa.utf8()),
    pa.field("key_sequence", pa.int32()),
    pa.field("key_name", pa.utf8()),
])

_SCHEMA_GET_KEYS = pa.schema([
    pa.field("pk_catalog_name", pa.utf8()),
    pa.field("pk_db_schema_name", pa.utf8()),
    pa.field("pk_table_name", pa.utf8()),
    pa.field("pk_column_name", pa.utf8()),
    pa.field("fk_catalog_name", pa.utf8()),
    pa.field("fk_db_schema_name", pa.utf8()),
    pa.field("fk_table_name", pa.utf8()),
    pa.field("fk_column_name", pa.utf8()),
    pa.field("key_sequence", pa.int32()),
    pa.field("fk_key_name", pa.utf8()),
    pa.field("pk_key_name", pa.utf8()),
    pa.field("update_rule", pa.uint8()),
    pa.field("delete_rule", pa.uint8()),
])

_SCHEMA_GET_SQL_INFO = pa.schema([
    pa.field("info_name", pa.uint32()),
    pa.field("value", pa.utf8()),
])

_SCHEMA_GET_XDBC_TYPE_INFO = pa.schema([
    pa.field("type_name", pa.utf8()),
    pa.field("data_type", pa.int32()),
])


# --- the dependency-bearing handler container --------------------------------


class FlightHandlers:
    """Bundle of state the Flight SQL handlers need.

    Held by ``slayer.flight.server.FlightSqlServer`` once at startup; every
    RPC dispatch delegates here. The handler methods build a fresh
    ``FlightCatalog`` per call (§7.2 — no caching).
    """

    def __init__(
        self,
        *,
        engine: SlayerQueryEngine,
        storage: StorageBackend,
    ) -> None:
        self._engine = engine
        self._storage = storage

    # ----- helpers ----------------------------------------------------------

    def _build_catalog(self) -> FlightCatalog:
        models_by_ds = self._fetch_models_by_datasource()
        return build_catalog(models_by_datasource=models_by_ds)

    def _fetch_models_by_datasource(self) -> Dict[str, List[SlayerModel]]:
        async def fetch() -> Dict[str, List[SlayerModel]]:
            datasources = await self._storage.list_datasources()
            out: Dict[str, List[SlayerModel]] = defaultdict(list)
            for ds in datasources:
                model_names = await self._storage.list_models(data_source=ds)
                for name in model_names:
                    model = await self._storage.get_model(name=name, data_source=ds)
                    if model is not None:
                        out[ds].append(model)
            return dict(out)

        return run_sync(fetch())

    # ----- catalog commands -------------------------------------------------

    def handle_get_catalogs(self) -> pa.Table:
        return pa.Table.from_pylist(
            [{"catalog_name": CATALOG_NAME}], schema=_SCHEMA_GET_CATALOGS,
        )

    def handle_get_db_schemas(self, cmd: "fsql_pb.CommandGetDbSchemas") -> pa.Table:
        catalog = self._build_catalog()
        # The filter pattern fields are optional and rarely populated by the
        # Apache JDBC driver during introspection (Phase 1.0 capture shows
        # both bare and `%` filter values); Phase 1 ignores the filter and
        # returns every schema. Phase 2 can add LIKE-pattern filtering.
        rows = [
            {"catalog_name": CATALOG_NAME, "db_schema_name": sch.name}
            for sch in catalog.schemas
        ]
        return pa.Table.from_pylist(rows, schema=_SCHEMA_GET_DB_SCHEMAS)

    def handle_get_tables(self, cmd: "fsql_pb.CommandGetTables") -> pa.Table:
        catalog = self._build_catalog()
        rows = []
        for sch in catalog.schemas:
            for tbl in sch.tables:
                rows.append({
                    "catalog_name": CATALOG_NAME,
                    "db_schema_name": sch.name,
                    "table_name": tbl.name,
                    "table_type": tbl.table_type,
                })
        return pa.Table.from_pylist(rows, schema=_SCHEMA_GET_TABLES)

    def handle_get_table_types(self) -> pa.Table:
        return pa.Table.from_pylist(
            [{"table_type": t} for t in ("TABLE", "VIEW", "SEMANTIC_MODEL")],
            schema=_SCHEMA_GET_TABLE_TYPES,
        )

    # ----- stubbed (empty-but-well-typed) ----------------------------------

    def handle_get_primary_keys(self) -> pa.Table:
        return _empty_table(_SCHEMA_GET_PRIMARY_KEYS)

    def handle_get_exported_keys(self) -> pa.Table:
        return _empty_table(_SCHEMA_GET_KEYS)

    def handle_get_imported_keys(self) -> pa.Table:
        return _empty_table(_SCHEMA_GET_KEYS)

    def handle_get_cross_reference(self) -> pa.Table:
        return _empty_table(_SCHEMA_GET_KEYS)

    def handle_get_xdbc_type_info(self) -> pa.Table:
        rows = [
            {"type_name": "VARCHAR", "data_type": 12},
            {"type_name": "BIGINT", "data_type": -5},
            {"type_name": "DOUBLE", "data_type": 8},
            {"type_name": "BOOLEAN", "data_type": 16},
            {"type_name": "DATE", "data_type": 91},
            {"type_name": "TIMESTAMP", "data_type": 93},
        ]
        return pa.Table.from_pylist(rows, schema=_SCHEMA_GET_XDBC_TYPE_INFO)

    def handle_get_sql_info(self) -> pa.Table:
        import slayer as _slayer
        # SqlInfo enum values come straight from the FlightSql.proto spec.
        # We expose the minimum the spec recommends.
        rows = [
            {"info_name": int(fsql_pb.SqlInfo.FLIGHT_SQL_SERVER_NAME),
             "value": "SLayer"},
            {"info_name": int(fsql_pb.SqlInfo.FLIGHT_SQL_SERVER_VERSION),
             "value": _slayer.__version__},
            {"info_name": int(fsql_pb.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY),
             "value": "true"},
        ]
        return pa.Table.from_pylist(rows, schema=_SCHEMA_GET_SQL_INFO)

    # ----- SQL translation paths (CommandStatementQuery + prepared) --------

    def get_flight_info_for_sql(
        self,
        descriptor: fl.FlightDescriptor,
        sql: str,
    ) -> fl.FlightInfo:
        """Translate ``sql``, derive a wire schema, return FlightInfo whose
        Ticket re-encodes the same SQL bytes so ``do_get`` can re-execute.
        """
        result = self._translate(sql)
        if isinstance(result, NoOpResult):
            # Emit an empty Flight info for transaction/SET/SHOW so do_get
            # can stream an empty record batch.
            schema = pa.schema([])
            return self._build_flight_info(descriptor, schema, sql)
        if isinstance(result, (ProbeResult, InfoSchemaResult)):
            return self._build_flight_info(descriptor, result.table.schema, sql)
        if isinstance(result, QueryResult):
            schema = self._derive_query_schema(result)
            return self._build_flight_info(descriptor, schema, sql)
        raise fl.FlightServerError(
            f"Unexpected translator result: {type(result).__name__}"
        )

    def do_get_for_sql(self, sql: str) -> fl.FlightDataStream:
        """Execute ``sql`` and return the record-batch stream."""
        result = self._translate(sql)
        if isinstance(result, ProbeResult):
            return _table_to_record_batch_stream(result.table)
        if isinstance(result, InfoSchemaResult):
            return _table_to_record_batch_stream(result.table)
        if isinstance(result, NoOpResult):
            return _table_to_record_batch_stream(pa.Table.from_pylist([]))
        if isinstance(result, QueryResult):
            table = self._execute_full(result)
            return _table_to_record_batch_stream(table)
        raise fl.FlightServerError(
            f"Unexpected translator result: {type(result).__name__}"
        )

    def handle_create_prepared_statement(
        self, cmd: "fsql_pb.ActionCreatePreparedStatementRequest",
    ) -> bytes:
        """Translate ``cmd.query``, return a serialised ActionCreatePreparedStatementResult."""
        sql = cmd.query
        result = self._translate(sql)
        if isinstance(result, NoOpResult):
            schema = pa.schema([])
        elif isinstance(result, (ProbeResult, InfoSchemaResult)):
            schema = result.table.schema
        elif isinstance(result, QueryResult):
            schema = self._derive_query_schema(result)
        else:
            raise fl.FlightServerError(
                f"Unexpected translator result: {type(result).__name__}"
            )
        response = fsql_pb.ActionCreatePreparedStatementResult()
        response.prepared_statement_handle = sql.encode("utf-8")
        response.dataset_schema = self._serialise_schema(schema)
        # The Apache flight-sql-jdbc-driver expects the do_action response
        # body to be an ``Any``-wrapped message (per the Flight SQL spec).
        return _pack_any(response, "ActionCreatePreparedStatementResult")

    def handle_close_prepared_statement(
        self, cmd: "fsql_pb.ActionClosePreparedStatementRequest",
    ) -> None:
        """No-op: handles are stateless (UTF-8 SQL bytes; nothing to free)."""
        return None

    # ----- private helpers --------------------------------------------------

    def _translate(self, sql: str):
        catalog = self._build_catalog()
        return translate(sql, catalog)

    def _derive_query_schema(self, result: "QueryResult") -> pa.Schema:
        """Build the wire schema from catalog-declared projection types.

        Phase 1 uses the catalog's declared ``DataType`` for each
        projected item rather than the LIMIT-0 execution's runtime
        type. ``SlayerResponse.attributes`` does not yet expose the
        per-column Arrow type, so the engine-side LIMIT-0 we run for
        validation cannot drive the schema today. Phase 2 follow-up
        replaces this with a real LIMIT-0-derived schema.
        """
        # Eagerly run the LIMIT 0 so the engine validates the query
        # (caught here rather than at do_get for a clearer error path).
        zero_query = result.query.model_copy(update={"limit": 0})

        async def execute_zero():
            return await self._engine.execute(query=zero_query)

        run_sync(execute_zero())  # only the side-effect; ignore the empty rows.
        return self._build_schema(result)

    def _execute_full(self, result: "QueryResult") -> pa.Table:
        """Run the full query, coerce rows, return a pa.Table matching the
        catalog projection's column names."""

        async def execute_full():
            return await self._engine.execute(query=result.query)

        response = run_sync(execute_full())
        schema = self._build_schema(result)
        rows = [
            self._rewrite_row(row, result.column_name_mapping)
            for row in response.data
        ]
        return pa.Table.from_pylist(rows, schema=schema)

    @staticmethod
    def _build_schema(result: "QueryResult") -> pa.Schema:
        """Build a pa.Schema in projection order from catalog-declared types."""
        fields = []
        for (_, projected_name), dt in zip(
            result.column_name_mapping, result.projection_types,
        ):
            arrow_type = datatype_to_arrow(dt) if dt is not None else pa.utf8()
            fields.append(pa.field(projected_name, arrow_type))
        return pa.schema(fields)

    @staticmethod
    def _rewrite_row(
        row: dict, mapping: List[Tuple[str, str]],
    ) -> dict:
        """Rewrite an engine row's keys into projected names + coerce Decimals."""
        out: dict = {}
        for engine_alias, projected_name in mapping:
            value = row.get(engine_alias)
            if isinstance(value, decimal.Decimal):
                value = float(value)
            out[projected_name] = value
        return out

    @staticmethod
    def _serialise_schema(schema: pa.Schema) -> bytes:
        """Serialise a pa.Schema into Arrow IPC bytes (Flight SQL's
        dataset_schema wire format)."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema):
            pass
        return sink.getvalue().to_pybytes()

    @staticmethod
    def _build_flight_info(
        descriptor: fl.FlightDescriptor,
        schema: pa.Schema,
        sql: str,
    ) -> fl.FlightInfo:
        """Build a FlightInfo carrying ``schema`` and a TicketStatementQuery
        whose ``statement_handle`` is the original SQL bytes."""
        ticket_msg = fsql_pb.TicketStatementQuery()
        ticket_msg.statement_handle = sql.encode("utf-8")
        ticket_bytes = _pack_any(ticket_msg, "TicketStatementQuery")
        endpoints = [fl.FlightEndpoint(fl.Ticket(ticket_bytes), [])]
        return fl.FlightInfo(schema, descriptor, endpoints, -1, -1)


# --- top-level dispatch -------------------------------------------------------


def decode_command(buf: bytes) -> Tuple[str, object]:
    """Public re-export for tests / the server."""
    return _decode_any(buf)


def decode_ticket(buf: bytes) -> Tuple[str, object]:
    """Tickets are also Any-wrapped (TicketStatementQuery / CommandPreparedStatementQuery)."""
    return _decode_any(buf)


__all__ = [
    "FlightHandlers",
    "decode_command",
    "decode_ticket",
]
