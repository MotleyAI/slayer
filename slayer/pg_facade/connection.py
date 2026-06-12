"""Per-connection Postgres-protocol state machine (DEV-1486).

One :class:`PgConnection` per accepted TCP connection. ``run()`` drives the
session: startup (SSLRequest / CancelRequest / protocol v3), cleartext-password
auth, datasource resolution from the ``database`` startup parameter, the
ParameterStatus burst, then the simple- and extended-query message loops.

Read-only: incoming SQL is translated via the shared ``slayer.facade``
translator (with the Postgres dialect, the datasource-aware probe matcher, and
the ``pg_catalog`` matcher injected) and executed through the engine. DML/DDL
is rejected. A per-connection transaction-status flag (``I``/``T``/``E``) is
reported on every ReadyForQuery.
"""

from __future__ import annotations

import asyncio
import logging
import re
import struct
from typing import Dict, List, Optional

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp
from pydantic import BaseModel, ConfigDict

from slayer.core.models import SlayerModel
from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.facade.probe_queries import match_probe as facade_match_probe
from slayer.facade.rows import RowBatch
from slayer.facade.translator import (
    InfoSchemaResult,
    NoOpResult,
    PgCatalogResult,
    ProbeResult,
    QueryResult,
    READ_ONLY_MESSAGE,
    TranslationError,
    translate,
)
from slayer.pg_facade import protocol as proto
from slayer.pg_facade.auth import verify_password
from slayer.pg_facade.identity import parameter_status_defaults, version_string
from slayer.pg_facade.pg_catalog import match_pg_catalog
from slayer.pg_facade.probes import match_pg_probe
from slayer.pg_facade.types import (
    datatype_to_oid,
    literal_for_substitution,
    value_from_binary,
    value_from_text,
    value_to_binary,
    value_to_text,
)

logger = logging.getLogger(__name__)

_BACKEND_PID = 1
_BACKEND_SECRET = 0
_PARAM_PLACEHOLDER = re.compile(r"\$(\d+)")
# The single schema the facade advertises (matches pg_namespace / current_schema).
PUBLIC_SCHEMA = "public"


class _PreparedStatement(BaseModel):
    sql: str
    parameter_oids: List[int]


class _Portal(BaseModel):
    sql: str
    result_format_codes: List[int]


class _Done(Exception):
    """Internal signal to end the session cleanly (Terminate / EOF)."""


class PgConnection:
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        engine,
        storage,
        token: Optional[str],
        tls_ctx=None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._engine = engine
        self._storage = storage
        self._token = token
        self._tls_ctx = tls_ctx
        self._tx_state: bytes = proto.TX_IDLE
        self._datasource: Optional[str] = None
        self._catalog: Optional[FacadeCatalog] = None
        self._statements: Dict[str, _PreparedStatement] = {}
        self._portals: Dict[str, _Portal] = {}
        # Extended protocol: after an error the backend discards every message
        # until the next Sync, then resumes with ReadyForQuery.
        self._skip_until_sync = False

    # ----- lifecycle --------------------------------------------------------

    async def run(self) -> None:
        try:
            startup = await self._handle_startup()
            if startup is None:
                return
            if not await self._authenticate():
                return
            if not await self._resolve_datasource(startup.parameters.get("database")):
                return
            self._catalog = await self._build_catalog()
            await self._send_startup_complete()
            await self._main_loop()
        except _Done:
            return
        except (asyncio.IncompleteReadError, ConnectionResetError):
            return

    # ----- startup ----------------------------------------------------------

    async def _read_startup_frame(self) -> Optional[bytes]:
        """Read a startup-style frame (no type byte). Returns the body (starting
        with the 4-byte code) or ``None`` on EOF / malformed length."""
        try:
            header = await self._reader.readexactly(4)
        except asyncio.IncompleteReadError:
            return None
        (length,) = struct.unpack(">i", header)
        # A startup frame must carry at least the 4-byte length + 4-byte code.
        if length < 8:
            return None
        try:
            return await self._reader.readexactly(length - 4)
        except asyncio.IncompleteReadError:
            return None

    async def _handle_startup(self) -> Optional[proto.StartupMessage]:
        while True:
            body = await self._read_startup_frame()
            if body is None:
                return None
            (code,) = struct.unpack_from(">i", body, 0)
            if code == proto.SSL_REQUEST_CODE or code == proto.GSSENC_REQUEST_CODE:
                if code == proto.SSL_REQUEST_CODE and self._tls_ctx is not None:
                    self._writer.write(b"S")
                    await self._flush()
                    await self._perform_tls_upgrade()
                else:
                    self._writer.write(b"N")
                    await self._flush()
                continue
            if code == proto.CANCEL_REQUEST_CODE:
                # Stateless server — nothing to cancel. Close.
                return None
            if code != proto.PROTOCOL_VERSION_3:
                await self._send_error(
                    code=proto.SQLSTATE_FEATURE_NOT_SUPPORTED,
                    message=f"unsupported protocol version {code}",
                    severity="FATAL",
                )
                return None
            return proto.decode_startup(body)

    async def _perform_tls_upgrade(self) -> None:
        """Upgrade the plaintext transport to TLS (best-effort).

        Real TLS is exercised by integration testing; unit tests monkeypatch
        this. asyncio's ``start_tls`` requires the running loop + transport.
        """
        loop = asyncio.get_running_loop()
        transport = self._writer.transport
        protocol_obj = transport.get_protocol()
        new_transport = await loop.start_tls(
            transport, protocol_obj, self._tls_ctx, server_side=True,
        )
        protocol_obj._stream_reader._transport = new_transport  # type: ignore[attr-defined]
        self._writer._transport = new_transport  # type: ignore[attr-defined]

    # ----- auth -------------------------------------------------------------

    async def _authenticate(self) -> bool:
        if self._token is None:
            self._writer.write(proto.encode_authentication_ok())
            await self._flush()
            return True
        self._writer.write(proto.encode_authentication_cleartext_password())
        await self._flush()
        msg = await self._read_message()
        if msg is None:
            return False
        type_char, body = msg
        if type_char != "p":
            await self._send_error(
                code=proto.SQLSTATE_INVALID_AUTHORIZATION,
                message="expected password message",
                severity="FATAL",
            )
            return False
        password = proto.decode_password(body)
        if not verify_password(password, self._token):
            await self._send_error(
                code=proto.SQLSTATE_INVALID_PASSWORD,
                message="password authentication failed",
                severity="FATAL",
            )
            return False
        self._writer.write(proto.encode_authentication_ok())
        await self._flush()
        return True

    # ----- datasource resolution -------------------------------------------

    async def _resolve_datasource(self, database: Optional[str]) -> bool:
        datasources = await self._storage.list_datasources()
        if database and database in datasources:
            self._datasource = database
            return True
        name = database if database else "(none)"
        await self._send_error(
            code=proto.SQLSTATE_UNDEFINED_DATABASE,
            message=f'database "{name}" does not exist',
            severity="FATAL",
        )
        return False

    async def _build_catalog(self) -> FacadeCatalog:
        assert self._datasource is not None
        models: List[SlayerModel] = []
        names = await self._storage.list_models(data_source=self._datasource)
        for name in names:
            model = await self._storage.get_model(name=name, data_source=self._datasource)
            if model is not None:
                models.append(model)
        # The Postgres facade advertises a single schema `public` (matching
        # pg_namespace / current_schema()), so the catalog's schema is named
        # `public` — this keeps qualified `public.<table>` resolution working.
        # The real datasource is carried separately (self._datasource) and
        # passed to the engine as the execution hint.
        return build_catalog(models_by_datasource={PUBLIC_SCHEMA: models})

    async def _send_startup_complete(self) -> None:
        for name, value in parameter_status_defaults():
            self._writer.write(proto.encode_parameter_status(name, value))
        self._writer.write(proto.encode_backend_key_data(_BACKEND_PID, _BACKEND_SECRET))
        self._writer.write(proto.encode_ready_for_query(self._tx_state))
        await self._flush()

    # ----- main message loop ------------------------------------------------

    async def _main_loop(self) -> None:
        while True:
            msg = await self._read_message()
            if msg is None:
                return
            type_char, body = msg
            # In skip-until-Sync mode (after an extended-query error) discard
            # everything until a Sync (or Terminate) resynchronises the stream.
            if self._skip_until_sync and type_char not in ("S", "X"):
                continue
            try:
                await self._dispatch_message(type_char, body)
            except _Done:
                raise
            except (struct.error, ValueError, IndexError) as exc:  # UnicodeDecodeError ⊂ ValueError
                # Malformed frontend message body — report a protocol violation
                # but keep the session alive.
                await self._send_error(
                    code=proto.SQLSTATE_PROTOCOL_VIOLATION,
                    message=f"malformed {type_char!r} message: {exc}",
                )
                self._fail_tx()
                if type_char == "Q":
                    await self._send_ready()  # simple-query error recovery
                else:
                    self._skip_until_sync = True  # extended-query error recovery

    async def _dispatch_message(self, type_char: str, body: bytes) -> None:
        if type_char == "Q":
            await self._handle_simple_query(proto.decode_query(body))
        elif type_char == "P":
            self._handle_parse(proto.decode_parse(body))
        elif type_char == "B":
            await self._handle_bind(proto.decode_bind(body))
        elif type_char == "D":
            await self._handle_describe(proto.decode_describe(body))
        elif type_char == "E":
            await self._handle_execute(proto.decode_execute(body))
        elif type_char == "S":
            await self._handle_sync()
        elif type_char == "C":
            self._handle_close(proto.decode_close(body))
        elif type_char == "H":
            await self._flush()
        elif type_char == "X":
            raise _Done()
        elif type_char in ("F", "d", "c", "f"):
            await self._extended_error(
                code=proto.SQLSTATE_FEATURE_NOT_SUPPORTED,
                message=f"message type {type_char!r} is not supported",
            )
        else:
            await self._extended_error(
                code=proto.SQLSTATE_FEATURE_NOT_SUPPORTED,
                message=f"unknown message type {type_char!r}",
            )

    async def _read_message(self):
        try:
            type_byte = await self._reader.readexactly(1)
            header = await self._reader.readexactly(4)
        except asyncio.IncompleteReadError:
            return None
        (length,) = struct.unpack(">i", header)
        if length < 4:
            return None  # malformed frame length — close.
        try:
            body = await self._reader.readexactly(length - 4)
        except asyncio.IncompleteReadError:
            return None
        return type_byte.decode("ascii"), body

    # ----- simple query -----------------------------------------------------

    async def _handle_simple_query(self, sql: str) -> None:
        try:
            statements = [s for s in sqlglot.parse(sql, dialect="postgres") if s is not None]
        except sqlglot.errors.ParseError as exc:
            await self._send_error(
                code=proto.SQLSTATE_SYNTAX_ERROR, message=f"SQL parse error: {exc}",
            )
            self._fail_tx()
            await self._send_ready()
            return
        if not statements:
            self._writer.write(proto.encode_empty_query_response())
            await self._send_ready()
            return
        for stmt in statements:
            if self._tx_state == proto.TX_FAILED and not _is_tx_end(stmt):
                await self._send_error(
                    code=proto.SQLSTATE_IN_FAILED_SQL_TRANSACTION,
                    message="current transaction is aborted, commands ignored "
                            "until end of transaction block",
                )
                break
            ok = await self._run_statement(
                stmt.sql(dialect="postgres"), result_formats=None, send_row_description=True,
            )
            if not ok:
                break
        await self._send_ready()

    # ----- extended query ---------------------------------------------------

    def _handle_parse(self, msg: proto.ParseMessage) -> None:
        self._statements[msg.name] = _PreparedStatement(
            sql=msg.query, parameter_oids=list(msg.parameter_oids),
        )
        self._writer.write(proto.encode_parse_complete())

    async def _handle_bind(self, msg: proto.BindMessage) -> None:
        stmt = self._statements.get(msg.statement)
        if stmt is None:
            await self._extended_error(
                code=proto.SQLSTATE_INTERNAL_ERROR,
                message=f"prepared statement {msg.statement!r} does not exist",
            )
            return
        try:
            proto.validate_format_codes(msg.parameter_format_codes)
            proto.validate_format_codes(msg.result_format_codes)
            substituted = self._substitute_params(stmt, msg)
        except (ValueError, struct.error) as exc:
            await self._extended_error(
                code=proto.SQLSTATE_FEATURE_NOT_SUPPORTED,
                message=f"could not bind parameter: {exc}",
            )
            return
        self._portals[msg.portal] = _Portal(
            sql=substituted, result_format_codes=list(msg.result_format_codes),
        )
        self._writer.write(proto.encode_bind_complete())

    def _substitute_params(self, stmt: _PreparedStatement, bind: proto.BindMessage) -> str:
        resolved = _resolve_param_oids(stmt)
        n = len(bind.parameter_values)
        # The client must supply exactly as many parameters as the statement
        # declares; otherwise a `$N` would be left unbound (or an extra value
        # silently dropped), which is a protocol error.
        if n != len(resolved):
            raise ValueError(
                f"bind supplied {n} parameter(s) but statement expects {len(resolved)}"
            )
        if not bind.parameter_values:
            return stmt.sql
        formats = proto.parse_result_format_codes(bind.parameter_format_codes, n)
        oids = list(resolved)
        literals: List[str] = []
        for raw, fmt, oid in zip(bind.parameter_values, formats, oids):
            if raw is None:
                literals.append("NULL")
                continue
            value = (
                value_from_text(raw, oid) if fmt == proto.FORMAT_TEXT
                else value_from_binary(raw, oid)
            )
            literals.append(literal_for_substitution(value))

        def repl(match: "re.Match[str]") -> str:
            idx = int(match.group(1))
            if 1 <= idx <= len(literals):
                return literals[idx - 1]
            return match.group(0)

        return _PARAM_PLACEHOLDER.sub(repl, stmt.sql)

    async def _handle_describe(self, msg: proto.DescribeMessage) -> None:
        if msg.kind == "S":
            stmt = self._statements.get(msg.name)
            if stmt is None:
                await self._extended_error(
                    code=proto.SQLSTATE_INTERNAL_ERROR,
                    message=f"prepared statement {msg.name!r} does not exist",
                )
                return
            self._writer.write(proto.encode_parameter_description(_resolve_param_oids(stmt)))
            self._describe_sql(stmt.sql, result_formats=None)
        else:
            portal = self._portals.get(msg.name)
            if portal is None:
                await self._extended_error(
                    code=proto.SQLSTATE_INTERNAL_ERROR,
                    message=f"portal {msg.name!r} does not exist",
                )
                return
            self._describe_sql(portal.sql, result_formats=portal.result_format_codes)

    def _describe_sql(self, sql: str, *, result_formats: Optional[List[int]]) -> None:
        try:
            result = self._translate(sql)
        except TranslationError:
            # Describe must not raise to the wire here; the subsequent Execute
            # surfaces the error. Report NoData so the client can proceed.
            self._writer.write(proto.encode_no_data())
            return
        fields = self._fields_for_result(result, result_formats)
        if fields is None:
            self._writer.write(proto.encode_no_data())
        else:
            self._writer.write(proto.encode_row_description(fields))

    async def _handle_execute(self, msg: proto.ExecuteMessage) -> None:
        portal = self._portals.get(msg.portal)
        if portal is None:
            await self._extended_error(
                code=proto.SQLSTATE_INTERNAL_ERROR,
                message=f"portal {msg.portal!r} does not exist",
            )
            return
        # Honour the failed-transaction state for the extended path too: only
        # COMMIT / ROLLBACK / END are accepted until the block ends.
        if self._tx_state == proto.TX_FAILED and not self._portal_is_tx_end(portal.sql):
            await self._extended_error(
                code=proto.SQLSTATE_IN_FAILED_SQL_TRANSACTION,
                message="current transaction is aborted, commands ignored "
                        "until end of transaction block",
            )
            return
        ok = await self._run_statement(
            portal.sql,
            result_formats=portal.result_format_codes,
            send_row_description=False,
        )
        if not ok:
            # _run_statement already sent the error; resync until Sync.
            self._skip_until_sync = True

    @staticmethod
    def _portal_is_tx_end(sql: str) -> bool:
        try:
            parsed = sqlglot.parse_one(sql, dialect="postgres")
        except sqlglot.errors.ParseError:
            return False
        return _is_tx_end(parsed)

    async def _handle_sync(self) -> None:
        self._skip_until_sync = False
        await self._send_ready()

    async def _extended_error(self, *, code: str, message: str) -> None:
        """Emit an error during an extended-query message and enter
        skip-until-Sync mode (per the PG extended-protocol error rule)."""
        await self._send_error(code=code, message=message)
        self._fail_tx()
        self._skip_until_sync = True

    def _handle_close(self, msg: proto.CloseMessage) -> None:
        if msg.kind == "S":
            self._statements.pop(msg.name, None)
        else:
            self._portals.pop(msg.name, None)
        self._writer.write(proto.encode_close_complete())

    # ----- statement execution ----------------------------------------------

    def _translate(self, sql: str):
        return translate(
            sql,
            self._catalog,
            dialect="postgres",
            probe_matcher=self._probe_matcher,
            catalog_matchers=[match_pg_catalog],
        )

    def _probe_matcher(self, parsed: exp.Expression) -> Optional[RowBatch]:
        assert self._datasource is not None
        pg = match_pg_probe(
            parsed, datasource=self._datasource, version_str=version_string(),
        )
        if pg is not None:
            return pg
        return facade_match_probe(parsed)

    async def _run_statement(
        self, sql: str, *, result_formats: Optional[List[int]], send_row_description: bool,
    ) -> bool:
        """Translate + respond. Returns False if an error was sent."""
        try:
            result = self._translate(sql)
        except TranslationError as exc:
            # Clients (BI pools especially) often swallow the statement that
            # failed; log it server-side so unsupported-SQL gaps are visible.
            logger.warning("pg facade: cannot translate %r: %s", sql, exc)
            await self._send_error(code=_sqlstate_for(exc), message=str(exc))
            self._fail_tx()
            return False

        if isinstance(result, (ProbeResult, InfoSchemaResult, PgCatalogResult)):
            self._emit_row_batch(result.batch, result_formats, send_row_description)
            return True
        if isinstance(result, NoOpResult):
            self._apply_tx_command(result.command_tag)
            self._writer.write(proto.encode_command_complete(_command_tag(result.command_tag)))
            return True
        if isinstance(result, QueryResult):
            return await self._run_query(result, result_formats, send_row_description)
        await self._send_error(
            code=proto.SQLSTATE_INTERNAL_ERROR,
            message=f"unexpected translator result {type(result).__name__}",
        )
        self._fail_tx()
        return False

    def _emit_row_batch(
        self, batch: RowBatch, result_formats: Optional[List[int]], send_row_description: bool,
    ) -> None:
        formats = proto.parse_result_format_codes(result_formats or [], len(batch.columns))
        if send_row_description:
            fields = [
                proto.FieldDescription(
                    name=col.name,
                    type_oid=datatype_to_oid(col.type),
                    format_code=formats[i],
                )
                for i, col in enumerate(batch.columns)
            ]
            self._writer.write(proto.encode_row_description(fields))
        for row in batch.rows:
            values = [
                _encode_value(row.get(col.name), datatype_to_oid(col.type), formats[i])
                for i, col in enumerate(batch.columns)
            ]
            self._writer.write(proto.encode_data_row(values))
        self._writer.write(proto.encode_command_complete(f"SELECT {len(batch.rows)}"))

    async def _run_query(
        self, result: QueryResult, result_formats: Optional[List[int]], send_row_description: bool,
    ) -> bool:
        try:
            response = await self._engine.execute(
                query=result.query, data_source=self._datasource,
            )
        except Exception as exc:  # noqa: BLE001 — surface any engine error to the client
            await self._send_error(code=proto.SQLSTATE_INTERNAL_ERROR, message=str(exc))
            self._fail_tx()
            return False
        mapping = result.column_name_mapping
        types = result.projection_types
        formats = proto.parse_result_format_codes(result_formats or [], len(mapping))
        if send_row_description:
            fields = [
                proto.FieldDescription(
                    name=projected,
                    type_oid=datatype_to_oid(types[i]),
                    format_code=formats[i],
                )
                for i, (_engine_alias, projected) in enumerate(mapping)
            ]
            self._writer.write(proto.encode_row_description(fields))
        for row in response.data:
            values = [
                _encode_value(row.get(engine_alias), datatype_to_oid(types[i]), formats[i])
                for i, (engine_alias, _projected) in enumerate(mapping)
            ]
            self._writer.write(proto.encode_data_row(values))
        self._writer.write(proto.encode_command_complete(f"SELECT {len(response.data)}"))
        return True

    def _fields_for_result(
        self, result, result_formats: Optional[List[int]],
    ) -> Optional[List[proto.FieldDescription]]:
        if isinstance(result, (ProbeResult, InfoSchemaResult, PgCatalogResult)):
            cols = result.batch.columns
            formats = proto.parse_result_format_codes(result_formats or [], len(cols))
            return [
                proto.FieldDescription(
                    name=c.name, type_oid=datatype_to_oid(c.type), format_code=formats[i],
                )
                for i, c in enumerate(cols)
            ]
        if isinstance(result, QueryResult):
            mapping = result.column_name_mapping
            types = result.projection_types
            formats = proto.parse_result_format_codes(result_formats or [], len(mapping))
            return [
                proto.FieldDescription(
                    name=projected, type_oid=datatype_to_oid(types[i]), format_code=formats[i],
                )
                for i, (_alias, projected) in enumerate(mapping)
            ]
        return None  # NoOp → NoData

    # ----- transaction state -------------------------------------------------

    def _apply_tx_command(self, command_tag: Optional[str]) -> None:
        if command_tag in ("BEGIN", "START TRANSACTION"):
            self._tx_state = proto.TX_IN_TRANSACTION
        elif command_tag in ("COMMIT", "ROLLBACK", "END"):
            self._tx_state = proto.TX_IDLE

    def _fail_tx(self) -> None:
        if self._tx_state == proto.TX_IN_TRANSACTION:
            self._tx_state = proto.TX_FAILED

    # ----- IO helpers --------------------------------------------------------

    async def _send_ready(self) -> None:
        self._writer.write(proto.encode_ready_for_query(self._tx_state))
        await self._flush()

    async def _send_error(self, *, code: str, message: str, severity: str = "ERROR") -> None:
        self._writer.write(
            proto.encode_error_response(code=code, message=message, severity=severity)
        )
        await self._flush()

    async def _flush(self) -> None:
        await self._writer.drain()


# --- module-level helpers ----------------------------------------------------


def _resolve_param_oids(stmt: _PreparedStatement) -> List[int]:
    """The parameter OIDs to report in ParameterDescription.

    asyncpg leaves ``Parse`` parameter OIDs empty and relies on the server to
    report how many parameters the query has. We infer the count from the
    highest ``$N`` placeholder, using the declared OID where present (else
    text, so the value arrives text-encoded and is trivially substitutable).
    """
    declared = stmt.parameter_oids
    max_idx = max(
        (int(m.group(1)) for m in _PARAM_PLACEHOLDER.finditer(stmt.sql)),
        default=0,
    )
    count = max(len(declared), max_idx)
    return [
        declared[i] if i < len(declared) and declared[i] else proto.OID_TEXT
        for i in range(count)
    ]


def _is_tx_end(stmt: exp.Expression) -> bool:
    if isinstance(stmt, (exp.Commit, exp.Rollback)):
        return True
    if isinstance(stmt, exp.Command) and str(stmt.this).upper() == "END":
        return True
    return False


def _command_tag(command_tag: Optional[str]) -> str:
    if command_tag in ("BEGIN", "START TRANSACTION"):
        return "BEGIN"
    if command_tag is None:
        return "SELECT 0"
    return command_tag


def _sqlstate_for(exc: TranslationError) -> str:
    msg = str(exc)
    if READ_ONLY_MESSAGE in msg:
        return proto.SQLSTATE_READ_ONLY_SQL_TRANSACTION
    if "parse error" in msg.lower():
        return proto.SQLSTATE_SYNTAX_ERROR
    if "Unknown table" in msg or "Unknown schema" in msg or "Unknown catalog" in msg:
        return proto.SQLSTATE_UNDEFINED_TABLE
    return proto.SQLSTATE_FEATURE_NOT_SUPPORTED


def _encode_value(value, oid: int, fmt: int) -> Optional[bytes]:
    if fmt == proto.FORMAT_BINARY:
        return value_to_binary(value, oid)
    return value_to_text(value)
