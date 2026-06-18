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
    ResetSettingOp,
    SetSettingOp,
    TranslationError,
    translate,
)
from slayer.facade.catalog_sql import executor_for
from slayer.pg_facade import protocol as proto
from slayer.pg_facade.auth import verify_password
from slayer.pg_facade.identity import parameter_status_defaults, version_string
from slayer.pg_facade.probes import (
    SESSION_SETTING_SEED,
    SHOW_ALIASES,
    match_pg_probe_with_mutation,
)
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

# DEV-1569: GUC_REPORT-class settings. After a successful SET / set_config /
# RESET of one of these, the server pushes a ``ParameterStatus`` message so
# drivers (asyncpg, pgjdbc, c3p0, …) see the new value out-of-band. The
# lowercase key maps to the canonical Postgres wire-case name — real
# Postgres emits ``DateStyle`` / ``TimeZone`` / ``IntervalStyle`` in
# camel-case on the wire even though SQL identifiers are case-insensitive.
# ``integer_datetimes``, ``is_superuser``, ``in_hot_standby``,
# ``default_transaction_read_only`` are GUC_REPORT in real Postgres too but
# the facade doesn't expose them as settable, so we don't list them here.
_GUC_REPORT_NAMES: Dict[str, str] = {
    "application_name": "application_name",
    "client_encoding": "client_encoding",
    "datestyle": "DateStyle",
    "intervalstyle": "IntervalStyle",
    "server_encoding": "server_encoding",
    "server_version": "server_version",
    "session_authorization": "session_authorization",
    "standard_conforming_strings": "standard_conforming_strings",
    "timezone": "TimeZone",
}


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
        # DEV-1569: per-connection session-settings mailbox. Captures SET /
        # set_config writes; consulted by SHOW / current_setting reads. Seeded
        # from the module-level SESSION_SETTING_SEED via dict(...) so each
        # connection owns its own copy (never aliasing the seed).
        self._session_settings: Dict[str, str] = dict(SESSION_SETTING_SEED)
        # DEV-1569: when True, ``_describe_sql`` is in flight — translator
        # calls must remain pure. Suppresses application of any session-
        # setting mutation hints surfaced by the probe matcher during a
        # Describe.
        self._in_describe = False

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
            param_oids = _resolve_param_oids(stmt)
            self._writer.write(proto.encode_parameter_description(param_oids))
            self._describe_sql(
                stmt.sql, result_formats=None, param_oids=param_oids,
            )
        else:
            portal = self._portals.get(msg.name)
            if portal is None:
                await self._extended_error(
                    code=proto.SQLSTATE_INTERNAL_ERROR,
                    message=f"portal {msg.name!r} does not exist",
                )
                return
            # Portal-describe: the bound values have already been
            # substituted into portal.sql by _handle_bind, so no $N
            # remain to typed-sentinel.
            self._describe_sql(
                portal.sql, result_formats=portal.result_format_codes,
            )

    def _describe_sql(
        self, sql: str, *, result_formats: Optional[List[int]],
        param_oids: Optional[List[int]] = None,
    ) -> None:
        # DEV-1558 fix: the catalog executor's Describe path runs the SQL
        # against DuckDB to obtain the cursor's column description. When the
        # prepared-statement form still has ``$N`` placeholders (asyncpg
        # sends Parse + Describe-Statement BEFORE Bind), DuckDB raises a
        # bind-parameter error. Substitute each ``$N`` with a TYPED
        # sentinel literal derived from the parameter's declared OID so
        # the resulting RowDescription advertises the correct projection
        # types even when ``$N`` appears in the projection itself. The
        # real value substitution still happens in ``_handle_bind`` for
        # Execute (Codex round 13 review).
        describe_sql = _substitute_typed_sentinels(sql, param_oids or [])
        # DEV-1569: ``_in_describe`` suppresses application of any
        # session-setting mutations surfaced by the translator during a
        # Describe pass. The Execute path applies them.
        self._in_describe = True
        try:
            try:
                result = self._translate(describe_sql)
            except TranslationError:
                # Describe must not raise to the wire here; the subsequent
                # Execute surfaces the error. Report NoData so the client
                # can proceed.
                self._writer.write(proto.encode_no_data())
                return
        finally:
            self._in_describe = False
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
            # Pass a lazy factory so the DuckDB executor is only
            # materialised when ``is_catalog_only(parsed)`` is True
            # (Codex round 16). Non-catalog model queries skip the
            # construction cost entirely.
            catalog_sql_executor=lambda: executor_for(self._catalog, self._datasource),
        )

    def _probe_matcher(self, parsed: exp.Expression):
        """Wraps the PG-facade and shared probe matchers.

        DEV-1569: ``SHOW`` / ``current_setting`` consult the per-connection
        ``self._session_settings``. For ``set_config`` matches, the returned
        ``ProbeMatcherOutcome`` carries a mutation hint that
        ``_run_statement`` applies on Execute (Describe leaves it pending).
        """
        assert self._datasource is not None
        pg = match_pg_probe_with_mutation(
            parsed, datasource=self._datasource, version_str=version_string(),
            session_settings=self._session_settings,
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
            # DEV-1569: set_config(...) mutation hint surfaces on ProbeResult.
            # Apply ONLY in the Execute path (not Describe). Pushes
            # ParameterStatus for reportable settings after CommandComplete.
            if isinstance(result, ProbeResult) and result.settings_mutation is not None:
                self._apply_set_setting(result.settings_mutation)
            return True
        if isinstance(result, NoOpResult):
            self._apply_tx_command(result.command_tag)
            self._writer.write(proto.encode_command_complete(_command_tag(result.command_tag)))
            # DEV-1569: apply SET / RESET captures AFTER CommandComplete so
            # the post-SET ParameterStatus push lands between CC and
            # ReadyForQuery (any-time ordering OK per PG protocol). Skipped
            # during Describe per _describe_sql / _in_describe.
            if result.set_setting is not None:
                self._apply_set_setting(result.set_setting)
            if result.reset_setting is not None:
                self._apply_reset_setting(result.reset_setting)
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
        # The catalog SQL executor stashes a position-aware key list on
        # the batch so duplicate output column names (Postgres allows
        # ``SELECT oid AS x, relname AS x``) don't collapse to a single
        # dict entry. Fall back to ``col.name`` for batches built by the
        # canned probe / info-schema paths where duplicates can't arise.
        row_keys = getattr(batch, "_row_keys", None) or [c.name for c in batch.columns]
        for row in batch.rows:
            values = [
                _encode_value(row.get(row_keys[i]), datatype_to_oid(batch.columns[i].type), formats[i])
                for i in range(len(batch.columns))
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

    # ----- DEV-1569: per-connection session-settings application -----------

    def _apply_set_setting(self, op: SetSettingOp) -> None:
        """Mutate the per-connection session-settings map and (for reportable
        names) push a ``ParameterStatus`` message to the client.

        Skipped during ``_describe_sql`` (Describe must remain pure — the
        same prepared statement may be Executed later, at which point
        mutation happens).
        """
        if self._in_describe:
            return
        self._session_settings[op.name] = op.value
        self._push_parameter_status_if_reportable(op.name, op.value)

    def _apply_reset_setting(self, op: ResetSettingOp) -> None:
        """Restore the per-connection session-settings map per the RESET
        intent. Pushes ``ParameterStatus`` for each reportable name whose
        value changed back to seed.

        DEV-1569 / Codex F2: multi-word names (``RESET TIME ZONE``,
        ``RESET SESSION AUTHORIZATION``) are alias-resolved via the same
        ``SHOW_ALIASES`` table that ``SHOW`` consults; without the
        resolution the lookup against ``SESSION_SETTING_SEED`` would
        silently miss.
        """
        if self._in_describe:
            return
        if op.reset_all:
            # Restore every name to its seed value; push ParameterStatus for
            # the seeded value of every reportable name (drivers latch onto
            # the post-RESET-ALL pushes to invalidate caches).
            self._session_settings = dict(SESSION_SETTING_SEED)
            for lower, _wire_name in _GUC_REPORT_NAMES.items():
                value = self._session_settings.get(lower, "")
                self._push_parameter_status_if_reportable(lower, value)
            return
        # RESET <name>: alias-resolve (multi-word names), then revert to
        # seed (if seeded) or drop the override.
        name = SHOW_ALIASES.get(op.name or "", op.name or "")
        if name in SESSION_SETTING_SEED:
            self._session_settings[name] = SESSION_SETTING_SEED[name]
            self._push_parameter_status_if_reportable(
                name, self._session_settings[name],
            )
        else:
            self._session_settings.pop(name, None)
            # Non-seeded names are by definition not reportable (the
            # _GUC_REPORT_NAMES set is a subset of the seed), so no push.

    def _push_parameter_status_if_reportable(self, name: str, value: str) -> None:
        wire_name = _GUC_REPORT_NAMES.get(name)
        if wire_name is None:
            return
        self._writer.write(proto.encode_parameter_status(wire_name, value))

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


# Typed sentinel literal per parameter OID. Used by Describe-Statement to
# substitute ``$N`` placeholders BEFORE Bind so DuckDB can produce a
# valid RowDescription whose column types reflect the projection's
# dependence on the parameter (Codex round 13).
#
# Each sentinel is a typed NULL (``CAST(NULL AS <type>)``) rather than a
# concrete literal: NULL is universally comparable (``col = CAST(NULL AS
# TEXT)`` always returns NULL/FALSE under DuckDB's standard SQL
# semantics), so we never trigger a conversion error like
# ``Conversion Error: Could not convert string '' to INT64`` when the
# parameter appears in a comparison against a column of a different
# type than the pgjdbc-declared OID — Metabase corpus #9 had pgjdbc
# declaring text OIDs for parameters that compared against int columns
# (``objsubid = $N``), which the literal ``''`` sentinel turned into
# an unanswerable text-vs-int comparison.
_TYPED_SENTINEL_BY_OID: Dict[int, str] = {
    proto.OID_TEXT: "CAST(NULL AS VARCHAR)",
    proto.OID_INT8: "CAST(NULL AS BIGINT)",
    proto.OID_FLOAT8: "CAST(NULL AS DOUBLE)",
    proto.OID_BOOL: "CAST(NULL AS BOOLEAN)",
    proto.OID_DATE: "CAST(NULL AS DATE)",
    proto.OID_TIMESTAMP: "CAST(NULL AS TIMESTAMP)",
}


def _substitute_typed_sentinels(sql: str, param_oids: List[int]) -> str:
    """Replace each ``$N`` placeholder with a typed sentinel literal
    derived from ``param_oids[N-1]``. Falls back to bare ``NULL`` when
    the OID is unknown (e.g. extra placeholders past the declared list)
    so DuckDB picks the most permissive coercion path."""
    def repl(match):
        idx = int(match.group(1)) - 1
        if 0 <= idx < len(param_oids):
            return _TYPED_SENTINEL_BY_OID.get(param_oids[idx], "NULL")
        return "NULL"
    return _PARAM_PLACEHOLDER.sub(repl, sql)


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
    return value_to_text(value, oid)
