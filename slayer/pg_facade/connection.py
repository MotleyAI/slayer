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
import time
from collections.abc import Awaitable, Callable, Iterable

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp
from pydantic import BaseModel, ConfigDict
from sqlglot.optimizer.scope import traverse_scope

from slayer.core.enums import DataType
from slayer.core.models import SlayerModel
from slayer.engine import timing
from slayer.facade.catalog import (
    FacadeCatalog,
    build_catalog_grouped_by_schema,
)
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
from slayer.facade.catalog_sql import build_catalog_relations, executor_for
from slayer.pg_facade import protocol as proto
from slayer.pg_facade.auth import Authenticator, StaticTokenAuthenticator
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

# Strips characteristics off a statement-initial ``BEGIN`` / ``START
# TRANSACTION`` (``READ ONLY``, ``ISOLATION LEVEL …``, ``DEFERRABLE`` …) so the
# sqlglot-based simple-query splitter — which rejects those forms — can still
# parse the statement list. Anchored to statement start (^ or after ``;``) so a
# ``begin`` column reference elsewhere is never touched. The stripped statement
# stays a plain transaction-open, handled as a no-op downstream (DEV-1594).
_TX_OPEN_STRIP_RE = re.compile(
    r"(?P<lead>^|;)(?P<ws>\s*)"
    r"(?P<verb>BEGIN(?:\s+WORK|\s+TRANSACTION)?|START\s+TRANSACTION)\b[^;]*",
    re.IGNORECASE,
)


def _strip_tx_open_characteristics(sql: str) -> str:
    return _TX_OPEN_STRIP_RE.sub(
        lambda m: f"{m.group('lead')}{m.group('ws')}{m.group('verb')}", sql
    )
# The default schema the facade advertises (matches pg_namespace /
# current_schema). Datasources without an explicit ``postgres_schema`` land here.
PUBLIC_SCHEMA = "public"

# Fallback logical-database name (``current_database()`` / ``table_catalog``)
# when the client sends no ``database`` startup parameter.
DEFAULT_DATABASE = "slayer"

# Per-connection scoping seams: resolve a storage from the authenticated
# principal, and an engine from that storage.
StorageProvider = Callable[[object], Awaitable[object]]
EngineFactory = Callable[[object], object]


def _default_engine_factory(storage: object) -> object:
    from slayer.engine.query_engine import SlayerQueryEngine

    return SlayerQueryEngine(storage=storage)

# DEV-1569: GUC_REPORT-class settings. After a successful SET / set_config /
# RESET of one of these, the server pushes a ``ParameterStatus`` message so
# drivers (asyncpg, pgjdbc, c3p0, …) see the new value out-of-band. The
# lowercase key maps to the canonical Postgres wire-case name — real
# Postgres emits ``DateStyle`` / ``TimeZone`` / ``IntervalStyle`` in
# camel-case on the wire even though SQL identifiers are case-insensitive.
# ``integer_datetimes``, ``is_superuser``, ``in_hot_standby``,
# ``default_transaction_read_only`` are GUC_REPORT in real Postgres too but
# the facade doesn't expose them as settable, so we don't list them here.
_GUC_REPORT_NAMES: dict[str, str] = {
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

# DEV-1570 type aliases — populated by ``_build_column_type_index`` below and
# cached per-connection. Declared here so the ``__init__`` annotation can
# reference them without a forward-ref dance.
ColumnTypeKey = tuple[str, str, str]  # (schema_lower, table_lower, column_lower)
ColumnTypeIndex = dict[ColumnTypeKey, DataType]


class _PreparedStatement(BaseModel):
    sql: str
    parameter_oids: list[int]


class _Portal(BaseModel):
    sql: str
    result_format_codes: list[int]


class _Done(Exception):
    """Internal signal to end the session cleanly (Terminate / EOF)."""


class PgConnection:
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        engine=None,
        storage=None,
        token: str | None = None,
        authenticator: Authenticator | None = None,
        storage_provider: "StorageProvider | None" = None,
        engine_factory: "EngineFactory | None" = None,
        tls_ctx=None,
        catalog_extra_relations=None,
        catalog_ttl_seconds: float | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._engine = engine
        self._storage = storage
        # Optional per-connection scoping: when ``storage_provider`` is given the
        # storage (and engine, via ``engine_factory``) is resolved from the
        # authenticated principal after auth — e.g. a tenant-scoped store.
        self._storage_provider = storage_provider
        self._engine_factory = engine_factory
        # Tracks whether ``_resolve_scope`` actually built per-connection
        # storage + engine objects (so teardown disposes only what this
        # connection owns; an auth failure before the swap mustn't touch
        # statically-provided storage / engine the host wants to keep alive).
        self._owns_scoped_resources: bool = False
        # ``authenticator`` wins; ``token`` is kept for back-compat and wraps
        # into the default static-token authenticator.
        # Explicit ``is None`` check — a custom authenticator whose
        # ``__bool__``/``__len__`` is falsey must not silently fall back
        # to the static-token path.
        self._authenticator: Authenticator = (
            authenticator if authenticator is not None
            else StaticTokenAuthenticator(token)
        )
        # Opaque host-defined principal set on successful auth (tenant/user).
        self._principal: object | None = None
        self._tls_ctx = tls_ctx
        # Extensibility hook for embedders to override / extend the pg_catalog
        # tables — see ``build_catalog_relations(..., extra_relations=...)``.
        self._catalog_extras = catalog_extra_relations
        self._tx_state: bytes = proto.TX_IDLE
        # Logical database name (``current_database()`` / ``table_catalog``),
        # taken from the ``database`` startup parameter. NOT a model-resolution
        # datasource — execution routes per query (see ``QueryResult.data_source``).
        self._database: str = DEFAULT_DATABASE
        self._catalog: FacadeCatalog | None = None
        # On-demand catalog refresh: when a TTL is set, an idle connection
        # re-checks storage at most once per window and rebuilds the catalog
        # only if the cheap ``graph_fingerprint`` actually moved (see
        # ``_maybe_refresh_catalog``). ``None`` keeps the catalog static for
        # the connection's lifetime (the historical behavior).
        self._catalog_ttl_seconds: float | None = catalog_ttl_seconds
        self._catalog_checked_at: float = 0.0
        self._catalog_fingerprint: str | None = None
        self._statements: dict[str, _PreparedStatement] = {}
        self._portals: dict[str, _Portal] = {}
        # Lazily-built (schema, table, column) -> DataType lookup, used by the
        # DEV-1570 empty-string-vs-non-text Bind rewrite. Built once per
        # connection on first need; ``None`` until then so connections that
        # never bind candidates pay zero cost.
        self._column_type_index: ColumnTypeIndex | None = None
        # Extended protocol: after an error the backend discards every message
        # until the next Sync, then resumes with ReadyForQuery.
        self._skip_until_sync = False
        # DEV-1569: per-connection session-settings mailbox. Captures SET /
        # set_config writes; consulted by SHOW / current_setting reads. Seeded
        # from the module-level SESSION_SETTING_SEED via dict(...) so each
        # connection owns its own copy (never aliasing the seed).
        self._session_settings: dict[str, str] = dict(SESSION_SETTING_SEED)
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
            if not await self._authenticate(startup):
                return
            await self._resolve_scope(startup.parameters.get("database"))
            self._catalog = await self._build_catalog()
            self._catalog_checked_at = time.monotonic()
            self._catalog_fingerprint = await self._read_fingerprint()
            await self._send_startup_complete()
            await self._main_loop()
        except _Done:
            return
        except (asyncio.IncompleteReadError, ConnectionResetError):
            return
        finally:
            await self._close_scoped_storage()

    async def _close_scoped_storage(self) -> None:
        """Release the per-connection storage + engine that ``_resolve_scope``
        built from ``storage_provider`` / ``engine_factory``.

        Gated on ``_owns_scoped_resources`` so an auth failure before the
        swap leaves any static storage/engine the host wants to keep alive
        untouched. Disposes the engine too (``SlayerQueryEngine.aclose``
        releases the async SQL-client pools — without this a long-lived
        facade with ``storage_provider`` leaks one engine per session).
        """
        if not self._owns_scoped_resources:
            return
        engine_aclose = getattr(self._engine, "aclose", None)
        if engine_aclose is not None:
            try:
                await engine_aclose()
            except Exception:  # noqa: BLE001 — teardown best-effort
                logger.exception("pg facade: scoped engine close failed")
        storage_aclose = getattr(self._storage, "aclose", None)
        if storage_aclose is not None:
            try:
                await storage_aclose()
            except Exception:  # noqa: BLE001 — teardown best-effort
                logger.exception("pg facade: scoped storage close failed")

    # ----- startup ----------------------------------------------------------

    async def _read_startup_frame(self) -> bytes | None:
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

    async def _handle_startup(self) -> proto.StartupMessage | None:
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
        """Upgrade the plaintext transport to TLS.

        Not covered by end-to-end handshake tests yet; unit tests monkeypatch
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

    async def _authenticate(self, startup: proto.StartupMessage) -> bool:
        username = startup.parameters.get("user")
        database = startup.parameters.get("database")

        password: str | None = None
        if self._authenticator.requires_password:
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
            try:
                password = proto.decode_password(body)
            except (ValueError, struct.error):
                # Keep auth-phase malformed input on the wire-protocol
                # path; without this, the client gets a silent disconnect
                # instead of a Postgres error response.
                await self._send_error(
                    code=proto.SQLSTATE_PROTOCOL_VIOLATION,
                    message="malformed password message",
                    severity="FATAL",
                )
                return False

        outcome = await self._authenticator.authenticate(
            username=username, password=password, database=database
        )
        if not outcome.ok:
            await self._send_error(
                code=proto.SQLSTATE_INVALID_PASSWORD,
                message=outcome.message,
                severity="FATAL",
            )
            return False

        self._principal = outcome.principal
        self._writer.write(proto.encode_authentication_ok())
        await self._flush()
        return True

    # ----- scope resolution ------------------------------------------------

    async def _resolve_scope(self, database: str | None) -> None:
        """Resolve per-connection scope after auth.

        The ``database`` startup parameter is the logical database name (one
        emulated DB per instance/tenant), not a datasource selector — every
        datasource the storage exposes appears as a schema. When a
        ``storage_provider`` is configured, the storage (and engine) is
        re-resolved from the authenticated principal so a host can scope the
        connection to one tenant.
        """
        self._database = database or DEFAULT_DATABASE
        if self._storage_provider is not None:
            self._storage = await self._storage_provider(self._principal)
            factory = self._engine_factory or _default_engine_factory
            self._engine = factory(self._storage)
            # Mark only after BOTH constructions succeed — partial state
            # would leave teardown unsure which side to dispose.
            self._owns_scoped_resources = True

    async def _build_catalog(self) -> FacadeCatalog:
        models_by_datasource: dict[str, list[SlayerModel]] = {}
        schema_by_datasource: dict[str, str] = {}
        for datasource in await self._storage.list_datasources():
            names = await self._storage.list_models(data_source=datasource)
            models = [
                model
                for name in names
                if (model := await self._storage.get_model(
                    name=name, data_source=datasource,
                )) is not None
            ]
            models_by_datasource[datasource] = models
            config = await self._storage.get_datasource(datasource)
            if config is not None and config.postgres_schema:
                schema_by_datasource[datasource] = config.postgres_schema
        priority = await self._storage.get_datasource_priority()
        return build_catalog_grouped_by_schema(
            models_by_datasource=models_by_datasource,
            schema_by_datasource=schema_by_datasource,
            datasource_priority=priority,
            default_schema=PUBLIC_SCHEMA,
        )

    async def _read_fingerprint(self) -> str | None:
        """Cheap storage staleness token, or ``None`` when unavailable.

        Only consulted when a catalog TTL is configured. ``OSError`` (e.g. a
        file backend caught mid-write) is treated as "unknown" so the next
        check forces a rebuild, matching the search-graph convention.
        """
        if self._catalog_ttl_seconds is None:
            return None
        try:
            return await self._storage.graph_fingerprint()
        except OSError:
            return None

    async def _maybe_refresh_catalog(self) -> None:
        """On-demand, TTL-throttled, change-gated catalog rebuild.

        Called at statement entry. Rebuilds the per-connection catalog only
        when (a) a TTL is configured, (b) the connection is idle — never
        mid-transaction, to avoid a catalog shift inside a txn, (c) the TTL
        window has elapsed since the last check, and (d) the storage
        fingerprint has actually changed. When nothing changed the cost is a
        single ``graph_fingerprint`` read per window. Backends that don't
        implement a real fingerprint report a constant, so they never rebuild
        and behave exactly as before.

        Best-effort: a transient storage failure during the fingerprint read
        or the rebuild must not propagate out of ``_run_statement`` and tear
        down the client connection. On failure we keep the existing (possibly
        stale) catalog and retry on the next TTL window.
        """
        if self._catalog_ttl_seconds is None or self._catalog is None:
            return
        if self._tx_state != proto.TX_IDLE:
            return
        now = time.monotonic()
        if now - self._catalog_checked_at < self._catalog_ttl_seconds:
            return
        # Stamp the check time up front so a failing refresh retries no sooner
        # than the next window rather than hammering storage every statement.
        self._catalog_checked_at = now
        try:
            fingerprint = await self._read_fingerprint()
            if fingerprint is not None and fingerprint == self._catalog_fingerprint:
                return
            # Build into a local and swap only on success, so a failed rebuild
            # never leaves the connection with a half-built or ``None`` catalog.
            catalog = await self._build_catalog()
        # Refresh is best-effort: keep the old catalog on any storage failure.
        except Exception:  # noqa: BLE001
            logger.warning(
                "pg facade: catalog refresh failed; keeping current catalog",
                exc_info=True,
            )
            return
        self._catalog = catalog
        self._catalog_fingerprint = fingerprint
        # Derived from the catalog; drop it so it rebuilds against the new one.
        self._column_type_index = None

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
            # BI tools (Metabase) wrap reads in ``BEGIN READ ONLY`` etc., which
            # sqlglot can't parse. Strip the transaction characteristics and
            # retry once before surfacing a syntax error.
            stripped = _strip_tx_open_characteristics(sql)
            try:
                statements = [
                    s for s in sqlglot.parse(stripped, dialect="postgres") if s is not None
                ] if stripped != sql else None
            except sqlglot.errors.ParseError:
                statements = None
            if statements is None:
                logger.warning("pg facade: cannot parse simple query %r: %s", sql, exc)
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
        empty_string_null_params = self._empty_string_null_params_for_bind(
            sql=stmt.sql, raw_values=bind.parameter_values, oids=oids,
        )
        literals: list[str] = []
        for i, (raw, fmt, oid) in enumerate(
            zip(bind.parameter_values, formats, oids), start=1,
        ):
            if raw is None:
                literals.append("NULL")
                continue
            if i in empty_string_null_params:
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

    def _empty_string_null_params_for_bind(
        self, *, sql: str, raw_values, oids: list[int],
    ) -> set[int]:
        # DEV-1570: pre-classify $N indices whose bound value is an empty
        # text-OID payload AND whose AST occurrence targets a non-TEXT catalog
        # column. Those positions emit ``NULL`` rather than ``''`` so DuckDB
        # doesn't trip a ``Could not convert string '' to INT64`` at Execute.
        # The column-type index is built lazily on first need.
        candidates = [
            i + 1 for i, (raw, oid) in enumerate(zip(raw_values, oids))
            if oid == proto.OID_TEXT and raw == b""
        ]
        if not candidates or self._catalog is None:
            return set()
        if self._column_type_index is None:
            self._column_type_index = _build_column_type_index(
                catalog=self._catalog, datasource=self._database,
                extra_relations=self._catalog_extras,
            )
        return _classify_empty_string_param_targets(
            sql=sql,
            column_type_index=self._column_type_index,
            candidate_param_indices=candidates,
        )

    async def _handle_describe(self, msg: proto.DescribeMessage) -> None:
        # Refresh here too, not just at Execute: Describe advertises the
        # RowDescription, and it stamps the TTL check so the following Execute
        # stays in the same window and won't shift the catalog underneath it —
        # otherwise a mid-window edit could make the rows disagree with the
        # already-sent RowDescription.
        await self._maybe_refresh_catalog()
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
        self, sql: str, *, result_formats: list[int] | None,
        param_oids: list[int] | None = None,
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
            except TranslationError as exc:
                # Describe must not raise to the wire here; the subsequent
                # Execute surfaces the error. Report NoData so the client
                # can proceed. Log it (debug) so the silent Describe path is
                # still greppable when a client swallows the later error.
                logger.debug("pg facade: cannot describe %r: %s", describe_sql, exc)
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
            catalog_sql_executor=lambda: executor_for(
                self._catalog, self._database,
                extra_relations=self._catalog_extras,
            ),
            # Convenience for interactive psql sessions: ``SELECT * FROM t``
            # in browse mode (no GROUP BY / HAVING / aggregate) expands to
            # every non-hidden column. Flight stays strict (its clients
            # always project explicit names).
            expand_star_in_browse_mode=True,
        )

    def _probe_matcher(self, parsed: exp.Expression):
        """Wraps the PG-facade and shared probe matchers.

        DEV-1569: ``SHOW`` / ``current_setting`` consult the per-connection
        ``self._session_settings``. For ``set_config`` matches, the returned
        ``ProbeMatcherOutcome`` carries a mutation hint that
        ``_run_statement`` applies on Execute (Describe leaves it pending).
        """
        pg = match_pg_probe_with_mutation(
            parsed, datasource=self._database, version_str=version_string(),
            session_settings=self._session_settings,
        )
        if pg is not None:
            return pg
        return facade_match_probe(parsed)

    async def _run_statement(
        self, sql: str, *, result_formats: list[int] | None, send_row_description: bool,
    ) -> bool:
        """Translate + respond. Returns False if an error was sent."""
        # Refresh the catalog before translating so an idle connection picks
        # up model/schema edits within the TTL window (no-op when disabled or
        # mid-transaction).
        await self._maybe_refresh_catalog()
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
        self, batch: RowBatch, result_formats: list[int] | None, send_row_description: bool,
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
        self, result: QueryResult, result_formats: list[int] | None, send_row_description: bool,
    ) -> bool:
        try:
            # The translator resolves the per-query datasource from the
            # referenced model(s) and rejects cross-datasource joins. A model
            # query always carries one; guard the impossible None rather than
            # passing it to the engine.
            if result.data_source is None:
                raise ValueError("could not resolve a datasource for the query")
            with timing.open_query_profile():
                response = await self._engine.execute(
                    query=result.query, data_source=result.data_source,
                )
        except Exception as exc:  # noqa: BLE001 — surface any engine error to the client
            code, message = _engine_error_fields(exc)
            await self._send_error(code=code, message=message)
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
        self, result, result_formats: list[int] | None,
    ) -> list[proto.FieldDescription] | None:
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

    def _apply_tx_command(self, command_tag: str | None) -> None:
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
_TYPED_SENTINEL_BY_OID: dict[int, str] = {
    proto.OID_TEXT: "CAST(NULL AS VARCHAR)",
    proto.OID_INT8: "CAST(NULL AS BIGINT)",
    proto.OID_FLOAT8: "CAST(NULL AS DOUBLE)",
    proto.OID_BOOL: "CAST(NULL AS BOOLEAN)",
    proto.OID_DATE: "CAST(NULL AS DATE)",
    proto.OID_TIMESTAMP: "CAST(NULL AS TIMESTAMP)",
}


def _substitute_typed_sentinels(sql: str, param_oids: list[int]) -> str:
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


# DEV-1570: empty-string-vs-non-text Bind rewrite -----------------------------
#
# Symmetric with the Describe-side typed-NULL substitution above. pgjdbc /
# Metabase binds an empty string (``b""``) for "null" against text-OID
# parameters; when that parameter targets a non-TEXT catalog column the
# resulting ``WHERE int_col = ''`` previously tripped DuckDB's
# ``Conversion Error: Could not convert string '' to INT64`` at Execute.
# The classifier below identifies $N indices whose AST occurrences land in a
# comparison / IN / BETWEEN predicate against a column that resolves via the
# FacadeCatalog to a non-TEXT DataType, so ``_substitute_params`` can swap
# their literal to NULL for those positions.

# Mirrors slayer.facade.catalog_sql._PG_CATALOG_NAMES — duplicating here keeps
# the classifier independent of catalog_sql internals. Bare names in user SQL
# resolve to pg_catalog only when they match a known relation (per the
# catalog executor's convention at slayer/facade/catalog_sql.py:712).
_PG_CATALOG_RELATIONS: frozenset = frozenset({
    "pg_namespace", "pg_class", "pg_attribute", "pg_type", "pg_proc",
    "pg_settings", "pg_description", "pg_stat_user_tables", "pg_enum",
    "pg_tables", "pg_views", "pg_matviews", "pg_constraint", "pg_index",
    "pg_attrdef",
})

# Binary-comparison sqlglot node classes the classifier walks. Excludes LIKE /
# ILIKE (text-only operators; collision with the empty-string-vs-non-text bug
# is not possible).
_COMPARISON_NODE_TYPES: tuple = (
    exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE,
    exp.NullSafeEQ, exp.NullSafeNEQ,
)


def _build_column_type_index(
    *, catalog: FacadeCatalog, datasource: str,
    extra_relations=None,
) -> ColumnTypeIndex:
    """Build the (schema_lower, table_lower, column_lower) -> DataType lookup
    used by the Bind-time empty-string-to-NULL rewrite (DEV-1570).

    Covers pg_catalog.*, information_schema.* (via the ``_is_<name>``
    builder-name remap), and user-model tables under ``PUBLIC_SCHEMA``.
    """
    out: ColumnTypeIndex = {}
    _index_catalog_relations(
        out=out, catalog=catalog, datasource=datasource,
        extra_relations=extra_relations,
    )
    _index_user_tables(out=out, catalog=catalog)
    return out


def _index_catalog_relations(
    *, out: ColumnTypeIndex, catalog: FacadeCatalog, datasource: str,
    extra_relations=None,
) -> None:
    """Populate ``out`` with pg_catalog / information_schema column types
    materialised by ``build_catalog_relations``. The ``_is_<name>`` builder
    convention is remapped to the SQL-visible ``information_schema.<name>``."""
    for rel in build_catalog_relations(
        catalog=catalog, datasource=datasource,
        extra_relations=extra_relations,
    ):
        if rel.name.startswith("_is_"):
            schema = "information_schema"
            table = rel.name[len("_is_"):]
        else:
            schema = "pg_catalog"
            table = rel.name
        table_lower = table.lower()
        for col in rel.columns:
            _record_column_type(
                out=out, key=(schema, table_lower, col.name.lower()),
                dt=col.type,
            )


def _index_user_tables(*, out: ColumnTypeIndex, catalog: FacadeCatalog) -> None:
    """Populate ``out`` with user-model column types under PUBLIC_SCHEMA."""
    for sch in catalog.schemas:
        schema_lower = sch.name.lower()
        for tbl in sch.tables:
            tbl_lower = tbl.name.lower()
            for d in tbl.dimensions:
                _record_column_type(
                    out=out, key=(schema_lower, tbl_lower, d.name.lower()),
                    dt=d.data_type,
                )
            for m in tbl.metrics:
                dt = m.data_type if m.data_type is not None else DataType.TEXT
                _record_column_type(
                    out=out, key=(schema_lower, tbl_lower, m.name.lower()),
                    dt=dt,
                )


def _record_column_type(
    *, out: ColumnTypeIndex, key: ColumnTypeKey, dt: DataType,
) -> None:
    """Insert (key → dt) into ``out`` unless the key already exists with a
    different type. Case-distinct quoted identifiers (e.g. ``id`` INT and
    ``"ID"`` TEXT in the same model) would otherwise collide because the
    index lower-cases column names — last-writer-wins on the previous
    implementation could rewrite an empty-string text comparison to NULL.
    Skip the second occurrence and warn once. Same-type collisions are
    silently ignored (no behaviour change). DEV-1570 / Codex CX-2."""
    existing = out.get(key)
    if existing is None:
        out[key] = dt
        return
    if existing != dt:
        logger.warning(
            "DEV-1570 column-type index: case-distinct collision on %s "
            "(existing=%s, new=%s); keeping existing. Empty-string rewrite "
            "may behave incorrectly if the case-distinct column is queried.",
            key, existing, dt,
        )


def _classify_empty_string_param_targets(
    *, sql: str,
    column_type_index: ColumnTypeIndex,
    candidate_param_indices: Iterable[int],
) -> set[int]:
    """Return the subset of ``candidate_param_indices`` whose AST occurrences
    appear in a comparison / IN / BETWEEN predicate against a column that
    resolves via ``column_type_index`` to a non-TEXT ``DataType``.

    Whole-parameter granularity: if ANY occurrence of $N targets a non-text
    column, $N is in the result set so ``_substitute_params`` substitutes
    ``NULL`` everywhere $N appears.

    Returns the empty set on parse failure or unexpected AST shape; the
    helper never raises (per Codex round 1, finding #1).
    """
    candidates = set(candidate_param_indices)
    if not candidates:
        return set()
    # sqlglot 30.4.3's tokenizer rejects PostgreSQL placeholders adjacent to
    # punctuation in some compact shapes (e.g. ``IN ($1,$2)`` no-whitespace
    # form trips a TokenError, while ``IN ($1, $2)`` parses cleanly).
    # Pad each $N with surrounding whitespace before parsing — this only
    # affects the AST we walk for classification; the literal substitution
    # downstream still uses the original ``stmt.sql``.
    normalised_sql = _PARAM_PLACEHOLDER.sub(r" $\1 ", sql)
    try:
        parsed = sqlglot.parse_one(sql=normalised_sql, dialect="postgres")
    except sqlglot.errors.SqlglotError:
        return set()
    if parsed is None:
        return set()
    try:
        column_to_table = _map_column_tables(
            parsed=parsed, column_type_index=column_type_index,
        )
        return _collect_non_text_params(
            parsed=parsed, candidates=candidates,
            column_to_table=column_to_table,
            column_type_index=column_type_index,
        )
    except Exception:  # NOSONAR(S110) — defensive: never raise out of bind path
        logger.debug("DEV-1570 classifier defensive catch", exc_info=True)
        return set()


def _map_column_tables(
    *, parsed: exp.Expression, column_type_index: ColumnTypeIndex,
) -> dict[int, tuple[str, str] | None]:
    """Resolve every Column node to its owning scope's (schema, table)."""
    column_to_table: dict[int, tuple[str, str] | None] = {}
    for scope in traverse_scope(parsed):
        sources = _resolved_table_sources(scope.sources)
        for col in scope.find_all(exp.Column):
            column_to_table[id(col)] = _resolve_column_table(
                col=col, scope_sources=sources,
                column_type_index=column_type_index,
            )
    return column_to_table


def _collect_non_text_params(
    *, parsed: exp.Expression,
    candidates: set[int],
    column_to_table: dict[int, tuple[str, str] | None],
    column_type_index: ColumnTypeIndex,
) -> set[int]:
    """Walk comparison / IN / BETWEEN nodes; collect $N indices whose paired
    Column resolves to a non-TEXT ``DataType``."""
    result: set[int] = set()
    for node in parsed.walk():
        for col, param_idx in _column_param_pairs_from_node(node):
            if param_idx not in candidates:
                continue
            table = column_to_table.get(id(col))
            if table is None:
                continue
            key: ColumnTypeKey = (table[0], table[1], col.name.lower())
            dt = column_type_index.get(key)
            if dt is not None and dt != DataType.TEXT:
                result.add(param_idx)
    return result


def _column_param_pairs_from_node(node) -> Iterable[tuple[exp.Column, int]]:
    """Yield (Column, param_index) pairs for each comparison-shape node."""
    if isinstance(node, _COMPARISON_NODE_TYPES):
        yield from _pair_column_and_param(node.this, node.expression)
        return
    if isinstance(node, exp.Between):
        value = node.this
        for bound_key in ("low", "high"):
            bound = node.args.get(bound_key)
            if bound is not None:
                yield from _pair_column_and_param(value, bound)
        return
    if isinstance(node, exp.In):
        value = node.this
        for el in (node.expressions or []):
            yield from _pair_column_and_param(value, el)


def _try_extract_column(node) -> exp.Column | None:
    """Return the underlying ``exp.Column`` if ``node`` is one (optionally
    wrapped in semantically-transparent ``exp.Paren`` layers). CAST / function
    / arithmetic wrappers around a column are documented out of scope —
    pin tests `test_cast_wrapped_column_not_classified`,
    `test_arithmetic_wrapped_column_not_classified`,
    `test_function_wrapped_column_not_classified`."""
    while isinstance(node, exp.Paren):
        node = node.this
    return node if isinstance(node, exp.Column) else None


def _try_extract_param_index(node) -> int | None:
    """Return the 1-based ``$N`` index if ``node`` is an ``exp.Parameter``
    (optionally wrapped in ``exp.Paren`` and/or ``exp.Cast`` layers).

    Unwrapping ``exp.Cast`` is parameter-side-only (Codex CX-3): a user
    writing ``objsubid = $1::int`` or ``objsubid = CAST($1 AS INT)`` has
    explicitly cast the parameter, and the empty-string-to-NULL rewrite
    still applies — ``CAST(NULL AS INT)`` is a harmless typed null, while
    leaving ``$1`` as ``''`` would still trip DuckDB's INT conversion.
    Asymmetric with ``_try_extract_column``, which keeps CAST-wrapped
    columns out of scope (their user-supplied cast is the documented
    boundary at which we stop classifying)."""
    while isinstance(node, (exp.Paren, exp.Cast)):
        node = node.this
    if not isinstance(node, exp.Parameter):
        return None
    try:
        return int(node.name)
    except (ValueError, AttributeError, TypeError):
        return None


def _pair_column_and_param(left, right) -> Iterable[tuple[exp.Column, int]]:
    """Two operands where one is a bare Column and the other is a $N
    Parameter -> yield (Column, $N). Returns nothing if both sides are
    the same kind or if neither is a Column / Parameter."""
    col_l, col_r = _try_extract_column(left), _try_extract_column(right)
    param_l, param_r = _try_extract_param_index(left), _try_extract_param_index(right)
    col = col_l if col_l is not None else col_r
    param_idx = param_l if param_l is not None else param_r
    # Need exactly one column and exactly one parameter — reject if both sides
    # match the same kind (col = col, param = param) or neither is matchable.
    if col_l is not None and col_r is not None:
        return
    if param_l is not None and param_r is not None:
        return
    if col is not None and param_idx is not None:
        yield col, param_idx


def _resolved_table_sources(sources) -> dict[str, tuple[str, str]]:
    """Given ``Scope.sources``, return ``{alias_lower: (schema_lower, table_lower)}``.

    Sources whose value is another ``Scope`` (CTE / derived subquery)
    are skipped — they lose physical-column lineage so the classifier
    can't resolve their column types.

    Bare table names (no schema qualifier) are inferred to ``pg_catalog``
    when the name matches a known relation, otherwise to ``PUBLIC_SCHEMA``.
    information_schema requires an explicit qualifier — bare names like
    ``columns`` never resolve there (Codex round 1, finding #4).
    """
    result: dict[str, tuple[str, str]] = {}
    for alias, src in sources.items():
        if not isinstance(src, exp.Table):
            continue
        tbl_name = src.name.lower()
        db_part = src.args.get("db")
        schema: str | None = None
        if db_part is not None:
            schema_raw = db_part.name if hasattr(db_part, "name") else str(db_part)
            schema = schema_raw.lower()
        if schema is None:
            schema = (
                "pg_catalog" if tbl_name in _PG_CATALOG_RELATIONS else PUBLIC_SCHEMA
            )
        result[alias.lower()] = (schema, tbl_name)
    return result


def _resolve_column_table(
    *, col: exp.Column,
    scope_sources: dict[str, tuple[str, str]],
    column_type_index: ColumnTypeIndex,
) -> tuple[str, str] | None:
    """Resolve a Column node to ``(schema, table_name)`` via its owning
    scope's sources. Returns ``None`` if unresolvable (no scope match,
    ambiguous bare name, or table not in scope)."""
    table_q = (col.table or "").lower()
    db_q = (col.db or "").lower()
    if table_q:
        if db_q:
            return (db_q, table_q)
        if table_q in scope_sources:
            return scope_sources[table_q]
        return None
    name_lower = col.name.lower()
    matches: list[tuple[str, str]] = []
    for _alias, (schema, tbl) in scope_sources.items():
        if (schema, tbl, name_lower) in column_type_index:
            matches.append((schema, tbl))
    if len(matches) == 1:
        return matches[0]
    return None


def _resolve_param_oids(stmt: _PreparedStatement) -> list[int]:
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


def _command_tag(command_tag: str | None) -> str:
    if command_tag in ("BEGIN", "START TRANSACTION"):
        return "BEGIN"
    if command_tag is None:
        return "SELECT 0"
    return command_tag


def _engine_error_fields(exc: BaseException) -> tuple[str, str]:
    """Extract a Postgres SQLSTATE + terse message from an engine execution error.

    SQLAlchemy wraps the driver error (asyncpg/psycopg) in a ``DBAPIError``
    whose ``.orig`` carries the real ``sqlstate`` and the bare server message.
    Surfacing those lets a client see e.g. ``permission denied for table Item``
    (42501) instead of the full ``(sqlalchemy...) <class ...>: ... [SQL: ...]``
    Python repr. Walks ``.orig`` / ``__cause__`` / ``__context__`` and returns
    the first driver error exposing a 5-char SQLSTATE; falls back to XX000 plus
    ``str(exc)`` when none is found.
    """
    seen: set[int] = set()
    stack: list[BaseException | None] = [exc]
    while stack:
        cur = stack.pop()
        if cur is None or id(cur) in seen:
            continue
        seen.add(id(cur))
        code = getattr(cur, "sqlstate", None) or getattr(cur, "pgcode", None)
        if isinstance(code, str) and len(code) == 5:
            message = getattr(cur, "message", None) or str(cur)
            return code, message
        stack.extend([getattr(cur, "orig", None), cur.__cause__, cur.__context__])
    return proto.SQLSTATE_INTERNAL_ERROR, str(exc)


def _sqlstate_for(exc: TranslationError) -> str:
    msg = str(exc)
    if READ_ONLY_MESSAGE in msg:
        return proto.SQLSTATE_READ_ONLY_SQL_TRANSACTION
    if "parse error" in msg.lower():
        return proto.SQLSTATE_SYNTAX_ERROR
    if "Unknown table" in msg or "Unknown schema" in msg or "Unknown catalog" in msg:
        return proto.SQLSTATE_UNDEFINED_TABLE
    return proto.SQLSTATE_FEATURE_NOT_SUPPORTED


def _encode_value(value, oid: int, fmt: int) -> bytes | None:
    if fmt == proto.FORMAT_BINARY:
        return value_to_binary(value, oid)
    return value_to_text(value, oid)
