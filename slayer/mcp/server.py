"""MCP server for SLayer."""

import asyncio
import functools
import json
import logging
import sys
import weakref
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from contextlib import asynccontextmanager
from inspect import cleandoc
from typing import TYPE_CHECKING, Annotated, Any, Literal

import anyio
import anyio.to_thread
from packaging.specifiers import SpecifierSet
from packaging.version import InvalidVersion, Version
from pydantic import (
    BaseModel, ConfigDict, Discriminator, Field, Tag, TypeAdapter, ValidationError, WrapValidator,
)
from sqlalchemy.exc import DatabaseError

from slayer import __version__, usage_reporting
from slayer import otel
from slayer.usage_reporting.payload import USAGE_KEYS
from slayer.core.enums import Conformance
from slayer.demo.jaffle_shop import DemoSetupError
from slayer.core.errors import (
    AmbiguousDatasetError,
    MemoryNotFoundError,
    SlayerError,
)
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.format import encode_json, encode_text
from slayer.core.granularity import CustomGranularity
from slayer.core.relationship_edges import RelationshipEdgeRef, edge_reference, resolve_relationship_refs, without_relationships
from slayer.core.query import QueryRefinement, SlayerQuery, deprecated_inputs_of
from slayer.core.recommend import render_recommendation_markdown
from slayer.core import vocabulary
from slayer.core.vocabulary import DATASET_KEYS, Spellings, converted_document, current_spellings, deprecated_spelling, renamed_arguments
from slayer.core.warnings import DeprecatedInputWarning, ResponseTruncationWarning, SlayerWarning
from slayer import async_utils
from slayer.live_schema import ingestion as engine_ingestion
from slayer.live_schema.ingestion import (
    _empty_ingest_message as _shared_empty_ingest_message,
    _friendly_db_error,
    _get_schemas,
    _hidden_internal_line,
    _unhide_hint,
    _updated_detail_lines,
    IngestableObject,
    key_read_failure_line,
    key_read_failures_header,
    list_ingestable_objects,
)
from slayer.live_schema.schema_drift import IdempotentIngestResult, IngestionError, DatasetAddition, ToDeleteEntry
from slayer.live_schema.schema_scope import schema_ref_from_token, validate_scope_args
from slayer.search.service import handle_edit_refresh
from slayer.engine.query_engine import ClientFactory, SlayerQueryEngine, SlayerResponse
from slayer.engine.response_meta import FieldMetadata, ResponseAttributes
from slayer.database.client import SqlClient, SqlClientError
from slayer.memories.help_seed import seed_help_memories
from slayer.inspect.dataset_render import (  # noqa: F401 — re-exported for backward-compat (tests + other modules import these names from slayer.mcp.server)
    _build_sample_query_args,
    _escape_md_cell,
    _format_meta,
    _get_row_count,
    _markdown_table,
    _md_code_span,
    _render_inspect_footer,
    _resolve_inspect_sections,
    _source_type_for,
    _strip_dataset_prefix,
    _truncate_description,
    render_dataset_inspection,
)
from slayer.inspect.collection_render import (
    render_datasource_list,
    render_datasets_summary,
    with_relationships_as_read,
)
from slayer.inspect.service import InspectService
from slayer.memories.service import MemoryService
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures

if TYPE_CHECKING:
    from mcp.server.mcpserver import Context, MCPServer
    from mcp.server.mcpserver.exceptions import ToolError
    from mcp.shared.exceptions import MCPError
    from mcp.types import CallToolResult, InputRequiredResult

logger = logging.getLogger(__name__)

VALID_DIMENSION_TYPES = {"string", "time", "date", "boolean", "number"}
_UNSET: Any = object()  # Sentinel to distinguish "not provided" from "explicitly set to None"


def _unset() -> Any:
    """``_UNSET`` as a default factory: JSON schemas skip factory defaults."""
    return _UNSET

# Response row cap when the caller passes no limit; an explicit limit is trusted verbatim.
_MCP_ROW_CAP = 20
_CAP_HINT = "pass a higher 'limit' to get more rows"
_DS_LOAD_FAILED = "Failed to load datasource '%s': %s"
_NESTED_CAP_HINT = "pass a higher 'limit' on the root query to get more rows"
#: A definition and its deprecated twin: an upsert spelling one replaces the other.
_DEFINITION_TWINS = (frozenset({"sql", "expression"}), frozenset({"formula", "expression"}))

#: The mcp releases SLayer runs on.
_MCP_RANGE = ">=2.3,<3"
# Shared remedy for every mcp-import failure below.
_MCP_REMEDY = (
    f"Install a supported version: pip install 'mcp{_MCP_RANGE}' "
    "(or upgrade SLayer: pip install -U motley-slayer)."
)

# Claude Code's default cap on each tool description and on the server instructions.
MAX_TOOL_DESCRIPTION_CHARS = 2048

SERVER_INSTRUCTIONS = """SLayer is a semantic layer for querying databases: describe the data you want (metrics, dimensions, filters) and SLayer writes the SQL, joins included.
ONE QUESTION = ONE QUERY. Build the whole answer in a single `query` call. Do NOT split it into several queries, do NOT combine or recompute query results yourself (by hand or in Python), and do NOT add stages for what one stage can do. One SLayer query computes all of these together: totals at any coarser grain and shares of them, ranks and top N per group, period-over-period changes, running totals, trailing windows, aggregates of aggregates, and aggregates over several joined datasets. Each aggregate runs over its own dataset's rows, so one query NEVER fans out or double counts. Explore the data first if you need to; then write the one query.
The query syntax is in the descriptions of the query tool's input fields. Overview: inspect(reference='memory:help.intro', entity_type='memory'). Full reference: inspect(reference=['memory:help.aggregations', 'memory:help.transforms', 'memory:help.time', 'memory:help.relationships', 'memory:help.queries'], entity_type='memory').
Typical workflow: search(question='...') → inspect the dataset → query.
To connect a new database: create_datasource → describe_datasource (verify + list tables) → ingest_datasource → datasets_summary."""

_ALWAYS_LOAD_META = {"anthropic/alwaysLoad": True}


def _query_form(value: Any) -> str:
    """The ``query`` argument's form, chosen by its JSON type."""
    if isinstance(value, str):
        return "name"
    return "stages" if isinstance(value, list) else "object"


class _SubmittedQuery(BaseModel):
    """The ``query`` argument as submitted, beside its native reading or why it has none."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    raw: Any
    parsed: Any = None
    error: ValidationError | TypeError | None = None

    def native(self) -> Any:
        """The native reading; raises the native validation error when there is none."""
        if self.error is not None:
            raise self.error
        return self.parsed


def _keep_submitted(value: Any, handler: Callable[[Any], Any]) -> _SubmittedQuery:
    """The strict binding judges the input as submitted, so native validation is recorded, never raised here."""
    try:
        return _SubmittedQuery(raw=value, parsed=handler(value))
    except (ValidationError, TypeError) as exc:
        return _SubmittedQuery(raw=value, error=exc)


#: Validates only the chosen form, so an error never lists the other forms' mismatches.
_QUERY_FORMS = Annotated[
    Annotated[str, Tag("name")] | Annotated[SlayerQuery, Tag("object")] | Annotated[list[SlayerQuery], Tag("stages")],
    Discriminator(_query_form),
]
_QUERY_ADAPTER: TypeAdapter[Any] = TypeAdapter(_QUERY_FORMS)
QueryArgument = Annotated[_QUERY_FORMS, WrapValidator(_keep_submitted)]


def _submitted(query: Any) -> _SubmittedQuery:
    """``query`` as the tool receives it: already wrapped by its validator, else (a direct call) wrapped here."""
    return query if isinstance(query, _SubmittedQuery) else _keep_submitted(query, _QUERY_ADAPTER.validate_python)


def _check_budget(*, what: str, text: str) -> None:
    if len(text) > MAX_TOOL_DESCRIPTION_CHARS:
        raise ValueError(f"{what} is {len(text)} chars, over the {MAX_TOOL_DESCRIPTION_CHARS}-char budget.")


def _agent_description(fn: Callable[..., Any]) -> str:
    """``fn``'s dedented docstring, checked against the description budget."""
    text = cleandoc(fn.__doc__ or "")
    _check_budget(what=f"MCP tool {fn.__name__!r} description", text=text)
    return text


def _mcp_supported(version_str: str) -> bool:
    """Whether ``version_str`` is a PEP 440 version in ``_MCP_RANGE``; pre-releases and unparseable versions are not."""
    try:
        return Version(version_str) in SpecifierSet(_MCP_RANGE)
    except InvalidVersion:
        return False


def _import_mcp_sdk() -> "tuple[type[MCPServer], type[ToolError], type[MCPError]]":
    """mcp 2.x's ``MCPServer``, ``ToolError`` and ``MCPError``, or an actionable ImportError."""
    try:
        installed = _pkg_version("mcp")
    except PackageNotFoundError as exc:
        raise ImportError(f"MCP package not found. {_MCP_REMEDY}") from exc
    if not _mcp_supported(installed):
        raise ImportError(f"mcp {installed} is installed, but SLayer requires mcp{_MCP_RANGE}. {_MCP_REMEDY}")
    try:
        from mcp.server.mcpserver import MCPServer  # ALLOW(import-not-top): SDK probe — at build time, so an out-of-range install gets the remedy, not a raw import error
        from mcp.server.mcpserver.exceptions import ToolError  # ALLOW(import-not-top): SDK probe, as above
        from mcp.shared.exceptions import MCPError  # ALLOW(import-not-top): SDK probe, as above
    except ImportError as exc:
        raise ImportError(
            f"mcp {installed} is installed, but 'mcp.server.mcpserver' could not be imported: {exc} {_MCP_REMEDY}"
        ) from exc
    return MCPServer, ToolError, MCPError


def _keeping_failure_text(
    fn: Callable[..., Any], *, tool_error: "type[ToolError]", mcp_error: "type[MCPError]",
) -> Callable[..., Any]:
    """``fn`` raising any failure but an MCP one as a ``tool_error`` with its message, which the SDK shows the agent."""

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return await fn(*args, **kwargs)
        except (tool_error, mcp_error):
            raise
        except Exception as exc:
            raise tool_error(str(exc)) from exc

    return wrapper


def _tool_operation(bound: Any) -> dict[str, str]:
    name = bound.arguments["name"]
    tool = name if f"mcp:{name}" in USAGE_KEYS else "other"
    return {
        "operation": tool,
        "mcp.method.name": "tools/call",
        "gen_ai.operation.name": "execute_tool",
        "gen_ai.tool.name": tool,
    }


#: Pre-2.0 tool arguments, by tool.
_OLD_ARGUMENTS: dict[str, dict[str, str]] = {
    "create_dataset": DATASET_KEYS,
    "edit_dataset": {"model_name": "dataset_name", **DATASET_KEYS},
}
_OLD_REMOVE_KEYS = {**DATASET_KEYS, "join_edges": "relationship_edges"}


_PROGRESS_INTERVAL_S = 1.0
_PREPARING = "Waiting for SLayer startup preparation (demo setup / startup ingest)"


class StartupOutcome(BaseModel):
    """Where startup preparation stands; only a demo failure fails tool calls."""

    state: Literal["pending", "ready", "demo_failed"] = "pending"
    reason: str | None = None


class StartupPreparation:
    """Demo setup, then startup ingest, run once in the background on the server's loop; tool calls wait for it."""

    def __init__(
        self,
        *,
        demo_setup: Callable[[], object] | None,
        ingest: Callable[[], Coroutine[Any, Any, object]] | None,
    ) -> None:
        self._demo_setup = demo_setup
        self._ingest = ingest
        self._task: asyncio.Task[None] | None = None
        self._sessions = 0
        self.outcome = StartupOutcome() if demo_setup or ingest else StartupOutcome(state="ready")

    @asynccontextmanager
    async def lifespan(self, _app: object) -> AsyncIterator[None]:
        """Run preparation while any session is open (mcp enters this per connection); the last one out cancels it."""
        if self.outcome.state != "pending":
            yield
            return
        self._sessions += 1
        self._ensure_running()
        try:
            yield
        finally:
            self._sessions -= 1
            if self._sessions == 0 and self._task is not None:
                with anyio.CancelScope(shield=True):
                    self._task.cancel()
                    await asyncio.wait({self._task})
                if self._sessions:  # a session arrived while the cancel was awaited
                    self._ensure_running()

    def _ensure_running(self) -> None:
        """Start preparation unless it is running or has finished; a cancelled run starts afresh."""
        if self.outcome.state == "pending" and (self._task is None or self._task.done()):
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def _run(self) -> None:
        if self._demo_setup is not None:
            try:
                await self._finish_on_cancel(anyio.to_thread.run_sync(self._demo_setup))
            except Exception as exc:  # noqa: BLE001 — becomes every tool call's error
                failure = exc if isinstance(exc, DemoSetupError) else DemoSetupError(str(exc))
                print(failure, file=sys.stderr, flush=True)
                self.outcome = StartupOutcome(state="demo_failed", reason=failure.reason)
                return
        if self._ingest is not None:
            try:
                await self._ingest()
            except Exception as exc:  # noqa: BLE001 — a startup-ingest failure never blocks tool calls
                print(f"Ingest-on-startup failed: {exc}", file=sys.stderr, flush=True)
        self.outcome = StartupOutcome(state="ready")

    @staticmethod
    async def _finish_on_cancel(work: Coroutine[Any, Any, object]) -> None:
        """Await ``work``; a cancel waits for it to finish (a thread cannot be stopped) before propagating."""
        task = asyncio.ensure_future(work)
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.wait({task})
            raise

    async def wait(self, *, report_progress: Callable[[float, str], Coroutine[Any, Any, None]]) -> None:
        """Hold the calling request until preparation ends, reporting progress; a cancelled caller never cancels it."""
        tick = 0
        while (task := self._task) is not None and not task.done():
            tick += 1
            try:
                await report_progress(tick, _PREPARING)
            except Exception:  # noqa: BLE001 — progress is best-effort
                logger.debug("MCP progress notification failed", exc_info=True)
            await asyncio.wait({task}, timeout=_PROGRESS_INTERVAL_S)
        if self.outcome.state == "demo_failed":
            raise DemoSetupError(self.outcome.reason or "unknown error")
        if self.outcome.state == "pending" and self._task is not None:
            raise RuntimeError("SLayer startup preparation was interrupted; reconnect to retry")



async def _report_progress(context: "Context | None", progress: float, message: str) -> None:
    """Progress on ``context``'s request; none without a context."""
    if context is not None:
        await context.report_progress(progress, message=message)



def _respelled(
    arguments: dict[str, Any], renames: dict[str, str], reported: dict[tuple[str, str], DeprecatedInputWarning],
) -> dict[str, Any]:
    out = renamed_arguments(arguments, renames=renames)
    for old, new in renames.items():
        if old in arguments and (old, new) not in reported:
            reported[(old, new)] = deprecated_spelling(old, new)
    return out


def _converted_arguments(
    *, tool: str, arguments: dict[str, Any],
) -> tuple[dict[str, Any], list[DeprecatedInputWarning]]:
    """``arguments`` with their pre-2.0 spellings replaced, and one deprecation per old spelling."""
    reported: dict[tuple[str, str], DeprecatedInputWarning] = {}
    out = _respelled(arguments, _OLD_ARGUMENTS.get(tool, {}), reported)
    if tool == "edit_dataset" and isinstance(out.get("remove"), dict):
        out["remove"] = _respelled(out["remove"], _OLD_REMOVE_KEYS, reported)
    return out, list(reported.values())


def _reported_text(text: str, lines: list[str]) -> str:
    """``text`` reporting ``lines``: in a JSON object's ``warnings`` list, else as trailing warning lines."""
    try:
        payload = json.loads(text)
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        existing = payload.get("warnings", [])
        payload["warnings"] = [*existing, *(line for line in lines if line not in existing)]
        return json.dumps(payload, indent=2)
    return text + "".join(f"\nWarning: {line}" for line in lines if line not in text)


def _with_deprecations(
    result: "CallToolResult | InputRequiredResult", renamed: list[DeprecatedInputWarning],
) -> "CallToolResult | InputRequiredResult":
    """``result`` whose first text block (and string structured ``result``) reports ``renamed``."""
    content = getattr(result, "content", None)
    if not renamed or content is None:
        return result
    lines = [r.human_message() for r in renamed]
    blocks = list(content)
    first = next((i for i, b in enumerate(blocks) if getattr(b, "type", None) == "text"), None)
    if first is not None:
        blocks[first] = blocks[first].model_copy(update={"text": _reported_text(blocks[first].text, lines)})
    structured = getattr(result, "structured_content", None)
    if isinstance(structured, dict) and isinstance(structured.get("result"), str):
        structured = {**structured, "result": _reported_text(structured["result"], lines)}
    return result.model_copy(update={"content": blocks, "structured_content": structured})


def _counted(
    server_cls: "type[MCPServer]", *, tool_error: "type[ToolError]", preparation: StartupPreparation,
) -> "type[MCPServer]":
    """``server_cls`` counting every tool call, and each session's client once; calls wait for ``preparation``."""

    class CountedMCPServer(server_cls):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, lifespan=preparation.lifespan, **kwargs)
            self._usage_reporting_sessions: weakref.WeakSet = weakref.WeakSet()

        @otel.request(surface="mcp", extract=_tool_operation)
        async def call_tool(
            self, name: str, arguments: dict[str, Any], context: "Context | None" = None,
        ) -> "CallToolResult | InputRequiredResult":
            session = self._observe_session(context)
            with usage_reporting.action() as action:
                try:
                    try:
                        await preparation.wait(report_progress=functools.partial(_report_progress, context))
                    except Exception as exc:
                        raise tool_error(str(exc)) from exc
                    # One request is one input document: its pre-2.0 spellings each warn once.
                    with converted_document(stored=False) as (spellings, _):
                        arguments, renamed = _converted_arguments(tool=name, arguments=arguments)
                        result = await super().call_tool(name, arguments, context)
                        result = _with_deprecations(result, [*renamed, *spellings.report()])
                except BaseException as exc:
                    usage_reporting.record(
                        surface="mcp", token=name, error=_counted_error(exc, tool_error=tool_error),
                        dry_run=action.dry_run, session=session,
                    )
                    raise
            usage_reporting.record(surface="mcp", token=name, dry_run=action.dry_run, session=session)
            return result

        def _observe_session(self, context: "Context | None") -> object | None:
            """The calling client connection (counting its client at its first call), or ``None``."""
            if context is None or not usage_reporting.is_active():
                return None
            try:
                # mcp 2.x builds a ServerSession per request and exposes the connection no other way.
                session = context.session._connection  # pyright: ignore[reportPrivateUsage] -- the only per-connection key
                if session in self._usage_reporting_sessions:
                    return session
                self._usage_reporting_sessions.add(session)
                info = session.client_params.client_info if session.client_params else None
            except Exception:
                logger.debug("usage reporting: no MCP session to identify", exc_info=True)
                return None
            usage_reporting.observe_mcp_client(
                name=getattr(info, "name", None), version=getattr(info, "version", None),
            )
            return session

    return CountedMCPServer


def _ambiguous_with_mcp_hint(exc: AmbiguousDatasetError) -> str:
    """Render an ``AmbiguousModelError`` with an MCP-specific remediation hint."""
    return (
        f"{exc} Pass data_source=... to this tool, or use the "
        f"set_datasource_priority tool to set a priority."
    )


#: Failures rendered as a friendly database message.
_DB_FAILURES = (DatabaseError, SqlClientError)


class ToolFailure(Exception):
    """A tool failure whose message is safe to show the client; raised from its cause."""


def _db_tool_failure(exc: Exception) -> ToolFailure:
    return ToolFailure(_friendly_db_error(exc))


def _counted_error(exc: BaseException, *, tool_error: "type[ToolError]") -> BaseException:
    """The failure to count: past every ``ToolError`` wrapping and a ``ToolFailure``, to their cause."""
    error = exc
    while isinstance(error, tool_error) and error.__cause__ is not None:
        error = error.__cause__
    if isinstance(error, ToolFailure) and error.__cause__ is not None:
        return error.__cause__
    return error


async def _test_connection(client: SqlClient) -> tuple[bool, str]:
    """Test a datasource connection. Returns (success, message)."""
    try:
        await client.ping()
        return True, "Connection successful."
    except Exception as e:
        return False, _friendly_db_error(e)


async def _fetch_tables(
    client: SqlClient, schema_name: str | None = None,
) -> tuple[list[IngestableObject] | None, str | None]:
    """Inspect a datasource's table and view objects (name + kind).

    Returns (objects, None) or (None, friendly_error). schema_name=None uses the
    default schema. Views are always included so a views-only schema isn't empty.
    """
    try:
        # Route through a SchemaRef so schema_name=None resolves to the
        # catalog-qualified default rather than sweeping every schema.
        ref = (
            schema_ref_from_token(schema_name, dialect_name=client.datasource.type, requested=schema_name)
            if schema_name
            else None
        )
        objects = await list_ingestable_objects(client=client, ref=ref, include_views=True)
        return sorted(objects, key=lambda o: o.name), None
    except Exception as e:
        if isinstance(e, _DB_FAILURES):
            return None, _friendly_db_error(e)
        return None, str(e)


async def _empty_ingest_message(*, schema_name: str, client: SqlClient) -> str:
    """Agent-facing wrapper over the shared engine renderer."""
    return await _shared_empty_ingest_message(
        schema_name=schema_name,
        client=client,
        retry_hint=(
            "Try: ingest_datasource with schema_name set to one of these."
        ),
    )


def _render_new_datasets_section(new_datasets: list[DatasetAddition]) -> list[str]:
    if not new_datasets:
        return []
    lines = [f"Created {len(new_datasets)} new dataset(s):"]
    for a in new_datasets:
        described = getattr(a, "described_fields", []) or []
        suffix = f", {len(described)} described" if described else ""
        lines.append(
            f"- {a.dataset_name} ({len(a.new_fields)} fields, "
            f"{len(a.new_relationships)} relationships{suffix})"
        )
    return lines


def _render_updated_section(updated: list[DatasetAddition]) -> list[str]:
    if not updated:
        return []
    lines = [f"Updated {len(updated)} existing dataset(s):"]
    for a in updated:
        lines.append(f"- {a.dataset_name} ({'; '.join(_updated_detail_lines(a))})")
    return lines


def _render_unchanged_section(unchanged: list[DatasetAddition]) -> list[str]:
    if not unchanged:
        return []
    return [
        f"Re-introspected {len(unchanged)} unchanged dataset(s): "
        f"{', '.join(a.dataset_name for a in unchanged)}"
    ]


def _render_drift_section(to_delete: list[ToDeleteEntry]) -> list[str]:
    if not to_delete:
        return []
    out = ["", "Pending drift (run validate_datasets / apply manually):"]
    out.extend(f"- {entry.tool}: {entry.dataset_name}" for entry in to_delete)
    return out


def _render_skipped_section(skipped: list[Any]) -> list[str]:
    """Objects that produced no dataset at all (no --exclude hint; the agent has none)."""
    if not skipped:
        return []
    out = ["", f"Skipped ({len(skipped)}) — not modellable, no dataset created:"]
    out.extend(f"- {entry.table_name}: {entry.reason}" for entry in skipped)
    return out


def _render_skipped_schemas_section(skipped_schemas: list[Any]) -> list[str]:
    """Requested schemas dropped from scope (foreign catalog / system schema)."""
    if not skipped_schemas:
        return []
    out = ["", f"Skipped schemas ({len(skipped_schemas)}):"]
    out.extend(f"- {entry.token}: {entry.reason}" for entry in skipped_schemas)
    return out


def _render_key_read_failures_section(lost: list[Any]) -> list[str]:
    if not lost:
        return []
    return ["", key_read_failures_header(len(lost)), *(f"- {key_read_failure_line(entry)}" for entry in lost)]


def _render_hidden_internals_section(
    hidden: list[Any], *, data_source: str | None = None
) -> list[str]:
    """Recognised ELT/migration internals modelled ``hidden``.

    Queryable but absent from datasets_summary; reporting them tells a hidden
    dataset from an uncreated one. The unhide hint is datasource-qualified.
    """
    if not hidden:
        return []
    out = [
        "",
        f"Hidden ({len(hidden)}) — recognised ELT/migration internals "
        f"(excluded from datasets_summary; still queryable by name):",
    ]
    out.extend(f"- {_hidden_internal_line(entry)}" for entry in hidden)
    out.append(f"  Use {_unhide_hint(data_source)} to surface one.")
    return out


def _render_broken_dependents_section(broken: list[Any]) -> list[str]:
    if not broken:
        return []
    return ["", f"Broken by a list re-type ({len(broken)}):", *(f"- {b.human_message()}" for b in broken)]


def _render_errors_section(errors: list[IngestionError]) -> list[str]:
    if not errors:
        return []
    out = ["", f"Errors ({len(errors)}):"]
    out.extend(f"- {err.dataset_name}: {err.error}" for err in errors)
    return out


async def _render_ingest_result(
    result: IdempotentIngestResult,
    *,
    schema_name: str,
    ds: DatasourceConfig,
    client: SqlClient,
) -> str:
    """Render an ``IdempotentIngestResult`` for the MCP ``ingest_datasource`` tool."""
    additions = list(result.additions)
    # Read defensively — older result shapes may lack these attributes.
    skipped = list(getattr(result, "skipped", None) or [])
    skipped_schemas = list(getattr(result, "skipped_schemas", None) or [])
    hidden_internals = list(getattr(result, "hidden_internals", None) or [])
    broken = list(getattr(result, "broken_dependents", None) or [])
    lost = list(getattr(result, "key_read_failures", None) or [])
    datasource_described = bool(getattr(result, "datasource_described", False))
    if (
        not broken
        and not lost
        and not additions
        and not result.to_delete
        and not result.errors
        and not skipped
        and not skipped_schemas
        and not hidden_internals
        and not datasource_described
    ):
        # Empty result: either no tables in scope, or all datasets already
        # skipped. Use the scan's own discovered objects (not _fetch_tables,
        # which only sees the default schema) to tell the two apart.
        scanned_objects = getattr(result, "objects", None) or []
        if not scanned_objects:
            return await _empty_ingest_message(schema_name=schema_name, client=client)
        return "Datasource already in sync — no additive changes."

    new_datasets = [a for a in additions if a.created]
    updated = [a for a in additions if not a.created and a.has_changes]
    unchanged = [
        a for a in additions if not a.created and not a.has_changes
    ]

    lines: list[str] = []
    lines.extend(_render_new_datasets_section(new_datasets))
    lines.extend(_render_updated_section(updated))
    lines.extend(_render_unchanged_section(unchanged))
    if datasource_described:
        lines.append("Datasource description imported.")
    lines.extend(_render_drift_section(list(result.to_delete)))
    # Same order as the CLI renderer, so the two surfaces read alike.
    lines.extend(_render_skipped_section(skipped))
    lines.extend(_render_skipped_schemas_section(skipped_schemas))
    lines.extend(_render_key_read_failures_section(lost))
    lines.extend(
        _render_hidden_internals_section(hidden_internals, data_source=ds.name)
    )
    lines.extend(_render_broken_dependents_section(broken))
    lines.extend(_render_errors_section(list(result.errors)))
    if not lines:
        lines.append("Datasource already in sync — no changes.")
    return "\n".join(lines)


def create_mcp_server(  # NOSONAR(S3776) — MCPServer tool-registration factory; complexity is the cumulative inline closure body of every @mcp.tool() handler. Splitting would require dependency-injecting the engine/storage/services into a separate module — out of scope for incremental PRs.
    storage: StorageBackend,
    *,
    ingest_on_startup: bool = False,
    always_load_query: bool = False,
    client_factory: ClientFactory | None = None,
    demo_setup: Callable[[], object] | None = None,
    _seed_help: bool = True,
):
    _check_budget(what="MCP server instructions", text=SERVER_INSTRUCTIONS)
    # Built first: an invalid SLAYER_NOW must fail before any side effect.
    engine = SlayerQueryEngine(storage=storage, client_factory=client_factory)
    # Seed conceptual-help memories (idempotent; _seed_help=False when
    # create_app already seeds). Best-effort — never abort the build on a
    # seed failure, and skip for metadata-only (non-StorageBackend) builds.
    if _seed_help and isinstance(storage, StorageBackend):
        try:
            # Via the modules so tests can monkeypatch both seams.
            async_utils.run_sync(seed_help_memories(storage=storage))
        except Exception as exc:  # noqa: BLE001 — seeding must not abort the build
            logger.warning(
                "SLayer help-memory seeding skipped: %s", exc, exc_info=True
            )

    raw_storage = storage

    async def startup_ingest() -> None:
        # Its own engine, so no client bound to this job outlives it in the serving engine.
        prep = SlayerQueryEngine(storage=raw_storage, client_factory=client_factory)
        try:
            await engine_ingestion.ingest_all_datasources_idempotent(
                storage=prep.storage, client_for=prep.client_for, stream=sys.stderr,
            )
        finally:
            try:
                await prep.aclose()
            finally:
                prep.close()

    preparation = StartupPreparation(demo_setup=demo_setup, ingest=startup_ingest if ingest_on_startup else None)
    # Every tool reads datasets, and the live database, through the server's engine.
    storage = engine.storage
    server_cls, tool_error, mcp_error = _import_mcp_sdk()

    mcp = _counted(server_cls, tool_error=tool_error, preparation=preparation)(
        "SLayer", instructions=SERVER_INSTRUCTIONS, version=__version__,
    )
    # Expose the closure engine so callers can dispose per-task pools via
    # mcp._slayer_engine.aclose() (idempotent; leaves the engine reusable).
    # The read-only introspection tools share this same engine.
    mcp._slayer_engine = engine

    def tool(**kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """``mcp.tool`` with the budgeted docstring as the description, keeping every failure's text."""
        return lambda fn: mcp.tool(description=_agent_description(fn), **kwargs)(
            _keeping_failure_text(fn, tool_error=tool_error, mcp_error=mcp_error),
        )

    @tool(meta=_ALWAYS_LOAD_META if always_load_query else None)
    async def query(
        query: Annotated[QueryArgument, Field(
            description="A query object, a list of query objects (stages), or a saved query-backed dataset's name.",
        )],
        variables: Annotated[dict[str, Any] | None, Field(
            description=(
                "Values for {placeholder}s in conditions, expressions, date_range bounds and dataset SQL; "
                "override a stage's own variables, an outer query's variables and the saved dataset's defaults."
            ),
        )] = None,
        refine: Annotated[QueryRefinement | None, Field(
            description="Saved-dataset-name form only: clauses merged into the saved query's final stage.",
        )] = None,
        show_sql: Annotated[bool, Field(description="Include the generated SQL in the response.")] = False,
        dry_run: Annotated[bool, Field(description="Return the generated SQL without executing it.")] = False,
        explain: Annotated[bool, Field(description="Run the database's EXPLAIN and return the query plan.")] = False,
        format: Annotated[str, Field(description='"markdown" (default), "json" or "csv".')] = "markdown",
        conformance: Annotated[Literal["slayer", "ossie-l3"], Field(
            description='"slayer" (default) or "ossie-l3": accept only Ossie Layer 3 queries, refusing others with '
                        "Ossie's error codes, and name columns by Ossie's output names.",
        )] = "slayer",
    ) -> str:
        """ONE QUESTION = ONE QUERY. Build the whole answer in a single `query` call. Describe the result you want (metrics, dimensions, filters, order) and SLayer writes the SQL, including the joins. A single query can:
        - put several grains on one row: a group's share of its total or of the grand total, the region total next to each city;
        - rank, keep the top N per group, and sort by values it does not display;
        - compute period-over-period changes and trailing windows (90 days, 3 months), which look back before date_range by themselves, and running totals;
        - aggregate fields of joined datasets over their own rows, without double counting;
        - nest aggregates (the average of per-city totals) and chain stages.

        The syntax is in the descriptions of the query object's fields: metrics, filter, order, time_dimensions[].date_range, population and dimensions. Full reference: inspect(reference=["memory:help.aggregations", "memory:help.transforms", "memory:help.time", "memory:help.relationships", "memory:help.queries"], entity_type="memory").

        `query` is a query object, a list of them (stages: all but the last carry a name; later stages use earlier ones as population), or a saved query-backed dataset's name (optionally with `refine`).

        First inspect the dataset and search for saved learnings. Then run the final query and check the row count, each aggregate's scope, NULLs and the sort. Without a `limit`, responses stop at 20 rows.
        """
        if dry_run:
            usage_reporting.mark_dry_run()
        try:
            fmt = format.lower().strip()
            if fmt not in ("json", "csv", "markdown"):
                raise ValueError(f"Invalid format '{format}'. Must be one of: json, csv, markdown")
            # Response row cap (spec: mcp/response-row-cap): with no explicit
            # limit on the root query, push down cap+1 so truncation is
            # detectable, then slice response-side. A run-by-name string is
            # opaque (its stored SQL can't take a pushed-down limit) — cap
            # response-side only.
            strict = conformance == Conformance.OSSIE_L3.value
            # A strict query is judged as submitted and takes no limit: its rows are capped response-side.
            exec_query: Any
            submitted = _submitted(query)
            if strict:
                exec_query, capped, cap_hint = submitted.raw, True, _CAP_HINT
            else:
                exec_query, capped, cap_hint = _apply_mcp_row_cap(submitted.native())
            usage_reporting.observe_query(submitted.raw if strict else submitted.parsed)
            result = await engine.execute(
                query=exec_query,
                variables=variables,
                dry_run=dry_run,
                explain=explain,
                refine=refine,
                conformance=conformance,
            )
            usage_reporting.observe_executed(result)
            if dry_run:
                return _format_dry_run(result=result, fmt=fmt)
            if capped:
                _cap_rows(result, hint=cap_hint)
            if explain:
                output = f"SQL:\n{result.sql}\n\nQuery Plan:\n"
                output += _format_output(result=result, fmt=fmt)
                return output
            output = _format_output(result=result, fmt=fmt)
            if show_sql and result.sql:
                output = f"SQL:\n{result.sql}\n\n{output}"
            return output
        except _DB_FAILURES as e:
            raise _db_tool_failure(e) from e

    # Dataset discovery

    @tool()
    async def datasets_summary(
        datasource_name: Annotated[str, Field(description="Datasource name (from list_datasources).")],
        format: Annotated[str, Field(description='"markdown" (default) or "json".')] = "markdown",
        compact: Annotated[bool, Field(
            description="Default true: per dataset its name, description, field count, metric names and relationship targets. False adds the full field and metric tables.",
        )] = True,
    ) -> str:
        """Brief summary of every non-hidden dataset in a datasource."""
        fmt = format.lower().strip()
        if fmt not in ("markdown", "json"):
            raise ValueError(
                f"Invalid format '{format}' for datasets_summary. Must be 'markdown' or 'json'."
            )

        try:
            ds = await storage.get_datasource(datasource_name)
        except Exception as exc:
            logger.warning(_DS_LOAD_FAILED, datasource_name, exc)
            return f"Datasource '{datasource_name}' has an invalid config."
        if ds is None:
            return f"Datasource '{datasource_name}' not found."

        loaded = DocumentLoadFailures().skip(await storage.load_datasets(data_source=datasource_name))
        matched = [m for m in loaded if not m.hidden]
        matched.extend(await storage.builtin_datasets(datasource_name))
        matched.sort(key=lambda m: m.name)

        # Rendering delegates to the shared renderer (also used by
        # the ``inspect`` dataset collection view) — one code path, no drift.
        read, inherited_errors = await with_relationships_as_read(matched, engine)
        return render_datasets_summary(
            datasource_name=datasource_name,
            datasets=read,
            fmt=fmt,
            compact=compact,
            inherited_errors=inherited_errors,
            model_metrics=await storage.list_model_scoped_metrics(datasource_name),
        )

    @tool()
    async def inspect(
        entity_type: Annotated[str, Field(
            description=(
                "Required: datasource, dataset, field, metric, aggregation, memory or semantic_model. Asserts the "
                "resolved kind and disambiguates a name shared by, e.g., a field and an aggregation."
            ),
        )],
        reference: Annotated[str | list[str] | None, Field(
            description=(
                "One reference, a list of same-kind references (batch), or omitted for the collection. "
                "Canonical (mydb, mydb.orders, mydb.orders.amount), a bare name, a relationship path "
                "(orders.customers.region), or memory:<id>."
            ),
        )] = None,
        compact: Annotated[bool, Field(
            description=(
                "Default true: the description only for field / metric / aggregation / datasource / "
                "memory (never shortens a help.* topic), a schema skeleton without "
                "database calls for a dataset. False: the full render."
            ),
        )] = True,
        format: Annotated[str, Field(description='"markdown" (default) or "json".')] = "markdown",
        num_rows: Annotated[int, Field(description="Sample rows for a dataset; ignored for other kinds.")] = 3,
        show_sql: Annotated[bool, Field(description="Include generated SQL for a dataset; ignored for other kinds.")] = False,
        sections: Annotated[list[str] | None, Field(
            description=(
                "Dataset sections to render: fields, metrics, aggregations, relationships, samples, learnings, "
                "saved_queries; default all. Ignored for other kinds."
            ),
        )] = None,
        descriptions_max_chars: Annotated[int | None, Field(
            description="Truncate every description, and a memory's body, to this many characters.",
        )] = None,
    ) -> str:
        """Inspect one entity by reference and kind, a batch when `reference` is a list, or a whole collection when `reference` is omitted. A point lookup: use `search` to find entities by meaning, with related memories.

        Before using a field as a filter, projection, group-by or relationship key, inspect it with compact=false and read its Description (the author's intent) and Sample values (stored literal forms; a top-N sample). Build text conditions from these, never from a guessed spelling, and never pick a field by its name alone.

        Collection: omit `reference` with entity_type "dataset" (every dataset, grouped by datasource) or "datasource".
        Batch: a list of same-kind references returns one block per id in input order (a JSON array in json); a bad id does not sink the batch.
        Help: reference "memory:help.intro" (entity_type "memory") for the overview; batch several help topics in one call.
        """
        return await InspectService(storage=storage, engine=engine).inspect(
            reference=reference,
            entity_type=entity_type,
            compact=compact,
            format=format,
            num_rows=num_rows,
            show_sql=show_sql,
            sections=sections,
            descriptions_max_chars=descriptions_max_chars,
        )

    # Dataset creation and editing

    @tool()
    async def create_dataset(
        name: Annotated[str, Field(description="Unique dataset name (lowercase, underscores).")],
        sql_table: Annotated[str | None, Field(description='Database table, e.g. "public.orders".')] = None,
        sql: Annotated[str | None, Field(description="A SQL query as the dataset's source, instead of sql_table.")] = None,
        data_source: Annotated[str | None, Field(description="Datasource name (from list_datasources).")] = None,
        description: Annotated[str | None, Field(description="What one row of this dataset represents.")] = None,
        fields: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Field definitions: {"name", "expression", "datatype"}; expression is SQL over the source '
                "columns (a bare name reads a source column, dataset.field a field; unset reads the column "
                "named like the field); datatype TEXT, INT, DOUBLE, BOOLEAN, "
                "DATE, TIMESTAMP, UNKNOWN, or ARRAY with element_type (one of the others); optional "
                "primary_key, unique (single-field uniqueness other than the primary key), "
                "allowed_aggregations, filter (a condition masking the field's value "
                "inside aggregates), granularity (only for a field truly bucketed at that grain), "
                "label, description, hidden, meta."
            ),
        )] = None,
        metrics: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Saved metrics, {"name": "aov", "expression": "sum(revenue) / count(*)"} plus optional '
                'label, description, meta; queries reference them by dataset ("orders.aov").'
            ),
        )] = None,
        aggregations: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Custom aggregations, {"name": "sum_sq", "expression": "SUM({value} * {value})", '
                '"params": [{"name": "weight", "sql": "quantity"}], "description": ..., "datatype": ...}; the expression '
                "is SQL with {value} for the aggregated field; datatype (optional) is the result type, else inferred."
            ),
        )] = None,
        query: Annotated[Any | None, Field(
            description=(
                "A query object or list of stages; makes the dataset query-backed. Excludes sql_table, "
                "sql, fields, metrics and aggregations."
            ),
        )] = None,
        variables: Annotated[dict[str, Any] | None, Field(
            description="Default values for {var} placeholders in the backing query.",
        )] = None,
        access_tags: Annotated[list[str] | None, Field(
            description="Access groups allowed to see the dataset; omit for public.",
        )] = None,
        semantic_model: Annotated[str | None, Field(
            description="Add the dataset to this semantic model (an Ossie document of the datasource); omit to store "
                        "it standalone.",
        )] = None,
    ) -> str:
        """Create a semantic dataset from a database table (sql_table), a SQL query (sql), or a SLayer query (query: the dataset becomes query-backed and its fields come from the result).

        Host a field or metric on the dataset whose row grain is 1:1 with what it describes, not merely one where its inputs live. Choose relationship keys by field Description (author intent); on ties take the shortest declared relationship path (long chains through lookup or log tables fan out rows). Define entities in dependency order and reference already-defined ones by name; in row-level SQL parenthesise weighted sums in comparisons ((a*w1 + b*w2) > t).

        Example: create_dataset(name="orders", sql_table="public.orders", data_source="mydb", fields=[{"name": "amount", "sql": "amount", "datatype": "Float"}], metrics=[{"name": "aov", "expression": "sum(amount) / count(*)"}])

        A custom aggregation named sum_sq is called like a built-in: sum_sq(field). Authoring guide: inspect(reference="memory:help.datasets", entity_type="memory").
        """
        notes: list[SlayerWarning] = []
        if query is not None:
            table_params = {
                k: v for k, v in {
                    "sql_table": sql_table, "sql": sql, "data_source": data_source,
                    "fields": fields, "metrics": metrics, "aggregations": aggregations,
                }.items()
                if v
            }
            if table_params:
                return (
                    f"Error: 'query' cannot be combined with {', '.join(table_params.keys())}. "
                    "Use 'query' alone to create from a query, or provide table details without 'query'."
                )
            try:
                # Accept a single SlayerQuery dict or a list of stage dicts.
                if isinstance(query, list):
                    parsed_query = [SlayerQuery.model_validate(q) for q in query]
                else:
                    parsed_query = SlayerQuery.model_validate(query)
                dataset = await engine.create_dataset_from_query(
                    query=parsed_query,
                    name=name,
                    description=description or "",
                    variables=variables,
                    access_tags=access_tags,
                    warnings_out=notes,
                )
            except _DB_FAILURES as e:
                raise _db_tool_failure(e) from e
            except Exception as e:
                return f"Error creating dataset from query: {e}"
            field_names = [c.name for c in dataset.fields]
            metric_names = [m.name for m in dataset.metrics]
            return (
                f"Dataset '{name}' created from query. "
                f"Fields: {field_names}. Metrics: {metric_names}."
            ) + _warning_lines(notes)

        data = _build_dict(
            name=name,
            sql_table=sql_table,
            sql=sql,
            data_source=data_source,
            description=description,
            fields=fields,
            metrics=metrics,
            aggregations=aggregations,
            access_tags=access_tags,
            semantic_model=semantic_model,
        )
        try:
            dataset = SlayerDataset.model_validate(data)
        except ValidationError as exc:
            return f"Invalid dataset '{name}': {exc}"
        existed = (
            await storage.get_dataset(name, data_source=dataset.data_source)
            is not None
        )
        # save_dataset normalizes, validates Mode-A relationship paths, and trial-executes
        # a raw-sql source against its datasource before it persists.
        try:
            await engine.save_dataset(dataset, warnings_out=notes)
        except _DB_FAILURES as e:
            raise _db_tool_failure(e) from e
        except Exception as e:
            return f"Error creating dataset '{dataset.name}': {e}"
        verb = "replaced" if existed else "created"
        return f"Dataset '{dataset.name}' {verb}." + _warning_lines(notes)

    def _upsert_entity(
        entity_list: list,
        spec: dict,
        entity_cls: type,
        id_field: str,
        changes: list,
        label: str,
    ) -> str | None:
        """Upsert a named entity in *entity_list*.

        Returns an error string on validation failure, ``None`` on success.
        """
        entity_id = spec.get(id_field, "")
        if not entity_id:
            return f"Missing '{id_field}' in {label} specification."

        existing = next((e for e in entity_list if getattr(e, id_field) == entity_id), None)
        if existing is not None:
            merged = existing.model_dump()
            for twins in _DEFINITION_TWINS:
                if twins & spec.keys():
                    for k in twins - spec.keys():
                        merged.pop(k, None)
            for k, v in spec.items():
                merged[k] = v
            try:
                updated = entity_cls.model_validate(merged)
            except Exception as exc:
                return f"Invalid {label} '{entity_id}': {exc}"
            idx = entity_list.index(existing)
            entity_list[idx] = updated
            changes.append(f"updated {label} '{entity_id}'")
        else:
            try:
                new_entity = entity_cls.model_validate(spec)
            except Exception as exc:
                return f"Invalid {label} '{entity_id}': {exc}"
            entity_list.append(new_entity)
            changes.append(f"created {label} '{entity_id}'")
        return None

    def _upsert_relationship(*, relationships: list[DatasetRelationship], spec: dict, changes: list) -> str | None:
        """Upsert one relationship: match by name, else target + key pairs, else the sole relationship to the target."""
        target = spec.get("to", "")
        if not target:
            return "Missing 'to' in relationship specification."
        name = spec.get("name")
        sources, targets = spec.get("from_columns"), spec.get("to_columns")
        pairs = set(zip(sources, targets)) if isinstance(sources, list) and isinstance(targets, list) else set()
        to_target = [j for j in relationships if j.to == target]
        existing = (
            next((j for j in relationships if name and j.name == name), None)
            # A differently named edge on the same keys is a parallel edge, never this one.
            or next((j for j in to_target if pairs and set(j.column_pairs) == pairs
                     and not (name and j.name and j.name != name)), None)
            # A differently named sole relationship is another edge, never this one.
            or (to_target[0] if len(to_target) == 1 and not (name and to_target[0].name) else None)
        )
        label = f"relationship '{name or target}'"
        if existing is None and not name and to_target:
            return (
                f"Dataset already has a relationship to '{target}'; give the new one a 'name' so the "
                f"parallel edges stay addressable."
            )
        try:
            updated = DatasetRelationship.model_validate({**(existing.model_dump() if existing else {}), **spec})
        except Exception as exc:
            return f"Invalid {label}: {exc}"
        if existing is None:
            relationships.append(updated)
            changes.append(f"created {label}")
        else:
            relationships[relationships.index(existing)] = updated
            changes.append(f"updated {label}")
        return None

    VALID_REMOVE_KEYS = {"fields", "metrics", "aggregations", "relationships", "relationship_edges", "enforced_keys"}

    @tool()
    async def edit_dataset(
        dataset_name: Annotated[str, Field(description="Dataset to edit.")],  # NOSONAR(S107) — each parameter is a field of the agent-facing MCP tool schema
        *,
        description: Annotated[str | None, Field(description="New dataset description.")] = None,
        data_source: Annotated[str | None, Field(
            description="The dataset's datasource; required when the name exists in several datasources.",
        )] = None,
        new_data_source: Annotated[str | None, Field(description="Move the dataset to another datasource (rare).")] = None,
        default_time_dimension: Annotated[str | None, Field(
            description="A date/time field: the default time axis for transforms.",
        )] = None,
        sql_table: Annotated[str | None, Field(description="Database table; clears sql and source_queries.")] = None,
        sql: Annotated[str | None, Field(description="SQL query as the source; clears sql_table and source_queries.")] = None,
        source_queries: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                "Replace the backing query with these stages (all but the last carry a name); makes the "
                "dataset query-backed, clears sql_table and sql, and refreshes the cached fields."
            ),
        )] = None,
        query_variables: Annotated[Any, Field(
            default_factory=_unset,
            description="Replace the backing query's default {var} values; null clears them.",
        )],
        hidden: Annotated[bool | None, Field(description="Hide the dataset from discovery (still queryable).")] = None,
        access_tags: Annotated[list[str] | None, Field(
            description="Replace the access groups allowed to see the dataset; [] makes it public.",
        )] = None,
        fields: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Fields to upsert by name; only the given keys change. {"name", "datatype", "element_type", "expression", '
                '"description", "primary_key", "unique", "hidden", "allowed_aggregations", "filter", '
                '"label", "granularity"}; datatype TEXT, INT, DOUBLE, BOOLEAN, DATE, TIMESTAMP, UNKNOWN, or '
                'ARRAY with element_type (one of the others); unique marks '
                "single-field uniqueness other than the primary key (used for relationship cardinality); set "
                "granularity only for a field truly bucketed at that grain (null clears it)."
            ),
        )] = None,
        metrics: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Saved metrics to upsert by name, {"name": "aov", "expression": "sum(revenue) / '
                'count(*)"} plus optional label, description, meta; queries use them by dataset (orders.aov).'
            ),
        )] = None,
        aggregations: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Custom aggregations to upsert by name, {"name", "expression" (SQL with {value} and '
                '{param} placeholders), "params": [{"name", "sql"}], "description", "datatype" (result type; '
                'omitted: inferred), "meta"}.'
            ),
        )] = None,
        relationships: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Relationships to upsert, {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"], '
                '"name", "cardinality": "many_to_one", "description", "meta"}, matched by name, else '
                "target and key fields, else the sole relationship to the target; a second relationship to a target needs a name. "
                "Keys name fields by name; a composite key lists several fields on each side, in pair order. "
                "cardinality (one_to_one / one_to_many / many_to_one / many_to_many, read "
                "source->target) decides paths only; omit it when unknown. trusted: true assumes it holds and "
                "skips de-duplication (wrong on data violating it)."
            ),
        )] = None,
        add_filters: Annotated[list[str] | None, Field(
            description='SQL conditions always applied to the dataset, e.g. ["deleted_at IS NULL"].',
        )] = None,
        remove_filters: Annotated[list[str] | None, Field(description="Dataset filters to remove (exact text).")] = None,
        remove: Annotated[dict[str, list[str | dict[str, Any] | list[str]]] | None, Field(
            description=(
                'Entities to delete before the upserts: {"fields": [...], "metrics": [...], '
                '"aggregations": [...], "relationships": [target dataset or edge name, ...], '
                '"relationship_edges": [{"to", "name", "from_columns", "to_columns"}, ...], '
                '"enforced_keys": [[field, ...], ...]}.'
            ),
        )] = None,
        meta: Annotated[dict[str, Any] | None, Field(
            default_factory=_unset,
            description="JSON metadata replacing the dataset's meta; null clears it.",
        )],
    ) -> str:
        """Edit an existing dataset in one call: metadata, upserts of fields / metrics / aggregations / relationships (by name; only the given keys change), dataset filters, and removals (applied first). A dataset of a semantic model is edited in place in its document.

        Host a field or metric on the dataset whose row grain is 1:1 with what it describes, not merely one where its inputs live. Choose relationship keys by field Description (author intent); on ties take the shortest declared relationship path (long chains through lookup or log tables fan out rows). Define entities in dependency order and reference already-defined ones by name; in row-level SQL parenthesise weighted sums in comparisons ((a*w1 + b*w2) > t).

        Example: edit_dataset(dataset_name="orders", fields=[{"name": "status", "datatype": "String"}], metrics=[{"name": "aov", "expression": "sum(revenue) / count(*)"}], remove={"metrics": ["old_metric"]})
        """
        try:
            dataset = await storage.get_dataset(dataset_name, data_source=data_source)
        except AmbiguousDatasetError as exc:
            return _ambiguous_with_mcp_hint(exc)
        if dataset is None:
            return f"Dataset '{dataset_name}' not found."

        spellings = current_spellings() or Spellings(stored=False)
        fields = [vocabulary.field(spec, spellings) for spec in fields] if fields else fields
        metrics = [vocabulary.metric(spec, spellings) for spec in metrics] if metrics else metrics
        aggregations = [vocabulary.aggregation(spec, spellings) for spec in aggregations] if aggregations else aggregations
        relationships = [vocabulary.relationship(spec, spellings) for spec in relationships] if relationships else relationships
        original_data_source = dataset.data_source
        changes: list[str] = []
        # Track field-level vs dataset-level changes so the post-save hook
        # refreshes only the touched fields when possible.
        changed_fields: set = set()
        dataset_level_change = False
        # Dataset-doc changes (metrics / relationships) don't invalidate DatasetField.sampled
        # but do change the embedding text — track separately to refresh
        # embeddings without a full per-field re-profile.
        dataset_doc_changed = False

        # --- Phase 1: Scalar metadata ---
        if description is not None:
            dataset.description = description
            changes.append("updated description")
        if new_data_source is not None and new_data_source != dataset.data_source:
            # Moving a dataset is delete-old + save-new: refuse if the target key
            # is taken and defer the delete until after the new save (Phase 5).
            # Here we only mutate the in-memory dataset.
            try:
                existing_target = await storage.get_dataset(
                    dataset.name, data_source=new_data_source
                )
            except AmbiguousDatasetError:
                existing_target = None  # Strict lookup; ambiguity is for bare names only.
            if existing_target is not None:
                return (
                    f"Dataset '{dataset.name}' already exists in datasource "
                    f"'{new_data_source}'. Pick a different name, delete "
                    f"the existing target first, or move to a different "
                    f"datasource."
                )
            dataset.data_source = new_data_source
            dataset.enforced_keys = []
            changes.append(
                f"moved data_source from '{original_data_source}' to '{new_data_source}'"
            )
        if default_time_dimension is not None:
            dataset.default_time_dimension = default_time_dimension
            changes.append(f"set default_time_dimension to '{default_time_dimension}'")
        explicit_sources = sum(
            1 for v in (sql_table, sql, source_queries) if v is not None
        )
        if explicit_sources > 1:
            return (
                "Specify at most one of 'sql_table', 'sql', or 'source_queries' "
                "when editing a dataset — the three source modes are mutually exclusive."
            )

        if query_variables is not _UNSET:
            dataset.query_variables = query_variables or {}
            changes.append(
                "updated query_variables"
                if query_variables
                else "cleared query_variables"
            )
        if hidden is not None:
            dataset.hidden = hidden
            changes.append(f"set hidden to {hidden}")
        if access_tags is not None:
            dataset.access_tags = list(access_tags)
            changes.append(f"set access_tags to {access_tags}")
        if meta is not _UNSET:
            dataset.meta = meta
            changes.append("updated meta" if meta is not None else "cleared meta")

        # --- Phase 2: Removals ---
        if remove:
            for key in remove:
                if key not in VALID_REMOVE_KEYS:
                    return (
                        f"Invalid remove key '{key}'. "
                        f"Must be one of: {', '.join(sorted(VALID_REMOVE_KEYS))}."
                    )

            for name in remove.get("fields", []):
                match = next((c for c in dataset.fields if c.name == name), None)
                if match is None:
                    return f"Field '{name}' not found on dataset '{dataset_name}'."
                dataset.fields.remove(match)
                changes.append(f"removed field '{name}'")

            for name in remove.get("metrics", []):
                match = next((m for m in dataset.metrics if m.name == name), None)
                if match is None:
                    return f"Metric '{name}' not found on dataset '{dataset_name}'."
                dataset.metrics.remove(match)
                changes.append(f"removed metric '{name}'")
                dataset_doc_changed = True

            for name in remove.get("aggregations", []):
                match = next((a for a in dataset.aggregations if a.name == name), None)
                if match is None:
                    return f"Aggregation '{name}' not found on dataset '{dataset_name}'."
                dataset.aggregations.remove(match)
                changes.append(f"removed aggregation '{name}'")
                dataset_doc_changed = True

            try:
                removed = resolve_relationship_refs(dataset=dataset, refs=[
                    r if isinstance(r, str) else RelationshipEdgeRef.model_validate(r)
                    for r in (*remove.get("relationships", []), *remove.get("relationship_edges", []))
                ])
            except ValueError as exc:
                return str(exc)
            for relationship in removed:
                changes.append(f"removed relationship '{edge_reference(dataset=dataset, relationship=relationship)}'")
            if removed:
                dataset.relationships = without_relationships(dataset=dataset, relationships=removed)
                dataset_doc_changed = True

            dropped_keys = [sorted(k) for k in remove.get("enforced_keys", []) if isinstance(k, list)]
            if dropped_keys:
                dataset.enforced_keys = [k for k in dataset.enforced_keys if sorted(k) not in dropped_keys]
                changes.append(f"removed enforced keys {dropped_keys}")
                dataset_level_change = True

        # --- Source switch: after the removals, so the switched dataset is checked without them ---
        if sql_table is not None:
            dataset.assign(sql_table=sql_table, sql=None, source_queries=None)
            dataset_level_change = True
            changes.append(f"set sql_table to '{sql_table}'")
        if sql is not None:
            dataset.assign(sql=sql, sql_table=None, source_queries=None)
            dataset_level_change = True
            changes.append(f"set sql to '{sql}'")
        if source_queries is not None:
            # Query-backed: the cached fields are cleared here and refreshed by engine.save_dataset.
            dataset.assign(source_queries=[SlayerQuery.model_validate(q) for q in source_queries],
                           sql_table=None, sql=None, fields=[])
            dataset_level_change = True
            changes.append(f"set source_queries ({len(source_queries)} stage(s))")

        # --- Phase 3: Entity upserts ---
        for spec in fields or []:
            field_name = spec.get("name")
            if isinstance(field_name, str):
                changed_fields.add(field_name)
            err = _upsert_entity(
                entity_list=dataset.fields, spec=spec, entity_cls=DatasetField,
                id_field="name", changes=changes, label="field",
            )
            if err:
                return err

        for spec in metrics or []:
            err = _upsert_entity(
                entity_list=dataset.metrics, spec=spec, entity_cls=DatasetMetric,
                id_field="name", changes=changes, label="metric",
            )
            if err:
                return err
            dataset_doc_changed = True

        for spec in aggregations or []:
            err = _upsert_entity(
                entity_list=dataset.aggregations, spec=spec, entity_cls=Aggregation,
                id_field="name", changes=changes, label="aggregation",
            )
            if err:
                return err
            dataset_doc_changed = True

        for spec in relationships or []:
            err = _upsert_relationship(relationships=dataset.relationships, spec=spec, changes=changes)
            if err:
                return err
            dataset_doc_changed = True

        # --- Phase 4: Filters ---
        if add_filters:
            existing_filters = set(dataset.filters)
            for f in add_filters:
                if f not in existing_filters:
                    dataset.filters.append(f)
                    existing_filters.add(f)
                    changes.append(f"added filter '{f}'")
                    dataset_level_change = True

        if remove_filters:
            for f in remove_filters:
                if f not in dataset.filters:
                    return f"Filter not found on dataset '{dataset_name}': {f}"
                dataset.filters.remove(f)
                changes.append(f"removed filter '{f}'")
                dataset_level_change = True

        if not changes:
            return f"No changes specified for dataset '{dataset_name}'."

        # --- Phase 5: Validate and save ---
        # Query-backed datasets route through engine.save_dataset so the
        # engine-managed field cache is refreshed and user-supplied cache
        # fields are rejected.
        # Before the round trip below, which drops what the converted source queries carry.
        notes: list[SlayerWarning] = [*deprecated_inputs_of(dataset.source_queries or [])]
        try:
            validated = SlayerDataset.model_validate(dataset.model_dump(mode="json"))
        except Exception as exc:
            return f"Validation error: {exc}"

        if validated.source_queries:
            # fields are engine-managed here; reject explicit user supply
            # rather than silently dropping it.
            if fields is not None:
                return (
                    "Validation error: cannot supply 'fields' on a "
                    f"query-backed dataset ('{dataset_name}'). Fields are "
                    "engine-managed (auto-derived from the backing query)."
                )
            # Strip the cached fields so save_dataset repopulates them from a
            # fresh expansion of the backing query.
            validated = validated.model_copy(update={"fields": []})
        # save_dataset refreshes a query-backed dataset's cache (and may recompute its
        # data_source, so cleanup below uses the returned identity); otherwise it
        # normalizes, validates Mode-A relationship paths and trial-executes a raw-sql source.
        try:
            saved_dataset = await engine.save_dataset(validated, warnings_out=notes)
        except Exception as exc:
            return f"Validation error: {exc}"

        # Atomic move: remove the source row only after the save succeeded and
        # only if the saved dataset actually landed at a different data_source
        # (the cache populator can override new_data_source).
        if saved_dataset.data_source != original_data_source:
            await storage.delete_dataset(
                name=saved_dataset.name, data_source=original_data_source
            )
        # Refresh sampled field values and subtree embeddings. Best-effort —
        # a raise is captured into refresh_warnings so the save still succeeds.
        refresh_warnings: list[str] = []
        if changed_fields or dataset_level_change or dataset_doc_changed:
            try:
                refresh_warnings = await handle_edit_refresh(
                    engine=engine,
                    storage=storage,
                    data_source=saved_dataset.data_source,
                    dataset_name=saved_dataset.name,
                    changed_fields=changed_fields,
                    dataset_level_change=dataset_level_change,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort post-save
                logger.warning(
                    "edit_dataset refresh hook raised for %s.%s: %s",
                    saved_dataset.data_source, saved_dataset.name, exc,
                )
                refresh_warnings = [
                    f"refresh hook raised: {exc}",
                ]
        response_payload: dict = {
            "success": True,
            "dataset_name": dataset_name,
            "changes": changes,
            "message": f"Applied {len(changes)} change(s) to '{dataset_name}'",
        }
        reply_warnings = [*(n.human_message() for n in notes), *refresh_warnings]
        if reply_warnings:
            response_payload["warnings"] = reply_warnings
        return json.dumps(response_payload, indent=2)

    # Datasource management

    @tool()
    async def create_datasource(
        name: Annotated[str, Field(description="Unique datasource name.")],
        type: Annotated[str, Field(description="Database type: postgres, mysql, sqlite, bigquery, snowflake, ...")],
        host: Annotated[str | None, Field(description="Database host (default localhost).")] = None,
        port: Annotated[int | None, Field(description="Database port, e.g. 5432.")] = None,
        database: Annotated[str | None, Field(description="Database name.")] = None,
        username: Annotated[str | None, Field(description="Database user; ${ENV_VAR} is resolved.")] = None,
        password: Annotated[str | None, Field(description="Database password; ${ENV_VAR} is resolved.")] = None,
        connection_string: Annotated[str | None, Field(
            description="Full connection string instead of the individual fields.",
        )] = None,
        schema_name: Annotated[str | None, Field(
            description="Default schema, also the single schema auto-ingested.",
        )] = None,
        schemas: Annotated[str, Field(
            description="Comma-separated schemas to ingest; excludes schema_name and all_schemas.",
        )] = "",
        all_schemas: Annotated[bool, Field(
            description="Ingest every non-system schema; excludes schema_name and schemas.",
        )] = False,
        auto_ingest: Annotated[bool, Field(description="Create datasets from the schema (default true).")] = True,
        granularities: Annotated[list[CustomGranularity] | None, Field(
            description=(
                "Custom time granularities, each {name, base, multiple, origin}: buckets start at "
                'origin + k * multiple * base, e.g. {name: "fiscal_year", base: "year", origin: '
                '"2000-04-01"}; usable wherever a built-in granularity is.'
            ),
        )] = None,
    ) -> str:
        """Create a database connection, verify it, and auto-ingest datasets. Use ${ENV_VAR} in credentials to read environment variables.

        Example: create_datasource(name="mydb", type="postgres", host="localhost", port=5432, database="app", username="user", password="${DB_PASSWORD}")
        """

        schemas_list = [s.strip() for s in schemas.split(",") if s.strip()] or None
        # Validate the scope BEFORE persisting — a conflicting request must not
        # leave a half-created datasource behind (§3.9).
        try:
            validate_scope_args(
                schema=schema_name or None,
                schemas=schemas_list,
                all_schemas=all_schemas,
            )
        except ValueError as exc:
            return f"Invalid scope: {exc}"

        data = _build_dict(
            name=name,
            type=type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            schema_name=schema_name,
        )
        ds = DatasourceConfig.model_validate({**data, "granularities": granularities or []})
        existed = await storage.get_datasource(name) is not None
        try:
            await storage.save_datasource(ds)
        except ValueError as exc:
            return f"Cannot create datasource: {exc}"
        verb = "replaced" if existed else "created"
        # Re-read: storage resolves ${ENV_VAR} credentials.
        ds = await storage.get_datasource(name) or ds
        client = engine.client_for(ds)

        ok, msg = await _test_connection(client)
        if not ok:
            return f"Datasource '{ds.name}' {verb}, but connection test failed.\n{msg}"

        lines = [f"Datasource '{ds.name}' {verb}. {msg}"]

        if not auto_ingest:
            return "\n".join(lines)

        try:
            # Via the module so tests can monkeypatch the seam.
            result = await engine_ingestion.ingest_datasource_idempotent(
                datasource=ds,
                storage=storage,
                client=client,
                schema=schema_name or None,
                schemas=schemas_list,
                all_schemas=all_schemas,
            )
        except _DB_FAILURES as e:
            lines.append(f"Auto-ingestion failed: {_friendly_db_error(e)}")
            return "\n".join(lines)

        lines.append(await _render_ingest_result(result, schema_name=schema_name or "", ds=ds, client=client))
        if any(a.created for a in result.additions):
            lines.extend(["", "Use datasets_summary and inspect to explore, then query to fetch data."])
        return "\n".join(lines)

    @tool()
    async def list_datasources() -> str:
        """List all configured database connections (names and types only, credentials are not shown). Use describe_datasource for connection details and status."""
        names = await storage.list_datasources()
        # Delegates to the shared renderer (also used by inspect).
        pairs: list[tuple[str, str | None]] = []
        for name in names:
            try:
                ds = await storage.get_datasource(name)
                pairs.append((name, ds.type if ds else "unknown"))
            except Exception as exc:
                logger.warning(_DS_LOAD_FAILED, name, exc)
                pairs.append((name, None))
        return render_datasource_list(pairs=pairs, fmt="markdown")

    @tool()
    async def describe_datasource(
        name: Annotated[str, Field(description="Datasource name (from list_datasources).")],
        list_tables: Annotated[bool, Field(description="Also list the tables of schema_name (default true).")] = True,
        schema_name: Annotated[str, Field(
            description='Schema whose tables to list, e.g. "public"; empty is the default schema.',
        )] = "",
    ) -> str:
        """Show datasource details: connection status, available schemas, and (by default) the tables in the given or default schema.

        Use it after create_datasource to verify the connection and see what is queryable before ingest_datasource.
        """
        try:
            ds = await storage.get_datasource(name)
        except Exception as exc:
            logger.warning(_DS_LOAD_FAILED, name, exc)
            return f"Datasource '{name}' has an invalid config."
        if ds is None:
            return f"Datasource '{name}' not found."

        lines = [f"Datasource: {ds.name}"]
        if ds.type:
            lines.append(f"Type: {ds.type}")
        if ds.host:
            lines.append(f"Host: {ds.host}")
        if ds.port:
            lines.append(f"Port: {ds.port}")
        if ds.database:
            lines.append(f"Database: {ds.database}")
        if ds.username:
            lines.append(f"Username: {ds.username}")
        if ds.connection_string:
            lines.append("Connection string: (set)")

        client = engine.client_for(ds)
        ok, msg = await _test_connection(client)
        lines.append(f"\nConnection: {'OK' if ok else 'FAILED'}")
        if not ok:
            lines.append(msg)
            return "\n".join(lines)

        schemas = await _get_schemas(client)
        if schemas:
            lines.append(f"Available schemas: {', '.join(schemas)}")

        if list_tables:
            tables, err = await _fetch_tables(client, schema_name=schema_name or None)
            schema_label = f" in schema '{schema_name}'" if schema_name else ""
            if err is not None:
                lines.append(f"\nTables{schema_label}: (error — {err})")
            elif tables:
                lines.append(f"\nTables ({len(tables)}){schema_label}:")
                for o in tables:
                    # Label non-table objects (views/matviews) explicitly.
                    suffix = "" if o.kind == "table" else f" ({o.kind})"
                    lines.append(f"  - {o.name}{suffix}")
                lines.append(
                    "\nUse ingest_datasource to create datasets from these tables."
                )
            else:
                lines.append(f"\nNo tables found{schema_label}.")

        return "\n".join(lines)

    @tool()
    async def edit_datasource(
        name: Annotated[str, Field(description="Datasource to update.")],
        description: Annotated[str | None, Field(description="New datasource description.")] = None,
        granularities: Annotated[list[CustomGranularity] | None, Field(
            description=(
                "Replace the custom time granularities, each {name, base, multiple, origin}: buckets "
                "start at origin + k * multiple * base."
            ),
        )] = None,
    ) -> str:
        """Update a datasource's metadata."""
        ds = await storage.get_datasource(name)
        if ds is None:
            return f"Datasource '{name}' not found."

        old_description = ds.description
        update: dict[str, Any] = {}
        if description is not None:
            update["description"] = description
        if granularities is not None:
            update["granularities"] = granularities
        ds = ds.model_copy(update=update)

        try:
            await storage.save_datasource(ds)
        except ValueError as exc:
            return f"Cannot update datasource '{name}': {exc}"

        # The embedding text includes the description, so refresh it inline on
        # a description change. Post-save and best-effort — warn and report
        # partial success rather than failing the already-committed save.
        refresh_warning: str | None = None
        if description is not None and description != old_description:
            datasets_in_ds = DocumentLoadFailures().skip(await storage.load_datasets(data_source=name))
            try:
                await search_service.refresh_datasource(
                    name=name,
                    datasets=datasets_in_ds,
                    description=ds.description,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort post-save refresh
                logger.warning(
                    "edit_datasource refresh failed for %r: %s", name, exc,
                )
                refresh_warning = str(exc)
        if refresh_warning:
            return (
                f"Datasource '{name}' updated. "
                f"Warning: embedding refresh failed: {refresh_warning}"
            )
        return f"Datasource '{name}' updated."

    # Delete operations

    @tool()
    async def delete_dataset(
        name: Annotated[str, Field(description="Dataset to delete.")],
        data_source: Annotated[str | None, Field(
            description="The dataset's datasource; required when the name exists in several datasources.",
        )] = None,
    ) -> str:
        """Delete a semantic dataset."""
        try:
            deleted = await storage.delete_dataset(name, data_source=data_source)
        except AmbiguousDatasetError as exc:
            return _ambiguous_with_mcp_hint(exc)
        if deleted:
            return f"Dataset '{name}' deleted."
        return f"Dataset '{name}' not found."

    async def _refreshed_documents(data_source: str) -> list[str]:
        """Best-effort search refresh of the datasource's semantic models after a committed write."""
        try:
            return await search_service.refresh_documents(data_source)
        except Exception as exc:  # noqa: BLE001 — best-effort post-save refresh
            logger.warning("semantic model search refresh failed for %r: %s", data_source, exc)
            return [f"search refresh failed: {exc}"]

    @tool()
    async def create_semantic_model(
        name: Annotated[str, Field(description="The semantic model's name, unique in its datasource.")],
        data_source: Annotated[str, Field(description="The datasource it describes (from list_datasources).")],
        content: Annotated[str | dict[str, Any] | None, Field(
            description="A whole Ossie 0.3.0.dev0 semantic-model document, as YAML/JSON text or an object.",
        )] = None,
        datasets: Annotated[list[str] | None, Field(
            description="Standalone datasets of the datasource to move into the new semantic model, instead of content.",
        )] = None,
    ) -> str:
        """Create a semantic model: an Ossie document holding datasets, their relationships and model-scoped metrics (metrics a query names bare). Give its Ossie content, or the standalone datasets it takes over (they keep their names)."""
        try:
            await storage.create_semantic_model(data_source=data_source, name=name, content=content, datasets=datasets)
        except Exception as exc:
            return f"Error creating semantic model '{name}': {type(exc).__name__}: {exc}"
        return f"Semantic model '{name}' created." + "".join(f"\nWarning: {w}" for w in await _refreshed_documents(data_source))

    @tool()
    async def edit_semantic_model(
        name: Annotated[str, Field(description="Semantic model to edit.")],
        data_source: Annotated[str, Field(description="Its datasource.")],
        description: Annotated[str | None, Field(description="New description of the semantic model.")] = None,
        ai_context: Annotated[str | dict[str, Any] | None, Field(
            description="New AI context: instructions text, or {instructions, synonyms, examples}.",
        )] = None,
        metrics: Annotated[list[dict[str, Any]] | None, Field(
            description='Model-scoped metrics to add or replace, {"name", "expression", "description", "ai_context"}; '
                        'the expression reads like a query: qualified fields inside aggregates, AGG(dataset.metric), '
                        'other model-scoped metrics by bare name.',
        )] = None,
        remove_metrics: Annotated[list[str] | None, Field(description="Model-scoped metrics to remove.")] = None,
        relationships: Annotated[list[dict[str, Any]] | None, Field(
            description='Relationships to add or replace, {"name", "from", "to", "from_columns", "to_columns"} '
                        '(many-to-one from "from").',
        )] = None,
        remove_relationships: Annotated[list[str] | None, Field(description="Relationships to remove, by name.")] = None,
        add_datasets: Annotated[list[str] | None, Field(
            description="Standalone datasets of the datasource to move into the semantic model.",
        )] = None,
    ) -> str:
        """Edit a semantic model in place: its description and AI context, its model-scoped metrics and relationships, and standalone datasets moved into it. Edit its datasets with edit_dataset."""
        edits = {k: v for k, v in {"description": description, "ai_context": ai_context}.items() if v is not None}
        try:
            await storage.edit_semantic_model(
                name, data_source=data_source, metrics=metrics, remove_metrics=remove_metrics,
                relationships=relationships, remove_relationships=remove_relationships, add_datasets=add_datasets,
                **edits,
            )
        except Exception as exc:
            return f"Error editing semantic model '{name}': {type(exc).__name__}: {exc}"
        return f"Semantic model '{name}' edited." + "".join(f"\nWarning: {w}" for w in await _refreshed_documents(data_source))

    @tool()
    async def delete_semantic_model(
        name: Annotated[str, Field(description="Semantic model to delete.")],
        data_source: Annotated[str | None, Field(
            description="Its datasource; required when the name exists in several datasources.",
        )] = None,
    ) -> str:
        """Delete a semantic model together with all its datasets and model-scoped metrics."""
        try:
            deleted = await storage.delete_semantic_model(name, data_source=data_source)
        except AmbiguousDatasetError as exc:
            return _ambiguous_with_mcp_hint(exc)
        return f"Semantic model '{name}' deleted." if deleted else f"Semantic model '{name}' not found."

    @tool()
    async def validate_datasets(
        data_source: Annotated[str | None, Field(description="Datasource to validate; omit for all.")] = None,
    ) -> str:
        """Diff stored datasets against the live database schemas.

        Returns, as JSON, the deletes (fields, metrics, relationships, filters, whole datasets) needed to keep the stored datasets valid. Read-only.
        """
        if data_source is not None:
            # Fail loudly on an unknown name — an empty result is otherwise
            # indistinguishable from "no drift".
            ds = await storage.get_datasource(data_source)
            if ds is None:
                return f"Datasource '{data_source}' not found."
        # Reuse the closure engine so its schema-drift SQL client is cached and
        # disposed at teardown.
        try:
            entries = await engine.validate_datasets(data_source=data_source)
        except _DB_FAILURES as exc:
            raise _db_tool_failure(exc) from exc
        return json.dumps([e.model_dump(mode="json") for e in entries], indent=2)

    @tool()
    async def recommend_population(
        items: Annotated[list[str], Field(
            description="Entity references: orders.revenue, customers.name, sum(orders.revenue), a saved metric's bare name, ...",
        )],
        data_source: Annotated[str | None, Field(
            description="Datasource scope; omitted, names resolve via the datasource priority list.",
        )] = None,
        root_hint: Annotated[str | None, Field(
            description=(
                "Intended root (dataset or <data_source>.<dataset>); honoured when it reaches every item, "
                "else the automatic pick is used with a warning."
            ),
        )] = None,
        format: Annotated[str, Field(description='"markdown" (default) or "json".')] = "markdown",  # noqa: A002
    ) -> str:
        """Recommend the population (a query's root dataset) for a set of dataset.field / metric items, with each item's reference path from that root, ready to drop into the query (a joined field comes back as customers.regions.name, a root-owned one as status; aggregations are kept).

        The root is the dataset from which every item is reachable with the fewest relationship hops. With at least one field item, an aggregated item on a dataset with no relationship path to the root does not block it: its path is flagged unrelated (aggregated over that dataset's own rows). When none reaches everything, population is null and coverage lists the best partial roots for a multi-stage query.

        Call it once the item list is final; explore with search / inspect first.
        """
        fmt = format.lower().strip()
        if fmt not in ("markdown", "json"):
            return (
                f"recommend_population failed: unknown format '{format}'. "
                f"Use 'markdown' or 'json'."
            )
        # Reuse the closure engine (see validate_datasets above).
        try:
            rec = await engine.recommend_population(
                items, data_source=data_source, root_hint=root_hint
            )
        except AmbiguousDatasetError as exc:
            return _ambiguous_with_mcp_hint(exc)
        except ValueError as exc:
            return f"recommend_population failed: {exc}"
        if fmt == "json":
            return json.dumps(rec.model_dump(mode="json"), indent=2)
        return render_recommendation_markdown(rec)

    @tool()
    async def delete_datasource(name: Annotated[str, Field(description="Datasource to delete.")]) -> str:
        """Delete a datasource configuration."""
        if await storage.delete_datasource(name):
            return f"Datasource '{name}' deleted."
        return f"Datasource '{name}' not found."

    # Ingestion

    @tool()
    async def ingest_datasource(
        datasource_name: Annotated[str, Field(description="An existing datasource (from list_datasources).")],
        include_tables: Annotated[str, Field(description="Comma-separated tables to ingest; empty for all.")] = "",
        schema_name: Annotated[str, Field(description='One schema, e.g. "public"; empty is the default schema.')] = "",
        schemas: Annotated[str, Field(
            description="Comma-separated schemas; excludes schema_name and all_schemas.",
        )] = "",
        all_schemas: Annotated[bool, Field(
            description="Every non-system schema; excludes schema_name and schemas.",
        )] = False,
    ) -> str:
        """Discover a database's tables and create or additively update datasets from them.

        Idempotent and additive: new fields and relationships are appended; existing definitions are never overwritten. Also returns the pending validate_datasets deletes.
        """

        ds = await storage.get_datasource(datasource_name)
        if ds is None:
            return f"Datasource '{datasource_name}' not found."

        schemas_list = [s.strip() for s in schemas.split(",") if s.strip()] or None
        try:
            validate_scope_args(
                schema=schema_name or None,
                schemas=schemas_list,
                all_schemas=all_schemas,
            )
        except ValueError as e:
            return f"Invalid scope: {e}"

        client = engine.client_for(ds)
        try:
            include = [t.strip() for t in include_tables.split(",") if t.strip()] or None
            result = await engine_ingestion.ingest_datasource_idempotent(
                datasource=ds,
                storage=storage,
                client=client,
                include_tables=include,
                schema=schema_name or None,
                schemas=schemas_list,
                all_schemas=all_schemas,
            )
        except _DB_FAILURES as e:
            raise _db_tool_failure(e) from e

        return await _render_ingest_result(
            result, schema_name=schema_name, ds=ds, client=client,
        )

    @tool()
    async def set_datasource_priority(
        priority: Annotated[list[str], Field(
            description="Existing datasource names, most preferred first; an empty list clears it.",
        )],
    ) -> str:
        """Set how a bare dataset name that exists in several datasources resolves: the first datasource in this list that has it wins; otherwise the name is ambiguous and errors."""
        try:
            await storage.set_datasource_priority(list(priority))
        except ValueError as exc:
            return str(exc)
        if not priority:
            return "Datasource priority cleared."
        return f"Datasource priority set: {list(priority)}."

    @tool()
    async def get_datasource_priority() -> str:
        """Return the datasource priority list (most preferred first), or [] when none is set."""
        priority = await storage.get_datasource_priority()
        return f"Datasource priority: {priority}"

    # Unified Memory surface

    memory_service = MemoryService(storage=storage)

    def _format_resolution_error(exc: Exception) -> str:
        """Convert a typed resolution / not-found / ambiguous error into
        a friendly text response (matches the existing convention of
        never raising back to the agent)."""
        if isinstance(exc, AmbiguousDatasetError):
            return _ambiguous_with_mcp_hint(exc)
        prefix = f"{type(exc).__name__}: "
        return f"Error: {exc}" if str(exc).startswith(prefix) else f"Error: {prefix}{exc}"

    @tool()
    async def save_memory(
        learning: Annotated[str, Field(description="The note text; required, non-empty.")],
        linked_entities: Annotated[Any, Field(
            description=(
                "A list of entity references (resolved to <datasource>.<dataset>[.<leaf>]; memory:<id> "
                "links another memory), or a query object whose entities are extracted and which is "
                "stored as an example query."
            ),
        )],
        id: Annotated[str | None, Field(  # noqa: A002 — MCP arg name
            description=(
                'Stable memory id such as "kb.policy.42" (no : / ? # or whitespace); an existing id '
                "is overwritten. Omit to allocate one."
            ),
        )] = None,
        description: Annotated[str | None, Field(
            description="One-line preview (at most 500 chars) shown by search and compact inspect.",
        )] = None,
    ) -> str:
        """Save an agent memory: a free-form note plus the SLayer entities it concerns.

        With a list of entity references the memory appears among search's memories; with a query object it appears among search's example queries. Returns the memory_id, the canonical entities stored and any warnings. Deleting a dataset, datasource or metric strips references to it from every memory; the note itself is kept.

        Examples:
        save_memory(learning="orders.is_returned in {0,1,NULL}; treat NULL as not returned", linked_entities=["orders.is_returned"])
        save_memory(learning="Paid revenue by status", linked_entities={"population": "orders", "metrics": ["sum(orders.amount)"], "filter": "orders.status = 'paid'"}, id="kb.paid-revenue")
        """
        try:
            response = await memory_service.save_memory(
                learning=learning,
                linked_entities=linked_entities,
                id=id,
                description=description,
            )
        except ValueError as exc:  # EntityResolutionError / AmbiguousDatasetError included
            return _format_resolution_error(exc)
        return response.model_dump_json(indent=2)

    @tool()
    async def forget_memory(
        id: Annotated[Any, Field(description="The memory_id returned by save_memory (a legacy int is accepted).")],  # noqa: A002 — MCP arg name
    ) -> str:
        """Delete a memory by id; other memories' memory:<id> links to it are removed."""
        try:
            response = await memory_service.forget_memory(identifier=id)
        except (
            MemoryNotFoundError,
            ValueError,
        ) as exc:
            return _format_resolution_error(exc)
        return response.model_dump_json(indent=2)

    # Semantic search. Pass the engine so the search service's post-fusion
    # field-hit hook can auto-refresh stale categorical fields.
    search_service = SearchService(storage=storage, engine=engine)

    @tool()
    async def search(
        entities: Annotated[list[str] | None, Field(
            description="Entity references; ranks memories tagged with overlapping entities.",
        )] = None,
        query: Annotated[Any, Field(
            description="A query object whose entities are extracted and searched like entities.",
        )] = None,
        question: Annotated[str | None, Field(
            description="Free text, matched by full text (and by embeddings when configured) over memories and entities.",
        )] = None,
        datasource: Annotated[str | None, Field(description="Restrict every hit to this datasource.")] = None,
        max_results: Annotated[int, Field(description="Maximum hits returned (default 10).")] = 10,
        cypher_filter: Annotated[str | None, Field(
            description=(
                "openCypher MATCH ... RETURN <node>.id AS id pre-filtering hits to the returned ids, e.g. "
                "MATCH (n:DatasetField) RETURN n.id AS id to spend max_results on fields only. Labels: "
                "Memory, Datasource, Dataset, DatasetField, Metric, Aggregation; without the "
                "advanced_search extra only such label filters work."
            ),
        )] = None,
        compact: Annotated[bool, Field(
            description="Default true: one-line hits. False returns full renders; avoid it for broad searches.",
        )] = True,
    ) -> str:
        """Search saved memories, example queries and entities (datasets, fields, metrics, ...) by entity overlap and by meaning.

        Call it before `query` to surface notes and example queries saved against the entities you plan to use. Discovery, not detail: hits are one-line descriptions; read the ones you need with `inspect`, batching same-kind ids in one call.

        Hits from all channels are fused into one ranked list. With no input it returns the newest memories, with a warning.
        """
        try:
            response = await search_service.search(
                entities=entities,
                query=query,
                question=question,
                datasource=datasource,
                max_results=max_results,
                cypher_filter=cypher_filter,
                compact=compact,
            )
        except (SlayerError, ValueError) as exc:
            return _format_resolution_error(exc)
        return response.model_dump_json(indent=2)

    return mcp


def _warning_lines(notes: list[SlayerWarning]) -> str:
    return "".join(f"\nWarning: {n.human_message()}" for n in notes)


def _build_dict(**kwargs: Any) -> dict[str, Any]:
    """Build a dict from keyword arguments, excluding None values."""
    return {k: v for k, v in kwargs.items() if v is not None}


def _format_table(data: list[dict[str, Any]], columns: list[str], max_rows: int = 50) -> str:
    """Format data as a pipe-separated table (used for sample data display)."""
    if not data:
        return "No results."

    truncated = len(data) > max_rows
    rows = data[:max_rows]

    header = " | ".join(columns)
    separator = " | ".join("-" * len(c) for c in columns)
    body_lines = []
    for row in rows:
        body_lines.append(" | ".join(encode_text(row.get(c)) for c in columns))

    result = f"{header}\n{separator}\n" + "\n".join(body_lines)
    if truncated:
        result += f"\n... ({len(data)} total rows, showing first {max_rows})"
    return result


def _json_metadata(result: SlayerResponse) -> dict[str, Any]:
    """Warnings, attributes and inferred population as json payload keys, empty ones omitted."""
    meta: dict[str, Any] = {}
    if result.warnings:
        meta["warnings"] = [w.model_dump(mode="json") for w in result.warnings]
    attributes = _json_attributes(result.attributes)
    if attributes:
        meta["attributes"] = attributes
    if result.population_inferred:
        if result.population is not None:
            meta["population"] = result.population
        meta["population_inferred"] = True
        meta["row_sources"] = list(result.row_sources)
    return meta


def _text_envelope(result: SlayerResponse, fmt: str, body: str) -> str:
    """``body`` wrapped in the metadata: leading `#` lines for csv, footers for markdown."""
    population = _inferred_population(result)
    sources = result.row_sources if result.population_inferred else []
    if fmt == "csv":
        return (
            _population_comment(population)
            + ("" if not sources else f"# row sources: {', '.join(sources)}\n")
            + _csv_attribute_comments(result)
            + _csv_warning_comments(result)
            + body
        )
    return (
        body
        + _attributes_footer(result.attributes)
        + _population_footer(population)
        + _row_sources_footer(sources, after_population=population is not None)
        + _format_warnings(result)
    )


def _inferred_population(result: SlayerResponse) -> str | None:
    return result.population if result.population_inferred else None


def _format_csv(data: list[dict[str, Any]], columns: list[str]) -> str:
    """Format data as CSV."""
    if not data:
        return ""
    lines = [",".join(columns)]
    for row in data:
        values = []
        for c in columns:
            v = encode_text(row.get(c))
            if "," in v or '"' in v or "\n" in v:
                v = '"' + v.replace('"', '""') + '"'
            values.append(v)
        lines.append(",".join(values))
    return "\n".join(lines)


def _cap_rows(result: SlayerResponse, *, hint: str) -> None:
    """Slice past-cap rows and append the truncation notice. No-limit paths only."""
    if len(result.data) <= _MCP_ROW_CAP:
        return
    result.data = result.data[:_MCP_ROW_CAP]
    result.warnings = [
        *result.warnings,
        ResponseTruncationWarning(returned_rows=_MCP_ROW_CAP, hint=hint),
    ]


def _cap_leaf(query: "SlayerQuery | dict") -> "tuple[SlayerQuery | dict, bool]":
    """Push ``limit = cap + 1`` into one query object with no limit; returns
    ``(query_or_capped, capped)`` and never mutates the caller's input."""
    limit = query.limit if isinstance(query, SlayerQuery) else query.get("limit")
    if limit is not None:
        return query, False
    capped = (
        query.model_copy(update={"limit": _MCP_ROW_CAP + 1})
        if isinstance(query, SlayerQuery)
        else {**query, "limit": _MCP_ROW_CAP + 1}
    )
    return capped, True


def _apply_mcp_row_cap(
    query: "str | SlayerQuery | dict | Sequence[SlayerQuery | dict]",
) -> "tuple[str | SlayerQuery | dict | list[SlayerQuery | dict], bool, str]":
    """Push the default row cap into the root query when the caller set no limit.

    Returns ``(query_to_execute, capped, hint)``. A run-by-name string is opaque,
    so it can't be pushed down — cap response-side. A single query object or the
    root (last) stage of a multi-stage list gets ``limit = cap + 1`` so truncation
    is detectable; an explicit limit is trusted verbatim.
    """
    if isinstance(query, str):
        return query, True, _CAP_HINT
    if isinstance(query, (SlayerQuery, dict)):
        capped_query, capped = _cap_leaf(query)
        return capped_query, capped, _CAP_HINT
    stages = list(query)
    if stages:
        capped_root, capped = _cap_leaf(stages[-1])
        return [*stages[:-1], capped_root], capped, _NESTED_CAP_HINT
    return stages, False, _CAP_HINT


def _csv_warning_comments(result: SlayerResponse) -> str:
    """Warnings as leading `#` comment lines for CSV output (uniform column count)."""
    lines = [f"# warning: {w.human_message()}" for w in (result.warnings or [])]
    return "" if not lines else "\n".join(lines) + "\n"


def _format_warnings(result: SlayerResponse) -> str:
    """Advisories appended to text output, rendered via each payload's human_message."""
    lines = [f"  - {w.human_message()}" for w in (result.warnings or [])]
    return "" if not lines else "\n\nWarnings:\n" + "\n".join(lines)


def _format_output(result: SlayerResponse, fmt: str) -> str:
    """Rows per ``fmt``; json is a bare array until metadata makes it {"data", ...}."""
    if fmt == "json":
        meta = _json_metadata(result)
        return encode_json({"data": result.data, **meta} if meta else result.data)
    body = (
        _format_csv(data=result.data, columns=result.columns)
        if fmt == "csv"
        else result.to_markdown()
    )
    return _text_envelope(result=result, fmt=fmt, body=body)


def _format_dry_run(result: SlayerResponse, fmt: str) -> str:
    """The generated SQL with the run's metadata, shaped per ``fmt``."""
    if fmt == "json":
        return encode_json({"sql": result.sql, **_json_metadata(result)})
    return _text_envelope(result=result, fmt=fmt, body=f"SQL:\n{result.sql}")


def _population_footer(population: str | None) -> str:
    """Trailing note naming the inferred population (markdown), or empty."""
    return "" if population is None else f"\n\nPopulation: {population} (inferred)"


def _row_sources_footer(sources: list[str], *, after_population: bool) -> str:
    """Trailing note listing the row sources (markdown), its own paragraph unless it follows the population's."""
    if not sources:
        return ""
    separator = "\n" if after_population else "\n\n"
    return f"{separator}Row sources: {', '.join(sources)}"


def _population_comment(population: str | None) -> str:
    """Leading `#` note naming the inferred population (csv), or empty."""
    return "" if population is None else f"# population: {population} (inferred)\n"


def _format_field_meta(entries: dict[str, FieldMetadata]) -> list[str]:
    """Format a dict of field metadata entries into lines."""
    lines = []
    for col, fm in entries.items():
        parts = []
        if fm.label:
            parts.append(f"label={fm.label}")
        if fm.format:
            fmt_parts = [f"type={fm.format.type.value}"]
            if fm.format.precision is not None:
                fmt_parts.append(f"precision={fm.format.precision}")
            if fm.format.symbol is not None:
                fmt_parts.append(f"symbol={fm.format.symbol}")
            parts.append(f"format=({', '.join(fmt_parts)})")
        if parts:
            lines.append(f"  {col}: {', '.join(parts)}")
    return lines


def _format_attributes(attributes: ResponseAttributes) -> str:
    """Format response attributes as a compact section."""
    lines = []
    dim_lines = _format_field_meta(attributes.dimensions)
    if dim_lines:
        lines.append("Dimension attributes:")
        lines.extend(dim_lines)
    metric_lines = _format_field_meta(attributes.metrics)
    if metric_lines:
        lines.append("Metric attributes:")
        lines.extend(metric_lines)
    return "\n".join(lines) if lines else ""


def _has_attributes(attributes: ResponseAttributes | None) -> bool:
    return bool(attributes and (attributes.dimensions or attributes.metrics))


def _attributes_footer(attributes) -> str:
    """Attributes block as a trailing footer, or empty when there's nothing to show."""
    if _has_attributes(attributes):
        return "\n\n" + _format_attributes(attributes=attributes)
    return ""


def _csv_attribute_comments(result: SlayerResponse) -> str:
    """Field attributes as leading `#` comment lines for CSV (never trailing rows)."""
    if not _has_attributes(result.attributes):
        return ""
    block = _format_attributes(attributes=result.attributes)
    return "\n".join(f"# {line}" for line in block.splitlines()) + "\n"


def _json_attributes(attributes) -> dict[str, Any] | None:
    """Structured attributes for the json payload, or None when there's nothing to show."""
    if _has_attributes(attributes):
        return attributes.model_dump(mode="json")
    return None
