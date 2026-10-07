"""MCP server for SLayer."""

import json
import logging
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from collections.abc import Callable, Sequence
from inspect import cleandoc
from typing import Annotated, Any

import sqlalchemy as sa
from pydantic import Field
from sqlalchemy.exc import DatabaseError

from slayer import __version__

from slayer.core.errors import (
    AmbiguousModelError,
    EntityResolutionError,
    MemoryNotFoundError,
    SlayerError,
)
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.granularity import CustomGranularity
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.core.recommend import render_recommendation_markdown
from slayer.core.warnings import ResponseTruncationWarning
from slayer import async_utils
from slayer.engine import ingestion as engine_ingestion
from slayer.engine.ingestion import (
    _empty_ingest_message as _shared_empty_ingest_message,
    _friendly_db_error,
    _get_schemas,
    _hidden_internal_line,
    _unhide_hint,
    IngestableObject,
    list_ingestable_objects,
)
from slayer.engine.schema_scope import schema_ref_from_token, validate_scope_args
from slayer.sql import engine_factory
from slayer.search.service import handle_edit_refresh
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.memories.help_seed import seed_help_memories
from slayer.inspect.model_render import (  # noqa: F401 — re-exported for backward-compat (tests + other modules import these names from slayer.mcp.server)
    _build_sample_query_args,
    _escape_md_cell,
    _format_meta,
    _get_row_count,
    _markdown_table,
    _md_code_span,
    _render_inspect_footer,
    _resolve_inspect_sections,
    _source_type_for,
    _strip_model_prefix,
    _truncate_description,
    render_model_inspection,
)
from slayer.inspect.collection_render import (
    render_datasource_list,
    render_models_summary,
)
from slayer.inspect.service import InspectService
from slayer.memories.service import MemoryService
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures

logger = logging.getLogger(__name__)

VALID_DIMENSION_TYPES = {"string", "time", "date", "boolean", "number"}
_UNSET: Any = object()  # Sentinel to distinguish "not provided" from "explicitly set to None"

# Response row cap when the caller passes no limit; an explicit limit is trusted verbatim.
_MCP_ROW_CAP = 20
_CAP_HINT = "pass a higher 'limit' to get more rows"
_DS_LOAD_FAILED = "Failed to load datasource '%s': %s"
_NESTED_CAP_HINT = "pass a higher 'limit' on the root query to get more rows"

# Shared remedy for every mcp-import failure below.
_MCP_REMEDY = (
    "Install a supported version: pip install 'mcp>=1.19,<2' "
    "(or upgrade SLayer, which pins mcp<2: pip install -U motley-slayer)."
)

# Claude Code's default cap on each tool description and on the server instructions.
MAX_TOOL_DESCRIPTION_CHARS = 2048

SERVER_INSTRUCTIONS = """SLayer is a semantic layer for querying databases: describe the data you want (measures, dimensions, filters) and SLayer writes the SQL, joins included.
One SLayer query can compute shares of a group or of the grand total, ranks and top N per group, period-over-period changes, running totals, trailing windows, joined models' aggregates without double counting, and aggregates of aggregates. Do not assemble an answer from several queries or by processing rows yourself unless one query cannot express it.
The query syntax is in the descriptions of the query tool's input fields. Overview: inspect(reference='memory:help.intro', entity_type='memory'). Full reference: inspect(reference=['memory:help.aggregations', 'memory:help.transforms', 'memory:help.time', 'memory:help.joins', 'memory:help.queries'], entity_type='memory').
Typical workflow: search(question='...') → inspect the model → query.
To connect a new database: create_datasource → describe_datasource (verify + list tables) → ingest_datasource_models → models_summary."""

_ALWAYS_LOAD_META = {"anthropic/alwaysLoad": True}


def _check_budget(*, what: str, text: str) -> None:
    if len(text) > MAX_TOOL_DESCRIPTION_CHARS:
        raise ValueError(f"{what} is {len(text)} chars, over the {MAX_TOOL_DESCRIPTION_CHARS}-char budget.")


def _agent_description(fn: Callable[..., Any]) -> str:
    """``fn``'s dedented docstring, checked against the description budget."""
    text = cleandoc(fn.__doc__ or "")
    _check_budget(what=f"MCP tool {fn.__name__!r} description", text=text)
    return text


def _mcp_major(version_str: str) -> int | None:
    """Best-effort major-version parse; ``None`` when unparseable."""
    try:
        return int(version_str.split(".", 1)[0].strip())
    except ValueError:
        return None


def _import_fastmcp():
    """Return the mcp 1.x ``FastMCP`` class, or raise an actionable ImportError.

    Absent package and wrong major (mcp 2.x dropped the module) get different remedies.
    """
    try:
        from mcp.server.fastmcp import FastMCP  # ALLOW(import-not-top): optional-dep probe — must attempt the import at call time to diagnose absent vs wrong-major
    except ImportError as exc:
        try:
            installed = _pkg_version("mcp")
        except PackageNotFoundError:
            raise ImportError(f"MCP package not found. {_MCP_REMEDY}") from exc
        major = _mcp_major(installed)
        if major is not None and major >= 2:
            detail = (
                f"mcp {installed} is installed, but SLayer targets the mcp 1.x "
                f"FastMCP API: mcp 2.x dropped 'mcp.server.fastmcp' (renamed to "
                f"mcp.server.mcpserver.MCPServer)."
            )
        else:
            # 1.x that failed for another reason — report that, not the rename.
            detail = (
                f"mcp {installed} is installed, but 'mcp.server.fastmcp' could "
                f"not be imported: {exc}"
            )
        raise ImportError(f"{detail} {_MCP_REMEDY}") from exc
    return FastMCP


def _set_server_version(mcp) -> None:
    """Stamp SLayer's version onto the lowlevel MCP server (best-effort; cosmetic)."""
    lowlevel = getattr(mcp, "_mcp_server", None)
    if lowlevel is None:
        logger.debug("MCP server exposes no _mcp_server; leaving serverInfo.version")
        return
    try:
        lowlevel.version = __version__
    except AttributeError:
        logger.debug("MCP serverInfo.version is read-only; leaving it", exc_info=True)


def _ambiguous_with_mcp_hint(exc: AmbiguousModelError) -> str:
    """Render an ``AmbiguousModelError`` with an MCP-specific remediation hint."""
    return (
        f"{exc} Pass data_source=... to this tool, or use the "
        f"set_datasource_priority tool to set a priority."
    )


def _test_connection(ds: DatasourceConfig) -> tuple[bool, str]:
    """Test a datasource connection. Returns (success, message)."""
    try:
        engine = engine_factory.get_engine(ds.resolve_env_vars())
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        # Cached engine — engine_factory owns lifecycle; don't dispose.
        return True, "Connection successful."
    except Exception as e:
        return False, _friendly_db_error(e)


def _fetch_tables(
    ds: DatasourceConfig, schema_name: str | None = None,
) -> tuple[list[IngestableObject] | None, str | None]:
    """Inspect a datasource's table and view objects (name + kind).

    Returns (objects, None) or (None, friendly_error). schema_name=None uses the
    default schema. Views are always included so a views-only schema isn't empty.
    """
    try:
        sa_engine = engine_factory.get_engine(ds.resolve_env_vars())
        inspector = sa.inspect(sa_engine)
        # Route through a SchemaRef so schema_name=None resolves to the
        # catalog-qualified default rather than sweeping every schema.
        ref = (
            schema_ref_from_token(
                schema_name, dialect_name=sa_engine.dialect.name,
                requested=schema_name,
            )
            if schema_name
            else None
        )
        objects = list_ingestable_objects(
            inspector=inspector, ref=ref, include_views=True
        )
        return sorted(objects, key=lambda o: o.name), None
    except Exception as e:
        if isinstance(e, DatabaseError):
            return None, _friendly_db_error(e)
        return None, str(e)


def _empty_ingest_message(*, schema_name: str, ds: DatasourceConfig) -> str:
    """Agent-facing wrapper over the shared engine renderer."""
    return _shared_empty_ingest_message(
        schema_name=schema_name,
        ds=ds,
        retry_hint=(
            "Try: ingest_datasource_models with schema_name set to one of these."
        ),
    )


def _addition_has_changes(a: Any) -> bool:
    """True when a non-created addition carries any change worth rendering."""
    return bool(
        a.new_columns
        or a.new_joins
        or getattr(a, "widened_columns", None)
        or getattr(a, "kind_change", None)
        or getattr(a, "described_columns", None)
        or getattr(a, "model_described", False)
    )


def _render_new_models_section(new_models: list[Any]) -> list[str]:
    if not new_models:
        return []
    lines = [f"Created {len(new_models)} new model(s):"]
    for a in new_models:
        described = getattr(a, "described_columns", []) or []
        suffix = f", {len(described)} described" if described else ""
        lines.append(
            f"- {a.model_name} ({len(a.new_columns)} columns, "
            f"{len(a.new_joins)} joins{suffix})"
        )
    return lines


def _addition_update_details(a: Any) -> list[str]:
    """Detail fragments for one non-created addition, in CLI-renderer order."""
    details = []
    if a.new_columns:
        details.append(f"+columns: {', '.join(a.new_columns)}")
    if a.new_joins:
        details.append(f"+joins: {', '.join(a.new_joins)}")
    widened = getattr(a, "widened_columns", []) or []
    if widened:
        details.append(f"widened: {', '.join(widened)}")
    kind_change = getattr(a, "kind_change", None)
    if kind_change:
        details.append(f"source_kind: {kind_change}")
    described = getattr(a, "described_columns", []) or []
    if described:
        details.append(f"+descriptions: {', '.join(described)}")
    if getattr(a, "model_described", False):
        details.append("+model description")
    return details


def _render_updated_section(updated: list[Any]) -> list[str]:
    if not updated:
        return []
    lines = [f"Updated {len(updated)} existing model(s):"]
    for a in updated:
        lines.append(f"- {a.model_name} ({'; '.join(_addition_update_details(a))})")
    return lines


def _render_unchanged_section(unchanged: list[Any]) -> list[str]:
    if not unchanged:
        return []
    return [
        f"Re-introspected {len(unchanged)} unchanged model(s): "
        f"{', '.join(a.model_name for a in unchanged)}"
    ]


def _render_drift_section(to_delete: list[Any]) -> list[str]:
    if not to_delete:
        return []
    out = ["", "Pending drift (run validate_models / apply manually):"]
    out.extend(f"- {entry.tool}: {entry.model_name}" for entry in to_delete)
    return out


def _render_skipped_section(skipped: list[Any]) -> list[str]:
    """Objects that produced no model at all (no --exclude hint; the agent has none)."""
    if not skipped:
        return []
    out = ["", f"Skipped ({len(skipped)}) — not modellable, no model created:"]
    out.extend(f"- {entry.table_name}: {entry.reason}" for entry in skipped)
    return out


def _render_skipped_schemas_section(skipped_schemas: list[Any]) -> list[str]:
    """Requested schemas dropped from scope (foreign catalog / system schema)."""
    if not skipped_schemas:
        return []
    out = ["", f"Skipped schemas ({len(skipped_schemas)}):"]
    out.extend(f"- {entry.token}: {entry.reason}" for entry in skipped_schemas)
    return out


def _render_hidden_internals_section(
    hidden: list[Any], *, data_source: str | None = None
) -> list[str]:
    """Recognised ELT/migration internals modelled ``hidden``.

    Queryable but absent from models_summary; reporting them tells a hidden
    model from an uncreated one. The unhide hint is datasource-qualified.
    """
    if not hidden:
        return []
    out = [
        "",
        f"Hidden ({len(hidden)}) — recognised ELT/migration internals "
        f"(excluded from models_summary; still queryable by name):",
    ]
    out.extend(f"- {_hidden_internal_line(entry)}" for entry in hidden)
    out.append(f"  Use {_unhide_hint(data_source)} to surface one.")
    return out


def _render_errors_section(errors: list[Any]) -> list[str]:
    if not errors:
        return []
    out = ["", f"Errors ({len(errors)}):"]
    out.extend(f"- {err.model_name}: {err.error}" for err in errors)
    return out


def _render_ingest_result(
    result: Any,
    *,
    schema_name: str,
    ds: DatasourceConfig,
) -> str:
    """Render an ``IdempotentIngestResult`` for the MCP ``ingest_datasource_models`` tool."""
    additions = list(result.additions)
    # Read defensively — older result shapes may lack these attributes.
    skipped = list(getattr(result, "skipped", None) or [])
    skipped_schemas = list(getattr(result, "skipped_schemas", None) or [])
    hidden_internals = list(getattr(result, "hidden_internals", None) or [])
    datasource_described = bool(getattr(result, "datasource_described", False))
    if (
        not additions
        and not result.to_delete
        and not result.errors
        and not skipped
        and not skipped_schemas
        and not hidden_internals
        and not datasource_described
    ):
        # Empty result: either no tables in scope, or all models already
        # skipped. Use the scan's own discovered objects (not _fetch_tables,
        # which only sees the default schema) to tell the two apart.
        scanned_objects = getattr(result, "objects", None) or []
        if not scanned_objects:
            return _empty_ingest_message(schema_name=schema_name, ds=ds)
        return "Datasource already in sync — no additive changes."

    new_models = [a for a in additions if a.created]
    updated = [a for a in additions if not a.created and _addition_has_changes(a)]
    unchanged = [
        a for a in additions if not a.created and not _addition_has_changes(a)
    ]

    lines: list[str] = []
    lines.extend(_render_new_models_section(new_models))
    lines.extend(_render_updated_section(updated))
    lines.extend(_render_unchanged_section(unchanged))
    if datasource_described:
        lines.append("Datasource description imported.")
    lines.extend(_render_drift_section(list(result.to_delete)))
    # Same order as the CLI renderer, so the two surfaces read alike.
    lines.extend(_render_skipped_section(skipped))
    lines.extend(_render_skipped_schemas_section(skipped_schemas))
    lines.extend(
        _render_hidden_internals_section(hidden_internals, data_source=ds.name)
    )
    lines.extend(_render_errors_section(list(result.errors)))
    if not lines:
        lines.append("Datasource already in sync — no changes.")
    return "\n".join(lines)


def _column_summary(c: Column) -> dict:
    entry: dict = {"name": c.name, "type": str(c.type)}
    if c.primary_key:
        entry["primary_key"] = True
    entry.update({k: v for k, v in (("label", c.label), ("description", c.description), ("filter", c.filter)) if v})
    if c.allowed_aggregations is not None:
        entry["allowed_aggregations"] = c.allowed_aggregations
    return entry


def _measure_summary(mm: ModelMeasure) -> dict:
    entry = {"name": mm.name, "formula": mm.formula}
    entry.update({k: v for k, v in (("label", mm.label), ("description", mm.description)) if v})
    return entry


def _model_to_summary(model: SlayerModel) -> dict:
    """Convert a SlayerModel to a summary dict."""
    return {
        "name": model.name,
        "description": model.description,
        "source_type": _source_type_for(model),
        "columns": [_column_summary(c) for c in model.columns if not c.hidden],
        "measures": [_measure_summary(mm) for mm in model.measures],
    }


def create_mcp_server(  # NOSONAR(S3776) — FastMCP tool-registration factory; complexity is the cumulative inline closure body of every @mcp.tool() handler. Splitting would require dependency-injecting the engine/storage/services into a separate module — out of scope for incremental PRs.
    storage: StorageBackend,
    *,
    ingest_on_startup: bool = False,
    always_load_query: bool = False,
    _seed_help: bool = True,
):
    _check_budget(what="MCP server instructions", text=SERVER_INSTRUCTIONS)
    # Built first: an invalid SLAYER_NOW must fail before any side effect.
    engine = SlayerQueryEngine(storage=storage)
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

    if ingest_on_startup:
        async_utils.run_sync(
            engine_ingestion.ingest_all_datasources_idempotent(
                storage=storage, stream=sys.stderr,
            )
        )
    FastMCP = _import_fastmcp()  # NOSONAR(S117) — holds a class object; CapWords matches the class it aliases

    mcp = FastMCP("SLayer", instructions=SERVER_INSTRUCTIONS)
    _set_server_version(mcp)
    # Expose the closure engine so callers can dispose per-task pools via
    # mcp._slayer_engine.aclose() (idempotent; leaves the engine reusable).
    # The read-only introspection tools share this same engine.
    mcp._slayer_engine = engine

    def tool(**kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """``mcp.tool`` with the budgeted docstring as the description."""
        return lambda fn: mcp.tool(description=_agent_description(fn), **kwargs)(fn)

    @tool(meta=_ALWAYS_LOAD_META if always_load_query else None)
    async def query(
        query: Annotated[str | SlayerQuery | list[SlayerQuery], Field(
            description="A query object, a list of query objects (stages), or a saved query-backed model's name.",
        )],
        variables: Annotated[dict[str, Any] | None, Field(
            description=(
                "Values for {placeholder}s in conditions, formulas, date_range bounds and model SQL; "
                "override a stage's own variables and the saved model's defaults."
            ),
        )] = None,
        refine: Annotated[QueryRefinement | None, Field(
            description="Saved-model-name form only: clauses merged into the saved query's final stage.",
        )] = None,
        show_sql: Annotated[bool, Field(description="Include the generated SQL in the response.")] = False,
        dry_run: Annotated[bool, Field(description="Return the generated SQL without executing it.")] = False,
        explain: Annotated[bool, Field(description="Run EXPLAIN ANALYZE and return the query plan.")] = False,
        format: Annotated[str, Field(description='"markdown" (default), "json" or "csv".')] = "markdown",
    ) -> str:
        """Run a SLayer query: describe the result you want (measures, dimensions, filters, order) and SLayer writes the SQL, including the joins.

        One query can usually produce the whole answer. Prefer that to running several queries and combining rows yourself or in Python. A single query can:
        - put several grains on one row: a group's share of its total or of the grand total, the region total next to each city;
        - rank, keep the top N per group, and sort by values it does not display;
        - compute period-over-period change, running totals and trailing windows (90 days, 3 months); these look back before date_range by themselves;
        - aggregate fields of joined models over their own rows, without double counting;
        - nest aggregates (the average of per-city totals) and chain stages.

        The syntax is in the descriptions of the query object's fields: measures, filters, order, time_dimensions[].date_range, source_model and dimensions. Full reference: inspect(reference=["memory:help.aggregations", "memory:help.transforms", "memory:help.time", "memory:help.joins", "memory:help.queries"], entity_type="memory").

        `query` is a query object, a list of them (stages: all but the last carry a name; later stages use earlier ones as source_model), or a saved query-backed model's name (optionally with `refine`).

        First inspect the model and search for saved learnings. Then run the final query and check the row count, each aggregate's scope, NULLs and the sort. Without a `limit`, responses stop at 20 rows.
        """
        try:
            fmt = format.lower().strip()
            if fmt not in ("json", "csv", "markdown"):
                raise ValueError(f"Invalid format '{format}'. Must be one of: json, csv, markdown")
            # Response row cap (spec: mcp/response-row-cap): with no explicit
            # limit on the root query, push down cap+1 so truncation is
            # detectable, then slice response-side. A run-by-name string is
            # opaque (its stored SQL can't take a pushed-down limit) — cap
            # response-side only.
            exec_query, capped, cap_hint = _apply_mcp_row_cap(query)
            result = await engine.execute(
                query=exec_query,
                variables=variables,
                dry_run=dry_run,
                explain=explain,
                refine=refine,
            )
            if dry_run:
                return f"SQL:\n{result.sql}"
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
        except Exception as e:
            if isinstance(e, DatabaseError):
                return _friendly_db_error(e)
            raise

    # Model discovery

    @tool()
    async def models_summary(
        datasource_name: Annotated[str, Field(description="Datasource name (from list_datasources).")],
        format: Annotated[str, Field(description='"markdown" (default) or "json".')] = "markdown",
        compact: Annotated[bool, Field(
            description="Default true: per model its name, description, column count, measure names and join targets. False adds the full column and measure tables.",
        )] = True,
    ) -> str:
        """Brief summary of every non-hidden model in a datasource."""
        fmt = format.lower().strip()
        if fmt not in ("markdown", "json"):
            raise ValueError(
                f"Invalid format '{format}' for models_summary. Must be 'markdown' or 'json'."
            )

        try:
            ds = await storage.get_datasource(datasource_name)
        except Exception as exc:
            logger.warning(_DS_LOAD_FAILED, datasource_name, exc)
            return f"Datasource '{datasource_name}' has an invalid config."
        if ds is None:
            return f"Datasource '{datasource_name}' not found."

        loaded = DocumentLoadFailures().skip(await storage.load_models(data_source=datasource_name))
        matched = [m for m in loaded if not m.hidden]
        matched.extend(await storage.builtin_models(datasource_name))
        matched.sort(key=lambda m: m.name)

        # Rendering delegates to the shared renderer (also used by
        # the ``inspect`` model collection view) — one code path, no drift.
        return render_models_summary(
            datasource_name=datasource_name,
            models=matched,
            fmt=fmt,
            compact=compact,
        )

    @tool()
    async def inspect_model(
        model_name: Annotated[str, Field(description="Model name.")],
        num_rows: Annotated[int, Field(description="Sample rows.")] = 3,
        show_sql: Annotated[bool, Field(description="Include generated SQL.")] = False,
        format: Annotated[str, Field(description='"markdown" or "json".')] = "markdown",
        sections: Annotated[list[str] | None, Field(description="Sections to render; default all.")] = None,
        descriptions_max_chars: Annotated[int | None, Field(description="Truncate descriptions to this length.")] = None,
        data_source: Annotated[str | None, Field(description="The model's datasource.")] = None,
        compact: Annotated[bool, Field(description="Compact rendering.")] = True,
    ) -> str:
        """DEPRECATED: use `inspect` with entity_type="model"."""
        try:
            model = await storage.get_model_or_builtin(model_name, data_source=data_source)
        except AmbiguousModelError as exc:
            return _ambiguous_with_mcp_hint(exc)
        if model is None:
            available = sorted(
                f"{m.data_source}.{m.name}"
                for m in DocumentLoadFailures().skip(await storage.load_models()) if not m.hidden
            )
            return f"Model '{model_name}' not found. Available models: {', '.join(available)}"
        return await render_model_inspection(
            model=model,
            storage=storage,
            engine=engine,
            num_rows=num_rows,
            show_sql=show_sql,
            format=format,
            sections=sections,
            descriptions_max_chars=descriptions_max_chars,
            compact=compact,
        )

    @tool()
    async def inspect(
        entity_type: Annotated[str, Field(
            description=(
                "Required: datasource, model, column, measure, aggregation or memory. Asserts the "
                "resolved kind and disambiguates a name shared by, e.g., a column and an aggregation."
            ),
        )],
        reference: Annotated[str | list[str] | None, Field(
            description=(
                "One reference, a list of same-kind references (batch), or omitted for the collection. "
                "Canonical (mydb, mydb.orders, mydb.orders.amount), a bare name, a join path "
                "(orders.customers.region), or memory:<id>."
            ),
        )] = None,
        compact: Annotated[bool, Field(
            description=(
                "Default true: the description only for column / measure / aggregation / datasource / "
                "memory (help.* topics always return their full body), a schema skeleton without "
                "database calls for a model. False: the full render."
            ),
        )] = True,
        format: Annotated[str, Field(description='"markdown" (default) or "json".')] = "markdown",
        num_rows: Annotated[int, Field(description="Sample rows for a model; ignored for other kinds.")] = 3,
        show_sql: Annotated[bool, Field(description="Include generated SQL for a model; ignored for other kinds.")] = False,
        sections: Annotated[list[str] | None, Field(
            description=(
                "Model sections to render: columns, measures, aggregations, joins, samples, learnings, "
                "saved_queries; default all. Ignored for other kinds."
            ),
        )] = None,
        descriptions_max_chars: Annotated[int | None, Field(
            description="Truncate every description, and a memory's body, to this many characters.",
        )] = None,
    ) -> str:
        """Inspect one entity by reference and kind, a batch when `reference` is a list, or a whole collection when `reference` is omitted. A point lookup: use `search` to find entities by meaning, with related memories.

        Before using a column as a filter, projection, group-by or join key, inspect it and read its Description (the author's intent) and Sample values (stored literal forms; a top-N sample). Build text conditions from these, never from a guessed spelling, and never pick a column by its name alone.

        Collection: omit `reference` with entity_type "model" (every model, grouped by datasource) or "datasource".
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

    # Model creation and editing

    @tool()
    async def create_model(
        name: Annotated[str, Field(description="Unique model name (lowercase, underscores).")],
        sql_table: Annotated[str | None, Field(description='Database table, e.g. "public.orders".')] = None,
        sql: Annotated[str | None, Field(description="A SQL query as the model's source, instead of sql_table.")] = None,
        data_source: Annotated[str | None, Field(description="Datasource name (from list_datasources).")] = None,
        description: Annotated[str | None, Field(description="What one row of this model represents.")] = None,
        columns: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Column definitions: {"name", "sql", "type"} with type string, number, time, date or '
                "boolean; optional primary_key, unique (single-column uniqueness other than the "
                "primary key), allowed_aggregations, filter (a condition masking the column's value "
                "inside aggregates), granularity (only for a column truly bucketed at that grain), "
                "label, description, hidden, meta."
            ),
        )] = None,
        measures: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Saved formulas, {"name": "aov", "formula": "sum(revenue) / count(*)"} plus optional '
                'label, description, meta; queries reference them by bare name ({"formula": "aov"}).'
            ),
        )] = None,
        aggregations: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Custom aggregations, {"name": "sum_sq", "formula": "SUM({value} * {value})", '
                '"params": [{"name": "weight", "sql": "quantity"}], "description": ...}; the formula '
                "is SQL with {value} for the aggregated column."
            ),
        )] = None,
        query: Annotated[Any | None, Field(
            description=(
                "A query object or list of stages; makes the model query-backed. Excludes sql_table, "
                "sql, columns, measures and aggregations."
            ),
        )] = None,
        variables: Annotated[dict[str, Any] | None, Field(
            description="Default values for {var} placeholders in the backing query.",
        )] = None,
    ) -> str:
        """Create a semantic model from a database table (sql_table), a SQL query (sql), or a SLayer query (query: the model becomes query-backed and its columns come from the result).

        Host a column or measure on the model whose row grain is 1:1 with what it describes, not merely one where its inputs live. Choose join keys by column Description (author intent); on ties take the shortest declared join path (long chains through lookup or log tables fan out rows). Define entities in dependency order and reference already-defined ones by name; in row-level SQL parenthesise weighted sums in comparisons ((a*w1 + b*w2) > t).

        Example: create_model(name="orders", sql_table="public.orders", data_source="mydb", columns=[{"name": "amount", "sql": "amount", "type": "number"}], measures=[{"name": "aov", "formula": "sum(amount) / count(*)"}])

        A custom aggregation named sum_sq is called like a built-in: sum_sq(column). Authoring guide: inspect(reference="memory:help.models", entity_type="memory").
        """
        if query is not None:
            table_params = {
                k: v for k, v in {
                    "sql_table": sql_table, "sql": sql, "data_source": data_source,
                    "columns": columns, "measures": measures, "aggregations": aggregations,
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
                model = await engine.create_model_from_query(
                    query=parsed_query,
                    name=name,
                    description=description or "",
                    variables=variables,
                )
            except Exception as e:
                if isinstance(e, DatabaseError):
                    return _friendly_db_error(e)
                return f"Error creating model from query: {e}"
            cols = [c.name for c in model.columns]
            meas = [m.name for m in model.measures]
            return (
                f"Model '{name}' created from query. "
                f"Columns: {cols}. Measures: {meas}."
            )

        data = _build_dict(
            name=name,
            sql_table=sql_table,
            sql=sql,
            data_source=data_source,
            description=description,
            columns=columns,
            measures=measures,
            aggregations=aggregations,
        )
        model = SlayerModel.model_validate(data)
        existed = (
            await storage.get_model(name, data_source=model.data_source)
            is not None
        )
        # save_model normalizes, validates Mode-A join paths, and trial-executes
        # a raw-sql source against its datasource before it persists.
        try:
            await engine.save_model(model)
        except Exception as e:
            if isinstance(e, DatabaseError):
                return _friendly_db_error(e)
            return f"Error creating model '{model.name}': {e}"
        verb = "replaced" if existed else "created"
        return f"Model '{model.name}' {verb}."

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

    VALID_REMOVE_KEYS = {"columns", "measures", "aggregations", "joins"}

    @tool()
    async def edit_model(
        model_name: Annotated[str, Field(description="Model to edit.")],  # NOSONAR(S107) — each parameter is a field of the agent-facing MCP tool schema
        description: Annotated[str | None, Field(description="New model description.")] = None,
        data_source: Annotated[str | None, Field(
            description="The model's datasource; required when the name exists in several datasources.",
        )] = None,
        new_data_source: Annotated[str | None, Field(description="Move the model to another datasource (rare).")] = None,
        default_time_dimension: Annotated[str | None, Field(
            description="A date/time column: the default time axis for transforms.",
        )] = None,
        sql_table: Annotated[str | None, Field(description="Database table; clears sql and source_queries.")] = None,
        sql: Annotated[str | None, Field(description="SQL query as the source; clears sql_table and source_queries.")] = None,
        source_queries: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                "Replace the backing query with these stages (all but the last carry a name); makes the "
                "model query-backed, clears sql_table and sql, and refreshes the cached columns."
            ),
        )] = None,
        query_variables: Annotated[Any, Field(
            description="Replace the backing query's default {var} values; null clears them.",
        )] = _UNSET,
        hidden: Annotated[bool | None, Field(description="Hide the model from discovery (still queryable).")] = None,
        columns: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Columns to upsert by name; only the given fields change. {"name", "type", "sql", '
                '"description", "primary_key", "unique", "hidden", "allowed_aggregations", "filter", '
                '"label", "granularity"}; type string, number, time, date or boolean; unique marks '
                "single-column uniqueness other than the primary key (used for join cardinality); set "
                "granularity only for a column truly bucketed at that grain (null clears it)."
            ),
        )] = None,
        measures: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Saved formulas to upsert by name, {"name": "aov", "formula": "sum(revenue) / '
                'count(*)"} plus optional label, description, meta; queries use them by bare name.'
            ),
        )] = None,
        aggregations: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Custom aggregations to upsert by name, {"name", "formula" (SQL with {value} and '
                '{param} placeholders), "params": [{"name", "sql"}], "description", "meta"}.'
            ),
        )] = None,
        joins: Annotated[list[dict[str, Any]] | None, Field(
            description=(
                'Joins to upsert by target_model, {"target_model": "customers", "join_pairs": '
                '[["customer_id", "id"]], "cardinality": "many_to_one", "description", "meta"}. '
                "Keys name columns by name; a composite key is one join with several pairs. "
                "cardinality (one_to_one / one_to_many / many_to_one / many_to_many, read "
                "source->target) is descriptive only; omit it when unknown."
            ),
        )] = None,
        add_filters: Annotated[list[str] | None, Field(
            description='SQL conditions always applied to the model, e.g. ["deleted_at IS NULL"].',
        )] = None,
        remove_filters: Annotated[list[str] | None, Field(description="Model filters to remove (exact text).")] = None,
        remove: Annotated[dict[str, list[str]] | None, Field(
            description=(
                'Entities to delete before the upserts: {"columns": [...], "measures": [...], '
                '"aggregations": [...], "joins": [target_model, ...]}.'
            ),
        )] = None,
        meta: Annotated[dict[str, Any] | None, Field(
            description="JSON metadata replacing the model's meta; null clears it.",
        )] = _UNSET,
    ) -> str:
        """Edit an existing model in one call: metadata, upserts of columns / measures / aggregations / joins (by name; only the given fields change), model filters, and removals (applied first).

        Host a column or measure on the model whose row grain is 1:1 with what it describes, not merely one where its inputs live. Choose join keys by column Description (author intent); on ties take the shortest declared join path (long chains through lookup or log tables fan out rows). Define entities in dependency order and reference already-defined ones by name; in row-level SQL parenthesise weighted sums in comparisons ((a*w1 + b*w2) > t).

        Example: edit_model(model_name="orders", columns=[{"name": "status", "type": "string"}], measures=[{"name": "aov", "formula": "sum(revenue) / count(*)"}], remove={"measures": ["old_metric"]})
        """
        try:
            model = await storage.get_model(model_name, data_source=data_source)
        except AmbiguousModelError as exc:
            return _ambiguous_with_mcp_hint(exc)
        if model is None:
            return f"Model '{model_name}' not found."

        original_data_source = model.data_source
        changes: list[str] = []
        # Track column-level vs model-level changes so the post-save hook
        # refreshes only the touched columns when possible.
        changed_columns: set = set()
        model_level_change = False
        # Model-doc changes (measures / joins) don't invalidate Column.sampled
        # but do change the embedding text — track separately to refresh
        # embeddings without a full per-column re-profile.
        model_doc_changed = False

        # --- Phase 1: Scalar metadata ---
        if description is not None:
            model.description = description
            changes.append("updated description")
        if new_data_source is not None and new_data_source != model.data_source:
            # Moving a model is delete-old + save-new: refuse if the target key
            # is taken and defer the delete until after the new save (Phase 5).
            # Here we only mutate the in-memory model.
            try:
                existing_target = await storage.get_model(
                    model.name, data_source=new_data_source
                )
            except AmbiguousModelError:
                existing_target = None  # Strict lookup; ambiguity is for bare names only.
            if existing_target is not None:
                return (
                    f"Model '{model.name}' already exists in datasource "
                    f"'{new_data_source}'. Pick a different name, delete "
                    f"the existing target first, or move to a different "
                    f"datasource."
                )
            model.data_source = new_data_source
            changes.append(
                f"moved data_source from '{original_data_source}' to '{new_data_source}'"
            )
        if default_time_dimension is not None:
            model.default_time_dimension = default_time_dimension
            changes.append(f"set default_time_dimension to '{default_time_dimension}'")
        explicit_sources = sum(
            1 for v in (sql_table, sql, source_queries) if v is not None
        )
        if explicit_sources > 1:
            return (
                "Specify at most one of 'sql_table', 'sql', or 'source_queries' "
                "when editing a model — the three source modes are mutually exclusive."
            )

        if sql_table is not None:
            model.sql_table = sql_table
            model.sql = None
            model.source_queries = None
            model_level_change = True
            changes.append(f"set sql_table to '{sql_table}'")
        if sql is not None:
            model.sql = sql
            model.sql_table = None
            model.source_queries = None
            model_level_change = True
            changes.append(f"set sql to '{sql}'")
        if source_queries is not None:
            # Switching to query-backed source mode. Cache columns and
            # backing_query_sql get refreshed when we save via engine.save_model.
            model.source_queries = [SlayerQuery.model_validate(q) for q in source_queries]
            model.sql_table = None
            model.sql = None
            # Clear the user-managed columns so the cache write succeeds.
            model.columns = []
            model.backing_query_sql = None
            changes.append(f"set source_queries ({len(source_queries)} stage(s))")
        if query_variables is not _UNSET:
            model.query_variables = query_variables or {}
            changes.append(
                "updated query_variables"
                if query_variables
                else "cleared query_variables"
            )
        if hidden is not None:
            model.hidden = hidden
            changes.append(f"set hidden to {hidden}")
        if meta is not _UNSET:
            model.meta = meta
            changes.append("updated meta" if meta is not None else "cleared meta")

        # --- Phase 2: Removals ---
        if remove:
            for key in remove:
                if key not in VALID_REMOVE_KEYS:
                    return (
                        f"Invalid remove key '{key}'. "
                        f"Must be one of: {', '.join(sorted(VALID_REMOVE_KEYS))}."
                    )

            for name in remove.get("columns", []):
                match = next((c for c in model.columns if c.name == name), None)
                if match is None:
                    return f"Column '{name}' not found on model '{model_name}'."
                model.columns.remove(match)
                changes.append(f"removed column '{name}'")

            for name in remove.get("measures", []):
                match = next((m for m in model.measures if m.name == name), None)
                if match is None:
                    return f"Measure '{name}' not found on model '{model_name}'."
                model.measures.remove(match)
                changes.append(f"removed measure '{name}'")
                model_doc_changed = True

            for name in remove.get("aggregations", []):
                match = next((a for a in model.aggregations if a.name == name), None)
                if match is None:
                    return f"Aggregation '{name}' not found on model '{model_name}'."
                model.aggregations.remove(match)
                changes.append(f"removed aggregation '{name}'")
                model_doc_changed = True

            for target in remove.get("joins", []):
                match = next((j for j in model.joins if j.target_model == target), None)
                if match is None:
                    return f"Join to '{target}' not found on model '{model_name}'."
                model.joins.remove(match)
                changes.append(f"removed join to '{target}'")
                model_doc_changed = True

        # --- Phase 3: Entity upserts ---
        for spec in columns or []:
            col_name = spec.get("name")
            if isinstance(col_name, str):
                changed_columns.add(col_name)
            err = _upsert_entity(
                entity_list=model.columns, spec=spec, entity_cls=Column,
                id_field="name", changes=changes, label="column",
            )
            if err:
                return err

        for spec in measures or []:
            err = _upsert_entity(
                entity_list=model.measures, spec=spec, entity_cls=ModelMeasure,
                id_field="name", changes=changes, label="measure",
            )
            if err:
                return err
            model_doc_changed = True

        for spec in aggregations or []:
            err = _upsert_entity(
                entity_list=model.aggregations, spec=spec, entity_cls=Aggregation,
                id_field="name", changes=changes, label="aggregation",
            )
            if err:
                return err
            model_doc_changed = True

        for spec in joins or []:
            err = _upsert_entity(
                entity_list=model.joins, spec=spec, entity_cls=ModelJoin,
                id_field="target_model", changes=changes, label="join",
            )
            if err:
                return err
            model_doc_changed = True

        # --- Phase 4: Filters ---
        if add_filters:
            existing_filters = set(model.filters)
            for f in add_filters:
                if f not in existing_filters:
                    model.filters.append(f)
                    existing_filters.add(f)
                    changes.append(f"added filter '{f}'")
                    model_level_change = True

        if remove_filters:
            for f in remove_filters:
                if f not in model.filters:
                    return f"Filter not found on model '{model_name}': {f}"
                model.filters.remove(f)
                changes.append(f"removed filter '{f}'")
                model_level_change = True

        if not changes:
            return f"No changes specified for model '{model_name}'."

        # --- Phase 5: Validate and save ---
        # Query-backed models route through engine.save_model so the
        # engine-managed column cache is refreshed and user-supplied cache
        # fields are rejected.
        try:
            validated = SlayerModel.model_validate(model.model_dump(mode="json"))
        except Exception as exc:
            return f"Validation error: {exc}"

        if validated.source_queries:
            # columns / backing_query_sql are engine-managed here; reject
            # explicit user supply rather than silently dropping it.
            if columns is not None:
                return (
                    "Validation error: cannot supply 'columns' on a "
                    f"query-backed model ('{model_name}'). Columns are "
                    "engine-managed (auto-derived from the backing query)."
                )
            # Strip cache fields so save_model repopulates them from a fresh
            # expansion of the backing query.
            validated = validated.model_copy(update={
                "columns": [],
                "backing_query_sql": None,
            })
            try:
                # save_model may recompute data_source for query-backed models,
                # so use the returned model's identity for cleanup below.
                saved_model = await engine.save_model(validated)
            except Exception as exc:
                return f"Validation error: {exc}"
        else:
            # save_model normalizes, validates Mode-A join paths, and trial-
            # executes a raw-sql source before it persists.
            try:
                saved_model = await engine.save_model(validated)
            except Exception as exc:
                return f"Validation error: {exc}"

        # Atomic move: remove the source row only after the save succeeded and
        # only if the saved model actually landed at a different data_source
        # (the cache populator can override new_data_source).
        if saved_model.data_source != original_data_source:
            await storage.delete_model(
                name=saved_model.name, data_source=original_data_source
            )
        # Refresh sampled column values and subtree embeddings. Best-effort —
        # a raise is captured into refresh_warnings so the save still succeeds.
        refresh_warnings: list[str] = []
        if changed_columns or model_level_change or model_doc_changed:
            try:
                refresh_warnings = await handle_edit_refresh(
                    engine=engine,
                    storage=storage,
                    data_source=saved_model.data_source,
                    model_name=saved_model.name,
                    changed_columns=changed_columns,
                    model_level_change=model_level_change,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort post-save
                logger.warning(
                    "edit_model refresh hook raised for %s.%s: %s",
                    saved_model.data_source, saved_model.name, exc,
                )
                refresh_warnings = [
                    f"refresh hook raised: {exc}",
                ]
        response_payload: dict = {
            "success": True,
            "model_name": model_name,
            "changes": changes,
            "message": f"Applied {len(changes)} change(s) to '{model_name}'",
        }
        if refresh_warnings:
            response_payload["warnings"] = refresh_warnings
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
        auto_ingest: Annotated[bool, Field(description="Create models from the schema (default true).")] = True,
        granularities: Annotated[list[CustomGranularity] | None, Field(
            description=(
                "Custom time granularities, each {name, base, multiple, origin}: buckets start at "
                'origin + k * multiple * base, e.g. {name: "fiscal_year", base: "year", origin: '
                '"2000-04-01"}; usable wherever a built-in granularity is.'
            ),
        )] = None,
    ) -> str:
        """Create a database connection, verify it, and auto-ingest models. Use ${ENV_VAR} in credentials to read environment variables.

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

        ok, msg = _test_connection(ds)
        if not ok:
            return f"Datasource '{ds.name}' {verb}, but connection test failed.\n{msg}"

        lines = [f"Datasource '{ds.name}' {verb}. {msg}"]

        if not auto_ingest:
            return "\n".join(lines)

        # Auto-ingest models
        try:
            # Via the module so tests can monkeypatch the seam.
            ingest_output = engine_ingestion.ingest_datasource_report(
                datasource=ds,
                schema=schema_name or None,
                schemas=schemas_list,
                all_schemas=all_schemas,
            )
        except Exception as e:
            if isinstance(e, DatabaseError):
                lines.append(f"Auto-ingestion failed: {_friendly_db_error(e)}")
                return "\n".join(lines)
            raise
        models = ingest_output.models

        if ingest_output.schema_description and not ds.description:
            try:
                ds = ds.model_copy(
                    update={"description": ingest_output.schema_description}
                )
                await storage.save_datasource(ds)
                lines.append("Datasource description imported.")
            except Exception as exc:  # noqa: BLE001 — best-effort
                lines.append(f"Could not save datasource description: {exc}")

        save_errors: list[str] = []
        saved_models = []
        for model in models:
            try:
                await storage.save_model(model)
                saved_models.append(model)
            except ValueError as exc:
                # e.g. quoted case-variant tables — report and continue.
                save_errors.append(f"- {model.name}: {exc}")
        models = saved_models

        if not models and not save_errors:
            lines.append("No tables found to ingest.")
            available = _get_schemas(ds)
            if available:
                lines.append(f"Available schemas: {', '.join(available)}")
        elif models:
            lines.append(f"Ingested {len(models)} model(s):")
            for m in models:
                lines.append(f"- {m.name} ({len(m.columns)} columns, {len(m.measures)} measures)")
            lines.append("")
            lines.append("Use models_summary and inspect to explore, then query to fetch data.")

        if save_errors:
            lines.append(f"Failed to save {len(save_errors)} model(s):")
            lines.extend(save_errors)

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

        Use it after create_datasource to verify the connection and see what is queryable before ingest_datasource_models.
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

        ok, msg = _test_connection(ds)
        lines.append(f"\nConnection: {'OK' if ok else 'FAILED'}")
        if not ok:
            lines.append(msg)
            return "\n".join(lines)

        schemas = _get_schemas(ds)
        if schemas:
            lines.append(f"Available schemas: {', '.join(schemas)}")

        if list_tables:
            tables, err = _fetch_tables(ds=ds, schema_name=schema_name or None)
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
                    "\nUse ingest_datasource_models to create models from these tables."
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
            models_in_ds = DocumentLoadFailures().skip(await storage.load_models(data_source=name))
            try:
                await search_service.refresh_datasource(
                    name=name,
                    models=models_in_ds,
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
    async def delete_model(
        name: Annotated[str, Field(description="Model to delete.")],
        data_source: Annotated[str | None, Field(
            description="The model's datasource; required when the name exists in several datasources.",
        )] = None,
    ) -> str:
        """Delete a semantic model."""
        try:
            deleted = await storage.delete_model(name, data_source=data_source)
        except AmbiguousModelError as exc:
            return _ambiguous_with_mcp_hint(exc)
        if deleted:
            return f"Model '{name}' deleted."
        return f"Model '{name}' not found."

    @tool()
    async def validate_models(
        data_source: Annotated[str | None, Field(description="Datasource to validate; omit for all.")] = None,
    ) -> str:
        """Diff stored models against the live database schemas.

        Returns, as JSON, the deletes (columns, measures, joins, filters, whole models) needed to keep the stored models valid. Read-only.
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
            entries = await engine.validate_models(data_source=data_source)
        except DatabaseError as exc:
            return _friendly_db_error(exc)
        return json.dumps([e.model_dump(mode="json") for e in entries], indent=2)

    @tool()
    async def recommend_root_model(
        items: Annotated[list[str], Field(
            description="Entity references: orders.revenue, customers.name, sum(orders.revenue), a saved measure's bare name, ...",
        )],
        data_source: Annotated[str | None, Field(
            description="Datasource scope; omitted, names resolve via the datasource priority list.",
        )] = None,
        root_hint: Annotated[str | None, Field(
            description=(
                "Intended root (model or <data_source>.<model>); honoured when it reaches every item, "
                "else the automatic pick is used with a warning."
            ),
        )] = None,
        format: Annotated[str, Field(description='"markdown" (default) or "json".')] = "markdown",  # noqa: A002
    ) -> str:
        """Recommend the root model (a query's source_model) for a set of model.column / measure items, with each item's reference path from that root, ready to drop into the query (a joined column comes back as customers.regions.name, a root-owned one as status; aggregations are kept).

        The root is the model from which every item is reachable with the fewest join hops. When none reaches everything, root_model is null and coverage lists the best partial roots for a multi-stage query.

        Call it once the item list is final; explore with search / inspect first.
        """
        fmt = format.lower().strip()
        if fmt not in ("markdown", "json"):
            return (
                f"recommend_root_model failed: unknown format '{format}'. "
                f"Use 'markdown' or 'json'."
            )
        # Reuse the closure engine (see validate_models above).
        try:
            rec = await engine.recommend_root_model(
                items, data_source=data_source, root_hint=root_hint
            )
        except AmbiguousModelError as exc:
            return _ambiguous_with_mcp_hint(exc)
        except (ValueError, EntityResolutionError) as exc:
            return f"recommend_root_model failed: {exc}"
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
    async def ingest_datasource_models(
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
        """Discover a database's tables and create or additively update models from them.

        Idempotent and additive: new columns and joins are appended; existing definitions are never overwritten. Also returns the pending validate_models deletes.
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

        try:
            include = [t.strip() for t in include_tables.split(",") if t.strip()] or None
            result = await engine_ingestion.ingest_datasource_idempotent(
                datasource=ds,
                storage=storage,
                include_tables=include,
                schema=schema_name or None,
                schemas=schemas_list,
                all_schemas=all_schemas,
            )
        except Exception as e:
            if isinstance(e, DatabaseError):
                return _friendly_db_error(e)
            raise

        return _render_ingest_result(
            result, schema_name=schema_name, ds=ds
        )

    @tool()
    async def set_datasource_priority(
        priority: Annotated[list[str], Field(
            description="Existing datasource names, most preferred first; an empty list clears it.",
        )],
    ) -> str:
        """Set how a bare model name that exists in several datasources resolves: the first datasource in this list that has it wins; otherwise the name is ambiguous and errors."""
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
        if isinstance(exc, AmbiguousModelError):
            return _ambiguous_with_mcp_hint(exc)
        prefix = f"{type(exc).__name__}: "
        return f"Error: {exc}" if str(exc).startswith(prefix) else f"Error: {prefix}{exc}"

    @tool()
    async def save_memory(
        learning: Annotated[str, Field(description="The note text; required, non-empty.")],
        linked_entities: Annotated[Any, Field(
            description=(
                "A list of entity references (resolved to <datasource>.<model>[.<leaf>]; memory:<id> "
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

        With a list of entity references the memory appears among search's memories; with a query object it appears among search's example queries. Returns the memory_id, the canonical entities stored and any warnings. Deleting a model, datasource or measure strips references to it from every memory; the note itself is kept.

        Examples:
        save_memory(learning="orders.is_returned in {0,1,NULL}; treat NULL as not returned", linked_entities=["orders.is_returned"])
        save_memory(learning="Paid revenue by status", linked_entities={"source_model": "orders", "measures": [{"formula": "sum(amount)"}], "filters": ["status = 'paid'"]}, id="kb.paid-revenue")
        """
        try:
            response = await memory_service.save_memory(
                learning=learning,
                linked_entities=linked_entities,
                id=id,
                description=description,
            )
        except (
            EntityResolutionError,
            AmbiguousModelError,
            ValueError,
        ) as exc:
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
    # column-hit hook can auto-refresh stale categorical columns.
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
                "MATCH (n:ModelColumn) RETURN n.id AS id to spend max_results on columns only. Labels: "
                "Memory, Datasource, Model, ModelColumn, Measure, Aggregation; without the "
                "advanced_search extra only such label filters work."
            ),
        )] = None,
        compact: Annotated[bool, Field(
            description="Default true: one-line hits. False returns full renders; avoid it for broad searches.",
        )] = True,
    ) -> str:
        """Search saved memories, example queries and entities (models, columns, measures, ...) by entity overlap and by meaning.

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
        body_lines.append(" | ".join(str(row.get(c, "")) for c in columns))

    result = f"{header}\n{separator}\n" + "\n".join(body_lines)
    if truncated:
        result += f"\n... ({len(data)} total rows, showing first {max_rows})"
    return result


def _format_json(
    data: list[dict[str, Any]],
    warnings: list[dict[str, Any]] | None = None,
    attributes: dict[str, Any] | None = None,
    population: str | None = None,
) -> str:
    """Bare array, or {"data", "warnings"?, "attributes"?, "population"?} once any is present.

    Attributes, warnings, and the inferred population ride inside the payload so
    the whole response stays strict-``json.loads``-able — never trailing prose.
    """
    if not warnings and not attributes and population is None:
        return json.dumps(data, default=str)
    payload: dict[str, Any] = {"data": data}
    if warnings:
        payload["warnings"] = warnings
    if attributes:
        payload["attributes"] = attributes
    if population is not None:
        payload["population"] = population
        payload["population_inferred"] = True
    return json.dumps(payload, default=str)


def _format_csv(data: list[dict[str, Any]], columns: list[str]) -> str:
    """Format data as CSV."""
    if not data:
        return ""
    lines = [",".join(columns)]
    for row in data:
        values = []
        for c in columns:
            v = str(row.get(c, ""))
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
    """Format query output in the requested format.

    Attributes and warnings stay machine-safe: both inside the json payload,
    both as leading `#` comment lines for csv, a prose attributes footer before
    the trailing Warnings block for markdown.
    """
    inferred_population = result.population if result.population_inferred else None
    if fmt == "csv":
        # Leading `#` lines, never trailing prose — trailing rows break the
        # column count for every CSV reader.
        return (
            _population_comment(inferred_population)
            + _csv_attribute_comments(result)
            + _csv_warning_comments(result)
            + _format_csv(data=result.data, columns=result.columns)
        )
    if fmt == "markdown":
        return (
            result.to_markdown()
            + _attributes_footer(result.attributes)
            + _population_footer(inferred_population)
            + _format_warnings(result)
        )
    return _format_json(
        data=result.data,
        warnings=[w.model_dump(mode="json") for w in (result.warnings or [])],
        attributes=_json_attributes(result.attributes),
        population=inferred_population,
    )


def _population_footer(population: str | None) -> str:
    """Trailing note naming the inferred population (markdown), or empty."""
    return "" if population is None else f"\n\nPopulation: {population} (inferred)"


def _population_comment(population: str | None) -> str:
    """Leading `#` note naming the inferred population (csv), or empty."""
    return "" if population is None else f"# population: {population} (inferred)\n"


def _format_field_meta(entries: dict[str, Any]) -> list[str]:
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


def _format_attributes(attributes) -> str:
    """Format response attributes as a compact section."""
    lines = []
    dim_lines = _format_field_meta(attributes.dimensions)
    if dim_lines:
        lines.append("Dimension attributes:")
        lines.extend(dim_lines)
    measure_lines = _format_field_meta(attributes.measures)
    if measure_lines:
        lines.append("Measure attributes:")
        lines.extend(measure_lines)
    return "\n".join(lines) if lines else ""


def _has_attributes(attributes) -> bool:
    return bool(attributes and (attributes.dimensions or attributes.measures))


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
