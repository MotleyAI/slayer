"""MCP server for SLayer."""

import json
import logging
from typing import Any

import sqlalchemy as sa

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
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.core.recommend import render_recommendation_markdown
from slayer.engine.ingestion import _friendly_db_error
from slayer.engine.profiling import handle_edit_refresh
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.help import TOPIC_SUMMARY_LINE, render_help
from slayer.inspect.model_render import (  # noqa: F401 — re-exported for backward-compat (tests + other modules import these names from slayer.mcp.server)
    _build_sample_query_args,
    _collect_measure_profile,
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
from slayer.inspect.service import InspectService
from slayer.memories.service import MemoryService
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)

VALID_DIMENSION_TYPES = {"string", "time", "date", "boolean", "number"}
_UNSET = object()  # Sentinel to distinguish "not provided" from "explicitly set to None"


def _ambiguous_with_mcp_hint(exc: AmbiguousModelError) -> str:
    """Render an ``AmbiguousModelError`` for the MCP surface.

    The exception itself is intentionally surface-neutral; we append an
    MCP-specific remediation pointing at the ``data_source`` tool argument
    and the ``set_datasource_priority`` MCP tool.
    """
    return (
        f"{exc} Pass data_source=... to this tool, or use the "
        f"set_datasource_priority tool to set a priority."
    )


def _test_connection(ds: DatasourceConfig) -> tuple[bool, str]:
    """Test a datasource connection. Returns (success, message)."""
    try:
        from slayer.sql import engine_factory
        engine = engine_factory.get_engine(ds.resolve_env_vars())
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        # Cached engine — engine_factory owns lifecycle; don't dispose.
        return True, "Connection successful."
    except Exception as e:
        return False, _friendly_db_error(e)


def _get_schemas(ds: DatasourceConfig) -> list[str]:
    """List available schemas for a datasource."""
    try:
        from slayer.sql import engine_factory
        engine = engine_factory.get_engine(ds.resolve_env_vars())
        inspector = sa.inspect(engine)
        schemas = inspector.get_schema_names()
        return schemas
    except Exception:
        return []


def _fetch_tables(
    ds: DatasourceConfig, schema_name: str | None = None,
) -> tuple[list[str] | None, str | None]:
    """Inspect a datasource's table names.

    Returns ``(tables, None)`` on success or ``(None, friendly_error_message)``
    on failure. ``schema_name=None`` uses the dialect's default schema.
    """
    try:
        from slayer.sql import engine_factory
        sa_engine = engine_factory.get_engine(ds.resolve_env_vars())
        inspector = sa.inspect(sa_engine)
        tables = inspector.get_table_names(schema=schema_name)
        return sorted(tables), None
    except Exception as e:
        if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
            return None, _friendly_db_error(e)
        return None, str(e)


def _empty_ingest_message(*, schema_name: str, ds: DatasourceConfig) -> str:
    schema_label = f" in schema '{schema_name}'" if schema_name else ""
    lines = [f"No tables found{schema_label}."]
    schemas = _get_schemas(ds)
    if schemas:
        lines.append(f"Available schemas: {', '.join(schemas)}")
        lines.append(
            "Try: ingest_datasource_models with schema_name set to one of these."
        )
    return "\n".join(lines)


def _render_new_models_section(new_models: list[Any]) -> list[str]:
    if not new_models:
        return []
    lines = [f"Created {len(new_models)} new model(s):"]
    for a in new_models:
        lines.append(
            f"- {a.model_name} ({len(a.new_columns)} columns, {len(a.new_joins)} joins)"
        )
    return lines


def _render_updated_section(updated: list[Any]) -> list[str]:
    if not updated:
        return []
    lines = [f"Updated {len(updated)} existing model(s):"]
    for a in updated:
        details = []
        if a.new_columns:
            details.append(f"+columns: {', '.join(a.new_columns)}")
        if a.new_joins:
            details.append(f"+joins: {', '.join(a.new_joins)}")
        lines.append(f"- {a.model_name} ({'; '.join(details)})")
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
    if not additions and not result.to_delete and not result.errors:
        # Two distinct cases produce an empty result:
        #   1. The schema actually has no tables (the agent should look
        #      elsewhere — show the "Try schema_name=..." hint).
        #   2. The schema has tables but every persisted model is sql /
        #      query-backed (silently skipped by the additive pass) — no
        #      additive work to do, but the existing models are healthy.
        # Probe the live table count so we don't misdirect the agent.
        tables, _err = _fetch_tables(ds=ds, schema_name=schema_name or None)
        if tables is None or not tables:
            return _empty_ingest_message(schema_name=schema_name, ds=ds)
        return "Datasource already in sync — no additive changes."

    new_models = [a for a in additions if a.created]
    updated = [a for a in additions if not a.created and (a.new_columns or a.new_joins)]
    unchanged = [
        a for a in additions
        if not a.created and not a.new_columns and not a.new_joins
    ]

    lines: list[str] = []
    lines.extend(_render_new_models_section(new_models))
    lines.extend(_render_updated_section(updated))
    lines.extend(_render_unchanged_section(unchanged))
    lines.extend(_render_drift_section(list(result.to_delete)))
    lines.extend(_render_errors_section(list(result.errors)))
    if not lines:
        lines.append("Datasource already in sync — no changes.")
    return "\n".join(lines)


def _model_to_summary(model: SlayerModel) -> dict:
    """Convert a SlayerModel to a summary dict."""
    columns = []
    for c in model.columns:
        if c.hidden:
            continue
        entry: dict = {"name": c.name, "type": str(c.type)}
        if c.primary_key:
            entry["primary_key"] = True
        if c.label:
            entry["label"] = c.label
        if c.description:
            entry["description"] = c.description
        if c.filter:
            entry["filter"] = c.filter
        if c.allowed_aggregations is not None:
            entry["allowed_aggregations"] = c.allowed_aggregations
        columns.append(entry)

    measures = []
    for mm in model.measures:
        entry = {"name": mm.name, "formula": mm.formula}
        if mm.label:
            entry["label"] = mm.label
        if mm.description:
            entry["description"] = mm.description
        measures.append(entry)

    return {
        "name": model.name,
        "description": model.description,
        "source_type": _source_type_for(model),
        "columns": columns,
        "measures": measures,
    }


def create_mcp_server(  # NOSONAR(S3776) — FastMCP tool-registration factory; complexity is the cumulative inline closure body of every @mcp.tool() handler. Splitting would require dependency-injecting the engine/storage/services into a separate module — out of scope for incremental PRs.
    storage: StorageBackend,
    *,
    ingest_on_startup: bool = False,
):
    if ingest_on_startup:
        import sys

        from slayer.async_utils import run_sync
        from slayer.engine.ingestion import ingest_all_datasources_idempotent

        run_sync(
            ingest_all_datasources_idempotent(storage=storage, stream=sys.stderr)
        )
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError:
        raise ImportError("MCP package not found. Reinstall SLayer: pip install motley-slayer")

    mcp = FastMCP(
        "SLayer",
        instructions=(
            "SLayer is a semantic layer for querying databases. "
            "Instead of writing SQL, describe what data you want using models, measures, dimensions, and filters. "
            "Call help() for an overview of SLayer concepts, and help(topic='...') for deep dives on specific topics. "
            "Typical workflow: list_datasources → models_summary → inspect → query. "
            "To connect a new database: create_datasource → describe_datasource (verify + list tables) → ingest_datasource_models → models_summary."
        ),
    )
    engine = SlayerQueryEngine(storage=storage)

    _help_description = (
        "Return conceptual help on SLayer. "
        "Call without a topic for the intro (what SLayer is, core entities, the query shape). "
        "Pass a topic name for a deep dive. "
        f"{TOPIC_SUMMARY_LINE} "
        "Args: topic (optional) — the topic name. Unknown topics return a friendly error listing the valid ones."
    )

    @mcp.tool(description=_help_description)
    async def help(topic: str | None = None) -> str:  # noqa: A001 — intentional shadow of builtin inside factory
        return render_help(topic=topic)

    @mcp.tool()
    async def query(  # NOSONAR S107 — FastMCP introspects this signature to expose each query option as a typed MCP tool argument; collapsing into a dict would degrade the agent-facing schema
        source_model: str | ModelExtension | SlayerModel,
        measures: list[dict[str, str]] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        time_dimensions: list[dict[str, Any]] | None = None,
        order: list[dict[str, str]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        whole_periods_only: bool = False,
        show_sql: bool = False,
        dry_run: bool = False,
        explain: bool = False,
        format: str = "markdown",
        variables: dict[str, Any] | None = None,
        distinct_dimension_values: bool = True,
    ) -> str:
        """Query data from a semantic model. Call inspect(reference="<ds>.<model>", entity_type="model") first to see available columns and measures.

        Args:
            source_model: One of three forms:
                - **Model name** (string) — name of a saved model from models_summary, e.g. ``"orders"``.
                - **Inline ModelExtension** (dict) — extend an existing model with extra columns/joins/measures
                  for this one query: ``{"source_name": "orders", "columns": [{"name": "double_amount",
                  "sql": "amount * 2", "type": "DOUBLE"}]}``.
                - **Inline SlayerModel** (dict) — define a model ad-hoc:
                  ``{"name": "ad_hoc", "sql_table": "things", "data_source": "test", "columns": [...]}``.
            measures: Aggregated values to return. Each is a formula: {"formula": "*:count"},
                {"formula": "revenue:sum / *:count", "name": "aov"} (arithmetic),
                {"formula": "cumsum(revenue:sum)"} (cumulative sum), {"formula": "change(revenue:sum)"} (diff from previous row),
                {"formula": "change_pct(revenue:sum)"} (% change), {"formula": "time_shift(revenue:sum, -1)"} (previous period via self-join),
                {"formula": "time_shift(revenue:sum, -1, 'year')"} (year-over-year), {"formula": "lag(revenue:sum, 1)"} (previous row via window function),
                {"formula": "lead(revenue:sum, 1)"} (next row via window function), {"formula": "last(revenue:sum)"} (most recent),
                {"formula": "rank(revenue:sum)"} (ranking). A bare name like {"formula": "aov"} resolves to a saved ModelMeasure on the model.
            dimensions: List of dimension names to group by, e.g. ["status", "region"].
            filters: Filter conditions as formula strings. Examples: "status == 'completed'",
                "amount > 100", "status in ('a', 'b')", "status is None",
                "name like '%acme%'". Filters on measures are automatically routed to HAVING.
                Supports and/or: "status == 'a' or status == 'b'".
                Filters can also reference computed measure names or contain inline transforms:
                "change(revenue:sum) > 0", "last(change(revenue:sum)) < 0".
            time_dimensions: Time grouping. Format: {"dimension": "created_at", "granularity": "day|week|month|quarter|year", "date_range": ["2024-01-01", "2024-12-31"]}.
            order: Sorting. Format: {"column": "measure_or_dim_name", "direction": "asc|desc"}.
            limit: Max rows to return.
            offset: Number of rows to skip.
            whole_periods_only: When true, snap date filters to time bucket boundaries based on granularity, exclude the current incomplete time bucket.
            show_sql: When true, include the generated SQL in the response for debugging.
            dry_run: When true, generate and return the SQL without executing it.
            explain: When true, run EXPLAIN ANALYZE and return the query plan.
            format: Output format — "markdown" (default, compact and LLM-friendly), "json" (structured), or "csv" (most compact). Case-insensitive.
            distinct_dimension_values: Default True (Cube.js-style auto-dedup for dim-only queries — emits GROUP BY <dim aliases>). Set False to emit raw rows: no top-level GROUP BY, just SELECT <dimensions/time_dimensions> with the usual WHERE/ORDER BY/LIMIT. Any measure reference (in measures, filters, or order) raises an error in this mode.

        Example: query(source_model="orders", measures=[{"formula": "*:count"}], dimensions=["status"], filters=["status == 'completed'"])

        Before calling this tool, run ``search`` first, supplying the entities you're thinking of using (and/or the query itself via the ``query`` arg, or a free-text ``question``). Read the returned memories and consider any matching example queries before formulating the final query.
        """
        data: dict[str, Any] = {"source_model": source_model}
        if dimensions:
            data["dimensions"] = list(dimensions)
        if filters:
            data["filters"] = filters
        if time_dimensions:
            data["time_dimensions"] = list(time_dimensions)
        if order:
            data["order"] = list(order)
        if limit is not None:
            data["limit"] = limit
        if offset is not None:
            data["offset"] = offset
        if whole_periods_only:
            data["whole_periods_only"] = True
        if measures:
            data["measures"] = measures
        if variables:
            data["variables"] = dict(variables)
        # DEV-1543: only emit when non-default so tool calls stay compact.
        if distinct_dimension_values is False:
            data["distinct_dimension_values"] = False
        try:
            fmt = format.lower().strip()
            if fmt not in ("json", "csv", "markdown"):
                raise ValueError(f"Invalid format '{format}'. Must be one of: json, csv, markdown")
            # Run-by-name shortcut: when ``source_model`` is a stored model
            # name (string) and no overrides are given, dispatch through
            # ``engine.execute(str)`` so the model's stored backing query
            # runs directly with run-by-name variable precedence
            # (``runtime_kwarg > stage > model.query_variables``). Inline
            # ``ModelExtension`` / ``SlayerModel`` values fall through to
            # the regular ``SlayerQuery`` path below — they have no stored
            # backing query and the run-by-name semantics don't apply.
            # See DEV-1373 for the variable-precedence asymmetry between
            # the two paths.
            no_overrides = (
                not measures and not dimensions and not filters
                and not time_dimensions and not order
                and limit is None and offset is None
                and not whole_periods_only
                # DEV-1543: explicit ``False`` is a real override; default
                # ``True`` falls through.
                and distinct_dimension_values
            )
            if isinstance(source_model, str) and no_overrides:
                model_name = source_model
                target = await storage.get_model(model_name)
                if target is not None and target.source_queries:
                    result = await engine.execute(
                        query=model_name,
                        variables=variables or {},
                        dry_run=dry_run,
                        explain=explain,
                    )
                    if dry_run:
                        return f"SQL:\n{result.sql}"
                    if explain:
                        output = f"SQL:\n{result.sql}\n\nQuery Plan:\n"
                        output += _format_output(result=result, fmt=fmt)
                        return output
                    output = _format_output(result=result, fmt=fmt)
                    if show_sql and result.sql:
                        output = f"SQL:\n{result.sql}\n\n{output}"
                    return output
            slayer_query = SlayerQuery.model_validate(data)
            result = await engine.execute(
                query=slayer_query,
                variables=variables,
                dry_run=dry_run,
                explain=explain,
            )
            if dry_run:
                return f"SQL:\n{result.sql}"
            if explain:
                output = f"SQL:\n{result.sql}\n\nQuery Plan:\n"
                output += _format_output(result=result, fmt=fmt)
                return output
            output = _format_output(result=result, fmt=fmt)
            if show_sql and result.sql:
                output = f"SQL:\n{result.sql}\n\n{output}"
            if result.attributes and (result.attributes.dimensions or result.attributes.measures):
                output += "\n\n" + _format_attributes(attributes=result.attributes)
            return output
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                return _friendly_db_error(e)
            raise

    @mcp.tool()
    async def query_nested(
        queries: list[dict[str, Any]],
        variables: dict[str, Any] | None = None,
        show_sql: bool = False,
        dry_run: bool = False,
        explain: bool = False,
        format: str = "markdown",
    ) -> str:
        """Run a multi-stage query as a DAG. Use this when one stage depends on the output of another.

        ``queries`` is a list of query dicts forming a DAG. Each entry has the
        same shape as the regular ``query`` tool's arguments
        (``source_model``, ``measures``, ``dimensions``, ``filters``,
        ``time_dimensions``, ``order``, ``limit``, ``offset``,
        ``whole_periods_only``) plus an optional ``name``. Stages reference
        each other by name via ``source_model: "<sibling_name>"`` or
        ``joins.target_model``.

        Order doesn't matter — the engine auto-sorts so every stage
        appears after the siblings it references. The **last entry of
        the input is always the entry point / DAG root** (its result is
        what's returned); only the non-final entries are reordered.
        Every non-final entry must have a ``name``. Cycles,
        self-references, and a non-final stage referencing the root are
        rejected with a clear error. Stages that aren't reachable from
        the root are accepted as utility sub-queries — they're silently
        dropped from the emitted SQL.

        Args:
            queries: Ordered list of stage dicts. Earlier stages must be
                named; the last stage is the one whose rows return.
            variables: Variable values for ``{var}`` placeholder
                substitution in filters. Runtime kwarg precedence:
                ``runtime > stage.variables > outer query.variables >
                model.query_variables``.
            show_sql: When true, include the generated SQL in the response.
            dry_run: When true, generate the SQL without executing it.
            explain: When true, run EXPLAIN ANALYZE and return the plan.
            format: ``markdown`` (default), ``json``, or ``csv``.

        Example:
            queries=[
                {"name": "monthly", "source_model": "orders",
                 "measures": [{"formula": "*:count"}, {"formula": "revenue:sum"}],
                 "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]},
                {"source_model": "monthly", "measures": [{"formula": "*:count"}]}
            ]

        For a single-stage query, prefer the regular ``query`` tool — its
        typed arguments give a more discoverable schema.
        """
        try:
            fmt = format.lower().strip()
            if fmt not in ("json", "csv", "markdown"):
                raise ValueError(f"Invalid format '{format}'. Must be one of: json, csv, markdown")
            if not queries:
                raise ValueError("'queries' must be a non-empty list of query dicts.")
            result = await engine.execute(
                query=list(queries),
                variables=variables,
                dry_run=dry_run,
                explain=explain,
            )
            if dry_run:
                return f"SQL:\n{result.sql}"
            if explain:
                output = f"SQL:\n{result.sql}\n\nQuery Plan:\n"
                output += _format_output(result=result, fmt=fmt)
                return output
            output = _format_output(result=result, fmt=fmt)
            if show_sql and result.sql:
                output = f"SQL:\n{result.sql}\n\n{output}"
            if result.attributes and (result.attributes.dimensions or result.attributes.measures):
                output += "\n\n" + _format_attributes(attributes=result.attributes)
            return output
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                return _friendly_db_error(e)
            raise

    # -----------------------------------------------------------------------
    # Model discovery
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def models_summary(
        datasource_name: str,
        format: str = "markdown",
        compact: bool = True,
    ) -> str:
        """Brief summary of all (non-hidden) models in a datasource.

        DEV-1549: compact-by-default rendering. Under ``compact=True``
        each model section emits its name, description, the column count
        (``Columns: N``), the comma-separated measure NAMES
        (``Measures: a, b, c``) and the ``Joins to:`` list — no
        per-column table, no per-measure formula block. Pass
        ``compact=False`` to restore the verbose markdown / JSON shape
        with full column and measure payloads.

        Args:
            datasource_name: Name of the datasource (from list_datasources).
            format: Output format — "markdown" (default, compact and
                LLM-friendly) or "json" (structured array of model summaries).
                Case-insensitive.
            compact: Default True — drop per-column / per-measure detail.
                Set False to surface the full per-model tables.
        """
        fmt = format.lower().strip()
        if fmt not in ("markdown", "json"):
            raise ValueError(
                f"Invalid format '{format}' for models_summary. Must be 'markdown' or 'json'."
            )

        try:
            ds = await storage.get_datasource(datasource_name)
        except Exception as exc:
            logger.warning("Failed to load datasource '%s': %s", datasource_name, exc)
            return f"Datasource '{datasource_name}' has an invalid config."
        if ds is None:
            return f"Datasource '{datasource_name}' not found."

        all_names = await storage.list_models(data_source=datasource_name)
        matched: list[SlayerModel] = []
        for n in all_names:
            try:
                m = await storage.get_model(n, data_source=datasource_name)
            except Exception:
                logger.warning("Failed to load model '%s', skipping", n, exc_info=True)
                continue
            if m is not None and not m.hidden:
                matched.append(m)
        matched.sort(key=lambda m: m.name)

        if not matched:
            return f"Datasource '{datasource_name}' has no models."

        if fmt == "json":
            if compact:
                return json.dumps(
                    {
                        "datasource_name": datasource_name,
                        "model_count": len(matched),
                        "models": [
                            {
                                "name": m.name,
                                "description": m.description,
                                "column_count": sum(
                                    1 for c in m.columns if not c.hidden
                                ),
                                "measure_names": [mm.name for mm in m.measures],
                                "joins_to": sorted(
                                    {j.target_model for j in m.joins}
                                ),
                            }
                            for m in matched
                        ],
                    },
                    indent=2,
                )
            return json.dumps(
                {
                    "datasource_name": datasource_name,
                    "model_count": len(matched),
                    "models": [
                        {
                            "name": m.name,
                            "description": m.description,
                            "columns": [
                                {"name": c.name, "type": str(c.type), "description": c.description}
                                for c in m.columns if not c.hidden
                            ],
                            "measures": [
                                {"name": mm.name, "formula": mm.formula, "description": mm.description}
                                for mm in m.measures
                            ],
                            "joins_to": sorted({j.target_model for j in m.joins}),
                        }
                        for m in matched
                    ],
                },
                indent=2,
            )

        sections: list[str] = [
            f"# Datasource: `{datasource_name}` — {len(matched)} model(s)"
        ]
        for m in matched:
            model_lines: list[str] = [f"## `{m.name}`"]
            if m.description:
                model_lines.append(m.description)

            if compact:
                visible_col_count = sum(1 for c in m.columns if not c.hidden)
                model_lines.append(f"Columns: {visible_col_count}")
                measure_names = ", ".join(
                    mm.name for mm in m.measures if mm.name is not None
                )
                model_lines.append(f"Measures: {measure_names}")
                if m.joins:
                    targets = sorted({j.target_model for j in m.joins})
                    rendered = ", ".join(f"`{t}`" for t in targets)
                    model_lines.append(f"Joins to: {rendered}")
                else:
                    model_lines.append("Joins to: _(none)_")
                sections.append("\n".join(model_lines))
                continue

            col_rows = [
                {"name": c.name, "type": str(c.type), "description": c.description}
                for c in m.columns if not c.hidden
            ]
            model_lines.append(f"**Columns ({len(col_rows)}):**")
            model_lines.append("")
            model_lines.append(
                _markdown_table(rows=col_rows, columns=["name", "type", "description"])
            )
            model_lines.append("")

            measure_rows = [
                {"name": mm.name, "formula": mm.formula, "description": mm.description}
                for mm in m.measures
            ]
            model_lines.append(f"**Measures ({len(measure_rows)}):**")
            model_lines.append("")
            model_lines.append(
                _markdown_table(rows=measure_rows, columns=["name", "formula", "description"])
            )
            model_lines.append("")

            if m.joins:
                targets = sorted({j.target_model for j in m.joins})
                rendered = ", ".join(f"`{t}`" for t in targets)
                model_lines.append(f"**Joins to:** {rendered}")
            else:
                model_lines.append("**Joins to:** _(none)_")

            sections.append("\n".join(model_lines))

        return "\n\n".join(sections)

    @mcp.tool()
    async def inspect_model(
        model_name: str,
        num_rows: int = 3,
        show_sql: bool = False,
        format: str = "markdown",
        sections: list[str] | None = None,
        descriptions_max_chars: int | None = None,
        data_source: str | None = None,
        compact: bool = True,
    ) -> str:
        """DEPRECATED: use the ``inspect`` tool. Return a complete-yet-compact view of a semantic model.

        Always emitted (regardless of ``sections``): model header + description,
        metadata bullets (data_source, sql_table, default_time_dimension,
        hidden, row_count), backing-query structure for query-backed models,
        and — when ``show_sql=True`` — the custom SQL block, model-level
        filters, and the cached backing-query SQL.

        Section-gated parts (subset selectable via ``sections``):

        - ``columns`` — unified row-level columns table with a ``sampled``
          column (distinct values for string/boolean, ``min .. max`` for
          number/date/time, or ``top20 ... (N distinct)`` for high-
          cardinality categoricals).
        - ``measures`` — named-formula library.
        - ``aggregations`` — custom aggregation definitions. The ``formula``
          column and the ``sql`` field of each ``params[]`` entry are gated
          by ``show_sql``.
        - ``joins`` — join definitions.
        - ``samples`` — live sample-data query (``COUNT(*)`` plus one
          aggregation per column).
        - ``learnings`` — learning-only memories whose canonical entities
          reference this model.

        When a section is omitted from ``sections``: ``columns``, ``measures``,
        ``aggregations`` and ``joins`` collapse to a one-line backticked CSV
        of names; ``samples`` and ``learnings`` are dropped entirely.
        A footer at the end of the response lists what was trimmed and how
        to fetch more.

        Args:
            model_name: Name of the model to inspect.
            num_rows: Max sample-data rows (default: 3).
            show_sql: When true, include the generated SQL for the sample-data
                query, the custom SQL block, model-level filters, the cached
                backing-query SQL, and aggregation formulas/param SQL.
            format: Output format — ``"markdown"`` (default) or ``"json"``.
                Case-insensitive.
            sections: Subset of ``["columns", "measures", "aggregations",
                "joins", "samples", "learnings"]``. Default (``None``
                or empty list) renders all six. Unknown names are ignored
                with a warning line at the end of the response. A non-empty
                list of *only* unknown names resolves to no sections (not
                all six) — "all sections" is reserved for ``None``/``[]`` so
                a typo can't silently trigger the full expensive payload.
            descriptions_max_chars: When set, every description field (model,
                column, measure, aggregation) longer than this is truncated
                with a ``... [truncated]`` suffix. Must be ``>= 0``. ``None``
                (default) means no truncation.
        """
        try:
            model = await storage.get_model(model_name, data_source=data_source)
        except AmbiguousModelError as exc:
            return _ambiguous_with_mcp_hint(exc)
        if model is None:
            identities = await storage._list_all_model_identities()
            available = []
            for ds_name, n in identities:
                m = await storage.get_model(n, data_source=ds_name)
                if m is not None and not m.hidden:
                    available.append(f"{ds_name}.{n}")
            available.sort()
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

    @mcp.tool()
    async def inspect(
        reference: str | list[str],
        entity_type: str,
        compact: bool = True,
        format: str = "markdown",
        num_rows: int = 3,
        show_sql: bool = False,
        sections: list[str] | None = None,
        descriptions_max_chars: int | None = None,
    ) -> str:
        """Inspect EXACTLY one entity by reference and kind — or a homogeneous
        BATCH when ``reference`` is a list.

        A clean point-lookup: no fusion / ranking / cypher, and no bundled
        memories. Use ``search`` instead when you want an entity surfaced *in
        context* (with related memories and ranked neighbours).

        Batch (DEV-1612): pass a ``list`` of references that all share the one
        ``entity_type``. Returns one rendered block per id, in input order,
        each echoing its resolved canonical id (a ``## <canonical>`` header in
        markdown; a JSON array under ``format="json"``). Per-id resolution
        errors are isolated — one bad id does not sink the batch (in JSON it
        becomes a ``{"reference": ..., "error": ...}`` element). A single
        ``str`` keeps its byte-for-byte single output; a one-element list is
        still batch-framed.

        Args:
            reference: The entity reference, or a list of references (batch).
                Accepts canonical forms (``mydb``, ``mydb.orders``,
                ``mydb.orders.amount``), bare names, join paths
                (``orders.customers.region`` → resolved to the owning model),
                and ``memory:<id>`` for memories. Normalised via the shared
                resolver; the normalised canonical id is echoed in the JSON
                shape.
            entity_type: REQUIRED. One of ``datasource``, ``model``,
                ``column``, ``measure``, ``aggregation``, ``memory``.
                Disambiguates the 3-part canonical collision (a name
                shared by, e.g., a column and an aggregation) and asserts
                the resolved kind — a mismatch returns a detailed error.
            compact: When true (default): description-only for
                column/measure/aggregation/datasource/memory; for
                ``entity_type="model"`` a cheap schema skeleton (column /
                measure / aggregation names + join targets, zero DB calls).
                False returns the full render (and, for the datasource kind,
                a per-model skeleton for each visible model).
            format: ``"markdown"`` (default) or ``"json"``.
            num_rows: Sample-data rows for ``entity_type="model"``. Ignored
                (with a warning) for other kinds.
            show_sql: Include generated SQL for ``entity_type="model"``.
                Ignored (with a warning) for datasource/memory; a silent
                no-op for column/measure/aggregation.
            sections: Section subset for ``entity_type="model"``. Ignored
                (with a warning) for other kinds.
            descriptions_max_chars: Truncate description fields to this many
                characters. Applies to every kind.
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

    # -----------------------------------------------------------------------
    # Model creation and editing
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def create_model(
        name: str,
        sql_table: str | None = None,
        sql: str | None = None,
        data_source: str | None = None,
        description: str | None = None,
        columns: list[dict[str, Any]] | None = None,
        measures: list[dict[str, Any]] | None = None,
        query: Any | None = None,
        variables: dict[str, Any] | None = None,
    ) -> str:
        """Create a new semantic model, either from a database table or from a query.

        **From a table** (provide sql_table or sql):
            create_model(name="orders", sql_table="public.orders", data_source="mydb",
                         columns=[...], measures=[...])

        **From a query** (provide query):
            create_model(name="monthly_summary", query={"source_model": "orders",
                         "measures": ["*:count", "amount:sum"],
                         "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]})
            Columns are auto-introspected from the query result.

        Args:
            name: Unique model name (lowercase, underscores).
            sql_table: Database table name, e.g. "public.orders".
            sql: Alternative to sql_table — a custom SQL expression for the model's source.
            data_source: Name of the datasource (from list_datasources).
            description: What this model represents.
            columns: List of column definitions. Each: {"name": "col", "sql": "col", "type": "string"}.
                Types: string, number, time, date, boolean. Optional fields: ``primary_key``,
                ``allowed_aggregations`` (whitelist), ``filter`` (CASE WHEN inside aggregation),
                ``label``, ``description``, ``hidden``, ``meta``.
            measures: List of named formula definitions on the model. Each:
                {"name": "aov", "formula": "revenue:sum / *:count", "label": "...",
                 "description": "...", "meta": {...}}.
                Queries can reference these by bare name (e.g. ``{"formula": "aov"}``).
                ``meta`` is an optional opaque dict for caller bookkeeping
                (e.g. linking the formula back to a source identifier).
            query: A SLayer query dict (or list of stage dicts for a multi-stage backing
                query). When provided, the query is saved as the model's ``source_queries``
                and the model becomes query-backed. Mutually exclusive with sql_table, sql,
                columns, and measures.
            variables: Default values for ``{var}`` placeholders in the backing query.
                Saved as ``query_variables`` on the model. Only meaningful when ``query``
                is provided.
        """
        if query is not None:
            table_params = {
                k: v for k, v in {
                    "sql_table": sql_table, "sql": sql, "data_source": data_source,
                    "columns": columns, "measures": measures,
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
                if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
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
        )
        model = SlayerModel.model_validate(data)
        existed = (
            await storage.get_model(name, data_source=model.data_source)
            is not None
        )
        await storage.save_model(model)
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

    @mcp.tool()
    async def edit_model(
        model_name: str,
        description: str | None = None,
        data_source: str | None = None,
        new_data_source: str | None = None,
        default_time_dimension: str | None = None,
        sql_table: str | None = None,
        sql: str | None = None,
        source_queries: list[dict[str, Any]] | None = None,
        query_variables: Any = _UNSET,
        hidden: bool | None = None,
        columns: list[dict[str, Any]] | None = None,
        measures: list[dict[str, Any]] | None = None,
        aggregations: list[dict[str, Any]] | None = None,
        joins: list[dict[str, Any]] | None = None,
        add_filters: list[str] | None = None,
        remove_filters: list[str] | None = None,
        remove: dict[str, list[str]] | None = None,
        meta: dict[str, Any] | None = _UNSET,
    ) -> str:
        """Edit an existing model in a single call — update metadata, upsert columns/measures/aggregations/joins,
        manage filters, and remove entities.

        Args:
            model_name: Name of the model to edit.
            description: New model description.
            data_source: Lookup key — the datasource the model belongs to.
                Required when the same name exists in multiple datasources
                (otherwise the priority list / single-match rules apply).
            new_data_source: Move the model to a different datasource (rare;
                renames its storage location). Pass ``None`` (default) to
                leave the data_source unchanged.
            default_time_dimension: Default time dimension (a column of type date/time) for
                time-dependent transforms.
            sql_table: Database table name. Setting this clears ``sql`` and ``source_queries``.
            sql: Custom SQL expression for the model source. Setting this clears ``sql_table`` and ``source_queries``.
            source_queries: Replace the model's backing query with this list of stages.
                Each stage is a SlayerQuery dict; non-final stages must have a ``name``.
                Setting this clears ``sql_table`` and ``sql``, makes the model query-backed,
                and refreshes the cached ``columns`` and ``backing_query_sql``.
            query_variables: Replace the model's default ``{var}`` placeholder values for
                its backing query. Pass null/None to clear. Only meaningful for
                query-backed models.
            hidden: Whether this model is hidden from discovery.
            meta: Arbitrary JSON metadata for the model (replaces existing meta). Pass null/None to clear.
            columns: Columns to create or update (upsert by name). Each dict:
                {"name": "col", "type": "string", "sql": "col", "description": "...",
                 "primary_key": false, "hidden": false, "allowed_aggregations": ["sum", "avg"],
                 "filter": "status = 'active'", "label": "..."}.
                If a column with this name exists, only the provided fields are updated.
                Types: string, number, time, date, boolean.
            measures: Named formula measures to create or update (upsert by name). Each dict:
                {"name": "aov", "formula": "revenue:sum / *:count", "label": "...",
                 "description": "...", "meta": {...}}.
                Queries can reference these by bare name (e.g. ``{"formula": "aov"}``).
                ``meta`` is an optional opaque dict for caller bookkeeping.
            aggregations: Aggregations to create or update (upsert by name). Each dict:
                {"name": "weighted_avg", "formula": "SUM({value} * {weight}) / NULLIF(SUM({weight}), 0)",
                 "params": [{"name": "weight", "sql": "quantity"}], "description": "...",
                 "meta": {...}}.
                ``meta`` is an optional opaque dict for caller bookkeeping.
            joins: Joins to create or update (upsert by target_model). Each dict:
                {"target_model": "customers", "join_pairs": [["customer_id", "id"]]}.
            add_filters: SQL filter strings to add (e.g. ["deleted_at IS NULL"]). Duplicates ignored.
            remove_filters: SQL filter strings to remove (exact match).
            remove: Named entities to delete, keyed by type:
                {"columns": ["col_name"], "measures": ["measure_name"],
                 "aggregations": ["agg_name"], "joins": ["target_model_name"]}.
                Removals are processed before upserts.

        Example — update a column and add a named measure:
            edit_model(model_name="orders",
                       columns=[{"name": "status", "type": "string"}],
                       measures=[{"name": "aov", "formula": "revenue:sum / *:count"}])
        Example — remove a measure:
            edit_model(model_name="orders", remove={"measures": ["old_metric"]})
        """
        try:
            model = await storage.get_model(model_name, data_source=data_source)
        except AmbiguousModelError as exc:
            return _ambiguous_with_mcp_hint(exc)
        if model is None:
            return f"Model '{model_name}' not found."

        original_data_source = model.data_source
        changes: list[str] = []
        # DEV-1375: track refresh-triggering changes so the post-save hook
        # knows whether to refresh just the touched columns or every
        # column on the model.
        changed_columns: set = set()
        model_level_change = False
        # DEV-1386: pure model-doc changes (measures / aggregations /
        # joins) don't invalidate ``Column.sampled`` but DO change the
        # embedding text rendered by ``slayer.search.render``. Track
        # these separately so the embedding refresh fires without
        # triggering a full per-column sample-value re-profile.
        model_doc_changed = False

        # --- Phase 1: Scalar metadata ---
        if description is not None:
            model.description = description
            changes.append("updated description")
        if new_data_source is not None and new_data_source != model.data_source:
            # v4: moving a model between datasources is delete-old +
            # save-new. To avoid losing the source row when validation/save
            # fails, we (a) refuse if a sibling already lives at the target
            # ``(new_data_source, model.name)`` key, and (b) defer the
            # delete-from-old until *after* the new save succeeds (handled
            # below in Phase 5). Here we only mutate the in-memory model.
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
            from slayer.core.query import SlayerQuery as _SlayerQuery
            model.source_queries = [_SlayerQuery.model_validate(q) for q in source_queries]
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
        # For query-backed models, columns are an engine-managed cache.
        # If we end up with source_queries set after this edit, we route through
        # engine.save_model so the cache is refreshed (and any user-supplied
        # cache fields are rejected). Otherwise, persist directly via storage.
        try:
            validated = SlayerModel.model_validate(model.model_dump(mode="json"))
        except Exception as exc:
            return f"Validation error: {exc}"

        if validated.source_queries:
            # ``columns`` and ``backing_query_sql`` are engine-managed for
            # query-backed models. Reject explicit user supply rather than
            # silently dropping (which would let the API report a successful
            # column edit that never persists).
            if columns is not None:
                return (
                    "Validation error: cannot supply 'columns' on a "
                    f"query-backed model ('{model_name}'). Columns are "
                    "engine-managed (auto-derived from the backing query)."
                )
            # Strip cache fields before save so engine.save_model can repopulate
            # them from a fresh _query_as_model pass. (These are present here
            # only because they were on the existing stored model, not from
            # this edit.)
            validated = validated.model_copy(update={
                "columns": [],
                "backing_query_sql": None,
            })
            try:
                # ``engine.save_model`` may RECOMPUTE ``data_source`` for
                # query-backed models from the resolved virtual model, so
                # we cannot trust ``validated.data_source`` after this
                # call — use the returned model's identity for the
                # post-save cleanup decision below.
                saved_model = await engine.save_model(validated)
            except Exception as exc:
                return f"Validation error: {exc}"
        else:
            try:
                await storage.save_model(validated)
                saved_model = validated
            except Exception as exc:
                # Source row is still intact because we deferred the
                # delete. Surface the failure as an error string instead
                # of letting MCP wrap it as a ToolError.
                return f"Storage error: {exc}"

        # v4 atomic move: only after the new save has succeeded do we
        # remove the source row, and only if the saved model actually
        # landed at a different ``data_source`` than where it started.
        # For query-backed models the engine-side cache populator can
        # override ``new_data_source`` (it derives ``data_source`` from
        # the backing query); without this guard a "move that didn't
        # move" silently deleted the just-saved row at the original key.
        if saved_model.data_source != original_data_source:
            await storage.delete_model(
                saved_model.name, data_source=original_data_source
            )
        # DEV-1375 / DEV-1386: refresh persisted ``Column.sampled``
        # values for any touched columns (or every column when a
        # source-level change made every column's sample suspect), and
        # refresh embeddings for the model subtree on any edit that
        # changed the indexed text. Best-effort: any raise here is
        # captured into ``refresh_warnings`` so the save's success
        # status survives a flaky embedding API.
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

    # -----------------------------------------------------------------------
    # Datasource management
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def create_datasource(
        name: str,
        type: str,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        username: str | None = None,
        password: str | None = None,
        connection_string: str | None = None,
        schema_name: str | None = None,
        auto_ingest: bool = True,
    ) -> str:
        """Create a database connection, verify it, and auto-ingest models. Use ${ENV_VAR} syntax in credentials to reference environment variables.

        Args:
            name: Unique datasource name.
            type: Database type — postgres, mysql, sqlite, bigquery, or snowflake.
            host: Database host (default: localhost).
            port: Database port (e.g. 5432 for Postgres).
            database: Database name.
            username: Database username.
            password: Database password.
            connection_string: Full connection string as alternative to individual fields.
            schema_name: Default schema name. Also used as the schema for auto-ingestion.
            auto_ingest: Automatically ingest models from the database schema (default: true). Set to false to skip.

        Example: create_datasource(name="mydb", type="postgres", host="localhost", port=5432, database="app", username="user", password="pass")
        """
        from slayer.engine.ingestion import ingest_datasource as _ingest

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
        ds = DatasourceConfig.model_validate(data)
        existed = await storage.get_datasource(name) is not None
        await storage.save_datasource(ds)
        verb = "replaced" if existed else "created"

        ok, msg = _test_connection(ds)
        if not ok:
            return f"Datasource '{ds.name}' {verb}, but connection test failed.\n{msg}"

        lines = [f"Datasource '{ds.name}' {verb}. {msg}"]

        if not auto_ingest:
            return "\n".join(lines)

        # Auto-ingest models
        try:
            models = _ingest(datasource=ds, schema=schema_name or None)
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                lines.append(f"Auto-ingestion failed: {_friendly_db_error(e)}")
                return "\n".join(lines)
            raise

        for model in models:
            await storage.save_model(model)

        if not models:
            lines.append("No tables found to ingest.")
            schemas = _get_schemas(ds)
            if schemas:
                lines.append(f"Available schemas: {', '.join(schemas)}")
        else:
            lines.append(f"Ingested {len(models)} model(s):")
            for m in models:
                lines.append(f"- {m.name} ({len(m.columns)} columns, {len(m.measures)} measures)")
            lines.append("")
            lines.append("Use models_summary and inspect to explore, then query to fetch data.")

        return "\n".join(lines)

    @mcp.tool()
    async def list_datasources() -> str:
        """List all configured database connections (names and types only, credentials are not shown). Use describe_datasource for connection details and status."""
        names = await storage.list_datasources()
        if not names:
            return "No datasources configured. Use create_datasource to add a database connection."
        lines = []
        for name in names:
            try:
                ds = await storage.get_datasource(name)
                ds_type = ds.type if ds else "unknown"
                lines.append(f"- {name} ({ds_type})")
            except Exception as exc:
                logger.warning("Failed to load datasource '%s': %s", name, exc)
                lines.append(f"- {name} (ERROR: invalid datasource config)")
        return "\n".join(lines)

    @mcp.tool()
    async def describe_datasource(
        name: str,
        list_tables: bool = True,
        schema_name: str = "",
    ) -> str:
        """Show datasource details: connection status, available schemas, and (by default) the tables in the given or default schema.

        Use this after create_datasource to verify the connection and explore
        what's queryable before calling ingest_datasource_models.

        Args:
            name: Datasource name (from list_datasources).
            list_tables: If True (default), append a list of tables from the
                schema named by ``schema_name`` (or the dialect's default
                schema when empty).
            schema_name: Database schema to list tables from (e.g. "public").
                Empty uses the dialect default. Ignored when list_tables=False.
        """
        try:
            ds = await storage.get_datasource(name)
        except Exception as exc:
            logger.warning("Failed to load datasource '%s': %s", name, exc)
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
                for t in tables:
                    lines.append(f"  - {t}")
                lines.append(
                    "\nUse ingest_datasource_models to create models from these tables."
                )
            else:
                lines.append(f"\nNo tables found{schema_label}.")

        return "\n".join(lines)

    @mcp.tool()
    async def edit_datasource(
        name: str,
        description: str | None = None,
    ) -> str:
        """Update a datasource's metadata.

        Args:
            name: Datasource name to update.
            description: New description for the datasource.
        """
        ds = await storage.get_datasource(name)
        if ds is None:
            return f"Datasource '{name}' not found."

        old_description = ds.description
        if description is not None:
            ds.description = description

        await storage.save_datasource(ds)

        # DEV-1549: the datasource embedding text now includes
        # ``DatasourceConfig.description``, so an edit to the
        # description must refresh the embedding inline — otherwise the
        # persisted row stays stale until the next ``slayer ingest``
        # and description-only semantic matches silently miss.
        #
        # The save is already committed at this point. Per CodeRabbit
        # round-7 review: the refresh is post-save and best-effort —
        # log a warning if it raises and surface a partial-success
        # message rather than telling the agent the save itself failed.
        refresh_warning: str | None = None
        if description is not None and description != old_description:
            models_in_ds: list[SlayerModel] = []
            for model_name in await storage.list_models(data_source=name):
                m = await storage.get_model(model_name, data_source=name)
                if m is not None:
                    models_in_ds.append(m)
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

    # -----------------------------------------------------------------------
    # Delete operations
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def delete_model(name: str, data_source: str | None = None) -> str:
        """Delete a semantic model.

        Args:
            name: Model name to delete.
            data_source: Datasource the model belongs to. Required when the
                same name exists in multiple datasources (otherwise the
                priority list / single-match rules apply).
        """
        try:
            deleted = await storage.delete_model(name, data_source=data_source)
        except AmbiguousModelError as exc:
            return _ambiguous_with_mcp_hint(exc)
        if deleted:
            return f"Model '{name}' deleted."
        return f"Model '{name}' not found."

    @mcp.tool()
    async def validate_models(data_source: str | None = None) -> str:
        """Diff persisted SLayer models against the live database schema(s).

        Returns a JSON-serialized list of pending delete operations
        (column drops, measure drops, join drops, filter removals, whole
        models) needed to keep stored models valid against the current
        live state. Read-only — does not modify storage.

        Args:
            data_source: Datasource name to validate. When omitted, every
                datasource is validated concurrently and results are
                concatenated.
        """
        if data_source is not None:
            # Fail loudly on an unknown name. Without this guard the engine
            # returns ``[]`` because no persisted models match, which is
            # indistinguishable from "no drift" — risky for an agent flow.
            ds = await storage.get_datasource(data_source)
            if ds is None:
                return f"Datasource '{data_source}' not found."
        engine = SlayerQueryEngine(storage=storage)
        try:
            entries = await engine.validate_models(data_source=data_source)
        except (sa.exc.OperationalError, sa.exc.DatabaseError) as exc:
            return _friendly_db_error(exc)
        return json.dumps([e.model_dump(mode="json") for e in entries], indent=2)

    @mcp.tool()
    async def recommend_root_model(
        items: list[str], data_source: str | None = None, format: str = "markdown"  # noqa: A002
    ) -> str:
        """Recommend the root model (query ``source_model``) for a set of
        ``model.column`` / ``model.metric`` items, and give each item's
        join-qualified reference path from that root.

        Introspects the join graph and picks the model from which every
        requested item is reachable (LEFT joins are directional; INNER
        joins traverse both ways), minimizing total join hops. The returned
        paths are ready to drop into a query whose ``source_model`` is the
        recommended root — e.g. a joined column comes back as
        ``customers.regions.name`` and a root-owned one as ``status``;
        aggregation suffixes (``:sum``) are preserved.

        When no single model reaches everything, ``root_model`` is null and
        ``coverage`` lists the best partial roots so you can split the
        request into a multi-stage query.

        Args:
            items: entity references (``orders.revenue``, ``customers.name``,
                ``orders.revenue:sum``, bare ``aov`` for a saved metric...).
            data_source: optional datasource scope; when omitted, names
                resolve via the datasource-priority list. All items must
                resolve to a single datasource.
            format: ``"markdown"`` (default) or ``"json"``.
        """
        fmt = format.lower().strip()
        if fmt not in ("markdown", "json"):
            return (
                f"recommend_root_model failed: unknown format '{format}'. "
                f"Use 'markdown' or 'json'."
            )
        engine = SlayerQueryEngine(storage=storage)
        try:
            rec = await engine.recommend_root_model(items, data_source=data_source)
        except AmbiguousModelError as exc:
            return _ambiguous_with_mcp_hint(exc)
        except (ValueError, EntityResolutionError) as exc:
            return f"recommend_root_model failed: {exc}"
        if fmt == "json":
            return json.dumps(rec.model_dump(mode="json"), indent=2)
        return render_recommendation_markdown(rec)

    @mcp.tool()
    async def delete_datasource(name: str) -> str:
        """Delete a datasource configuration.

        Args:
            name: Datasource name to delete.
        """
        if await storage.delete_datasource(name):
            return f"Datasource '{name}' deleted."
        return f"Datasource '{name}' not found."

    # -----------------------------------------------------------------------
    # Ingestion
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def ingest_datasource_models(datasource_name: str, include_tables: str = "", schema_name: str = "") -> str:
        """Auto-discover tables in a database and create / additively update semantic models from them.

        Idempotent (DEV-1356): re-runs are additive only. New columns and joins
        are appended to existing models; existing column / join definitions
        are never overwritten. After the additive pass, returns the pending
        ``validate_models`` deletes alongside the additions.

        Args:
            datasource_name: Name of an existing datasource (from list_datasources).
            include_tables: Comma-separated list of table names to include. If empty, all tables are ingested.
            schema_name: Database schema to inspect (e.g. "public"). If empty, uses the default schema.
        """
        from slayer.engine.ingestion import ingest_datasource_idempotent

        ds = await storage.get_datasource(datasource_name)
        if ds is None:
            return f"Datasource '{datasource_name}' not found."

        try:
            include = [t.strip() for t in include_tables.split(",") if t.strip()] or None
            result = await ingest_datasource_idempotent(
                datasource=ds,
                storage=storage,
                include_tables=include,
                schema=schema_name or None,
            )
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                return _friendly_db_error(e)
            raise

        return _render_ingest_result(
            result, schema_name=schema_name, ds=ds
        )

    @mcp.tool()
    async def set_datasource_priority(priority: list[str]) -> str:
        """Configure how SLayer disambiguates bare model names that exist in
        multiple datasources.

        When two datasources both define a model named ``users``, calling
        ``edit_model("users")`` (no ``data_source=``) is ambiguous. SLayer
        walks this priority list and picks the first datasource that has
        the requested name. If none of the candidates appear in the list,
        an ``AmbiguousModelError`` is raised.

        Args:
            priority: Datasource names, most-preferred first. Each entry
                must already exist (run ``list_datasources`` first). Pass
                an empty list to clear the priority.
        """
        try:
            await storage.set_datasource_priority(list(priority))
        except ValueError as exc:
            return str(exc)
        if not priority:
            return "Datasource priority cleared."
        return f"Datasource priority set: {list(priority)}."

    @mcp.tool()
    async def get_datasource_priority() -> str:
        """Return the configured datasource priority list (most-preferred
        first), or ``[]`` if none is set."""
        priority = await storage.get_datasource_priority()
        return f"Datasource priority: {priority}"

    # ---------- DEV-1357 v2: unified Memory surface -------------------

    memory_service = MemoryService(storage=storage)

    def _format_resolution_error(exc: Exception) -> str:
        """Convert a typed resolution / not-found / ambiguous error into
        a friendly text response (matches the existing convention of
        never raising back to the agent)."""
        if isinstance(exc, AmbiguousModelError):
            return _ambiguous_with_mcp_hint(exc)
        return f"Error: {type(exc).__name__}: {exc}"

    @mcp.tool()
    async def save_memory(
        learning: str,
        linked_entities: Any,
        id: str | None = None,  # noqa: A002 — MCP arg name
        description: str | None = None,
    ) -> str:
        """Save an agent memory: a free-form note plus the SLayer
        entities it concerns.

        ``linked_entities`` accepts either:

        * a list of entity reference strings — each item is resolved to
          the canonical ``<datasource>.<model>[.<leaf>]`` form. Bare
          names use the datasource priority list; ambiguous bare-column
          matches are rejected. ``memory:<id>`` is also valid here
          (cross-memory references; the target memory must exist).
        * a ``SlayerQuery`` (dict) — entities are auto-extracted from
          ``source_model``, ``dimensions``, ``time_dimensions``,
          ``measures``, and ``filters``; resolution warnings are
          non-fatal. The query itself is stored alongside the
          learning, so the memory surfaces in ``search``'s
          ``example_queries`` list (vs the ``memories`` list for
          entity-list memories).

        DEV-1428: ``id`` is an optional canonical memory id. Omit to
        auto-allocate a monotonic int-shaped id (``"1"``, ``"2"``, ...);
        supply a string for a stable user-controlled id
        (``"kb.policy.42"``). Charset excludes ``:``, ``/``, ``?``,
        ``#``, whitespace. Duplicate id → unconditional upsert,
        ``created_at`` preserved.

        Returns the assigned ``memory_id`` (string), the canonical
        entities stored, and any non-fatal warnings.

        Cascade-on-delete: when a model / datasource / measure is
        deleted, every ``memory:<id>`` and ``<ds>.<model>[.<leaf>]``
        reference under it is automatically stripped from every other
        memory's ``entities`` list. Memories with zero entities after
        the strip are kept (the learning text stands alone).

        Search is lenient: stale entity tags in saved memories are
        filtered out at retrieval time rather than raising.

        Args:
            learning: The note text. Required, non-empty.
            linked_entities: List of entity strings, or an inline
                ``SlayerQuery`` payload.
            id: Optional canonical memory id (see above).

        Examples:
            save_memory(
                learning="orders.is_returned in {0,1,NULL}; treat NULL as not returned",
                linked_entities=["orders.is_returned"],
            )

            save_memory(
                learning="Paid revenue by status",
                linked_entities={
                    "source_model": "orders",
                    "measures": [{"formula": "amount:sum"}],
                    "filters": ["status = 'paid'"],
                },
                id="kb.paid-revenue",
            )
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

    @mcp.tool()
    async def forget_memory(id: Any) -> str:  # noqa: A002 — MCP arg name
        """Delete a memory by id.

        Cascades: every other memory's ``memory:<id>`` reference to
        this id is automatically stripped from its ``entities`` list.

        Args:
            id: The ``memory_id`` returned by ``save_memory``. Accepts
                strings (the canonical form, including user-supplied
                ``"kb.policy"``-style ids) as well as legacy ints
                (coerced to their decimal string form).

        Raises a friendly error if the id is invalid or the memory does
        not exist.
        """
        try:
            response = await memory_service.forget_memory(identifier=id)
        except (
            MemoryNotFoundError,
            ValueError,
        ) as exc:
            return _format_resolution_error(exc)
        return response.model_dump_json(indent=2)

    # ---------- DEV-1375: semantic search -----------------------------

    # DEV-1516: pass the engine so the search service's post-fusion
    # column-hit hook can auto-refresh stale categorical columns.
    search_service = SearchService(storage=storage, engine=engine)

    @mcp.tool()
    async def search(
        entities: list[str] | None = None,
        query: Any = None,
        question: str | None = None,
        datasource: str | None = None,
        max_results: int = 10,
        cypher_filter: str | None = None,
        compact: bool = True,
    ) -> str:
        """Up to three-channel semantic search over memories + canonical entities.

        Call this BEFORE ``query`` to surface any notes or example
        queries previously saved against the entities you're
        considering.

        Channel 1 (entity-overlap BM25 over memories): runs when
        ``entities`` and/or ``query`` is supplied. Memories whose
        canonical entity tags overlap the resolved input are ranked.

        Channel 2 (tantivy full-text over memories ∪ entities): runs
        when ``question`` is supplied. The in-memory index covers every
        memory + every searchable entity (datasource / non-hidden model /
        non-hidden column / named measure / aggregation).

        Channel 3 (dense embedding similarity, optional): runs when
        ``question`` is supplied AND the ``advanced_search`` extra is
        installed AND a provider API key is configured for the active
        embedding model. Cosine similarity between the question
        embedding and persisted entity/memory embeddings. Skipped with
        a single warning into ``SearchResponse.warnings`` when any
        precondition fails — tantivy + BM25 continue to work.

        All hits (memories, example queries, entities) are fused via
        Reciprocal Rank Fusion (k=60) into a single ranked
        ``results`` list capped at ``max_results``.

        Empty input (no entities, no query, no question) returns the
        newest memories capped at ``max_results``, with a warning.

        Args:
            entities: Canonical entity reference strings.
            query: Optional ``SlayerQuery`` (dict). Entities are
                auto-extracted to broaden channel-1 input.
            question: Free-text query for the tantivy full-text channel.
            datasource: Optional datasource name. When set, scope all
                three channels to that one datasource. Entity hits are
                limited to docs rooted at the datasource (exact match
                or dotted-path descendant). Memories surface when any
                of their tagged entities is rooted at the datasource —
                a memory spanning multiple datasources surfaces from
                each. BM25 / IDF stats reflect only the filtered subset.
                Unknown datasource raises ``ValueError``.
            max_results: Maximum total number of hits to return (default 10).
            cypher_filter: Optional openCypher MATCH query returning
                ``… AS id`` that pre-filters all three channels to the
                returned canonical IDs. When ``advanced_search`` is not
                installed, only simple
                ``MATCH (n:Label1:Label2) RETURN n.id AS id`` patterns are
                supported as a kind filter (multi-label uses union
                semantics; allowed labels: Memory, Datasource, Model,
                Column, Measure, Aggregation).
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


def _format_json(data: list[dict[str, Any]], columns: list[str]) -> str:
    """Format data as JSON array."""
    import json

    return json.dumps(data, default=str)


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


def _format_output(result: SlayerResponse, fmt: str) -> str:
    """Format query output in the requested format."""
    if fmt == "csv":
        return _format_csv(data=result.data, columns=result.columns)
    if fmt == "markdown":
        return result.to_markdown()
    return _format_json(data=result.data, columns=result.columns)


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
    return "\n".join(lines)if lines else ""