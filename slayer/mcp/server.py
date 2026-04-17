"""MCP server for SLayer."""

import json
import logging
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

import sqlalchemy as sa

from slayer.core.models import (
    Aggregation,
    DatasourceConfig,
    Dimension,
    Measure,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.help import TOPIC_SUMMARY_LINE, render_help
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)

VALID_DIMENSION_TYPES = {"string", "time", "date", "boolean", "number"}
_UNSET = object()  # Sentinel to distinguish "not provided" from "explicitly set to None"

# Aggregations that are safe for sample-data extraction: zero extra args,
# no time-column context needed.
_SAFE_SAMPLE_AGGS = frozenset({"avg", "sum", "min", "max", "count", "count_distinct", "median"})


def _test_connection(ds: DatasourceConfig) -> tuple[bool, str]:
    """Test a datasource connection. Returns (success, message)."""
    try:
        conn_str = ds.resolve_env_vars().get_connection_string()
        engine = sa.create_engine(conn_str)
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        engine.dispose()
        return True, "Connection successful."
    except Exception as e:
        return False, _friendly_db_error(e)


def _get_schemas(ds: DatasourceConfig) -> list[str]:
    """List available schemas for a datasource."""
    try:
        conn_str = ds.resolve_env_vars().get_connection_string()
        engine = sa.create_engine(conn_str)
        inspector = sa.inspect(engine)
        schemas = inspector.get_schema_names()
        engine.dispose()
        return schemas
    except Exception:
        return []


def _friendly_db_error(exc: Exception) -> str:
    """Convert a database exception into a user-friendly message with hints."""
    msg = str(exc)
    # Extract the core error from SQLAlchemy wrapper
    if hasattr(exc, "orig") and exc.orig:
        msg = str(exc.orig)

    hints = []
    msg_lower = msg.lower()
    if "no password supplied" in msg_lower or "password authentication failed" in msg_lower:
        hints.append("Check that username and password are correct.")
    elif "does not exist" in msg_lower and "database" in msg_lower:
        hints.append("Verify the database name is correct.")
    elif "could not translate host" in msg_lower or "name or service not known" in msg_lower:
        hints.append("Check that the host address is correct.")
    elif "connection refused" in msg_lower:
        hints.append("Check that the database server is running and the port is correct.")
    elif "timeout" in msg_lower:
        hints.append("The database server is not responding. Check host/port and network access.")

    result = f"Database error: {msg}"
    if hints:
        result += "\nHint: " + " ".join(hints)
    return result


def _fetch_tables(
    ds: DatasourceConfig, schema_name: Optional[str] = None,
) -> Tuple[Optional[List[str]], Optional[str]]:
    """Inspect a datasource's table names.

    Returns ``(tables, None)`` on success or ``(None, friendly_error_message)``
    on failure. ``schema_name=None`` uses the dialect's default schema.
    """
    try:
        conn_str = ds.resolve_env_vars().get_connection_string()
        sa_engine = sa.create_engine(conn_str)
        inspector = sa.inspect(sa_engine)
        tables = inspector.get_table_names(schema=schema_name)
        sa_engine.dispose()
        return sorted(tables), None
    except Exception as e:
        if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
            return None, _friendly_db_error(e)
        return None, str(e)


def _escape_md_cell(value: Any) -> str:
    """Escape a value for inclusion in a markdown table cell.

    Pipes become ``\\|``, carriage returns and newlines collapse to a single
    space, and ``None``/empty renders as an em-dash so empty columns stay
    aligned in the rendered table.
    """
    if value is None:
        return "—"
    s = str(value).replace("|", "\\|").replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip()
    return s if s else "—"


def _md_code_span(value: Any) -> str:
    """Wrap *value* in a CommonMark inline code span, safe for any content.

    The fence is chosen to be one backtick longer than the longest contiguous
    run of backticks inside the value, so embedded backticks never break the
    span.  Per the CommonMark spec, a space is added inside the fence when the
    content starts or ends with a backtick.
    """
    text = str(value).replace("|", "\\|").replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return "` `"
    # Find the longest run of consecutive backticks
    max_run = 0
    run = 0
    for ch in text:
        if ch == "`":
            run += 1
            if run > max_run:
                max_run = run
        else:
            run = 0
    fence = "`" * (max_run + 1)
    # CommonMark: space padding needed when content starts or ends with backtick
    if text.startswith("`") or text.endswith("`"):
        return f"{fence} {text} {fence}"
    return f"{fence}{text}{fence}"


def _cell_is_present(value: Any) -> bool:
    """A cell is 'present' when it carries information: not None, and not an
    empty (or whitespace-only) string. Every other value counts as present."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _markdown_table(rows: List[Dict[str, Any]], columns: List[str]) -> str:
    """Render a list of row dicts as a GitHub-flavored markdown table.

    Columns with no present cell across every row are dropped automatically so
    uninformative all-empty columns don't clutter the output. The degenerate
    cases collapse:

    - ``rows`` is empty, or every column gets pruned → ``"_(none)_"``.
    - Exactly one column survives pruning → a comma-separated, backtick-wrapped
      list of its values, much denser than a one-column table.

    Otherwise a normal markdown table is produced over the surviving columns.
    """
    if not rows:
        return "_(none)_"

    kept = [c for c in columns if any(_cell_is_present(r.get(c)) for r in rows)]
    if not kept:
        return "_(none)_"

    if len(kept) == 1:
        col = kept[0]
        rendered = []
        for r in rows:
            v = r.get(col)
            if not _cell_is_present(v):
                continue
            rendered.append(_md_code_span(v))
        return ", ".join(rendered)

    header = "| " + " | ".join(kept) + " |"
    sep = "| " + " | ".join("---" for _ in kept) + " |"
    body = [
        "| " + " | ".join(_escape_md_cell(r.get(c)) for c in kept) + " |"
        for r in rows
    ]
    return "\n".join([header, sep] + body)


def _build_sample_query_args(model: SlayerModel, num_rows: int) -> Dict[str, Any]:
    """Build the ``SlayerQuery`` payload for ``inspect_model``'s sample data.

    - First field is always ``*:count``.
    - For each non-hidden measure:
      - If ``allowed_aggregations`` is restricted and doesn't include ``avg``,
        use the first entry of ``allowed_aggregations``. Empty list → skip.
      - Else (avg is permitted): prefer ``avg``, but if the measure shares its
        name with a non-numeric dimension (string/boolean/date/time), fall back
        to ``count_distinct`` so the generated SQL is valid for that column
        type. Auto-ingested string measures (e.g. ``sku``) can't be averaged.
    - Groups by up to two non-primary-key, non-hidden dimensions so the sample
      shows variation without exploding table width.
    """
    # dim type lookup by name (for same-named measures created by auto-ingestion)
    dim_types = {d.name: str(d.type) for d in model.dimensions}

    fields: List[Dict[str, str]] = [{"formula": "*:count"}]
    for m in model.measures:
        if m.hidden:
            continue
        allowed = m.allowed_aggregations
        if allowed is not None and "avg" not in allowed:
            if not allowed:
                continue
            safe = next((a for a in allowed if a in _SAFE_SAMPLE_AGGS), None)
            agg = safe if safe else allowed[0]
        else:
            # avg is permitted; drop to count_distinct when the backing column
            # is non-numeric so AVG(VARCHAR) / AVG(BOOL) don't blow up.
            if dim_types.get(m.name) in ("string", "boolean", "date", "time"):
                agg = "count_distinct"
            else:
                agg = "avg"
        fields.append({"formula": f"{m.name}:{agg}"})

    dims: List[Dict[str, str]] = []
    for d in model.dimensions:
        if d.hidden or d.primary_key:
            continue
        dims.append({"name": d.name})
        if len(dims) >= 2:
            break

    return {
        "source_model": model.name,
        "fields": fields,
        "dimensions": dims,
        "limit": num_rows,
    }


def _strip_model_prefix(
    columns: List[str],
    data: List[Dict[str, Any]],
    model_name: str,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Drop the redundant ``{model_name}.`` prefix from sample-data column keys.

    Keeps the markdown table compact (the model name already appears in the
    ``# Model: X`` heading above the sample).
    """
    prefix = f"{model_name}."

    def _strip(key: str) -> str:
        return key[len(prefix):] if key.startswith(prefix) else key

    new_cols = [_strip(c) for c in columns]
    new_data = [{_strip(k): v for k, v in row.items()} for row in data]
    return new_cols, new_data


async def _get_row_count(
    model: SlayerModel, engine: SlayerQueryEngine,
) -> Optional[int]:
    """Return the total row count of ``model``'s underlying table, or ``None``
    on any failure. Uses a bare ``*:count`` query — the same aggregation a user
    would run to ask for the count.

    The result column is read positionally (the query has exactly one field)
    rather than by name, because SLayer's column-naming convention for the
    bare-count-no-dimensions case is ``{model}._count`` rather than the
    with-dimensions ``{model}.count``.
    """
    try:
        q = SlayerQuery.model_validate({
            "source_model": model.name,
            "fields": [{"formula": "*:count"}],
        })
        r = await engine.execute(query=q)
    except Exception:
        return None
    if not r.data or not r.columns:
        return None
    val = r.data[0].get(r.columns[0])
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


class _DimProfileEntry(NamedTuple):
    """One row of dimension-profile output.

    Exactly one of two population modes is used:
    - Categorical (string/boolean): ``distinct_count`` and ``values`` are set.
      When cardinality exceeds the cap, both are ``None`` to signal overflow.
    - Numeric/temporal: ``min_value`` and ``max_value`` are set.
    """
    name: str
    type_str: str
    distinct_count: Optional[int]
    values: Optional[List[Any]]
    min_value: Optional[Any]
    max_value: Optional[Any]


def _format_dim_profile_value(entry: _DimProfileEntry) -> str:
    """Render a profile entry as a single-cell string for the Dimensions table.

    - Enumerated categorical → ```a`, `b`, `c`` (backticked, comma-separated).
    - Overflowed categorical → ``> 20 distinct``.
    - Numeric/temporal range → ``<min> .. <max>``.
    """
    if entry.values is not None:
        return ", ".join(_md_code_span(v) for v in entry.values)
    if (
        entry.distinct_count is None
        and entry.values is None
        and entry.min_value is None
        and entry.max_value is None
    ):
        return "> 20 distinct"
    return f"{entry.min_value} .. {entry.max_value}"


async def _collect_dim_profile(
    model: SlayerModel,
    engine: SlayerQueryEngine,
    *,
    max_values: int = 20,
    max_dims: int = 10,
) -> List[_DimProfileEntry]:
    """Produce one profile entry per eligible dimension (non-hidden, non-pk).

    - string/boolean dims: distinct values (or overflow marker) via one query
      per dim.
    - number/date/time dims: min and max via one batched query across all such
      dims, using a ``ModelExtension`` with transient inline measures.

    Caps the total number of eligible dims at ``max_dims``. Individual failures
    are swallowed — that dim is simply omitted from the result.
    """
    eligible = [
        d for d in model.dimensions
        if not d.hidden and not d.primary_key
    ][:max_dims]
    categorical = [d for d in eligible if str(d.type) in ("string", "boolean")]
    numeric_temporal = [d for d in eligible if str(d.type) in ("number", "date", "time")]

    entries: Dict[str, _DimProfileEntry] = {}

    # --- categorical dims: one query per dim
    for d in categorical:
        try:
            q = SlayerQuery.model_validate({
                "source_model": model.name,
                "dimensions": [{"name": d.name}],
                "fields": [{"formula": "*:count"}],
                "limit": max_values + 1,
            })
            r = await engine.execute(query=q)
        except Exception:
            continue
        value_key = f"{model.name}.{d.name}"
        values = [row.get(value_key) for row in r.data]
        overflow = len(values) > max_values
        entries[d.name] = _DimProfileEntry(
            name=d.name,
            type_str=str(d.type),
            distinct_count=None if overflow else len(values),
            values=None if overflow else values,
            min_value=None,
            max_value=None,
        )

    # --- numeric/temporal dims: ONE batched query for all mins and maxes
    if numeric_temporal:
        ext_measures = [
            {"name": f"_slayer_range_{d.name}", "sql": d.sql if d.sql else d.name}
            for d in numeric_temporal
        ]
        fields: List[Dict[str, str]] = []
        for d in numeric_temporal:
            fields.append({"formula": f"_slayer_range_{d.name}:min"})
            fields.append({"formula": f"_slayer_range_{d.name}:max"})
        row: Dict[str, Any] = {}
        try:
            q = SlayerQuery.model_validate({
                "source_model": {"source_name": model.name, "measures": ext_measures},
                "fields": fields,
            })
            r = await engine.execute(query=q)
            if r.data:
                row = r.data[0]
        except Exception:
            row = {}
        for d in numeric_temporal:
            mn = row.get(f"{model.name}._slayer_range_{d.name}_min")
            mx = row.get(f"{model.name}._slayer_range_{d.name}_max")
            if mn is None and mx is None:
                continue  # query failed or empty table
            entries[d.name] = _DimProfileEntry(
                name=d.name,
                type_str=str(d.type),
                distinct_count=None,
                values=None,
                min_value=mn,
                max_value=mx,
            )

    # Preserve declaration order in the rendered output
    return [entries[d.name] for d in eligible if d.name in entries]


async def _collect_reachable_fields(
    model: SlayerModel,
    storage: StorageBackend,
    *,
    max_depth: int = 5,
) -> Tuple[List[str], List[str]]:
    """BFS the join graph from ``model``; return sorted fully-qualified dotted
    paths for every reachable non-hidden, non-pk dimension and non-hidden
    measure (excluding the root model's own fields — those live in the main
    Dimensions/Measures tables). Depth is measured in path segments and capped
    at ``max_depth``. Cycles are broken by a visited-path set.
    """
    reachable_dims: set[str] = set()
    reachable_measures: set[str] = set()
    visited: set[str] = set()
    queue: List[Tuple[str, str]] = []  # (full_path, target_model_name)

    def _derive_path(base: str, join: ModelJoin) -> str:
        if base:
            return f"{base}.{join.target_model}"
        return join.target_model

    for j in model.joins:
        path = _derive_path("", j)
        if path not in visited:
            queue.append((path, j.target_model))

    while queue:
        path, target_name = queue.pop(0)
        if path in visited:
            continue
        visited.add(path)
        if path.count(".") + 1 > max_depth:
            continue
        target = await storage.get_model(target_name)
        if target is None:
            continue
        for d in target.dimensions:
            if not d.hidden and not d.primary_key:
                reachable_dims.add(f"{path}.{d.name}")
        for m in target.measures:
            if not m.hidden:
                reachable_measures.add(f"{path}.{m.name}")
        for j in target.joins:
            sub_path = _derive_path(path, j)
            # Per-path cycle check: don't revisit any model already on this
            # path (prevents bounce-backs from peer joins while preserving
            # diamond joins where the same model is reached via independent paths).
            path_models = set(path.split("."))
            path_models.add(model.name)  # include root
            if sub_path not in visited and j.target_model not in path_models:
                queue.append((sub_path, j.target_model))

    return sorted(reachable_dims), sorted(reachable_measures)


def _model_to_summary(model: SlayerModel) -> dict:
    """Convert a SlayerModel to a summary dict."""
    dims = []
    for d in model.dimensions:
        if d.hidden:
            continue
        entry = {"name": d.name, "type": str(d.type)}
        if d.label:
            entry["label"] = d.label
        if d.description:
            entry["description"] = d.description
        dims.append(entry)

    measures = []
    for m in model.measures:
        if m.hidden:
            continue
        entry: dict = {"name": m.name}
        if m.label:
            entry["label"] = m.label
        if m.description:
            entry["description"] = m.description
        if m.filter:
            entry["filter"] = m.filter
        measures.append(entry)

    return {
        "name": model.name,
        "description": model.description,
        "dimensions": dims,
        "measures": measures,
    }


def create_mcp_server(storage: StorageBackend):
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
            "Typical workflow: list_datasources → models_summary → inspect_model → query. "
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
    async def help(topic: Optional[str] = None) -> str:  # noqa: A001 — intentional shadow of builtin inside factory
        return render_help(topic=topic)

    @mcp.tool()
    async def query(
        source_model: str,
        fields: Optional[List[Dict[str, str]]] = None,
        dimensions: Optional[List[str]] = None,
        filters: Optional[List[str]] = None,
        time_dimensions: Optional[List[Dict[str, Any]]] = None,
        order: Optional[List[Dict[str, str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        whole_periods_only: bool = False,
        show_sql: bool = False,
        dry_run: bool = False,
        explain: bool = False,
        format: str = "markdown",
    ) -> str:
        """Query data from a semantic model. Call inspect_model first to see available fields and dimensions.

        Args:
            source_model: Name of the model to query (from models_summary).
            fields: Data columns to return. Each is a formula: {"formula": "count"} (measure),
                {"formula": "revenue / count", "name": "aov"} (arithmetic),
                {"formula": "cumsum(revenue)"} (cumulative sum), {"formula": "change(revenue)"} (diff from previous row),
                {"formula": "change_pct(revenue)"} (% change), {"formula": "time_shift(revenue, -1)"} (previous period via self-join),
                {"formula": "time_shift(revenue, -1, 'year')"} (year-over-year), {"formula": "lag(revenue, 1)"} (previous row via window function),
                {"formula": "lead(revenue, 1)"} (next row via window function), {"formula": "last(revenue)"} (most recent),
                {"formula": "rank(revenue)"} (ranking).
            dimensions: List of dimension names to group by, e.g. ["status", "region"].
            filters: Filter conditions as formula strings. Examples: "status == 'completed'",
                "amount > 100", "status in ('a', 'b')", "status is None",
                "name like '%acme%'". Filters on measures are automatically routed to HAVING.
                Supports and/or: "status == 'a' or status == 'b'".
                Filters can also reference computed field names or contain inline transforms:
                "change(revenue) > 0", "last(change(revenue)) < 0".
            time_dimensions: Time grouping. Format: {"dimension": "created_at", "granularity": "day|week|month|quarter|year", "date_range": ["2024-01-01", "2024-12-31"]}.
            order: Sorting. Format: {"column": "field_name", "direction": "asc|desc"}.
            limit: Max rows to return.
            offset: Number of rows to skip.
            whole_periods_only: When true, snap date filters to time bucket boundaries based on granularity, exclude the current incomplete time bucket.
            show_sql: When true, include the generated SQL in the response for debugging.
            dry_run: When true, generate and return the SQL without executing it.
            explain: When true, run EXPLAIN ANALYZE and return the query plan.
            format: Output format — "markdown" (default, compact and LLM-friendly), "json" (structured), or "csv" (most compact). Case-insensitive.

        Example: query(source_model="orders", fields=[{"formula": "count"}], dimensions=["status"], filters=["status == 'completed'"])
        """
        data: Dict[str, Any] = {"source_model": source_model}
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
        if dry_run:
            data["dry_run"] = True
        if explain:
            data["explain"] = True
        if fields:
            data["fields"] = fields
        try:
            fmt = format.lower().strip()
            if fmt not in ("json", "csv", "markdown"):
                raise ValueError(f"Invalid format '{format}'. Must be one of: json, csv, markdown")
            slayer_query = SlayerQuery.model_validate(data)
            result = await engine.execute(query=slayer_query)
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
    async def models_summary(datasource_name: str, format: str = "markdown") -> str:
        """Brief summary of all (non-hidden) models in a datasource.

        For each model: name, description, a table of its dimensions
        (just name + description), a table of its measures (just name
        + description), and a comma-separated list of the model names it joins
        to. No field types, no distinct values, no sample data, and no
        expansion of joined models' fields — call inspect_model for any of
        that.

        Args:
            datasource_name: Name of the datasource (from list_datasources).
            format: Output format — "markdown" (default, compact and
                LLM-friendly) or "json" (structured array of model summaries).
                Case-insensitive.
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

        all_names = await storage.list_models()
        matched: List[SlayerModel] = []
        for n in all_names:
            try:
                m = await storage.get_model(n)
            except Exception:
                logger.warning("Failed to load model '%s', skipping", n, exc_info=True)
                continue
            if m is not None and not m.hidden and m.data_source == datasource_name:
                matched.append(m)
        matched.sort(key=lambda m: m.name)

        if not matched:
            return f"Datasource '{datasource_name}' has no models."

        if fmt == "json":
            return json.dumps(
                {
                    "datasource_name": datasource_name,
                    "model_count": len(matched),
                    "models": [
                        {
                            "name": m.name,
                            "description": m.description,
                            "dimensions": [
                                {"name": d.name, "description": d.description}
                                for d in m.dimensions if not d.hidden
                            ],
                            "measures": [
                                {"name": meas.name, "description": meas.description}
                                for meas in m.measures if not meas.hidden
                            ],
                            "joins_to": sorted({j.target_model for j in m.joins}),
                        }
                        for m in matched
                    ],
                },
                indent=2,
            )

        sections: List[str] = [
            f"# Datasource: `{datasource_name}` — {len(matched)} model(s)"
        ]
        for m in matched:
            model_lines: List[str] = [f"## `{m.name}`"]
            if m.description:
                model_lines.append(m.description)

            dim_rows = [
                {"name": d.name, "description": d.description}
                for d in m.dimensions if not d.hidden
            ]
            model_lines.append(f"**Dimensions ({len(dim_rows)}):**")
            model_lines.append("")
            model_lines.append(
                _markdown_table(rows=dim_rows, columns=["name", "description"])
            )
            model_lines.append("")

            measure_rows = [
                {"name": meas.name, "description": meas.description}
                for meas in m.measures if not meas.hidden
            ]
            model_lines.append(f"**Measures ({len(measure_rows)}):**")
            model_lines.append("")
            model_lines.append(
                _markdown_table(rows=measure_rows, columns=["name", "description"])
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
    ) -> str:
        """Return a complete-yet-compact view of a semantic model.

        Sections always emitted (when non-empty): model header + description,
        metadata bullets (data_source, sql_table, default_time_dimension,
        hidden, row_count), custom SQL block, model-level filters, dimensions
        table (includes a ``sampled`` column — distinct values for
        string/boolean dims, ``min .. max`` for number/date/time dims, or
        ``> 20 distinct`` for high-cardinality categoricals), measures table,
        custom aggregations, joins, reachable fields via joins up to depth 5,
        and a sample-data table with ``COUNT(*)`` plus one aggregation per
        measure (``avg`` when permitted, else the first allowed aggregation).

        Every markdown table in the response auto-prunes columns whose cells
        are entirely empty, and collapses to a comma-separated backticked list
        when only one column survives pruning.

        Args:
            model_name: Name of the model to inspect.
            num_rows: Max sample-data rows (default: 3).
            show_sql: When true, include the generated SQL for the sample-data
                query in the response (useful for debugging or understanding
                how SLayer translates the model to SQL).
            format: Output format — "markdown" (default, rich structured
                document), or "json" (structured JSON with all model metadata
                and sample data). Case-insensitive.
        """
        fmt = format.lower().strip()
        if fmt not in ("markdown", "json"):
            raise ValueError(
                f"Invalid format '{format}' for inspect_model. Must be 'markdown' or 'json'."
            )

        model = await storage.get_model(model_name)
        if model is None:
            all_names = await storage.list_models()
            available = []
            for n in all_names:
                m = await storage.get_model(n)
                if m is not None and not m.hidden:
                    available.append(n)
            available.sort()
            return f"Model '{model_name}' not found. Available models: {', '.join(available)}"

        sections: List[str] = [f"# Model: `{model.name}`"]
        if model.description:
            sections.append(model.description)

        # Metadata bullets (incl. row_count from a cheap *:count query)
        meta: List[str] = []
        if model.data_source:
            meta.append(f"- **data_source:** `{model.data_source}`")
        if model.sql_table:
            meta.append(f"- **sql_table:** `{model.sql_table}`")
        if model.default_time_dimension:
            meta.append(
                f"- **default_time_dimension:** `{model.default_time_dimension}`"
            )
        if model.hidden:
            meta.append("- **hidden:** true")
        row_count = await _get_row_count(model=model, engine=engine)
        if row_count is not None:
            meta.append(f"- **row_count:** {row_count:,}")
        if meta:
            sections.append("\n".join(meta))

        if show_sql and model.sql:
            sections.append(f"## SQL\n\n```sql\n{model.sql}\n```")

        if show_sql and model.filters:
            filter_lines = "\n".join(f"- `{f}`" for f in model.filters)
            sections.append(f"## Filters (model-level)\n\n{filter_lines}")

        # Dimension profile (distinct values / min-max) — folded into the
        # Dimensions table as a ``sampled`` column so there's one table per
        # dimension rather than a separate section the LLM has to cross-join by
        # name.
        profile_entries = await _collect_dim_profile(model=model, engine=engine)
        profile_by_name: Dict[str, str] = {
            e.name: _format_dim_profile_value(e) for e in profile_entries
        }

        # Dimensions table
        dim_rows: List[Dict[str, Any]] = []
        for d in model.dimensions:
            if d.hidden:
                continue
            dim_rows.append({
                "name": d.name,
                "type": str(d.type),
                "primary_key": "yes" if d.primary_key else "",
                "sql": d.sql if d.sql else d.name,
                "label": d.label,
                "description": d.description,
                "sampled": profile_by_name.get(d.name),
            })
        dim_columns = ["name", "type", "primary_key", "sql", "label", "description", "sampled"]
        if not show_sql:
            dim_columns.remove("sql")
        sections.append(
            f"## Dimensions ({len(dim_rows)})\n\n"
            + _markdown_table(rows=dim_rows, columns=dim_columns)
        )

        # Measures table
        measure_rows: List[Dict[str, Any]] = []
        for m in model.measures:
            if m.hidden:
                continue
            aggs = ", ".join(m.allowed_aggregations) if m.allowed_aggregations else "all"
            measure_rows.append({
                "name": m.name,
                "sql": m.sql if m.sql else m.name,
                "allowed_aggregations": aggs,
                "filter": m.filter,
                "label": m.label,
                "description": m.description,
            })
        meas_columns = ["name", "sql", "allowed_aggregations", "filter", "label", "description"]
        if not show_sql:
            meas_columns = [c for c in meas_columns if c not in ("sql", "filter")]
        sections.append(
            f"## Measures ({len(measure_rows)})\n\n"
            + _markdown_table(rows=measure_rows, columns=meas_columns)
        )

        # Custom aggregations (if any)
        if model.aggregations:
            agg_rows: List[Dict[str, Any]] = []
            for a in model.aggregations:
                params = (
                    "; ".join(f"{p.name}={p.sql}" for p in a.params) if a.params else None
                )
                agg_rows.append({
                    "name": a.name,
                    "formula": a.formula or "(built-in override)",
                    "params": params,
                    "description": a.description,
                })
            sections.append(
                f"## Aggregations ({len(agg_rows)})\n\n"
                + _markdown_table(
                    rows=agg_rows,
                    columns=["name", "formula", "params", "description"],
                )
            )

        # Joins table (always rendered, even when empty, to keep structure predictable)
        join_rows: List[Dict[str, Any]] = []
        for j in model.joins:
            pairs = "; ".join(f"{src} = {tgt}" for src, tgt in j.join_pairs)
            join_rows.append({
                "target_model": j.target_model,
                "join_pairs": pairs,
            })
        sections.append(
            f"## Joins ({len(join_rows)})\n\n"
            + _markdown_table(
                rows=join_rows,
                columns=["target_model", "join_pairs"],
            )
        )

        # Reachable via joins (omitted when empty)
        reach_dims, reach_measures = await _collect_reachable_fields(
            model=model, storage=storage,
        )
        if reach_dims or reach_measures:
            lines = ["## Reachable via joins (max depth: 5)", ""]
            if reach_dims:
                rendered = ", ".join(f"`{d}`" for d in reach_dims)
                lines.append(f"**Dimensions ({len(reach_dims)}):** {rendered}")
            if reach_measures:
                rendered = ", ".join(f"`{m}`" for m in reach_measures)
                lines.append(f"**Measures ({len(reach_measures)}):** {rendered}")
            sections.append("\n".join(lines))

        # Sample data via a regular SlayerQuery (same path the `query` MCP tool takes)
        query_args = _build_sample_query_args(model=model, num_rows=num_rows)
        sample_sql: Optional[str] = None
        sample_data: Optional[Dict[str, Any]] = None
        sample_error: Optional[str] = None
        try:
            sample_query = SlayerQuery.model_validate(query_args)
            sample_result = await engine.execute(query=sample_query)
            sample_sql = sample_result.sql
            cols, data = _strip_model_prefix(
                columns=sample_result.columns,
                data=sample_result.data,
                model_name=model.name,
            )
            sample_data = {"columns": cols, "rows": data}
            sample_result.columns = cols
            sample_result.data = data
            sample_section = f"## Sample Data\n\n{sample_result.to_markdown()}"
            if show_sql and sample_sql:
                sample_section = (
                    f"## Sample Data SQL\n\n```sql\n{sample_sql}\n```\n\n"
                    + sample_section
                )
            sections.append(sample_section)
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                err = _friendly_db_error(e)
            else:
                err = str(e)
            sample_error = err
            sample_section = f"## Sample Data\n\n_Error fetching sample data: {err}_"
            if show_sql and sample_sql:
                sample_section = (
                    f"## Sample Data SQL\n\n```sql\n{sample_sql}\n```\n\n"
                    + sample_section
                )
            sections.append(sample_section)

        if fmt == "json":
            # Return a structured JSON representation instead of the markdown document
            return json.dumps(
                {
                    "model_name": model.name,
                    "description": model.description,
                    "data_source": model.data_source,
                    **({"sql_table": model.sql_table} if show_sql else {}),
                    **({"sql": model.sql} if show_sql else {}),
                    "default_time_dimension": model.default_time_dimension,
                    "hidden": model.hidden,
                    "row_count": row_count,
                    **({"filters": model.filters} if show_sql else {}),
                    "dimensions": [
                        {
                            "name": d.name,
                            "type": str(d.type),
                            "primary_key": d.primary_key,
                            **({"sql": d.sql} if show_sql else {}),
                            "label": d.label,
                            "description": d.description,
                            "sampled": profile_by_name.get(d.name),
                        }
                        for d in model.dimensions if not d.hidden
                    ],
                    "measures": [
                        {
                            "name": m.name,
                            **({"sql": m.sql} if show_sql else {}),
                            "allowed_aggregations": m.allowed_aggregations,
                            **({"filter": m.filter} if show_sql else {}),
                            "label": m.label,
                            "description": m.description,
                        }
                        for m in model.measures if not m.hidden
                    ],
                    "aggregations": [
                        {
                            "name": a.name,
                            "formula": a.formula,
                            "params": [
                                {"name": p.name, "sql": p.sql} for p in (a.params or [])
                            ],
                            "description": a.description,
                        }
                        for a in model.aggregations
                    ],
                    "joins": [
                        {
                            "target_model": j.target_model,
                            "join_pairs": j.join_pairs,
                        }
                        for j in model.joins
                    ],
                    "reachable_dimensions": reach_dims,
                    "reachable_measures": reach_measures,
                    "sample_data": sample_data,
                    "sample_data_error": sample_error,
                    **({"sample_sql": sample_sql} if show_sql and sample_sql else {}),
                },
                indent=2,
                default=str,
            )

        return "\n\n".join(sections)

    # -----------------------------------------------------------------------
    # Model creation and editing
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def create_model(
        name: str,
        sql_table: Optional[str] = None,
        sql: Optional[str] = None,
        data_source: Optional[str] = None,
        description: Optional[str] = None,
        dimensions: Optional[List[Dict[str, str]]] = None,
        measures: Optional[List[Dict[str, Union[str, List[str]]]]] = None,
        query: Optional[Dict] = None,
    ) -> str:
        """Create a new semantic model, either from a database table or from a query.

        **From a table** (provide sql_table or sql):
            create_model(name="orders", sql_table="public.orders", data_source="mydb",
                         dimensions=[...], measures=[...])

        **From a query** (provide query):
            create_model(name="monthly_summary", query={"source_model": "orders",
                         "fields": ["*:count", "amount:sum"],
                         "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]})
            Dimensions and measures are auto-introspected from the query result.

        Args:
            name: Unique model name (lowercase, underscores).
            sql_table: Database table name, e.g. "public.orders".
            sql: Alternative to sql_table — a custom SQL expression for the model's source.
            data_source: Name of the datasource (from list_datasources).
            description: What this model represents.
            dimensions: List of dimension definitions. Each: {"name": "col", "sql": "col", "type": "string"}.
                Types: string, number, time, date, boolean.
            measures: List of measure definitions. Each: {"name": "total", "sql": "amount"}.
                Optional: "allowed_aggregations": ["sum", "avg"] to restrict usable aggregations.
            query: A SLayer query dict. When provided, the query's SQL becomes the model source
                and dimensions/measures are auto-introspected. Mutually exclusive with
                sql_table, sql, dimensions, and measures.
        """
        if query is not None:
            table_params = {
                k: v for k, v in {
                    "sql_table": sql_table, "sql": sql, "data_source": data_source,
                    "dimensions": dimensions, "measures": measures,
                }.items()
                if v
            }
            if table_params:
                return (
                    f"Error: 'query' cannot be combined with {', '.join(table_params.keys())}. "
                    "Use 'query' alone to create from a query, or provide table details without 'query'."
                )
            try:
                parsed_query = SlayerQuery.model_validate(query)
                model = await engine.create_model_from_query(
                    query=parsed_query, name=name, description=description,
                )
            except Exception as e:
                if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                    return _friendly_db_error(e)
                return f"Error creating model from query: {e}"
            dims = [d.name for d in model.dimensions]
            meas = [m.name for m in model.measures]
            return (
                f"Model '{name}' created from query. "
                f"Dimensions: {dims}. Measures: {meas}."
            )

        data = _build_dict(
            name=name,
            sql_table=sql_table,
            sql=sql,
            data_source=data_source,
            description=description,
            dimensions=dimensions,
            measures=measures,
        )
        model = SlayerModel.model_validate(data)
        existed = await storage.get_model(name) is not None
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
    ) -> Optional[str]:
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

    VALID_REMOVE_KEYS = {"dimensions", "measures", "aggregations", "joins"}

    @mcp.tool()
    async def edit_model(
        model_name: str,
        description: Optional[str] = None,
        data_source: Optional[str] = None,
        default_time_dimension: Optional[str] = None,
        sql_table: Optional[str] = None,
        sql: Optional[str] = None,
        hidden: Optional[bool] = None,
        dimensions: Optional[List[Dict[str, Any]]] = None,
        measures: Optional[List[Dict[str, Any]]] = None,
        aggregations: Optional[List[Dict[str, Any]]] = None,
        joins: Optional[List[Dict[str, Any]]] = None,
        add_filters: Optional[List[str]] = None,
        remove_filters: Optional[List[str]] = None,
        remove: Optional[Dict[str, List[str]]] = None,
        meta: Optional[Dict[str, Any]] = _UNSET,
    ) -> str:
        """Edit an existing model in a single call — update metadata, upsert dimensions/measures/aggregations/joins,
        manage filters, and remove entities.

        Args:
            model_name: Name of the model to edit.
            description: New model description.
            data_source: New data source name.
            default_time_dimension: Default time dimension for time-dependent transforms.
            sql_table: Database table name.
            sql: Custom SQL expression for the model source.
            hidden: Whether this model is hidden from discovery.
            meta: Arbitrary JSON metadata for the model (replaces existing meta). Pass null/None to clear.
            dimensions: Dimensions to create or update (upsert by name). Each dict: {"name": "col", "type": "string", "sql": "col", "description": "...", "primary_key": false, "hidden": false}.
                If a dimension with this name exists, only the provided fields are updated; omitted fields keep current values.
                Types: string, number, time, date, boolean.
            measures: Measures to create or update (upsert by name). Each dict: {"name": "total", "sql": "amount", "description": "...", "hidden": false, "allowed_aggregations": ["sum", "avg"]}.
                If a measure with this name exists, only the provided fields are updated.
            aggregations: Aggregations to create or update (upsert by name). Each dict: {"name": "weighted_avg", "formula": "SUM({value} * {weight}) / NULLIF(SUM({weight}), 0)", "params": [{"name": "weight", "sql": "quantity"}], "description": "..."}.
                If an aggregation with this name exists, only the provided fields are updated.
            joins: Joins to create or update (upsert by target_model). Each dict: {"target_model": "customers", "join_pairs": [["customer_id", "id"]]}.
                If a join to this target_model exists, its join_pairs are updated.
            add_filters: SQL filter strings to add (e.g. ["deleted_at IS NULL"]). Duplicates are ignored.
            remove_filters: SQL filter strings to remove (exact match).
            remove: Named entities to delete, keyed by type: {"dimensions": ["name1"], "measures": ["name2"], "aggregations": ["name3"], "joins": ["target_model_name"]}.
                Removals are processed before upserts, so you can remove and re-add in one call.

        Example — update a dimension and add a measure:
            edit_model(model_name="orders", dimensions=[{"name": "status", "type": "string"}], measures=[{"name": "profit", "sql": "revenue - cost"}])
        Example — remove a measure:
            edit_model(model_name="orders", remove={"measures": ["old_metric"]})
        """
        model = await storage.get_model(model_name)
        if model is None:
            return f"Model '{model_name}' not found."

        changes: List[str] = []

        # --- Phase 1: Scalar metadata ---
        if description is not None:
            model.description = description
            changes.append("updated description")
        if data_source is not None:
            model.data_source = data_source
            changes.append(f"set data_source to '{data_source}'")
        if default_time_dimension is not None:
            model.default_time_dimension = default_time_dimension
            changes.append(f"set default_time_dimension to '{default_time_dimension}'")
        if sql_table is not None and sql is not None:
            return "Specify only one of 'sql_table' or 'sql' when editing a model."

        if sql_table is not None:
            model.sql_table = sql_table
            model.sql = None
            changes.append(f"set sql_table to '{sql_table}'")
        if sql is not None:
            model.sql = sql
            model.sql_table = None
            changes.append(f"set sql to '{sql}'")
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

            for name in remove.get("dimensions", []):
                match = next((d for d in model.dimensions if d.name == name), None)
                if match is None:
                    return f"Dimension '{name}' not found on model '{model_name}'."
                model.dimensions.remove(match)
                changes.append(f"removed dimension '{name}'")

            for name in remove.get("measures", []):
                match = next((m for m in model.measures if m.name == name), None)
                if match is None:
                    return f"Measure '{name}' not found on model '{model_name}'."
                model.measures.remove(match)
                changes.append(f"removed measure '{name}'")

            for name in remove.get("aggregations", []):
                match = next((a for a in model.aggregations if a.name == name), None)
                if match is None:
                    return f"Aggregation '{name}' not found on model '{model_name}'."
                model.aggregations.remove(match)
                changes.append(f"removed aggregation '{name}'")

            for target in remove.get("joins", []):
                match = next((j for j in model.joins if j.target_model == target), None)
                if match is None:
                    return f"Join to '{target}' not found on model '{model_name}'."
                model.joins.remove(match)
                changes.append(f"removed join to '{target}'")

        # --- Phase 3: Entity upserts ---
        for spec in dimensions or []:
            err = _upsert_entity(
                entity_list=model.dimensions, spec=spec, entity_cls=Dimension,
                id_field="name", changes=changes, label="dimension",
            )
            if err:
                return err

        for spec in measures or []:
            err = _upsert_entity(
                entity_list=model.measures, spec=spec, entity_cls=Measure,
                id_field="name", changes=changes, label="measure",
            )
            if err:
                return err

        for spec in aggregations or []:
            err = _upsert_entity(
                entity_list=model.aggregations, spec=spec, entity_cls=Aggregation,
                id_field="name", changes=changes, label="aggregation",
            )
            if err:
                return err

        for spec in joins or []:
            err = _upsert_entity(
                entity_list=model.joins, spec=spec, entity_cls=ModelJoin,
                id_field="target_model", changes=changes, label="join",
            )
            if err:
                return err

        # --- Phase 4: Filters ---
        if add_filters:
            existing_filters = set(model.filters)
            for f in add_filters:
                if f not in existing_filters:
                    model.filters.append(f)
                    existing_filters.add(f)
                    changes.append(f"added filter '{f}'")

        if remove_filters:
            for f in remove_filters:
                if f not in model.filters:
                    return f"Filter not found on model '{model_name}': {f}"
                model.filters.remove(f)
                changes.append(f"removed filter '{f}'")

        if not changes:
            return f"No changes specified for model '{model_name}'."

        # --- Phase 5: Validate and save ---
        try:
            validated = SlayerModel.model_validate(model.model_dump(mode="json"))
        except Exception as exc:
            return f"Validation error: {exc}"

        await storage.save_model(validated)
        return json.dumps({
            "success": True,
            "model_name": model_name,
            "changes": changes,
            "message": f"Applied {len(changes)} change(s) to '{model_name}'",
        }, indent=2)

    # -----------------------------------------------------------------------
    # Datasource management
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def create_datasource(
        name: str,
        type: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection_string: Optional[str] = None,
        schema_name: Optional[str] = None,
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
                lines.append(f"- {m.name} ({len(m.dimensions)} dims, {len(m.measures)} measures)")
            lines.append("")
            lines.append("Use models_summary and inspect_model to explore, then query to fetch data.")

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
        description: Optional[str] = None,
    ) -> str:
        """Update a datasource's metadata.

        Args:
            name: Datasource name to update.
            description: New description for the datasource.
        """
        ds = await storage.get_datasource(name)
        if ds is None:
            return f"Datasource '{name}' not found."

        if description is not None:
            ds.description = description

        await storage.save_datasource(ds)
        return f"Datasource '{name}' updated."

    # -----------------------------------------------------------------------
    # Delete operations
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def delete_model(name: str) -> str:
        """Delete a semantic model.

        Args:
            name: Model name to delete.
        """
        if await storage.delete_model(name):
            return f"Model '{name}' deleted."
        return f"Model '{name}' not found."

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
        """Auto-discover tables in a database and create semantic models from them. Inspects the schema and generates one model per table with dimensions and measures inferred from column types.

        Args:
            datasource_name: Name of an existing datasource (from list_datasources).
            include_tables: Comma-separated list of table names to include. If empty, all tables are ingested.
            schema_name: Database schema to inspect (e.g. "public"). If empty, uses the default schema.
        """
        from slayer.engine.ingestion import ingest_datasource as _ingest

        ds = await storage.get_datasource(datasource_name)
        if ds is None:
            return f"Datasource '{datasource_name}' not found."

        try:
            include = [t.strip() for t in include_tables.split(",") if t.strip()] or None
            models = _ingest(datasource=ds, include_tables=include, schema=schema_name or None)
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                return _friendly_db_error(e)
            raise

        for model in models:
            await storage.save_model(model)

        if not models:
            schema_label = f" in schema '{schema_name}'" if schema_name else ""
            lines = [f"No tables found{schema_label}."]
            schemas = _get_schemas(ds)
            if schemas:
                lines.append(f"Available schemas: {', '.join(schemas)}")
                lines.append("Try: ingest_datasource_models with schema_name set to one of these.")
            return "\n".join(lines)

        lines = [f"Ingested {len(models)} model(s):"]
        for m in models:
            lines.append(f"- {m.name} ({len(m.dimensions)} dims, {len(m.measures)} measures)")
        lines.append("")
        lines.append("Use models_summary and inspect_model to explore, then query to fetch data.")
        return "\n".join(lines)

    return mcp


def _build_dict(**kwargs: Any) -> Dict[str, Any]:
    """Build a dict from keyword arguments, excluding None values."""
    return {k: v for k, v in kwargs.items() if v is not None}


def _format_table(data: List[Dict[str, Any]], columns: List[str], max_rows: int = 50) -> str:
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


def _format_json(data: List[Dict[str, Any]], columns: List[str]) -> str:
    """Format data as JSON array."""
    import json

    return json.dumps(data, default=str)


def _format_csv(data: List[Dict[str, Any]], columns: List[str]) -> str:
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


def _format_field_meta(entries: Dict[str, Any]) -> List[str]:
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