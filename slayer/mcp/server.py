"""MCP server for SLayer."""

import json
import logging
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import sqlalchemy as sa

from slayer.core.errors import (
    AmbiguousModelError,
    EntityResolutionError,
    MemoryNotFoundError,
)
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.help import TOPIC_SUMMARY_LINE, render_help
from slayer.memories.service import MemoryService
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)

VALID_DIMENSION_TYPES = {"string", "time", "date", "boolean", "number"}
_UNSET = object()  # Sentinel to distinguish "not provided" from "explicitly set to None"

# Aggregations that are safe for sample-data extraction: zero extra args,
# no time-column context needed.
_SAFE_SAMPLE_AGGS = frozenset({"avg", "sum", "min", "max", "count", "count_distinct", "median"})

# Section-level budgeting for inspect_model output.
# columns/measures/aggregations/joins fall back to a names-only CSV when the
# caller drops the section from `sections`; reachable_fields/samples are fully
# omitted (they have no natural "names" to list).
_INSPECT_SECTIONS_NAMES_ONLY = ("columns", "measures", "aggregations", "joins")
_INSPECT_SECTIONS_OMITTABLE = ("reachable_fields", "samples", "learnings")
_VALID_INSPECT_SECTIONS = _INSPECT_SECTIONS_NAMES_ONLY + _INSPECT_SECTIONS_OMITTABLE
_TRUNCATION_MARKER = " ... [truncated]"
_MAX_REACHABLE_FIELDS_DEPTH = 20


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


def _truncate_description(text: Optional[str], max_chars: Optional[int]) -> Optional[str]:
    """Trim a description to ``max_chars`` and append the truncation marker.

    Returns the input unchanged when ``max_chars`` is ``None`` or the text is
    already short enough. ``max_chars=0`` is allowed and yields just the
    marker for any non-empty input.
    """
    if text is None or max_chars is None:
        return text
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + _TRUNCATION_MARKER


def _format_meta(meta: Optional[Dict[str, Any]]) -> Optional[str]:
    """Compact JSON for the ``inspect_model`` meta cell.

    Returns ``None`` when ``meta`` is ``None`` so ``_markdown_table``'s
    all-empty-column pruner hides the meta column when no row has meta set.
    """
    if meta is None:
        return None
    return json.dumps(meta, sort_keys=True, default=str)


def _resolve_inspect_sections(
    sections: Optional[List[str]],
) -> Tuple[List[str], List[str]]:
    """Validate and normalise the ``sections`` argument for ``inspect_model``.

    Returns ``(resolved, unknown)`` where ``resolved`` is the list of valid
    section names to render (preserving the canonical order, not the caller's
    order) and ``unknown`` is the unrecognised entries (in caller order) for
    the warning line.

    ``sections=None`` and ``sections=[]`` both resolve to all six valid
    sections — that's the documented "I want everything" path.

    A non-empty list of *only* unknown names resolves to ``[]`` (not all six):
    "all sections" is reserved for the explicit None/[] forms so a typo like
    ``sections=["sample"]`` can't silently trigger the full expensive payload.
    The footer warns about the unknown names and lists what was dropped, so
    the caller can correct and re-call.
    """
    if not sections:
        return list(_VALID_INSPECT_SECTIONS), []
    valid_set = {s for s in sections if s in _VALID_INSPECT_SECTIONS}
    unknown = [s for s in sections if s not in _VALID_INSPECT_SECTIONS]
    # Canonical order so output is stable regardless of caller's order
    resolved = [s for s in _VALID_INSPECT_SECTIONS if s in valid_set]
    return resolved, unknown


def _render_inspect_footer(
    *,
    included: List[str],
    names_only: List[str],
    omitted: List[str],
    unknown: List[str],
) -> Optional[str]:
    """Build the per-call truncation footer for ``inspect_model``.

    Returns ``None`` when there is nothing to report (no trimming, no
    unknown names). Otherwise returns a quoted-markdown block.
    """
    if not (names_only or omitted or unknown):
        return None
    lines: List[str] = []
    if unknown:
        # repr() escapes newlines / quote chars so a caller-supplied value
        # like "foo\n> evil" can't forge additional footer lines.
        quoted = ", ".join(repr(u) for u in unknown)
        lines.append(
            f"> Warning: ignored unknown sections: {quoted}. "
            f"Valid: {', '.join(_VALID_INSPECT_SECTIONS)}."
        )
    if names_only or omitted:
        lines.append(f"> Sections shown: {', '.join(included) if included else '(none)'}.")
        if names_only:
            lines.append(f"> Names-only: {', '.join(names_only)}.")
        if omitted:
            lines.append(f"> Omitted: {', '.join(omitted)}.")
        lines.append("> Re-call inspect_model with `sections=[...]` to fetch.")
    return "\n".join(lines) if lines else None


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


def _build_sample_query_args(
    model: SlayerModel,
    num_rows: int,
    measure_types: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Build the ``SlayerQuery`` payload for ``inspect_model``'s sample data.

    - First entry is always ``*:count``.
    - For each non-hidden, non-primary-key column:
      - If ``allowed_aggregations`` is restricted and doesn't include ``avg``,
        use the first safe entry (or skip if empty).
      - Else (avg is permitted): prefer ``avg``, but fall back to
        ``count_distinct`` for non-numeric columns (inferred from
        ``measure_types`` or the column's own ``type``).
    - Groups by up to two non-primary-key, non-hidden columns of non-numeric
      type so the sample shows variation without exploding table width.
    """
    measure_types = measure_types or {}

    # Pick up to two categorical columns to group by first, so we don't also
    # aggregate them as measures (count_distinct(status) grouped by status is
    # always 1, which isn't useful sample data).
    dims: List[Dict[str, str]] = []
    dim_names: set[str] = set()
    for c in model.columns:
        if c.hidden or c.primary_key:
            continue
        if str(c.type) not in ("string", "boolean"):
            continue
        dims.append({"name": c.name})
        dim_names.add(c.name)
        if len(dims) >= 2:
            break

    measures: List[Dict[str, str]] = [{"formula": "*:count"}]
    for c in model.columns:
        if c.hidden or c.primary_key or c.name in dim_names:
            continue
        allowed = c.allowed_aggregations
        if allowed is not None and "avg" not in allowed:
            if not allowed:
                continue
            safe = next((a for a in allowed if a in _SAFE_SAMPLE_AGGS), None)
            agg = safe if safe else allowed[0]
        else:
            inferred = measure_types.get(c.name)
            if inferred and inferred != "number":
                agg = "count_distinct"
            elif str(c.type) in ("string", "boolean", "date", "time"):
                agg = "count_distinct"
            else:
                agg = "avg"
        measures.append({"formula": f"{c.name}:{agg}"})

    return {
        "source_model": model.name,
        "measures": measures,
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
            "measures": [{"formula": "*:count"}],
        })
        r = await engine.execute(query=q, data_source=model.data_source or None)
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
        c for c in model.columns
        if not c.hidden and not c.primary_key
    ][:max_dims]
    categorical = [c for c in eligible if str(c.type) in ("string", "boolean")]
    numeric_temporal = [c for c in eligible if str(c.type) in ("number", "date", "time")]

    entries: Dict[str, _DimProfileEntry] = {}

    # --- categorical dims: one query per dim
    for c in categorical:
        try:
            q = SlayerQuery.model_validate({
                "source_model": model.name,
                "dimensions": [{"name": c.name}],
                "measures": [{"formula": "*:count"}],
                "limit": max_values + 1,
            })
            r = await engine.execute(query=q, data_source=model.data_source or None)
        except Exception:
            continue
        value_key = f"{model.name}.{c.name}"
        values = [row.get(value_key) for row in r.data]
        overflow = len(values) > max_values
        entries[c.name] = _DimProfileEntry(
            name=c.name,
            type_str=str(c.type),
            distinct_count=None if overflow else len(values),
            values=None if overflow else values,
            min_value=None,
            max_value=None,
        )

    # --- numeric/temporal dims: ONE batched query for all mins and maxes
    if numeric_temporal:
        ext_columns = [
            {"name": f"_slayer_range_{c.name}", "sql": c.sql if c.sql else c.name,
             "type": str(c.type)}
            for c in numeric_temporal
        ]
        measures_payload: List[Dict[str, str]] = []
        for c in numeric_temporal:
            measures_payload.append({"formula": f"_slayer_range_{c.name}:min"})
            measures_payload.append({"formula": f"_slayer_range_{c.name}:max"})
        row: Dict[str, Any] = {}
        try:
            q = SlayerQuery.model_validate({
                "source_model": {"source_name": model.name, "columns": ext_columns},
                "measures": measures_payload,
            })
            r = await engine.execute(query=q, data_source=model.data_source or None)
            if r.data:
                row = r.data[0]
        except Exception:
            row = {}
        for c in numeric_temporal:
            mn = row.get(f"{model.name}._slayer_range_{c.name}_min")
            mx = row.get(f"{model.name}._slayer_range_{c.name}_max")
            if mn is None and mx is None:
                continue  # query failed or empty table
            entries[c.name] = _DimProfileEntry(
                name=c.name,
                type_str=str(c.type),
                distinct_count=None,
                values=None,
                min_value=mn,
                max_value=mx,
            )

    # Preserve declaration order in the rendered output
    return [entries[c.name] for c in eligible if c.name in entries]


async def _collect_measure_profile(
    model: SlayerModel,
    engine: SlayerQueryEngine,
) -> Dict[str, str]:
    """Probe min/max for each non-hidden, non-primary-key column via a single
    batched query.

    Returns ``{column_name: "min .. max"}`` for columns with data, or
    ``{column_name: "all NULL"}`` for columns where both min and max are NULL.
    Skips primary-key columns (their values are identifiers, not values to
    profile).
    """
    columns = [c for c in model.columns if not c.hidden and not c.primary_key]
    if not columns:
        return {}

    # Use ModelExtension with inline columns to bypass allowed_aggregations
    ext_columns = [
        {"name": f"_slayer_probe_{c.name}", "sql": c.sql if c.sql else c.name,
         "type": str(c.type)}
        for c in columns
    ]
    measures_payload: List[Dict[str, str]] = []
    for c in columns:
        measures_payload.append({"formula": f"_slayer_probe_{c.name}:min"})
        measures_payload.append({"formula": f"_slayer_probe_{c.name}:max"})

    try:
        q = SlayerQuery.model_validate({
            "source_model": {"source_name": model.name, "columns": ext_columns},
            "measures": measures_payload,
        })
        r = await engine.execute(query=q, data_source=model.data_source or None)
        row = r.data[0] if r.data else {}
    except Exception:
        return {}

    result: Dict[str, str] = {}
    for c in columns:
        mn = row.get(f"{model.name}._slayer_probe_{c.name}_min")
        mx = row.get(f"{model.name}._slayer_probe_{c.name}_max")
        if mn is None and mx is None:
            result[c.name] = "all NULL"
        else:
            result[c.name] = f"{mn} .. {mx}"
    return result


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
        # v4 (DEV-1330): walk the join graph within the *root* model's
        # data_source. Cross-datasource joins aren't auto-mirrored, so any
        # bare-name resolution that crosses a datasource boundary would be
        # picking up a sibling model that isn't actually reachable from
        # ``model``.
        try:
            target = await storage.get_model(target_name, data_source=model.data_source or None)
        except Exception:  # noqa: BLE001 — AmbiguousModelError or storage misses
            target = None
        if target is None:
            continue
        for c in target.columns:
            if c.hidden:
                continue
            if not c.primary_key:
                reachable_dims.add(f"{path}.{c.name}")
            reachable_measures.add(f"{path}.{c.name}")
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


def _build_backing_query_info(model: SlayerModel) -> Optional[dict]:
    """Build the ``backing_query`` block for inspect_model output.

    Returns ``None`` for non-query-backed models. For query-backed models,
    returns ``{variables, required_variables, stages}`` where:

    - ``variables``: ``model.query_variables`` (defaults).
    - ``required_variables``: placeholder names that have no default.
    - ``stages``: each stage dumped as a dict, ready for JSON output.
    """
    if not model.source_queries:
        return None
    from slayer.core.query import extract_placeholder_names

    all_placeholders: set = set()
    stage_dicts: List[dict] = []
    # A placeholder is "required" only if it has no default at any layer the
    # engine consults: model.query_variables OR the stage's own variables.
    defaulted: set = set(model.query_variables.keys())
    for q in model.source_queries:
        all_placeholders |= extract_placeholder_names(q)
        if q.variables:
            defaulted |= set(q.variables.keys())
        stage_dicts.append(q.model_dump(mode="json", exclude_none=True))
    required = sorted(all_placeholders - defaulted)
    return {
        "variables": dict(model.query_variables),
        "required_variables": required,
        "stages": stage_dicts,
    }


def _render_field_value(v: Any) -> str:
    """Pick the most descriptive label out of a query-stage field value.

    Stage list entries can be plain strings, simple `{name}` dicts, formula
    dicts, or wrapper dicts like `{"dimension": {"name": ...}}`. Try each
    shape in priority order and fall back to `str(v)` if nothing matches.
    """
    if not isinstance(v, dict):
        return str(v)
    name = v.get("name")
    if name:
        return str(name)
    formula = v.get("formula")
    if formula:
        return str(formula)
    inner = v.get("dimension")
    if isinstance(inner, dict):
        inner_name = inner.get("name")
        if inner_name:
            return str(inner_name)
    return str(v)


def _render_stage_field_list(key: str, val: list) -> str:
    """Render a stage's field list (dimensions / measures / filters / etc.)."""
    if key == "filters":
        return "; ".join(f"`{f}`" for f in val)
    return "; ".join(_render_field_value(v) for v in val)


def _render_source_model(src: Any) -> Optional[str]:
    """Render a stage's ``source_model`` (str or ModelExtension dict)."""
    if isinstance(src, str):
        return f"- source_model: `{src}`"
    if isinstance(src, dict):
        sn = src.get("source_name") or src.get("name")
        if sn:
            return f"- source_model: `{sn}` (extension)"
    return None


def _render_stage(i: int, stage: dict, total: int) -> List[str]:
    """Render one stage's markdown lines."""
    title = stage.get("name") or ("final" if i == total else f"stage {i}")
    out: List[str] = [f"\n**{i}. {title}**"]
    src_line = _render_source_model(stage.get("source_model"))
    if src_line:
        out.append(src_line)
    for key in ("dimensions", "time_dimensions", "measures", "filters"):
        val = stage.get(key)
        if not val:
            continue
        out.append(f"- {key}: {_render_stage_field_list(key, val)}")
    return out


def _backing_query_markdown_section(info: dict) -> str:
    """Format the ``backing_query`` info as a markdown section."""
    lines: List[str] = ["## Backing Query"]
    stages = info.get("stages") or []
    for i, stage in enumerate(stages, start=1):
        lines.extend(_render_stage(i, stage, len(stages)))
    variables = info.get("variables") or {}
    required = info.get("required_variables") or []
    if variables or required:
        lines.append("\n**Variables:**")
        for k, v in variables.items():
            lines.append(f"- `{k}`: default `{v}`")
        for k in required:
            lines.append(f"- `{k}`: required")
    return "\n".join(lines)


def _source_type_for(model: SlayerModel) -> str:
    """Classify a model's source mode for summary/inspect output."""
    if model.source_queries:
        return "query"
    if model.sql_table:
        return "table"
    if model.sql:
        return "sql"
    return "unknown"


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
    async def query(  # NOSONAR S107 — FastMCP introspects this signature to expose each query option as a typed MCP tool argument; collapsing into a dict would degrade the agent-facing schema
        source_model: str,
        measures: Optional[List[Dict[str, str]]] = None,
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
        variables: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Query data from a semantic model. Call inspect_model first to see available columns and measures.

        Args:
            source_model: Name of the model to query (from models_summary).
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

        Example: query(source_model="orders", measures=[{"formula": "*:count"}], dimensions=["status"], filters=["status == 'completed'"])

        Before calling this tool, run ``recall_memories`` first, supplying the entities you're thinking of using (or the query itself via the ``about`` arg). Read the returned learnings and consider any matching saved queries before formulating the final query.
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
        if measures:
            data["measures"] = measures
        if variables:
            data["variables"] = dict(variables)
        try:
            fmt = format.lower().strip()
            if fmt not in ("json", "csv", "markdown"):
                raise ValueError(f"Invalid format '{format}'. Must be one of: json, csv, markdown")
            # Run-by-name shortcut: when only source_model is given (no
            # measures / dimensions / filters / time_dimensions / order /
            # limit / offset), dispatch through ``engine.execute(str)`` so
            # the model's stored backing query runs directly. Variables flow
            # through with the documented run-by-name precedence.
            no_overrides = (
                not measures and not dimensions and not filters
                and not time_dimensions and not order
                and limit is None and offset is None
                and not whole_periods_only
            )
            if no_overrides:
                target = await storage.get_model(source_model)
                if target is not None and target.source_queries:
                    result = await engine.execute(
                        source_model,
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

    # -----------------------------------------------------------------------
    # Model discovery
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def models_summary(datasource_name: str, format: str = "markdown") -> str:
        """Brief summary of all (non-hidden) models in a datasource.

        For each model: name, description, a table of its columns (name +
        type + description), a table of its named-formula measures (name
        + formula + description), and a comma-separated list of the model
        names it joins to. No distinct values, no sample data, and no
        expansion of joined models' fields — call inspect_model for any
        of that.

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

        all_names = await storage.list_models(data_source=datasource_name)
        matched: List[SlayerModel] = []
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

        sections: List[str] = [
            f"# Datasource: `{datasource_name}` — {len(matched)} model(s)"
        ]
        for m in matched:
            model_lines: List[str] = [f"## `{m.name}`"]
            if m.description:
                model_lines.append(m.description)

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
        sections: Optional[List[str]] = None,
        descriptions_max_chars: Optional[int] = None,
        reachable_fields_depth: int = 5,
        data_source: Optional[str] = None,
    ) -> str:
        """Return a complete-yet-compact view of a semantic model.

        Always emitted (regardless of ``sections``): model header + description,
        metadata bullets (data_source, sql_table, default_time_dimension,
        hidden, row_count), backing-query structure for query-backed models,
        and — when ``show_sql=True`` — the custom SQL block, model-level
        filters, and the cached backing-query SQL.

        Section-gated parts (subset selectable via ``sections``):

        - ``columns`` — unified row-level columns table with a ``sampled``
          column (distinct values for string/boolean, ``min .. max`` for
          number/date/time, or ``> 20 distinct`` for high-cardinality
          categoricals).
        - ``measures`` — named-formula library.
        - ``aggregations`` — custom aggregation definitions. The ``formula``
          column and the ``sql`` field of each ``params[]`` entry are gated
          by ``show_sql``.
        - ``joins`` — join definitions.
        - ``reachable_fields`` — BFS-walked fields reachable via joins.
        - ``samples`` — live sample-data query (``COUNT(*)`` plus one
          aggregation per column).

        When a section is omitted from ``sections``: ``columns``, ``measures``,
        ``aggregations`` and ``joins`` collapse to a one-line backticked CSV
        of names; ``reachable_fields`` and ``samples`` are dropped entirely.
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
                "joins", "reachable_fields", "samples"]``. Default (``None``
                or empty list) renders all six. Unknown names are ignored
                with a warning line at the end of the response. A non-empty
                list of *only* unknown names resolves to no sections (not
                all six) — "all sections" is reserved for ``None``/``[]`` so
                a typo can't silently trigger the full expensive payload.
            descriptions_max_chars: When set, every description field (model,
                column, measure, aggregation) longer than this is truncated
                with a ``... [truncated]`` suffix. Must be ``>= 0``. ``None``
                (default) means no truncation.
            reachable_fields_depth: Max BFS depth (in path segments) for the
                reachable-fields walk. Default 5; allowed range
                ``[0, 20]``. Ignored when ``reachable_fields`` is not in
                ``sections``.
        """
        fmt = format.lower().strip()
        if fmt not in ("markdown", "json"):
            raise ValueError(
                f"Invalid format '{format}' for inspect_model. Must be 'markdown' or 'json'."
            )
        if descriptions_max_chars is not None and descriptions_max_chars < 0:
            raise ValueError(
                f"descriptions_max_chars must be >= 0, got {descriptions_max_chars}."
            )
        if reachable_fields_depth < 0 or reachable_fields_depth > _MAX_REACHABLE_FIELDS_DEPTH:
            raise ValueError(
                f"reachable_fields_depth must be between 0 and {_MAX_REACHABLE_FIELDS_DEPTH}, "
                f"got {reachable_fields_depth}."
            )

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

        # Resolve section gating up front so we can short-circuit DB calls
        # for parts the caller doesn't want.
        included, unknown = _resolve_inspect_sections(sections)
        included_set = set(included)

        # Categorise non-included sections into "names-only" (still listed,
        # just collapsed to CSV) vs "fully omitted" (no heading at all).
        names_only_sections = [
            s for s in _INSPECT_SECTIONS_NAMES_ONLY if s not in included_set
        ]
        omitted_sections = [
            s for s in _INSPECT_SECTIONS_OMITTABLE if s not in included_set
        ]

        truncated_model_desc = _truncate_description(model.description, descriptions_max_chars)
        out_sections: List[str] = [f"# Model: `{model.name}`"]
        if truncated_model_desc:
            out_sections.append(truncated_model_desc)

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
        if model.meta is not None:
            meta.append(f"- **meta:** {json.dumps(model.meta, sort_keys=True, default=str)}")
        row_count = await _get_row_count(model=model, engine=engine)
        if row_count is not None:
            meta.append(f"- **row_count:** {row_count:,}")
        if meta:
            out_sections.append("\n".join(meta))

        if show_sql and model.sql:
            out_sections.append(f"## SQL\n\n```sql\n{model.sql}\n```")

        if show_sql and model.filters:
            filter_lines = "\n".join(f"- `{f}`" for f in model.filters)
            out_sections.append(f"## Filters (model-level)\n\n{filter_lines}")

        # Backing-query section (query-backed models only). Structure is
        # always-on (it's the model's identity for query-backed models, like
        # `sql_table` is for table-backed); only the SQL cache is gated by
        # show_sql.
        backing_info = _build_backing_query_info(model)
        if backing_info is not None:
            out_sections.append(_backing_query_markdown_section(backing_info))
            if show_sql and model.backing_query_sql:
                out_sections.append(
                    f"## Backing Query SQL\n\n```sql\n{model.backing_query_sql}\n```"
                )

        # ------------------------------------------------------------------
        # DB-hitting computations — skip when their consumers aren't requested.
        # ------------------------------------------------------------------
        # Dimension/measure profile populates the ``sampled`` column of the
        # columns table. Only needed when ``columns`` is fully included.
        profile_by_name: Dict[str, str] = {}
        measure_profile: Dict[str, str] = {}
        if "columns" in included_set:
            profile_entries = await _collect_dim_profile(model=model, engine=engine)
            profile_by_name = {
                e.name: _format_dim_profile_value(e) for e in profile_entries
            }
            measure_profile = await _collect_measure_profile(model=model, engine=engine)

        # ``measure_types`` informs the sample query's choice of avg vs
        # count_distinct. Only needed when ``samples`` is in the included set.
        measure_types: Dict[str, str] = {}
        if "samples" in included_set:
            measure_types = await engine.get_column_types(
                model_name=model.name,
                data_source=model.data_source or None,
            )

        # ------------------------------------------------------------------
        # Columns section
        # ------------------------------------------------------------------
        visible_columns = [c for c in model.columns if not c.hidden]
        if "columns" in included_set:
            col_rows: List[Dict[str, Any]] = []
            for c in visible_columns:
                aggs = ", ".join(c.allowed_aggregations) if c.allowed_aggregations else "all"
                col_rows.append({
                    "name": c.name,
                    "type": str(c.type),
                    "primary_key": "yes" if c.primary_key else "",
                    "sql": c.sql if c.sql else c.name,
                    "allowed_aggregations": aggs,
                    "filter": c.filter,
                    "label": c.label,
                    "description": _truncate_description(c.description, descriptions_max_chars),
                    "meta": _format_meta(c.meta),
                    "sampled": (
                        profile_by_name.get(c.name)
                        or measure_profile.get(c.name)
                    ),
                })
            col_columns = [
                "name", "type", "primary_key", "sql", "allowed_aggregations",
                "filter", "label", "description", "meta", "sampled",
            ]
            if not show_sql:
                col_columns = [c for c in col_columns if c not in ("sql", "filter")]
            out_sections.append(
                f"## Columns ({len(col_rows)})\n\n"
                + _markdown_table(rows=col_rows, columns=col_columns)
            )
        elif visible_columns:
            csv = ", ".join(_md_code_span(c.name) for c in visible_columns)
            out_sections.append(
                f"## Columns ({len(visible_columns)} — names only)\n\n{csv}"
            )

        # ------------------------------------------------------------------
        # Measures section
        # ------------------------------------------------------------------
        if "measures" in included_set:
            measure_rows: List[Dict[str, Any]] = []
            for mm in model.measures:
                measure_rows.append({
                    "name": mm.name,
                    "formula": mm.formula,
                    "label": mm.label,
                    "description": _truncate_description(mm.description, descriptions_max_chars),
                    "meta": _format_meta(mm.meta),
                })
            out_sections.append(
                f"## Measures ({len(measure_rows)})\n\n"
                + _markdown_table(
                    rows=measure_rows,
                    columns=["name", "formula", "label", "description", "meta"],
                )
            )
        elif model.measures:
            csv = ", ".join(_md_code_span(mm.name) for mm in model.measures)
            out_sections.append(
                f"## Measures ({len(model.measures)} — names only)\n\n{csv}"
            )

        # ------------------------------------------------------------------
        # Aggregations section
        # ------------------------------------------------------------------
        if "aggregations" in included_set:
            if model.aggregations:
                agg_rows: List[Dict[str, Any]] = []
                for a in model.aggregations:
                    if a.params:
                        if show_sql:
                            params = "; ".join(f"{p.name}={p.sql}" for p in a.params)
                        else:
                            params = ", ".join(p.name for p in a.params)
                    else:
                        params = None
                    agg_rows.append({
                        "name": a.name,
                        "formula": a.formula or "(built-in override)",
                        "params": params,
                        "description": _truncate_description(
                            a.description, descriptions_max_chars,
                        ),
                        "meta": _format_meta(a.meta),
                    })
                agg_columns = ["name", "formula", "params", "description", "meta"]
                if not show_sql:
                    agg_columns = [c for c in agg_columns if c != "formula"]
                out_sections.append(
                    f"## Aggregations ({len(agg_rows)})\n\n"
                    + _markdown_table(rows=agg_rows, columns=agg_columns)
                )
        elif model.aggregations:
            csv = ", ".join(_md_code_span(a.name) for a in model.aggregations)
            out_sections.append(
                f"## Aggregations ({len(model.aggregations)} — names only)\n\n{csv}"
            )

        # ------------------------------------------------------------------
        # Joins section
        # ------------------------------------------------------------------
        if "joins" in included_set:
            join_rows: List[Dict[str, Any]] = []
            for j in model.joins:
                pairs = "; ".join(f"{src} = {tgt}" for src, tgt in j.join_pairs)
                join_rows.append({
                    "target_model": j.target_model,
                    "join_pairs": pairs,
                })
            out_sections.append(
                f"## Joins ({len(join_rows)})\n\n"
                + _markdown_table(
                    rows=join_rows,
                    columns=["target_model", "join_pairs"],
                )
            )
        elif model.joins:
            csv = ", ".join(_md_code_span(j.target_model) for j in model.joins)
            out_sections.append(
                f"## Joins ({len(model.joins)} — names only)\n\n{csv}"
            )

        # ------------------------------------------------------------------
        # Reachable via joins (fully omitted when not in sections)
        # ------------------------------------------------------------------
        reach_dims: List[str] = []
        reach_measures: List[str] = []
        if "reachable_fields" in included_set:
            reach_dims, reach_measures = await _collect_reachable_fields(
                model=model, storage=storage, max_depth=reachable_fields_depth,
            )
            if reach_dims or reach_measures:
                lines = [
                    f"## Reachable via joins (max depth: {reachable_fields_depth})", "",
                ]
                if reach_dims:
                    rendered = ", ".join(f"`{d}`" for d in reach_dims)
                    lines.append(f"**Dimensions ({len(reach_dims)}):** {rendered}")
                if reach_measures:
                    rendered = ", ".join(f"`{m}`" for m in reach_measures)
                    lines.append(f"**Measures ({len(reach_measures)}):** {rendered}")
                out_sections.append("\n".join(lines))

        # ------------------------------------------------------------------
        # Sample data (fully omitted when not in sections)
        # ------------------------------------------------------------------
        sample_sql: Optional[str] = None
        sample_data: Optional[Dict[str, Any]] = None
        sample_error: Optional[str] = None
        if "samples" in included_set:
            query_args = _build_sample_query_args(
                model=model, num_rows=num_rows, measure_types=measure_types,
            )
            try:
                sample_query = SlayerQuery.model_validate(query_args)
                sample_result = await engine.execute(
                    query=sample_query, data_source=model.data_source or None
                )
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
                out_sections.append(sample_section)
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
                out_sections.append(sample_section)

        # ------------------------------------------------------------------
        # Learnings (DEV-1357 v2) — surfaces only memories where ``query`` is
        # ``None``; query-bearing memories are recall-only. Auto-pruned when
        # no learning-shaped memory matches.
        # ------------------------------------------------------------------
        relevant_learnings: List[Any] = []
        wanted: List[str] = []
        if "learnings" in included_set:
            ds = model.data_source
            wanted = [f"{ds}.{model.name}"]
            wanted.extend(f"{ds}.{model.name}.{c.name}" for c in model.columns)
            wanted.extend(
                f"{ds}.{model.name}.{m.name}"
                for m in model.measures
                if m.name is not None
            )
            wanted.extend(
                f"{ds}.{model.name}.{a.name}" for a in model.aggregations
            )
            candidates = await storage.list_memories(entities=wanted)
            relevant_learnings = [m for m in candidates if m.query is None]
            if relevant_learnings:
                lines = [f"## Learnings ({len(relevant_learnings)})", ""]
                for memory in relevant_learnings:
                    matched = sorted(set(wanted) & set(memory.entities))
                    matched_md = ", ".join(f"`{e}`" for e in matched)
                    lines.append(
                        f"- **M{memory.id}** ({matched_md}): {memory.learning}"
                    )
                out_sections.append("\n".join(lines))

        # ------------------------------------------------------------------
        # Per-call truncation footer (only when something was trimmed or an
        # unknown section name was supplied).
        # ------------------------------------------------------------------
        footer = _render_inspect_footer(
            included=included,
            names_only=names_only_sections,
            omitted=omitted_sections,
            unknown=unknown,
        )

        if fmt == "json":
            payload: Dict[str, Any] = {
                "model_name": model.name,
                "description": truncated_model_desc,
                "data_source": model.data_source,
                "source_type": _source_type_for(model),
            }
            if show_sql:
                payload["sql_table"] = model.sql_table
                payload["sql"] = model.sql
            if backing_info is not None:
                payload["backing_query"] = backing_info
                if show_sql and model.backing_query_sql:
                    payload["backing_query_sql"] = model.backing_query_sql
            payload["default_time_dimension"] = model.default_time_dimension
            payload["hidden"] = model.hidden
            payload["meta"] = model.meta
            payload["row_count"] = row_count
            if show_sql:
                payload["filters"] = model.filters

            # Columns
            if "columns" in included_set:
                payload["columns"] = [
                    {
                        "name": c.name,
                        "type": str(c.type),
                        "primary_key": c.primary_key,
                        **({"sql": c.sql} if show_sql else {}),
                        "allowed_aggregations": c.allowed_aggregations,
                        **({"filter": c.filter} if show_sql else {}),
                        "label": c.label,
                        "description": _truncate_description(
                            c.description, descriptions_max_chars,
                        ),
                        "meta": c.meta,
                        "sampled": (
                            profile_by_name.get(c.name)
                            or measure_profile.get(c.name)
                        ),
                    }
                    for c in visible_columns
                ]
            elif visible_columns:
                payload["columns_names"] = [c.name for c in visible_columns]

            # Measures
            if "measures" in included_set:
                payload["measures"] = [
                    {
                        "name": mm.name,
                        "formula": mm.formula,
                        "label": mm.label,
                        "description": _truncate_description(
                            mm.description, descriptions_max_chars,
                        ),
                        "meta": mm.meta,
                    }
                    for mm in model.measures
                ]
            elif model.measures:
                payload["measures_names"] = [mm.name for mm in model.measures]

            # Aggregations
            if "aggregations" in included_set:
                payload["aggregations"] = [
                    {
                        "name": a.name,
                        **({"formula": a.formula} if show_sql else {}),
                        "params": [
                            ({"name": p.name, "sql": p.sql} if show_sql else {"name": p.name})
                            for p in (a.params or [])
                        ],
                        "description": _truncate_description(
                            a.description, descriptions_max_chars,
                        ),
                        "meta": a.meta,
                    }
                    for a in model.aggregations
                ]
            elif model.aggregations:
                payload["aggregations_names"] = [a.name for a in model.aggregations]

            # Joins
            if "joins" in included_set:
                payload["joins"] = [
                    {
                        "target_model": j.target_model,
                        "join_pairs": j.join_pairs,
                    }
                    for j in model.joins
                ]
            elif model.joins:
                payload["joins_names"] = [j.target_model for j in model.joins]

            # Reachable fields
            if "reachable_fields" in included_set:
                payload["reachable_dimensions"] = reach_dims
                payload["reachable_measures"] = reach_measures

            # Samples
            if "samples" in included_set:
                payload["sample_data"] = sample_data
                payload["sample_data_error"] = sample_error
                if show_sql and sample_sql:
                    payload["sample_sql"] = sample_sql

            # Learnings (DEV-1357 v2) — Memory carries ``learning``,
            # not ``body``; reading ``.body`` here would AttributeError
            # the moment a memory matches and the caller asked for JSON
            # output.
            if "learnings" in included_set and relevant_learnings:
                payload["learnings"] = [
                    {
                        "id": memory.id,
                        "learning": memory.learning,
                        "matched_entities": sorted(
                            set(wanted) & set(memory.entities)
                        ),
                    }
                    for memory in relevant_learnings
                ]

            # Top-level gating-state arrays (only when non-empty)
            if names_only_sections:
                payload["names_only_sections"] = names_only_sections
            if omitted_sections:
                payload["omitted_sections"] = omitted_sections
            if unknown:
                payload["unknown_sections"] = unknown

            return json.dumps(payload, indent=2, default=str)

        if footer:
            out_sections.append(footer)
        return "\n\n".join(out_sections)

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
        columns: Optional[List[Dict[str, Any]]] = None,
        measures: Optional[List[Dict[str, Any]]] = None,
        query: Optional[Any] = None,
        variables: Optional[Dict[str, Any]] = None,
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

    VALID_REMOVE_KEYS = {"columns", "measures", "aggregations", "joins"}

    @mcp.tool()
    async def edit_model(
        model_name: str,
        description: Optional[str] = None,
        data_source: Optional[str] = None,
        new_data_source: Optional[str] = None,
        default_time_dimension: Optional[str] = None,
        sql_table: Optional[str] = None,
        sql: Optional[str] = None,
        source_queries: Optional[List[Dict[str, Any]]] = None,
        query_variables: Any = _UNSET,
        hidden: Optional[bool] = None,
        columns: Optional[List[Dict[str, Any]]] = None,
        measures: Optional[List[Dict[str, Any]]] = None,
        aggregations: Optional[List[Dict[str, Any]]] = None,
        joins: Optional[List[Dict[str, Any]]] = None,
        add_filters: Optional[List[str]] = None,
        remove_filters: Optional[List[str]] = None,
        remove: Optional[Dict[str, List[str]]] = None,
        meta: Optional[Dict[str, Any]] = _UNSET,
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
        changes: List[str] = []

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
            changes.append(f"set sql_table to '{sql_table}'")
        if sql is not None:
            model.sql = sql
            model.sql_table = None
            model.source_queries = None
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
        for spec in columns or []:
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
                lines.append(f"- {m.name} ({len(m.columns)} columns, {len(m.measures)} measures)")
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
    async def delete_model(name: str, data_source: Optional[str] = None) -> str:
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
            lines.append(f"- {m.name} ({len(m.columns)} columns, {len(m.measures)} measures)")
        lines.append("")
        lines.append("Use models_summary and inspect_model to explore, then query to fetch data.")
        return "\n".join(lines)

    @mcp.tool()
    async def set_datasource_priority(priority: List[str]) -> str:
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
    ) -> str:
        """Save an agent memory: a free-form note plus the SLayer
        entities it concerns.

        ``linked_entities`` accepts either:

        * a list of entity reference strings — each item is resolved to
          the canonical ``<datasource>.<model>[.<leaf>]`` form. Bare
          names use the datasource priority list; ambiguous bare-column
          matches are rejected.
        * a ``SlayerQuery`` (dict) — entities are auto-extracted from
          ``source_model``, ``dimensions``, ``time_dimensions``,
          ``measures``, and ``filters``; resolution warnings are
          non-fatal. The query itself is stored alongside the
          learning, so the memory surfaces in
          ``recall_memories``'s ``queries`` list (vs the
          ``learnings`` list for entity-list memories).

        Returns the assigned ``memory_id`` (a positive int), the
        canonical entities stored, and any non-fatal warnings.

        Args:
            learning: The note text. Required, non-empty.
            linked_entities: List of entity strings, or an inline
                ``SlayerQuery`` payload.

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
            )
        """
        try:
            response = await memory_service.save_memory(
                learning=learning, linked_entities=linked_entities
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

        Args:
            id: The ``memory_id`` returned by ``save_memory`` (a positive
                int). String forms (``"42"``) are accepted and coerced.

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

    @mcp.tool()
    async def recall_memories(
        about: Any,
        max_learnings: Optional[int] = None,
        max_queries: Optional[int] = 2,
    ) -> str:
        """Look up agent memories by entity overlap.

        Call this BEFORE ``query`` to surface any notes or example
        queries previously saved against the entities you're
        considering.

        ``about`` accepts either:

        * a list of entity reference strings — each is resolved
          strictly; resolution failures raise.
        * a ``SlayerQuery`` (dict) — the entity extractor walks the
          query and warnings are non-fatal.

        Empty input (``about=[]`` or a query with no extractable
        entities) returns all memories ranked by recency (newest first)
        with an explanatory warning.

        Stored memories are ranked by the size of the intersection
        between their stored entity set and the input set. Ties break
        by recency (newest first). The result splits memories without
        an attached query (``learnings``) from those with one
        (``queries``); each list is capped independently.

        Args:
            about: List of entity reference strings, or an inline
                ``SlayerQuery`` payload.
            max_learnings: Cap on returned learning-shaped memories.
                ``None`` = all.
            max_queries: Cap on returned query-bearing memories.
                Default ``2``.
        """
        try:
            response = await memory_service.recall_memories(
                about=about,
                max_learnings=max_learnings,
                max_queries=max_queries,
            )
        except (
            EntityResolutionError,
            AmbiguousModelError,
            ValueError,
        ) as exc:
            return _format_resolution_error(exc)
        return response.model_dump_json(indent=2)

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