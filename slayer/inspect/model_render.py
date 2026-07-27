"""DEV-1588: model-render core extracted from ``slayer/mcp/server.py``.

This module owns the helpers + the full ``render_model_inspection`` body
that the legacy MCP ``inspect_model`` tool used to inline. Both the
``inspect`` surfaces (via :class:`slayer.inspect.service.InspectService`)
and the kept-but-deprecated ``inspect_model`` tool now delegate here, so
there is a single source of truth for the model render.

IMPORTANT: this module must NOT import ``slayer.mcp`` — ``mcp/server.py``
imports from here, so the reverse would be a circular import.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import sqlalchemy as sa

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import _friendly_db_error
from slayer.engine.profiling import (
    _is_sample_cached,
    _profile_numeric_temporal_columns,
    ensure_column_sample_fresh,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.search.render import compact_description_from_learning
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)

# Aggregations that are safe for sample-data extraction: zero extra args,
# no time-column context needed.
_SAFE_SAMPLE_AGGS = frozenset({"avg", "sum", "min", "max", "count", "count_distinct", "median"})

# Section-level budgeting for inspect_model output.
# columns/measures/aggregations/joins fall back to a names-only CSV when the
# caller drops the section from `sections`; samples/learnings are fully
# omitted (they have no natural "names" to list).
_INSPECT_SECTIONS_NAMES_ONLY = ("columns", "measures", "aggregations", "joins")
_INSPECT_SECTIONS_OMITTABLE = ("samples", "learnings")
_VALID_INSPECT_SECTIONS = _INSPECT_SECTIONS_NAMES_ONLY + _INSPECT_SECTIONS_OMITTABLE
_TRUNCATION_MARKER = " ... [truncated]"
# Placeholder rendered for an empty section / pruned markdown table.
_NONE_PLACEHOLDER = "_(none)_"


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


def _truncate_description(text: str | None, max_chars: int | None) -> str | None:
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


def _format_meta(meta: dict[str, Any] | None) -> str | None:
    """Compact JSON for the ``inspect_model`` meta cell.

    Returns ``None`` when ``meta`` is ``None`` so ``_markdown_table``'s
    all-empty-column pruner hides the meta column when no row has meta set.
    """
    if meta is None:
        return None
    return json.dumps(meta, sort_keys=True, default=str)


def _resolve_inspect_sections(
    sections: list[str] | None,
) -> tuple[list[str], list[str]]:
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
    included: list[str],
    names_only: list[str],
    omitted: list[str],
    unknown: list[str],
) -> str | None:
    """Build the per-call truncation footer for ``inspect_model``.

    Returns ``None`` when there is nothing to report (no trimming, no
    unknown names). Otherwise returns a quoted-markdown block.
    """
    if not (names_only or omitted or unknown):
        return None
    lines: list[str] = []
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


def _markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
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
        return _NONE_PLACEHOLDER

    kept = [c for c in columns if any(_cell_is_present(r.get(c)) for r in rows)]
    if not kept:
        return _NONE_PLACEHOLDER

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


def _choose_sample_dims(
    model: SlayerModel,
) -> tuple[list[dict[str, str]], set]:
    """Pick up to two categorical (TEXT/BOOLEAN) non-hidden, non-PK columns to
    group the sample by, so they aren't also aggregated as measures
    (count_distinct(status) grouped by status is always 1)."""
    dims: list[dict[str, str]] = []
    dim_names: set = set()
    for c in model.columns:
        if c.hidden or c.primary_key:
            continue
        # DEV-1361: TEXT/BOOLEAN are the categorical-shaped types.
        if c.type not in (DataType.TEXT, DataType.BOOLEAN):
            continue
        dims.append({"name": c.name})
        dim_names.add(c.name)
        if len(dims) >= 2:
            break
    return dims, dim_names


def _choose_sample_agg(
    column: Column,
    *,
    measure_types: dict[str, str],
) -> str | None:
    """Pick a sample aggregation for ``column``, or ``None`` to skip it.

    - With a restricted ``allowed_aggregations`` that excludes ``avg``: prefer
      the first zero-arg-safe built-in (``_SAFE_SAMPLE_AGGS``); if none, fall
      back to the first allowed entry (even if it needs extra context — an
      intentional, tested behavior). Empty list → skip.
    - Otherwise (``avg`` permitted): prefer ``avg`` for numeric columns, else
      ``count_distinct`` (type inferred from ``measure_types`` — the lowercase
      ``engine.get_column_types`` contract — or the column's own ``type``).
    """
    allowed = column.allowed_aggregations
    if allowed is not None and "avg" not in allowed:
        if not allowed:
            return None
        safe = next((a for a in allowed if a in _SAFE_SAMPLE_AGGS), None)
        return safe if safe else allowed[0]
    inferred = measure_types.get(column.name)
    inferred_norm = inferred.strip().lower() if isinstance(inferred, str) else None
    if inferred_norm and inferred_norm != "number":
        return "count_distinct"
    if column.type not in (DataType.INT, DataType.DOUBLE):
        return "count_distinct"
    return "avg"


def _build_sample_query_args(
    model: SlayerModel,
    num_rows: int,
    measure_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the ``SlayerQuery`` payload for ``inspect_model``'s sample data.

    First measure is always ``*:count``; then one aggregation per non-hidden,
    non-primary-key, non-grouped column (see :func:`_choose_sample_agg`).
    """
    measure_types = measure_types or {}
    dims, dim_names = _choose_sample_dims(model)

    measures: list[dict[str, str]] = [{"formula": "*:count"}]
    for c in model.columns:
        if c.hidden or c.primary_key or c.name in dim_names:
            continue
        agg = _choose_sample_agg(c, measure_types=measure_types)
        if agg is None:
            continue
        measures.append({"formula": f"{c.name}:{agg}"})

    return {
        "source_model": model.name,
        "measures": measures,
        "dimensions": dims,
        "limit": num_rows,
    }


def _strip_model_prefix(
    columns: list[str],
    data: list[dict[str, Any]],
    model_name: str,
) -> tuple[list[str], list[dict[str, Any]]]:
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
) -> int | None:
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


async def _collect_measure_profile(
    model: SlayerModel,
    engine: SlayerQueryEngine,
) -> dict[str, str]:
    """Probe min/max for each non-hidden, non-primary-key NUMERIC/TEMPORAL
    column via a single batched query.

    Returns ``{column_name: "min .. max"}`` for columns with data, or
    ``{column_name: "all NULL"}`` for columns where both min and max are NULL.
    Skips primary-key columns (their values are identifiers, not values to
    profile).

    DEV-1480: text/boolean columns are excluded here so they are served
    exclusively by the categorical dim profile (which populates both
    ``Column.sampled`` and ``Column.sampled_values``). Mixing the two
    paths for the same column would leave ``sampled_values=None`` while
    ``sampled`` is set, which ``_is_sample_cached`` correctly treats as a
    cache miss — leading to permanent re-profile every ``inspect_model``
    call.
    """
    _NUMERIC_TEMPORAL = (
        DataType.INT, DataType.DOUBLE, DataType.DATE, DataType.TIMESTAMP,
    )
    columns = [
        c for c in model.columns
        if not c.hidden and not c.primary_key
        and c.type in _NUMERIC_TEMPORAL
    ]
    if not columns:
        return {}

    # Use ModelExtension with inline columns to bypass allowed_aggregations
    ext_columns = [
        {"name": f"_slayer_probe_{c.name}", "sql": c.sql if c.sql else c.name,
         "type": str(c.type)}
        for c in columns
    ]
    measures_payload: list[dict[str, str]] = []
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

    result: dict[str, str] = {}
    for c in columns:
        mn = row.get(f"{model.name}._slayer_probe_{c.name}_min")
        mx = row.get(f"{model.name}._slayer_probe_{c.name}_max")
        if mn is None and mx is None:
            result[c.name] = "all NULL"
        else:
            result[c.name] = f"{mn} .. {mx}"
    return result


def _build_backing_query_info(model: SlayerModel) -> dict | None:
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
    stage_dicts: list[dict] = []
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


def _render_source_model(src: Any) -> str | None:
    """Render a stage's ``source_model`` (str or ModelExtension dict)."""
    if isinstance(src, str):
        return f"- source_model: `{src}`"
    if isinstance(src, dict):
        sn = src.get("source_name") or src.get("name")
        if sn:
            return f"- source_model: `{sn}` (extension)"
    return None


def _render_stage(i: int, stage: dict, total: int) -> list[str]:
    """Render one stage's markdown lines."""
    title = stage.get("name") or ("final" if i == total else f"stage {i}")
    out: list[str] = [f"\n**{i}. {title}**"]
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
    lines: list[str] = ["## Backing Query"]
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


# ---------------------------------------------------------------------------
# Model schema skeleton (DEV-1588 follow-up)
# ---------------------------------------------------------------------------

def model_skeleton_fields(
    *, model: SlayerModel, max_chars: int | None = None,
) -> dict[str, Any]:
    """Cheap, DB-free structured skeleton of a model.

    Shape: ``{name, canonical_id, description, column_names, measure_names,
    aggregation_names, joins_to}``. Used by ``inspect(model, compact=True)``
    JSON and by each entry of ``inspect(datasource, compact=False)``'s
    ``models`` list (DEV-1588). ``description`` is truncated by ``max_chars``;
    ``canonical_id`` falls back to the bare name when ``data_source`` is unset
    (e.g. a not-yet-refined query-backed model).
    """
    canonical_id = (
        f"{model.data_source}.{model.name}" if model.data_source else model.name
    )
    return {
        "name": model.name,
        "canonical_id": canonical_id,
        "description": _truncate_description(model.description, max_chars),
        "column_names": [c.name for c in model.columns if not c.hidden],
        "measure_names": [m.name for m in model.measures if m.name is not None],
        "aggregation_names": [a.name for a in model.aggregations],
        "joins_to": sorted({j.target_model for j in model.joins}),
    }


def _skeleton_csv(names: list[str]) -> str:
    return ", ".join(names) if names else _NONE_PLACEHOLDER


def render_model_skeleton(
    *, model: SlayerModel, max_chars: int | None = None,
) -> str:
    """Heading-less markdown schema skeleton (DB-free).

    An optional truncated description line (only when set), then four lines —
    ``Columns`` / ``Measures`` / ``Aggregations`` / ``Joins to`` — always
    present, each empty value rendered ``_(none)_`` (aligned to
    ``models_summary(compact)``). The caller prepends the ``#``/``##`` heading.
    """
    fields = model_skeleton_fields(model=model, max_chars=max_chars)
    lines: list[str] = []
    if fields["description"]:
        lines.append(fields["description"])
    lines.append(f"Columns: {_skeleton_csv(fields['column_names'])}")
    lines.append(f"Measures: {_skeleton_csv(fields['measure_names'])}")
    lines.append(f"Aggregations: {_skeleton_csv(fields['aggregation_names'])}")
    lines.append(f"Joins to: {_skeleton_csv(fields['joins_to'])}")
    return "\n".join(lines)


async def render_model_inspection(  # NOSONAR(S3776) — faithful extraction of the inspect_model tool body; the section-gating + cache-miss + dual markdown/json render is intentionally a single linear pass
    *,
    model: SlayerModel,
    storage: StorageBackend,
    engine: SlayerQueryEngine | None,
    num_rows: int = 3,
    show_sql: bool = False,
    format: str = "markdown",
    sections: list[str] | None = None,
    descriptions_max_chars: int | None = None,
    compact: bool = True,
) -> str:
    """Render a complete-yet-compact view of an already-resolved model.

    This is the verbatim body of the legacy ``inspect_model`` MCP tool,
    extracted (DEV-1588) so the new ``inspect`` surfaces and the kept
    ``inspect_model`` tool share one implementation.

    ``engine=None`` contract: when no engine is supplied, the DB-hitting
    blocks (row count, live profiling, sample data) are skipped and the
    rest of the render proceeds without raising.
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
    out_sections: list[str] = [f"# Model: `{model.name}`"]
    if truncated_model_desc:
        out_sections.append(truncated_model_desc)

    # Metadata bullets (incl. row_count from a cheap *:count query)
    meta: list[str] = []
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
    row_count: int | None = None
    if engine is not None:
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
    # DB-hitting computations — skip when their consumers aren't requested
    # (and when no engine is available, DEV-1588).
    # ------------------------------------------------------------------
    profile_by_name: dict[str, str] = {}
    profile_values_by_name: dict[str, list[str] | None] = {}
    distinct_count_by_name: dict[str, int | None] = {}
    measure_profile: dict[str, str] = {}
    if engine is not None and "columns" in included_set:
        uncached_columns: list[Column] = []
        for c in model.columns:
            if c.hidden or c.primary_key:
                continue
            # DEV-1480 cache validity: categorical needs
            # ``sampled_values`` to be present (the structured field
            # is authoritative); numeric/temporal needs ``sampled``.
            if _is_sample_cached(c):
                if c.sampled is not None:
                    profile_by_name[c.name] = c.sampled
                profile_values_by_name[c.name] = c.sampled_values
                distinct_count_by_name[c.name] = c.distinct_count
            else:
                # v6-upgrade fallback: a categorical column may have
                # legacy ``sampled`` text but no ``sampled_values``
                # yet. Surface the legacy text in case the live
                # re-profile below fails for transient reasons —
                # ``profile_column`` will overwrite on success.
                if c.sampled is not None:
                    profile_by_name[c.name] = c.sampled
                uncached_columns.append(c)
        if uncached_columns:
            # DEV-1480: split the live profile into two paths so we
            # preserve the pre-DEV-1480 batching for numeric/temporal
            # columns. Categorical columns fire a top-values query
            # (and a secondary count_distinct on overflow) per column —
            # there's no efficient cross-column batching for those.
            # Numeric/temporal columns share one batched min/max query.
            _CATEGORICAL = (DataType.TEXT, DataType.BOOLEAN)
            _NUMERIC_TEMPORAL = (
                DataType.INT, DataType.DOUBLE,
                DataType.DATE, DataType.TIMESTAMP,
            )
            cat_uncached = [
                c for c in uncached_columns if c.type in _CATEGORICAL
            ]
            num_uncached = [
                c for c in uncached_columns if c.type in _NUMERIC_TEMPORAL
            ]

            async def _persist_sample(
                *, col_name: str,
                sampled: str | None,
                sampled_values: list[str] | None,
                distinct_count: int | None,
            ) -> None:
                try:
                    await storage.update_column_sampled(
                        data_source=model.data_source,
                        model_name=model.name,
                        column_name=col_name,
                        sampled=sampled,
                        sampled_values=sampled_values,
                        distinct_count=distinct_count,
                    )
                except Exception as exc:
                    logger.warning(
                        "inspect_model: failed to persist sampled value for "
                        "%s.%s.%s: %s",
                        model.data_source, model.name, col_name, exc,
                    )

            # Categorical: one top-values query per column (+ optional
            # count_distinct on overflow). DEV-1516: delegates to the
            # shared ``ensure_column_sample_fresh`` helper so the
            # cache-miss + persist + render-dict-population pattern is
            # owned by exactly one place (also used by the search
            # service's post-fusion column-hit hook).
            for col in cat_uncached:
                refreshed = await ensure_column_sample_fresh(
                    model=model, column=col,
                    engine=engine, storage=storage,
                )
                # On any failure (profile raise / None / persist raise)
                # the helper returns the INPUT column. Legacy ``sampled``
                # text on the input still feeds the markdown cell — the
                # pre-pass above has already populated
                # ``profile_by_name[col.name]`` from ``col.sampled``,
                # so we only overwrite when we actually have something
                # fresher (avoids clobbering the legacy fallback with
                # ``None`` and producing an empty cell).
                if refreshed.sampled is not None:
                    profile_by_name[col.name] = refreshed.sampled
                profile_values_by_name[col.name] = refreshed.sampled_values
                distinct_count_by_name[col.name] = refreshed.distinct_count

            # Numeric/temporal: one batched min/max query for all of
            # them at once (restores the pre-DEV-1480 batching for
            # wide models).
            if num_uncached:
                num_entries = await _profile_numeric_temporal_columns(
                    model=model, columns=num_uncached, engine=engine,
                )
                for col in num_uncached:
                    entry = num_entries.get(col.name)
                    if entry is None:
                        continue
                    if entry.min_value is None and entry.max_value is None:
                        continue
                    sampled_text = f"{entry.min_value} .. {entry.max_value}"
                    profile_by_name[col.name] = sampled_text
                    # Numeric/temporal columns carry no structured list
                    # and no distinct_count per the DEV-1480 contract.
                    profile_values_by_name[col.name] = None
                    distinct_count_by_name[col.name] = None
                    await _persist_sample(
                        col_name=col.name,
                        sampled=sampled_text,
                        sampled_values=None,
                        distinct_count=None,
                    )
            measure_profile = await _collect_measure_profile(model=model, engine=engine)
            # Persist any measure-side (numeric/temporal) profile
            # values to ``Column.sampled`` so subsequent
            # ``inspect_model`` / search calls hit the cache
            # instead of re-running the live profile query.
            for col in uncached_columns:
                sampled_value = measure_profile.get(col.name)
                if sampled_value is None or col.name in profile_by_name:
                    # Either no measure-side value for this column
                    # (already covered by dim profile above), or
                    # the dim profile already won the cache slot.
                    continue
                profile_by_name[col.name] = sampled_value
                try:
                    await storage.update_column_sampled(
                        data_source=model.data_source,
                        model_name=model.name,
                        column_name=col.name,
                        sampled=sampled_value,
                        sampled_values=None,
                        distinct_count=None,
                    )
                except Exception as exc:
                    logger.warning(
                        "inspect_model: failed to persist sampled value for "
                        "%s.%s.%s: %s",
                        model.data_source, model.name, col.name, exc,
                    )

    # ``measure_types`` informs the sample query's choice of avg vs
    # count_distinct. Only needed when ``samples`` is in the included set.
    measure_types: dict[str, str] = {}
    if engine is not None and "samples" in included_set:
        measure_types = await engine.get_column_types(
            model_name=model.name,
            data_source=model.data_source or None,
        )

    # ------------------------------------------------------------------
    # Columns section
    # ------------------------------------------------------------------
    visible_columns = [c for c in model.columns if not c.hidden]
    if "columns" in included_set:
        col_rows: list[dict[str, Any]] = []
        for c in visible_columns:
            aggs = ", ".join(c.allowed_aggregations) if c.allowed_aggregations else "all"
            # DEV-1480: key-presence check (not ``or`` truthiness) so an
            # all-NULL categorical column's ``sampled=""`` doesn't
            # silently fall through to the measure_profile fallback's
            # ``"all NULL"`` text.
            if c.name in profile_by_name:
                sampled_cell = profile_by_name[c.name]
            else:
                sampled_cell = measure_profile.get(c.name)
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
                "sampled": sampled_cell,
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
        measure_rows: list[dict[str, Any]] = []
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
            agg_rows: list[dict[str, Any]] = []
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
        join_rows: list[dict[str, Any]] = []
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
    # Sample data (fully omitted when not in sections / no engine)
    # ------------------------------------------------------------------
    sample_sql: str | None = None
    sample_data: dict[str, Any] | None = None
    sample_error: str | None = None
    if engine is not None and "samples" in included_set:
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
            sample_section = f"## Data Profile\n\n{sample_result.to_markdown()}"
            if show_sql and sample_sql:
                sample_section = (
                    f"## Data Profile SQL\n\n```sql\n{sample_sql}\n```\n\n"
                    + sample_section
                )
            out_sections.append(sample_section)
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                err = _friendly_db_error(e)
            else:
                err = str(e)
            sample_error = err
            sample_section = f"## Data Profile\n\n_Error fetching data profile: {err}_"
            if show_sql and sample_sql:
                sample_section = (
                    f"## Data Profile SQL\n\n```sql\n{sample_sql}\n```\n\n"
                    + sample_section
                )
            out_sections.append(sample_section)

    # ------------------------------------------------------------------
    # Learnings (DEV-1357 v2) — surfaces only memories where ``query`` is
    # ``None``; query-bearing memories are recall-only. Auto-pruned when
    # no learning-shaped memory matches.
    # ------------------------------------------------------------------
    relevant_learnings: list[Any] = []
    wanted: list[str] = []
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
                # DEV-1549: compact mode emits Memory.description (or
                # the first-paragraph fallback computed from learning);
                # verbose dumps the full learning body.
                if compact:
                    body = (
                        memory.description
                        if memory.description
                        else compact_description_from_learning(memory.learning)
                    )
                else:
                    body = memory.learning
                lines.append(
                    f"- **M{memory.id}** ({matched_md}): {body}"
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
        payload: dict[str, Any] = {
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
            col_payloads: list[dict[str, Any]] = []
            for c in visible_columns:
                # DEV-1480 key-presence (not ``or`` truthiness) so empty
                # string ``sampled=""`` (all-NULL categorical) survives.
                if c.name in profile_by_name:
                    sampled_cell = profile_by_name[c.name]
                else:
                    sampled_cell = measure_profile.get(c.name)
                col_payloads.append({
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
                    "sampled": sampled_cell,
                    # DEV-1480: structured top-50 list + true cardinality,
                    # surfaced only in the JSON shape (the markdown table
                    # text format is unchanged per the issue).
                    "sampled_values": profile_values_by_name.get(c.name),
                    "distinct_count": distinct_count_by_name.get(c.name),
                })
            payload["columns"] = col_payloads
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
            # DEV-1549: compact JSON Learnings drops ``learning`` and
            # surfaces ``description`` (Memory.description or the
            # first-paragraph fallback). Verbose JSON keeps the full
            # learning key as today.
            if compact:
                payload["learnings"] = [
                    {
                        "id": memory.id,
                        "description": (
                            memory.description
                            if memory.description
                            else compact_description_from_learning(
                                memory.learning,
                            )
                        ),
                        "matched_entities": sorted(
                            set(wanted) & set(memory.entities)
                        ),
                    }
                    for memory in relevant_learnings
                ]
            else:
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
