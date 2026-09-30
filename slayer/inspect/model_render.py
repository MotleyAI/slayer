"""Model-render core shared by the ``inspect`` surfaces and the deprecated ``inspect_model`` tool.

Must NOT import ``slayer.mcp`` — ``mcp/server.py`` imports from here (circular).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from typing import Any

from sqlalchemy.exc import DatabaseError, OperationalError

from slayer.core.enums import DataType
from slayer.core.join_walker import OrientedJoin, neighbors
from slayer.core.models import Column, SlayerModel, is_identifier
from slayer.core.query import (
    SlayerQuery,
    extract_model_variables,
    extract_placeholder_names,
)
from slayer.engine.ingestion import _friendly_db_error
from slayer.engine.profiling import ensure_samples_fresh
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.search.render import compact_description_from_learning
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)

# Zero-arg aggregations needing no time-column context.
_SAFE_SAMPLE_AGGS = frozenset({"avg", "sum", "min", "max", "count", "count_distinct", "median"})

# Deliberately narrow: other failures must keep their own cause, not be relabeled as a type problem.
_UNSUPPORTED_GROUPING_SIGNATURES = (
    "could not identify an equality operator",
    "could not identify a comparison function",
)
# Postgres undefined_function, raised for the missing GROUP BY / DISTINCT operator.
_UNSUPPORTED_GROUPING_SQLSTATES = frozenset({"42883"})


def _is_unsupported_grouping_error(exc: BaseException) -> bool:
    """True when ``exc`` says the database can't group/deduplicate a column type (SQLSTATE, then message)."""
    for err in (exc, getattr(exc, "orig", None)):
        if err is None:
            continue
        code = getattr(err, "sqlstate", None) or getattr(err, "pgcode", None)
        if code in _UNSUPPORTED_GROUPING_SQLSTATES:
            return True
    text = str(exc).lower()
    orig = getattr(exc, "orig", None)
    if orig is not None:
        text = f"{text} {str(orig).lower()}"
    return any(sig in text for sig in _UNSUPPORTED_GROUPING_SIGNATURES)

# Dropped sections: these collapse to a names-only CSV, the omittable ones vanish.
_INSPECT_SECTIONS_NAMES_ONLY = ("columns", "measures", "aggregations", "joins")
_INSPECT_SECTIONS_OMITTABLE = ("samples", "learnings", "saved_queries")
_VALID_INSPECT_SECTIONS = _INSPECT_SECTIONS_NAMES_ONLY + _INSPECT_SECTIONS_OMITTABLE
_TRUNCATION_MARKER = " ... [truncated]"
_NONE_PLACEHOLDER = "_(none)_"


def _escape_md_cell(value: Any) -> str:
    """Escape a markdown table cell; ``None``/empty renders as an em-dash."""
    if value is None:
        return "—"
    s = str(value).replace("|", "\\|").replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip()
    return s if s else "—"


def _md_code_span(value: Any) -> str:
    """Wrap *value* in a CommonMark code span whose fence outruns any embedded backtick run."""
    text = str(value).replace("|", "\\|").replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return "` `"
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
    if text.startswith("`") or text.endswith("`"):
        return f"{fence} {text} {fence}"
    return f"{fence}{text}{fence}"


def _cell_is_present(value: Any) -> bool:
    """True unless ``None`` or a blank string."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _truncate_description(text: str | None, max_chars: int | None) -> str | None:
    """Trim to ``max_chars`` plus the truncation marker; ``max_chars=None`` disables."""
    if text is None or max_chars is None:
        return text
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + _TRUNCATION_MARKER


def _format_meta(meta: dict[str, Any] | None) -> str | None:
    """Compact JSON for a meta cell; ``None`` stays ``None`` so the column can be pruned."""
    if meta is None:
        return None
    return json.dumps(meta, sort_keys=True, default=str)


def _resolve_inspect_sections(
    sections: list[str] | None,
) -> tuple[list[str], list[str]]:
    """Return ``(resolved, unknown)``, ``resolved`` in canonical order.

    ``None``/``[]`` mean all sections; a list of only unknown names resolves to ``[]``
    so a typo can't trigger the full expensive payload.
    """
    if not sections:
        return list(_VALID_INSPECT_SECTIONS), []
    valid_set = {s for s in sections if s in _VALID_INSPECT_SECTIONS}
    unknown = [s for s in sections if s not in _VALID_INSPECT_SECTIONS]
    resolved = [s for s in _VALID_INSPECT_SECTIONS if s in valid_set]
    return resolved, unknown


def _render_inspect_footer(
    *,
    included: list[str],
    names_only: list[str],
    omitted: list[str],
    unknown: list[str],
) -> str | None:
    """Quoted-markdown truncation footer, or ``None`` when nothing was trimmed or unknown."""
    if not (names_only or omitted or unknown):
        return None
    lines: list[str] = []
    if unknown:
        # repr() so a caller-supplied value can't forge extra footer lines.
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
    """Render rows as a GFM table, pruning all-empty columns.

    A single surviving column renders as a comma-separated code-span list; none as ``_(none)_``.
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


def _render_column_type(column: Column) -> str:
    """``type`` cell; opaque columns get their raw DB type and a not-queryable marker."""
    if not column.type.is_opaque:
        return str(column.type)
    detail = column.db_type or "unrecognized DB type"
    return f"{column.type} ({detail}; not queryable)"


def _choose_sample_dims(
    model: SlayerModel,
) -> tuple[list[dict[str, str]], set]:
    """Up to two visible categorical non-identifier columns to group the sample by (not also aggregated)."""
    dims: list[dict[str, str]] = []
    dim_names: set = set()
    for c in model.columns:
        if c.hidden or is_identifier(column=c, columns=model.columns):
            continue
        # Also excludes opaque columns, which can't be GROUP BY'd.
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

    Restricted aggs without ``avg`` → first zero-arg-safe one, else the first allowed (intentional).
    Opaque columns are skipped: no equality operator, so they'd sink the whole profile query.
    """
    if column.type.is_opaque:
        return None
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
    """Sample-data query: ``count(*)`` plus one aggregation per visible ungrouped non-identifier column."""
    measure_types = measure_types or {}
    dims, dim_names = _choose_sample_dims(model)

    measures: list[dict[str, str]] = [{"formula": "count(*)"}]
    for c in model.columns:
        if c.hidden or is_identifier(column=c, columns=model.columns) or c.name in dim_names:
            continue
        agg = _choose_sample_agg(c, measure_types=measure_types)
        if agg is None:
            continue
        measures.append({"formula": f"{agg}({c.name})"})

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
    """Drop the redundant ``{model_name}.`` prefix from sample-data column keys."""
    prefix = f"{model_name}."

    def _strip(key: str) -> str:
        return key[len(prefix):] if key.startswith(prefix) else key

    new_cols = [_strip(c) for c in columns]
    new_data = [{_strip(k): v for k, v in row.items()} for row in data]
    return new_cols, new_data


async def _get_row_count(
    model: SlayerModel, engine: SlayerQueryEngine,
) -> int | None:
    """Row count via a bare ``count(*)`` query, or ``None`` on any failure.

    Read positionally: the no-dimensions column is named ``{model}._count``, not ``{model}.count``.
    """
    try:
        q = SlayerQuery.model_validate({
            "source_model": model.name,
            "measures": [{"formula": "count(*)"}],
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


def _build_backing_query_info(model: SlayerModel) -> dict | None:
    """``{variables, required_variables, stages}`` for a query-backed model, else ``None``."""
    if not model.source_queries:
        return None
    all_placeholders: set = set()
    stage_dicts: list[dict] = []
    # Required = no default in model.query_variables nor in the stage's own variables.
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
    """Most descriptive label of a query-stage field value (name, formula, wrapped name, else ``str``)."""
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


async def load_visible_models(storage: StorageBackend, ds_name: str | None) -> list[SlayerModel]:
    """Visible, name-sorted models of one datasource; unloadable models are skipped."""
    models: list[SlayerModel] = []
    for name in await storage.list_models(data_source=ds_name):
        try:
            m = await storage.get_model(name, data_source=ds_name)
        except Exception:  # noqa: BLE001 — one bad model must not sink the DS
            continue
        if m is not None and not m.hidden:
            models.append(m)
    models.sort(key=lambda m: m.name)
    return models


def saved_queries_index(
    models: Iterable[SlayerModel], *, max_chars: int | None,
) -> dict[str, list[dict[str, Any]]]:
    """Model name → the visible saved queries (``name``, ``description``) any of whose stages read it."""
    index: dict[str, list[dict[str, Any]]] = {}
    for m in models:
        if m.hidden or not m.source_queries:
            continue
        # Stage names are query-local, so a source naming one is not a model.
        local = {s.name for s in m.source_queries if s.name} | {m.name}
        for target in dict.fromkeys(s.source_model_name for s in m.source_queries):
            if target is not None and target not in local:
                index.setdefault(target, []).append(
                    {"name": m.name, "description": _truncate_description(m.description, max_chars)},
                )
    return index


# Model schema skeleton
def model_skeleton_fields(
    *,
    model: SlayerModel,
    max_chars: int | None = None,
    saved_queries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Cheap, DB-free structured skeleton of a model.

    ``canonical_id`` falls back to the bare name when ``data_source`` is unset.
    """
    canonical_id = (
        f"{model.data_source}.{model.name}" if model.data_source else model.name
    )
    mv = extract_model_variables(model)
    fields: dict[str, Any] = {
        "name": model.name,
        "canonical_id": canonical_id,
        "description": _truncate_description(model.description, max_chars),
        "column_names": [c.name for c in model.columns if not c.hidden],
        "measure_names": [m.name for m in model.measures if m.name is not None],
        "aggregation_names": [a.name for a in model.aggregations],
        "joins_to": sorted({j.target_model for j in model.joins}),
        "variables": {"required": mv.required, "optional": mv.optional},
    }
    if saved_queries:
        fields["saved_queries"] = saved_queries
    return fields


def _skeleton_csv(names: list[str]) -> str:
    return ", ".join(names) if names else _NONE_PLACEHOLDER


def render_model_skeleton(
    *,
    model: SlayerModel,
    max_chars: int | None = None,
    saved_queries: list[dict[str, Any]] | None = None,
) -> str:
    """Heading-less, DB-free markdown schema skeleton; the caller prepends the heading."""
    fields = model_skeleton_fields(model=model, max_chars=max_chars, saved_queries=saved_queries)
    lines: list[str] = []
    if fields["description"]:
        lines.append(fields["description"])
    lines.append(f"Columns: {_skeleton_csv(fields['column_names'])}")
    lines.append(f"Measures: {_skeleton_csv(fields['measure_names'])}")
    lines.append(f"Aggregations: {_skeleton_csv(fields['aggregation_names'])}")
    lines.append(f"Joins to: {_skeleton_csv(fields['joins_to'])}")
    var_line = _render_variables_line(fields["variables"])
    if var_line:
        lines.append(var_line)
    if saved_queries:
        lines.append(f"Saved queries: {', '.join(q['name'] for q in saved_queries)}")
    return "\n".join(lines)


def _saved_queries_markdown(saved_queries: list[dict[str, Any]]) -> str:
    lines = [f"## Saved queries ({len(saved_queries)})", ""]
    for q in saved_queries:
        lines.append(f"- `{q['name']}` — {q['description']}" if q["description"] else f"- `{q['name']}`")
    return "\n".join(lines)


def _render_variables_line(variables: dict[str, list[str]]) -> str | None:
    """``Variables:`` skeleton line, or ``None`` when the model takes no variables."""
    required = variables.get("required") or []
    optional = variables.get("optional") or []
    if not required and not optional:
        return None
    parts = [f"{name} (required)" for name in required] + list(optional)
    return f"Variables: {', '.join(parts)}"


async def _oriented_hops(
    model: SlayerModel, storage: StorageBackend
) -> list[OrientedJoin]:
    """Declared outgoing plus peer-declared incoming hops, oriented from ``model``; unloadable peers skipped."""
    models_by_name: dict[str, SlayerModel] = {model.name: model}
    try:
        peer_names = await storage.list_models(model.data_source)
    except Exception:  # best-effort — fall back to declared joins only
        peer_names = []
    for nm in peer_names:
        if nm in models_by_name:
            continue
        try:
            peer = await storage.get_model(nm, data_source=model.data_source)
        except Exception:
            peer = None
        if peer is not None:
            models_by_name[nm] = peer
    return neighbors(model=model, models_by_name=models_by_name)


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

    ``engine=None`` skips the DB-hitting blocks (row count, profiling, sample data).
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

    included, unknown = _resolve_inspect_sections(sections)
    included_set = set(included)

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

    # Backing-query structure is always on; only its SQL is gated by show_sql.
    backing_info = _build_backing_query_info(model)
    if backing_info is not None:
        out_sections.append(_backing_query_markdown_section(backing_info))
        if show_sql and model.backing_query_sql:
            out_sections.append(
                f"## Backing Query SQL\n\n```sql\n{model.backing_query_sql}\n```"
            )

    # Rendered samples come only from the profiling owner's returned columns.
    sampled_by_name: dict[str, Column] = {}
    if engine is not None and "columns" in included_set:
        outcome = await ensure_samples_fresh(
            model=model, columns=list(model.columns), engine=engine, storage=storage,
        )
        sampled_by_name = {
            c.name: c for c in outcome.columns
            if not c.hidden and not is_identifier(column=c, columns=model.columns)
        }

    # Informs the sample query's avg vs count_distinct choice.
    measure_types: dict[str, str] = {}
    if engine is not None and "samples" in included_set:
        measure_types = await engine.get_column_types(
            model_name=model.name,
            data_source=model.data_source or None,
        )

    visible_columns = [c for c in model.columns if not c.hidden]
    if "columns" in included_set:
        col_rows: list[dict[str, Any]] = []
        for c in visible_columns:
            aggs = ", ".join(c.allowed_aggregations) if c.allowed_aggregations else "all"
            sampled_col = sampled_by_name.get(c.name)
            col_rows.append({
                "name": c.name,
                "type": _render_column_type(c),
                "primary_key": "yes" if c.primary_key else "",
                "unique": "yes" if c.unique else "",
                "sql": c.sql if c.sql else c.name,
                "allowed_aggregations": aggs,
                "filter": c.filter,
                "label": c.label,
                "description": _truncate_description(c.description, descriptions_max_chars),
                "meta": _format_meta(c.meta),
                "sampled": sampled_col.sampled if sampled_col is not None else None,
            })
        col_columns = [
            "name", "type", "primary_key", "unique", "sql", "allowed_aggregations",
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

    hops = await _oriented_hops(model, storage)
    if "joins" in included_set:
        join_rows: list[dict[str, Any]] = []
        for h in hops:
            pairs = "; ".join(f"{src} = {tgt}" for src, tgt in h.join_pairs)
            join_rows.append({
                "target_model": h.target_model,
                "join_pairs": pairs,
                "cardinality": str(h.cardinality) if h.cardinality else "",
            })
        out_sections.append(
            f"## Joins ({len(join_rows)})\n\n"
            + _markdown_table(
                rows=join_rows,
                columns=["target_model", "join_pairs", "cardinality"],
            )
        )
    elif hops:
        csv = ", ".join(_md_code_span(h.target_model) for h in hops)
        out_sections.append(
            f"## Joins ({len(hops)} — names only)\n\n{csv}"
        )

    sample_sql: str | None = None
    sample_data: dict[str, Any] | None = None
    sample_error: str | None = None
    # Lets JSON callers tell a count-only fallback from a complete profile.
    sample_reduced_reason: str | None = None
    if engine is not None and "samples" in included_set:
        query_args = _build_sample_query_args(
            model=model, num_rows=num_rows, measure_types=measure_types,
        )
        # Older models may hold ungroupable types as TEXT: retry count-only, but ONLY on that failure.
        note = ""
        try:
            try:
                sample_query = SlayerQuery.model_validate(query_args)
                sample_result = await engine.execute(
                    query=sample_query, data_source=model.data_source or None
                )
            except Exception as exc:
                if not _is_unsupported_grouping_error(exc):
                    raise
                minimal_args = dict(query_args)
                minimal_args["measures"] = [{"formula": "count(*)"}]
                minimal_args["dimensions"] = []
                sample_query = SlayerQuery.model_validate(minimal_args)
                try:
                    sample_result = await engine.execute(
                        query=sample_query, data_source=model.data_source or None
                    )
                except Exception:
                    # Report the original cause, not this second failure.
                    raise exc
                sample_reduced_reason = (
                    "at least one column's type does not support the "
                    "grouping/DISTINCT this profile uses"
                )
                note = f"\n\n_Reduced to a row count: {sample_reduced_reason}._"
            sample_sql = sample_result.sql
            cols, data = _strip_model_prefix(
                columns=sample_result.columns,
                data=sample_result.data,
                model_name=model.name,
            )
            sample_data = {"columns": cols, "rows": data}
            sample_result.columns = cols
            sample_result.data = data
            sample_section = f"## Data Profile\n\n{sample_result.to_markdown()}{note}"
            if show_sql and sample_sql:
                sample_section = (
                    f"## Data Profile SQL\n\n```sql\n{sample_sql}\n```\n\n"
                    + sample_section
                )
            out_sections.append(sample_section)
        except Exception as e:
            if isinstance(e, (OperationalError, DatabaseError)):
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

    # Learnings: query-bearing memories are recall-only, so only ``query is None`` ones show.
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

    saved_queries: list[dict[str, Any]] = []
    if "saved_queries" in included_set:
        peers = await load_visible_models(storage, model.data_source)
        saved_queries = saved_queries_index(peers, max_chars=descriptions_max_chars).get(model.name, [])
        if saved_queries:
            out_sections.append(_saved_queries_markdown(saved_queries))

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
                sampled_col = sampled_by_name.get(c.name)
                col_payloads.append({
                    "name": c.name,
                    "type": str(c.type),
                    # Opaque columns only (see _render_column_type).
                    **(
                        {"db_type": c.db_type, "queryable": False}
                        if c.type.is_opaque else {}
                    ),
                    "primary_key": c.primary_key,
                    "unique": c.unique,
                    **({"granularity": c.granularity.value} if c.granularity is not None else {}),
                    **({"sql": c.sql} if show_sql else {}),
                    "allowed_aggregations": c.allowed_aggregations,
                    **({"filter": c.filter} if show_sql else {}),
                    "label": c.label,
                    "description": _truncate_description(
                        c.description, descriptions_max_chars,
                    ),
                    "meta": c.meta,
                    "sampled": sampled_col.sampled if sampled_col is not None else None,
                    # Structured top-50 + cardinality: JSON shape only.
                    "sampled_values": sampled_col.sampled_values if sampled_col is not None else None,
                    "distinct_count": sampled_col.distinct_count if sampled_col is not None else None,
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

        # Joins — declared outgoing plus reverse-reachable incoming, oriented.
        if "joins" in included_set:
            payload["joins"] = [
                {
                    "target_model": h.target_model,
                    "join_pairs": h.join_pairs,
                    "cardinality": h.cardinality,
                }
                for h in hops
            ]
        elif hops:
            payload["joins_names"] = [h.target_model for h in hops]

        # Samples
        if "samples" in included_set:
            payload["sample_data"] = sample_data
            payload["sample_data_error"] = sample_error
            payload["sample_data_reduced"] = sample_reduced_reason is not None
            payload["sample_data_reduced_reason"] = sample_reduced_reason
            if show_sql and sample_sql:
                payload["sample_sql"] = sample_sql

        if "learnings" in included_set and relevant_learnings:
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

        if saved_queries:
            payload["saved_queries"] = saved_queries

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
