"""Model-render core shared by the ``inspect`` surfaces.

Must NOT import ``slayer.mcp`` — ``mcp/server.py`` imports from here (circular).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from typing import Any

from sqlalchemy.exc import DatabaseError, OperationalError

from slayer.core.enums import DataType
from slayer.core.relationship_edges import edge_reference
from slayer.core.relationship_walker import OrientedRelationship, canonical_token, neighbors
from slayer.core.models import DatasetField, SlayerDataset, field_type_text, is_identifier
from slayer.core.query import (
    SlayerQuery,
    extract_dataset_variables,
    extension_variable_refs,
    extract_placeholder_names,
)
from slayer.live_schema.ingestion import _friendly_db_error
from slayer.engine.profiling import ensure_samples_fresh
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database.client import ResultColumnType
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures

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
_INSPECT_SECTIONS_NAMES_ONLY = ("fields", "metrics", "aggregations", "relationships")
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
        lines.append("> Re-call inspect with `sections=[...]` to fetch.")
    return "\n".join(lines) if lines else None


def _markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> str:
    """Render rows as a GFM table, pruning all-empty columns.

    A single surviving column renders as a comma-separated code-span list; none as ``_(none)_``.
    """
    if not rows:
        return _NONE_PLACEHOLDER

    kept = [c for c in fields if any(_cell_is_present(r.get(c)) for r in rows)]
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


def _render_field_type(field: DatasetField) -> str:
    """``type`` cell; opaque columns get their raw DB type and a not-queryable marker, a list its operations."""
    if not field.datatype.is_opaque:
        return field_type_text(field)
    detail = field.db_type or "unrecognized DB type"
    return f"{field.datatype} ({detail}; not queryable)"


def _choose_sample_dims(
    dataset: SlayerDataset,
) -> tuple[list[dict[str, str]], set]:
    """Up to two visible categorical non-identifier columns to group the sample by (not also aggregated)."""
    dims: list[dict[str, str]] = []
    dim_names: set = set()
    for c in dataset.fields:
        if c.hidden or is_identifier(field=c, fields=dataset.fields):
            continue
        # Also excludes opaque columns, which can't be GROUP BY'd.
        if c.datatype not in (DataType.TEXT, DataType.BOOLEAN):
            continue
        dims.append({"name": c.name})
        dim_names.add(c.name)
        if len(dims) >= 2:
            break
    return dims, dim_names


def _choose_sample_agg(
    field: DatasetField,
    *,
    metric_types: dict[str, ResultColumnType],
) -> str | None:
    """Pick a sample aggregation for ``field``, or ``None`` to skip it.

    Restricted aggs without ``avg`` → first zero-arg-safe one, else the first allowed (intentional).
    Opaque columns are skipped: no equality operator, so they'd sink the whole profile query.
    """
    if field.datatype.is_opaque:
        return None
    if field.datatype is DataType.ARRAY:
        return "count"
    allowed = field.allowed_aggregations
    if allowed is not None and "avg" not in allowed:
        if not allowed:
            return None
        safe = next((a for a in allowed if a in _SAFE_SAMPLE_AGGS), None)
        return safe if safe else allowed[0]
    inferred = metric_types.get(field.name)
    if inferred is not None and inferred.type not in (DataType.INT, DataType.DOUBLE):
        return "count_distinct"
    if field.datatype not in (DataType.INT, DataType.DOUBLE):
        return "count_distinct"
    return "avg"


def _build_sample_query_args(
    dataset: SlayerDataset,
    num_rows: int,
    metric_types: dict[str, ResultColumnType] | None = None,
) -> dict[str, Any]:
    """Sample-data query: ``count(*)`` plus one aggregation per visible ungrouped non-identifier column."""
    metric_types = metric_types or {}
    dims, dim_names = _choose_sample_dims(dataset)

    metrics: list[dict[str, str]] = [{"expression": "count(*)"}]
    for c in dataset.fields:
        if c.hidden or is_identifier(field=c, fields=dataset.fields) or c.name in dim_names:
            continue
        agg = _choose_sample_agg(c, metric_types=metric_types)
        if agg is None:
            continue
        metrics.append({"expression": f"{agg}({c.name})"})

    return {
        "population": dataset.name,
        "metrics": metrics,
        "dimensions": dims,
        "limit": num_rows,
    }


def _strip_dataset_prefix(
    fields: list[str],
    data: list[dict[str, Any]],
    dataset_name: str,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Drop the redundant ``{model_name}.`` prefix from sample-data column keys."""
    prefix = f"{dataset_name}."

    def _strip(key: str) -> str:
        return key[len(prefix):] if key.startswith(prefix) else key

    new_cols = [_strip(c) for c in fields]
    new_data = [{_strip(k): v for k, v in row.items()} for row in data]
    return new_cols, new_data


async def _get_row_count(
    dataset: SlayerDataset, engine: SlayerQueryEngine,
) -> int | None:
    """Row count via a bare ``count(*)`` query, or ``None`` on any failure.

    Read positionally: the no-dimensions column is named ``{model}._count``, not ``{model}.count``.
    """
    try:
        q = SlayerQuery.model_validate({
            "population": dataset.name,
            "metrics": [{"expression": "count(*)"}],
        })
        r = await engine.execute(query=q, data_source=dataset.data_source or None)
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


def _build_backing_query_info(dataset: SlayerDataset) -> dict | None:
    """``{variables, required_variables, stages}`` for a query-backed model, else ``None``."""
    if not dataset.source_queries:
        return None
    all_placeholders: set = set()
    stage_dicts: list[dict] = []
    # Required = no default in model.query_variables nor in the stage's own variables.
    defaulted: set = set(dataset.query_variables.keys())
    for q in dataset.source_queries:
        all_placeholders |= extract_placeholder_names(q) | extension_variable_refs(q)
        if q.variables:
            defaulted |= set(q.variables.keys())
        stage_dicts.append(q.model_dump(mode="json", exclude_none=True))
    required = sorted(all_placeholders - defaulted)
    return {
        "variables": dict(dataset.query_variables),
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
    formula = v.get("expression")
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
    if key == "filter":
        return "; ".join(f"`{f}`" for f in val)
    return "; ".join(_render_field_value(v) for v in val)


def _render_source_dataset(src: Any) -> str | None:
    """Render a stage's ``source_model``."""
    return f"- source_model: `{src}`" if isinstance(src, str) else None


def _render_extensions(extensions: Any) -> str | None:
    """Render a stage's ``extensions`` as the models they extend."""
    names = [e["source_name"] for e in extensions or [] if isinstance(e, dict) and e.get("source_name")]
    return f"- extensions: {', '.join(f'`{n}`' for n in names)}" if names else None


def _render_stage(i: int, stage: dict, total: int) -> list[str]:
    """Render one stage's markdown lines."""
    title = stage.get("name") or ("final" if i == total else f"stage {i}")
    out: list[str] = [f"\n**{i}. {title}**"]
    for line in (_render_source_dataset(stage.get("population")), _render_extensions(stage.get("extensions"))):
        if line:
            out.append(line)
    for key in ("dimensions", "time_dimensions", "metrics", "filter"):
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


def _source_type_for(dataset: SlayerDataset) -> str:
    """Classify a model's source mode for summary/inspect output."""
    if dataset.source_queries:
        return "query"
    if dataset.sql_table:
        return "table"
    if dataset.sql:
        return "sql"
    return "unknown"


async def load_visible_datasets(
    storage: StorageBackend, ds_name: str, *, failures: DocumentLoadFailures | None = None,
) -> list[SlayerDataset]:
    """Visible, name-sorted models of one datasource; unloadable models are skipped with a warning."""
    loaded = (failures or DocumentLoadFailures()).skip(await storage.load_datasets(data_source=ds_name))
    return sorted((m for m in loaded if not m.hidden), key=lambda m: m.name)


def saved_queries_index(
    datasets: Iterable[SlayerDataset], *, max_chars: int | None,
) -> dict[str, list[dict[str, Any]]]:
    """Model name → the visible saved queries (``name``, ``description``) any of whose stages read it."""
    index: dict[str, list[dict[str, Any]]] = {}
    for m in datasets:
        if m.hidden or not m.source_queries:
            continue
        # Stage names are query-local, so a source naming one is not a model.
        local = {s.name for s in m.source_queries if s.name} | {m.name}
        for target in dict.fromkeys(s.population_name for s in m.source_queries):
            if target is not None and target not in local:
                index.setdefault(target, []).append(
                    {"name": m.name, "description": _truncate_description(m.description, max_chars)},
                )
    return index


# Model schema skeleton
def dataset_skeleton_fields(
    *,
    dataset: SlayerDataset,
    max_chars: int | None = None,
    saved_queries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Cheap, DB-free structured skeleton of a model.

    ``canonical_id`` falls back to the bare name when ``data_source`` is unset.
    """
    canonical_id = (
        f"{dataset.data_source}.{dataset.name}" if dataset.data_source else dataset.name
    )
    mv = extract_dataset_variables(dataset)
    fields: dict[str, Any] = {
        "name": dataset.name,
        "canonical_id": canonical_id,
        "description": _truncate_description(dataset.description, max_chars),
        "field_names": [c.name for c in dataset.fields if not c.hidden],
        "metric_names": [m.name for m in dataset.metrics if m.name is not None],
        "aggregation_names": [a.name for a in dataset.aggregations],
        "relationships_to": sorted({edge_reference(dataset=dataset, relationship=j) for j in dataset.relationships}),
        "variables": {"required": mv.required, "optional": mv.optional},
    }
    if saved_queries:
        fields["saved_queries"] = saved_queries
    return fields


def _skeleton_csv(names: list[str]) -> str:
    return ", ".join(names) if names else _NONE_PLACEHOLDER


def render_dataset_skeleton(
    *,
    dataset: SlayerDataset,
    max_chars: int | None = None,
    saved_queries: list[dict[str, Any]] | None = None,
) -> str:
    """Heading-less, DB-free markdown schema skeleton; the caller prepends the heading."""
    fields = dataset_skeleton_fields(dataset=dataset, max_chars=max_chars, saved_queries=saved_queries)
    lines: list[str] = []
    if fields["description"]:
        lines.append(fields["description"])
    lines.append(f"Fields: {_skeleton_csv(fields['field_names'])}")
    lines.append(f"Metrics: {_skeleton_csv(fields['metric_names'])}")
    lines.append(f"Aggregations: {_skeleton_csv(fields['aggregation_names'])}")
    lines.append(f"Relationships to: {_skeleton_csv(fields['relationships_to'])}")
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
    dataset: SlayerDataset, storage: StorageBackend, *, failures: DocumentLoadFailures,
) -> list[OrientedRelationship]:
    """Declared outgoing plus peer-declared incoming hops, oriented from ``dataset``; unloadable peers skipped."""
    peers = failures.skip(await storage.load_datasets(data_source=dataset.data_source, exclude=dataset.name))
    return neighbors(dataset=dataset, datasets_by_name={**{m.name: m for m in peers}, dataset.name: dataset})


async def render_dataset_inspection(  # NOSONAR(S3776) — the section-gating + cache-miss + dual markdown/json render is intentionally a single linear pass
    *,
    dataset: SlayerDataset,
    storage: StorageBackend,
    engine: SlayerQueryEngine | None,
    num_rows: int = 3,
    show_sql: bool = False,
    format: str = "markdown",
    sections: list[str] | None = None,
    descriptions_max_chars: int | None = None,
) -> str:
    """Render a complete-yet-compact view of an already-resolved model.

    ``engine=None`` skips the DB-hitting blocks (row count, profiling, sample data).
    """
    fmt = format.lower().strip()
    if fmt not in ("markdown", "json"):
        raise ValueError(
            f"Invalid format '{format}'. Must be 'markdown' or 'json'."
        )
    failures = DocumentLoadFailures()
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

    truncated_model_desc = _truncate_description(dataset.description, descriptions_max_chars)
    out_sections: list[str] = [f"# Model: `{dataset.name}`"]
    if truncated_model_desc:
        out_sections.append(truncated_model_desc)

    meta: list[str] = []
    if dataset.data_source:
        meta.append(f"- **data_source:** `{dataset.data_source}`")
    if dataset.sql_table:
        meta.append(f"- **sql_table:** `{dataset.sql_table}`")
    if dataset.default_time_dimension:
        meta.append(
            f"- **default_time_dimension:** `{dataset.default_time_dimension}`"
        )
    if dataset.hidden:
        meta.append("- **hidden:** true")
    if dataset.access_tags:
        meta.append(f"- **access_tags:** {', '.join(f'`{t}`' for t in dataset.access_tags)}")
    if dataset.meta is not None:
        meta.append(f"- **meta:** {json.dumps(dataset.meta, sort_keys=True, default=str)}")
    row_count: int | None = None
    if engine is not None:
        row_count = await _get_row_count(dataset=dataset, engine=engine)
    if row_count is not None:
        meta.append(f"- **row_count:** {row_count:,}")
    if meta:
        out_sections.append("\n".join(meta))

    if show_sql and dataset.sql:
        out_sections.append(f"## SQL\n\n```sql\n{dataset.sql}\n```")

    if show_sql and dataset.filters:
        filter_lines = "\n".join(f"- `{f}`" for f in dataset.filters)
        out_sections.append(f"## Filters (model-level)\n\n{filter_lines}")

    # Backing-query structure is always on; only its SQL is gated by show_sql.
    backing_info = _build_backing_query_info(dataset)
    if backing_info is not None:
        out_sections.append(_backing_query_markdown_section(backing_info))

    # Rendered samples come only from the profiling owner's returned columns.
    sampled_by_name: dict[str, DatasetField] = {}
    if engine is not None and "fields" in included_set:
        outcome = await ensure_samples_fresh(
            dataset=dataset, fields=list(dataset.fields), engine=engine, storage=storage,
        )
        sampled_by_name = {
            c.name: c for c in outcome.columns
            if not c.hidden and not is_identifier(field=c, fields=dataset.fields)
        }

    # Informs the sample query's avg vs count_distinct choice.
    metric_types: dict[str, ResultColumnType] = {}
    if engine is not None and "samples" in included_set:
        metric_types = await engine.get_result_column_types(
            dataset_name=dataset.name,
            data_source=dataset.data_source or None,
        )

    visible_columns = [c for c in dataset.fields if not c.hidden]
    if "fields" in included_set:
        col_rows: list[dict[str, Any]] = []
        for c in visible_columns:
            aggs = ", ".join(c.allowed_aggregations) if c.allowed_aggregations else "all"
            sampled_col = sampled_by_name.get(c.name)
            col_rows.append({
                "name": c.name,
                "datatype": _render_field_type(c),
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
            "name", "datatype", "primary_key", "unique", "sql", "allowed_aggregations",
            "filter", "label", "description", "meta", "sampled",
        ]
        if not show_sql:
            col_columns = [c for c in col_columns if c not in ("sql", "filter")]
        out_sections.append(
            f"## Columns ({len(col_rows)})\n\n"
            + _markdown_table(rows=col_rows, fields=col_columns)
        )
    elif visible_columns:
        csv = ", ".join(_md_code_span(c.name) for c in visible_columns)
        out_sections.append(
            f"## Columns ({len(visible_columns)} — names only)\n\n{csv}"
        )

    if "metrics" in included_set:
        measure_rows: list[dict[str, Any]] = []
        for mm in dataset.metrics:
            measure_rows.append({
                "name": mm.name,
                "expression": mm.expression,
                "label": mm.label,
                "description": _truncate_description(mm.description, descriptions_max_chars),
                "meta": _format_meta(mm.meta),
            })
        out_sections.append(
            f"## Measures ({len(measure_rows)})\n\n"
            + _markdown_table(
                rows=measure_rows,
                fields=["name", "expression", "label", "description", "meta"],
            )
        )
    elif dataset.metrics:
        csv = ", ".join(_md_code_span(mm.name) for mm in dataset.metrics)
        out_sections.append(
            f"## Measures ({len(dataset.metrics)} — names only)\n\n{csv}"
        )

    if "aggregations" in included_set:
        if dataset.aggregations:
            agg_rows: list[dict[str, Any]] = []
            for a in dataset.aggregations:
                if a.params:
                    if show_sql:
                        params = "; ".join(f"{p.name}={p.sql}" for p in a.params)
                    else:
                        params = ", ".join(p.name for p in a.params)
                else:
                    params = None
                agg_rows.append({
                    "name": a.name,
                    "expression": a.expression or "(built-in override)",
                    "params": params,
                    "description": _truncate_description(
                        a.description, descriptions_max_chars,
                    ),
                    "meta": _format_meta(a.meta),
                })
            agg_columns = ["name", "expression", "params", "description", "meta"]
            if not show_sql:
                agg_columns = [c for c in agg_columns if c != "expression"]
            out_sections.append(
                f"## Aggregations ({len(agg_rows)})\n\n"
                + _markdown_table(rows=agg_rows, fields=agg_columns)
            )
    elif dataset.aggregations:
        csv = ", ".join(_md_code_span(a.name) for a in dataset.aggregations)
        out_sections.append(
            f"## Aggregations ({len(dataset.aggregations)} — names only)\n\n{csv}"
        )

    hops = await _oriented_hops(dataset, storage, failures=failures)
    if "relationships" in included_set:
        join_rows: list[dict[str, Any]] = []
        for h in hops:
            pairs = "; ".join(f"{src} = {tgt}" for src, tgt in h.join_pairs)
            join_rows.append({
                "name": h.name or "",
                "to": h.to,
                "join_pairs": pairs,
                "cardinality": str(h.cardinality) if h.cardinality else "",
            })
        out_sections.append(
            f"## Joins ({len(join_rows)})\n\n"
            + _markdown_table(
                rows=join_rows,
                fields=["name", "to", "join_pairs", "cardinality"],
            )
        )
    elif hops:
        csv = ", ".join(_md_code_span(canonical_token(h)) for h in hops)
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
            dataset=dataset, num_rows=num_rows, metric_types=metric_types,
        )
        # Older models may hold ungroupable types as TEXT: retry count-only, but ONLY on that failure.
        note = ""
        try:
            try:
                sample_query = SlayerQuery.model_validate(query_args)
                sample_result = await engine.execute(
                    query=sample_query, data_source=dataset.data_source or None
                )
            except Exception as exc:
                if not _is_unsupported_grouping_error(exc):
                    raise
                minimal_args = dict(query_args)
                minimal_args["metrics"] = [{"expression": "count(*)"}]
                minimal_args["dimensions"] = []
                sample_query = SlayerQuery.model_validate(minimal_args)
                try:
                    sample_result = await engine.execute(
                        query=sample_query, data_source=dataset.data_source or None
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
            cols, data = _strip_dataset_prefix(
                fields=sample_result.columns,
                data=sample_result.data,
                dataset_name=dataset.name,
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
        ds = dataset.data_source
        wanted = [f"{ds}.{dataset.name}"]
        wanted.extend(f"{ds}.{dataset.name}.{c.name}" for c in dataset.fields)
        wanted.extend(
            f"{ds}.{dataset.name}.{m.name}"
            for m in dataset.metrics
            if m.name is not None
        )
        wanted.extend(
            f"{ds}.{dataset.name}.{a.name}" for a in dataset.aggregations
        )
        candidates = await storage.list_memories(entities=wanted, failures=failures)
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

    saved_queries: list[dict[str, Any]] = []
    if "saved_queries" in included_set:
        peers = await load_visible_datasets(storage, dataset.data_source, failures=failures)
        saved_queries = saved_queries_index(peers, max_chars=descriptions_max_chars).get(dataset.name, [])
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
            "dataset_name": dataset.name,
            "description": truncated_model_desc,
            "data_source": dataset.data_source,
            "source_type": _source_type_for(dataset),
        }
        if show_sql:
            payload["sql_table"] = dataset.sql_table
            payload["sql"] = dataset.sql
        if backing_info is not None:
            payload["backing_query"] = backing_info
        payload["default_time_dimension"] = dataset.default_time_dimension
        payload["hidden"] = dataset.hidden
        payload["access_tags"] = dataset.access_tags
        payload["meta"] = dataset.meta
        payload["row_count"] = row_count
        if show_sql:
            payload["filter"] = dataset.filters

        # Columns
        if "fields" in included_set:
            col_payloads: list[dict[str, Any]] = []
            for c in visible_columns:
                sampled_col = sampled_by_name.get(c.name)
                col_payloads.append({
                    "name": c.name,
                    "datatype": str(c.datatype),
                    **({"element_type": str(c.element_type)} if c.element_type is not None else {}),
                    # Opaque columns only (see _render_column_type).
                    **(
                        {"db_type": c.db_type, "queryable": False}
                        if c.datatype.is_opaque else {}
                    ),
                    "primary_key": c.primary_key,
                    "unique": c.unique,
                    **({"granularity": str(c.granularity)} if c.granularity is not None else {}),
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
            payload["fields"] = col_payloads
        elif visible_columns:
            payload["columns_names"] = [c.name for c in visible_columns]

        # Measures
        if "metrics" in included_set:
            payload["metrics"] = [
                {
                    "name": mm.name,
                    "expression": mm.expression,
                    "label": mm.label,
                    "description": _truncate_description(
                        mm.description, descriptions_max_chars,
                    ),
                    "meta": mm.meta,
                }
                for mm in dataset.metrics
            ]
        elif dataset.metrics:
            payload["measures_names"] = [mm.name for mm in dataset.metrics]

        # Aggregations
        if "aggregations" in included_set:
            payload["aggregations"] = [
                {
                    "name": a.name,
                    **({"expression": a.expression} if show_sql else {}),
                    "params": [
                        ({"name": p.name, "sql": p.sql} if show_sql else {"name": p.name})
                        for p in (a.params or [])
                    ],
                    "description": _truncate_description(
                        a.description, descriptions_max_chars,
                    ),
                    "meta": a.meta,
                }
                for a in dataset.aggregations
            ]
        elif dataset.aggregations:
            payload["aggregations_names"] = [a.name for a in dataset.aggregations]

        # Joins — declared outgoing plus reverse-reachable incoming, oriented.
        if "relationships" in included_set:
            payload["relationships"] = [
                {
                    "name": h.name,
                    "to": h.to,
                    "join_pairs": h.join_pairs,
                    "cardinality": h.cardinality,
                }
                for h in hops
            ]
        elif hops:
            payload["joins_names"] = [canonical_token(h) for h in hops]

        # Samples
        if "samples" in included_set:
            payload["sample_data"] = sample_data
            payload["sample_data_error"] = sample_error
            payload["sample_data_reduced"] = sample_reduced_reason is not None
            payload["sample_data_reduced_reason"] = sample_reduced_reason
            if show_sql and sample_sql:
                payload["sample_sql"] = sample_sql

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
