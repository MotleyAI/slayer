"""DEV-1667: shared renderers for the ``inspect`` collection views.

A null/omitted ``reference`` on ``inspect`` renders the *collection* at an
``entity_type``. These pure renderers are the single code path shared by the
``inspect`` collection dispatch AND the ``models_summary`` / ``list_datasources``
MCP tools (kept as thin aliases), guaranteeing byte-identical output.

``slayer.inspect`` must NOT import ``slayer.mcp`` (cycle avoidance); the shared
markdown/skeleton helpers live in ``slayer.inspect.model_render``.
"""

from __future__ import annotations

import json
from typing import Any

from slayer.core.models import SlayerModel
from slayer.inspect.model_render import (
    _markdown_table,
    _truncate_description,
    model_skeleton_fields,
)

# markdown rule separating per-datasource blocks in compact=False collections
# (same rule the DEV-1612 batch view uses between per-id blocks).
BLOCK_SEP = "\n\n---\n\n"

_NO_DATASOURCES = (
    "No datasources configured. Use create_datasource to add a database "
    "connection."
)


def _visible_column_count(model: SlayerModel) -> int:
    return sum(1 for c in model.columns if not c.hidden)


def _join_targets(model: SlayerModel) -> list[str]:
    return sorted({j.target_model for j in model.joins})


# ---------------------------------------------------------------------------
# models_summary — extracted verbatim from the MCP tool (byte-identical)
# ---------------------------------------------------------------------------


def render_models_summary(
    *,
    datasource_name: str,
    models: list[SlayerModel],
    fmt: str,
    compact: bool,
    descriptions_max_chars: int | None = None,
) -> str:
    """Render a datasource's (already hidden-filtered + name-sorted) models.

    Extracted from ``mcp/server.py::models_summary`` so both the tool and the
    ``inspect`` model collection (compact=False) render through one path. With
    ``descriptions_max_chars=None`` the output is byte-identical to the tool.
    """
    if not models:
        # An empty datasource must still emit valid JSON under fmt="json"
        # (a plain-text sentinel would break json.loads for the caller).
        if fmt == "json":
            return json.dumps(
                {"datasource_name": datasource_name, "model_count": 0, "models": []},
                indent=2,
            )
        return f"Datasource '{datasource_name}' has no models."
    desc = _desc_fn(descriptions_max_chars)
    if fmt == "json":
        return _models_summary_json(
            datasource_name=datasource_name, models=models, compact=compact,
            desc=desc,
        )
    return _models_summary_markdown(
        datasource_name=datasource_name, models=models, compact=compact,
        desc=desc,
    )


def _desc_fn(descriptions_max_chars: int | None):
    def _desc(text: str | None) -> str | None:
        return _truncate_description(text, descriptions_max_chars)
    return _desc


def _models_summary_json(
    *, datasource_name: str, models: list[SlayerModel], compact: bool, desc,
) -> str:
    if compact:
        model_payload = [
            {
                "name": m.name,
                "description": desc(m.description),
                "column_count": _visible_column_count(m),
                "measure_names": [mm.name for mm in m.measures],
                "joins_to": _join_targets(m),
            }
            for m in models
        ]
    else:
        model_payload = [
            {
                "name": m.name,
                "description": desc(m.description),
                "columns": [
                    {
                        "name": c.name,
                        "type": str(c.type),
                        "description": desc(c.description),
                    }
                    for c in m.columns if not c.hidden
                ],
                "measures": [
                    {
                        "name": mm.name,
                        "formula": mm.formula,
                        "description": desc(mm.description),
                    }
                    for mm in m.measures
                ],
                "joins_to": _join_targets(m),
            }
            for m in models
        ]
    return json.dumps(
        {
            "datasource_name": datasource_name,
            "model_count": len(models),
            "models": model_payload,
        },
        indent=2,
    )


def _models_summary_markdown(
    *, datasource_name: str, models: list[SlayerModel], compact: bool, desc,
) -> str:
    sections: list[str] = [
        f"# Datasource: `{datasource_name}` — {len(models)} model(s)"
    ]
    for m in models:
        model_lines: list[str] = [f"## `{m.name}`"]
        if m.description:
            model_lines.append(desc(m.description) or "")
        if compact:
            _append_compact_model_lines(model_lines=model_lines, m=m)
        else:
            _append_verbose_model_lines(model_lines=model_lines, m=m, desc=desc)
        sections.append("\n".join(model_lines))
    return "\n\n".join(sections)


def _append_compact_model_lines(*, model_lines: list[str], m: SlayerModel) -> None:
    model_lines.append(f"Columns: {_visible_column_count(m)}")
    measure_names = ", ".join(mm.name for mm in m.measures if mm.name is not None)
    model_lines.append(f"Measures: {measure_names}")
    if m.joins:
        rendered = ", ".join(f"`{t}`" for t in _join_targets(m))
        model_lines.append(f"Joins to: {rendered}")
    else:
        model_lines.append("Joins to: _(none)_")


def _append_verbose_model_lines(
    *, model_lines: list[str], m: SlayerModel, desc,
) -> None:
    col_rows = [
        {"name": c.name, "type": str(c.type), "description": desc(c.description)}
        for c in m.columns if not c.hidden
    ]
    model_lines.append(f"**Columns ({len(col_rows)}):**")
    model_lines.append("")
    model_lines.append(
        _markdown_table(rows=col_rows, columns=["name", "type", "description"])
    )
    model_lines.append("")

    measure_rows = [
        {"name": mm.name, "formula": mm.formula, "description": desc(mm.description)}
        for mm in m.measures
    ]
    model_lines.append(f"**Measures ({len(measure_rows)}):**")
    model_lines.append("")
    model_lines.append(
        _markdown_table(rows=measure_rows, columns=["name", "formula", "description"])
    )
    model_lines.append("")

    if m.joins:
        rendered = ", ".join(f"`{t}`" for t in _join_targets(m))
        model_lines.append(f"**Joins to:** {rendered}")
    else:
        model_lines.append("**Joins to:** _(none)_")


# ---------------------------------------------------------------------------
# Model collection — compact=True one-liner index
# ---------------------------------------------------------------------------

# A per-DS group is (data_source, models) where ``models is None`` marks a
# datasource whose config failed to load (invalid-config tolerance).
ModelGroup = tuple[str, list[SlayerModel] | None]


def render_model_oneliner_index(
    *,
    groups: list[ModelGroup],
    fmt: str,
    warnings: list[str],
) -> str:
    """The compact=True model collection: one terse line per model, grouped by
    datasource. Deliberately terser than ``models_summary`` (scales to large
    catalogs)."""
    if fmt == "json":
        return _oneliner_index_json(groups=groups, warnings=warnings)
    return _oneliner_index_markdown(groups=groups, warnings=warnings)


def _oneliner_index_json(
    *, groups: list[ModelGroup], warnings: list[str],
) -> str:
    entries: list[dict[str, Any]] = []
    for ds, models in groups:
        if models is None:
            entries.append(
                {"data_source": ds, "error": "invalid config", "models": []}
            )
            continue
        entries.append({
            "data_source": ds,
            "model_count": len(models),
            "models": [
                {
                    "name": m.name,
                    "column_count": _visible_column_count(m),
                    "joins_to": _join_targets(m),
                }
                for m in models
            ],
        })
    return json.dumps({
        "entity_type": "model",
        "collection": True,
        "datasources": entries,
        "warnings": warnings,
    }, indent=2, default=str)


def _oneliner_index_markdown(
    *, groups: list[ModelGroup], warnings: list[str],
) -> str:
    blocks: list[str] = []
    for ds, models in groups:
        if models is None:
            blocks.append(f"# Datasource: `{ds}` — (ERROR: invalid config)")
            continue
        lines = [f"# Datasource: `{ds}` — {len(models)} model(s)"]
        for m in models:
            joins = _join_targets(m)
            joins_str = (
                ", ".join(f"`{t}`" for t in joins) if joins else "_(none)_"
            )
            lines.append(
                f"- `{m.name}` ({_visible_column_count(m)} cols; "
                f"joins: {joins_str})"
            )
        blocks.append("\n".join(lines))
    body = "\n\n".join(blocks)
    if warnings:
        warn_block = "\n".join(f"> Warning: {w}" for w in warnings)
        return f"{body}\n\n{warn_block}" if body else warn_block
    return body


# ---------------------------------------------------------------------------
# Datasource collection — compact=True listing (list_datasources alias)
# ---------------------------------------------------------------------------

# A datasource pair is (name, type) where ``type is None`` marks an
# invalid-config datasource.
DatasourcePair = tuple[str, str | None]


def render_datasource_list(
    *,
    pairs: list[DatasourcePair],
    fmt: str,
    warnings: list[str] | None = None,
) -> str:
    """The compact=True datasource collection. In markdown this is byte-identical
    to the ``list_datasources`` tool (the tool delegates here)."""
    if fmt == "json":
        entries: list[dict[str, Any]] = []
        for name, ds_type in pairs:
            if ds_type is None:
                entries.append({"name": name, "error": "invalid config"})
            else:
                entries.append({"name": name, "type": ds_type})
        return json.dumps({
            "entity_type": "datasource",
            "collection": True,
            "datasources": entries,
            "warnings": warnings or [],
        }, indent=2)

    if not pairs:
        return _NO_DATASOURCES
    lines = [
        f"- {name} ({ds_type})" if ds_type is not None
        else f"- {name} (ERROR: invalid datasource config)"
        for name, ds_type in pairs
    ]
    return "\n".join(lines)


def datasource_skeleton_fields(
    *,
    name: str,
    description: str | None,
    models: list[SlayerModel],
    descriptions_max_chars: int | None,
) -> dict[str, Any]:
    """The datasource compact=False JSON per-DS element: name + description +
    per-model skeletons."""
    return {
        "name": name,
        "description": _truncate_description(description, descriptions_max_chars),
        "models": [
            model_skeleton_fields(model=m, max_chars=descriptions_max_chars)
            for m in models
        ],
    }
