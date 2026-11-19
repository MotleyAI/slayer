"""Shared renderers for the ``inspect`` collection views.

A null/omitted ``reference`` on ``inspect`` renders the *collection* at an
``entity_type``. These pure renderers are the single code path shared by the
``inspect`` collection dispatch AND the ``datasets_summary`` / ``list_datasources``
MCP tools (kept as thin aliases), guaranteeing byte-identical output.

``slayer.inspect`` must NOT import ``slayer.mcp`` (cycle avoidance); the shared
markdown/skeleton helpers live in ``slayer.inspect.dataset_render``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from slayer.core.relationship_walker import addressable_token, neighbors
from slayer.core.models import SlayerDataset, field_type_text
from slayer.core.time_spine import TIME_SPINE_DATASET, is_spine
from slayer.inspect.dataset_render import (
    _markdown_table,
    _truncate_description,
    dataset_skeleton_fields,
    saved_queries_index,
)

# markdown rule separating per-datasource blocks in compact=False collections
# (same rule the batch view uses between per-id blocks).
BLOCK_SEP = "\n\n---\n\n"

_NO_DATASOURCES = (
    "No datasources configured. Use create_datasource to add a database "
    "connection."
)


def _visible_field_count(dataset: SlayerDataset) -> int:
    return sum(1 for c in dataset.fields if not c.hidden)


def _relationship_targets(
    dataset: SlayerDataset, all_datasets: list[SlayerDataset] | None = None
) -> list[str]:
    """The hop token of every one-hop neighbour, in either direction, and each unaddressable neighbour
    marked ambiguous; with ``all_datasets`` reverse-reachable neighbours are included, else only declared
    forward targets. The spine's virtual edges are left out (its description names its wiring)."""
    if is_spine(dataset):
        return []
    datasets_by_name = {m.name: m for m in (all_datasets or [dataset])}
    datasets_by_name.setdefault(dataset.name, dataset)
    tokens: set[str] = set()
    ambiguous: set[str] = set()
    for hop in neighbors(dataset=dataset, datasets_by_name=datasets_by_name):
        if hop.to == TIME_SPINE_DATASET:
            continue
        token = addressable_token(current=dataset, edge=hop, datasets_by_name=datasets_by_name)
        if token is None:
            ambiguous.add(hop.to)
        else:
            tokens.add(token)
    return sorted(tokens) + [f"{t} (ambiguous: name the edges)" for t in sorted(ambiguous)]


async def with_relationships_as_read(
    datasets: list[SlayerDataset], engine: Any,
) -> tuple[list[SlayerDataset], dict[str, str]]:
    """``datasets`` with each query-backed one's inherited edges merged in (none without an ``engine``), and why
    a dataset's could not be derived, by dataset name."""
    if engine is None:
        return datasets, {}
    out: list[SlayerDataset] = []
    errors: dict[str, str] = {}
    for m in datasets:
        if m.source_queries:
            relationships, error = await engine.relationships_as_read(m)
            m = m.model_copy(update={"relationships": relationships})
            if error is not None:
                errors[m.name] = error
        out.append(m)
    return out, errors


def _inherited_payload(m: SlayerDataset, errors: Mapping[str, str]) -> dict[str, Any]:
    """A query-backed dataset's inherited edges with their source edges, and why they could not be derived."""
    if not m.source_queries:
        return {}
    return {
        "inherited_relationships": [
            {"name": j.name, "to": j.to,
             "inherited_from": {"dataset": o.dataset_spelling, "relationship": o.relationship}}
            for j in m.relationships if (o := j.inherited_from) is not None
        ],
        "inherited_relationships_error": errors.get(m.name),
    }


def _inherited_lines(m: SlayerDataset, errors: Mapping[str, str]) -> list[str]:
    """Markdown twin of ``_inherited_payload``."""
    if not m.source_queries:
        return []
    listed = [f"`{j.name or j.to}` (inherited from `{o.dataset_spelling}.{o.relationship}`)"
              for j in m.relationships if (o := j.inherited_from) is not None]
    lines = [f"Inherited relationships: {', '.join(listed)}"] if listed else []
    if m.name in errors:
        lines.append(f"Inherited relationships unavailable: {errors[m.name]}")
    return lines


# ---------------------------------------------------------------------------
# datasets_summary — extracted verbatim from the MCP tool (byte-identical)
# ---------------------------------------------------------------------------


def render_datasets_summary(
    *,
    datasource_name: str,
    datasets: list[SlayerDataset],
    fmt: str,
    compact: bool,
    descriptions_max_chars: int | None = None,
    inherited_errors: Mapping[str, str] | None = None,
) -> str:
    """Render a datasource's (already hidden-filtered + name-sorted) datasets.

    Extracted from ``mcp/server.py::datasets_summary`` so both the tool and the
    ``inspect`` dataset collection (compact=False) render through one path. With
    ``descriptions_max_chars=None`` the output is byte-identical to the tool.
    """
    if not datasets:
        # An empty datasource must still emit valid JSON under fmt="json"
        # (a plain-text sentinel would break json.loads for the caller).
        if fmt == "json":
            return json.dumps(
                {"datasource_name": datasource_name, "dataset_count": 0, "datasets": []},
                indent=2,
            )
        return f"Datasource '{datasource_name}' has no datasets."
    desc = _desc_fn(descriptions_max_chars)
    errors = inherited_errors or {}
    if fmt == "json":
        return _datasets_summary_json(
            datasource_name=datasource_name, datasets=datasets, compact=compact,
            desc=desc, errors=errors,
        )
    return _datasets_summary_markdown(
        datasource_name=datasource_name, datasets=datasets, compact=compact,
        desc=desc, errors=errors,
    )


def _desc_fn(descriptions_max_chars: int | None):
    def _desc(text: str | None) -> str | None:
        return _truncate_description(text, descriptions_max_chars)
    return _desc


def _datasets_summary_json(
    *, datasource_name: str, datasets: list[SlayerDataset], compact: bool, desc, errors: Mapping[str, str],
) -> str:
    if compact:
        dataset_payload = [
            {
                "name": m.name,
                "description": desc(m.description),
                "field_count": _visible_field_count(m),
                "metric_names": [mm.name for mm in m.metrics],
                "relationships_to": _relationship_targets(m, datasets),
                **_inherited_payload(m, errors),
            }
            for m in datasets
        ]
    else:
        dataset_payload = [
            {
                "name": m.name,
                "description": desc(m.description),
                "fields": [
                    {
                        "name": c.name,
                        "datatype": str(c.datatype),
                        **({"element_type": str(c.element_type)} if c.element_type is not None else {}),
                        "description": desc(c.description),
                    }
                    for c in m.fields if not c.hidden
                ],
                "metrics": [
                    {
                        "name": mm.name,
                        "expression": mm.expression,
                        "description": desc(mm.description),
                    }
                    for mm in m.metrics
                ],
                "relationships_to": _relationship_targets(m, datasets),
                **_inherited_payload(m, errors),
            }
            for m in datasets
        ]
    return json.dumps(
        {
            "datasource_name": datasource_name,
            "dataset_count": len(datasets),
            "datasets": dataset_payload,
        },
        indent=2,
    )


def _datasets_summary_markdown(
    *, datasource_name: str, datasets: list[SlayerDataset], compact: bool, desc, errors: Mapping[str, str],
) -> str:
    sections: list[str] = [
        f"# Datasource: `{datasource_name}` — {len(datasets)} dataset(s)"
    ]
    for m in datasets:
        dataset_lines: list[str] = [f"## `{m.name}`"]
        if m.description:
            dataset_lines.append(desc(m.description) or "")
        if compact:
            _append_compact_dataset_lines(
                dataset_lines=dataset_lines, m=m, all_datasets=datasets,
            )
        else:
            _append_verbose_dataset_lines(
                dataset_lines=dataset_lines, m=m, desc=desc, all_datasets=datasets,
            )
        dataset_lines.extend(_inherited_lines(m, errors))
        sections.append("\n".join(dataset_lines))
    return "\n\n".join(sections)


def _append_compact_dataset_lines(
    *, dataset_lines: list[str], m: SlayerDataset,
    all_datasets: list[SlayerDataset] | None = None,
) -> None:
    dataset_lines.append(f"Fields: {_visible_field_count(m)}")
    metric_names = ", ".join(mm.name for mm in m.metrics if mm.name is not None)
    dataset_lines.append(f"Metrics: {metric_names}")
    targets = _relationship_targets(m, all_datasets)
    if targets:
        rendered = ", ".join(f"`{t}`" for t in targets)
        dataset_lines.append(f"Relationships to: {rendered}")
    else:
        dataset_lines.append("Relationships to: _(none)_")


def _append_verbose_dataset_lines(
    *, dataset_lines: list[str], m: SlayerDataset, desc,
    all_datasets: list[SlayerDataset] | None = None,
) -> None:
    col_rows = [
        {"name": c.name, "datatype": field_type_text(c), "description": desc(c.description)}
        for c in m.fields if not c.hidden
    ]
    dataset_lines.extend([
        f"**Fields ({len(col_rows)}):**",
        "",
        _markdown_table(rows=col_rows, fields=["name", "datatype", "description"]),
        "",
    ])

    metric_rows = [
        {"name": mm.name, "expression": mm.expression, "description": desc(mm.description)}
        for mm in m.metrics
    ]
    dataset_lines.extend([
        f"**Metrics ({len(metric_rows)}):**",
        "",
        _markdown_table(rows=metric_rows, fields=["name", "expression", "description"]),
        "",
    ])

    targets = _relationship_targets(m, all_datasets)
    if targets:
        rendered = ", ".join(f"`{t}`" for t in targets)
        dataset_lines.append(f"**Relationships to:** {rendered}")
    else:
        dataset_lines.append("**Relationships to:** _(none)_")


# ---------------------------------------------------------------------------
# Dataset collection — compact=True one-liner index
# ---------------------------------------------------------------------------

# A per-DS group is (data_source, datasets) where ``datasets is None`` marks a
# datasource whose config failed to load (invalid-config tolerance).
DatasetGroup = tuple[str, list[SlayerDataset] | None]


def render_dataset_oneliner_index(
    *,
    groups: list[DatasetGroup],
    fmt: str,
    warnings: list[str],
    inherited_errors: Mapping[str, Mapping[str, str]] | None = None,
) -> str:
    """The compact=True dataset collection: one terse line per dataset, grouped by
    datasource. Deliberately terser than ``datasets_summary`` (scales to large
    catalogs)."""
    errors = inherited_errors or {}
    if fmt == "json":
        return _oneliner_index_json(groups=groups, warnings=warnings, errors=errors)
    return _oneliner_index_markdown(groups=groups, warnings=warnings, errors=errors)


def _oneliner_index_json(
    *, groups: list[DatasetGroup], warnings: list[str], errors: Mapping[str, Mapping[str, str]],
) -> str:
    entries: list[dict[str, Any]] = []
    for ds, datasets in groups:
        if datasets is None:
            entries.append(
                {"data_source": ds, "error": "invalid config", "datasets": []}
            )
            continue
        entries.append({
            "data_source": ds,
            "dataset_count": len(datasets),
            "datasets": [
                {
                    "name": m.name,
                    "field_count": _visible_field_count(m),
                    "relationships_to": _relationship_targets(m, datasets),
                    **_inherited_payload(m, errors.get(ds, {})),
                }
                for m in datasets
            ],
        })
    return json.dumps({
        "entity_type": "dataset",
        "collection": True,
        "datasources": entries,
        "warnings": warnings,
    }, indent=2, default=str)


def _oneliner_index_markdown(
    *, groups: list[DatasetGroup], warnings: list[str], errors: Mapping[str, Mapping[str, str]],
) -> str:
    blocks: list[str] = []
    for ds, datasets in groups:
        if datasets is None:
            blocks.append(f"# Datasource: `{ds}` — (ERROR: invalid config)")
            continue
        lines = [f"# Datasource: `{ds}` — {len(datasets)} dataset(s)"]
        for m in datasets:
            relationships = _relationship_targets(m, datasets)
            relationships_str = (
                ", ".join(f"`{t}`" for t in relationships) if relationships else "_(none)_"
            )
            lines.append(
                f"- `{m.name}` ({_visible_field_count(m)} fields; "
                f"relationships: {relationships_str})"
            )
            lines.extend(f"  - {line}" for line in _inherited_lines(m, errors.get(ds, {})))
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
    datasets: list[SlayerDataset],
    descriptions_max_chars: int | None,
) -> dict[str, Any]:
    """The datasource compact=False JSON per-DS element: name + description +
    per-dataset skeletons."""
    saved = saved_queries_index(datasets, max_chars=descriptions_max_chars)
    return {
        "name": name,
        "description": _truncate_description(description, descriptions_max_chars),
        "datasets": [
            dataset_skeleton_fields(
                dataset=m, max_chars=descriptions_max_chars, saved_queries=saved.get(m.name),
            )
            for m in datasets
        ],
    }
