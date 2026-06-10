"""Entity-text rendering for the search index (DEV-1375).

Each entity kind (datasource / model / column / measure / aggregation /
memory) gets a ``render_*_text`` helper that returns the full plain-text
content the tantivy ``text`` field is built from.

Spec rules pinned by ``tests/test_search_render.py``:

* Named children (columns / measures / aggregations / join targets) are
  mentioned by name + kind only — never with their descriptions, since
  each one has its own indexed doc.
* Non-named children (model filters, model sql, join_pairs, aggregation
  params) get their full text content included so the search-text is
  self-contained and an agent searching for a literal SQL fragment can
  find the model.
* Leaf entities include parent model + datasource name in their text so
  searches like ``"orders amount"`` surface ``orders.amount``.
* ``meta`` is excluded from indexed text in v1 — arbitrary user JSON,
  tracked as DEV-1377 follow-up.
* Hidden columns / hidden models are skipped at the *call site*; these
  helpers expect their input to already be filtered. The model renderer
  itself also filters hidden columns out of its CSV (see
  ``render_model_text``'s ``visible_columns`` filter), so a hidden
  column will never appear in the indexed text of any entity.
"""

from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel

from slayer.core.models import (
    Aggregation,
    Column,
    ModelMeasure,
    SlayerModel,
)
from slayer.memories.models import Memory


class RenderedEntity(BaseModel):
    """One (canonical_id, kind, text) triple produced by the unified
    dispatch (DEV-1513). Carries the indexed text + the entity-kind tag
    every caller needs (corpus build, embedding refresh, named-entity
    surfacing). Single source of truth for "what counts as an indexable
    entity" — filter rules (hidden model -> empty list, hidden column
    skipped, unnamed measure skipped) live in ``collect_model_entity_pairs``
    and ``render_datasource_pair`` only.

    DEV-1549: ``description`` carries the entity's structured description
    field (``None`` when the entity has none). The search service surfaces
    it as ``SearchHit.description`` under compact mode.
    """

    canonical_id: str
    kind: str
    text: str
    description: Optional[str] = None


def _named_children_csv(items: List[tuple[str, str]]) -> str:
    """Render ``[("a", "column"), ("b", "column")]`` as ``"a (column), b (column)"``."""
    return ", ".join(f"{name} ({kind})" for name, kind in items)


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------


def render_datasource_text(*, name: str, models: List[SlayerModel]) -> str:
    """Datasource doc: name + named-child mentions for each model.

    No model descriptions — each model has its own indexed doc.
    """
    lines: List[str] = [f"Datasource: {name}"]
    visible = [m for m in models if not m.hidden]
    if visible:
        lines.append(
            "Models: " + _named_children_csv(
                [(m.name, "model") for m in visible]
            )
        )
    return "\n".join(lines)


def render_datasource_pair(
    *,
    name: str,
    models: List[SlayerModel],
    description: Optional[str] = None,
) -> RenderedEntity:
    """Unified dispatch (DEV-1513) for the datasource doc. Used by both
    the tantivy corpus builder and the embedding refresh path so the
    visibility filter is applied in exactly one place.

    DEV-1549: ``description`` is the datasource's free-form description
    (DatasourceConfig.description), surfaced as ``SearchHit.description``
    under compact mode. ``None`` when the datasource has none.
    """
    return RenderedEntity(
        canonical_id=name,
        kind="datasource",
        text=render_datasource_text(name=name, models=models),
        description=description,
    )


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


def render_model_text(*, model: SlayerModel) -> str:
    """Model doc: own metadata, non-named children in full, named children
    by name + kind only."""
    lines: List[str] = [
        f"Model: {model.data_source}.{model.name}",
    ]
    if model.description:
        lines.append(f"Description: {model.description}")
    if model.sql_table:
        lines.append(f"sql_table: {model.sql_table}")
    if model.sql:
        # Non-named child: the SQL block in full.
        lines.append(f"SQL block: {model.sql}")
    if model.source_queries:
        # Non-named child: stage names so a search for the stage name
        # surfaces the parent.
        stage_names = [
            getattr(s, "name", None) or "" for s in model.source_queries
        ]
        lines.append(
            "Backing query stages: "
            + ", ".join(n for n in stage_names if n)
        )
    if model.default_time_dimension:
        lines.append(f"default_time_dimension: {model.default_time_dimension}")
    if model.filters:
        # Non-named children: include each filter expression in full.
        lines.append("Filters: " + "; ".join(model.filters))
    visible_columns = [c for c in model.columns if not c.hidden]
    if visible_columns:
        lines.append(
            "Columns: " + _named_children_csv(
                [(c.name, "column") for c in visible_columns]
            )
        )
    if model.measures:
        lines.append(
            "Measures: " + _named_children_csv(
                [(m.name or "", "measure") for m in model.measures if m.name]
            )
        )
    if model.aggregations:
        lines.append(
            "Aggregations: " + _named_children_csv(
                [(a.name, "aggregation") for a in model.aggregations]
            )
        )
    if model.joins:
        # Named-child mentions (target model name + kind).
        lines.append(
            "Joins: " + _named_children_csv(
                [(j.target_model, "model") for j in model.joins]
            )
        )
        # Non-named-child: the join_pairs in full.
        for j in model.joins:
            pairs = "; ".join(f"{src}={tgt}" for src, tgt in j.join_pairs)
            lines.append(f"Join pairs to {j.target_model}: {pairs}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Column
# ---------------------------------------------------------------------------


def render_column_text(*, model: SlayerModel, column: Column) -> str:
    """Column doc: parent qualifier + per-field metadata + cached sample."""
    lines: List[str] = [
        f"Column: {model.data_source}.{model.name}.{column.name}",
        f"Type: {column.type}",
    ]
    if column.description:
        lines.append(f"Description: {column.description}")
    if column.label:
        lines.append(f"Label: {column.label}")
    if column.format:
        lines.append(f"Format: {column.format}")
    if column.allowed_aggregations:
        lines.append("Allowed aggregations: " + ", ".join(column.allowed_aggregations))
    if column.sql:
        lines.append(f"SQL: {column.sql}")
    if column.filter:
        lines.append(f"Filter: {column.filter}")
    # DEV-1516: prefer the structured ``sampled_values`` list (full top-50)
    # over the 20-truncated ``sampled`` text. ``is None`` gates the fallback
    # so an authoritative empty list (``[]``) does not re-surface a stale
    # ``sampled`` text; an empty list simply skips the line (avoids a bare
    # ``Sample values: `` trailer in the indexed text).
    if column.sampled_values is not None:
        if column.sampled_values:
            # JSON-encode the list to preserve values that contain commas
            # (e.g. ``"R$ 1,000–3,000"``) — comma-joining would re-introduce
            # the exact ambiguity that the structured ``sampled_values`` field
            # was meant to solve.
            lines.append(
                "Sample values: "
                + json.dumps(column.sampled_values, ensure_ascii=False)
            )
            # Overflow signal: render true cardinality on a follow-up line
            # only when STRICTLY greater than the values we returned. Equal
            # means we returned the entire set; emitting a hint would be
            # noise. Gated on ``sampled_values is not None`` so the legacy
            # ``"... (N distinct)"`` suffix in ``sampled`` text does not get
            # duplicated by an extra line.
            if (
                column.distinct_count is not None
                and column.distinct_count > len(column.sampled_values)
            ):
                lines.append(f"Distinct count: {column.distinct_count}")
    elif column.sampled:
        # Fallback for numeric/temporal columns (``sampled`` is a min/max
        # range, not a list) and pre-DEV-1480 legacy data where the
        # structured field was never populated.
        lines.append(f"Sample values: {column.sampled}")
    if column.primary_key:
        lines.append("Primary key: yes")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Measure
# ---------------------------------------------------------------------------


def render_measure_text(*, model: SlayerModel, measure: ModelMeasure) -> str:
    name = measure.name or ""
    lines: List[str] = [
        f"Measure: {model.data_source}.{model.name}.{name}",
        f"Formula: {measure.formula}",
    ]
    if measure.description:
        lines.append(f"Description: {measure.description}")
    if measure.label:
        lines.append(f"Label: {measure.label}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def render_aggregation_text(*, model: SlayerModel, aggregation: Aggregation) -> str:
    lines: List[str] = [
        f"Aggregation: {model.data_source}.{model.name}.{aggregation.name}",
    ]
    if aggregation.formula:
        lines.append(f"Formula: {aggregation.formula}")
    if aggregation.description:
        lines.append(f"Description: {aggregation.description}")
    if aggregation.params:
        # Non-named children: param name=sql pairs in full.
        params = "; ".join(f"{p.name}={p.sql}" for p in aggregation.params)
        lines.append(f"Params: {params}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Memory
# ---------------------------------------------------------------------------


def render_memory_text(*, memory: Memory) -> str:
    """Memory doc for tantivy: learning text + tagged canonical entities
    so the memory surfaces both via natural-language search and via
    exact-entity search."""
    lines: List[str] = [memory.learning]
    if memory.entities:
        lines.append("Tagged entities: " + ", ".join(memory.entities))
    return "\n".join(lines)


def compact_description_from_learning(learning: str) -> str:
    """DEV-1549 compact-mode fallback: take the first non-empty
    paragraph (text up to the first blank line) of ``learning`` and
    cap at 500 chars (suffix-truncated, no ellipsis).

    No special-case for ``description:`` keyword lines (the user
    explicitly rejected that during spec review).
    """
    para: List[str] = []
    started = False
    for line in learning.splitlines():
        if line.strip():
            para.append(line)
            started = True
        elif started:
            break
    return "\n".join(para)[:500]


def render_memory_text_for_embedding(*, memory: Memory) -> str:
    """Memory doc for embeddings: learning text + optional description.

    DEV-1428: entity tags are excluded so the cascade-strip path (which
    only rewrites ``entities``) does not change the embedding content
    hash. The cascade still skips embedding work for free.

    DEV-1549 (Codex#5): when ``description`` is set, append it so the
    user-supplied summary contributes to semantic recall. Cascade-strip
    only touches ``entities``, so the hash skip on tag-only mutations is
    preserved.
    """
    if memory.description:
        return f"{memory.learning}\n\n{memory.description}"
    return memory.learning


# ---------------------------------------------------------------------------
# Unified entity-pair dispatch (DEV-1513)
# ---------------------------------------------------------------------------


def collect_model_entity_pairs(*, model: SlayerModel) -> List[RenderedEntity]:
    """Walk a model's subtree (model + visible columns + named measures
    + custom aggregations) into the unified ``RenderedEntity`` shape.

    Filter rules (single source of truth):

    * Hidden model -> returns ``[]``.
    * Hidden column skipped.
    * ``ModelMeasure`` whose ``name is None`` skipped (defensive: the
      Pydantic validator already rejects unnamed measures, but the skip
      keeps the helper aligned with the documented filter set).

    Used by the tantivy corpus build, the embedding refresh path, and
    the new named-entity surfacing path. The leaf ``render_*_text``
    helpers are still the single source of truth for *what* each kind's
    text looks like; this helper is the single source of truth for
    *which* entities exist and at which canonical id."""
    if model.hidden:
        return []
    qualifier = f"{model.data_source}.{model.name}"
    out: List[RenderedEntity] = [RenderedEntity(
        canonical_id=qualifier,
        kind="model",
        text=render_model_text(model=model),
        description=model.description,
    )]
    for column in model.columns:
        if column.hidden:
            continue
        out.append(RenderedEntity(
            canonical_id=f"{qualifier}.{column.name}",
            kind="column",
            text=render_column_text(model=model, column=column),
            description=column.description,
        ))
    for measure in model.measures:
        if measure.name is None:
            continue
        out.append(RenderedEntity(
            canonical_id=f"{qualifier}.{measure.name}",
            kind="measure",
            text=render_measure_text(model=model, measure=measure),
            description=measure.description,
        ))
    for aggregation in model.aggregations:
        out.append(RenderedEntity(
            canonical_id=f"{qualifier}.{aggregation.name}",
            kind="aggregation",
            text=render_aggregation_text(model=model, aggregation=aggregation),
            description=aggregation.description,
        ))
    return out
