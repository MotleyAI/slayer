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

from typing import List

from slayer.core.models import (
    Aggregation,
    Column,
    ModelMeasure,
    SlayerModel,
)
from slayer.memories.models import Memory


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
    if column.sampled is not None:
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


def render_memory_text_for_embedding(*, memory: Memory) -> str:
    """Memory doc for embeddings: learning text ONLY.

    DEV-1428: by excluding the entity tags from the embedded text, the
    cascade-strip path (which rewrites the tag list) does not change the
    embedding content hash, so the per-memory refresh hash-skips. This
    is what lets the cascade live entirely in the storage layer with
    zero embedding cost per deleted entity.
    """
    return memory.learning
