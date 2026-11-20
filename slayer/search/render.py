"""Entity-text rendering for the search index.

Each entity kind (datasource / dataset / field / metric / aggregation /
memory) gets a ``render_*_text`` helper that returns the full plain-text
content the tantivy ``text`` field is built from.

Spec rules pinned by ``tests/test_search_render.py``:

* Named children (fields / metrics / aggregations / relationship targets) are
  mentioned by name + kind only — never with their descriptions, since
  each one has its own indexed doc.
* Non-named children (dataset filters, dataset sql, relationship keys, aggregation
  params) get their full text content included so the search-text is
  self-contained and an agent searching for a literal SQL fragment can
  find the dataset.
* Leaf entities include parent dataset + datasource name in their text so
  searches like ``"orders amount"`` surface ``orders.amount``.
* ``meta`` is excluded from indexed text in v1 — arbitrary user JSON,
  tracked as DEV-1377 follow-up.
* Hidden fields / hidden datasets are skipped at the *call site*; these
  helpers expect their input to already be filtered. The dataset renderer
  itself also filters hidden fields out of its CSV (see
  ``render_dataset_text``'s ``visible_fields`` filter), so a hidden
  field will never appear in the indexed text of any entity.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pydantic import BaseModel
from typing_extensions import deprecated

from slayer.core.field_expression import Expression, ExpressionObject, portable_text
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasetMetric,
    SlayerDataset,
    field_type_text,
)
from slayer.core.semantic_model import AIContext, ModelScopedMetric
from slayer.core.vocabulary import name_twins
from slayer.memories.models import Memory


class RenderedEntity(BaseModel):
    """One (canonical_id, kind, text) triple produced by the unified
    dispatch. Carries the indexed text + the entity-kind tag
    every caller needs (corpus build, embedding refresh, named-entity
    surfacing). Single source of truth for "what counts as an indexable
    entity" — filter rules (hidden dataset -> empty list, hidden field
    skipped, unnamed metric skipped) live in ``collect_dataset_entity_pairs``
    and ``render_datasource_pair`` only.

    ``description`` carries the entity's structured description
    field (``None`` when the entity has none). The search service surfaces
    it as ``SearchHit.description`` under compact mode.
    """

    canonical_id: str
    kind: str
    text: str
    description: str | None = None


def _named_children_csv(items: list[tuple[str, str]]) -> str:
    """Render ``[("a", "field"), ("b", "field")]`` as ``"a (field), b (field)"``."""
    return ", ".join(f"{name} ({kind})" for name, kind in items)


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------


def render_datasource_text(
    *,
    name: str,
    datasets: list[SlayerDataset],
    description: str | None = None,
) -> str:
    """Datasource doc: name + own description (when set) + named-child
    mentions for each dataset.

    No dataset descriptions are included here — each dataset has its own
    indexed doc.

    ``description`` is included so the lexical BM25 / tantivy
    channels can match terms that live only in the datasource's
    ``DatasourceConfig.description`` field, in parity with the other
    entity render helpers.
    """
    lines: list[str] = [f"Datasource: {name}"]
    if description:
        lines.append(f"Description: {description}")
    visible = [m for m in datasets if not m.hidden]
    if visible:
        lines.append(
            "Datasets: " + _named_children_csv(
                [(m.name, "dataset") for m in visible]
            )
        )
    return "\n".join(lines)


def render_datasource_pair(
    *,
    name: str,
    datasets: list[SlayerDataset],
    description: str | None = None,
) -> RenderedEntity:
    """Unified dispatch for the datasource doc. Used by both
    the tantivy corpus builder and the embedding refresh path so the
    visibility filter is applied in exactly one place.

    ``description`` is the datasource's free-form description
    (DatasourceConfig.description), surfaced as ``SearchHit.description``
    under compact mode AND woven into the indexed text so lexical /
    embedding channels can match terms that live only there.
    """
    return RenderedEntity(
        canonical_id=name,
        kind="datasource",
        text=render_datasource_text(
            name=name, datasets=datasets, description=description,
        ),
        description=description,
    )


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


def render_dataset_text(*, dataset: SlayerDataset) -> str:
    """Dataset doc: own metadata, non-named children in full, named children
    by name + kind only."""
    lines: list[str] = [
        f"Dataset: {dataset.data_source}.{dataset.name}",
    ]
    if dataset.description:
        lines.append(f"Description: {dataset.description}")
    if dataset.sql_table:
        lines.append(f"sql_table: {dataset.sql_table}")
    if dataset.sql:
        # Non-named child: the SQL block in full.
        lines.append(f"SQL block: {dataset.sql}")
    if dataset.source_queries:
        # Non-named child: stage names so a search for the stage name
        # surfaces the parent.
        stage_names = [
            getattr(s, "name", None) or "" for s in dataset.source_queries
        ]
        lines.append(
            "Backing query stages: "
            + ", ".join(n for n in stage_names if n)
        )
    if dataset.default_time_dimension:
        lines.append(f"default_time_dimension: {dataset.default_time_dimension}")
    if dataset.filters:
        # Non-named children: include each filter expression in full.
        lines.append("Filters: " + "; ".join(dataset.filters))
    visible_fields = [c for c in dataset.fields if not c.hidden]
    if visible_fields:
        lines.append(
            "Fields: " + _named_children_csv(
                [(c.name, "field") for c in visible_fields]
            )
        )
    if dataset.metrics:
        lines.append(
            "Metrics: " + _named_children_csv(
                [(m.name or "", "metric") for m in dataset.metrics if m.name]
            )
        )
    if dataset.aggregations:
        lines.append(
            "Aggregations: " + _named_children_csv(
                [(a.name, "aggregation") for a in dataset.aggregations]
            )
        )
    if dataset.relationships:
        # Named-child mentions (target dataset name + kind).
        lines.append(
            "Relationships: " + _named_children_csv(
                [(j.to, "dataset") for j in dataset.relationships]
            )
        )
        # Non-named-child: the relationship keys in full.
        for j in dataset.relationships:
            pairs = "; ".join(f"{src}={tgt}" for src, tgt in j.column_pairs)
            lines.append(f"Relationship keys to {j.to}: {pairs}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Field
# ---------------------------------------------------------------------------


def _definition_lines(field: DatasetField) -> list[str]:
    """The field's definition: portable text, each dialect entry, a deprecated ``sql``; a self-name read is omitted."""
    expression = field.expression
    portable = expression.sql if expression is not None and not isinstance(expression, str) else expression
    lines: list[str] = []
    if portable and portable.strip() != field.name:
        lines.append(f"Expression: {portable}")
    if expression is not None and not isinstance(expression, str):
        lines.extend(f"Expression ({e.dialect}): {e.expression}" for e in expression.dialects or [])
    if field.sql:
        lines.append(f"SQL: {field.sql}")
    return lines


def render_field_text(*, dataset: SlayerDataset, field: DatasetField) -> str:
    """Field doc: parent qualifier + per-field metadata + cached sample."""
    lines: list[str] = [
        f"Field: {dataset.data_source}.{dataset.name}.{field.name}",
        f"Type: {field_type_text(field)}",
    ]
    if field.description:
        lines.append(f"Description: {field.description}")
    if field.label:
        lines.append(f"Label: {field.label}")
    if field.format:
        lines.append(f"Format: {field.format}")
    if field.allowed_aggregations:
        lines.append("Allowed aggregations: " + ", ".join(field.allowed_aggregations))
    lines.extend(_definition_lines(field))
    if field.filter:
        lines.append(f"Filter: {field.filter}")
    # Prefer the structured ``sampled_values`` list (full top-50)
    # over the 20-truncated ``sampled`` text. ``is None`` gates the fallback
    # so an authoritative empty list (``[]``) does not re-surface a stale
    # ``sampled`` text; an empty list simply skips the line (avoids a bare
    # ``Sample values: `` trailer in the indexed text).
    if field.sampled_values is not None:
        if field.sampled_values:
            # JSON-encode the list to preserve values that contain commas
            # (e.g. ``"R$ 1,000–3,000"``) — comma-joining would re-introduce
            # the exact ambiguity that the structured ``sampled_values`` field
            # was meant to solve.
            lines.append(
                "Sample values: "
                + json.dumps(field.sampled_values, ensure_ascii=False)
            )
            # Overflow signal: render true cardinality on a follow-up line
            # only when STRICTLY greater than the values we returned. Equal
            # means we returned the entire set; emitting a hint would be
            # noise. Gated on ``sampled_values is not None`` so the legacy
            # ``"... (N distinct)"`` suffix in ``sampled`` text does not get
            # duplicated by an extra line.
            if (
                field.distinct_count is not None
                and field.distinct_count > len(field.sampled_values)
            ):
                lines.append(f"Distinct count: {field.distinct_count}")
    elif field.sampled:
        # Fallback for numeric/temporal fields (``sampled`` is a min/max
        # range, not a list) and legacy data where the
        # structured field was never populated.
        lines.append(f"Sample values: {field.sampled}")
    if field.primary_key:
        lines.append("Primary key: yes")
    if field.load_warning:
        lines.append(f"Load warning: {field.load_warning}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Metric
# ---------------------------------------------------------------------------


def render_metric_text(*, dataset: SlayerDataset, metric: DatasetMetric) -> str:
    name = metric.name or ""
    lines: list[str] = [
        f"Metric: {dataset.data_source}.{dataset.name}.{name}",
        f"Expression: {metric.text}",
    ]
    if metric.description:
        lines.append(f"Description: {metric.description}")
    if metric.label:
        lines.append(f"Label: {metric.label}")
    if metric.load_warning:
        lines.append(f"Load warning: {metric.load_warning}")
    return "\n".join(lines)


def expression_text(expression: Expression | None) -> str:
    """An Ossie expression as text: its portable SQL, else each dialect's entry."""
    portable = portable_text(expression)
    if portable is not None or not isinstance(expression, ExpressionObject):
        return portable or ""
    return "; ".join(f"{e.dialect}: {e.expression}" for e in expression.dialects or [])


def ai_context_text(ai_context: AIContext | None) -> str:
    """An Ossie ``ai_context`` as one line."""
    if ai_context is None or isinstance(ai_context, str):
        return ai_context or ""
    parts = [ai_context.instructions or "",
             *(f"{label}: {', '.join(items)}" for label, items in
               (("synonyms", ai_context.synonyms), ("examples", ai_context.examples)) if items)]
    return "; ".join(p for p in parts if p)


def render_model_scoped_metric_text(*, metric: ModelScopedMetric) -> str:
    lines: list[str] = [
        f"Metric: {metric.name} (model-scoped, semantic model {metric.semantic_model!r} "
        f"of datasource {metric.data_source!r})",
        f"Expression: {expression_text(metric.expression)}",
    ]
    if metric.description:
        lines.append(f"Description: {metric.description}")
    if ai := ai_context_text(metric.ai_context):
        lines.append(f"AI context: {ai}")
    if metric.load_warning:
        lines.append(f"Load warning: {metric.load_warning}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def render_aggregation_text(*, dataset: SlayerDataset, aggregation: Aggregation) -> str:
    lines: list[str] = [
        f"Aggregation: {dataset.data_source}.{dataset.name}.{aggregation.name}",
    ]
    if aggregation.expression:
        lines.append(f"Expression: {aggregation.expression}")
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
    """Memory doc for tantivy: learning text + optional description +
    tagged canonical entities so the memory surfaces both via
    natural-language search and via exact-entity search.

    ``description`` is included here so the lexical BM25 /
    tantivy channels can match terms that live only in
    ``Memory.description``. Without this, installs without the optional
    embedding extra would lose recall for the new field.
    """
    lines: list[str] = [memory.learning]
    if memory.description:
        lines.append(memory.description)
    if memory.entities:
        lines.append("Tagged entities: " + ", ".join(memory.entities))
    return "\n".join(lines)


def compact_description_from_learning(learning: str) -> str:
    """Compact-mode fallback: take the first non-empty
    paragraph (text up to the first blank line) of ``learning`` and
    cap at 500 chars (suffix-truncated, no ellipsis).

    No special-case for ``description:`` keyword lines (the user
    explicitly rejected that during spec review).
    """
    para: list[str] = []
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

    Entity tags are excluded so the cascade-strip path (which
    only rewrites ``entities``) does not change the embedding content
    hash. The cascade still skips embedding work for free.

    When ``description`` is set, append it so the
    user-supplied summary contributes to semantic recall. Cascade-strip
    only touches ``entities``, so the hash skip on tag-only mutations is
    preserved.
    """
    if memory.description:
        return f"{memory.learning}\n\n{memory.description}"
    return memory.learning


# ---------------------------------------------------------------------------
# Unified entity-pair dispatch
# ---------------------------------------------------------------------------


def collect_dataset_entity_pairs(
    *, dataset: SlayerDataset, include_hidden: bool = False
) -> list[RenderedEntity]:
    """Walk a dataset's subtree (dataset + fields + named metrics
    + custom aggregations) into the unified ``RenderedEntity`` shape.

    Filter rules (single source of truth):

    * Hidden dataset -> returns ``[]`` (unless ``include_hidden``).
    * Hidden field skipped (unless ``include_hidden``).
    * ``DatasetMetric`` whose ``name is None`` skipped (defensive: the
      Pydantic validator already rejects unnamed metrics, but the skip
      keeps the helper aligned with the documented filter set).

    ``include_hidden=True`` is the ``inspect`` escape hatch:
    it emits the hidden dataset + hidden fields so a deliberate
    point-lookup can render them. The default (``False``) preserves the
    search/index corpus behavior — hidden entities never leak.

    Used by the tantivy corpus build, the embedding refresh path, and
    the new named-entity surfacing path. The leaf ``render_*_text``
    helpers are still the single source of truth for *what* each kind's
    text looks like; this helper is the single source of truth for
    *which* entities exist and at which canonical id."""
    if dataset.hidden and not include_hidden:
        return []
    qualifier = f"{dataset.data_source}.{dataset.name}"
    out: list[RenderedEntity] = [RenderedEntity(
        canonical_id=qualifier,
        kind="dataset",
        text=render_dataset_text(dataset=dataset),
        description=dataset.description,
    )]
    for field in dataset.fields:
        if field.hidden and not include_hidden:
            continue
        out.append(RenderedEntity(
            canonical_id=f"{qualifier}.{field.name}",
            kind="field",
            text=render_field_text(dataset=dataset, field=field),
            description=field.description,
        ))
    for metric in dataset.metrics:
        if metric.name is None:
            continue
        out.append(RenderedEntity(
            canonical_id=f"{qualifier}.{metric.name}",
            kind="metric",
            text=render_metric_text(dataset=dataset, metric=metric),
            description=metric.description,
        ))
    for aggregation in dataset.aggregations:
        out.append(RenderedEntity(
            canonical_id=f"{qualifier}.{aggregation.name}",
            kind="aggregation",
            text=render_aggregation_text(dataset=dataset, aggregation=aggregation),
            description=aggregation.description,
        ))
    return out


if TYPE_CHECKING:
    @deprecated("collect_model_entity_pairs is deprecated; use collect_dataset_entity_pairs", category=None)
    def collect_model_entity_pairs(
        *, dataset: SlayerDataset, include_hidden: bool = False,
    ) -> list[RenderedEntity]: ...


__getattr__ = name_twins(
    namespace=globals(), renames={"collect_model_entity_pairs": "collect_dataset_entity_pairs"},
    arguments={"collect_model_entity_pairs": {"model": "dataset"}},
)
