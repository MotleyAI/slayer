"""In-memory tantivy index over memories + searchable entities (DEV-1375).

Schema (per ``tests/test_search_index.py``):

* ``id`` — raw exact-match: ``"memory:<int>"`` for memories, the
  canonical entity string otherwise. Used internally for hit
  identification.
* ``kind`` — raw exact-match: ``"memory"`` / ``"datasource"`` / ``"model"``
  / ``"column"`` / ``"measure"`` / ``"aggregation"``.
* ``canonical`` — raw exact-match. Same value as ``id`` for entities;
  for memories, the stringified memory id. Lets agents search the
  literal canonical string and get the doc back.
* ``text`` — analyzed with tantivy's ``en_stem`` (Porter stemmer + default
  tokenizer, splits on punctuation including ``.`` and ``_``). Holds the
  rendered text from ``slayer.search.render``.

The index is rebuilt fresh per ``search`` call (no persistence in v1).
"""

from __future__ import annotations

from typing import List, Optional

import tantivy
from pydantic import BaseModel, ConfigDict

from slayer.core.models import SlayerModel
from slayer.memories.models import Memory
from slayer.search.render import (
    render_aggregation_text,
    render_column_text,
    render_datasource_text,
    render_measure_text,
    render_memory_text,
    render_model_text,
)


# ---------------------------------------------------------------------------
# Hit shape
# ---------------------------------------------------------------------------


class IndexHit(BaseModel):
    """One result from ``search_index``. Type-discriminated by ``kind``."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: str
    kind: str
    canonical: str
    text: str
    score: float
    memory_id: Optional[int] = None  # populated only when kind == "memory"


# ---------------------------------------------------------------------------
# Schema + index construction
# ---------------------------------------------------------------------------


def _build_schema() -> tantivy.Schema:
    builder = tantivy.SchemaBuilder()
    builder.add_text_field("id", stored=True, tokenizer_name="raw")
    builder.add_text_field("kind", stored=True, tokenizer_name="raw")
    builder.add_text_field("canonical", stored=True, tokenizer_name="raw")
    builder.add_text_field("text", stored=True, tokenizer_name="en_stem")
    return builder.build()


def _add_doc(
    *,
    writer: "tantivy.IndexWriter",
    doc_id: str,
    kind: str,
    canonical: str,
    text: str,
) -> None:
    doc = tantivy.Document()
    doc.add_text("id", doc_id)
    doc.add_text("kind", kind)
    doc.add_text("canonical", canonical)
    doc.add_text("text", text)
    writer.add_document(doc)


def _add_datasource_docs(
    *, writer, datasources: List[str], visible_models: List[SlayerModel],
) -> None:
    """One datasource doc per datasource, with mentions of its visible models."""
    models_by_ds: dict[str, List[SlayerModel]] = {}
    for m in visible_models:
        models_by_ds.setdefault(m.data_source, []).append(m)
    for ds in datasources:
        ds_models = models_by_ds.get(ds, [])
        _add_doc(
            writer=writer, doc_id=ds, kind="datasource",
            canonical=ds,
            text=render_datasource_text(name=ds, models=ds_models),
        )


def _add_model_subtree_docs(*, writer, model: SlayerModel) -> None:
    """Model doc + per-child docs (columns, measures, aggregations)."""
    model_canonical = f"{model.data_source}.{model.name}"
    _add_doc(
        writer=writer, doc_id=model_canonical, kind="model",
        canonical=model_canonical, text=render_model_text(model=model),
    )
    for column in model.columns:
        if column.hidden:
            continue
        col_canonical = f"{model_canonical}.{column.name}"
        _add_doc(
            writer=writer, doc_id=col_canonical, kind="column",
            canonical=col_canonical,
            text=render_column_text(model=model, column=column),
        )
    for measure in model.measures:
        if measure.name is None:
            continue
        measure_canonical = f"{model_canonical}.{measure.name}"
        _add_doc(
            writer=writer, doc_id=measure_canonical, kind="measure",
            canonical=measure_canonical,
            text=render_measure_text(model=model, measure=measure),
        )
    for aggregation in model.aggregations:
        agg_canonical = f"{model_canonical}.{aggregation.name}"
        _add_doc(
            writer=writer, doc_id=agg_canonical, kind="aggregation",
            canonical=agg_canonical,
            text=render_aggregation_text(model=model, aggregation=aggregation),
        )


def _add_memory_docs(*, writer, memories: List[Memory]) -> None:
    for memory in memories:
        _add_doc(
            writer=writer, doc_id=f"memory:{memory.id}", kind="memory",
            canonical=str(memory.id),
            text=render_memory_text(memory=memory),
        )


def build_in_memory_index(
    *,
    memories: List[Memory],
    models: List[SlayerModel],
    datasources: List[str],
) -> tantivy.Index:
    """Build a fresh in-RAM tantivy index covering the corpus.

    Hidden models and hidden columns are skipped entirely. The caller is
    expected to pass datasource names + every model in scope; this
    function does *not* call into storage.
    """
    schema = _build_schema()
    index = tantivy.Index(schema=schema)
    writer = index.writer()

    # Hidden models must not leak into the datasource doc either —
    # otherwise a query against a hidden model's name surfaces the parent
    # datasource and breaks the contract.
    visible_models = [m for m in models if not m.hidden]
    _add_datasource_docs(
        writer=writer, datasources=datasources, visible_models=visible_models,
    )
    for model in visible_models:
        _add_model_subtree_docs(writer=writer, model=model)
    _add_memory_docs(writer=writer, memories=memories)

    writer.commit()
    index.reload()
    return index


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


def search_index(
    *,
    index: tantivy.Index,
    question: str,
    limit: int = 20,
    fields: Optional[List[str]] = None,
) -> List[IndexHit]:
    """Run a tantivy query against ``index``.

    Args:
        index: The index built by :func:`build_in_memory_index`.
        question: The query text (parsed by tantivy's default query parser
            against ``fields``).
        limit: Max hits to return.
        fields: Which schema fields to query against (default: ``["text"]``).
            Pass ``["canonical"]`` for an exact-match canonical lookup.

    Returns:
        List of :class:`IndexHit` in score-desc order.
    """
    if not question or not question.strip():
        return []
    if fields is None:
        fields = ["text"]
    try:
        query = index.parse_query(question, fields)
    except (ValueError, RuntimeError):
        return []
    searcher = index.searcher()
    raw_hits = searcher.search(query, limit).hits
    out: List[IndexHit] = []
    for score, address in raw_hits:
        doc = searcher.doc(address)
        kind = str(doc.get_first("kind"))
        canonical = str(doc.get_first("canonical"))
        memory_id: Optional[int] = None
        if kind == "memory":
            try:
                memory_id = int(canonical)
            except (TypeError, ValueError):
                memory_id = None
        out.append(IndexHit(
            id=str(doc.get_first("id")),
            kind=kind,
            canonical=canonical,
            text=str(doc.get_first("text")),
            score=float(score),
            memory_id=memory_id,
        ))
    return out
