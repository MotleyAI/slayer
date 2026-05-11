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

from typing import Dict, List, Optional, Tuple

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

    Returns just the tantivy index for callers that don't need the
    canonical-text lookups. ``build_in_memory_corpus`` returns both.
    """
    corpus = build_in_memory_corpus(
        memories=memories, models=models, datasources=datasources,
    )
    return corpus.index


class Corpus(BaseModel):
    """The tantivy index plus the parallel ``canonical_id → text`` and
    ``canonical_id → kind`` maps. The embedding channel (DEV-1386) uses
    the maps to recover hit text without re-rendering the entity or
    round-tripping through the raw ``canonical`` tantivy field."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    index: "tantivy.Index"
    canonical_to_text: Dict[str, str]
    canonical_to_kind: Dict[str, str]


def _render_model_subtree_pairs(
    model: SlayerModel,
) -> List[Tuple[str, str, str]]:
    """Render docs for one model: the model itself + its visible columns +
    named measures + custom aggregations. Hidden columns and unnamed
    measures are skipped to match the indexer's filter rules."""
    model_canonical = f"{model.data_source}.{model.name}"
    pairs: List[Tuple[str, str, str]] = [(
        model_canonical, "model", render_model_text(model=model),
    )]
    for column in model.columns:
        if column.hidden:
            continue
        pairs.append((
            f"{model_canonical}.{column.name}", "column",
            render_column_text(model=model, column=column),
        ))
    for measure in model.measures:
        if measure.name is None:
            continue
        pairs.append((
            f"{model_canonical}.{measure.name}", "measure",
            render_measure_text(model=model, measure=measure),
        ))
    for aggregation in model.aggregations:
        pairs.append((
            f"{model_canonical}.{aggregation.name}", "aggregation",
            render_aggregation_text(model=model, aggregation=aggregation),
        ))
    return pairs


def _collect_render_pairs(
    *,
    memories: List[Memory],
    visible_models: List[SlayerModel],
    datasources: List[str],
) -> List[Tuple[str, str, str]]:
    """Return ``[(canonical_id, kind, rendered_text), ...]`` for every
    doc that goes into the index. Same filter rules as the indexer:
    hidden models and hidden columns are skipped."""
    out: List[Tuple[str, str, str]] = []
    models_by_ds: Dict[str, List[SlayerModel]] = {}
    for m in visible_models:
        models_by_ds.setdefault(m.data_source, []).append(m)
    for ds in datasources:
        out.append((
            ds, "datasource",
            render_datasource_text(name=ds, models=models_by_ds.get(ds, [])),
        ))
    for model in visible_models:
        out.extend(_render_model_subtree_pairs(model))
    for memory in memories:
        out.append((
            f"memory:{memory.id}", "memory",
            render_memory_text(memory=memory),
        ))
    return out


def build_in_memory_corpus(
    *,
    memories: List[Memory],
    models: List[SlayerModel],
    datasources: List[str],
) -> Corpus:
    """Build the index AND the parallel canonical lookup maps in one walk.

    The embedding channel (DEV-1386) reads from the same render pipeline
    as tantivy, so rendering once here keeps the two channels in sync
    without paying for two traversals.
    """
    schema = _build_schema()
    index = tantivy.Index(schema=schema)
    writer = index.writer()

    visible_models = [m for m in models if not m.hidden]
    pairs = _collect_render_pairs(
        memories=memories,
        visible_models=visible_models,
        datasources=datasources,
    )
    canonical_to_text: Dict[str, str] = {}
    canonical_to_kind: Dict[str, str] = {}
    for canonical, kind, text in pairs:
        # Memory docs use ``id="memory:<int>"`` and ``canonical="<int>"``
        # to match the DEV-1375 tantivy schema; entity docs use the same
        # canonical string for both ``id`` and ``canonical`` fields.
        if kind == "memory":
            int_part = canonical.split(":", 1)[1]
            _add_doc(
                writer=writer, doc_id=canonical, kind="memory",
                canonical=int_part, text=text,
            )
        else:
            _add_doc(
                writer=writer, doc_id=canonical, kind=kind,
                canonical=canonical, text=text,
            )
        canonical_to_text[canonical] = text
        canonical_to_kind[canonical] = kind

    writer.commit()
    index.reload()
    return Corpus(
        index=index,
        canonical_to_text=canonical_to_text,
        canonical_to_kind=canonical_to_kind,
    )


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
