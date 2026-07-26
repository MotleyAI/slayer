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


import tantivy
from pydantic import BaseModel, ConfigDict

from slayer.core.models import SlayerModel
from slayer.memories.models import Memory
from slayer.search.render import (
    collect_model_entity_pairs,
    render_datasource_pair,
    render_memory_text,
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
    memory_id: str | None = None  # populated only when kind == "memory"


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
    memories: list[Memory],
    models: list[SlayerModel],
    datasources: list[str],
    datasource_descriptions: dict[str, str | None] | None = None,
) -> tantivy.Index:
    """Build a fresh in-RAM tantivy index covering the corpus.

    Hidden models and hidden columns are skipped entirely. The caller is
    expected to pass datasource names + every model in scope; this
    function does *not* call into storage.

    DEV-1549: ``datasource_descriptions`` mirrors the symmetric kwarg on
    :func:`build_in_memory_corpus` so direct callers of this helper can
    also surface datasource-description text in the lexical index.

    Returns just the tantivy index for callers that don't need the
    canonical-text lookups. ``build_in_memory_corpus`` returns both.
    """
    corpus = build_in_memory_corpus(
        memories=memories,
        models=models,
        datasources=datasources,
        datasource_descriptions=datasource_descriptions,
    )
    return corpus.index


class Corpus(BaseModel):
    """The tantivy index plus the parallel ``canonical_id → text`` and
    ``canonical_id → kind`` maps. The embedding channel (DEV-1386) uses
    the maps to recover hit text without re-rendering the entity or
    round-tripping through the raw ``canonical`` tantivy field.

    DEV-1549: ``canonical_to_description`` lets the search service
    surface the entity's structured description on ``SearchHit`` without
    re-loading the entity at hit-construction time.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    index: "tantivy.Index"
    canonical_to_text: dict[str, str]
    canonical_to_kind: dict[str, str]
    canonical_to_description: dict[str, str | None] = {}


def _collect_render_pairs(
    *,
    memories: list[Memory],
    visible_models: list[SlayerModel],
    datasources: list[str],
    datasource_descriptions: dict[str, str | None] | None = None,
) -> list[tuple[str, str, str, str | None]]:
    """Return ``[(canonical_id, kind, rendered_text, description), ...]``
    for every doc that goes into the index. Routes through the unified
    dispatch helpers in ``slayer.search.render`` (DEV-1513). Hidden
    models and hidden columns are skipped inside the helpers.

    DEV-1549: the fourth tuple element carries the entity's structured
    ``description`` field (``None`` when absent) so callers can build a
    ``canonical_id → description`` map symmetrical to the existing
    canonical-text map.
    """
    out: list[tuple[str, str, str, str | None]] = []
    models_by_ds: dict[str, list[SlayerModel]] = {}
    for m in visible_models:
        models_by_ds.setdefault(m.data_source, []).append(m)
    descriptions = datasource_descriptions or {}
    for ds in datasources:
        pair = render_datasource_pair(
            name=ds,
            models=models_by_ds.get(ds, []),
            description=descriptions.get(ds),
        )
        out.append((pair.canonical_id, pair.kind, pair.text, pair.description))
    for model in visible_models:
        for re in collect_model_entity_pairs(model=model):
            out.append((re.canonical_id, re.kind, re.text, re.description))
    for memory in memories:
        # Memories surface ``Memory.description`` directly so the search
        # service can flip on compact rendering without re-loading the
        # memory.
        out.append((
            f"memory:{memory.id}", "memory",
            render_memory_text(memory=memory),
            memory.description,
        ))
    return out


def build_in_memory_corpus(
    *,
    memories: list[Memory],
    models: list[SlayerModel],
    datasources: list[str],
    datasource_descriptions: dict[str, str | None] | None = None,
) -> Corpus:
    """Build the index AND the parallel canonical lookup maps in one walk.

    The embedding channel (DEV-1386) reads from the same render pipeline
    as tantivy, so rendering once here keeps the two channels in sync
    without paying for two traversals.

    DEV-1549: ``datasource_descriptions`` is an optional
    ``{ds_name → description}`` map (``None`` description when the
    datasource has none). When omitted, datasource hits get
    ``description=None``.
    """
    schema = _build_schema()
    index = tantivy.Index(schema=schema)
    # `num_threads=1` pins doc-id assignment to insertion order so the
    # tantivy tiebreak (lower internal doc id wins on equal scores) is
    # deterministic across rebuilds (DEV-1414). The default
    # ``num_threads=0`` lets tantivy auto-pick a thread count, and with
    # multiple writer threads the order in which threads commit their
    # local segments determines doc-id assignment — which is
    # non-deterministic for small in-RAM corpora that finish
    # processing within microseconds.
    writer = index.writer(num_threads=1)

    visible_models = [m for m in models if not m.hidden]
    pairs = _collect_render_pairs(
        memories=memories,
        visible_models=visible_models,
        datasources=datasources,
        datasource_descriptions=datasource_descriptions,
    )
    canonical_to_text: dict[str, str] = {}
    canonical_to_kind: dict[str, str] = {}
    canonical_to_description: dict[str, str | None] = {}
    for canonical, kind, text, description in pairs:
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
        canonical_to_description[canonical] = description

    writer.commit()
    index.reload()
    return Corpus(
        index=index,
        canonical_to_text=canonical_to_text,
        canonical_to_kind=canonical_to_kind,
        canonical_to_description=canonical_to_description,
    )


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


def _apply_kind_filter(
    *,
    query: "tantivy.Query",
    schema: "tantivy.Schema",
    kind_filter: str | None,
    exclude_kind: str | None,
) -> "tantivy.Query":
    """Wrap ``query`` in a boolean query that ``Must`` includes (or
    ``MustNot`` excludes) docs whose ``kind`` field exactly equals the
    supplied value. Returns ``query`` unchanged when neither argument
    is set. The caller has already validated mutual exclusivity."""
    if kind_filter is None and exclude_kind is None:
        return query
    target = kind_filter if kind_filter is not None else exclude_kind
    occur = (
        tantivy.Occur.Must if kind_filter is not None
        else tantivy.Occur.MustNot
    )
    kind_term = tantivy.Query.term_query(schema, "kind", target)
    return tantivy.Query.boolean_query([
        (tantivy.Occur.Must, query),
        (occur, kind_term),
    ])


def search_index(
    *,
    index: tantivy.Index,
    question: str,
    limit: int = 20,
    fields: list[str] | None = None,
    kind_filter: str | None = None,
    exclude_kind: str | None = None,
) -> list[IndexHit]:
    """Run a tantivy query against ``index``.

    Args:
        index: The index built by :func:`build_in_memory_index`.
        question: The query text (parsed by tantivy's default query parser
            against ``fields``).
        limit: Max hits to return.
        fields: Which schema fields to query against (default: ``["text"]``).
            Pass ``["canonical"]`` for an exact-match canonical lookup.
        kind_filter: When set, restrict results to docs whose ``kind``
            field exactly equals this value (e.g. ``"memory"``,
            ``"model"``). Combined with the text query via ``Must``.
        exclude_kind: When set, exclude docs whose ``kind`` field equals
            this value. Combined with the text query via ``MustNot``.
        ``kind_filter`` and ``exclude_kind`` are mutually exclusive
        (DEV-1414): one is for keeping a single kind, the other for
        dropping a single kind. Pass at most one.

    Returns:
        List of :class:`IndexHit` in score-desc order.
    """
    if kind_filter is not None and exclude_kind is not None:
        raise ValueError(
            "kind_filter and exclude_kind are mutually exclusive; pass "
            "at most one."
        )
    if not question or not question.strip():
        return []
    if fields is None:
        fields = ["text"]
    try:
        query = index.parse_query(question, fields)
    except (ValueError, RuntimeError):
        return []
    query = _apply_kind_filter(
        query=query,
        schema=index.schema,
        kind_filter=kind_filter,
        exclude_kind=exclude_kind,
    )
    searcher = index.searcher()
    raw_hits = searcher.search(query, limit).hits
    out: list[IndexHit] = []
    for score, address in raw_hits:
        doc = searcher.doc(address)
        kind = str(doc.get_first("kind"))
        canonical = str(doc.get_first("canonical"))
        memory_id: str | None = None
        if kind == "memory":
            memory_id = canonical or None
        out.append(IndexHit(
            id=str(doc.get_first("id")),
            kind=kind,
            canonical=canonical,
            text=str(doc.get_first("text")),
            score=float(score),
            memory_id=memory_id,
        ))
    return out
