# Semantic search (DEV-1375)

SLayer ships a `search` tool that lets agents find both **memories** and
**entities** (datasources, models, columns, named measures, custom
aggregations) using two parallel retrieval channels merged by Reciprocal
Rank Fusion.

This complements [`recall_memories`](memories.md), which is memory-only
and uses entity-overlap BM25. `search` is broader: when you don't yet
know which entity to look at, the tantivy full-text channel surfaces
entities matching your natural-language question; when you have entity
references in hand, the BM25 channel pulls back the most relevant
memories.

## The two retrieval channels

### Channel 1 — entity-overlap BM25 over memories

Same path as `recall_memories`. Inputs are resolved to canonical entity
strings (`<ds>`, `<ds>.<model>`, or `<ds>.<model>.<leaf>` — see
[memories.md](memories.md#the-canonical-entity-form)) and scored against
each memory's stored entity tags via `BM25Plus`. Memories with zero
overlap are excluded.

Activated when `entities` and/or `query` is supplied to `search`.

### Channel 2 — tantivy full-text over memories ∪ entities

A fresh tantivy in-memory index is built per call covering:

- one doc per non-hidden datasource;
- one doc per non-hidden model (excluding model `meta`);
- one doc per non-hidden column on each non-hidden model (including the
  cached `Column.sampled` snapshot, the column's `sql` expression, and
  its `description` / `label` / `format` / `allowed_aggregations`);
- one doc per named `ModelMeasure` (formula + description + label);
- one doc per custom `Aggregation` (formula + params + description);
- one doc per memory (learning text + canonical entity tags).

The index uses tantivy's `en_stem` analyzer (Porter stemmer + default
tokenizer that splits on `_` and `.`), so a search for `"shipped"`
matches docs containing `"shipping"`, and `"customer"` matches
`customer_id`. An exact-match `canonical` field also lets agents paste
a literal canonical string and get the doc back.

Activated when `question` is supplied.

### Reciprocal Rank Fusion

Memory hits from both channels are fused via RRF (`k = 60`):

```
score(d) = Σ_r 1 / (k + rank_r(d))
```

Entities only come from channel 2 — they bypass RRF and surface their
raw tantivy BM25 score.

## Tool surface

```python
search(
    entities: Optional[List[str]] = None,
    query: Optional[Union[SlayerQuery, dict]] = None,
    question: Optional[str] = None,
    max_memories: int = 5,
    max_entities: int = 5,
) -> SearchResponse
```

| Surface | How to call |
|---|---|
| MCP | `search(entities=[...], question="...")` tool |
| REST | `POST /search` with `SearchRequest` body |
| CLI  | `slayer search --entity <e> --question "..." [--format json]` |
| Python client | `await client.search(entities=[...], question="...")` |

### Behaviour matrix

| `entities`/`query` | `question` | Result |
|---|---|---|
| set | set | Both channels run. Memories RRF-fused. Entities from tantivy only. |
| set | unset/empty | Channel 1 only. `entities=[]`. |
| unset/empty | set | Channel 2 only. Memories from tantivy memory subset; entities from tantivy entity subset. |
| unset/empty | unset/empty | Recency fallback: newest `max_memories` memories with a warning. |

### Response shape

```python
class MemoryHit(BaseModel):
    id: int                          # memory id (forget_memory(id=hit.id) works)
    score: float                     # RRF-fused (or single-channel raw)
    text: str                        # full indexed text (no truncation)
    matched_entities: List[str]      # canonical entities that channel-1 input
                                     # overlapped with the memory's tags
    query: Optional[SlayerQuery]     # set when the memory was saved with a query

class EntityHit(BaseModel):
    id: str                          # canonical entity string
    kind: str                        # "datasource"|"model"|"column"|"measure"|"aggregation"
    score: float                     # raw tantivy BM25
    text: str                        # full indexed text (no truncation)

class SearchResponse(BaseModel):
    memories: List[MemoryHit]
    entities: List[EntityHit]
    warnings: List[str]
```

## Sample-value cache

For richer search results, every column carries an optional
`Column.sampled` field — a formatted snapshot of the column's distinct
values (categorical) or `min .. max` range (numeric / temporal). The
field is populated:

- on every `slayer ingest` / `ingest_datasource_models` MCP call /
  `POST /ingest` for every table-backed model in the touched datasource;
- on `slayer search refresh-samples [--data-source X] [--model M ...]`;
- on `edit_model` (column edits → that column; model-level filter / sql /
  source-query body change → every column);
- lazily on `inspect_model` when the cached value is `None` (write-back
  best-effort).

sql-mode and query-backed models are silently skipped in v1 (tracked as
[DEV-1377](https://linear.app/motley-ai/issue/DEV-1377/search-index-hardening-meta-exfiltration-text-size-bounds-and-broader)).

## Index design notes

- The index is built **fresh on every search call** in v1 (no
  persistence, no invalidation logic). For typical SLayer setups (tens
  to low-hundreds of models, tens to low-thousands of memories) this is
  fast; persistent on-disk indexing is a future follow-up.
- `meta` is **excluded** from indexed text — arbitrary user JSON, see
  DEV-1377.
- Hidden models and hidden columns are skipped entirely from the index.
- Each indexed doc has four schema fields: `id` (raw), `kind` (raw),
  `canonical` (raw, exact-match), `text` (en-stemmed + tokenised).
