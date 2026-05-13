# Semantic search

SLayer ships a `search` tool that lets agents find both **memories** and
**entities** (datasources, models, columns, named measures, custom
aggregations) using up to three parallel retrieval channels merged by
Reciprocal Rank Fusion. It is the **only** retrieval surface — there is
no separate recall tool.

A third channel (dense embeddings via litellm) is gated behind the
optional `embedding_search` extra. When the extra is not installed or
no provider API key is configured, the embedding channel emits a
warning into `SearchResponse.warnings` and search degrades gracefully
via tantivy + BM25 alone.

When you have entity references in hand, the BM25 channel pulls back
the most relevant memories. When you don't yet know which entity to
look at, the tantivy full-text channel surfaces entities matching your
natural-language question. Both run together when both inputs are
supplied.

## The three retrieval channels

### Channel 1 — entity-overlap BM25 over memories

Inputs are resolved to canonical entity strings (`<ds>`, `<ds>.<model>`,
or `<ds>.<model>.<leaf>` — see
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

### Channel 3 — dense embedding similarity

A persistent embeddings sidecar table holds one row per indexable doc
(memory or non-hidden datasource / model / column / measure /
aggregation) under each configured `embedding_model_name`. On search,
the question is embedded once, the corpus matrix is loaded fresh, and
top-k cosine similarities are computed with numpy.

Activated when **all of the following** hold:

- `question` is supplied;
- the `embedding_search` extra is installed (`pip install motley-slayer[embedding_search]`);
- at least one embedding row exists for the active model name;
- the query-embedding call succeeds.

When any precondition is not met, the channel emits a one-line warning
into `SearchResponse.warnings` and contributes no rankings — search
continues via channels 1 and 2.

**Configuration.** `SLAYER_EMBEDDING_MODEL` (env var) selects the
embedding model, in `<provider>/<model-name>` litellm format. Defaults
to `openai/text-embedding-3-small`. Provider credentials
(`OPENAI_API_KEY`, `AZURE_API_KEY`, etc.) are read by litellm itself.

**Refresh.** Embedding rows are refreshed inline on the same write-side
edges as `Column.sampled`:

- `slayer ingest` / `ingest_datasource_models` MCP / `POST /ingest` —
  refreshes the datasource doc plus every visible model + its visible
  children (columns, named measures, aggregations);
- `edit_model` — refreshes the model's whole subtree;
- `save_memory` — refreshes that one memory.

Each refresh hashes the rendered indexed text and compares it to the
stored `content_hash`; the litellm call is skipped when the source text
hasn't changed since the last refresh, so idempotent re-runs are cheap.

**Model changes.** Switching `SLAYER_EMBEDDING_MODEL` mid-project leaves
old rows in place but inert — the search service reads only rows
matching the active model name. Re-run `slayer ingest` (or re-save
memories) to populate the new model's rows. A dimension-mismatch
between the question embedding and stored rows is detected and emits a
warning instead of crashing.

**Failure mode.** Per-entity embed failures (rate limits, transient
network errors, bad keys) are non-fatal: the failing row is simply not
written, and a warning is appended to the surfaced response (or to
`IdempotentIngestResult.errors` on ingest).

### Reciprocal Rank Fusion

Memory rankings from every active channel are fused via RRF (`k = 60`):

```text
score(d) = Σ_r 1 / (k + rank_r(d))
```

Entity rankings from channels 2 and 3 are RRF-fused the same way.
Channel 1 contributes to the memory ranking only (it operates on
memory entity tags, not on entity docs).

## Tool surface

```python
search(
    entities: Optional[List[str]] = None,
    query: Optional[Union[SlayerQuery, dict]] = None,
    question: Optional[str] = None,
    max_memories: int = 5,
    max_example_queries: int = 2,
    max_entities: int = 5,
) -> SearchResponse
```

| Surface | How to call |
|---|---|
| MCP | `search(entities=[...], question="...")` tool |
| REST | `POST /search` with `SearchRequest` body |
| CLI  | `slayer search --entity <e> --question "..." [--format json]` |
| Python client | `await client.search(entities=[...], question="...")` |

### `datasource` filter (DEV-1409)

All four surfaces accept an optional `datasource: Optional[str] = None`
argument. When set, every channel pre-filters its corpus to that one
datasource:

- **Entity hits** (channels 2 and 3) include only docs whose
  `canonical_id` is rooted at the requested datasource — exact name
  match (`<ds>`) or strict dotted-path descendant (`<ds>.<model>`,
  `<ds>.<model>.<leaf>`). Character-prefix matches do NOT qualify, so
  `datasource="prod"` excludes a sibling datasource named `prod_v2`.
- **Memory hits** (channels 1, 2, 3, and the recency fallback) include
  only memories whose `entities` list has at least one entry rooted at
  the requested datasource. A memory that references both `prod.*` and
  `staging.*` surfaces from each datasource when each is filtered
  independently; an untagged memory drops out under any filter.
- BM25 and tantivy IDF statistics reflect the filtered subset only —
  pre-filter, not post-filter. The embedding cosine corpus (channel 3)
  is filtered before the numpy matrix is built, so cosine scores are
  computed only against eligible rows.

Unknown datasource → `ValueError` (HTTP 400 on REST, MCP-formatted error
on the MCP tool). Validation runs before any corpus walk so typos
surface fast.

`canonical_id` rooting uses `slayer.memories.resolver.canonical_id_rooted_at`,
which encodes the same dotted-namespace rule the embedding cascade-delete
already enforces (DEV-1405). Datasource names cannot contain `.` (rejected
by `DatasourceConfig.name` + `SlayerModel.data_source` validators), so the
prefix match is unambiguous.

### Behaviour matrix

| `entities`/`query` | `question` | Result |
|---|---|---|
| set | set | All eligible channels run. Memories RRF-fused (channels 1 + 2 + 3); entities RRF-fused (channels 2 + 3). Channel 3 is skipped with a warning when the `embedding_search` extra is missing. Query-bearing memories partitioned out to `example_queries`. |
| set | unset/empty | Channel 1 only. Memories partitioned by `query` presence; no entity hits. |
| unset/empty | set | Channels 2 and 3 (when eligible). Memories RRF-fused; entities RRF-fused. |
| unset/empty | unset/empty | Recency fallback: newest `max_memories` learning-only memories + newest `max_example_queries` query-bearing memories, with a warning. |

### Response shape

Memories are partitioned by `Memory.query is None`: learning-only
memories land in `memories`, query-bearing memories in
`example_queries`. The two lists are capped independently so a few
bulky example queries cannot crowd out small learning-only notes.

```python
class MemoryHit(BaseModel):
    id: int                          # memory id (forget_memory(id=hit.id) works)
    score: float                     # RRF-fused (or single-channel raw)
    text: str                        # full indexed text (no truncation)
    matched_entities: List[str]      # canonical entities that channel-1 input
                                     # overlapped with the memory's tags

class ExampleQueryHit(BaseModel):
    id: int                          # memory id
    score: float                     # RRF-fused
    text: str                        # full indexed text
    matched_entities: List[str]
    query: SlayerQuery               # always set on this hit type

class EntityHit(BaseModel):
    id: str                          # canonical entity string
    kind: str                        # "datasource"|"model"|"column"|"measure"|"aggregation"
    score: float                     # raw tantivy BM25
    text: str                        # full indexed text (no truncation)

class SearchResponse(BaseModel):
    memories: List[MemoryHit]            # learning-only (query is None)
    example_queries: List[ExampleQueryHit]   # query-bearing
    entities: List[EntityHit]
    resolved_input_entities: List[str]   # echo of the resolver output
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

sql-mode and query-backed models are silently skipped in v1.

## Index design notes

- The tantivy index is built **fresh on every search call** in v1 (no
  persistence, no invalidation logic). For typical SLayer setups (tens
  to low-hundreds of models, tens to low-thousands of memories) this is
  fast; persistent on-disk indexing is a future follow-up.
- `meta` is **excluded** from indexed text — arbitrary user JSON.
- Hidden models and hidden columns are skipped entirely from the index.
- Each tantivy doc has four schema fields: `id` (raw), `kind` (raw),
  `canonical` (raw, exact-match), `text` (en-stemmed + tokenised).

## Embedding sidecar design notes

- **Stored**, not rebuilt per call. Rows live in an indexed `embeddings`
  SQLite table — in the main `.db` file for `SQLiteStorage`, or at
  `<base_dir>/embeddings.db` for `YAMLStorage` (DEV-1405). Keyed by
  `(canonical_id, embedding_model_name)`. Both backends share the same
  SQL through a `SidecarEmbeddingStore` helper. Search loads the corpus
  matrix fresh per call and runs cosine similarity in numpy.
- Same render pipeline as tantivy (`slayer/search/render.py`) — every
  doc that goes into the tantivy index also feeds the embedding text.
- Refresh is **inline** on the same write-side edges as
  `Column.sampled`: ingest, `edit_model`, `save_memory`. SHA256 content
  hash makes idempotent re-runs cheap. The hot path
  (`EmbeddingService._apply_pending`) issues one batched
  `get_embeddings_for_canonical_ids` for the hash-skip filter and one
  batched `save_embeddings` for the persist step (DEV-1405) — refresh
  cost is independent of subtree size.
- **Cascade** semantics (DEV-1405 fix): `delete_embeddings_for_canonical`
  matches the canonical id exactly OR as a strict dotted-path descendant
  (`<root>.<...>`) — never as a character prefix. So `delete_memory(4)`
  removes only `memory:4` (not `memory:42`, `memory:43`, …);
  `delete_datasource("orders")` does not touch a sibling datasource
  named `orders_archive`; `delete_model("orders", "customers")` does not
  touch a sibling `customers_v2`.
- Optional pip extra: `pip install motley-slayer[embedding_search]`
  installs `litellm` + `numpy`. When omitted, the embedding channel
  emits a one-line warning and contributes nothing.
- **Storage shape**: embeddings are stored as JSON lists of floats —
  portable, debuggable, dialect-neutral. ~6 KB per 1536-dim row.
