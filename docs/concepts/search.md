# Semantic search

SLayer ships a `search` tool that lets agents find both **memories** and
**entities** (datasources, models, columns, named measures, custom
aggregations) using up to three parallel retrieval channels merged by
Reciprocal Rank Fusion. It is the **only** retrieval surface — there is
no separate recall tool.

A third channel (dense embeddings via litellm) is gated behind the
optional `advanced_search` extra. When the extra is not installed or
no provider API key is configured, the embedding channel emits a
warning into `SearchResponse.warnings` and search degrades gracefully
via tantivy + BM25 alone.

When you have entity references in hand, the BM25 channel pulls back
the most relevant memories. When you don't yet know which entity to
look at, the tantivy full-text channel surfaces entities matching your
natural-language question. Both run together when both inputs are
supplied.

## The three retrieval channels

### Channel 1 — entity-overlap BM25 with implicit self-references

Inputs are resolved to canonical entity strings (`<ds>`, `<ds>.<model>`,
or `<ds>.<model>.<leaf>` — see
[memories.md](memories.md#the-canonical-entity-form)) and scored against
each memory's stored entity tags via `BM25Plus`. Memories with zero
overlap are excluded.

**Implicit self-references (DEV-1513).** Channel 1 contributes to BOTH
the memory ranking AND the entity ranking via a single unifying model:
every doc is conceptually tagged with an implicit reference to itself.

- A memory `M` is treated as having effective tags `M.entities ∪ {memory:<M.id>}`, so an `entities=["memory:<id>"]` ref surfaces the named memory itself at the top of the memory BM25 ranking.
- An entity `E` is treated as having a single tag `{<canonical_of_E>}`, so an `entities=["<ds>.<model>.<col>"]` ref surfaces the named entity at the top of the entities bucket.

Concretely:

```json
{
  "call": {"entities": ["mydb.orders.amount"], "max_results": 5},
  "response": {
    "results": [{"id": "mydb.orders.amount", "kind": "column", "score": 0.0}]
  }
}
```

```json
{
  "call": {"entities": ["memory:42"], "max_results": 5},
  "response": {
    "results": [{"id": "42", "kind": "memory", "matched_entities": ["memory:42"]}]
  }
}
```

Filter rules for the new entity surfacing:

- `memory:<id>` refs contribute to the memory portion of the ranking only — they surface as `kind="memory"` hits.
- Refs not rooted at `datasource` (when set) drop with a warning `entity '<X>' is not rooted at datasource '<ds>'; dropped from entities bucket.` The memory side fires the symmetric `memory:<id> is not rooted at datasource '<ds>'; dropped.` when the named memory has no entities rooted at the requested datasource.
- Refs on a hidden model or hidden column drop from the entities portion with `entity '<X>' is on a hidden model/column; dropped from entities bucket.` BM25 over original memory tags is unaffected — memories tagged with that canonical still surface.
- An explicitly-named `memory:<id>` whose attached `Memory.query` has stale references emits the standard stale-query warning (the user explicitly asked for that memory; they deserve to know the query is broken).

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
- the `advanced_search` extra is installed (`pip install motley-slayer[advanced_search]`);
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

Entity rankings from channels 1, 2, and 3 are RRF-fused the same way.
Channel 1's entity ranking is the user-supplied canonical refs in
supplied order (DEV-1513); channels 2 and 3 contribute fuzzy hits.

### Ranking stability (DEV-1414)

Each channel produces a **full per-kind ranking** — channel 2 runs as
two kind-filtered tantivy queries (one over memory docs only, one over
entity docs only), and channel 3 partitions the embedding corpus by
`entity_kind` and ranks each side independently. The per-kind rankings
are RRF-fused into a single flat list before the `max_results` cap is
applied. Because the fusion is deterministic, the relative order of any
subset of the flat list is stable with respect to the corpus and
question — changing only `max_results` never reorders existing entries
nor causes an entry to appear or disappear unless the cap boundary
moves past it.

## Tool surface

```python
search(
    entities: Optional[List[str]] = None,
    query: Optional[Union[SlayerQuery, dict]] = None,
    question: Optional[str] = None,
    datasource: Optional[str] = None,
    max_results: int = 10,
    cypher_filter: Optional[str] = None,
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

- **Entity hits** (channels 1, 2, and 3) include only docs whose
  `canonical_id` is rooted at the requested datasource — exact name
  match (`<ds>`) or strict dotted-path descendant (`<ds>.<model>`,
  `<ds>.<model>.<leaf>`). Character-prefix matches do NOT qualify, so
  `datasource="prod"` excludes a sibling datasource named `prod_v2`.
  Channel 1 (DEV-1513) drops a user-supplied `entities=` ref that
  isn't rooted at the requested datasource with a warning rather than
  silently surfacing it.
- **Memory hits** (channels 1, 2, 3, and the recency fallback) include
  only memories whose `entities` list has at least one entry rooted at
  the requested datasource. A memory that references both `prod.*` and
  `staging.*` surfaces from each datasource when each is filtered
  independently; an untagged memory drops out under any filter. A
  user-supplied `entities=["memory:<id>"]` ref whose memory was
  filtered out emits a symmetric warning.
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

### `cypher_filter` graph pre-filter (DEV-1464)

All four surfaces accept an optional `cypher_filter: Optional[str] = None`
argument. When set, an openCypher `MATCH … RETURN … AS id` query is run
against an ephemeral in-memory property graph (LadybugDB) built from
the current storage state. The returned canonical IDs become a **hard
allowlist** that pre-filters all three channels before any ranking:

- Only memories whose `memory:<id>` is in the returned set are ranked by
  channels 1 / 2 / 3.
- Only entity docs whose `canonical_id` is in the returned set are ranked
  by channels 2 and 3.
- If the query returns an empty set, a warning is emitted and an empty
  response is returned immediately (no channels fire).

**Naive fallback (no `advanced_search` required)**. When the
`advanced_search` extra is not installed, a simple subset of Cypher is
supported without LadybugDB. The naive parser accepts only:

```cypher
MATCH (var:Label1:Label2:...) RETURN var.id AS id
```

Labels must be one or more of `Memory`, `Datasource`, `Model`,
`Column` (or its alias `ModelColumn`), `Measure`, `Aggregation`
(case-insensitive). The colon-separated
multi-label form is a union — it returns hits whose kind matches any of
the listed labels. Any other Cypher (WHERE clauses, relationships,
multiple MATCH clauses, etc.) raises `SlayerError` with a hint to
install the `advanced_search` extra.

**Full `advanced_search` path**: When the `advanced_search` extra is
installed, the full openCypher query runs against the property graph
(see graph schema below). Complex filters, relationship traversals, and
property conditions are all supported.

**Query safety**: the Cypher statement must be:
- A single statement (no semicolons).
- Read-only (no `CREATE` / `MERGE` / `DELETE` / `SET` / `DROP` / `CALL`).
- Returns exactly one column aliased `id` (e.g. `RETURN n.id AS id`).

**Graph schema** (nodes and relationships in the ephemeral graph):

| Node table | Properties |
|---|---|
| `Memory` | `id` (`memory:<id>` form), `learning` |
| `Datasource` | `id`, `name` |
| `Model` | `id` (`<ds>.<model>`), `name`, `description` |
| `ModelColumn` | `id` (`<ds>.<model>.<col>`), `name`, `data_type`, `description` |
| `Measure` | `id` (`<ds>.<model>.<name>`), `name`, `description` |
| `Aggregation` | `id` (`<ds>.<model>.<name>`), `name` |

Note: the column node table is named `ModelColumn` (not `Column`) because `Column` is a reserved keyword in LadybugDB ≥ 0.15.

| Relationship | From → To |
|---|---|
| `MENTIONS` | Memory → {Datasource, Model, ModelColumn, Measure, Aggregation, Memory} |
| `CONTAINS` | Datasource → Model, Model → {ModelColumn, Measure, Aggregation} |
| `JOINS` | Model → Model |

Hidden models and hidden columns are excluded from the graph. The graph is
rebuilt automatically when the storage fingerprint changes (file mtime for
YAML and SQLite).

**Example** — surface only memories that mention the `orders` model:

```cypher
MATCH (m:Memory)-[:MENTIONS]->(n:Model {id: 'shop.orders'})
RETURN m.id AS id
```

**Example** — surface columns in the `shop` datasource:

```cypher
MATCH (d:Datasource {id: 'shop'})-[:CONTAINS*1..3]->(c:ModelColumn)
RETURN c.id AS id
```

**Multi-label union**: `MATCH (n:Memory:ModelColumn)` returns nodes from both
`Memory` AND `ModelColumn` tables (LadybugDB union semantics).

### Behaviour matrix

| `entities`/`query` | `question` | Result |
|---|---|---|
| set | set | All eligible channels run. Memories and entities are RRF-fused across all active channels. Channel 3 is skipped with a warning when the `advanced_search` extra is missing. |
| set | unset/empty | Channel 1 only. Memory hits ranked by entity-tag overlap; entity hits = the named refs themselves (DEV-1513). |
| unset/empty | set | Channels 2 and 3 (when eligible). Memories and entities RRF-fused. |
| unset/empty | unset/empty | Recency fallback: newest memories (any kind) capped at `max_results`, with a warning. |

### Response shape

All hits — memories (both learning-only and query-bearing) and entities
(datasources, models, columns, measures, aggregations) — are returned
as a single flat ranked `results` list capped at `max_results`.
Query-bearing memories have `query` set; learning-only memories have
`query=None`; entity hits have `kind` set to their entity type.

```python
class SearchHit(BaseModel):
    id: str            # memory id OR canonical entity string
    kind: str          # "memory"|"datasource"|"model"|"column"|"measure"|"aggregation"
    score: float       # RRF-fused score
    text: str          # full indexed text (no truncation)
    matched_entities: List[str]   # channel-1 overlap (memory hits only;
                                  # stale tags filtered, DEV-1428)
    query: Optional[SlayerQuery]  # set on query-bearing memory hits

class SearchResponse(BaseModel):
    results: List[SearchHit]
    resolved_input_entities: List[str]   # echo of the resolver output
    warnings: List[str]
```

### Lenient input validation (DEV-1428)

Unresolved entity / memory references in `search(entities=...)` and
`search(query=...)` are demoted to warnings rather than raising. The
dropped token does not appear in `resolved_input_entities`, but the
search proceeds against whatever did resolve. Examples:

- `entities=["mydb.orders.amount", "memory:nonexistent"]` returns a
  normal response; `warnings` includes
  `entity 'memory:nonexistent' dropped: No memory with id 'nonexistent'.`
- A stale entity tag inside a saved memory does not contribute to
  channel-1 BM25 ranking, and is excluded from any hit's
  `matched_entities` list.
- A query-bearing memory hit whose attached `Memory.query` references a
  vanished column gets the warning
  `example_query memory:<id>: attached query has stale references (...); re-save to clean.`
  but is still surfaced with its stored query intact.

`memory:<id>` is also accepted in `entities` (cross-memory linking) —
the resolver checks the memory exists and the canonical form
round-trips as-is.

## Sample-value cache

For richer search results, every column carries three optional
sample-value fields:

- `Column.sampled` — a formatted text snapshot. For categorical columns,
  the top-20 most-common values comma-joined; for high-cardinality
  categoricals (> 50 distinct), the top-20 plus a `... (N distinct)`
  suffix carrying the true total. For numeric / temporal columns, the
  `min .. max` range.
- `Column.sampled_values` (DEV-1480) — the structured top-50-by-frequency
  list for categorical columns. Stays `None` for numeric / temporal
  columns. Consumers comparing predicate literals against actual stored
  values should read this field directly — text-split on `sampled` is
  ambiguous for values that themselves contain commas
  (e.g. `"R$ 1,000–3,000"`).
- `Column.distinct_count` (DEV-1480) — the true total cardinality at
  profile time. Set for every profiled categorical column (computed via a
  secondary `count_distinct` query when overflow is detected so the count
  is exact, not capped). Stays `None` for numeric / temporal columns.

All three are populated:

- on every `slayer ingest` / `ingest_datasource_models` MCP call /
  `POST /ingest` for every table-backed model in the touched datasource;
- on `slayer search refresh-samples [--data-source X] [--model M ...]`;
- on `edit_model` (column edits → that column; model-level filter / sql /
  source-query body change → every column);
- lazily on `inspect_model` when the cached value is missing (write-back
  best-effort);
- lazily inside `search()` itself for any column hit whose persisted
  `sampled_values` is stale (DEV-1516). The post-fusion column-hit hook
  groups hits by `(data_source, model_name)` — refreshes within a model
  serialise (the storage write is a model-level read-modify-write);
  refreshes across different models run concurrently via
  `asyncio.gather`. When `search()` is constructed without an engine
  (storage-only contexts), the hook is a silent no-op.

Cache validity for categorical columns requires `sampled_values is not None` —
v6 (legacy `sampled` only) models re-profile on the next `inspect_model`
or `search()` column hit so the structured field gets populated.

sql-mode and query-backed models are silently skipped in v1.

### How sample values surface in search results

The per-column doc rendered by `slayer/search/render.py:render_column_text`
prefers the structured `sampled_values` list (full top-50) over the
20-truncated `sampled` text. When `sampled_values` is populated:

```text
Column: warehouse.orders.status
Type: TEXT
Description: Order status.
Sample values: ["paid", "refunded", "cancelled", "pending", …]  ← JSON-encoded, all 50
Distinct count: 12345        ← only when distinct_count > len(sampled_values)
```

The list is rendered as a JSON array (not comma-joined) so values that
themselves contain commas — `"R$ 1,000–3,000"`, locale-formatted numbers,
multi-clause labels — survive unambiguously to the consumer. This is why
DEV-1480 introduced the structured `sampled_values` field in the first
place; comma-joining it back to a flat string would re-introduce the
exact ambiguity it was meant to solve.

When `sampled_values` is `None` (numeric / temporal columns, or legacy
v6 data, or rare overflow-with-failed-count_distinct rows), the renderer
falls back to the persisted `sampled` text — which already carries the
`... (N distinct)` suffix for the legacy overflow case, so no extra
`Distinct count` line is emitted. An empty `sampled_values=[]` list is
authoritative-empty: the line is skipped entirely (no fallback to stale
`sampled`).

This same text feeds both the per-column search index doc AND
`EntityHit.text` returned by `search()` — single renderer, single
source of truth. `inspect_model`'s markdown `## Columns` table is the
**all-columns-at-once** surface and continues to show the 20-truncated
`sampled` text per column for readability on wide models. JSON
`inspect_model` output already carries the full `sampled_values` list.

**Known limitation.** The refresh hook runs **after** RRF fusion, on the
top-K hits being returned. Ranking (BM25 / tantivy / embeddings) still
operates on whatever the corpus held at index-build time. A query whose
only match against a column is a newly-revealed value in positions 21-50
may still fail to surface that column. The text the agent sees IS
refreshed; tantivy / embeddings will catch up on the next
`slayer ingest` content-hash pass.

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
  cost is independent of subtree size. **Memories** are included in the
  `slayer ingest` / `--ingest-on-startup` per-datasource refresh
  (DEV-1416), filtered to memories with at least one canonical entity
  rooted at the current datasource — so `embeddings.db` can be repaired
  by re-running ingest, no separate `slayer embeddings refresh` step
  required.
- **Cascade** semantics (DEV-1405 fix): `delete_embeddings_for_canonical`
  matches the canonical id exactly OR as a strict dotted-path descendant
  (`<root>.<...>`) — never as a character prefix. So `delete_memory(4)`
  removes only `memory:4` (not `memory:42`, `memory:43`, …);
  `delete_datasource("orders")` does not touch a sibling datasource
  named `orders_archive`; `delete_model("orders", "customers")` does not
  touch a sibling `customers_v2`.
- Optional pip extra: `pip install motley-slayer[advanced_search]`
  installs `litellm` + `numpy`. When omitted, the embedding channel
  emits a one-line warning and contributes nothing.
- **Storage shape**: embeddings are stored as JSON lists of floats —
  portable, debuggable, dialect-neutral. ~6 KB per 1536-dim row.
