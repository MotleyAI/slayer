# Memories

SLayer carries an agent-memory layer alongside the semantic layer. A
**memory** is a free-form note that an agent has written about a part of
the schema, optionally bundled with an example `SlayerQuery`. Memories
are indexed by the **canonical entities** they reference (models,
columns, named measures, custom aggregations), so before issuing a new
query an agent can call [`search`](search.md) and pull back every note
or example previously saved against the entities in its draft (plus
canonical entity matches via tantivy full-text — see the search docs).

A memory has two flavours:

- **Learning** — a memory with no attached query. Surfaces in
  `inspect_model` and as a `kind="memory"` hit in `search` with
  `hit.query is None`.
- **Query-bearing** — a memory whose `query` field carries a
  `SlayerQuery`. Surfaces only via `search`, as a `kind="memory"` hit
  with `hit.query is not None`. Not rendered in `inspect_model`.

Both flavours land in the same flat `SearchResponse.results` list
(DEV-1532); callers split on `hit.query` rather than on separate
buckets. The split is implicit at save time: pass an entity list to
`save_memory` to record a learning; pass a `SlayerQuery` and the
memory carries that query.

## The canonical entity form

Every persisted entity is exactly one of:

| Form | Example |
|------|---------|
| `<datasource>` | `mydb` |
| `<datasource>.<model>` | `mydb.orders` |
| `<datasource>.<model>.<leaf>` | `mydb.orders.amount` |

Inputs that aren't already in this shape are normalised at save time:

- **Aggregation suffixes are stripped.** `revenue:sum`,
  `revenue:weighted_avg(weight=qty)`, and `revenue:corr(other=qty)` all
  canonicalise to `<ds>.<model>.revenue`. The aggregation itself is
  not an independent entity.
- **`*:count` collapses to the source model.** It's "count of all rows
  on this model," so the entity is the model.
- **Multi-hop dotted paths keep only the leaf.** A query referencing
  `orders.customers.regions.name` produces `{mydb.orders,
  <regions-ds>.regions.name}` — intermediate hops on the join path
  are discarded.
- **Named measures and custom aggregations are opaque.** A learning
  tagged against `mydb.orders.aov` does **not** also recurse into the
  `aov` formula and tag every column it references.

Equality is plain string equality on the canonical form, so two
callers using `revenue:sum` and `mydb.orders.revenue` reach the same
record.

## The two write-side MCP tools

Memory retrieval is part of [`search`](search.md) (one tool covers
both memories and canonical entity discovery). This page covers only
the write side.

### `save_memory(learning, linked_entities, id=None)`

Persist a memory. `linked_entities` accepts either form:

- **List of entity strings** — each is resolved strictly; ambiguous
  bare-column matches and unknown segments raise. `memory:<id>`
  references to other memories are also valid (cross-memory linking).
- **An inline `SlayerQuery` (dict)** — the entity extractor walks
  `source_model`, `dimensions`, `time_dimensions`, `measures`, and
  `filters`; resolution warnings are non-fatal. The query is also
  stored on the memory.

`id` is optional. Omit it to let the allocator pick the next monotonic
int-shaped id (`"1"`, `"2"`, ...). Supply a string for a stable
user-controlled id (`"kb.policy.42"`) — useful for knowledge-base
ingestion pipelines. Charset excludes `:`, `/`, `?`, `#`, whitespace,
and ASCII control characters. Duplicate id → unconditional **upsert**,
`created_at` preserved.

Returns `memory_id` (a non-empty string), the canonical entities
stored, and any non-fatal warnings.

**Embedding side effect.** When the `advanced_search` extra is
installed and `SLAYER_EMBEDDING_MODEL` resolves to a configured
provider, `save_memory` also embeds the new memory inline so it
participates in the embedding-similarity search channel right away.
Embed failures are non-fatal and surface as warnings; the memory is
still persisted. Without the extra installed, no embedding is created
and search continues via the tantivy + BM25 channels.

Learning form:

```json
{
  "learning": "orders.is_returned in {0,1,NULL}; treat NULL as not returned",
  "linked_entities": ["orders.is_returned"]
}
```

Query-bearing form:

```json
{
  "learning": "Total paid revenue",
  "linked_entities": {
    "source_model": "orders",
    "measures": [{"formula": "amount:sum"}],
    "filters": ["status = 'paid'"]
  }
}
```

### `forget_memory(id)`

Delete by id. Accepts the canonical string id (`"1"`, `"kb.policy.42"`)
or — for back-compat — a legacy int that is stringified decimally.
Raises a friendly error if the id is invalid or the memory does not
exist.

**Cascade-on-delete.** Removing a memory also strips every
`memory:<id>` reference to it from every other memory's `entities`
list (exact-match only — `memory:42` never strips `memory:421`).

## Recommended agent workflow

1. **Plan the query.** Decide the source model and the columns / measures you
   intend to use.
2. **Call `search` first.** Pass the entities you're considering (and/or
   the draft query, and/or a free-text `question`). Walk
   `response.results`: hits with `kind="memory"` are prior notes (a
   `hit.query is not None` marks a saved example query); other kinds
   surface canonical entities matched by the full-text / embedding
   channels. Memory hits may flag pitfalls you'd otherwise hit (NULL
   handling, units, deprecated columns, etc.).
3. **Issue the actual query** via the `query` tool.
4. **Save what you learn.** When you discover a non-obvious quirk
   (encoding, NULL semantics, business rule), call `save_memory`
   with the entities involved so the next agent benefits.

## `inspect_model` integration

`inspect_model` automatically renders a `Learnings` section listing
every memory **whose `query` is `None`** and whose stored entity set
overlaps the model's own entity set (the model itself, every column,
every named measure, every custom aggregation). Query-bearing memories
appear only via `search` (as `kind="memory"` hits with
`hit.query is not None`). The section is auto-pruned when there are
no matches — no header is emitted in that case.

## Surfaces

The memory write-side tools are also available outside MCP:

- **REST**: `POST /memories`, `DELETE /memories/{id}`.
- **CLI**: `slayer memory save --learning ... --entities ...`,
  `slayer memory forget <id>`.
- **Python client**: `SlayerClient.save_memory(...)`,
  `forget_memory(...)` — all async; the local-mode client
  (constructed with `storage=`) skips HTTP and goes through
  `MemoryService` directly.

For retrieval, see [`search`](search.md) (MCP `search`, REST `POST
/search`, CLI `slayer search`, `SlayerClient.search`).

## Storage layout

YAML uses a single `memories.yaml` file alongside the model and
datasource folders. SQLite uses a `memories` table plus a
`memory_entities` index table for the entity-overlap filter.

IDs are non-empty strings (DEV-1428). The auto-allocator walks
`max(int-shaped id) + 1` over the existing corpus where "int-shaped"
means pure-digit, no-leading-zero (`"42"` counts; `"001"` and
`"42abc"` do not). User-supplied ids share the namespace; duplicates
upsert (and preserve the original `created_at`). Ids of deleted
memories may be reused by the allocator; `delete_memory` cascades to
drop the matching embedding row AND strips every other memory's
`memory:<id>` reference to it, so reuse never strands data.

### Cascade-on-delete

When a `delete_model` / `delete_datasource` / `forget_memory` /
`edit_model_remove` call removes a leaf, every dangling reference to
it is stripped from every memory's `entities` list. The match
predicate splits by ref kind:

- `<ds>.<model>[.<leaf>]` — exact match OR strict dotted-path
  descendant (`mydb.orders` strips both `mydb.orders` and
  `mydb.orders.amount`; `mydb.orders_archive` is **not** touched).
- `memory:<id>` — exact-match only (`memory:42` does not strip
  `memory:421` or `memory:42.y`).

Memories with zero entities after the strip are kept — the learning
text stands alone, and the memory still surfaces via the tantivy and
embedding channels.

The embedded text for a memory is `learning` only (entity tags are
excluded), so cascade-strip rewrites do **not** change the embedding
content hash and the per-memory refresh hash-skips. Zero embedding
calls per deleted entity.

### Defense-in-depth cleanup at ingest

`slayer ingest` / `--ingest-on-startup` runs a second cleanup pass
over each datasource's memories: every reference is probed against
storage, and ones that resolve to a definitive "not found" are
stripped from the persisted `entities` list. Transient lookup failures
keep the reference (a raise is treated as "ref intact"), so infra
hiccups never drop data.

Stale `Memory.query` (the optional inline query attached to
example-queries memories) gets a warning rather than a rewrite — the
query is left alone, and an agent reading the warning re-saves the
memory to clean it.

### Search-time semantics

`search(entities=...)` is **lenient**: unresolved entities and memory
references become warnings rather than raising. The surviving
canonical set shows up in `resolved_input_entities`.

Stale tags on persisted memories are filtered out at retrieval time
(belt) before BM25 ranking, so they neither contribute to scoring nor
surface in `matched_entities`. No write-back — the persisted entity
list is unchanged.
