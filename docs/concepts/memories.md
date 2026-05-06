# Memories

SLayer carries an agent-memory layer alongside the semantic layer. A
**memory** is a free-form note that an agent has written about a part of
the schema, optionally bundled with an example `SlayerQuery`. Memories
are indexed by the **canonical entities** they reference (models,
columns, named measures, custom aggregations), so before issuing a new
query an agent can call `recall_memories` and pull back every note or
example previously saved against the entities in its draft.

A memory has two flavours:

- **Learning** — a memory with no attached query. Surfaces in
  `inspect_model` and in the `learnings` list of `recall_memories`.
- **Query-bearing** — a memory whose `query` field carries a
  `SlayerQuery`. Surfaces only in the `queries` list of
  `recall_memories`.

The split is implicit: pass an entity list to `save_memory` to record
a learning; pass a `SlayerQuery` and the memory carries that query.

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

## The three MCP tools

### `save_memory(learning, linked_entities)`

Persist a memory. `linked_entities` accepts either form:

- **List of entity strings** — each is resolved strictly; ambiguous
  bare-column matches and unknown segments raise.
- **An inline `SlayerQuery` (dict)** — the entity extractor walks
  `source_model`, `dimensions`, `time_dimensions`, `measures`, and
  `filters`; resolution warnings are non-fatal. The query is also
  stored on the memory.

Returns `memory_id` (a positive int), the canonical entities stored,
and any non-fatal warnings.

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

Delete by id. Accepts a positive int (the `memory_id` returned by
`save_memory`); decimal-string forms (`"42"`) are also accepted. Raises
a friendly error if the id is invalid or the memory does not exist.

### `recall_memories(about, max_learnings=None, max_queries=2)`

Look up memories by entity overlap. `about` accepts the same union as
`save_memory`'s `linked_entities`: a list of entity strings, or a
`SlayerQuery` (dict). Empty input or zero-extracted entities returns
all memories ranked by recency (newest first) with an explanatory
warning.

The result splits memories without an attached query (`learnings`)
from those with one (`queries`). Each list is capped independently by
`max_learnings` / `max_queries`.

```json
{
  "about": {
    "source_model": "orders",
    "measures": [{"formula": "amount:sum"}]
  }
}
```

## Recommended agent workflow

1. **Plan the query.** Decide the source model and the columns / measures you
   intend to use.
2. **Call `recall_memories` first.** Pass either the entity list or the
   draft query itself. Read the returned learnings and consider any
   matching saved queries. They may flag pitfalls you'd otherwise hit
   (NULL handling, units, deprecated columns, etc.).
3. **Issue the actual query** via the `query` tool.
4. **Save what you learn.** When you discover a non-obvious quirk
   (encoding, NULL semantics, business rule), call `save_memory`
   with the entities involved so the next agent benefits.

## `inspect_model` integration

`inspect_model` automatically renders a `Learnings` section listing
every memory **whose `query` is `None`** and whose stored entity set
overlaps the model's own entity set (the model itself, every column,
every named measure, every custom aggregation). Query-bearing memories
appear only via `recall_memories`. The section is auto-pruned when
there are no matches — no header is emitted in that case.

## Surfaces

The memory tools are also available outside MCP:

- **REST**: `POST /memories`, `DELETE /memories/{id}`, `POST /memories/recall`.
- **CLI**: `slayer memory save --learning ... --entities ...`,
  `slayer memory forget <id>`, `slayer memory recall --about ...`.
- **Python client**: `SlayerClient.save_memory(...)`,
  `forget_memory(...)`, `recall_memories(...)` — all async; the local-
  mode client (constructed with `storage=`) skips HTTP and goes
  through `MemoryService` directly.

## Storage layout

YAML uses `memories.yaml` and `counters.yaml` files alongside the
existing model and datasource folders. SQLite uses a `memories` table
plus a `memory_entities` index table for the entity-overlap filter,
and a single `memory_seq` row in `id_counters`.

IDs are monotonic positive ints and never reused: once `42` is
allocated, the next `save_memory` returns `43` even if `42` was
deleted.
