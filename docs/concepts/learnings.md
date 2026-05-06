# Learnings & saved queries

SLayer carries an agent-memory layer alongside the semantic layer:

- **Learnings** — short free-form notes an agent has recorded about an
  entity, e.g. "the `is_returned` column on `orders` is in
  `{0, 1, NULL}`; treat `NULL` as not returned."
- **Saved queries** — example `SlayerQuery` objects with a human
  description, recallable for reuse.

Both are indexed by the **canonical entities** they reference (models,
columns, named measures, custom aggregations), so before issuing a new
query an agent can call one tool — `recall` — and pull back every note
or example query previously saved against the entities in its draft.

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

## The four MCP tools

### `save_learning(learning, linked_entities)`

Persist a free-form note. `linked_entities` must be non-empty; each
entry is resolved to canonical form (with aggregation stripped, leaf
rule applied, etc.). Returns the assigned `learning_id` (e.g. `"L42"`),
the canonical forms stored, and any non-fatal warnings (e.g. a
model-vs-column collision warning when a bare name resolves to a model
but a column with the same name exists).

```json
{
  "learning": "orders.is_returned ∈ {0,1,NULL}; treat NULL as not returned",
  "linked_entities": ["orders.is_returned"]
}
```

### `save_query(query, description)`

Persist a `SlayerQuery` (as a dict) or a model name, alongside a human
description. The entity extractor walks the query's `source_model`,
`dimensions`, `time_dimensions`, `measures`, and `filters`, applies the
leaf rule, and stores the canonical entity set with the saved query.
The `source_model` is always tagged automatically.

```json
{
  "query": {
    "source_model": "orders",
    "measures": [{"formula": "amount:sum"}],
    "filters": ["status = 'paid'"]
  },
  "description": "Total paid revenue"
}
```

### `delete_learning_or_query(id)`

Delete by ID. Accepts the `L<int>` and `Q<int>` forms returned by the
two save tools. Raises a friendly error if the ID does not exist.

### `recall(entities=None, query=None, max_learnings=None, max_queries=2)`

Look up learnings and saved queries by entity overlap. At least one of
`entities` or `query` must be supplied. The two sources are unioned
and deduplicated. Stored records are ranked by the size of the
intersection between their stored entity set and the input set; ties
break by recency (newest first).

```json
{
  "query": {
    "source_model": "orders",
    "measures": [{"formula": "amount:sum"}]
  }
}
```

## Recommended agent workflow

1. **Plan the query.** Decide the source model and the columns / measures you
   intend to use.
2. **Call `recall` first.** Pass either the entity list or the draft query
   itself. Read the returned learnings and consider any matching saved
   queries. They may flag pitfalls you'd otherwise hit (NULL handling,
   units, deprecated columns, etc.).
3. **Issue the actual query** via the `query` tool.
4. **Save what you learn.** When you discover a non-obvious quirk
   (encoding, NULL semantics, business rule), call `save_learning`
   with the entities involved so the next agent benefits.

## `inspect_model` integration

`inspect_model` automatically renders a `Learnings` section listing
every learning whose stored entity set overlaps the model's own entity
set (the model itself, every column, every named measure, every custom
aggregation). The section is auto-pruned when there are no matches — no
header is emitted in that case.

## Storage layout

See [`docs/configuration/storage.md`](../configuration/storage.md) for
the on-disk layout. YAML uses `learnings.yaml`, `saved_queries.yaml`,
and `counters.yaml` files alongside the existing model and datasource
folders. SQLite uses dedicated `learnings` and `saved_queries` tables
plus a per-row `*_entities` index table for the entity-overlap filter.

IDs are monotonic and never reused: once `L17` is allocated, the next
`save_learning` is `L18` even if `L17` was deleted.
