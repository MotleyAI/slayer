# Auto-Ingestion: From Database to Semantic Models

Defining a semantic layer manually — writing out every dimension, measure, and join — is tedious and error-prone, especially for schemas with dozens of tables and FK relationships. SLayer's auto-ingestion removes that cold-start friction: point it at a database, and it generates a complete set of models ready to query.

## What auto-ingestion does

Given a datasource configuration, `ingest_datasource()` introspects the database schema and produces one `SlayerModel` per table, complete with:

- **Dimensions** for every column
- **Measures** generated from column types (see rules below)
- **Joins** derived from the table's own foreign key constraints (one `ModelJoin` per FK)

No join-related SQL is baked into the models — joins are resolved dynamically at query time via the [join graph](../05_joins/joins.md).

## FK graph discovery

The first step is building a directed graph from FK constraints: each edge means "this table has a foreign key pointing to that table." SLayer validates that the graph is acyclic (cycles would create infinite join chains) and raises a `RollupGraphError` if any are found.

## Join generation

Each table's own FK relationships become `ModelJoin` objects — one per FK. For example, `order_items` gets these joins:

| Target | Source column | Target column |
|--------|--------------|---------------|
| orders | `order_id` | `id` |
| products | `sku` | `sku` |

That's it — only the table's own FKs. Tables reachable via multiple hops (e.g. `order_items → orders → customers`) are **not** stored in the joins list. Instead, SLayer walks the join graph at query time: each intermediate model declares its own direct joins, so the path `orders → customers → regions` is resolved by following `orders.joins` to `customers`, then `customers.joins` to `regions`.

Diamond joins — where the same table is reachable via multiple FK paths — are disambiguated by the path notation in the query. `customers.regions.name` and `warehouses.regions.name` each walk a different chain and produce distinct table aliases (`customers__regions` vs `warehouses__regions`). See the [joins post](../05_joins/joins.md) for details on diamond joins and how to recombine them.

## Dimension and measure generation

For each table, SLayer generates dimensions and measures from the table's own columns only. Columns from joined tables are **not** stored as dimension objects — they are resolved at query time via dot syntax (e.g., `orders.customers.name`).

The measure generation rules:

| Column type | Measures generated |
|------------|-------------------|
| Any table | `*:count` always available (no explicit measure needed) |
| Numeric, non-ID | One measure per column (e.g., `{name: "amount", sql: "amount"}`). Aggregate at query time: `amount:sum`, `amount:avg`, etc. |
| Non-numeric, non-ID | One measure per column. Use `name:count_distinct` at query time. |
| ID / FK columns | No measures (skipped) |

A column is considered an ID if its name is `id` or ends with `_id`, `_key`, `_pk`, or `_fk`.

## Querying auto-ingested models

Once ingested, models are queried like any other. Joined dimensions use dot syntax to walk the join graph:

```json
{
  "source_model": "order_items",
  "fields": ["*:count", "quantity:sum"],
  "dimensions": ["orders.customers.name"],
  "order": [{"column": "quantity:sum", "direction": "desc"}],
  "limit": 5
}
```

Result keys include the full path from the source model: `order_items.orders.customers.name`, `order_items.count`, `order_items.quantity_sum`.

---

See the [companion notebook](auto_ingest_nb.ipynb) for runnable code demonstrating auto-ingestion end to end.
