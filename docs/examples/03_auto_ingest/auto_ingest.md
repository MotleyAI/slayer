# Auto-Ingestion: From Database to Semantic Models

Defining a semantic layer manually — writing out every dimension, measure, and join — is tedious and error-prone, especially for schemas with dozens of tables and FK relationships. SLayer's auto-ingestion removes that cold-start friction: point it at a database, and it generates a complete set of models ready to query.

## What auto-ingestion does

Given a datasource configuration, `ingest_datasource()` introspects the database schema and produces one `SlayerModel` per table, complete with:

- **Dimensions** for every column
- **Measures** generated from column types (see rules below)
- **Joins** derived from foreign key constraints, including multi-hop transitive joins

No join-related SQL is baked into the models — joins are resolved dynamically at query time via the join graph.

## FK graph discovery

The first step is building a directed graph from FK constraints: each edge means "this table has a foreign key pointing to that table." SLayer validates that the graph is acyclic (cycles would create infinite join chains) and raises a `RollupGraphError` if any are found.

For each table, SLayer then computes the **transitive closure** via BFS — the set of all tables reachable by following FK chains. For example, if `order_items` references `orders`, and `orders` references `customers`, then `order_items` can transitively reach `customers` even though it has no direct FK to it.

## Join generation

FK relationships become `ModelJoin` objects via BFS traversal from each table. Each join specifies a target model and the column pairs to join on.

For multi-hop joins, the source column is **path-qualified** to indicate which already-joined table the FK comes from. For example, `order_items` gets these joins:

| Target | Source column | Target column | Type |
|--------|--------------|---------------|------|
| orders | `order_id` | `id` | direct |
| products | `sku` | `sku` | direct |
| customers | `orders.customer_id` | `id` | multi-hop |
| stores | `orders.store_id` | `id` | multi-hop |

The path-qualified source column `orders.customer_id` means "the `customer_id` column in the already-joined `orders` table."

Diamond joins — where the same table is reachable via multiple FK paths — are handled automatically. Each path produces a separate `ModelJoin` with a unique path-based alias, so `customers.regions.name` and `warehouses.regions.name` refer to independent copies of the `regions` table. See the [joins post](../05_joins/joins.md) for details on diamond joins and how to recombine them.

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
