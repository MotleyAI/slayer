# Auto-Ingestion: From Database to Datasets

Defining a semantic layer manually — writing out every dimension, metric, and relationship — is tedious and error-prone, especially for schemas with dozens of tables and FK relationships. SLayer's auto-ingestion removes that cold-start friction: point it at a database, and it generates a complete set of datasets ready to query.

## What auto-ingestion does

Given a datasource configuration, `engine.ingest(ds)` (or `engine.ingest_sync(ds)`) introspects the database schema and produces one `SlayerDataset` per table, complete with:

- **Dimensions** for every column
- **Metrics** generated from column types (see rules below)
- **Relationships** derived from the table's own foreign key constraints (one `DatasetRelationship` per FK)

No join-related SQL is baked into the datasets — relationships are resolved dynamically at query time via the [relationship graph](../05_joins/joins.md).

## FK graph discovery

The first step is reading the FK constraints: each one means "this table has a foreign key pointing to that table," and each becomes a relationship even when the FKs form a cycle.

## Relationship generation

Each table's own FK relationships become `DatasetRelationship` objects — one per FK. For example, `items` gets these relationships:

| Target | Source column | Target column |
|--------|--------------|---------------|
| orders | `order_id` | `id` |
| products | `sku` | `sku` |

That's it — only the table's own FKs. Tables reachable via multiple hops (e.g. `items → orders → customers`) are **not** stored in the relationships list. Instead, SLayer walks the relationship graph at query time: each intermediate dataset declares its own direct relationships, so the path `orders → customers → regions` is resolved by following `orders.relationships` to `customers`, then `customers.relationships` to `regions`.

Diamond relationships — where the same table is reachable via multiple FK paths — are disambiguated by the path notation in the query. `customers.regions.name` and `warehouses.regions.name` each walk a different chain and produce distinct table aliases (`customers__regions` vs `warehouses__regions`). See the [relationships post](../05_joins/joins.md) for details on diamond relationships and how to recombine them.

## Dimension and metric generation

For each table, SLayer generates dimensions and metrics from the table's own columns only. Columns from joined tables are **not** stored as dimension objects — they are resolved at query time via dot syntax (e.g., `orders.customers.name`).

The metric generation rules:

| Column type | Metrics generated |
|------------|-------------------|
| Any table | `count(*)` always available (no explicit metric needed) |
| Numeric, non-ID | One metric per column (e.g., `{name: "amount", sql: "amount"}`). Aggregate at query time: `sum(amount)`, `avg(amount)`, etc. |
| Non-numeric, non-ID | One metric per column. Use `count_distinct(name)` at query time. |
| ID / FK columns | No metrics (skipped) |

A column is considered an ID if its name is `id` or ends with `_id`, `_key`, `_pk`, or `_fk`.

## Querying auto-ingested datasets

Once ingested, datasets are queried like any other. Joined dimensions use dot syntax to walk the relationship graph:

```json
{
  "population": "items",
  "metrics": ["count(items.*)"],
  "dimensions": ["orders.customers.name"],
  "order": [{"field": "items._count", "direction": "desc"}],
  "limit": 5
}
```

Result keys are the references as written: `orders.customers.name`, `items._count`.

---

See the [companion notebook](auto_ingest_nb.ipynb) for runnable code demonstrating auto-ingestion end to end.

> **Tip:** When running a long-lived REST or MCP server, you can re-run the same idempotent ingestion every boot with `slayer serve --ingest-on-startup` / `slayer mcp --ingest-on-startup` — useful for YAML-drop datasource workflows. See [Ingesting at Startup](../../concepts/ingestion.md#ingesting-at-startup).
