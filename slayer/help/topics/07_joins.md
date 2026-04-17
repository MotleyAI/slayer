# Joins

SLayer models relate to each other via **joins**. Only LEFT JOIN is supported
— joins are used for enrichment, not set operations.

## Declaring joins

```yaml
name: orders
sql_table: public.orders
joins:
  - target_model: customers
    join_pairs: [["customer_id", "id"]]
  - target_model: products
    join_pairs: [["product_id", "id"]]
```

Auto-ingestion creates direct joins from foreign keys automatically (one join
per FK on the source table). Multi-hop paths like `orders → customers →
regions` are resolved at query time by walking each intermediate model's own
joins — no transitive joins are baked in at ingestion.

## Referencing joined data

In **queries**, use dots:

- Dimension: `{"dimensions": ["customers.name"]}`
- Multi-hop: `{"dimensions": ["customers.regions.name"]}`
- Measure: `{"fields": ["customers.*:count"]}`
- Cross-model transform: `{"fields": [{"formula": "cumsum(customers.score:avg)"}]}`

SLayer walks the join graph via BFS and inserts the LEFT JOINs.

In **SQL snippets** (dimension `sql`, measure `sql`, model `filters`), use
`__` instead of dots, because dots aren't valid SQL:

```yaml
filters:
  - "customers__regions.name = 'US'"
```

Single-dot `customers.name` (table.column) is also fine in SQL snippets — it's
only multi-hop paths that need `__`.

## Cross-model measures — why sub-queries

A dimension from a joined model is just another column to GROUP BY — no
cardinality issue. A **measure** from a joined model is different: a LEFT JOIN
can duplicate rows, so aggregating after the join would double-count.

SLayer splits any query containing a cross-model measure: it evaluates that
measure in a scoped sub-query (same dimensions, scoped to the joined model),
then LEFT-JOINs the result back on the shared dimensions.

Upshot:

```json
{
  "source_model": "orders",
  "fields": ["customers.*:count"],
  "dimensions": ["customers.name"]
}
```

gives exactly the same answer as:

```json
{
  "source_model": "customers",
  "fields": ["*:count"],
  "dimensions": ["name"]
}
```

## Diamond joins

When the same model is reachable via multiple paths (e.g.
`orders → customers → regions` AND `orders → warehouses → regions`), each path
produces a **separate** sub-query with its own alias:

- `customers.regions.name` → alias `customers__regions`
- `warehouses.regions.name` → alias `warehouses__regions`

```json
{
  "source_model": "orders",
  "fields": ["*:count"],
  "dimensions": ["customers.regions.name", "warehouses.regions.name"]
}
```

If you really want to re-collapse the diamond, add a model filter that equates
them (using `__` syntax):

```yaml
filters:
  - "customers__regions.id = warehouses__regions.id"
```

## See also

- `help(topic='models')` — declaring joins and the `__` alias convention.
- `help(topic='extending')` — adding ad-hoc joins via `ModelExtension`.
- `help(topic='queries')` — dotted dimensions and time dimensions.
