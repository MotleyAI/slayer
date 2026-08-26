# Joins

{{product}} models relate to each other via **joins**. Only LEFT JOIN is supported
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
- Measure: `{"measures": ["customers.*:count"]}`
- Cross-model transform: `{"measures": [{"formula": "cumsum(customers.score:avg)"}]}`

{{product}} walks the join graph via BFS and inserts the LEFT JOINs.

In **SQL snippets** (dimension `sql`, measure `sql`, model `filters`), use the
same dotted join paths — dots now work in Mode-A SQL too:

```yaml
filters:
  - "customers.regions.name = 'US'"
```

Single-dot `customers.name` (table.column) works as well. The legacy
`__`-delimited split-alias form is no longer accepted — it is a hard error.

## Cross-model measures — why sub-queries

A dimension from a joined model is just another column to GROUP BY — no
cardinality issue. A **measure** from a joined model is different: a LEFT JOIN
can duplicate rows, so aggregating after the join would double-count.

{{product}} splits any query containing a cross-model measure: it evaluates that
measure in a scoped sub-query (same dimensions, scoped to the joined model),
then LEFT-JOINs the result back on the shared dimensions.

Upshot:

```json
{
  "source_model": "orders",
  "measures": ["customers.*:count"],
  "dimensions": ["customers.name"]
}
```

gives the same answer as the following — but only for customers that appear on
at least one order. Because the first query is rooted at `orders` and enriches
via LEFT JOIN, it omits customers with no orders; the `customers`-rooted query
below includes them (with a zero count), so the row sets can differ:

```json
{
  "source_model": "customers",
  "measures": ["*:count"],
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
  "measures": ["*:count"],
  "dimensions": ["customers.regions.name", "warehouses.regions.name"]
}
```

If you really want to re-collapse the diamond, add a model filter that equates
them (dotted paths, since model filters are SQL snippets):

```yaml
filters:
  - "customers.regions.id = warehouses.regions.id"
```

## See also

- `memory:help.models` — declaring joins and the dotted join-path convention
  (generated SQL uses `__` aliases internally).
- `memory:help.extending` — adding ad-hoc joins via `ModelExtension`.
- `memory:help.queries` — dotted dimensions and time dimensions.
