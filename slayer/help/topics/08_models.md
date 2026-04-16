# Models

A model maps a database table (or SQL subquery) to queryable fields. This page
covers the concepts; for the schema of `create_model` / `edit_model`, see
those tools' own documentation.

## Source: sql_table vs sql

Exactly one of:

- `sql_table: "public.orders"` — a named table. The default when auto-ingesting.
- `sql: "SELECT id, created_at, amount * quantity AS revenue FROM raw_orders"`
  — an inline SQL subquery. Useful for transforming or flattening before the
  semantic layer sees the data.

Either becomes the FROM clause at query time.

## Dimensions

```yaml
dimensions:
  - {name: id, sql: id, type: number, primary_key: true}
  - {name: status, sql: status, type: string}
  - {name: created_at, sql: created_at, type: time}
```

Types: `string`, `number`, `boolean`, `time`, `date`. `label` is optional and
propagates to query result metadata.

## Measures

```yaml
measures:
  - {name: revenue, sql: amount}
  - {name: quantity, sql: qty, allowed_aggregations: [sum, avg, min, max]}
```

A measure's `sql` is a **row-level** expression, not an aggregate. Plain column
names are fine; for complex expressions prefix with the model name:

```yaml
measures:
  - {name: line_total, sql: "orders.amount * orders.quantity"}
```

## default_time_dimension

An optional model-level field naming the "canonical" time dimension. Transforms
and `:first` / `:last` aggregations fall back to it when the query's own time
dimensions don't disambiguate.

```yaml
name: orders
sql_table: public.orders
default_time_dimension: created_at
```

## hidden models

`hidden: true` removes the model from discovery endpoints (like
`datasource_summary`) but it remains queryable by name — useful for internal
building blocks that shouldn't clutter an agent's picture of the schema.

## Result column naming

SLayer returns columns as `{model}.{col}`:

| Query field | Result column |
|-------------|--------------|
| `*:count` on `orders` | `orders.count` |
| `revenue:sum` on `orders` | `orders.revenue_sum` |
| `customers.name` dimension | `orders.customers.name` |
| `customers.regions.name` multi-hop | `orders.customers.regions.name` |

Colon becomes underscore; `*:count` collapses to `count`. Remember these when
writing `order` clauses — use the canonical name (`revenue_sum`, `count`), not
the colon form. Example: `{"column": "revenue_sum", "direction": "desc"}`.

## Saving a query as a model

Any query can be persisted as a model via `create_model_from_query` (or the
`create_model` MCP tool with a `query` parameter). Column names in the new
model use `__` to encode the original join path:

| Inner query field | New model column |
|-------------------|------------------|
| `stores.name` | `stores__name` |
| `customers.regions.name` | `customers__regions__name` |
| `revenue:sum` | `revenue_sum` |

See `help(topic='extending')` for multi-stage queries using this.

## See also

- `help(topic='joins')` — `joins` list and the `__` SQL alias convention.
- `help(topic='extending')` — inline model extension for one-off dims/filters.
- `help(topic='filters')` — model-level `filters` and filtered measures.
