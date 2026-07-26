# Models

A model maps a database table (or SQL subquery) to queryable columns and measures. This page
covers the concepts; for the schema of `create_model` / `edit_model`, see
those tools' own documentation.

## Source: sql_table vs sql

Exactly one of:

- `sql_table: "public.orders"` — a named table. The default when auto-ingesting.
- `sql: "SELECT id, created_at, amount * quantity AS revenue FROM raw_orders"`
  — an inline SQL subquery. Useful for transforming or flattening before the
  semantic layer sees the data.

Either becomes the FROM clause at query time.

## Columns

A model has a single `columns` list. Each column is a row-level SQL expression
that can be used as a group-by key, an aggregation source, or both — the role
is decided per query.

```yaml
columns:
  - {name: id, sql: id, type: number, primary_key: true}
  - {name: status, sql: status, type: string}
  - {name: created_at, sql: created_at, type: time}
  - {name: revenue, sql: amount, type: number}
  - {name: quantity, sql: qty, type: number, allowed_aggregations: [sum, avg, min, max]}
```

Types: `string`, `number`, `boolean`, `time`, `date`. `label` is optional and
propagates to query result metadata. A column's `sql` is a **row-level**
expression, not an aggregate. Plain column names are fine; for complex
expressions prefix with the model name:

```yaml
columns:
  - {name: line_total, sql: "orders.amount * orders.quantity", type: number}
```

`primary_key: true` restricts the column to `count`/`count_distinct`
aggregations regardless of its type. `allowed_aggregations` further narrows
the type-default whitelist (validated at model construction).

## Named-formula measures

`measures` is an optional library of saved formulas — same shape as a
query's inline `measures` entries. Queries reference them by bare name.

```yaml
measures:
  - name: aov
    formula: "revenue:sum / *:count"
    label: "Average Order Value"
```

## default_time_dimension

An optional model-level field naming the "canonical" time dimension. Used to
disambiguate when a query has 2+ `time_dimensions` entries (and no
`main_time_dimension` is set). Also used by `:first` / `:last` aggregations
for time column resolution. Transforms still require an explicit
`time_dimensions` entry in the query.

```yaml
name: orders
sql_table: public.orders
default_time_dimension: created_at
```

## hidden models

`hidden: true` removes the model from discovery endpoints (like
`models_summary`) but it remains queryable by name — useful for internal
building blocks that shouldn't clutter an agent's picture of the schema.

## Result column naming

SLayer returns columns as `{model}.{col}`:

| Query field | Result column |
|-------------|--------------|
| `*:count` on `orders` | `orders._count` |
| `revenue:sum` on `orders` | `orders.revenue_sum` |
| `customers.name` dimension | `orders.customers.name` |
| `customers.regions.name` multi-hop | `orders.customers.regions.name` |

Colon becomes underscore; `*:count` keeps a leading `_` so the alias never
collides with a user-defined column literally named `count`. When writing
`order` clauses, use the short canonical name (`revenue_sum`, `count`) — not
the colon form. Example: `{"column": "revenue_sum", "direction": "desc"}` or
`{"column": "count", "direction": "desc"}`.

## Saving a query as a model

Any query can be persisted as a model via `create_model_from_query` (or the
`create_model` MCP tool with a `query` parameter). Column names in the new
model use `__` to encode the original join path:

| Inner query field | New model column |
|-------------------|------------------|
| `stores.name` | `stores__name` |
| `customers.regions.name` | `customers__regions__name` |
| `revenue:sum` | `revenue_sum` |

See `memory:help.extending` for multi-stage queries using this.

## See also

- `memory:help.joins` — `joins` list and the `__` SQL alias convention.
- `memory:help.extending` — inline model extension for one-off dims/filters.
- `memory:help.filters` — model-level `filters` and filtered measures.
