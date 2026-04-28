# SLayer — conceptual help

SLayer is a lightweight semantic layer for AI agents. Instead of writing raw SQL,
you describe what data you want — **measures**, **dimensions**, **filters** — and
SLayer generates and executes the query against your database.

## Core entities

- **datasource** — a database connection (postgres, mysql, sqlite, duckdb, …).
- **model** — a named mapping from a table (or SQL subquery) to queryable fields.
- **dimension** — a column to group/filter by (e.g. `status`, `created_at`).
- **measure** — a named row-level SQL expression on a model (e.g. `{"name": "adams_revenue", "sql": "amount", "filter": "customers.name='Adam'"}`).
  Not an aggregate — aggregation is chosen at query time. 
- **aggregation** — how a measure is rolled up: `sum`, `avg`, `count`, `weighted_avg`, …
  Applied via colon syntax: `revenue:sum`.
- **field** — one output column of a query. A formula over measures and aggregations; normal arithmetic expressions work. 
  It's fine to have a query with just dimensions and no fields.
- **filter** — a condition that restricts rows (WHERE or HAVING, routed automatically).
- **join** — a LEFT-JOIN relationship between two models. Joins let you reach
  another model's dimensions/measures via dotted paths like `customers.regions.name`.
- **time dimension** — a time column queried with a granularity
  (`day`/`week`/`month`/…), producing one row per time bucket.

## The query shape

```json
{
  "source_model": "orders",
  "fields": ["*:count", "revenue:sum / orders.amount:sum"],
  "dimensions": ["status"],
  "filters": ["status <> 'cancelled'", "customers.regions.name='Asia'"],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
  "order": [{"column": "customers.revenue:sum", "direction": "desc"}],
  "limit": 10
}
```

You can add ad hoc measures, dimensions, etc to the source_model, like
```json
{
  "source_model": {
    "source_name": "orders",
    "measures": [{"name": "adams_revenue", "sql": "amount", "filter": "customers.name='Adam'"}],
    ...
  }
...
}
```

## Things that are easy to get wrong

1. **Measures are not aggregates.** A measure is just a named SQL expression.
   Pick the aggregation at query time with colon syntax: `revenue:sum`,
   `revenue:avg`, `price:weighted_avg(weight=quantity)`.

2. **Use `*:count` for counting rows.** `*:count` is `COUNT(*)` and is always
   available without a measure definition. When you just need to count records,
   use `*:count` — not a primary-key column. Only add that to queries when you actually need it.
   You can also aggregate dimensions directly: `customer_id:count_distinct` for `COUNT(DISTINCT customer_id)`.

3. **Joined data is reached via DOTTED paths, not by JOINing manually.**
   `customers.regions.name` on a query of `orders` auto-walks the join graph
   (`orders → customers → regions`). Don't try to add SQL joins yourself.

4. **Filters on measures or computed fields route themselves.** `"amount > 100"`
   becomes WHERE; `"revenue:sum > 1000"` becomes HAVING; `"change(revenue:sum) > 0"`
   becomes a post-filter on an outer wrapper query. Write the condition; SLayer
   decides where it lands.

5. It's critically important to choose the right source_model for a query. Put EXTRA THOUGHT into that.

6. When picking a measure for a query, MAKE SURE to consider the underlying values range 
   shown under "values" in inspect_model. If that's all NULL, maybe that's not the measure you want.

7. **`time_shift`, `change`, `change_pct` can only wrap aggregated measures** —
   e.g. `time_shift(revenue:sum, -1)`, `change(amount:avg)`. They cannot wrap
   other transforms or arithmetic expressions (`change(cumsum(x))` won't work).
   The reverse direction is fine: `cumsum(change(x))` works because window
   transforms *can* wrap self-join transforms.

## Deep dives

Call `help(topic='...')` for detail pages on specific subjects.
Available topics: `queries`, `formulas`, `aggregations`, `transforms`,
`time`, `filters`, `joins`, `models`, `extending`, `workflow`.

Recommended starting order for an unfamiliar agent: `help(topic='workflow')` for
tool-chaining, then `help(topic='queries')` for the query model.
