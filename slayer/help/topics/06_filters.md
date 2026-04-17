# Filters

Filters are formula strings. They go in the query's `filters` list and/or on
a model's `filters` list. SLayer routes them to the right SQL stage
automatically — there is no explicit HAVING keyword.

## Operators

| Operator | Example |
|----------|---------|
| `=` | `"status = 'active'"` |
| `<>` | `"status <> 'cancelled'"` |
| `>` `>=` `<` `<=` | `"amount >= 100"` |
| `in` | `"status in ('a', 'b')"` |
| `IS NULL` / `IS NOT NULL` | `"discount IS NULL"` |
| `like` / `not like` | `"name like '%acme%'"` |

## Boolean logic

Combine with `and`, `or`, `not` inside a **single** string:

```json
{
  "source_model": "orders",
  "fields": ["*:count"],
  "filters": ["status = 'completed' or status = 'pending'"]
}
```

Multiple entries in the `filters` list are AND-ed:

```json
{
  "source_model": "orders",
  "fields": ["*:count"],
  "filters": ["status = 'completed'", "amount > 100"]
}
```

## Auto-routing (where does each filter land?)

- Filter references only dimensions / raw columns → **WHERE** (inner query).
- Filter references an aggregated measure (e.g. `revenue:sum > 1000`) →
  **HAVING**.
- Filter references a transform or computed field (e.g.
  `change(revenue:sum) > 0`) → **post-filter** on an outer wrapper.

Inner and outer filters can mix in one query — SLayer splits them.

## Filtering on computed fields

Reference a named field from `fields` by its `name`:

```json
{
  "source_model": "orders",
  "fields": [
    "revenue:sum",
    {"formula": "change(revenue:sum)", "name": "rev_change"}
  ],
  "filters": ["rev_change < 0"],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

Or write the transform **inline** in the filter — no need to add it to `fields`:

```json
{
  "source_model": "orders",
  "fields": ["revenue:sum"],
  "filters": ["last(change(revenue:sum)) < 0"],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

Limitation: bare measure renames like `{"formula": "*:count", "name": "n"}`
cannot be filtered by `n`. Reference the underlying `*:count` instead.

## Filtered measures — CASE WHEN inside an aggregate

A **measure** can carry a `filter` that restricts which rows participate in the
aggregate. It becomes CASE WHEN inside the aggregate, so other measures in the
same query are not affected:

```yaml
measures:
  - name: active_revenue
    sql: amount
    filter: "status = 'active'"
```

`active_revenue:sum` → `SUM(CASE WHEN status = 'active' THEN amount END)`.
Combine arithmetically: `{"formula": "active_revenue:sum / revenue:sum", "name": "active_share"}`.

## Model-level filters

Always-applied WHERE conditions on the underlying table:

```yaml
name: active_orders
sql_table: public.orders
filters:
  - "deleted_at IS NULL"
  - "status <> 'test'"
```

These are WHERE-only. They do not reference measures or transforms.

## See also

- `help(topic='formulas')` — parsing rules shared with `fields`.
- `help(topic='transforms')` — the transforms you can wrap in a filter.
- `help(topic='queries')` — where filters sit in the evaluation order.
