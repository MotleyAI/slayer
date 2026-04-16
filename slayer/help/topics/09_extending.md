# Extending

Three mechanisms let you reshape a query's world at query time without
editing the stored model.

## ModelExtension — inline tweaks to source_model

`source_model` can be a plain string (model name) **or** an object that extends
a named model for the lifetime of this query:

```json
{
  "source_model": {
    "source_name": "orders",
    "dimensions": [{
      "name": "tier",
      "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END",
      "type": "string"
    }],
    "joins": [{"target_model": "customer_scores", "join_pairs": [["customer_id", "id"]]}],
    "filters": ["subtotal > tax_paid * 5"]
  },
  "fields": ["*:count", "revenue:sum"],
  "dimensions": ["tier"]
}
```

Fields allowed on `ModelExtension`: `dimensions`, `measures`, `joins`,
`filters`. All optional. The stored `orders` model is not modified.

Use this when the concept is one-off — don't clutter the persisted model with
it.

## Query lists — named sub-queries

Pass a **list** of queries to the query tool / engine. Every earlier query
gets a `name`; later queries reference that name as if it were a stored model.
The **last** query is the main one whose results are returned.

```json
[
  {
    "name": "monthly_store_revenue",
    "source_model": "orders",
    "fields": ["revenue:sum"],
    "dimensions": ["stores.name"],
    "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
  },
  {
    "source_model": "monthly_store_revenue",
    "fields": ["revenue_sum:avg"],
    "dimensions": ["stores.name"]
  }
]
```

Named queries **shadow** stored models if they share a name. To join a stored
model to a named query, use `ModelExtension` (`source_model` object) on the
outer query to declare the join explicitly.

Columns of the inner query become measures and dimensions of the outer. Any
dotted name in the inner (`stores.name`) is rewritten to `__`
(`stores__name`) — there is no join in the virtual model to walk.

## create_model_from_query — persist as a real model

If the sub-query is useful beyond one call, persist it. Via MCP: call
`create_model` with a `query` parameter. The query's SQL becomes the source
and dimensions and measures are auto-introspected. It then behaves like any
other model — editable, queryable by name.

## When to use which

| Need | Use |
|------|-----|
| One-off expression dimension | `ModelExtension.dimensions` |
| One-off filter in SQL | `ModelExtension.filters` |
| Re-aggregate an aggregate | Query list (two queries) |
| Persist a multi-stage result | `create_model_from_query` |
| Join a named sub-query into the main query | `ModelExtension.joins` + query list |

## See also

- `help(topic='models')` — permanent model shape; `hidden: true` for internal ones.
- `help(topic='joins')` — the `__` vs dot convention for joined paths.
- `help(topic='workflow')` — when to extend inline vs edit the stored model.
