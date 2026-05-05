# Multi-stage queries, made easy

Most semantic layers and BI tools basically parameterize a GROUP BY, maybe with a couple of twists. But what if you need something more?

- **Example 1:** Average monthly revenue per store. First calculate total revenue, grouped by store and month; then average the result across months per store.
- **Example 2:** Orders grouped by customer activity bucket. First calculate order count per customer, bucket that, then use the bucketed result as a dimension to group orders by.

In many architectures, this is treated as a bolt-on. For example, Cube.js supports a specific, named list of multistage measures that you have to specify with a special syntax inside the measure definition. This is also how we initially did it in Motley.

But one day, I had an insight: SLayer can automatically generate a model definition by introspecting an SQL query (string columns become dimensions, numeric columns become measures — one measure per column — and so on). But each SLayer query resolves to an SQL query!

## Queries as models

In most semantic layers and BI tools, queries and models are completely different beasties. Models define the available dimensions and measures, and queries request them and get data back.

In SLayer, things are more dynamic. The reasoning is simple: a SLayer query resolves to a SQL query to the datasource (the one that we use to fetch the query's result). At the same time, SLayer's [introspection](../../concepts/ingestion.md) allows us to take any SQL SELECT query and define a model from it, generating measures and dimensions from the columns that that SQL query returns, according to the type of these columns.

Put these two together, and hey presto: any query automatically implies a model!

Actually there's a bit more to it — for example, automatic propagation of metadata such as labels or descriptions from both the source model and any labels defined in the query itself — but that is the basic idea.

## How queries as models enable multi-stage queries

Combined with another powerful SLayer feature — [inline joins and dimensions](../../concepts/queries.md#modelextension) — this makes multi-stage queries a natural, effortless thing.

For the first example above, all you need to do is use the (revenue by store and month) query as the root model of a second query. Pass both as a [query list](../../concepts/queries.md#query-lists):

```json
[
  {
    "name": "monthly_store_revenue",
    "source_model": "orders",
    "measures": ["order_total:sum"],
    "dimensions": ["stores.name"],
    "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}]
  },
  {
    "source_model": "monthly_store_revenue",
    "measures": ["order_total_sum:avg"],
    "dimensions": ["stores.name"]
  }
]
```

The inner query produces (store, month, revenue) rows. The outer query uses the inner's name as `source_model` and requests `order_total_sum:avg` — aggregating the inner query's `order_total_sum` measure with `avg` at query time.

The second example is more elaborate, as we have two logical steps: first, calculate the order count per customer; then, bucket it and use the bucketed value as a dimension in the parent query.

As we want to use a result of a child query as a dimension, we use a [dynamic join](../../concepts/queries.md#modelextension) inside the parent query to make it available. For the bucketing, we use an inline dimension with a CASE expression; since the child query is a joined model like any other, we reference its columns using the standard `table.column` syntax in the dimension's SQL:

```json
[
  {
    "name": "customer_activity",
    "source_model": "orders",
    "measures": ["*:count"],
    "dimensions": ["customer_id"]
  },
  {
    "source_model": {
      "source_name": "orders",
      "joins": [{"target_model": "customer_activity", "join_pairs": [["customer_id", "customer_id"]]}],
      "columns": [{"name": "activity_bucket", "sql": "CASE WHEN customer_activity._count >= 500 THEN 'High' WHEN customer_activity._count >= 200 THEN 'Medium' ELSE 'Low' END", "type": "string"}]
    },
    "measures": ["*:count", "order_total:sum"],
    "dimensions": ["activity_bucket"]
  }
]
```

The inner query computes total orders per customer. The outer query joins this result to `orders` via `ModelExtension`, defines a CASE-based bucket dimension, and groups orders by that bucket.

## Stages can reference each other — DAGs, not just chains

Stages in a query list are not restricted to a linear pipeline. Any stage may use any *prior* named sibling stage as its `source_model` or as a `joins.target_model`, so several rollups can run in parallel and feed a single final stage. For example, "max per-customer order total" computed by joining a per-customer rollup back to `customers`:

```json
[
  {
    "name": "kpis",
    "source_model": "orders",
    "dimensions": ["customer_id"],
    "measures": ["order_total:sum"]
  },
  {
    "name": "tagged",
    "source_model": {
      "source_name": "customers",
      "joins": [{"target_model": "kpis", "join_pairs": [["id", "customer_id"]]}]
    },
    "dimensions": ["name", "kpis.order_total_sum"]
  },
  {
    "source_model": "tagged",
    "measures": ["kpis__order_total_sum:max"]
  }
]
```

Stage 1 aggregates orders per customer; stage 2 — itself a non-final stage — joins that result back to `customers` to attach the per-customer total to each row; stage 3 takes the max. Forward references (`a → b` where `b` is later in the list) and self references are rejected with a clear error.

---

See the [companion notebook](multistage_queries_nb.ipynb) for runnable code demonstrating these examples.
