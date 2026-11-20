# SQL or custom expressions?

Every semantic layer, by its very nature, defines a domain-specific language (DSL) that contains some operations that are natural semantically but nontrivial to implement in SQL - that’s one of its main sources of value. 

However, these can never cover every possible eventuality that raw SQL can express, nor does it make sense to fully duplicate SQL in the DSL. 

This leads to a challenge: how to combine the simplicity of custom transforms (such as `time_shift` or `last` for example) that are natural semantically, with the power that raw SQL can offer, without making a mess of the syntax? 

Many semantic layers achieve that via some sort of escape syntax, such as {} or $, that allows to inject DSL expressions into the SQL used in defining fields. In a way, that option is almost the only one available if the only place for any nontrivial operations or expressions is in field definitions, and all queries do is retrieve these.

In SLayer, we chose a different path: the dataset defines the abstraction between the DSL and the underlying database, so the `sql` and `filter` properties of a dataset's `fields` (and the dataset-level `filters` list) only allow raw SQL. And the query underlying a `sql`-mode dataset can be any valid SQL SELECT expression at all.

Any [transforms](../../concepts/expressions.md#transform-functions) from our DSL are then applied at query time inside a query's `metrics` list; the `filter` list of a query likewise only refers to DSL-side entities — field names, query-time aggregations, transforms, and named expressions from that query.

What if a query needs an expression the dataset doesn't define? Write it right where it is used, in the DSL: a query carries no raw SQL, so the expression uses SLayer's portable function library and the dataset's field names.

```json
{
  "population": "orders",
  "metrics": ["count(orders.*)", "sum(orders.order_total)"],
  "dimensions": ["stores.name"],
  "filter": ["day_of_week(orders.ordered_at) IN (6, 7)"]
}
```

Here, `day_of_week(orders.ordered_at) IN (6, 7)` keeps the weekend orders (ISO numbering, Monday = 1) on every database. An expression that can only be phrased in a database's own SQL belongs on the dataset side: add it to the dataset as a field with that `sql`, and filter on the field by name.

What if you want to get fancy, and use expressions such as time-shift for defining derived metrics or dimensions?

That is also doable — save the query as a [query-backed dataset](../../concepts/datasets.md#query-backed-datasets) (via `engine.create_dataset_from_query()` or the `create_dataset` MCP tool with a `query` argument). The result becomes a regular dataset you can query by name or use as `population` in other queries. See the [Multistage Queries tutorial](../06_multistage_queries/multistage_queries.md) for a worked example.

---

See the [companion notebook](sql_vs_dsl_nb.ipynb) for runnable code demonstrating the SQL/DSL boundary.