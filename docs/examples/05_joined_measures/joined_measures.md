# Measures from joined models

A key requirement for an expressive semantic layer is [joins](../05_joins/joins.md). Once you have a join defined, it's easy to reuse dimensions from child models — you've already joined the underlying table, just refer to dimension definitions prefixed with the right subquery alias.

Using *measures* from child models is harder. Why not just do all the joins, then apply the measure definition to the corresponding columns in the joined-up expression? Because the join might change the cardinality (number of rows) in the pre-aggregation result, thus breaking aggregations such as sum or average.

For measures from joined models to be useful, we must ensure that querying a measure from a joined model, grouped only by dimensions available in that joined model, gives the same result as querying that measure directly from the joined model. So for example if `orders` has a join to `customers`, and `customers` has a dimension `name` and a measure `count`, then these two queries must give the same result:

```json
{"source_model": "orders", "fields": ["customers.count"], "dimensions": ["customers.name"]}
```

```json
{"source_model": "customers", "fields": ["count"], "dimensions": ["name"]}
```

How do we achieve that? Through a sub-query. Suppose we have a query that references a measure from a joined model. We split that query into two parts: one that contains the [cross-model measure](../../concepts/queries.md#cross-model-measures) reference, and the other that contains all the other fields. We evaluate the second one as usual; and for the first one, we change the source model to the model of that measure, then drop all dimensions that are not reachable from that model.

We then evaluate both queries (the results may have different cardinality because the dimensions in one are only a subset of the other), and left join the results to each other by all the dimensions they share.

This way we guarantee that the values of that joined measure are exactly the same as in the original — as that is exactly how it's evaluated.

[Transforms](../../concepts/formulas.md) like `cumsum()` and `change()` work on cross-model measures too — the transform is applied after the sub-query join:

```json
{
  "source_model": "orders",
  "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}],
  "fields": [
    "customers.count",
    {"formula": "cumsum(customers.count)", "name": "cumulative_customers"}
  ]
}
```

---

See the [companion notebook](joined_measures.ipynb) for runnable code demonstrating cross-model measures.
