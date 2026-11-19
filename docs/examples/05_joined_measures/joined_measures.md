# Metrics from joined datasets

A key requirement for an expressive semantic layer is [relationships](../05_joins/joins.md). Once you have a relationship defined, it's easy to reuse dimensions from child datasets — you've already joined the underlying table, just refer to dimension definitions prefixed with the right subquery alias.

Using *metrics* from child datasets is harder. Why not just do all the joins, then apply the metric definition to the corresponding columns in the joined-up expression? Because the join might change the cardinality (number of rows) in the pre-aggregation result, thus breaking aggregations such as sum or average.

For metrics from joined datasets to be useful, we must ensure that querying a metric from a joined dataset, grouped only by dimensions available in that joined dataset, gives the same result as querying that metric directly from the joined dataset. So for example if `orders` has a relationship to `customers`, and `customers` has a dimension `name`, then these two queries must give the same result:

```json
{"population": "orders", "metrics": ["count(customers.*)"], "dimensions": ["customers.name"]}
```

```json
{"population": "customers", "metrics": ["count(*)"], "dimensions": ["name"]}
```

How do we achieve that? Through a sub-query. Suppose we have a query that references a metric from a joined dataset. We split that query into two parts: one that contains the [cross-dataset metric](../../concepts/queries.md#cross-dataset-metrics) reference, and the other that contains all the other fields. We evaluate the second one as usual; and for the first one, we change the population to the dataset of that metric, then drop all dimensions that are not reachable from that dataset.

We then evaluate both queries (the results may have different cardinality because the dimensions in one are only a subset of the other), and left join the results to each other by all the dimensions they share.

This way we guarantee that the values of that joined metric are exactly the same as in the original — as that is exactly how it's evaluated.

[Transforms](../../concepts/formulas.md) like `cumsum()` and `change()` work on cross-dataset metrics too — the transform is applied after the sub-query join:

```json
{
  "population": "orders",
  "time_dimensions": [{"field": "ordered_at", "granularity": "month"}],
  "metrics": [
    "count(customers.*)",
    {"metric": "cumsum(count(customers.*))", "alias": "cumulative_customers"}
  ]
}
```

---

See the [companion notebook](joined_measures_nb.ipynb) for runnable code demonstrating cross-dataset metrics.
