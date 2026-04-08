# Measures from joined models

A key requirement for an expressive semantic layer is [joins](link to joins.md). Once you have a join defined, it’s very easy to enable the reuse of dimensions from child models - you’ve already joined the underlying table, just refer to dimension definitions prefixed with the right subquery alias. 

Using *measures* from child models is harder. Why not just do all the joins, then apply the measure definition to the corresponding columns in the joined-up expression? Because the join might change the cardinality (number of rows) in the pre-aggregation result, thus breaking aggregations such as sum or average.

For measures from joined models to be useful, we must make sure that if we query a measure from a joined model, grouped only by dimensions that are available in that joined model, the result must be the same as if we just queried the same measure and dimension with that model as source. So for example if we have `model_a` that has a join to `model_b` , and `model_b`  has a dimension `dim` and a measure `m`, then the query {source_model=’model_a’, fields={formula=”m”}, dimensions=[{name=”dim”}]}} must give the same result as the version with source_model = `model_a`.

How do we achieve that? Through a subquery of course. Suppose we have a query that references a measure from a joined model. Then we split that query into two parts: one that contains the fields entry with the reference to a joined measure, and otherwise identical to the original one; and the other that contains all the other fields entries, and is otherwise identical to the original one. We evaluate the second one as usual; and as for the first one, we change the source model to the model of that measure, then drop all dimensions that are not reachable from that model. 

We then evaluate both queries (the results will in general have different cardinality because the dimensions in one are only a subset of the other), and outer join (so we don’t lose any rows) the results to each other by all the dimensions they share. 

This way we guarantee that the values of that joined measure are exactly the same as in the original - as that is exactly how it’s evaluated!