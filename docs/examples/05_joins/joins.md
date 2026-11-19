# Relationships in SLayer

Joins are an important concept when dealing with tabular data, as are foreign keys - a closely related concept. For example, you could have an `orders` table with a `customer_id` column that is a foreign key pointing to the `id` field in the `customers` table containing customer details. When you want to retrieve some customer details along with each order, you’d join the two tables via this pair of columns. 

In SQL, there are many kinds of joins serving various purposes; SLayer follows the example of other semantic layers such as Cube.js by initially only supporting left joins, as the kind most frequently used for data enrichment.

If you think of left joins as edges of a graph whose vertices are datasets, then the metrics, dimensions, **and filters** in a dataset have access to the columns of not just the SQL expression underlying that dataset (the “sql” field of the dataset definition), but also of those underlying any dataset that is reachable from that dataset in the relationship graph.

The graph may contain cycles; a route hop always binds the edge it names, so in a cycle `x → y → z → x` the route `x.z` follows the direct edge between `x` and `z`, never the route through `y`, while a reference starting at `z` (`z.amount`) is reached from `x` by its only to-one path, `x → y → z`.

The data model for a SLayer relationship is covered in the [Datasets section](../../concepts/models.md#relationships), it consists of a target dataset plus list of column pairs (of the sql expressions underlying the datasets) to join on.

## Referencing joined datasets in queries

SLayer refers to metrics and dimensions in queries using a dot syntax that starts at a dataset. That is, if dataset_a has a relationship to dataset_b, and dataset_b has a relationship to dataset_c, then a query on dataset_a can refer to a dimension defined in dataset_c as `dataset_c.my_dimension`, reached by its only to-one path, or spell the route explicitly as `dataset_a.dataset_b.dataset_c.my_dimension`, and similarly for metrics. 

The explicit route may seem verbose, but avoids ambiguity when there are multiple ways of reaching a given dataset within the relationship graph. 

## Referencing joined datasets in sql snippets

As described in the discussion of [SQL vs expressions](../02_sql_vs_dsl/sql_vs_dsl.md), you will use SQL snippets when defining metrics, dimensions, and filters at dataset level. 

You reference fields of joined datasets with the same **dotted** relationship-path syntax as above: if dataset_a has a relationship to dataset_b, and dataset_b has a relationship to dataset_c, then the metrics, dimensions and filters in dataset_a refer to a column from the underlying query of dataset_c as `dataset_b.dataset_c.column_name`. Dots separate relationship hops and the leaf column follows the final dot; SLayer resolves the path through the relationship graph and emits the correct subquery aliases at resolution time. (The legacy `__`-delimited split-alias form is no longer accepted — it is a hard error.)

## Auto-ingesting schemas

When auto-ingesting a schema, SLayer introspects foreign key relationships and creates a **direct** relationship for each FK on a table (one hop only). Multi-hop reachability (e.g. `orders → customers → regions`) is not baked in at ingestion; instead, at query time SLayer walks each intermediate dataset's own relationships to resolve the full path and generate the correct SQL.

## Recombining relationship patterns

For now, if there are multiple ways in the relationship graph to reach a given dataset, we treat these as separate copies when constructing queries, de facto turning the relationship graph into a tree. For example, if we have relationships like A → B → C and A → D→ C, then B.C.column1 and D.C.column1 will refer to separate subqueries in the SLayer-constructed query. 

If that is not the desired behavior, add a filter `B.C.column1 = D.C.column1` to the query, or to the dataset to make it permanent (dotted paths work in both), re-creating the diamond pattern.

## Dynamic relationships

Finally, it’s worth reminding the reader that relationships can be added to a dataset at query time via [query extensions](../../concepts/queries.md#query-extensions). This is especially useful to join datasets that are themselves dynamically created as the result of a query (see the upcoming post on how that mechanism enables powerful and elegant multi-stage query semantics).

---

See the [companion notebook](joins_nb.ipynb) for runnable code demonstrating all relationship patterns.