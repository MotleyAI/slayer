# Joins in SLayer

Joins are an important concept when dealing with tabular data, as are foreign keys - a closely related concept. For example, you could have an `orders` table with a `customer_id` column that is a foreign key pointing to the `id` field in the `customers` table containing customer details. When you want to retrieve some customer details along with each order, you’d join the two tables via this pair of columns. 

In SQL, there are many kinds of joins serving various purposes; SLayer follows the example of other semantic layers such as Cube.js by initially only supporting left joins, as the kind most frequently used for data enrichment.

If you think of left joins as directed edges of a graph whose vertices are models (we assume that the graph thus defined is acyclic, **and throw an error otherwise**), then the measures, dimensions, **and filters** in a model have access to the columns of not just the SQL expression underlying that model (the “sql” field of the model definition), but also of those underlying any model that is reachable from that model in the join graph.

The data model for a SLayer join is covered in the [Models section](../../concepts/models.md#joins), it consists of a target model plus list of column pairs (of the sql expressions underlying the models) to join on.

## Referencing joined models in queries

SLayer refers to measures and dimensions in joined models using a dot syntax. That is, if model_a has a join to model_b, and model_b has a join to model_c, then a query on model_a can refer to a measure defined in model_c as “model_b.model_c.my_dimension”, and similarly for measures. 

That may seem verbose, but avoids ambiguity when there are multiple ways of reaching a given model within the join graph. 

## Referencing joined models in sql snippets

As described in the discussion of [SQL vs expressions](../02_sql_vs_dsl/index.md), you will use SQL snippets when defining measures, dimensions, and filters at model level. 

As the multidot syntax described above would not be valid SQL, the syntax for referring to columns of the SQL expressions underlying the joined models is the same as above with double underscores substituted for dots, that is if model_a has a join to model_b, and model_b has a join to model_c, then the measures, dimensions and filters in model_a can refer to a column from the underlying query of model_c as `model_b__model_c.column_name`. These will be substituted for correct aliases for the corresponding subquery at resolution time.

## Auto-ingesting schemas

When auto-ingesting a schema, SLayer introspects foreign key relationships between different tables, and turns these into join relationships between the models corresponding to the tables. These are then resolved at query time to generate the correct SQL. 

## Recombining join patterns

For now, if there are multiple ways in the join graph to reach a given model, we treat these as separate copies when constructing queries, de facto turning the join graph into a tree. For example, if we have joins like A → B → C and A → D→ C, then B.C.column1 and D.C.column1 will refer to separate subqueries in the SLayer-constructed query. 

If that is not the desired behavior, you can add to the model a filter `B__C.column1==D__C.column1` (using the `__` alias syntax, since model filters are SQL snippets), re-creating the diamond pattern.

## Dynamic joins

Finally, it’s worth reminding the reader that joins can be added to a model at query time via [dynamic model extension](../../concepts/queries.md#modelextension). This is especially useful to join models that are themselves dynamically created as the result of a query (see the upcoming post on how that mechanism enables powerful and elegant multi-stage query semantics).

---

See the [companion notebook](joins_nb.ipynb) for runnable code demonstrating all join patterns.