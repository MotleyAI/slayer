# SQL or custom expressions?

Every semantic layer, by its very nature, defines a domain-specific language (DSL) that contains some operations that are natural semantically but nontrivial to implement in SQL - that’s one of its main sources of value. 

However, these can never cover every possible eventuality that raw SQL can express, nor does it make sense to fully duplicate SQL in the DSL. 

This leads to a challenge: how to combine the simplicity of custom transforms (such as `time_shift` or `last` for example) that are natural semantically, with the power that raw SQL can offer, without making a mess of the syntax? 

Many semantic layers achieve that via some sort of escape syntax, such as {} or $, that allows to inject DSL expressions into the SQL used in defining measures or dimensions. In a way, that option is almost the only one available if the only place for any nontrivial operations or expressions is in measure/dimension definitions, and all queries do is retrieve these. 

In SLayer, we chose a different path: The model defines the abstraction between the DSL and the underlying database; so in the measure, dimension and filter definitions **inside a model,** only raw SQL is allowed. And of course the query underlying a model can be any valid SQL SELECT expression at all.

Any [transforms](../../concepts/formulas.md#transform-functions) from our DSL are then done at query time, in the fields attribute of a query definition; the filter definitions in the “filters” field of a query likewise only refer to the DSL-side entities such as dimensions, measures, and fields from that query.

What if you want to add to a query a filter that directly references the underlying tables, and can only be phrased in terms of SQL, rather than DSL-side entities? Just use the [dynamic model extension feature](../../concepts/queries.md#modelextension) of a query, and add the desired filter to the model definition right inside the query.

```json
{
  “source_model”: {
    “source_name”: “orders”,
    “filters”: [“subtotal > tax_paid * 5”]
  },
  “fields”: [{“formula”: “count”}, {“formula”: “order_total_sum”}],
  “dimensions”: [{“name”: “stores.name”}]
}
```

Here, `subtotal > tax_paid * 5` is a raw SQL condition on the underlying table columns — it's added to the model definition via `ModelExtension`, not to the query's filters. The query's own `fields` and `dimensions` still use DSL-level names.

What if you want to get fancy, and use expressions such as time-shift for defining derived measures or dimensions?

That is also doable - all you need to do is to use the [“Query result as model” semantics](../../concepts/models.md#creating-models-from-queries). That powerful mechanism will be covered in an upcoming post.

---

See the [companion notebook](sql_vs_dsl_nb.ipynb) for runnable code demonstrating the SQL/DSL boundary.