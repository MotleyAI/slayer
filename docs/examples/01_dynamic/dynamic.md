# Make it dynamic!

SLayer's intended audience is **agents and humans formulating ad hoc queries**, not dashboards replaying the same request day in, day out. 

For the dashboard case, pre-computing every aggregate can be a crucial performance booster, and that requires specifying them all upfront.  For the ad hoc case, it's the wrong trade-off — the query you need next is the one nobody foresaw when the dataset was written, so the priority shifts to **how easily you can shape the query at the moment you write it**. 

The rest of this post is a tour of what "shape it at the moment you write it" actually means in SLayer.

## Query-time transforms and aggregations

The first form of dynamism is **query-time transforms**: expressions defined right inside the query, not the dataset. Time-shifted metrics, period-over-period changes, cumulative sums, ratios between metrics — all computed at query time, no dataset edit required. The time-specific family (time shifts, `change`, and `last` over an aggregate) is covered in depth in the [time dimensions post](../04_time/time.md).

Closely related is **query-time aggregation**. A metric in SLayer is just a **named row-level expression** with the choice of aggregation deferred to query time.

At the semantic level this is the natural framing: *aggregation is a property of the question, not of the metric*. Asking for the sum, average, or median of the same underlying quantity is the same question at different resolutions, so a semantic layer should let you pick the resolution in the query, not bake a new named metric into the dataset for each combination.

In contrast, most semantic layers force you to pre-declare one metric per (expression × aggregation) pair, which multiplies the dataset surface area fast.

SLayer's answer is to pick the aggregation at query time: `sum(orders.revenue)`, `avg(orders.revenue)`, `percentile(orders.revenue, p=0.25)`. 
[Custom aggregations](../07_aggregations/aggregations.md) defined with SQL templates and parameters (weighted averages, percentiles, trimmed means) plug in the same way.

Contrast this with pre-definition-heavy semantic layers. In something like Cube.js, every new shape of question requires a pre-declared measure or dimension in the cube: time-shifted revenue is one measure, its change over period another, the same pair repeated for every granularity and time column you care about, plus every bucket dimension you might plausibly want. The model definition explodes combinatorially — and the resulting thing has to fit into an agent's context window. 

SLayer trades pre-computation for the ability to **write the exact query you need, once, in the space where you were going to write it anyway**. But that is not all!

## Extending the dataset inline

The second form of dynamism is **extending the dataset for the lifetime of one query**. Sometimes a dimension only makes sense for the question you're asking right now: group by whether some floating-point number is positive, bucket a continuous value by derived category, filter on a condition that won't come up again. 

The semantic concept is trivial; the SQL that expresses it is a view, a dbt model, or a CTE chain, and making a commit to a source controlled config repo to change any of those for a one-off is disproportionate.

SLayer's answer are dataset extension semantics: when constructing a query, the `population` can be a plain dataset name, or it can be that dataset name plus extra fields, named-expression metrics, filters, and [relationships](../05_relationships/relationships.md) appended for the lifetime of this single query. 

The persisted dataset stays untouched.

## Dynamic relationships — the surprising one

You might reasonably ask why one would want to add *relationships* at query time. If there's a relationship between this dataset and another, shouldn't it live in the dataset definition?

Often, yes. But not always — because **queries themselves can be used as datasets**. A SLayer query resolves to a SQL query, and SLayer's [introspection](../../concepts/ingestion.md) already turns any SQL query into a [query-backed dataset](../../concepts/datasets.md#query-backed-datasets) by deriving its `fields` from the query's output. 

Dynamic relationships are what stitch a query-as-dataset back into a bigger query.



## Multistage queries fall out of two simple pieces

Put queries-as-datasets together with inline relationships and you get **[multistage queries](../06_multistage_queries/multistage_queries.md)** — not as a bolt-on with its own syntax, but as a natural consequence of the two features above. Two patterns this makes easy, both of which are genuinely awkward in most semantic layers:

- **Nested aggregation**: sum revenue per store, then average those store totals across months or regions. A single SQL pass can't express this; SLayer expresses it as a **two-element query list** where the outer query references the inner by name.
- **Grouping by a calculated dimension**: bucket customers by their total spend, then count how many fall in each bucket. The bucket depends on an aggregate — so the inner query computes the totals, and the outer query extends the inner query's output with a `CASE WHEN` field.

Each of these is **simple at the semantic level and non-trivial at the SQL level**. That's exactly the gap a good semantic layer should close — and SLayer closes it in the query, not the dataset, saving agent tokens and keeping the dataset small enough to fit.

Here is the nested-aggregation example, written out in full. The inner query computes monthly revenue per store; the outer averages across months per store:

```json
[
  {
    "name": "monthly_store_revenue",
    "population": "orders",
    "metrics": ["sum(orders.order_total)"],
    "dimensions": ["stores.name"],
    "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}]
  },
  {
    "population": "monthly_store_revenue",
    "metrics": ["avg(monthly_store_revenue.order_total_sum)"],
    "dimensions": ["monthly_store_revenue.stores__name"]
  }
]
```

Two queries, one list. `sum(orders.order_total)` from the inner query becomes the field `order_total_sum` in the outer (read as `monthly_store_revenue.order_total_sum`), aggregable again the same way as any other metric. See the [multistage queries post](../06_multistage_queries/multistage_queries.md) for the calculated-bucket pattern and further examples.

What would it take to ask this question in the semantic layer you're currently using?
