# Cross-Dataset Queries

## Cross-dataset metrics

When datasets have [relationships](datasets.md#relationships), you can reference metrics from joined datasets using a dotted path inside the aggregation — `aggregation(dataset_name.metric_name)`:

```json
{
  "population": "orders",
  "metrics": [
    "count(orders.*)",
    "avg(customers.score)"
  ],
  "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]
}
```

SLayer computes the joined metric in its own sub-query **rooted at the metric's
dataset** (here `customers`) and LEFT JOINs it back to the main query — so the
aggregate runs over the target's rows once, never through a 1:N join that would
multiply them.

It reads your dimensions and filters from that dataset by its own relationship
paths ([references](references.md#identifier-resolution)), so how the
population reaches it never changes a value.

The method-call spelling `order_items.count(*)` or `order_items.sum(price * qty)` (further arguments literals only) means `count(order_items.*)` / `sum(order_items.price * order_items.qty)`.

The sub-query groups by the subset of your dimensions the engine can **prove
safe** — those reachable from the metric's dataset over relationship hops that are
provably many-to-one (a primary key on the far side, or a declared
[relationship `cardinality`](datasets.md#relationship-cardinality)). Each remaining dimension is
**associated**: each cell aggregates over the distinct rows of the metric's dataset
reaching it, and the response carries a `warnings` entry (`kind: "associated"`)
because the cells may overlap and are not additive. Declare the missing relationship
cardinality (or primary key) to make such a dimension partition the rows.

Query **filters** still restrict the metric: a conjunct the sub-query can only
reach across an unproven hop is pushed down as a correlated `EXISTS` semi-join —
the metric counts exactly the target rows related to at least one row passing
the filter (each row once, never multiplied through the join), surfaced as an
informational `kind: "semi_join_pushed"` entry in `.warnings` (never an error,
in any mode). The correlation path resolves through the same
[bidirectional traversal](datasets.md#bidirectional-traversal) as every other hop
— no declared reverse relationship is needed, and a hop spanned by two or more edges
fails the whole query with the ambiguous-hop error (in every mode) rather than
guessing. Pushdown is total over the conjunct's boolean shape: a row survives
when the predicate holds on at least one row of its join product over the
referenced branches, each hop joined as declared (LEFT by default), so
`tier = 'gold' or orders.status = 'ok'` keeps a gold customer with no orders and
`orders.id is null` reads as "no orders"; a reference with no resolvable relationship path
is refused with a typed error in every mode. On ClickHouse the semi-join needs
server ≥ 25.4 (the required setting is attached automatically), and older servers
fail with a clear error, as does a `readonly = 1` user whose profile leaves
`allow_experimental_correlated_subqueries` off (the 25.4–25.7 default) — enable it
in the user's profile, use `readonly = 2`, or upgrade to 25.8+.

`to_many_handling` chooses how such a dimension resolves — omitted or `associate`
(association, the default above), or `error` (refuse) — and a semi-join-pushed
filter is applied, never erroring, in every mode.
Example: `{"population": "orders", "dimensions": ["orders.status"], "metrics": ["sum(customers.spend)"]}` gives each status the spend of the distinct customers with an order in it.
To repeat a value across such a dimension instead, give the aggregate `partition_by=` with the dimensions it
is attributable to: `sum(customers.spend, partition_by=[])` repeats the total on every status.
An explicit `partition_by=` key the aggregate's home does not determine resolves by the same mode, local or cross-dataset: rooted at `customers` by `orders.status`, `sum(customers.spend, partition_by=orders.status)` gives each status the spend of its distinct customers without a warning, and is a typed error under `error`.

Association serves every aggregate. A dataset without a primary key uses a
per-query row identity. This includes datasets with a standalone unique field that may
contain NULLs. Each source row counts separately, and copies made by a join count once.
A list element is told apart by its parent row and its position in the list.

`first`/`last` and `window=` associate too, local or cross-dataset. `first`/`last` picks among each cell's distinct home rows by a ranking key the home determines. A windowed aggregate counts a home row once in each bucket where one of its associated rows falls in the bucket's frame, so `{"population": "customers", "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}], "metrics": ["sum(customers.spend, window='1y')"]}` gives each month the spend of the customers with an order in the trailing year. A filter on the time axis's relationship binds to that same order. Under `error`, a window whose time axis fans from its home is a typed error. An input crossing an unproven hop fails with a typed error in every mode. Each cell aggregates over the metric's own home rows by the home's relationship path, so an entity with no population row still counts in the cells its path reaches. An aggregate reads like a field of a dataset keyed by its grain, so an entity with no related row counts in the NULL cell whenever the result has one, however the query is rooted. A pushed filter binds to the same related row as a dimension it shares a hop with. An attached (aggregate- or transform-valued) parameter or source constituent is computed at its own home and attached per home row in every mode, so `weighted_avg(customers.spend, weight=sum(orders.amount, partition_by=customers.regions.name))` weights each customer by its region's order total in every mode.

Every input of an aggregate — its source, arguments (`weight=`), definition defaults, and its field-level `filter=` — is traced recursively through derived-field definitions, and an input whose expansion crosses a fanning (not provably to-one) hop fails closed with a typed error instead of silently multiplying rows.

A dataset (or query stage) that no relationship path reaches is read only through aggregates: rooted at `customers`, `"count(events.id) / count(customers.*)"` computes `count(events.id)` over every event, repeats it on every row (per bucket of a `time_spine` time dimension) with a `kind: "unrelated_dataset"` warning, and a filter on it restricts its rows, while grouping, ordering or filtering by its fields alongside other datasets' is a typed error.

A filter **on** the cross-dataset value itself (`"avg(customers.score) > 4"`)
restricts the result rows, uniformly with local aggregate filters — groups that
fail the predicate are dropped, not returned with `NULL`.

Cross-dataset aggregates also support `window=`, `partition_by=`, and `first` /
`last` (associating, as above), inside
[dimension expressions](queries.md#expression-dimensions), and as hidden
[order-only fields](queries.md#ordering-by-something-you-dont-project).

An [aggregated expression](expressions.md#expression-aggregation) may mix host and joined-dataset fields (`sum(orders.amount - customers.discount)`); it is homed on the source's own dataset when that determines every operand, else the deepest dataset that does, so the relationship hop's cardinality decides exact grain vs association exactly as for a single cross-dataset metric.

A cross-dataset **parametric** aggregate keeps its kwarg signature in the result key, so two variants on the same target field do not collide:

```json
{
  "population": "orders",
  "dimensions": ["customers.region"],
  "metrics": [
    "percentile(customers.revenue, p=0.5)",
    "percentile(customers.revenue, p=0.95)"
  ]
}
```

surfaces two distinct result keys — `customers.revenue_percentile_p_0_5` and `customers.revenue_percentile_p_0_95`. (A non-parametric `sum(customers.revenue)` surfaces as `customers.revenue_sum`.)

## Multi-hop dimensions

Dimensions from joined datasets can be referenced with dotted paths. SLayer auto-resolves multi-hop relationship chains by walking each intermediate dataset's own relationships:

```json
{
  "population": "orders",
  "dimensions": ["customers.regions.name"],
  "metrics": ["count(orders.*)"]
}
```

This walks `orders → customers → regions` via the relationship graph and resolves `name` from the `regions` dataset. Works with both ingested rollup datasets and explicit relationships.

A dotted reference may target a *derived* field on the joined dataset — i.e., a field whose own `expression` is an expression rather than a source column. The engine recursively inlines the derivation at query time, and the same chaining works whether the reference appears in a query's `dimensions` / `metrics` / `filter` or inside another dataset's field expression. The planner also walks the chain to discover the joins each derived field's SQL implies, so a filter that names only a bare local derived field (no dimension entry) still triggers the right LEFT JOINs. See [Datasets → Derived fields referencing other fields](datasets.md#derived-fields-referencing-other-fields) and [Filters and Auto-Joins](filters.md#filters-and-auto-joins).

SQL dimensions can be mixed with regular dimensions. The expression goes directly into SELECT and GROUP BY.

## Choosing a root dataset

`recommend_population` is the explain surface for the dimension anchor: give it the `dataset.field` / `dataset.metric` items you want (aggregation suffixes allowed) and it returns the population it would pick plus each item's relationship-qualified reference path from it — ready to paste into a query.

```python
rec = engine.recommend_population_sync(["customers.name", "products.category"])
rec.population          # "orders"  (the only dataset that determines both to-one)
{ip.input_item: ip.path for ip in rec.item_paths}
# {"customers.name": "customers.name", "products.category": "products.category"}
```

A root is valid when it **determines** every requested field along provably to-one relationship paths; saved metrics and aggregation-suffixed items are attachments that only need to be reachable and never steer the choice (each `ItemPath` flags `attachment`). Among valid roots, the one with the fewest total relationship hops wins — the same rule as the dimension anchor, so the two surfaces cannot disagree, and a tie recommends no root. Items are qualified (`orders.status`, `sum(orders.revenue)`, `count(orders.*)` — a row count is an attachment on its dataset); a bare name is accepted only for a [model-scoped metric](semantic-models.md#model-scoped-metrics), an attachment, and any other bare name or `count(*)` is refused with the qualified spellings. Each path is the item as written when a query rooted at the recommended root reads it along the chosen path, else its explicit route from the root (`orders.customers.region` when two to-one paths reach `customers`). With at least one field item, an attachment on a dataset no relationship path reaches never blocks a root: it comes back as written (`count(events.id)`) and flagged `unrelated`.

When no single dataset reaches everything, `population` is `None`, `reachable` is `False`, and `coverage` lists the best partial roots (each with its reachable / unreachable items) so you can split the request into a multi-stage [`source_queries`](datasets.md#query-backed-datasets) query.

### Forcing a root with `root_hint`

Sometimes you already know the host you want — often a **bridge** dataset that owns none of the requested items but matches the grain you're building on. Pass `root_hint` (a bare dataset name or `<data_source>.<dataset>`) to force it:

```python
rec = engine.recommend_population_sync(
    ["customers.name", "regions.name"], root_hint="orders"
)
rec.population   # "orders"  (honored — it reaches both, overriding the closer auto-pick)
```

When the hint determines every item it's honored outright, overriding the fewest-hops pick. When it can't, the auto-pick is used instead and `warnings` explains which owning datasets the hint missed and which root was chosen. If no dataset reaches everything (`reachable` is `False`), the hint's own row is included in `coverage` too, so you can see exactly what it reaches. `root_hint` is resolved after the datasource is fixed from the items, so it names a dataset *within* that datasource — it can't choose the datasource. A hint that isn't a dataset in the resolved datasource raises.

Surfaces: MCP `recommend_population(items, data_source=None, root_hint=None, format="markdown")`, REST `POST /recommend-population` (`{"items": [...], "data_source": null, "root_hint": null}`), CLI `slayer recommend-population ITEM... [--data-source X] [--root-hint M] [--format json|text]`, and `SlayerClient.recommend_population(_sync)`. The optional `data_source` scopes name resolution to one datasource; all items must resolve to a single datasource.
