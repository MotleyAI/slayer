# Queries

A `SlayerQuery` specifies what data to retrieve from a dataset.

## Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | No | Name for this query — used to reference it from other queries in a list |
| `population` | string | No | The population (query root): a stored dataset or stage name. Omit it to infer the population — see [Population](#population). |
| `extensions` | list[DatasetExtension] | No | Query-scoped metrics and relationships added to any dataset the query reads — see [Query extensions](#query-extensions). |
| `metrics` | list[str \| QueryMetric] | No | Computed/aggregated values — expressions, arithmetic, transforms. Aggregations are function calls (`sum(orders.revenue)`, incl. expressions like `sum(orders.amount - orders.cost)`) — see [Expressions](expressions.md) and [Aggregation syntax](references.md#aggregation-syntax). |
| `dimensions` | list[str \| QueryDimension] | No | Fields to group by — dataset-qualified references (`"orders.status"`, `customers.name`), row-level expressions, or `{"field": …, "alias": …, "label": …}` for a result name or label (a [computed expression](#expression-dimensions)). |
| `time_dimensions` | list[TimeDimension] | No | Time dimensions with granularity |
| `main_time_dimension` | string | No | Explicit time dimension name for transforms (overrides auto-detection) |
| `filter` | str \| list[str] | No | A condition, or a list of conditions all of which must hold. Supports `{variable}` placeholders. See [Filters](filters.md#filters). |
| `variables` | dict[str, Any] | No | Values for `{variable}` placeholders. See [Variables](filters.md#variables). |
| `order` | list[OrderItem] | No | Sort specifications |
| `limit` | int | No | Maximum rows to return |
| `offset` | int | No | Number of rows to skip |
| `whole_periods_only` | bool | No | Snap time bounds to bucket boundaries and exclude the current incomplete bucket — see [Time](time.md#whole_periods_only) |
| `time_zone` | str | No | IANA zone timestamps are bucketed, filtered and returned in; a stage without one takes the final stage's — see [Time zones](time.md#time-zones) |
| `to_many_handling` | str | No | How an aggregate resolves query dimensions [unattributable from its root](cross-dataset-queries.md#cross-dataset-metrics). Omitted (the default) or `associate`: per-cell value over the distinct associated rows; or `error` (refuse). Semi-join-pushed filters are always applied and never error. |

A key a query, refinement or entry does not read is refused with an `UnknownKeyError` naming it, its location and the keys that object takes; a camelCase spelling of a key it takes (`timeDimensions`, `dateRange`) is read as the snake_case key. On a query (not a [refinement](saved-and-multistage-queries.md#refining-a-saved-query)), `null` for a setting that cannot be null, such as `whole_periods_only` or `distinct_dimension_values`, means its default.

You can pass a single query or a **list of queries** to `execute()`. When passing a list, earlier queries are named sub-queries that later queries can reference. The last query in the list is the main one whose results are returned. See [Query Lists](saved-and-multistage-queries.md#query-lists) for examples.

Every Ossie Layer 3 query is a valid query here, with Ossie's rows; to have SLayer also refuse what Ossie refuses, with Ossie's error codes and output names, run it under the [Ossie Layer 3 binding](../interfaces/ossie-layer-3.md).

A query carries no tenant scoping of its own. To force every query through an engine to one tenant's rows — joins and sub-queries included — configure a policy at engine construction; see [Row-Level Security](row-level-security.md).

## Dimensions

Each entry in `dimensions` is either a string (a field reference or an expression) or an object with `field` and an optional `alias` and `label`. Both styles support dotted paths for joined datasets, auto-resolved via the relationship graph. An entry without `alias` or `label` serializes as its string.

```json
"dimensions": [
  "orders.status",
  "customers.name",
  "customers.regions.name",
  {"field": "orders.status", "label": "Order Status"}
]
```

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | A field reference (dotted paths for joined datasets) or a row-level expression. A double-quoted segment (`orders."status"`) matches its declared name exactly. |
| `alias` | string | Optional result key |
| `label` | string | Optional human-readable display name |

### Expression dimensions

To group by a computed expression (rather than a bare field), give its text as
`field`, with an optional `alias`:

```json
"dimensions": [
  "orders.region",
  {"field": "lower(orders.city)", "alias": "city_lc"},
  {"field": "CASE WHEN orders.amount > 2000 THEN 'big' ELSE 'small' END", "alias": "size"}
]
```

The expression is a [Mode-B expression](references.md) (scalar functions,
arithmetic, `CASE` / `iif`) over the dataset's fields; it is projected **and**
grouped. A bare-string entry that is not a valid identifier / dotted path is
also parsed as an expression (`"round(orders.amount)"`), auto-named from its text —
alias it to control the result key (the bare alias). An alias that collides with an
existing field or metric is rejected. The alias is resolvable from `filter`
and `order`. The alias keys the result even when the field is a bare joined
path: `{"field": "customers.region_id", "alias": "region_id"}` returns
`region_id`, and a downstream stage reads it as `<stage>.region_id`.

An expression (as a dimension, time dimension or order key) reads the fields of one dataset and of datasets it
reaches over to-one relationships, and takes the values a field of that dataset would — so
`orders.amount * returns.amount`, combining two to-many children of `customers`, is an error, and
`coalesce(orders.shipped_at, customers.signup_ts)` is NULL for a customer without orders.

#### Grouping by an expression over an aggregate

A computed dimension may also group by a value derived from an aggregate at a
**finer grain** than the query — e.g. band cities by whether each city's total
crosses a threshold, then group by `(region, band)`:

```json
{
  "population": "orders",
  "dimensions": [
    "orders.region",
    {"field": "CASE WHEN sum(orders.amount, partition_by=orders.city) > 5000 THEN 1 ELSE 0 END", "alias": "band"}
  ],
  "metrics": [{"metric": "sum(orders.amount)", "alias": "band_total"}]
}
```

The aggregate inside the expression **must** carry
[`partition_by=`](expressions.md#aggregate-at-a-coarser-grain-partition_by), which
names the grain it aggregates over (here, `orders.city`). SLayer computes that
aggregate in a synthesized internal stage, attaches it to the raw rows on the
partition grain, evaluates the expression per row, then regroups by the query's
dimensions — so every metric still aggregates the raw rows once (non-additive
metrics like `avg` / `count_distinct` reconcile correctly, and a `NULL`
partition value keeps its own group). The `partition_by` grain may be any
groupable key: a query dimension, a finer local field (`orders.city`), a joined path
(`customers.region_id`), a time bucket, or `[]` for the grand total. A base-row
filter (`orders.status = 'ok'`) also constrains the partition aggregate; a filter on
the computed dimension name (`band == 1`) applies after regrouping.

A dimension expression may band a windowed partitioned aggregate
(`sum(orders.amount, window='90d', partition_by=orders.region)`), a `first` / `last`, or a
transform over a grained aggregate — `rank(sum(orders.revenue, partition_by=orders.region), direction='desc')` as
a DIMENSION ranks the partitions (it evaluates at the producer grain), whereas
the same expression as a METRIC ranks the result rows (query grain).

An aggregation-derived dimension combines with transform metrics: alongside
`band` you can declare `time_shift(sum(orders.amount), -1)`, `change` / `change_pct`,
`cumsum`, `lag` / `lead`, `consecutive_periods(...)`, or `rank(sum(orders.amount), direction='desc')`,
with or without plain and `partition_by=` metrics in the same query. Every
transform treats the computed dimension as an ordinary grouping dimension (a
running total accumulates within each `(region, band)` group; a time shift
compares each group only against itself).

A transform over aggregates at **different** partition grains unions the grains
and broadcasts each aggregate to the union —
`rank(sum(orders.amount, partition_by=orders.region) - sum(orders.amount, partition_by=orders.city), direction='desc')` ranks
the `(region, city)` rows, each region
total broadcast across its cities and each city total against its region. The
same holds as a bare metric (`sum(orders.a, partition_by=orders.region) - sum(orders.b, partition_by=orders.city)`
evaluated at the query grain) and recursively for a nested transform, which
accumulates within its **own** grain before broadcasting into the outer union.

A cross-dataset aggregate source inside a dimension expression is legal: it
compiles through the same target-rooted producer as a
[cross-dataset metric](cross-dataset-queries.md#cross-dataset-metrics), with the same exact-grain vs
broadcast semantics.

An aggregate partitioned by another computed dimension — even one carrying an
attached aggregate — compiles as a nested producer
([re-aggregation](expressions.md#re-aggregation-aggregate-over-an-attached-value)).
A dimension expression may itself hold a re-aggregation
(`CASE WHEN max(sum(orders.amount, partition_by=[orders.region, orders.city]), partition_by=orders.region) > 45 THEN 1 ELSE 0 END`):
it composes with every metric, `partition_by=` key and measure a plain dimension over `orders.region` does.

Ill-typed shapes (raise a clear error naming the remedy, by design): a bare
aggregate without `partition_by=` — the ungrained default would include the
dimension being defined — and a time-ordered transform (`cumsum`, `lag`, …)
whose evaluation grain lacks its time-ordering key — include that key in
`partition_by=` so the transform accumulates within its own grain.

### Dim-only queries deduplicate

A query with no metrics and at least one dimension or time-dimension returns the **distinct combinations** of those dimensions, not the raw underlying-row stream. SLayer emits `GROUP BY` over all dim/time-dim aliases before applying `LIMIT`, so a row cap can't silently drop unique tuples that only appear past `limit` rows.

```json
{"population": "orders", "dimensions": ["orders.status"], "limit": 100}
```

Emits `SELECT orders.status FROM orders GROUP BY orders.status ORDER BY orders.status LIMIT 100`.

### Raw rows (`distinct_dimension_values`)

Set `distinct_dimension_values: false` on a query to opt out of the auto-dedup and project raw rows instead. No top-level `GROUP BY`; the usual `WHERE` / `ORDER BY` / `LIMIT` / `OFFSET` still apply.

```json
{
  "population": "orders",
  "dimensions": ["orders.status", "orders.amount"],
  "filter": ["orders.amount > 100"],
  "order": [{"field": "orders.amount", "direction": "desc"}],
  "limit": 100,
  "distinct_dimension_values": false
}
```

Emits roughly `SELECT orders.status, orders.amount FROM orders WHERE orders.amount > 100 ORDER BY orders.amount DESC LIMIT 100` — one row per source row.

**Rules** when `distinct_dimension_values=False`:

- `metrics` must be empty — `DistinctDimensionValuesError` otherwise.
- At least one of `dimensions` / `time_dimensions` must be non-empty (nothing to project otherwise).
- Filters / order items must not reference any metric — neither an aggregation (`sum(orders.amount) > 100`, `count(orders.*) > 0`), transform calls (`rank(sum(orders.amount), direction='desc') <= 5`), nor a saved `DatasetMetric`.
- The population must determine every dimension, time dimension and row-level order key, derived fields' definitions included, over provably to-one hops only: a fanning hop (a list child included) or an unproven one (no declared cardinality and no covering unique key) raises `DistinctDimensionValuesError` naming the entry and the hop.

**Time dimensions** are allowed: each one emits its `DATE_TRUNC` truncation as a projected column without aggregating. For raw field values (no truncation), put the time field in `dimensions` instead.

**Multi-stage**: the flag is per-stage in DAG queries — an inner stage with `false` produces a flat raw-row sub-query that the outer stage can aggregate over.

## TimeDimension

A time dimension with a required granularity and an optional date range. Supports an optional `label` for human-readable output. To use a time field without truncation, add it as a regular dimension instead. `field` is text; an object there is refused.

`field` may be any row-level expression typed DATE or TIMESTAMP (`"coalesce(orders.shipped_at, orders.created_at)"`, `"CAST(orders.created_txt AS DATE)"`, also as `date_trunc('month', coalesce(orders.shipped_at, orders.created_at))`), and an optional `alias` sets its result key and lets `main_time_dimension` and `partition_by=` refer to it (an unaliased expression is named like a computed dimension). Names are unique across a query's dimensions and time dimensions. An aliased time dimension's key never carries a granularity suffix; unaliased time dimensions sharing one source at different granularities get one each (`orders.created_at.month`, `orders.created_at.year`).

```json
{
  "field": "orders.created_at",
  "granularity": "month",
  "date_range": ["2024-01-01", "2024-12-31"],
  "label": "Order Month"
}
```

**Granularities**: `second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`

A `dimensions` or `time_dimensions` entry may also be written as the string `date_trunc('<unit>', expr)` (e.g. `"date_trunc('month', orders.created_at)"`) — equivalent to a `TimeDimension` with that field and granularity (use the dict form above to add `date_range` or `label`), and usable as an `order` key too, where it must match a projected time dimension on the same expression at that granularity or the query fails. A `time_dimensions` string in any other form is an error naming its `date_trunc` spelling.

A downstream stage of a multi-stage query may declare a time dimension on any DATE/TIMESTAMP column of its upstream stage — re-bucketing a column the upstream already truncated only at the same or a nesting-coarser granularity (e.g. `month` → `year`). The same rule applies to a dataset field that carries a `granularity` (a query-backed dataset's bucketed cache field, or a hand-set one): a finer or non-nesting time dimension over it is the same typed error, judged for every bucketed field an expression time dimension reads (`CAST(month_col AS DATE)` at `day` fails).

`week` is Monday-anchored (ISO-8601); `week_sunday` is Sunday-anchored (weeks start Sunday, end Saturday) for tools that use Sunday weeks. Both are dataset granularities you set on a `TimeDimension` — `week_sunday` is the SLayer value, not a wire keyword sent by a BI tool.

`date_range` is one time point (`"2025-Q1"`, `"last month"`) or a `[lower, upper]` pair with either bound `null`, read with the [time-point semantics](time.md#date_range) — a date-only bound there covers its whole day. Cube's `dateRange` reads as `date_range`; its `compareDateRange` is refused — compare periods with `change`, `change_pct` or `time_shift`.

## OrderItem

A sort specification: `field` is text naming a dimension (`orders.status`, `customers.region`), a declared metric's alias (`revenue`), or an expression (`count(orders.*)`); `direction` is `asc` or `desc`.
An optional `nulls` (`first` or `last`) places NULL values first or last on every dialect; without it the database's default placement applies.

```json
{"field": "count(orders.*)", "direction": "desc"}
```

Via MCP: `{"field": "count(orders.*)", "direction": "desc"}`

A query (or the final stage of a query list) with no `order` that projects dimensions is ordered by its dimensions and time dimensions ascending, in result-column order, exactly as if it listed them in `order`; raw-row queries and inner stages are left unordered.

### Ordering by something you don't project

`order` may reference a field or aggregate that is **not** declared as a dimension/metric — the classic "top-N by metric X, display only Y, Z" pattern:

```json
{"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"],
 "order": [{"field": "sum(orders.amount)", "direction": "desc"}], "limit": 10}
```

The `sum(orders.amount)` aggregate is computed as a hidden column, sorted on, and **stripped from the result** — the response projects only `orders.status` and `orders._count`. This works for local aggregates, cross-dataset aggregates (`sum(customers.revenue)`), and inner-stage columns re-aggregated in a later DAG stage (`max(s1.customers__revenue_sum)`).

What each shape of an *undeclared* order target does:

| Order target | Behavior |
| --- | --- |
| An aggregate (`sum(orders.amount)`, `sum(customers.revenue)`) | Computed hidden, sorted on, stripped from the result. Always allowed. |
| An inline **transform** (`rank(sum(orders.amount), direction='desc')`, `cumsum(...)`, `change(...)`, `lag`/`lead`/`ntile`) | Computed hidden, sorted on, stripped. |
| An inline **composite** (`sum(orders.revenue) / sum(orders.cnt)`, `abs(sum(orders.amount))`, `change(sum(orders.amount)) / 2`) | Computed hidden, sorted on, stripped. |
| A **windowed** aggregate (`sum(orders.amount, window='90d')`), alone or inside a composite | Computed hidden in its own rolling-window CTE, sorted on, stripped. |
| A raw row field, in a **raw-rows** query (`distinct_dimension_values: false`, no metrics) | Sorted on directly (`ORDER BY orders.created_at`). Applies to a **joined** field (`customers.regions.name`) and to a derived field whose `sql` reaches through a relationship — the join is pulled in for the sort. |
| A raw row field or row-level expression (`orders.amount - orders.fee`), in an **aggregated / dedup** query | Sorted on **per group**: ASC by each group's minimum, DESC by each group's maximum. The wrap is implicit — you do not write `min(orders.created_at)`. |
| A **joined** row field (`customers.regions.name`) in an aggregated query | Same per-group wrap, computed in a CTE rooted at the population with the join pulled inside, so each group gets its own extreme rather than one global value. |

Ordering by an undeclared row field in a grouped query is **not** the same as
ordering by the field itself — there is no single value per group to sort by.
SLayer picks the extreme the direction puts first: `asc` sorts each group by its
`min`, `desc` by its `max`. Write `{"field": "max(orders.created_at)", "direction": "asc"}`
explicitly if you want the other one.

NULLs sort **last** in both directions, on every database, so the same query
returns the same row order regardless of backend. (SQL Server is the one
exception: its native ordering is used because the portable emulation makes the
statement fail there.)
An explicit `nulls` (`first` or `last`) places NULLs there on every database, SQL Server included.

An order target that names nothing SLayer can resolve is an error, never a
silently unsorted result.

Transform and composite order targets accept the full expression syntax, so
`{"field": "sum(orders.revenue) / sum(orders.cnt)"}` and `{"field": "change(sum(orders.revenue))"}` both
work without declaring a metric, and their operands may also be the names of metrics declared in the same query (`{"field": "rev / cnt"}`), as in filters.

Windowed metrics work in declared composites and transforms too — see [expressions](expressions.md#windowed-aggregations).

## Response

Query results are returned as a `SlayerResponse`:

| Field | Type | Description |
|-------|------|-------------|
| `data` | list[dict] | Rows as dictionaries |
| `columns` | list[str] | Column names: each reference's qualified path (e.g., `"orders._count"`, `"customers.regions.name"` for multi-hop), or an entry's bare alias, **in the order you declared them** |
| `row_count` | int | Number of rows |
| `sql` | string | The generated SQL (useful for debugging) |
| `population` | string \| null | The population: the named one, else the dimension anchor (see [Population](#population)) |
| `population_inferred` | bool | Whether the rows were inferred (no `population` given, or a saved stage's pinned anchor) |
| `row_sources` | list[str] | The datasets whose rows supplied the result; `["unit"]` with no dimensions |
| `attributes` | ResponseAttributes | Field metadata split by type: `attributes.dimensions` and `attributes.metrics`, each a dict of column alias → FieldMetadata (label, format) |
| `warnings` | list[SlayerWarning] | Advisories, discriminated by `kind`: input normalizations (`"normalization"`), a value repeated by the deprecated `broadcast` mode (`"broadcast"` — `metric`, `location`, per-dimension `dimensions[].reason`, and `partition_by`, the dimensions giving the same values without a warning), a distinct-entity attribution over an unattributable dimension (`"associated"` — `metric`, `location`, `dimensions`; cells overlap and are not additive), an aggregate over a dataset no relationship path reaches (`"unrelated_dataset"` — `dataset`, `population`, `metrics`, `location`), a semi-join-pushed filter (`"semi_join_pushed"` — `metric` (`null` for a population-level push), `location`, `filter_text`), a query run without its statement timeout (`"statement_timeout_skipped"` — `datasource`, `timeout_seconds`, `reason`: `readonly_user` or `timeout_rejected`), [`whole_periods_only`](time.md#whole_periods_only) over non-nesting granularities (`"whole_periods_non_nesting"` — `field`, `granularities`, `location`), a [list](filters.md#list-fields) read as its element set (`"whole_list"` — `operation`: `group` or `count_distinct`, `list`), a timestamp read as stored under a query zone because it has no source zone (`"time_zone_undeclared"` — `dataset`, `field`, `path`, `time_zone`; see [Time zones](time.md#time-zones)), and a deprecated input form that was converted (`"deprecated_input"` — `old`, `new`, `removal`; see [Upgrading to 2.0](../upgrading/2.0.md)) |

`columns` — and the key order of each row in `data` — follows the order you
declared fields in the query: dimensions, then time dimensions, then metrics,
each in the order given. This holds regardless of how a metric is computed, so
a metric on a joined dataset appears where you declared it rather than after the
local ones. Fields used only for ordering are computed but not returned.

```json
{
  "data": [
    {"orders.status": "completed", "orders._count": 42},
    {"orders.status": "pending", "orders._count": 15}
  ],
  "columns": ["orders.status", "orders._count"],
  "row_count": 2,
  "sql": "SELECT ..."
}
```

Result keys are always this canonical dotted form, on every backend. Where a
database's own rules force a different spelling in the emitted SQL — BigQuery
and SQL Server reject dotted column aliases, and most engines cap identifier
length — the alias is rewritten on the way out and restored on the way back, so
`data` and `columns` do not vary by dialect. Only `sql` shows the rewritten
form, since that is what actually ran. See
[Database support](../database-support.md#identifier-length-limits).

An ill-typed query fails with a `QueryTypeError` subclass (a `ValueError`, e.g. `PartitionKeyError`) whose text is a `<ClassName>: <summary>` first line, then an optional `  at <subject>` line naming the offending metric, filter, dimension or transform, then an optional `  suggestion: <remedy>` line.

---

## Population

`population` names the query's **population**: the dataset whose rows the result is quantified over, one result row per distinct combination of the dimensions among its filtered rows. Metrics never change those rows. A metric's home row with no matching population row (an order whose customer is missing) lands in no row.

The population decides only the rows. Each metric is read from its own dataset and meets the population on dimension values, so the population never needs a route to it. A restricted population (dataset filters, row-level security, a filtered stage) narrows a metric only where the metric's own dataset reads the restricted rows along its own paths.

Omit `population` and the rows are the **row supply**:

- Each metric supplies the dimension values its home's filtered rows reach along the home's own relationship paths. A home row with no related row lands in the NULL value. The rows are the union over every metric.
- A metric whose aggregates don't reach every dimension (`partition_by=`, an [unrelated dataset](cross-dataset-queries.md#cross-dataset-metrics)) supplies the rows of the **dimension anchor** that agree with it.
- With no metrics, the rows are the dimension anchor's: the smallest dataset that determines every dimension along to-one paths, read from the dimensions only, never from filters or metrics. Dimension groups no relationship path connects take the product of their anchors.
- With no dimensions there is exactly one row, even when a filter matches nothing.
- Only `metrics` supply rows: an aggregate in a filter, an order entry or a computed dimension reads them and adds none.
- Filters restrict every home's rows (and the anchor's) before anything is counted. They never pick where rows come from, so adding a filter never adds a row or changes a result key — except a computed dimension read from an aggregate over the filtered rows.

<!-- doc-check: not-a-query -->
```json
{"dimensions": ["customers.region"], "metrics": ["sum(orders.amount)"]}
```

returns one row per region some order reaches, and a NULL-region row for orders whose customer is missing. A region with no orders is absent: name `"population": "customers"` to list every customer region.

The response carries `population` (the dimension anchor; `a × b` for a product; `null` with no dimensions, or when the query never needs the anchor), `population_inferred`, and `row_sources`: the datasets whose rows supplied the result (`["unit"]` with no dimensions; the named population when one is given). MCP output prints them only when the rows are inferred.

A query that needs the dimension anchor fails closed with a `PopulationInferenceError` naming the candidates when several minimal candidates tie, connected dimension groups have no single determining dataset, a dimension's path is ambiguous, or the referenced datasets don't belong to exactly one datasource.

A time dimension on `time_spine.timestamp` makes the population `time_spine × P`: P named, else the row supply of the remaining dimensions, every bucket for each of its tuples — see [Time spine](time.md#time-spine).

Saving a query-backed dataset writes each inferred stage's dimension anchor as its `population`, marked `population_inferred: true`: the anchor can no longer move, and the rows stay inferred. `population_inferred` without `population` is refused.

---

## Examples

### Count by status

```json
{
  "population": "orders",
  "metrics": ["count(orders.*)"],
  "dimensions": ["orders.status"]
}
```

### Monthly revenue with date range

```json
{
  "population": "orders",
  "metrics": ["sum(orders.revenue)"],
  "time_dimensions": [{
    "field": "orders.created_at",
    "granularity": "month",
    "date_range": ["2024-01-01", "2024-12-31"]
  }]
}
```

### Top 5 customers by revenue

```json
{
  "population": "orders",
  "metrics": ["sum(orders.revenue)"],
  "dimensions": ["orders.customer_name"],
  "order": [{"field": "sum(orders.revenue)", "direction": "desc"}],
  "limit": 5
}
```

### Filtered count with OR logic

```json
{
  "population": "orders",
  "metrics": ["count(orders.*)"],
  "filter": ["orders.status = 'completed' or orders.status = 'pending'"]
}
```

### Derived metrics with transforms

```json
{
  "population": "orders",
  "metrics": [
    "count(orders.*)",
    "sum(orders.revenue)",
    {"metric": "sum(orders.revenue) / count(orders.*)", "alias": "aov", "label": "Average Order Value"},
    {"metric": "cumsum(sum(orders.revenue))", "alias": "running"},
    {"metric": "change(sum(orders.revenue))", "alias": "mom_change"}
  ],
  "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]
}
```

### Statistical aggregations

The `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, and `covar_pop` aggregations behave like the rest of the aggregations. `corr` / `covar_samp` / `covar_pop` take two fields — the second rides as a named `other` parameter, the same way `weighted_avg` takes `weight`:

```json
{
  "population": "orders",
  "metrics": [
    {"metric": "stddev_samp(orders.latency)", "alias": "latency_sd"},
    {"metric": "var_pop(orders.latency)", "alias": "latency_var_pop"},
    {"metric": "corr(orders.price, other=orders.quantity)", "alias": "price_qty_corr"},
    {"metric": "covar_samp(orders.price, other=orders.quantity)", "alias": "price_qty_cov"}
  ],
  "dimensions": ["orders.status"]
}
```

Edge cases match Postgres exactly:
- sample stddev/variance/covariance return NULL when N ≤ 1
- population stddev/variance/covariance return 0 at N = 1 and NULL at N = 0
- `corr` additionally returns NULL when either side has zero variance (covariance is well-defined in that case and just returns 0)

See [database-support.md](../database-support.md#aggregation-support) for the per-engine support matrix.

### Share of parent (`partition_by=`)

`partition_by=` on an aggregation computes it over a subset of the query's
dimensions and repeats the value across the finer rows — the share-of-parent /
percent-of-total shape. See [Expressions](expressions.md#aggregate-at-a-coarser-grain-partition_by).

```json
{
  "population": "orders",
  "dimensions": ["orders.region", "orders.city"],
  "metrics": [
    {"metric": "sum(orders.revenue)", "alias": "city_rev"},
    {"metric": "sum(orders.revenue) / sum(orders.revenue, partition_by=orders.region)", "alias": "share_of_region"},
    {"metric": "sum(orders.revenue) / sum(orders.revenue, partition_by=[])", "alias": "share_of_total"}
  ]
}
```

`share_of_region` sums to 1.0 within each region; `share_of_total` sums to 1.0
over the whole result. An unnamed partitioned metric's result key carries the
partition in its suffix (`orders.revenue_sum_partition_by_region`,
`orders.revenue_sum_partition_by` for the grand total) — name the metric to
control the key.

A `partition_by` element may also be a time bucket the query groups by, so with time dimension `ordered_at` at month, `partition_by=[date_trunc('month', ordered_at)]` means `partition_by=[ordered_at]`.

### Query extensions

`extensions` adds metrics and relationships to any dataset a query object reads — its population, a joined dataset, or a stage — without modifying the stored dataset:

```json
{
  "population": "orders",
  "extensions": [
    {
      "dataset": "orders",
      "relationships": [{"to": "customer_scores", "from_columns": ["customer_id"], "to_columns": ["id"]}]
    },
    {"dataset": "customers", "metrics": [{"name": "credit_total", "expression": "sum(credit)"}]}
  ],
  "dimensions": [{"field": "CASE WHEN orders.amount > 100 THEN 'high' ELSE 'low' END", "alias": "tier"}],
  "metrics": ["count(orders.*)", "customers.credit_total"]
}
```

Each entry's keys are `dataset` (required — the dataset or stage to extend; `source_name` is its pre-2.0 spelling), `metrics` and `relationships` (both optional — merged with the dataset's own); any other key, or a second entry for one dataset, is rejected when the query is constructed. A per-query field is an expression in the position that uses it (a computed dimension, a filter, an aggregation's argument); `fields` is rejected.

An extension is visible only in the query object that declares it: other stages see that stage's output, and stored relationships, fields, metrics and query-backed datasets never see it.

A relationship repeating an edge between its two datasets — declared on either dataset, with the same key pairs (`from_columns` / `to_columns`) in either direction, whatever their order — is that edge, and its `name` becomes a local alias; a name, pairs or cardinality that conflicts with an existing edge is an error.

`"join_type": "inner"` keeps only the population rows with a match along the extended dataset's path, without multiplying rows; a `left` join (the default) never restricts, and a new edge is always traversed as LEFT.

An extension object in `population` (`{"source_name": ...}`) still works with a deprecation warning, and is removed in SLayer 3.0 — see [Upgrading to 2.0](../upgrading/2.0.md#part-2-deprecated-in-20-removed-in-30).

---

## Moved sections

- <a id="filters"></a>[Filters](filters.md#filters)
- <a id="comparison-operators"></a>[Comparison Operators](filters.md#comparison-operators)
- <a id="boolean-logic"></a>[Boolean Logic](filters.md#boolean-logic)
- <a id="field-vs-metric-filters"></a>[Field vs metric filters](filters.md#field-vs-metric-filters)
- <a id="scalar-functions-in-filters"></a>[Scalar Functions in Filters](filters.md#scalar-functions-in-filters)
- <a id="filtering-on-computed-metrics"></a>[Filtering on Computed Metrics](filters.md#filtering-on-computed-metrics)
- <a id="filters-and-auto-joins"></a>[Filters and Auto-Joins](filters.md#filters-and-auto-joins)
- <a id="list-fields"></a>[List fields](filters.md#list-fields)
- <a id="window-functions-in-filters"></a>[Window functions in filters](filters.md#window-functions-in-filters)
- <a id="variables"></a>[Variables](filters.md#variables)
- <a id="variables-passed-as-a-runtime-kwarg"></a>[Variables passed as a runtime kwarg](filters.md#variables-passed-as-a-runtime-kwarg)
- <a id="cross-dataset-metrics"></a>[Cross-dataset metrics](cross-dataset-queries.md#cross-dataset-metrics)
- <a id="multi-hop-dimensions"></a>[Multi-hop dimensions](cross-dataset-queries.md#multi-hop-dimensions)
- <a id="choosing-a-root-dataset"></a>[Choosing a root dataset](cross-dataset-queries.md#choosing-a-root-dataset)
- <a id="forcing-a-root-with-root_hint"></a>[Forcing a root with `root_hint`](cross-dataset-queries.md#forcing-a-root-with-root_hint)
- <a id="query-lists"></a>[Query lists](saved-and-multistage-queries.md#query-lists)
- <a id="run-a-saved-query-by-name"></a>[Run a saved query by name](saved-and-multistage-queries.md#run-a-saved-query-by-name)
- <a id="refining-a-saved-query"></a>[Refining a saved query](saved-and-multistage-queries.md#refining-a-saved-query)
