# Fields and Aggregations Are Separate Things

Most semantic layers force you to bake the aggregation into the field definition. You want revenue? Define `revenue_sum`. Want average revenue too? Define `revenue_avg`. Five aggregation types per numeric field, times twenty fields, and you're staring at a hundred definitions before you've written a single query.

SLayer takes a different approach: **a field is just a named SQL expression** — a row-level fact about your data. The **aggregation** — how you want to roll it up — is specified when you query, not when you define the dataset.

## What this looks like

A dataset defines fields as bare expressions:

```yaml
fields:
  - name: subtotal
    sql: subtotal
    datatype: Float
  - name: tax_paid
    sql: tax_paid
    datatype: Float
  - name: order_total
    sql: order_total
    datatype: Float
```

No `type: sum` or `type: avg`. Just what the field is.

At query time, you pick the aggregation with a function call:

```json
{
  "population": "orders",
  "metrics": ["sum(orders.subtotal)", "avg(orders.subtotal)", "min(orders.order_total)", "max(orders.order_total)"],
  "dimensions": ["stores.name"]
}
```

`sum(orders.subtotal)` means "take the `subtotal` field and SUM it." `min(orders.order_total)` means "take the `order_total` field and find the MIN." One field definition, as many aggregations as you need.

## Aggregating an expression

An aggregation can also take a same-dataset **expression** as its value, not
just a bare field. Declared parameters may be passed positionally in
declaration order, so `percentile(orders.price, 0.9)` means `p=0.9`:

```json
{
  "population": "orders",
  "metrics": [
    {"metric": "sum(orders.order_total - orders.tax_paid)", "alias": "net_revenue"},
    "count_distinct(concat(orders.customer_id, orders.store_id))",
    "percentile(orders.tax_paid / orders.subtotal, p=0.5)"
  ]
}
```

The expression may use same-dataset fields, scalar functions, arithmetic,
and literals; an unnamed one derives its result key from the expression
(`sum(orders.order_total - orders.tax_paid)` → `orders.order_total_tax_paid_sum`). Dotted
joined-dataset paths inside an expression, filtered fields, and nested
aggregations are rejected with clear errors — see
[Expressions → Expression aggregation](../../concepts/expressions.md#expression-aggregation).

## COUNT(*) and the star metric

COUNT(\*) doesn't aggregate a specific field — it counts rows. In SLayer, `*` is the "all rows" placeholder:

```json
{
  "metrics": ["count(orders.*)", "sum(orders.revenue)"]
}
```

`count(orders.*)` produces `COUNT(*)`. Result column: `orders._count` (the underscore prefix distinguishes it from any dimension that might happen to be called `count`).

> **Note:** `*` can only be used with `count`. Combinations like `sum(*)` or `avg(*)` are invalid — use a named metric instead.

You can also count non-null values of a specific field: `count(customers.email)` produces `COUNT(email)`. And `count_distinct(orders.customer_id)` gives you `COUNT(DISTINCT customer_id)`.

## Built-in aggregations

These are always available — no definition needed:

| Aggregation | What it does |
|------------|-------------|
| `sum` | SUM(expr) |
| `avg` | AVG(expr) |
| `<agg>(window='90d')` | any aggregation over the source rows in the trailing range ending at each output bucket |
| `min` / `max` | MIN/MAX(expr) |
| `count` | COUNT(expr), or COUNT(\*) with `*` |
| `count_distinct` | COUNT(DISTINCT expr) |
| `count_distinct_approx` | Database-native approximate distinct count; exact `COUNT(DISTINCT expr)` fallback — see database support below |
| `first` / `last` | Value from the earliest/latest record per group (by time) |
| `weighted_avg` | SUM(expr \* weight) / SUM(weight), a NULL `expr` skipping its weight |
| `median` | PERCENTILE_CONT(0.5) — see database support below |
| `percentile` | PERCENTILE_CONT(p) — specify `p` as an argument; see database support below |
| `stddev_samp` / `stddev_pop` | Sample / population standard deviation |
| `var_samp` / `var_pop` | Sample / population variance |
| `corr` | `corr(price, other=quantity)` — Pearson correlation between two fields |
| `covar_samp` / `covar_pop` | `covar_samp(price, other=quantity)` — sample / population covariance |

### Database support for `median` / `percentile`

| Engine | Supported? | How |
|---|---|---|
| Postgres | yes | Native `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)`. |
| DuckDB | yes | sqlglot rewrites ordered-set percentiles to DuckDB's `QUANTILE_CONT(x, p ORDER BY x)` syntax. |
| SQLite | yes | Python aggregate UDFs registered on every connection by SLayer. |
| ClickHouse | yes | Native `median(x)` and parametric `quantile(p)(x)`. |
| MySQL | **no** | No native function and no Python-UDF mechanism — SLayer raises `NotImplementedError`. Use MariaDB or compute client-side. |

### Database support for `count_distinct_approx`

`count_distinct_approx` emits each database's native approximate-distinct function where one exists, and falls back to an **exact** `COUNT(DISTINCT expr)` where it does not. The fallback is exact (more accurate, never approximate), so results are always at least as precise as requested.

| Engine | Emitted SQL |
|---|---|
| DuckDB / Spark / Databricks | `approx_count_distinct(x)` |
| ClickHouse | `uniq(x)` |
| BigQuery / Snowflake / T-SQL / Oracle | `APPROX_COUNT_DISTINCT(x)` |
| Trino / Presto | `approx_distinct(x)` |
| Redshift | `APPROXIMATE COUNT(DISTINCT x)` |
| Postgres / SQLite / MySQL | `COUNT(DISTINCT x)` (exact fallback) |

Any aggregation can take a trailing time `window` when the query has a time
dimension:

```json
{
  "metrics": [
    {"metric": "sum(orders.revenue, window='30d')", "alias": "revenue_30d"},
    {"metric": "avg(orders.revenue, window='1y2m3w5d6h7min8s')", "alias": "avg_window"}
  ],
  "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]
}
```

Duration units are `y`, `m`, `w`, `d`, `h`, `min`, and `s`. The window is
computed over raw source rows, so it may be larger, equal to, or smaller than
the query's time granularity.

## Custom aggregations

The built-ins cover common cases. When they don't, define your own:

```yaml
aggregations:
  - name: trimmed_mean
    expression: "AVG(CASE WHEN {value} BETWEEN {lo} AND {hi} THEN {value} END)"
    params:
      - name: lo
        sql: "0"
      - name: hi
        sql: "1000"
```

`{value}` is the metric's SQL expression. `{lo}` and `{hi}` are parameters with defaults that can be overridden at query time:

```json
"trimmed_mean(orders.score, lo=10, hi=90)"
```

You can also override built-in aggregation defaults. If `weighted_avg` should default to a specific weight field in your dataset:

```yaml
aggregations:
  - name: weighted_avg
    params:
      - name: weight
        sql: subtotal
```

Now `weighted_avg(orders.tax_rate)` uses `subtotal` as the weight without you specifying it every time. But you can still override: `weighted_avg(orders.tax_rate, weight=orders.order_total)`.

## Controlling which aggregations apply

Not every aggregation makes sense for every field. `avg(orders.customer_id)`? Probably not useful. The `allowed_aggregations` field lets you whitelist:

```yaml
fields:
  - name: customer_id
    sql: customer_id
    datatype: Integer
    allowed_aggregations: [count, count_distinct]
  - name: revenue
    sql: amount
    datatype: Float
    allowed_aggregations: [sum, avg, min, max, weighted_avg]
```

SLayer validates this at query time and at dataset creation — if you try `sum(orders.customer_id)`, you get a clear error listing the valid options.

## first and last

`first` and `last` return the value from the earliest or latest record in each group, ordered by a time field. They need a time dimension to know what "earliest" and "latest" mean:

```json
{
  "metrics": ["last(accounts.balance)", "first(accounts.balance)"],
  "time_dimensions": [{"field": "accounts.updated_at", "granularity": "month"}]
}
```

If you want to use a specific time field (overriding the query's time dimension), pass it as an argument:

```json
"last(accounts.balance, accounts.created_at)"
```

This explicit time argument takes priority over everything — query-level `time_dimensions`, `main_time_dimension`, and the dataset's `default_time_dimension`.

Don't confuse the `last` *aggregation* (`last(accounts.balance)`) with the `last()` *transform* (`last(sum(orders.revenue))`). The aggregation picks the latest record's value within each time bucket. The transform broadcasts the latest time bucket's aggregated value to every row. Different operations, different use cases.

## Percentiles

`median` is built in, but you might want the 95th percentile, or Q1/Q3:

```json
{
  "metrics": [
    "median(requests.latency)",
    "percentile(requests.latency, p=0.95)",
    "percentile(requests.latency, p=0.25)"
  ]
}
```

## Composing with transforms and arithmetic

Arithmetic:

```json
{"metric": "sum(orders.revenue) / count(orders.*)", "alias": "aov"}
```

Transforms:

```json
"cumsum(sum(orders.revenue))"
"change(sum(orders.revenue))"
"time_shift(sum(orders.revenue), -1, 'year')"
```

Cross-dataset:

```json
"count(customers.*)"
"cumsum(count(customers.*))"
```

Conditionals — `CASE WHEN` (or `iif(cond, then, otherwise)`) can branch on an aggregated value; the branch runs after grouping:

```json
{"metric": "CASE WHEN sum(orders.revenue) >= 10000 THEN 'high' ELSE 'standard' END", "alias": "tier"}
```

See [Expressions — Conditionals](../../concepts/expressions.md#conditionals-case-when-iif) for branch-typing rules, and [Queries — Expression dimensions](../../concepts/queries.md) for grouping by a computed expression.

## Share of parent (`partition_by`)

Most aggregations take an optional `partition_by=` to compute over a subset of the query's dimensions, repeated across the finer rows — `SUM(x) OVER (PARTITION BY …)`:

```json
{
  "population": "orders",
  "dimensions": ["stores.regions.name", "stores.name"],
  "metrics": [
    {"metric": "sum(orders.order_total) / sum(orders.order_total, partition_by=stores.regions.name)", "alias": "share_of_region"},
    {"metric": "sum(orders.order_total) / sum(orders.order_total, partition_by=[])", "alias": "share_of_total"}
  ]
}
```

`partition_by=stores.regions.name` is the region total on every store row (so `share_of_region` sums to 1.0 per region); `partition_by=[]` is the grand total; a list (`[stores.regions.name, orders.status]`) also works. The total is computed over rows passing row-level filters — filters on the metric (`having`) and pagination never change it.

A local `partition_by` aggregate also composes with the rest of the query: combined with `window=` (a rolling total at the partition grain, per the query's time bucket), on `first`/`last`, nested inside a transform (`cumsum(sum(orders.order_total, partition_by=stores.regions.name))`), and referenced in a filter or `order` target (`sum(orders.order_total, partition_by=stores.regions.name) > 5000`) — for filters and `order` targets, cross-dataset included (`sum(customers.spend, partition_by=customers.regions.name) > 100`). A filter's top-level `AND` conjuncts route independently; a single predicate valid as neither a row-level field nor a metric (e.g. a partitioned aggregate OR-ed with a raw row field that isn't a query dimension) raises a typing error naming both failures. Cross-dataset `partition_by` composes the same way — with `window=` when the query's active time dimension is attributable from the aggregate's root, on `first`/`last` (`last(customers.spend, partition_by=customers.tier)`), and nested inside a transform.

A partitioned aggregate can itself be re-aggregated — `{"metric": "avg(sum(orders.order_total, partition_by=[stores.name, stores.regions.name]))", "alias": "avg_store_total"}` grouped by `stores.regions.name` averages each region's **store totals** (one input per store cell, never a row-weighted value), and the outer aggregation can take a parameter at the same grain — `weighted_avg(sum(orders.order_total, partition_by=[stores.name, stores.regions.name]), weight=count(orders.id, partition_by=[stores.name, stores.regions.name]))` weights each store total by its row count; see [re-aggregation](../../concepts/expressions.md#re-aggregation-aggregate-over-an-attached-value).

## Result column naming

The aggregation and field join with an underscore in result keys:

| Expression | Result key |
|---------|-----------|
| `sum(orders.revenue)` | `orders.revenue_sum` |
| `count(orders.*)` | `orders._count` |
| `avg(orders.revenue)` | `orders.revenue_avg` |
| `count(customers.*)` | `customers._count` |
| `sum(orders.revenue, partition_by=orders.region)` | `orders.revenue_sum_partition_by_region` |

When a query is saved as a dataset (`create_dataset` with a `query` parameter), these canonical names become the new dataset's field names.

---

See the [companion notebook](aggregations_nb.ipynb) for runnable code demonstrating all of the above.
