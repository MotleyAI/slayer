# Columns and Aggregations Are Separate Things

Most semantic layers force you to bake the aggregation into the column definition. You want revenue? Define `revenue_sum`. Want average revenue too? Define `revenue_avg`. Five aggregation types per numeric column, times twenty columns, and you're staring at a hundred definitions before you've written a single query.

SLayer takes a different approach: **a column is just a named SQL expression** — a row-level fact about your data. The **aggregation** — how you want to roll it up — is specified when you query, not when you define the model.

## What this looks like

A model defines columns as bare expressions:

```yaml
columns:
  - name: subtotal
    sql: subtotal
    type: number
  - name: tax_paid
    sql: tax_paid
    type: number
  - name: order_total
    sql: order_total
    type: number
```

No `type: sum` or `type: avg`. Just what the column is.

At query time, you pick the aggregation with a function call:

```json
{
  "source_model": "orders",
  "measures": ["sum(subtotal)", "avg(subtotal)", "min(order_total)", "max(order_total)"],
  "dimensions": ["stores.name"]
}
```

`sum(subtotal)` means "take the `subtotal` column and SUM it." `min(order_total)` means "take the `order_total` column and find the MIN." One column definition, as many aggregations as you need.

## Aggregating an expression

An aggregation can also take a same-model **expression** as its value, not
just a bare column. Declared parameters may be passed positionally in
declaration order, so `percentile(price, 0.9)` means `p=0.9`:

```json
{
  "source_model": "orders",
  "measures": [
    {"formula": "sum(order_total - tax_paid)", "name": "net_revenue"},
    "count_distinct(upper(email))",
    "percentile(subtotal * quantity, p=0.5)"
  ]
}
```

The expression may use bare same-model columns, scalar functions, arithmetic,
and literals; an unnamed one derives its result key from the expression
(`sum(order_total - tax_paid)` → `orders.order_total_tax_paid_sum`). Dotted
joined-model paths inside an expression, filtered columns, and nested
aggregations are rejected with clear errors — see
[Formulas → Expression aggregation](../../concepts/formulas.md#expression-aggregation).

## COUNT(*) and the star measure

COUNT(\*) doesn't aggregate a specific column — it counts rows. In SLayer, `*` is the "all rows" placeholder:

```json
{
  "measures": ["count(*)", "sum(revenue)"]
}
```

`count(*)` produces `COUNT(*)`. Result column: `orders._count` (the underscore prefix distinguishes it from any dimension that might happen to be called `count`).

> **Note:** `*` can only be used with `count`. Combinations like `sum(*)` or `avg(*)` are invalid — use a named measure instead.

You can also count non-null values of a specific column: `count(email)` produces `COUNT(email)`. And `count_distinct(customer_id)` gives you `COUNT(DISTINCT customer_id)`.

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
| `weighted_avg` | SUM(expr \* weight) / SUM(weight) |
| `median` | PERCENTILE_CONT(0.5) — see database support below |
| `percentile` | PERCENTILE_CONT(p) — specify `p` as an argument; see database support below |
| `stddev_samp` / `stddev_pop` | Sample / population standard deviation |
| `var_samp` / `var_pop` | Sample / population variance |
| `corr` | `corr(price, other=quantity)` — Pearson correlation between two columns |
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
  "measures": [
    {"formula": "sum(revenue, window='30d')", "name": "revenue_30d"},
    {"formula": "avg(revenue, window='1y2m3w5d6h7min8s')", "name": "avg_window"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
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
    formula: "AVG(CASE WHEN {value} BETWEEN {lo} AND {hi} THEN {value} END)"
    params:
      - name: lo
        sql: "0"
      - name: hi
        sql: "1000"
```

`{value}` is the measure's SQL expression. `{lo}` and `{hi}` are parameters with defaults that can be overridden at query time:

```json
{"formula": "trimmed_mean(score, lo=10, hi=90)"}
```

You can also override built-in aggregation defaults. If `weighted_avg` should default to a specific weight column in your model:

```yaml
aggregations:
  - name: weighted_avg
    params:
      - name: weight
        sql: subtotal
```

Now `weighted_avg(tax_rate)` uses `subtotal` as the weight without you specifying it every time. But you can still override: `weighted_avg(tax_rate, weight=order_total)`.

## Controlling which aggregations apply

Not every aggregation makes sense for every column. `avg(customer_id)`? Probably not useful. The `allowed_aggregations` field lets you whitelist:

```yaml
columns:
  - name: customer_id
    sql: customer_id
    type: number
    allowed_aggregations: [count, count_distinct]
  - name: revenue
    sql: amount
    type: number
    allowed_aggregations: [sum, avg, min, max, weighted_avg]
```

SLayer validates this at query time and at model creation — if you try `sum(customer_id)`, you get a clear error listing the valid options.

## first and last

`first` and `last` return the value from the earliest or latest record in each group, ordered by a time column. They need a time dimension to know what "earliest" and "latest" mean:

```json
{
  "measures": ["last(balance)", "first(balance)"],
  "time_dimensions": [{"dimension": "updated_at", "granularity": "month"}]
}
```

If you want to use a specific time column (overriding the query's time dimension), pass it as an argument:

```json
{"formula": "last(balance, created_at)"}
```

This explicit time argument takes priority over everything — query-level `time_dimensions`, `main_time_dimension`, and the model's `default_time_dimension`.

Don't confuse the `last` *aggregation* (`last(balance)`) with the `last()` *transform* (`last(sum(revenue))`). The aggregation picks the latest record's value within each time bucket. The transform broadcasts the latest time bucket's aggregated value to every row. Different operations, different use cases.

## Percentiles

`median` is built in, but you might want the 95th percentile, or Q1/Q3:

```json
{
  "measures": [
    "median(latency)",
    "percentile(latency, p=0.95)",
    "percentile(latency, p=0.25)"
  ]
}
```

## Composing with transforms and arithmetic

Arithmetic:

```json
{"formula": "sum(revenue) / count(*)", "name": "aov"}
```

Transforms:

```json
{"formula": "cumsum(sum(revenue))"}
{"formula": "change(sum(revenue))"}
{"formula": "time_shift(sum(revenue), -1, 'year')"}
```

Cross-model:

```json
{"formula": "count(customers.*)"}
{"formula": "cumsum(count(customers.*))"}
```

Conditionals — `CASE WHEN` (or `iif(cond, then, otherwise)`) can branch on an aggregated value; the branch runs after grouping:

```json
{"formula": "CASE WHEN sum(revenue) >= 10000 THEN 'high' ELSE 'standard' END", "name": "tier"}
```

See [Formulas — Conditionals](../../concepts/formulas.md#conditionals-case-when-iif) for branch-typing rules, and [Queries — Expression dimensions](../../concepts/queries.md) for grouping by a computed expression.

## Share of parent (`partition_by`)

Most aggregations take an optional `partition_by=` to compute over a subset of the query's dimensions, repeated across the finer rows — `SUM(x) OVER (PARTITION BY …)`:

```json
{
  "source_model": "orders",
  "dimensions": ["region", "city"],
  "measures": [
    {"formula": "sum(revenue) / sum(revenue, partition_by=region)", "name": "share_of_region"},
    {"formula": "sum(revenue) / sum(revenue, partition_by=[])", "name": "share_of_total"}
  ]
}
```

`partition_by=region` is the region total on every city row (so `share_of_region` sums to 1.0 per region); `partition_by=[]` is the grand total; a list (`[region, channel]`) or dotted path also work. The total is computed over rows passing row-level filters — filters on the measure (`having`) and pagination never change it.

A local `partition_by` aggregate also composes with the rest of the query: combined with `window=` (a rolling total at the partition grain, per the query's time bucket), on `first`/`last`, nested inside a transform (`cumsum(sum(revenue, partition_by=region))`), and referenced in a filter or `order` target (`sum(revenue, partition_by=region) > 5000`) — for filters and `order` targets, cross-model included (`sum(customers.spend, partition_by=customers.regions.name) > 100`). A filter's top-level `AND` conjuncts route independently; a single predicate valid as neither a row-level field nor a measure (e.g. a partitioned aggregate OR-ed with a raw row column that isn't a query dimension) raises a typing error naming both failures. Cross-model `partition_by` composes the same way — with `window=` when the query's active time dimension is attributable from the aggregate's root, on `first`/`last` (`last(customers.spend, partition_by=customers.tier)`), and nested inside a transform.

A partitioned aggregate can itself be re-aggregated — `{"formula": "avg(sum(revenue, partition_by=[city, region]))", "name": "avg_city_total"}` grouped by `region` averages each region's **city totals** (one input per city cell, never a row-weighted value), and the outer aggregation can take a parameter at the same grain — `weighted_avg(sum(revenue, partition_by=[city, region]), weight=count(id, partition_by=[city, region]))` weights each city total by its row count; see [re-aggregation](../../concepts/formulas.md#re-aggregation-aggregate-over-an-attached-value).

## Result column naming

The aggregation and column join with an underscore in result keys:

| Formula | Result key |
|---------|-----------|
| `sum(revenue)` | `orders.revenue_sum` |
| `count(*)` | `orders._count` |
| `avg(revenue)` | `orders.revenue_avg` |
| `count(customers.*)` | `orders.customers._count` |
| `sum(revenue, partition_by=region)` | `orders.revenue_sum_partition_by_region` |

When a query is saved as a model (`create_model` with a `query` parameter), these canonical names become the new model's column names.

---

See the [companion notebook](aggregations_nb.ipynb) for runnable code demonstrating all of the above.
