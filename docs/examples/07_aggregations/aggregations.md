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

At query time, you pick the aggregation with colon syntax:

```json
{
  "source_model": "orders",
  "measures": ["subtotal:sum", "subtotal:avg", "order_total:min", "order_total:max"],
  "dimensions": ["stores.name"]
}
```

`subtotal:sum` means "take the `subtotal` column and SUM it." `order_total:min` means "take the `order_total` column and find the MIN." One column definition, as many aggregations as you need.

## COUNT(*) and the star measure

COUNT(\*) doesn't aggregate a specific column — it counts rows. In SLayer, `*` is the "all rows" placeholder:

```json
{
  "measures": ["*:count", "revenue:sum"]
}
```

`*:count` produces `COUNT(*)`. Result column: `orders._count` (the underscore prefix distinguishes it from any dimension that might happen to be called `count`).

> **Note:** `*` can only be used with `count`. Combinations like `*:sum` or `*:avg` are invalid — use a named measure instead.

You can also count non-null values of a specific column: `email:count` produces `COUNT(email)`. And `customer_id:count_distinct` gives you `COUNT(DISTINCT customer_id)`.

## Built-in aggregations

These are always available — no definition needed:

| Aggregation | What it does |
|------------|-------------|
| `sum` | SUM(expr) |
| `avg` | AVG(expr) |
| `sum(window='90d')` / `avg(window='90d')` | trailing range SUM/AVG ending at each output bucket |
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
| `corr` | `price:corr(other=quantity)` — Pearson correlation between two columns |
| `covar_samp` / `covar_pop` | `price:covar_samp(other=quantity)` — sample / population covariance |

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

`sum` and `avg` can take a trailing time `window` when the query has a time
dimension:

```json
{
  "measures": [
    {"formula": "revenue:sum(window='30d')", "name": "revenue_30d"},
    {"formula": "revenue:avg(window='1y2m3w5d6h7min8s')", "name": "avg_window"}
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
{"formula": "score:trimmed_mean(lo=10, hi=90)"}
```

You can also override built-in aggregation defaults. If `weighted_avg` should default to a specific weight column in your model:

```yaml
aggregations:
  - name: weighted_avg
    params:
      - name: weight
        sql: subtotal
```

Now `tax_rate:weighted_avg` uses `subtotal` as the weight without you specifying it every time. But you can still override: `tax_rate:weighted_avg(weight=order_total)`.

## Controlling which aggregations apply

Not every aggregation makes sense for every column. `customer_id:avg`? Probably not useful. The `allowed_aggregations` field lets you whitelist:

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

SLayer validates this at query time and at model creation — if you try `customer_id:sum`, you get a clear error listing the valid options.

## first and last

`first` and `last` return the value from the earliest or latest record in each group, ordered by a time column. They need a time dimension to know what "earliest" and "latest" mean:

```json
{
  "measures": ["balance:last", "balance:first"],
  "time_dimensions": [{"dimension": "updated_at", "granularity": "month"}]
}
```

If you want to use a specific time column (overriding the query's time dimension), pass it as an argument:

```json
{"formula": "balance:last(created_at)"}
```

This explicit time argument takes priority over everything — query-level `time_dimensions`, `main_time_dimension`, and the model's `default_time_dimension`.

Don't confuse the `last` *aggregation* (`balance:last`) with the `last()` *transform* (`last(revenue:sum)`). The aggregation picks the latest record's value within each time bucket. The transform broadcasts the latest time bucket's aggregated value to every row. Different operations, different use cases.

## Percentiles

`median` is built in, but you might want the 95th percentile, or Q1/Q3:

```json
{
  "measures": [
    "latency:median",
    "latency:percentile(p=0.95)",
    "latency:percentile(p=0.25)"
  ]
}
```

## Composing with transforms and arithmetic

Arithmetic:

```json
{"formula": "revenue:sum / *:count", "name": "aov"}
```

Transforms:

```json
{"formula": "cumsum(revenue:sum)"}
{"formula": "change(revenue:sum)"}
{"formula": "time_shift(revenue:sum, -1, 'year')"}
```

Cross-model:

```json
{"formula": "customers.*:count"}
{"formula": "cumsum(customers.*:count)"}
```

## Result column naming

The colon becomes an underscore in result keys:

| Formula | Result key |
|---------|-----------|
| `revenue:sum` | `orders.revenue_sum` |
| `*:count` | `orders._count` |
| `revenue:avg` | `orders.revenue_avg` |
| `customers.*:count` | `orders.customers._count` |

When a query is saved as a model (`create_model` with a `query` parameter), these canonical names become the new model's column names.

---

See the [companion notebook](aggregations_nb.ipynb) for runnable code demonstrating all of the above.
