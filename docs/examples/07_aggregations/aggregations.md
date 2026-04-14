# Measures and Aggregations Are Separate Things

Most semantic layers force you to bake the aggregation into the measure definition. You want revenue? Define `revenue_sum`. Want average revenue too? Define `revenue_avg`. Five aggregation types per numeric column, times twenty columns, and you're staring at a hundred measure definitions before you've written a single query.

SLayer takes a different approach: **a measure is just a named SQL expression** — a row-level fact about your data. The **aggregation** — how you want to roll it up — is specified when you query, not when you define the model.

## What this looks like

A model defines measures as bare expressions:

```yaml
measures:
  - name: revenue
    sql: amount
  - name: price
    sql: unit_price
  - name: quantity
    sql: qty
```

No `type: sum` or `type: avg`. Just what the column is.

At query time, you pick the aggregation with colon syntax:

```json
{
  "source_model": "orders",
  "fields": ["revenue:sum", "revenue:avg", "price:min", "price:max"],
  "dimensions": ["status"]
}
```

`revenue:sum` means "take the `revenue` measure (which is the `amount` column) and SUM it." `price:min` means "take the `price` measure and find the MIN." One measure definition, as many aggregations as you need.

## COUNT(*) and the star measure

COUNT(\*) doesn't aggregate a specific column — it counts rows. In SLayer, `*` is the "all rows" placeholder:

```json
{
  "fields": ["*:count", "revenue:sum"]
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
| `min` / `max` | MIN/MAX(expr) |
| `count` | COUNT(expr), or COUNT(\*) with `*` |
| `count_distinct` | COUNT(DISTINCT expr) |
| `first` / `last` | Value from the earliest/latest record per group (by time) |
| `weighted_avg` | SUM(expr \* weight) / SUM(weight) |
| `median` | PERCENTILE_CONT(0.5) |
| `percentile` | PERCENTILE_CONT(p) — specify `p` as an argument |

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
        sql: quantity
```

Now `price:weighted_avg` uses `quantity` as the weight without you specifying it every time. But you can still override: `price:weighted_avg(weight=revenue)`.

## Controlling which aggregations apply

Not every aggregation makes sense for every measure. `customer_id:avg`? Probably not useful. The `allowed_aggregations` field lets you whitelist:

```yaml
measures:
  - name: customer_id
    sql: customer_id
    allowed_aggregations: [count, count_distinct]
  - name: revenue
    sql: amount
    allowed_aggregations: [sum, avg, min, max, weighted_avg]
```

SLayer validates this at query time and at model creation — if you try `customer_id:sum`, you get a clear error listing the valid options.

## first and last

`first` and `last` return the value from the earliest or latest record in each group, ordered by a time column. They need a time dimension to know what "earliest" and "latest" mean:

```json
{
  "fields": ["balance:last", "balance:first"],
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
  "fields": [
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
