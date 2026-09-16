# Aggregations

An aggregation is picked at query time via colon syntax (`measure:agg`) or
the equivalent functional spelling (`agg(measure)` — e.g. `sum(revenue)`,
`count(*)`, `percentile(price, p=0.9)`); both produce identical SQL and
result keys, and the functional form also accepts a same-model expression
(`sum(amount - cost)`). It is
not baked into the measure definition.

## Built-in aggregations

| Aggregation | Example | SQL |
|-------------|---------|-----|
| `sum` | `sum(revenue)` | `SUM(expr)` |
| `avg` | `avg(revenue)` | `AVG(expr)` |
| `sum` / `avg` with `window` | `sum(revenue, window='90d')` | trailing range aggregate |
| `min` / `max` | `min(revenue)` | `MIN(expr)` / `MAX(expr)` |
| `count` | `count(*)` | `COUNT(*)` |
| `count` (non-null) | `count(email)` | `COUNT(email)` |
| `count_distinct` | `count_distinct(customer_id)` | `COUNT(DISTINCT customer_id)` |
| `median` | `median(latency)` | `PERCENTILE_CONT(0.5) …` |
| `percentile` | `percentile(latency, p=0.95)` | `PERCENTILE_CONT(0.95) …` |
| `weighted_avg` | `weighted_avg(price, weight=quantity)` | `SUM(price*qty)/SUM(qty)` |
| `stddev_samp` | `stddev_samp(latency)` | `STDDEV_SAMP(expr)` — NULL when N ≤ 1 |
| `stddev_pop` | `stddev_pop(latency)` | `STDDEV_POP(expr)` — 0 at N=1, NULL at N=0 |
| `var_samp` | `var_samp(latency)` | `VAR_SAMP(expr)` (or `VARIANCE` on SQLite/MySQL) |
| `var_pop` | `var_pop(latency)` | `VAR_POP(expr)` (or `VARIANCE_POP` on SQLite/MySQL) |
| `corr` | `corr(price, other=quantity)` | `CORR(price, quantity)` — Pearson r |
| `covar_samp` | `covar_samp(price, other=quantity)` | `COVAR_SAMP(price, quantity)` — sample covariance |
| `covar_pop` | `covar_pop(price, other=quantity)` | `COVAR_POP(price, quantity)` — population covariance |
| `first` / `last` | `last(balance, updated_at)` | earliest / latest record's value |

## first and last — per-group snapshots

`first` and `last` return the value from the earliest or latest **record** in
each group, ordered by a time column. They need to know which time column.
Resolution:

1. Explicit argument: `last(balance, updated_at)` — highest priority.
2. Query's `main_time_dimension`.
3. Single entry in `time_dimensions`.
4. First time dim appearing in `filters`.
5. Model's `default_time_dimension`.

If none resolves, the aggregation errors at query time.

Don't confuse:

- `:first`/`:last` aggregation — per-group record's earliest/latest value.
- `first(x)`/`last(x)` transform — broadcasts the earliest/most recent bucket's
  aggregated value to every row. See `memory:help.transforms`.

## Windowed sum and average

`sum` and `avg` accept `window='...'` for trailing time-window aggregations:

```json
{
  "source_model": "orders",
  "measures": [
    {"formula": "sum(revenue, window='30d')", "name": "revenue_30d"},
    {"formula": "avg(revenue, window='1y2m')", "name": "avg_14m"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

The window is applied to raw source rows and ends at each output bucket's end.
It can be larger than, equal to, or smaller than the query time granularity.
Duration syntax is compact: `y`, `m`, `w`, `d`, `h`, `min`, `s`, combinable as
in `1y2m3w5d6h7min8s`, `90d`, `6h`, or `15min`.

## Coarser grain — `partition_by` (share of parent)

Most aggregations take an optional `partition_by=` to compute over a subset of
the query's dimensions, repeated across the finer rows (like `SUM(x) OVER
(PARTITION BY …)`):

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

`partition_by=region` is the region total on every city row; `partition_by=[]` is
the grand total; a list (`[region, channel]`) or dotted path also work. Computed
over rows passing row-level filters (HAVING/pagination never change the parent
total). Composes with `window=`, `first`/`last`, transforms, and filters; only
`partition_by` on a *cross-model* `first`/`last` aggregate is still deferred.

## Allowed aggregations (whitelist)

A column can restrict which aggregations make sense. Model-side:

```yaml
columns:
  - name: customer_id
    sql: customer_id
    type: number
    allowed_aggregations: [count, count_distinct]
```

`avg(customer_id)` would then error with a clear message listing the valid
options. Validated at both model creation and query time.

```json
{
  "source_model": "orders",
  "measures": ["count_distinct(customer_id)"]
}
```

## Custom aggregations

Defined at model level. `{value}` is the measure's SQL; named placeholders are
kwargs:

```yaml
aggregations:
  - name: trimmed_mean
    formula: "AVG(CASE WHEN {value} BETWEEN {lo} AND {hi} THEN {value} END)"
    params:
      - {name: lo, sql: "0"}
      - {name: hi, sql: "1000"}
```

Query time:

```json
{
  "source_model": "orders",
  "measures": [{"formula": "trimmed_mean(score, lo=10, hi=90)"}]
}
```

You can also override built-in defaults. If you declare `weighted_avg` with a
default `weight` of `quantity`, then `weighted_avg(price)` uses it without the
arg, and `weighted_avg(price, weight=revenue)` overrides.

## See also

- `memory:help.formulas` — where `:agg` fits in the broader formula language.
- `memory:help.transforms` — `first()`/`last()` transforms vs `:first`/`:last` aggregations.
- `memory:help.models` — declaring measures and their `allowed_aggregations`.
