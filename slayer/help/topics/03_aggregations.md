# Aggregations

An aggregation is picked at query time via colon syntax: `measure:agg`. It is
not baked into the measure definition.

## Built-in aggregations

| Aggregation | Example | SQL |
|-------------|---------|-----|
| `sum` | `revenue:sum` | `SUM(expr)` |
| `avg` | `revenue:avg` | `AVG(expr)` |
| `min` / `max` | `revenue:min` | `MIN(expr)` / `MAX(expr)` |
| `count` | `*:count` | `COUNT(*)` |
| `count` (non-null) | `email:count` | `COUNT(email)` |
| `count_distinct` | `customer_id:count_distinct` | `COUNT(DISTINCT customer_id)` |
| `median` | `latency:median` | `PERCENTILE_CONT(0.5) …` |
| `percentile` | `latency:percentile(p=0.95)` | `PERCENTILE_CONT(0.95) …` |
| `weighted_avg` | `price:weighted_avg(weight=quantity)` | `SUM(price*qty)/SUM(qty)` |
| `first` / `last` | `balance:last(updated_at)` | earliest / latest record's value |

## first and last — per-group snapshots

`first` and `last` return the value from the earliest or latest **record** in
each group, ordered by a time column. They need to know which time column.
Resolution:

1. Explicit argument: `balance:last(updated_at)` — highest priority.
2. Query's `main_time_dimension`.
3. Single entry in `time_dimensions`.
4. First time dim appearing in `filters`.
5. Model's `default_time_dimension`.

If none resolves, the aggregation errors at query time.

Don't confuse:

- `:last` aggregation — per-group record's latest value.
- `last(x)` transform — broadcasts the most recent bucket's aggregated value to
  every row. See `help(topic='transforms')`.

## Allowed aggregations (whitelist)

A measure can restrict which aggregations make sense. Model-side:

```yaml
measures:
  - name: customer_id
    sql: customer_id
    allowed_aggregations: [count, count_distinct]
```

`customer_id:avg` would then error with a clear message listing the valid
options. Validated at both model creation and query time.

```json
{
  "source_model": "orders",
  "fields": ["customer_id:count_distinct"]
}
```

## Custom aggregations

Defined at model level. `{expr}` is the measure's SQL; named placeholders are
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
  "fields": [{"formula": "score:trimmed_mean(lo=10, hi=90)"}]
}
```

You can also override built-in defaults. If you declare `weighted_avg` with a
default `weight` of `quantity`, then `price:weighted_avg` uses it without the
arg, and `price:weighted_avg(weight=revenue)` overrides.

## See also

- `help(topic='formulas')` — where `:agg` fits in the broader formula language.
- `help(topic='transforms')` — `last()` transform vs `:last` aggregation.
- `help(topic='models')` — declaring measures and their `allowed_aggregations`.
