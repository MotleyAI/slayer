# Time

Time is the most load-bearing dimension in most analytical queries. SLayer
treats it specially in a few places.

## time_dimensions vs plain dimensions on the same column

- `time_dimensions: [{"dimension": "created_at", "granularity": "month"}]`
  → grouped by the **truncated** value. One row per month.
- `dimensions: ["created_at"]` → grouped by the **raw** timestamp. One row per
  distinct timestamp.

You can use both forms of the same column in one query.

## Granularities

`second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`. Always
supplied alongside a time dimension, and also used as an argument by
`time_shift(x, n, 'year')`.

## date_range

A time dimension may carry a `date_range: [start, end]` (ISO dates):

```json
{
  "source_model": "orders",
  "fields": ["revenue:sum"],
  "time_dimensions": [{
    "dimension": "created_at",
    "granularity": "month",
    "date_range": ["2025-01-01", "2025-12-31"]
  }]
}
```

This becomes a WHERE filter. It does **not** itself snap to bucket edges (the
first/last bucket may be partial) — use `whole_periods_only` for that.

## whole_periods_only

When `true`, SLayer snaps the `date_range` to the granularity's bucket edges
and drops the current incomplete bucket. Useful when a dashboard should not
show "this month is half-done, the bar looks tiny":

```json
{
  "source_model": "orders",
  "fields": ["revenue:sum"],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
  "whole_periods_only": true
}
```

## Which time dimension wins for transforms

When a transform needs "the" time dimension:

1. Query's `main_time_dimension` (highest priority).
2. The single entry in `time_dimensions`, if only one.
3. Model's `default_time_dimension`, if it is among the query's time dims.

Set `main_time_dimension` whenever you have two or more time dimensions.

## The three meanings of "last" — don't mix them up

SLayer has **three** distinct things named `last`:

1. `:last(time_col)` — the **aggregation**. Per group, returns the value from
   the record with the latest `time_col`. See `help(topic='aggregations')`.

2. `last(x)` — the **transform**. Broadcasts the aggregated value from the
   most recent time bucket to every row. See `help(topic='transforms')`.

3. `last(…)` inside a `filters` string (e.g. `"last(change(revenue:sum)) < 0"`)
   — a post-filter on the transform output. See `help(topic='filters')`.

They all concern "latest something" but operate on different levels: record /
bucket / filter. Pick the one that matches your question.

## Year-over-year with time_shift

```json
{
  "source_model": "orders",
  "fields": [
    "revenue:sum",
    {"formula": "time_shift(revenue:sum, -1, 'year')", "name": "prev_year"},
    {"formula": "revenue:sum / time_shift(revenue:sum, -1, 'year') - 1",
     "name": "yoy_growth"}
  ],
  "time_dimensions": [{
    "dimension": "created_at", "granularity": "month",
    "date_range": ["2025-01-01", "2025-12-31"]
  }]
}
```

## See also

- `help(topic='transforms')` — the transform family.
- `help(topic='aggregations')` — `:first` and `:last` aggregations.
- `help(topic='queries')` — where `time_dimensions` and `main_time_dimension` sit.
