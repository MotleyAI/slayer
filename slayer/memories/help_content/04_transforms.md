# Transforms

A transform wraps an aggregated value and computes across result rows. Transforms
work in measures, filters and order, and nest in either order
(`cumsum(change(sum(amount)))`, `change(cumsum(sum(amount)))`).

## Catalogue

| Transform | Result |
|---|---|
| `cumsum(x)` | running total, starting at the first returned bucket |
| `change(x)` | `x` minus the previous bucket's value |
| `change_pct(x)` | `(x - previous) / previous`; NULL when the previous value is 0 or missing |
| `time_shift(x, n[, 'year'])` | the value `n` buckets (or another granularity's periods) away |
| `lag(x, n)` / `lead(x, n)` | the value `n` result rows back / ahead |
| `first(x)` / `last(x)` | the earliest / latest bucket's value, repeated on every row |
| `consecutive_periods(predicate)` | length of the current trailing run where the predicate holds |
| `rank(x, direction=...)` | rank, ties share a rank, gaps after ties |
| `dense_rank(x, direction=...)` | rank without gaps |
| `percent_rank(x)` | relative rank in [0, 1], lowest value 0 |
| `ntile(x, n=N)` | bucket 1..N, bucket 1 holds the lowest values |

## Time axis

Every transform except the rank family (`rank`, `dense_rank`, `percent_rank`, `ntile`)
needs a time dimension. With several, set `main_time_dimension`. Time-ordered
transforms run separately for every combination of the other dimensions: grouped by
store, `cumsum` gives one running total per store and `change` never compares one
store with another.

`change`, `change_pct` and `time_shift` match buckets by calendar, so a missing
month gives NULL rather than comparing with an older one, and they read the bucket
before the query's date range. `lag` / `lead` shift by result row: NULL at the edges
and blind to gaps. Prefer `change_pct` for growth and `time_shift` for custom
arithmetic or a different grain (`time_shift(x, -1, 'year')` on months is
year-over-year).

`consecutive_periods` takes a predicate (`sum(amount) > 0`) or a value (true when
non-NULL and non-zero); a false or missing bucket resets the run to 0.

## Ranking

`rank` and `dense_rank` require `direction='desc'` (highest is 1) or `'asc'`.
`partition_by=` ranks within groups; its keys must be query dimensions. A NULL value
ranks NULL, so a rank filter drops it. Top N per group is a filter:
`rank(sum(amount), partition_by=region, direction='desc') <= 3`. Never write raw
`OVER (...)` SQL.

## Limits

`partition_by=` is accepted on the rank family only; other transforms partition by
the query's dimensions. A row-level column that is not a query dimension cannot be
mixed into a transform's input. A row-level condition applies before transforms, so
it changes what a rank or running total sees; to keep the transform over all rows and
drop rows afterwards, filter in an outer stage.
