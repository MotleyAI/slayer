# Transforms

Transforms are functions applied to aggregated measures, producing computed
measures: `cumsum(revenue:sum)`, `change(revenue:sum)`, etc. Each transform
becomes an extra CTE in the generated SQL.

## The transform family

| Transform | Purpose | SQL strategy |
|-----------|---------|--------------|
| `cumsum(x)` | Running total over time | Window: `SUM(x) OVER (PARTITION BY dims ORDER BY time)` |
| `time_shift(x, n)` | Value N periods back/ahead | Self-join CTE with INTERVAL offset |
| `time_shift(x, n, 'year')` | Value at a different granularity offset (e.g. YoY) | Self-join CTE with INTERVAL offset |
| `change(x)` | `x − previous(x)` | Desugars to `x − time_shift(x, -1)` |
| `change_pct(x)` | `(x − previous) / previous` | Desugars to `(x − ts) / ts` where `ts = time_shift(x, -1)` |
| `lag(x, n)` / `lead(x, n)` | N rows back / ahead | `LAG` / `LEAD` window fn, partitioned by dimensions |
| `consecutive_periods(predicate)` | Current trailing run length where predicate is true | Staged window CTEs with reset groups |
| `rank(x[, partition_by=...])` | Rank by x, descending; ties skip ranks | `RANK() OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `percent_rank(x[, partition_by=...])` | Relative rank in `[0, 1]`, descending | `PERCENT_RANK() OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `dense_rank(x[, partition_by=...])` | Rank by x, descending; ties don't skip ranks | `DENSE_RANK() OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `ntile(x, n=N[, partition_by=...])` | Bucket rows into N equal groups, descending | `NTILE(N) OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `first(x)` | Broadcast earliest bucket's value to every row | Window |
| `last(x)` | Broadcast latest bucket's value to every row | Window |

## Self-join vs window-function — important trade-off

`time_shift` (and `change`/`change_pct` which desugar into it) uses a
**self-join CTE** with an INTERVAL-shifted time column. The shifted sub-query
applies the time offset to every occurrence of the time dimension (WHERE,
GROUP BY, SELECT), so it can reach outside the current result set to fetch
the previous/next value. Consequences:

- No NULLs at the first / last rows when the database actually has the data.
- Handles **gaps** in the time series correctly — shifts by calendar, not by row.
- Slightly heavier SQL.

`lag` and `lead` use SQL `LAG` / `LEAD`:

- NULLs at the first / last N rows (the window can't see beyond the result set).
- Shift by **row position**, not by period — skips produce wrong "previous".
- Faster, simpler SQL.

Use `time_shift`, `change`, `change_pct` unless you have a specific reason to
prefer `lag` / `lead`.

## Time dimension requirement

All time-ordered transforms (`cumsum`, `time_shift`, `change`, `change_pct`,
`first`, `last`, `lag`, `lead`, `consecutive_periods`) require an explicit
`time_dimensions` entry in the query. With a single entry it's used
automatically; with 2+ entries, `main_time_dimension` disambiguates (or
`default_time_dimension` if among query's time dims). The rank-family
transforms (`rank`, `percent_rank`, `dense_rank`, `ntile`) do **not** need a
time dimension.

Time-ordered window transforms partition by the query's non-time dimensions.
A `cumsum(revenue:sum)` grouped by `status` computes one running total per
status. The rank-family transforms default to **no `PARTITION BY`** — they
rank across the entire result set unless `partition_by=` is passed.

## consecutive_periods

`consecutive_periods(predicate)` returns an integer count of how many
consecutive output periods, ending at the current row, have `predicate` true.
False or NULL breaks the run and returns 0 for that row.

```json
{
  "source_model": "orders",
  "measures": [
    {"formula": "consecutive_periods(revenue:sum > 0)", "name": "positive_run"},
    {"formula": "consecutive_periods(revenue:sum > 0) >= 3", "name": "positive_3_periods"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

## Nesting

Window transforms can wrap self-join transforms: `cumsum(change(x))` works
(the identity `cumsum(change(x)) == x − x[0]` holds for rows after the first).
Self-join transforms cannot wrap other self-join or change transforms.

```json
{
  "source_model": "orders",
  "measures": [
    "revenue:sum",
    {"formula": "cumsum(change(revenue:sum))", "name": "cumsum_delta"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

## Rank-family transforms

`rank`, `percent_rank`, `dense_rank`, and `ntile` are all timeless ranking
transforms. They order rows by the inner measure descending and emit a per-row
rank value. By default they do **not** partition — every row is ranked against
every other row in the result set:

```json
{
  "source_model": "orders",
  "dimensions": ["customer_name"],
  "measures": [
    "revenue:sum",
    {"formula": "rank(revenue:sum)", "name": "rnk"}
  ],
  "filters": ["rank(revenue:sum) <= 10"]
}
```

Choosing the right one:

- `rank` — standard SQL `RANK`; ties share a rank, skip the next (`1, 1, 3`).
- `dense_rank` — ties share a rank, no gaps (`1, 1, 2`). Use for "top N tiers".
- `percent_rank` — relative rank in `[0, 1]`. Use for cross-query-comparable rankings.
- `ntile(x, n=N)` — required `n=` kwarg, must be a positive integer; bucket all rows into `N` equal-sized groups (`1` is the top bucket).

To rank within partitions, pass `partition_by=` referencing one or more query
dimensions or time dimensions. Columns must already be grouped on — partitioning
by a non-dimension column errors at enrichment time:

```json
{
  "source_model": "orders",
  "dimensions": ["region", "customer_name"],
  "measures": [
    "revenue:sum",
    {"formula": "dense_rank(revenue:sum, partition_by=region)", "name": "rnk_in_region"},
    {"formula": "ntile(revenue:sum, n=4, partition_by=region)", "name": "quartile_in_region"}
  ]
}
```

Multiple partition columns: `partition_by=[region, channel]`. Cross-model
dotted paths work too: `partition_by=customers.region`.

Raw `OVER (...)` SQL inside a `ModelMeasure.formula` or filter string is
rejected with an actionable error pointing at the rank-family / `first()` /
`last()` / `lag()` / `lead()` transforms. For non-standard window expressions,
define a `Column` whose `sql` is the window expression and filter on the
column — SLayer auto-promotes the predicate to a post-aggregation outer
`WHERE`.

## first() and last() — broadcast transforms

`first(x)` projects the **earliest** bucket's aggregated value onto every row.
`last(x)` projects the **most recent** bucket's aggregated value onto every row.

```json
{
  "source_model": "orders",
  "measures": [
    "revenue:sum",
    {"formula": "first(revenue:sum)", "name": "initial_revenue"},
    {"formula": "last(revenue:sum)", "name": "latest_revenue"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

Useful for filtering on trend: `"filters": ["last(change(revenue:sum)) < 0"]`.

## See also

- `help(topic='time')` — granularity, whole_periods_only, main_time_dimension.
- `help(topic='filters')` — filtering on transform outputs.
- `help(topic='aggregations')` — `:first`/`:last` aggregation vs `first()`/`last()` transform.
