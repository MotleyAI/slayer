# Transforms

Transforms are functions applied to aggregated measures, producing computed
fields: `cumsum(revenue:sum)`, `change(revenue:sum)`, etc. Each transform
becomes an extra CTE in the generated SQL.

## The transform family

| Transform | Purpose | SQL strategy |
|-----------|---------|--------------|
| `cumsum(x)` | Running total over time | Window: `SUM(x) OVER (ORDER BY time)` |
| `time_shift(x, n)` | Value N buckets back/ahead | Self-join CTE on granularity |
| `time_shift(x, n, 'year')` | Value at a different granularity offset (e.g. YoY) | Self-join CTE with calendar arithmetic |
| `change(x)` | `x − previous(x)` | Self-join CTE |
| `change_pct(x)` | `(x − previous) / previous` | Self-join CTE |
| `lag(x, n)` / `lead(x, n)` | N rows back / ahead | `LAG` / `LEAD` window fn |
| `rank(x)` | Rank by x, descending | `RANK() OVER (ORDER BY x DESC)` |
| `last(x)` | Broadcast latest bucket's value to every row | Window + join |

## Self-join vs window-function — important trade-off

`time_shift`, `change`, and `change_pct` use **self-join CTEs**: they can reach
outside the current result set to fetch the previous/next value. Consequences:

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

`cumsum`, `time_shift`, `change`, `change_pct`, `last`, `lag`, `lead` all need
an ordering time dimension. Resolution: `main_time_dimension` → single
`time_dimensions` entry → model's `default_time_dimension` (if in query).
`rank` does **not** need a time dimension.

## Nesting

Transforms compose freely. The identity `cumsum(change(x)) == x − x[0]` holds
for rows after the first.

```json
{
  "source_model": "orders",
  "fields": [
    "revenue:sum",
    {"formula": "change(cumsum(revenue:sum))", "name": "cumsum_delta"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

## rank does not partition

`rank(x)` ranks across the **entire result set**. With multiple dimensions
(e.g. `status` + `month`), every `(status, month)` row is ranked together —
no auto-partition by dimension. Use `filters: ["rank(revenue:sum) <= 10"]` for
top-N.

## last() — broadcast transform

`last(x)` projects the most recent bucket's aggregated value onto every row:

```json
{
  "source_model": "orders",
  "fields": [
    "revenue:sum",
    {"formula": "last(revenue:sum)", "name": "latest_revenue"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

Useful for filtering on trend: `"filters": ["last(change(revenue:sum)) < 0"]`.

## See also

- `help(topic='time')` — granularity, whole_periods_only, main_time_dimension.
- `help(topic='filters')` — filtering on transform outputs.
- `help(topic='aggregations')` — `:last` aggregation vs `last()` transform.
