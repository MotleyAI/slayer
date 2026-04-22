# Transforms

Transforms are functions applied to aggregated measures, producing computed
fields: `cumsum(revenue:sum)`, `change(revenue:sum)`, etc. Each transform
becomes an extra CTE in the generated SQL.

## The transform family

| Transform | Purpose | SQL strategy |
|-----------|---------|--------------|
| `cumsum(x)` | Running total over time | Window: `SUM(x) OVER (ORDER BY time)` |
| `time_shift(x, n)` | Value N periods back/ahead | Self-join CTE with INTERVAL offset |
| `time_shift(x, n, 'year')` | Value at a different granularity offset (e.g. YoY) | Self-join CTE with INTERVAL offset |
| `change(x)` | `x − previous(x)` | Desugars to `x − time_shift(x, -1)` |
| `change_pct(x)` | `(x − previous) / previous` | Desugars to `(x − ts) / ts` where `ts = time_shift(x, -1)` |
| `lag(x, n)` / `lead(x, n)` | N rows back / ahead | `LAG` / `LEAD` window fn |
| `rank(x)` | Rank by x, descending | `RANK() OVER (ORDER BY x DESC)` |
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
`first`, `last`, `lag`, `lead`) require an explicit `time_dimensions` entry in
the query. With a single entry it's used automatically; with 2+ entries,
`main_time_dimension` disambiguates (or `default_time_dimension` if among
query's time dims). `rank` does **not** need a time dimension.

## Nesting

Window transforms can wrap self-join transforms: `cumsum(change(x))` works
(the identity `cumsum(change(x)) == x − x[0]` holds for rows after the first).
Self-join transforms cannot wrap other self-join or change transforms.

```json
{
  "source_model": "orders",
  "fields": [
    "revenue:sum",
    {"formula": "cumsum(change(revenue:sum))", "name": "cumsum_delta"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

## rank does not partition

`rank(x)` ranks across the **entire result set**. With multiple dimensions
(e.g. `status` + `month`), every `(status, month)` row is ranked together —
no auto-partition by dimension. Use `filters: ["rank(revenue:sum) <= 10"]` for
top-N.

## first() and last() — broadcast transforms

`first(x)` projects the **earliest** bucket's aggregated value onto every row.
`last(x)` projects the **most recent** bucket's aggregated value onto every row.

```json
{
  "source_model": "orders",
  "fields": [
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
