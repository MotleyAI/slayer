# Time

## Time points

A string compared with a DATE / TIMESTAMP expression or a `gran(col)` call is a time
point. An instant (`'2025-03-01 10:00:00'`) compares as written. A period is a
half-open range:

- absolute: `'2025'`, `'2025-Q1'`, `'2025-03'`, `'2025-W05'` (ISO week),
  `'2025-03-01'` (a date-only string means the whole day);
- relative, read from the clock (pinned by `SLAYER_NOW` when set): `'today'`,
  `'this month'` / `'last month'` / `'next month'` (any granularity), `'last 7 days'`
  (excludes the current day), `'3 months ago'`, `'year to date'`.

`ts >= P` starts at P, `ts <= P` ends with P, `ts = P` and `ts in '2025-Q1'` are
inside P.

## date_range

A time dimension's `date_range` is one period (`"last month"`) or `[lower, upper]`
with either bound null (one-sided). It decides which buckets come back, not which rows
a computation may read: `change`, `change_pct`, `time_shift` and `window=` aggregates
read rows before the range, so the first returned bucket already has its comparison.
Never widen the range to feed them.

`cumsum` accumulates from the first returned bucket. For a running total since the
first row, compute it in a named stage without `date_range`, then filter the time in
an outer stage:

```json
[
  {"name": "running", "source_model": "orders",
   "time_dimensions": [{"dimension": "order_date", "granularity": "month"}],
   "measures": [{"formula": "cumsum(sum(amount))", "name": "running_total"}]},
  {"source_model": "running", "dimensions": ["order_date", "running_total"],
   "filters": ["order_date >= '2025'"]}
]
```

`whole_periods_only: true` snaps date conditions to whole buckets and drops the current
incomplete bucket.

## Granularity calls

`month(created_at)` in `dimensions`, `time_dimensions`, filters or order buckets the
column: second, minute, hour, day, week (ISO, Monday), week_sunday, month, quarter,
year, or a custom granularity defined on the datasource (fiscal years and the like).

## Gaps

Buckets without rows are absent. A time dimension on `time_spine.timestamp` (with a
lower `date_range` bound) returns every bucket, empty ones included, and lines up
several models on one time axis.
