# Time

## Time points

A string compared with a DATE / TIMESTAMP expression or a `date_trunc` bucket is a time
point. An instant (`'2025-03-01 10:00:00'`) compares as written, and so does a date
(`'2025-03-01'` is `2025-03-01 00:00:00` after `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`).
A period is a half-open range:

- absolute: `'2025'`, `'2025-Q1'`, `'2025-03'`, `'2025-W05'` (ISO week), and a date
  after `in` (`ts in '2025-03-01'` is that whole day; `ts < '2025-03-02'` ends with it);
- relative, read from the clock (pinned by `SLAYER_NOW` when set): `'today'`,
  `'this month'` / `'last month'` / `'next month'` (any granularity), `'last 7 days'`
  (excludes the current day), `'3 months ago'`, `'year to date'`.

`ts >= P` starts at P, `ts <= P` ends with P, `ts = P` and `ts in '2025-Q1'` are
inside P.

## date_range

A time dimension's `date_range` is one period (`"last month"`) or `[lower, upper]`
with either bound null (one-sided). It decides which buckets come back, not which rows
a computation may read: `change`, `change_pct`, `time_shift` and `window=` aggregates
read rows before the range (a forward `time_shift`, after it), so the first returned bucket
already has its comparison.
Never widen the range to feed them.

`cumsum` accumulates from the first returned bucket. A running total since the first
row, in two stages:

```json
[
  {"name": "running", "population": "orders",
   "time_dimensions": [{"field": "orders.order_date", "granularity": "month"}],
   "metrics": [{"metric": "cumsum(sum(orders.amount))", "alias": "running_total"}]},
  {"population": "running", "dimensions": ["running.order_date", "running.running_total"],
   "filter": "running.order_date >= '2025'"}
]
```

`whole_periods_only: true` snaps date conditions to whole buckets and drops the current
incomplete bucket.

## Time buckets

`date_trunc('month', orders.created_at)` in `dimensions`, `time_dimensions`, filters or order buckets
the field, or any DATE/TIMESTAMP expression
(`date_trunc('month', coalesce(orders.shipped_at, orders.created_at))`):
second, minute, hour, day, week (ISO, Monday), week_sunday, month, quarter, year, or a custom
granularity defined on the datasource (fiscal years and the like). `month(x)` and its siblings
`year`, `quarter`, `week`, `day`, `hour`, `minute`, `second` are integer extraction
(`date_part('month', x)`, the month of year), never a bucket.

## Time of day

A `Time` field, `TIME '10:30'`, `current_time()` and `CAST(x AS TIME)` are a time of day: group, order,
`min` / `max`, compare with another one or a `'HH:MM'` string, and read `hour` / `minute` / `second`.
It takes no granularity, `date_range` or arithmetic.

## Time zones

`"time_zone": "America/New_York"` on a query buckets, filters and returns timestamps as that zone's
wall clock, and "today" / `now()` follow it. A `DateTime` field converts from its own `time_zone`, else its
datasource's; `time_zone: civil` never converts; a field with neither is read as stored and warns
`time_zone_undeclared`.
