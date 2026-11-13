# Time

The single reference for how SLayer reads time: time points, `date_range`, time buckets, frame bounds, `whole_periods_only`, the time spine and custom granularities. Date *functions* (`date_part`, `date_diff`, `date_add`, `now()`, `current_date()`) are in [References](references.md#date-and-time-functions).

## Time points

A string compared with a temporal expression — or used as a `date_range` bound — is a **time point**: either an **instant** or a **period** `[start, next_start)`.

| Form | Example | Meaning |
|---|---|---|
| Instant | `'2025-03-01 10:00:00'`, `'2025-03-01T10:00'`, `'2025-03-01 10:00:00.250'` | That moment (up to six fractional digits; no zone offset) |
| Date | `'2025-03-15'` | In a comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`, `BETWEEN`), the instant `2025-03-15 00:00:00`; after `in` / `not in` and in `date_range`, the day `[2025-03-15, 2025-03-16)` |
| Month | `'2025-03'` | `[2025-03-01, 2025-04-01)` |
| Quarter | `'2025-Q1'` | `[2025-01-01, 2025-04-01)` |
| ISO week | `'2025-W05'` | `[2025-01-27, 2025-02-03)`, Monday-anchored |
| Year | `'2025'` | `[2025-01-01, 2026-01-01)` |

A non-existent period (`'2025-13'`, `'2025-02-29'`, `'2021-W53'`) or any other string compared with a temporal operand is a typed error listing these forms.

### Relative tokens

Relative tokens are periods computed from "now", matched case-insensitively. `<unit>` is any granularity (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`), plural `s` optional.

| Token | With now = Tue 2026-09-29 12:00 |
|---|---|
| `today`, `yesterday`, `tomorrow` | `today` = `[2026-09-29, 2026-09-30)` |
| `this <unit>`, `last <unit>`, `next <unit>` | `last month` = `[2026-08-01, 2026-09-01)` |
| `last N <units>`, `next N <units>` — N whole units, **excluding the current one** | `last 7 days` = `[2026-09-22, 2026-09-29)` |
| `N <units> ago`, `N <units> from now` — the single unit N steps away | `3 months ago` = `[2026-06-01, 2026-07-01)` |
| `week to date`, `month to date`, `quarter to date`, `year to date` | `year to date` = `[2026-01-01, 2026-09-30)` |

"Now" is the SLayer host's local wall-clock time, read once per execution, so every stage of a multi-stage query sees the same instant; a new day's bounds are new SQL, so a cached result never crosses days. SQL-side `now()` / `current_date()` read the *database* clock instead. Setting `SLAYER_NOW` to a naive ISO-8601 datetime or date (`2025-07-15T12:00:00`, `2025-07-15` = midnight) pins "now" for every engine built without an explicit `clock=`; a timezone-aware or unparseable value fails at startup.

### Comparisons

A comparison of a temporal operand `x` with a period `P` reads the period as a whole:

| Filter | Means |
|---|---|
| `x >= P` / `x > P` | `x >= start(P)` / `x >= next_start(P)` |
| `x < P` / `x <= P` | `x < start(P)` / `x < next_start(P)` |
| `x = P`, `x in 'P'` | `start(P) <= x < next_start(P)` |
| `x != P`, `x not in 'P'` | outside `[start(P), next_start(P))` |

So `ordered_at <= '2024-12'` includes Dec 31 10:00, and `ordered_at = '2024-06'` is June. A literal on the left mirrors the comparison; an instant keeps a plain comparison. `in` with a tuple (`code in ('2025-Q1',)`) keeps its value-list meaning.

A date-only string in a comparison is its midnight, as in SQL: `ordered_at <= '2024-12-31'` stops at `2024-12-31 00:00:00`. For the whole day write `ordered_at in '2024-12-31'`, or `ordered_at < '2025-01-01'` as an upper bound. Where this differs from the whole day (`<=`, `>`, `=`, `!=` or `BETWEEN` against a TIMESTAMP), the response carries a `date_only_midnight` warning naming that spelling; it is dropped in SLayer 3.0.

A temporal operand is anything typed DATE or TIMESTAMP: a declared column (base, derived, joined or a stage column), `min` / `max` / `first` / `last` of one, a date function or `interval` arithmetic, the clock, a `CAST(… AS DATE)` / `CAST(… AS TIMESTAMP)`, a `DATE '…'` / `TIMESTAMP '…'` literal, or a `coalesce` / `iif` over those (ISO literals inside count as dates). A plain date string against a non-temporal column keeps its string meaning; a relative token or single-string `in` there is a type error naming the column — declare its `type`. A DATE operand rejects sub-day points (`'last 6 hours'`, `'2025-01-01 10:00:00'`).

## Time buckets

`date_trunc('<unit>', expr)` — `date_trunc('month', created_at)`, `date_trunc('week', ts)`, `date_trunc('fiscal_year', CAST(ts AS DATE))`, … — is the time bucket of the DATE / TIMESTAMP row-level expression `expr` for a built-in unit or a datasource granularity (any case), usable anywhere a row-level expression is: filters, aggregation sources (`count_distinct(date_trunc('month', created_at))`), computed dimensions and arithmetic. Compared with a time point it filters `expr` exactly: `date_trunc('month', created_at) >= '2024-03-15'` means `created_at >= 2024-04-01`.

`year(x)` … `second(x)` are integer extraction (`month(created_at)` is the month of year, 1–12), and comparing one with a date (`month(created_at) = '2025-03'`) is an error naming the `date_trunc` form.

## `date_range`

A time dimension's `date_range` is one time point, or a `[lower, upper]` pair where either bound may be `null` (open):

```json
{"dimension": "created_at", "granularity": "month", "date_range": "last 3 months"}
{"dimension": "created_at", "granularity": "month", "date_range": ["2025-01-15", "2025-03"]}
{"dimension": "created_at", "granularity": "month", "date_range": ["2024-01-01", null]}
```

A pair means `x >= lower` and `x <= upper` with each date-only bound read as its whole day, so `["2024-01-01", "2024-12-31"]` covers all of Dec 31; write an instant (`'2024-12-31 00:00:00'`) for a midnight-inclusive end. `[]`, three or more elements, `[null, null]` and unparseable bounds are rejected when the query is built; a relative unit the datasource does not define (`last fortnight`) fails when the query is planned.

## Time bounds do not clip the window

A trailing window has to read rows from *before* the earliest bucket you asked for — otherwise that bucket silently under-counts. So a **time bound narrows which buckets come back, not which rows the window may reach**. These return identical numbers:

```json
{"time_dimensions": [{"dimension": "created_at", "granularity": "month", "date_range": "2025"}]}
```
```json
{"time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
 "filters": ["created_at in '2025'"]}
```

A bound is a *frame* bound when it bounds a **time dimension's own expression** (a column or any temporal row-level expression) by a time point — `<`, `<=`, `>`, `>=`, a period `=` / `in`, a `date_range`, or `date_trunc('<unit>', expr)` against a point. Everything else restricts the window's input like any row filter:

- an instant `=` (a date-only `=` included), and `!=` / `not in` against a period;
- a bound on a temporal expression that is not one of the query's time dimensions, or against another column;
- a bound wrapped in `or` or `not`;
- `filters` declared on the **model**.

Mixed filters are split: `"created_at >= '2025-01-01' and status = 'paid'"` restricts the window's input to paid rows while still reaching back before January. The same holds for [`time_shift`](formulas.md#transform-functions). To clip the underlying rows, apply the bound in an inner stage of a multi-stage query.

## `whole_periods_only`

With `whole_periods_only: true`, every frame bound on a time dimension's expression snaps down to a bucket boundary, and the upper bound is clamped to now (added when absent), so each returned bucket is complete and the current one is left out. A monthly query with `date_range: ["2025-01-15", "2025-03-10"]` returns January and February; with no range it returns every month before the current one. With several granularities on one expression the earliest boundary wins; a pair that does not nest (`week` with `month`) returns a `whole_periods_non_nesting` warning.

## Time spine

Every datasource has a built-in, never-stored model `time_spine` with one column, `timestamp`; each model's **axis** — its `default_time_dimension`, else its only DATE / TIMESTAMP column — joins it many-to-one, so a time dimension on `time_spine.timestamp` returns every bucket in range, gaps included, with each fact attributed through its own axis:

```json
{"dimensions": ["customers.region"],
 "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": "month", "date_range": ["2025-01-01", "2025-06-30"]}],
 "measures": [{"formula": "sum(orders.amount)"}, {"formula": "sum(returns.amount)"}]}
```

The population is `time_spine × P` (P the population of the remaining dimensions, or `source_model`); the spine needs a lower frame bound (the upper defaults to the current bucket), a filter on it must be a frame bound, it has no countable rows (no aggregation over it, no raw-rows query), a model reaches it by its nearest axis over to-one hops (equal-length routes are a typed error), and wrap a measure in `coalesce(..., 0)` to fill empty buckets.

A stage of a multi-stage query, or a saved query, can be a spine query too; later stages then see one row per bucket.

A query reaches the spine in any case (`TIME_SPINE.timestamp`) unless quoted, `time_spine` in any case is reserved as a model name, and a model's `joins` never target it, since it already joins each model through its axis.

An expression is never an axis: a time dimension whose expression reads `time_spine.timestamp` (`time_spine.timestamp + INTERVAL '1 day'`) is a typed error that names the remedy of bucketing the column itself, while any other expression time dimension is an ordinary dimension of P.

## Custom granularities

A datasource's `granularities` list defines named buckets `{name, base, multiple, origin}` — e.g. `{"name": "fiscal_year", "base": "month", "multiple": 12, "origin": "2024-04-01"}` or `{"name": "quarter_hour", "base": "minute", "multiple": 15}` — usable wherever a built-in granularity is (time dimensions, `date_trunc('fiscal_year', order_date)`, `Column.granularity`, transform units), each bucket starting at `origin + k × multiple × base`.
