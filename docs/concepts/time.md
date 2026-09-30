# Time

The single reference for how SLayer reads time: time points, `date_range`, granularity calls, frame bounds and `whole_periods_only`. Date *functions* (`date_part`, `date_diff`, `date_add`, `now()`, `current_date()`) are in [References](references.md#date-and-time-functions).

## Time points

A string compared with a temporal expression — or used as a `date_range` bound — is a **time point**: either an **instant** or a **period** `[start, next_start)`.

| Form | Example | Meaning |
|---|---|---|
| Instant | `'2025-03-01 10:00:00'`, `'2025-03-01T10:00'`, `'2025-03-01 10:00:00.250'` | That moment (up to six fractional digits; no zone offset) |
| Day | `'2025-03-15'` | `[2025-03-15, 2025-03-16)` — a date-only string is the whole day |
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

"Now" is the SLayer host's local wall-clock time, read once per execution, so every stage of a multi-stage query sees the same instant; a new day's bounds are new SQL, so a cached result never crosses days. SQL-side `now()` / `current_date()` read the *database* clock instead.

### Comparisons

A comparison of a temporal operand `x` with a period `P` reads the period as a whole:

| Filter | Means |
|---|---|
| `x >= P` / `x > P` | `x >= start(P)` / `x >= next_start(P)` |
| `x < P` / `x <= P` | `x < start(P)` / `x < next_start(P)` |
| `x = P`, `x in 'P'` | `start(P) <= x < next_start(P)` |
| `x != P`, `x not in 'P'` | outside `[start(P), next_start(P))` |

So `ordered_at <= '2024-12-31'` includes Dec 31 10:00, and `ordered_at = '2024-06'` is June. A literal on the left mirrors the comparison; an instant keeps a plain comparison. `in` with a tuple (`code in ('2025-Q1',)`) keeps its value-list meaning.

A temporal operand is anything typed DATE or TIMESTAMP: a declared column (base, derived, joined or a stage column), `min` / `max` / `first` / `last` of one, a date function or `interval` arithmetic, the clock, or a `coalesce` / `iif` over those (ISO literals inside count as dates). A plain date string against a non-temporal column keeps its string meaning; a relative token or single-string `in` there is a type error naming the column — declare its `type`. A DATE operand rejects sub-day points (`'last 6 hours'`, `'2025-01-01 10:00:00'`).

## Granularity calls

`gran(col)` — `month(created_at)`, `week(ts)`, … — is the time bucket of `col`, usable anywhere a row-level expression is: filters, aggregation sources (`count_distinct(month(created_at))`), computed dimensions and arithmetic. Compared with a time point it filters `col` exactly: `month(created_at) >= '2024-03-15'` means `created_at >= 2024-04-01`.

## `date_range`

A time dimension's `date_range` is one time point, or a `[lower, upper]` pair where either bound may be `null` (open):

```json
{"dimension": "created_at", "granularity": "month", "date_range": "last 3 months"}
{"dimension": "created_at", "granularity": "month", "date_range": ["2025-01-15", "2025-03"]}
{"dimension": "created_at", "granularity": "month", "date_range": ["2024-01-01", null]}
```

A pair means `x >= lower` and `x <= upper` in the comparison semantics above, so a date-only upper bound covers its whole day; write an instant (`'2024-12-31 00:00:00'`) for a midnight-inclusive end. `[]`, three or more elements, `[null, null]` and unparseable bounds are rejected when the query is built.

## Time bounds do not clip the window

A trailing window has to read rows from *before* the earliest bucket you asked for — otherwise that bucket silently under-counts. So a **time bound narrows which buckets come back, not which rows the window may reach**. These return identical numbers:

```json
{"time_dimensions": [{"dimension": "created_at", "granularity": "month", "date_range": "2025"}]}
```
```json
{"time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
 "filters": ["created_at in '2025'"]}
```

A bound is a *frame* bound when it bounds a **time dimension's own column** by a time point — `<`, `<=`, `>`, `>=`, a period `=` / `in`, a `date_range`, or `gran(col)` against a point. Everything else restricts the window's input like any row filter:

- an instant `=`, and `!=` / `not in` against a period;
- a bound on a time column that is not one of the query's time dimensions, or against another column;
- a bound wrapped in `or` or `not`;
- `filters` declared on the **model**.

Mixed filters are split: `"created_at >= '2025-01-01' and status = 'paid'"` restricts the window's input to paid rows while still reaching back before January. The same holds for [`time_shift`](formulas.md#transform-functions). To clip the underlying rows, apply the bound in an inner stage of a multi-stage query.

## `whole_periods_only`

With `whole_periods_only: true`, every frame bound on a time dimension's column snaps down to a bucket boundary, and the upper bound is clamped to now (added when absent), so each returned bucket is complete and the current one is left out. A monthly query with `date_range: ["2025-01-15", "2025-03-10"]` returns January and February; with no range it returns every month before the current one. With several granularities on one column the earliest boundary wins; a pair that does not nest (`week` with `month`) returns a `whole_periods_non_nesting` warning.
