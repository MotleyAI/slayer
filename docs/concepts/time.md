# Time

The single reference for how SLayer reads time: time points, `date_range`, time buckets, frame bounds, `whole_periods_only`, the time spine, custom granularities and time zones. Date *functions* (`date_part`, `date_diff`, `date_add`, `now()`, `current_date()`) are in [References](references.md#date-and-time-functions).

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

"Now" is the SLayer host's local wall-clock time, read once per execution, so every stage of a multi-stage query sees the same instant; a new day's bounds are new SQL, so a cached result never crosses days. SQL-side `now()` / `current_date()` read the *database* clock instead. Under a [time zone](#time-zones), both are that one reading taken in the zone. Setting `SLAYER_NOW` to a naive ISO-8601 datetime or date (`2025-07-15T12:00:00`, `2025-07-15` = midnight) pins "now" for every engine built without an explicit `clock=`; a timezone-aware or unparseable value fails at startup.

### Comparisons

A comparison of a temporal operand `x` with a period `P` reads the period as a whole:

| Filter | Means |
|---|---|
| `x >= P` / `x > P` | `x >= start(P)` / `x >= next_start(P)` |
| `x < P` / `x <= P` | `x < start(P)` / `x < next_start(P)` |
| `x = P`, `x in 'P'` | `start(P) <= x < next_start(P)` |
| `x != P`, `x not in 'P'` | outside `[start(P), next_start(P))` |

So `orders.ordered_at <= '2024-12'` includes Dec 31 10:00, and `orders.ordered_at = '2024-06'` is June. A literal on the left mirrors the comparison; an instant keeps a plain comparison. `in` with a tuple (`orders.code in ('2025-Q1',)`) keeps its value-list meaning.

A date-only string in a comparison is its midnight, as in SQL: `orders.ordered_at <= '2024-12-31'` stops at `2024-12-31 00:00:00`. For the whole day write `orders.ordered_at in '2024-12-31'`, or `orders.ordered_at < '2025-01-01'` as an upper bound. Where this differs from the whole day (`<=`, `>`, `=`, `!=` or `BETWEEN` against a TIMESTAMP), the response carries a `date_only_midnight` warning naming that spelling; it is dropped in SLayer 3.0.

A temporal operand is anything typed DATE or TIMESTAMP: a declared field (base, derived, joined or a stage column), `min` / `max` / `first` / `last` of one, a date function or `interval` arithmetic, the clock, a `date_trunc` bucket in any position, a `CAST(… AS DATE)` / `CAST(… AS TIMESTAMP)`, a `DATE '…'` / `TIMESTAMP '…'` literal, or a `coalesce` / `iif` over those (ISO literals inside count as dates). A plain date string against a non-temporal field keeps its string meaning; a relative token or single-string `in` there is a type error naming the field — declare its `datatype`. A DATE operand rejects sub-day points (`'last 6 hours'`, `'2025-01-01 10:00:00'`).

## Time buckets

`date_trunc('<unit>', expr)` — `date_trunc('month', orders.created_at)`, `date_trunc('week', ts)`, `date_trunc('fiscal_year', CAST(ts AS DATE))`, … — is the time bucket of the DATE / TIMESTAMP row-level expression `expr` for a built-in unit or a datasource granularity (any case), usable anywhere a row-level expression is: filters, aggregation sources (`count_distinct(date_trunc('month', orders.created_at))`), computed dimensions and arithmetic. Compared with a time point it filters `expr` exactly: `date_trunc('month', orders.created_at) >= '2024-03-15'` means `orders.created_at >= 2024-04-01`.

A bucket is a DATE when `expr` is a DATE and the unit (a custom granularity's base) is `day` or coarser, and a TIMESTAMP otherwise, so `date_trunc('hour', orders.order_date)` is the TIMESTAMP at that day's midnight; every backend returns exactly that type, as a date or a naive timestamp (on SQLite `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` text).

Wherever a DATE meets a TIMESTAMP — a comparison, `in`, `IS [NOT] DISTINCT FROM`, a TIMESTAMP-typed `coalesce` / `ifnull` / `iif` / `CASE` / `greatest` / `least`, the comparison inside `nullif` (which keeps its first argument's type), and the time spine's join to a DATE axis — the DATE reads as its midnight instant, and on SQLite two temporal values compare equal whatever ISO spelling they are stored in.

`year(x)` … `second(x)` are integer extraction (`month(orders.created_at)` is the month of year, 1–12), and comparing one with a date (`month(orders.created_at) = '2025-03'`) is an error naming the `date_trunc` form.

## `date_range`

A time dimension's `date_range` is one time point, or a `[lower, upper]` pair where either bound may be `null` (open):

```json
{"field": "orders.created_at", "granularity": "month", "date_range": "last 3 months"}
{"field": "orders.created_at", "granularity": "month", "date_range": ["2025-01-15", "2025-03"]}
{"field": "orders.created_at", "granularity": "month", "date_range": ["2024-01-01", null]}
```

A pair means `x >= lower` and `x <= upper` with each date-only bound read as its whole day, so `["2024-01-01", "2024-12-31"]` covers all of Dec 31; write an instant (`'2024-12-31 00:00:00'`) for a midnight-inclusive end. `[]`, three or more elements, `[null, null]` and unparseable bounds are rejected when the query is built; a relative unit the datasource does not define (`last fortnight`) fails when the query is planned.

## Time bounds do not clip the window

A trailing window has to read rows from *before* the earliest bucket you asked for — otherwise that bucket silently under-counts. So a **time bound narrows which buckets come back, not which rows the window may reach**. These return identical numbers:

```json
{"time_dimensions": [{"field": "orders.created_at", "granularity": "month", "date_range": "2025"}]}
```
```json
{"time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
 "filter": ["orders.created_at in '2025'"]}
```

A bound is a *frame* bound when it bounds a **time dimension's own expression** (a field or any temporal row-level expression) by a time point — `<`, `<=`, `>`, `>=`, a period `=` / `in`, a `date_range`, or `date_trunc('<unit>', expr)` against a point. Everything else restricts the window's input like any row filter:

- an instant `=` (a date-only `=` included), and `!=` / `not in` against a period;
- a bound on a temporal expression that is not one of the query's time dimensions, or against another field;
- a bound wrapped in `or` or `not`;
- `filters` declared on the **dataset**.

Mixed filters are split: `"orders.created_at >= '2025-01-01' and orders.status = 'paid'"` restricts the window's input to paid rows while still reaching back before January. The same holds for [`time_shift`](expressions.md#transform-functions). To clip the underlying rows, apply the bound in an inner stage of a multi-stage query.

## `whole_periods_only`

With `whole_periods_only: true`, every frame bound on a time dimension's expression snaps down to a bucket boundary, and the upper bound is clamped to now (added when absent), so each returned bucket is complete and the current one is left out. A monthly query with `date_range: ["2025-01-15", "2025-03-10"]` returns January and February; with no range it returns every month before the current one. With several granularities on one expression the earliest boundary wins; a pair that does not nest (`week` with `month`) returns a `whole_periods_non_nesting` warning.

## Time spine

Every datasource has a built-in, never-stored dataset `time_spine` with one field, `timestamp`; each dataset's **axis** — its `default_time_dimension`, else its only DATE / TIMESTAMP field — joins it many-to-one, so a time dimension on `time_spine.timestamp` returns every bucket in range, gaps included, with each fact attributed through its own axis:

```json
{"dimensions": ["customers.region"],
 "time_dimensions": [{"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2025-01-01", "2025-06-30"]}],
 "metrics": ["sum(orders.amount)", "sum(returns.amount)"]}
```

The population is `time_spine × P`; the spine needs a lower frame bound (the upper defaults to the current bucket), a filter on it must be a frame bound, it has no countable rows (no aggregation over it, no raw-rows query), and wrap a metric in `coalesce(..., 0)` to fill empty buckets.

Without `population`, the rows are every bucket in range × the union of each measure's tuples of the other dimensions that have rows inside the frame — the dimension anchor's in-frame tuples with no measures, the buckets alone with no other dimensions. The spine bounds restrict every dataset like a field filter, so a group whose facts all lie outside the range is no row; name `population` (`"population": "customers"`) to keep every group in every bucket. A trailing window or `time_shift` still reads rows before the lower bound.

A dataset reaches the spine by its nearest axis over to-one hops; one with none reaches it by association across its nearest **fanning axis** — `count(customers.*)` by spine month counts the customers with an order that month (the `associated` warning). Equal-length routes, or two fanning axes at the same distance, are a typed error naming each route (`customers.orders.order_date`, `customers.returns.return_date`); the remedy is a time dimension on one of those fields instead of the spine. A dataset with no route to any axis meets every bucket.

A stage of a multi-stage query, or a saved query, can be a spine query too; later stages then see one row per bucket.

A query reaches the spine in any case (`TIME_SPINE.timestamp`) unless quoted, `time_spine` in any case is reserved as a dataset name, and a dataset's `relationships` never target it, since it already joins each dataset through its axis.

An expression is never an axis: a time dimension whose expression reads `time_spine.timestamp` (`time_spine.timestamp + INTERVAL '1 day'`) is a typed error that names the remedy of bucketing the field itself, while any other expression time dimension is an ordinary dimension of P.

## Custom granularities

A datasource's `granularities` list defines named buckets `{name, base, multiple, origin}` — e.g. `{"name": "fiscal_year", "base": "month", "multiple": 12, "origin": "2024-04-01"}` or `{"name": "quarter_hour", "base": "minute", "multiple": 15}` — usable wherever a built-in granularity is (time dimensions, `date_trunc('fiscal_year', orders.order_date)`, `DatasetField.granularity`, transform units), each bucket starting at `origin + k × multiple × base`. A granularity renamed because the library reserved its name carries `renamed_from` with the former name; a save keeps it.

## Time zones

A query's `time_zone` (an IANA name such as `"America/New_York"`) buckets, filters and returns timestamps as that zone's wall clock:

<!-- doc-check: not-a-query -->
```json
{"population": "orders", "time_zone": "America/New_York",
 "time_dimensions": [{"field": "orders.ordered_at", "granularity": "day"}],
 "metrics": ["sum(orders.amount)"]}
```

Every read of a dataset timestamp is converted once, where it is read, so every position sees the converted value: buckets, custom granularities (their origin is read in the zone), filters, `date_range`, relative tokens, `whole_periods_only`, date functions, windows, `time_shift`, aggregates, join keys, raw rows and the time spine. A query without `time_zone` is unchanged, whatever source zones its datasets declare.

- **Source zone.** A `DateTimeTz` field holds instants and has no source zone. A `DateTime` field's source zone is its own `time_zone`, else its datasource's `time_zone`; `time_zone: "civil"` marks zone-free civil time, never converted. A field `time_zone` is accepted only on a `DateTime` field reading a plain column. Under a zone, a `DateTime` field with no source zone is read as stored with a `time_zone_undeclared` warning; so is a source column no field maps.
- **Definitions and predicates.** Derived fields and dataset metrics (their `FILTER (WHERE …)` included) read converted values, so each means what its expression written in the query means; a dialect entry sees converted values too. A field's `filter`, a dataset's `filters` and row-level security read stored values, so a dataset's rows never depend on the zone.
- **Stages.** A stage without its own `time_zone` takes the final stage's, and so do the stored stages of the query-backed datasets it reads. A stage's timestamp output carries its zone; a stage in another zone converts it, a repeated local hour reading as its later occurrence. Re-bucketing a field already bucketed in another zone is a typed error. A query-backed dataset whose final stage pins a zone stamps it on its fields.
- **Now.** One clock reading per execution, taken in each stage's zone, drives relative tokens, `whole_periods_only`, the spine's upper bound and `now()` / `current_date()`, which render as literals; `now()` still bypasses the result cache, a zoned `current_date()` caches per day.
- **Results.** A `DateTimeTz` value comes back as the zone's naive wall clock, and the response's `time_zone` reports the zone. Elapsed-time arithmetic across a DST transition is off by the shift.
- **DST.** A skipped wall-clock time takes the offset before the transition and a repeated one its later occurrence (Postgres, DuckDB and SQLite); BigQuery, Snowflake and SQL Server apply their own rule.
- **Pruning.** A filter comparing a converted column with a time point also compares the stored column with the point widened by two days, so indexes and partitions still prune; never on a frame bound.
- **Databases.** Every Tier-1 database converts: SQL Server maps the zone to its Windows name (a zone with none is a typed error) and MySQL needs its time zone tables loaded, checked once per zone before the query runs. A Tier-2 database refuses a query that needs a conversion.
