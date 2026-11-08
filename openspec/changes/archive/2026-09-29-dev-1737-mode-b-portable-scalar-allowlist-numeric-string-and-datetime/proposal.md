## Why

Mode B has no date or time functions, so extracting a weekday, measuring the days
between two timestamps, or offsetting a date needs dialect-specific `Column.sql`.
sqlglot's generic date nodes are not portable (SQLite has no `YEAR()`, Postgres no
`DAY_OF_WEEK()`, month differences mean different things per engine), and SQLite's
date arithmetic is already wrong today: hourly `time_shift` returns all-NULL and
month offsets overflow past month-end. The numeric/string half of DEV-1737 landed in
DEV-1753; this change delivers the calendar half.

## What Changes

- New Mode-B scalars `date_part(part, ts)`, `date_diff(unit, start, end)`,
  `date_add(ts, n, unit)`, `current_date()`, `now()`, plus the operator spelling
  `ts ± interval(n, unit)` that the parser rewrites to `date_add`.
- One unit vocabulary shared with `time_shift` / time dimensions (`second` … `year`,
  `week_sunday`); extraction parts add `day_of_week` (ISO Mon=1…Sun=7), `day_of_year`,
  ISO `week`, `iso_year`, `day` (of month).
- Pinned semantics: `date_diff` counts calendar boundaries crossed; `date_add` clamps
  to month-end; non-literal counts truncate toward zero.
- Typed operands: date functions accept only DATE/TIMESTAMP-typed operands (declared
  columns, `min`/`max`/`first`/`last`, date functions, clock calls, ISO literals,
  conditionals over those) and reject anything else with a `QueryTypeError`; results
  carry INT / DATE / TIMESTAMP response types.
- ISO-8601 string literals in date positions bind as typed date/timestamp literals.
- Queries using `now()` / `current_date()` bypass the per-engine result cache.
- One date-arithmetic primitive per dialect serves `date_add`, `time_shift`
  join-backs, Sunday-week truncation and trailing-window frames — fixing SQLite's
  sub-day `time_shift` and month-end overflow in `time_shift` and trailing windows.
- One canonical scalar spec table; the typed allowlist, arity table and the legacy
  formula parser's set derive from it (OSI's verbatim carry-over uses a derived
  `sql_passthrough` subset that excludes the date functions).
- The new names become reserved: a custom aggregation named `date_part`,
  `date_diff`, `date_add`, `interval`, `current_date` or `now` is rejected at model
  validation.

## Capabilities

### New Capabilities
- `queries/date-functions`: the Mode-B date/time function surface — syntax, units,
  semantics, operand typing, literals, the `interval` spelling, per-dialect
  portability, and clock-function cache behaviour.

### Modified Capabilities
- `queries/transforms`: `time_shift` offsets are exact on every dialect (sub-day units
  keep the time of day; month offsets clamp at month-end).
- `aggregations/trailing-window`: month-denominated frame bounds clamp at month-end on
  every dialect.

## Impact

- `slayer/core/keys.py` (scalar spec table, `temporal_type`), `slayer/core/enums.py`
  (`DatePart`), `slayer/core/formula.py`, `slayer/osi/expression.py`.
- `slayer/engine/syntax.py` (unit literals, `interval` rewrite), `binding.py`,
  `elaborate_env.py` (typing / errors), `key_metadata.py` (result types),
  `query_engine.py` / `cache.py` (clock bypass).
- `slayer/sql/render/row_expr.py`, `value_expr.py`, `slayer/sql/dialects/*` (new typed
  hooks; `build_time_offset_expr` and `duration_interval_exprs` removed; SQLite UDF),
  `slayer/sql/generator.py` call sites.
- Generated SQL changes for `time_shift`, Sunday-week buckets and trailing windows on
  several dialects (byte-equivalence goldens re-blessed).
- `architecture/system.arc42.md` §3.10 wording; docs `docs/concepts/references.md`,
  `queries.md`, caching docs, `.claude/skills/slayer-query.md`.
