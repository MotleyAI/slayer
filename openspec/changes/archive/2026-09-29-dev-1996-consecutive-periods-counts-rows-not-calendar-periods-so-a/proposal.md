## Why

`consecutive_periods(p)` counts consecutive result rows, so a streak carries on
across time buckets with no data — while `time_shift` / `change` / `change_pct`
in the same query are calendar-aware and the live spec already describes the
result as a count of consecutive months. A probe also showed SQLite's time
offset drops the time of day at sub-day grains, so `time_shift` / `change` at
`hour` / `minute` / `second` on SQLite are always NULL — the same offset the
calendar-aware streak needs.

## What Changes

- `consecutive_periods` counts consecutive **calendar** buckets of the query's
  active time bucket: a bucket absent from its series is a gap that breaks the
  run, like a false predicate; runs stay per non-time grain combination.
- The series is the query's rows: the date range bounds it (no lookback), a
  row filter that empties a bucket makes a gap, a measure-typed filter never
  breaks a run; a NULL time bucket is adjacent to nothing.
- **BREAKING** (fail-closed): the undocumented, silently ignored `period=`
  keyword of `consecutive_periods` is rejected with the unsupported-keyword
  error.
- SQLite time offsets at `hour` / `minute` / `second` keep the time of day, so
  `time_shift` / `change` / `change_pct` at sub-day grains return the prior
  bucket's value on SQLite (as on DuckDB).
- One calendar-adjacency helper (the bucket *n* periods away) shared by the
  `time_shift` join-back and the `consecutive_periods` gap test.
- `architecture/semantics.arc42.md` Axiom 11.3 gains the calendar-adjacency
  clause (approved wording); `docs/concepts/formulas.md` gains one sentence.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/transforms`: `consecutive_periods` counts calendar periods and
  rejects `period=`; sub-day time offsets on SQLite.

## Impact

- `slayer/sql/generator.py` — `_emit_consecutive_periods_ctes_for_planned`
  (new predecessor CTE), `_emit_time_shift_ctes_for_planned` (uses the shared
  helper).
- `slayer/sql/dialects/sqlite.py` — `build_time_offset_expr`.
- `slayer/engine/binding.py` — `_TRANSFORM_KWARG_RULES`.
- Golden SQL for every `consecutive_periods` query is re-blessed.
- `architecture/semantics.arc42.md`, `docs/concepts/formulas.md`.
