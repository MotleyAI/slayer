# Proposal: dev-1751-pg-facade-one-sided-time-filter-emits-between-x-and-null

## Why

A one-sided time filter through the Postgres facade (`WHERE created_at >= '2024-01-01'`
with no upper bound) is lifted into a half-open `TimeDimension.date_range == ['2024-01-01', None]`,
which the planner renders as `BETWEEN '2024-01-01' AND NULL` — never true, so the query
silently returns zero rows. Silently wrong numbers are the worst failure class for a
semantic layer, and any BI tool issuing an open-ended date filter hits this.

## What Changes

- The facade stops lifting relational comparators (`>=`, `>`, `<=`, `<`) on time
  dimensions into `date_range`; they translate verbatim into `SlayerQuery.filters`,
  paired or not. Only a source-SQL `BETWEEN` (both bounds literal) lifts. The
  comparator lift predates DEV-1732's frame-bound generalization, which made the
  verbatim spelling semantically identical for window/time-shift CTEs; lifting now
  only loses information (strictness, one-sidedness). This also fixes the adjacent
  boundary bug where `>= a AND < b` lifted to an *inclusive* `BETWEEN a AND b`.
- The planner fails closed: a 2-element `date_range` containing a `None` bound raises
  `ValueError` (naming the time dimension and the received range) instead of emitting
  a `NULL` bound. The check runs before any scope-based skip, so non-`ModelScope`
  stages fail loudly too. The ratified wrong-length behavior (silent skip +
  `MALFORMED_DATE_RANGE` warning) is untouched.
- Docs: `docs/interfaces/flight-sql.md` row claiming comparators get the "same lift"
  as `BETWEEN` is corrected; `docs/concepts/queries.md` documents the two-bound
  `date_range` contract.

## Capabilities

### New Capabilities

- `facade/time-filter-translation`: how the SQL facade translates WHERE-clause time
  predicates into `SlayerQuery` — `BETWEEN` lifts to `date_range`, relational
  comparators pass through verbatim as filters.
- `queries/date-range`: the `TimeDimension.date_range` contract at planning time —
  two non-null string bounds render as an inclusive range filter; a `None` bound is
  a hard error; wrong-length ranges warn and no-op.

### Modified Capabilities

None.

## Impact

- `slayer/facade/translator.py`: `_lift_time_comparator` deleted; `_classify_where_conjunct`
  and `_apply_where` simplified.
- `slayer/engine/stage_planner.py`: None-bound validation in the `date_range` filter loop
  (before the `ModelScope` check) / `_build_date_range_filter`.
- Tests: two facade translator tests rewritten (consented), new translator/planner/execution
  coverage.
- Docs: `docs/interfaces/flight-sql.md`, `docs/concepts/queries.md`.
