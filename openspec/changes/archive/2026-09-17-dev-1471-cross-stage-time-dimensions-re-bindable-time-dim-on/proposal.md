# Proposal: cross-stage time dimensions — re-bindable time dimensions on downstream stages

## Why

A downstream stage of a multi-stage query has no time axis: the binder refuses every `TimeDimension` against a stage schema, so a stage cannot bucket an upstream temporal column, `time_shift` / `change` / `cumsum` and windowed measures fail there, and a downstream `date_range` is silently skipped. Under `architecture/semantics.arc42.md` Axiom 9 (closure) the refusal is a violation — the stage column carries its type, so bucketing it is well-typed — and a production agent hit it on 2026-09-17 when bucketing per-customer last-order dates by month.

## What Changes

- A `TimeDimension` on a downstream stage binds against any DATE / TIMESTAMP column of its upstream stage's schema — a bucketed column, a raw temporal column, an aggregate output (including the auto-named `partition_by` form), a multi-hop flat name — and takes part in the stage's grain like a model time dimension.
- Re-bucketing is typed: the same granularity is idempotent, a coarser granularity the upstream bucket nests into re-truncates, and a finer or non-nesting granularity is a typed checker error; a non-temporal or untyped stage column is a typed checker error. The upstream granularity rides the stage schema.
- `date_range` on a stage time dimension filters the stage's rows; the silent skip is gone.
- The stage-bound bucket is the active time axis, so the shift family, `cumsum`, `first` / `last`, `consecutive_periods` and windowed aggregates work on downstream stages; `main_time_dimension` disambiguates two stage time dimensions.
- The functional surfaces (`gran(col)` order keys, granularity-suffixed result keys) apply to stage time dimensions.
- The time-dimension column type rules move from the binder into the checker (engine principle 9), as typed errors with raise-ledger rows.
- SQLite stops emitting `CAST(... AS DATE / TIMESTAMP)` on declared-type casts (a dialect capability), so temporal `max` / `min` / derived-column values return full dates instead of the leading year — required for the cohort shape to execute on SQLite.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/time-dimensions`: ADDED — time dimensions bind on stage datasets; re-bucketing a stage column is typed; the functional surfaces apply to stage time dimensions.
- `queries/date-range`: ADDED — a `date_range` on a stage time dimension filters the stage's rows.
- `queries/transforms`: ADDED — the time axis on stage datasets.
- `aggregations/native-type-preservation`: ADDED — declared temporal casts are suppressed on dialects without native temporal storage.

## Impact

- `slayer/engine/binding.py` (stage arm of `bind_time_dimension`, no type checks), `slayer/ir/bound.py` (`BoundTimeDimension`), `slayer/engine/elaborate_env.py` (one checker rule), `slayer/core/errors.py` (typed error), `slayer/core/enums.py` (granularity nesting), `slayer/core/scope.py` (`StageColumn.granularity`), `slayer/engine/compile/stages.py` (stamp it), `slayer/engine/bind_inputs.py` (checker call, stage time axis, `date_range` on stages).
- `slayer/sql/dialects/base.py` + `sqlite.py` (declared-cast capability), `slayer/sql/generator.py` (one cast helper at the three seams).
- Tests: two direct-binder test files move to the new return shape; one pin test retired; two skips lifted; ledger +2 rows; SQLite goldens that pinned the destructive temporal cast re-blessed.
- Docs: `docs/concepts/queries.md`, `docs/concepts/formulas.md`, `docs/examples/06_multistage_queries/multistage_queries.md` (one sentence each). No arc42 edit; the guards baseline is unchanged.
- Deferred: the saved query-backed model's cached `Column` still carries no granularity (DEV-1929).
