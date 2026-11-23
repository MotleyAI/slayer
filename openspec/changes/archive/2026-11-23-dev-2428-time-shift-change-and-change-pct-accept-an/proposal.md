## Why

Axiom 11.3 requires a time-ordered transform's axis — the query's active time bucket — to be in its operand grain. `time_shift`, `change` and `change_pct` accept an operand whose grain lacks it as a measure, filter or order key (returning a broadcast total shifted along an axis it does not vary on) and refuse it as an aggregation source, parameter or dimension, so legality depends on position (Axiom 13, Law 4). The cause is wider than the three ops: the axis check judges a transform by a different grain rule per position, so a windowed inner and a nested `first`/`last` are also judged differently by position.

## What Changes

- **BREAKING**: `time_shift`, `change` and `change_pct` over an operand whose grain lacks the active time bucket fail with `TimeAxisError` in every position, as `cumsum`, `lag`, `lead`, `first`, `last` and `consecutive_periods` already do as a measure.
- **BREAKING**: any time-ordered transform over a nested `first` / `last` transform (which drops the axis) fails with `TimeAxisError` as an aggregation source, aggregation parameter or in a dimension, as it already does as a measure, filter or order key; its suggestion names the axis-preserving remedy, not `partition_by=`.
- A time-ordered transform over a windowed inner is accepted as an aggregation source, aggregation parameter and in a dimension, as it already is as a measure (Axiom 11.1: a windowed inner carries the bucket).
- One axis rule for every position: every time-ordered transform node is judged at its operand grain; the per-position rules and the calendar-step exemption are removed.
- Spec scenarios and docs examples that lose the axis are rewritten to keep it; the 2.0 upgrade guide lists the change. Gates 2.0.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/type-errors`: the time-axis violation covers every time-ordered transform in every position, judged at the operand grain.
- `queries/partitioned-aggregates`: transforms over partitioned aggregates (local, cross-model, re-aggregation constituents) are subject to the time-axis rule in every position; axis-losing scenarios rewritten.
- `aggregations/functional-form`: the shifted partitioned ranked aggregation scenario keeps the time axis.

## Impact

- `slayer/engine/elaborate_env.py` (axis check), `slayer/engine/bind_inputs.py` (call site).
- Tests: a new every-position test file; aligned `tests/test_dev1835_guard_dissolution.py`, `tests/test_dev1868_transform_cm_exec.py`, `tests/test_dev1868_golden_sql.py` + `tests/golden/dev1868_sql_baseline.json`, `tests/test_dev2136_default_ranking_key.py`, `tests/_dev1871_raise_ledger.py`.
- Docs: `docs/concepts/expressions.md`, `docs/examples/07_aggregations/aggregations.md`, `docs/upgrading/2.0.md`.
- No API, dependency or architecture-model change.
