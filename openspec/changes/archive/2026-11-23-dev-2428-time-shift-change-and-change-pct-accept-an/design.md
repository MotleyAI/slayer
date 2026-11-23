## Context

See proposal.md › Why. The axis check (`check_dimension_temporal_axis` in `slayer/engine/elaborate_env.py`, called from `slayer/engine/bind_inputs.py` after partition keys are rewritten to query buckets) walks each root with two arms:

- dimension roots and transforms inside aggregation sources / parameters (`_temporal_axis_transforms`) are judged at `regroup_root_grain` — the union of the outermost aggregates' explicit partitions, blind to a windowed inner's bucket, an ungrained inner, and a nested `first`/`last` dropping the axis;
- transforms outside every aggregation (`_consumer_transforms`) are judged at `transform_operand_grain` (Axioms 11.1, 11.3b, 11.5) but skip `time_shift`, `change`, `change_pct` (`_CALENDAR_STEP_TRANSFORMS`).

### Symptom, structural cause, fix

- Symptom: a calendar-step transform over a time-less operand is legal as a measure / filter / order key and refused as a source, parameter or dimension; plan-time probes also found `cumsum(sum(x, window='90d', partition_by=region))` legal only as a measure, and `time_shift(last(sum(x, partition_by=[region, t])), -1)` legal in every position.
- Cause: legality is judged by a position-chosen grain rule plus an op exemption, so it depends on position (Axiom 13, Law 4) instead of on the operand's type (Axioms 9, 11).
- Fix: one walker, one rule — every time-ordered transform node reachable from any root is judged at `transform_operand_grain`; the per-position arms and the exemption go.

Principles that apply: `architecture/semantics.arc42.md` Axioms 9, 11.1, 11.3 (a/b), 11.4, 11.5, 13 and Law 4 (position parity); `architecture/system.arc42.md` §3.10 (one typing); `architecture/python.engine.arc42.md` §3.1 (typed pipeline). No arc42 or model edit: Axiom 11.3 already states the rule. The spec delta modifies existing spec groups only (already mapped in the model); no import change.

## Goals / Non-Goals

**Goals:**

- One axis rule, judged at the operand grain, for every time-ordered transform node in every position.
- A remedy that can actually fix the query: `partition_by=` when an inner lacks the axis, the axis-preserving operand when a nested `first` / `last` dropped it.

**Non-Goals:**

- Any change to how calendar-step transforms evaluate an axis-keeping operand (re-aggregation and series regimes, frame rule, leaf grains).
- Any change to bare `first(x)` / `last(x)` aggregations at the query grain (`time_shift(last(amount), -1)` stays legal).
- arc42 / LikeC4 edits.

## Decisions

1. Follow Axiom 11.3 with no exception: `time_shift`, `change`, `change_pct` refuse an operand whose grain lacks the axis in every position, as the other time-ordered transforms do. Options: refuse everywhere vs. axiom exception for self-join transforms (the issue's "to settle"). Refuse: the owner's ruling. (user, issue ruling 2026-11-23)
2. Structural fix: one walker judges every time-ordered transform node at `transform_operand_grain` in every position; `_temporal_axis_transforms`, `_consumer_transforms`, `_CALENDAR_STEP_TRANSFORMS` and the `is_dimension` arm are deleted. Consequences: a time-ordered transform over a windowed inner is newly accepted as a source / parameter / dimension; one over a nested `first` / `last` is newly refused there. Options: (A) structural fix; (B) delete the exemption only. A: removes the bug class (Law 4) and matches the ruling's "all positions"; B leaves two position-dependent shapes standing and its only advantage is a smaller diff. Full unit-suite probe of A: only the three tests in decision 3 fail. (user)
3. Align the existing tests pinning refused shapes, each keeping its purpose with an axis-keeping twin: `tests/test_dev1835_guard_dissolution.py::TestChangeOverAttached` (refusal + `partition_by=[region, ordered_at]` with values); `tests/test_dev1868_transform_cm_exec.py::TestChangeOverCrossModelRanked` (refusal + `rank(LAST_REGION, direction='desc')` with values); delete golden case `w2/change_last_region` from `tests/test_dev1868_golden_sql.py` and its five entries in `tests/golden/dev1868_sql_baseline.json`; `tests/test_dev2136_default_ranking_key.py::TestLocalProducers::test_shifted_partition_ranks_by_time_dimension_source` → `time_shift(first(orders.amount, partition_by=[orders.region, orders.created_at]), 1)`, US 2024 = 30 (verified at plan time; `last` does not discriminate the ranking key on that fixture). (user)
4. Rename `check_dimension_temporal_axis` → `check_temporal_axis`: it covers every position. Mechanical updates at its call site and the raise-ledger row in `tests/_dev1871_raise_ledger.py`. (default)
5. `TimeAxisError` text is mechanism-neutral: the summary states that the operand grain does not contain the axis, so the operand does not vary along it (no "join back would duplicate rows"). (default)
6. Axis-losing spec scenarios are replaced by axis-keeping twins preserving each scenario's purpose (`first` with a +1 shift for the ranking-key scenario; `rank` for the cross-model ranked one); the stale "top-level measure transform keeps evaluating at the query grain" parenthetical in `queries/partitioned-aggregates` › Re-aggregation consumes attached operands as datasets points to the every-position rule. (default)
7. The new test file is named descriptively (`tests/test_time_axis_every_position.py`), with no issue key. (default)
8. No arc42 edit: Axiom 11.3 already states the rule. (default)
9. `queries/partitioned-aggregates` › Cross-model partitioned aggregates nest inside transforms gains the time-axis qualifier in its requirement text, not only in its ranked scenario. (codex)
10. Test coverage for active-axis resolution: a downstream stage's own time dimension, a spine bucket, two time dimensions with `main_time_dimension` (an inner partitioned only by the other time column is refused). (codex)
11. Test coverage for a raw time-column partition key rewritten to the active bucket before the check, at a built-in and a custom granularity. (codex)
12. When the axis is missing only because a nested `first` / `last` dropped it, the suggestion names applying the time-ordered transform to that `first` / `last`'s operand (or an axis-preserving operand) and does not offer `partition_by=`; otherwise the `partition_by=` remedy stays. Tests assert which suggestion each case carries. (codex)
13. Rejected (codex): "`time_shift(first(amount, partition_by=[region, created_at]), 1)` is not axis-keeping". `first(<row field>, …)` is the ranked aggregation (an aggregate at its partition grain, which contains the bucket), not the collapsing transform; the plan-time probe executed it under the unified walker.
14. Rejected (codex): "rewrite `time_shift(last(amount), -1)` / `time_shift(last(amount) / 2, -1)` scenarios as refusals". These bare `last` aggregations are at the query grain, which contains the axis; the unified-walker probe kept their tests green.

## Risks / Trade-offs

- [Spec scenario values for the windowed-inner source / dimension forms were not executed at plan time] → pr-tests verifies them by execution before asserting and raises any contradiction instead of re-specifying.
- [A saved query or stored metric using a refused shape now fails at query time] → breaking by design; the 2.0 upgrade guide names the remedy. The axis is query-dependent, so no save-time check applies (system §3.11 covers only definitions guaranteed to fail every query).
- [`transform_operand_grain` in a dimension root sees the query grain for an ungrained inner] → a dimension's ungrained inner is already refused earlier with `ComputedDimensionError` (probed), so the check never judges it there.
