# DEV-1928 design — one row-attach path for every constituent kind

## Context

See proposal.md — Why. Substrate facts verified on the branch (stacked on the DEV-1832
head `e61b8189`):

- Row-attach discovery (`compile/stages._plan_regroups`) inlines every
  `attached_inputs(root)` of an `is_row_attach_root` into `row_aggs` / `cm_row`, and each
  is attached into the row grain. A re-aggregation constituent's producer is coarser, so
  the DEV-1824 grain-cover assertion (`_assert_attach_covers_producer_grain`) fires:
  `sum(amount * min(X, partition_by=region))` reaches it today as a user-visible internal
  error, and `sum(amount * last(X))` is pre-empted by
  `elaborate_env.check_collapsing_transform_not_row_mixed`. A transform constituent
  survives only because its grain happens to be coverable.
- `_synthesize_reaggregation_producer` applies the combined-consumer rule
  (`check_reaggregation_partition_key_is_query_dim`) to every explicit outer partition key;
  the pure `sum(min(X, partition_by=region))` passes only because inside a carrier the key
  is a carrier grain key. Bind time is already lenient for attached operands
  (`bind_inputs._reagg_operand_keys`), and `assert_partition_key_attributable` has already
  run on every key.
- `_build_carrier_attach` builds join pairs from the synthesized producer's projected
  grain (`ordered_pks` matched to row slots) and asserts equality with
  `_producer_grain_slot_ids`.
- `effective_root_grain` / `constituent_grain` already put the active bucket in the grain
  of a transform constituent with a windowed inner, but the nested transform-root
  producer's prebound (`_regroup_producer_prebound`) never receives the query's time
  dimension, so `check_windowed_time_dimension` fires inside it (`stages.py:212`).
- `_assert_broadcast_coherence` (D5) inspects only `attach_phase == "combined"`
  substitutions; the DEV-1859 row attaches sit outside it by design.
- The guard ratchet counts `NotImplementedError` deferral sites only; the removed guard
  is a `ValueError`, so `guards.baseline` is untouched.

## Goals / Non-Goals

**Goals:** one row-attach path for every attached constituent kind — aggregate,
transform, re-aggregation, empty grain included — with the grain-cover assertion
satisfied by construction; the windowed inner grained by the query's bucket inside its
own nested producer; the cross-model grained inner recorded as the Axiom 7 boundary it is;
goldens byte-identical except named additions.

**Non-Goals:** a `time_dimension=` kwarg (dropped, no issue); cross-model temporal
association (a target-homed inner accumulating along a host axis); extending D5
coherence to row attaches; a positive inverse of the boundary that needs a target-level
time dimension (new fixture column plus a cross-model time dimension).

## Decisions

1. **Attached-constituent synthesis context** (Codex F1). A re-aggregation attached
   input of a row-attach root is synthesized through `_synthesize_reaggregation_producer`
   in a context whose projected dimensions ARE the constituent's own grain, so each
   partition key is attributable (checked at bind time) but need not be a query
   dimension — the compile-time mirror of `_reagg_operand_keys`. Top-level
   re-aggregations keep the combined-consumer rule. Alternative — special-case the
   collapse only — rejected: the hand-written form is the same class and reaches an
   internal assertion today.
2. **Join on the producer's projected grain** (Codex F2). The attach's join pairs are the
   synthesized producer's `ordered_pks` matched to its row slots — the
   `_build_carrier_attach` pattern — so bucket substitution and normalisation are honoured
   and `_assert_attach_covers_producer_grain` holds by the same invariant as every other
   producer path. An empty grain yields zero join pairs: a scalar producer cross-joined
   onto every row, never an empty `ON` (Codex F3).
3. **The row-attach cardinality invariant is the grain-cover assertion, not D5**
   (Codex F4). D5 governs measure-level combined-phase broadcasts; a row attach's
   coarse-to-row broadcast is a many-to-one join whose exactly-one-value-per-row property
   is the grain-cover assertion. The new route sits below D5; one test proves D5 still
   fires at the level above (a mixed re-aggregation root combined with a coarser
   explicit-grain measure). Alternative — extend D5 to row attaches — rejected: wrong
   layer, and the DEV-1859 row attaches are deliberately outside it.
4. **Thread the active time dimension into the nested producer** (Codex F5). The
   transform-root producer for a constituent with a windowed inner receives the query's
   active time dimension through `_regroup_producer_prebound`, so the bucket is in its
   GROUP BY, projection and join pairs and `check_windowed_time_dimension` resolves; the
   test asserts the exact bucket key appears in the producer's projected grain and join
   keys. Alternative — expose the bucket only as a row slot — rejected: a slot without
   grain membership gives wrong window semantics. If the probe shows the shape is
   undefined rather than unplumbed, stop and escalate — never a silent boundary.
5. **Permanent boundary, narrowly named.** The cross-model grained inner keeps the
   partition-key attributability error; its test moves to a class named for exactly this
   shape (a target-homed inner naming a host time key across a fanning hop), beside a
   positive pin that a to-one cross-model partition key on a local-homed inner stays
   legal. No arc42 edit: Axiom 7 already states the rule; the requirement text records it.
6. **Guard removal with no compiler-side replacement** (Codex F10). Delete
   `check_collapsing_transform_not_row_mixed`, its call and its ledger row; parity derives
   from the ledger. No new user-facing raise in the compiler (engine P9).
7. **Accounting.** Named golden cases are added (`lifted/mixed_reagg`,
   `lifted/mixed_collapse`, `lifted/windowed_inner`) so the baseline delta is their
   addition only; `guards.baseline` and `index.yaml` unchanged.

## Risks / Trade-offs

- [The attached-constituent context loosens a check] → only the query-dimension rule is
  skipped, only for a constituent of a row-attach root; attributability stays enforced at
  bind time; pinned by the hand-written and collapse executed cases.
- [An empty-grain producer renders an invalid join] → executed on both engines with a
  zero-join-pair structure pin.
- [Threading the time dimension changes other nested producers] → goldens byte-identical
  except the named additions; the full suite is the gate.
- [The mode axis is bypassed by the new route] → broadcast- and error-mode tests with the
  existing fanning-dimension fixture.
- [The windowed shape is undefined rather than unplumbed] → escalate, never pin a boundary.

## Migration Plan

Pure planner change; no stored artifacts. Rollback = revert the PR. Stacked on DEV-1832:
the PR targets its branch until it merges, then main; integrate forward by merging, never
rebasing; archive this change after DEV-1832's.
