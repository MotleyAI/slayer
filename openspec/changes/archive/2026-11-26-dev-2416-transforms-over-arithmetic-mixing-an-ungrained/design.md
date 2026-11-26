## Context

See proposal.md for motivation. The planner computes each nested layer (a transform's
operand, a second-order aggregation's operand) as a sub-query grouped at that layer's
grain. Inside a sub-query an `AggregateKey` with `partition_keys=None` is read at the
sub-query's grouping, while the axioms give it the query grain.

Structural cause: "no `partition_by=` means the query grain" has three implementations in
`slayer/core/keys.py` that must agree by hand.

- The checker's `transform_operand_grain` — correct.
- The planner's `effective_root_grain` → `regroup_root_grain` → `_inner_partition_grain` —
  unions explicit keys only (the reported bug).
- Two bind-time rewrites, `_grain_transform_inner_aggregates` and `_grained_at`, that write
  the query grain onto same-dataset unwindowed inners only, and only in two positions.

A second cause sits under the rewrites: `partition_keys` carries two facts, the grain and
whether the user wrote it. The mode gate derives the second from the first
(`slayer/engine/compile/stages.py`, `explicit` in `_synthesize_cross_dataset_producer`;
`_check_param_partition_keys`), so a rewrite turns an unwritten grain into a written one.

Applicable principles: semantics Axioms 2.3, 2.4, 2.7, 5, 6, 8, 9, 10, 11.1, 11.4, 11.5,
11.6 and Laws 1, 3, 4, 6; system P10 (one typing); engine P1.

## Goals / Non-Goals

**Goals:**
- One definition of a transform's operand grain, read by checker and planner.
- One rule, by position and not by kind of aggregate, for where a nested aggregate with no
  `partition_by=` is given the query grain.
- A grain given by that rule stays unwritten for modes, warnings and names.
- Every query that works today keeps its values, warnings and errors.

**Non-Goals:**
- A transform over a coarser operand inside an aggregation ranking one cell per query
  cell (`max(rank(sum(orders.amount, partition_by=orders.region), direction='desc'))`
  reads North 3 for 2). Different cause; DEV-2494.
- `_shifted_producer_grain` keeps its own union (decision 9).
- The refusals listed in DEV-2427 (not reproduced on the Appendix B seed; see its comment).
- Dimension position: an aggregate with no `partition_by=` there stays refused.
- Carrying the resolved grain on every aggregate key, top level included (about 60 readers
  of `partition_keys is None`).

## Decisions

1. **The planner reads the checker's grain.** `effective_root_grain`, for a
   `TransformKey`, returns `transform_operand_grain(agg.input, query_grain=<projected
   dimensions and time dimensions>, active_bucket=…)`. `_inner_partition_grain`, the
   transform arm of `regroup_root_grain`, and the "no grained inner" and "windowed inner
   gains the bucket" special cases of `effective_root_grain` are deleted. Alternative:
   patch `_inner_partition_grain` to default to the query grain — a third copy of the rule.
   (user)

2. **One pass gives a defaulted grain to nested aggregates** — (user, approved wording):

   > 2. **One pass gives a defaulted grain to nested aggregates.** An aggregate with no `partition_by` takes the query grain, marked as defaulted, when it sits under a transform inside an aggregation's source or parameters, under a transform whose operand grain differs from the query grain, or in the operand of a `first` / `last` over an aggregate — at any depth and for every kind of aggregate. A defaulted grain resolves per the mode axis and warns exactly as an unwritten one. The two existing helpers are removed.

   Options weighed, all measured:
   - "No skip" (the two helpers stop skipping windowed and cross-dataset inners; first
     approved): fixes the four finer-operand shapes, but a working cross-dataset query
     becomes a `PartitionKeyError` under `broadcast` and names a `partition_by=` nobody
     wrote under `error`.
   - Uniform conditional (rewrite only when the operand grain differs from the query
     grain): 6 of 1934 related tests fail, four of them on values.
   - Split rule (same-dataset unwindowed always, others only when the operand is finer):
     no regression, but the boundary is where tests pass, not a semantic one; it loses the
     `associated` warning in one shape and leaves
     `rank(max(sum(orders.amount), partition_by=orders.region), direction='desc')` wrong.
   - Taken: rule by position with a defaulted marker. Probe: every wrong value fixed, mode
     table unchanged, 5730 related tests pass.

3. **Scope extensions.** (a) Windowed and cross-dataset inners under a transform in
   aggregation-source or parameter position. (b) The same for the operand of a `first` /
   `last` over an aggregate. (c) An aggregate inside a second-order aggregation under a
   transform whose operand grain differs from the query grain (the wrong value found
   while probing). (user)

4. **The marker.** `AggregateKey.grain_defaulted: bool = False`, listed in
   `NON_IDENTITY_KEY_FIELDS` and left out of the handwritten `__eq__` / `__hash__`, as
   `builtin` is: a defaulted and a written spelling of one grain are one value (Axiom 2.7)
   and intern to one slot. When the query also writes the same aggregate with the same
   grain, the pass pins without the marker, so that slot is judged as written whatever the
   order of occurrence. Alternative: the marker in equality — two slots for one value and
   two keys with one spelled name. (default)

5. **Invariant.** `grain_defaulted` is false whenever `partition_keys` is `None`, enforced
   by a model validator; `_strip_redundant_partitions` and every other rewrite that clears
   `partition_keys` clears the marker. (codex)

6. **Readers.** Each reader of `partition_keys is None` / `is not None` is classified once:
   a grain question reads `partition_keys`; a "did the user ask for this grain" question
   (mode gate, `associated` / `broadcast` warnings, error-mode refusal, routing through the
   mode axis) reads `partition_keys is not None and not grain_defaulted`. Known members of
   the second class: `explicit` in `_synthesize_cross_dataset_producer`,
   `_check_param_partition_keys`, `local_broadcasts` and `_host_inline` in
   `slayer/engine/compile/discovery.py`, `crossing_local_root_predicate` in
   `slayer/engine/cardinality/relationship_safety.py`. One accessor on the key answers the
   second question; no reader spells the conjunction itself. (codex)

7. **Names.** Display, canonical alias and warning / error text spell a defaulted grain as
   unwritten (`slayer/core/refs.py`: `_aggregate_display`, the canonical-name builders),
   so no message carries `…_partition_by_…` for it. (codex)

8. **Where the pass runs.** Once, over metric, filter and order keys (dimensions skipped),
   after partition-key validation and after the dimensions' ranked grains are settled,
   immediately before `settle_ranked_grains` on those keys, with the settled query grain.
   Post-order; a `first` / `last` over an aggregate is not itself pinned (its grain comes
   from its operand, Axiom 5) but its whole subtree is visited. (default; traversal
   wording (codex))

9. **`_shifted_producer_grain` keeps its own union.** Its row-leaf branch falls back to the
   query grain, which is right because a legal leaf is a projected dimension; `time_shift`
   over the share already works; the shifted producer is owned by DEV-2429 and DEV-1959.
   rejected (codex)

10. **Dimension position.** Probed under decision 1: an aggregate with no `partition_by=`
    in a dimension keeps the existing `ComputedDimensionError`, an explicit one is
    unchanged, and a mixed-operand measure beside a computed dimension becomes correct.
    Design unchanged; tests added. (codex)

11. **Row-level leaf beside a coarser aggregate.**
    `rank(sum(orders.amount, partition_by=orders.region) * iif(orders.city = 'A', 1, 2), direction='desc')`
    is an internal `MaterialisationStageError` today and reads 4, 3, 1, 1 under decision 1.
    Test added. (codex)

12. **Windowed coverage.** Tests for a nested windowed inner, a windowed inner under
    `last`, and two time buckets. (codex)

13. **Spec placement.** One ADDED requirement in `queries/transforms`. No new capability,
    so no `architecture/` mapping change. (default)

14. **Tests.** New file `tests/test_transform_operand_grain.py`; `first` / `last` of the
    share are kept as pinned regressions although `first` / `last` are aggregations since
    DEV-2436. No docs change: no documented behaviour changes. (default)

15. **Existing test aligned.** `tests/test_dev1867_grain.py`
    `TestRegroupRootGrain::test_transform_root_union_of_inner_grains` asserts the deleted
    transform arm of `regroup_root_grain` and is deleted; the class's other two tests
    stay. (user)

16. **arc42 edit** — (user, approved wording): in `architecture/semantics.arc42.md`, after
    the `[enforced: test:tests/test_dev2218_transform_domain.py]` tag of Axiom 11, add

    `    [enforced: test:tests/test_transform_operand_grain.py]`

17. **A separate wrong value is not in scope.** A transform over a coarser operand inside
    an aggregation (see Non-Goals) has a different cause; filed as DEV-2494 under
    DEV-2451. Alternative: fix here — undiagnosed, and two unrelated fixes in one PR. (user)

## Risks / Trade-offs

- [A reader of `partition_keys` classified wrongly changes a warning or a route for a
  query that works today] → decision 6's audit is a task of its own; a regression test
  with a same-dataset inner under a transform and a query dimension reached through a
  fanning hop, in the default and `error` modes; the mode-table scenarios.
- [The pass pins too widely] → pinning under a top-level transform whose operand grain
  equals the query grain broke 251 tests in the probe (about 180 golden SQL, the rest
  `TimeSpineError` on shifted filters, `ParameterGrainError`, the mode-gate tests). The
  rule stays as worded in decision 2.
- [The pass runs too early] → before partition-key validation and dimension settling it
  compares grains whose keys are not canonical yet (47 failures in the probe). Decision 8
  fixes the place.
- [Honouring the marker in the same-dataset readers changes a warning] → today a
  same-dataset inner pinned by the old helpers is judged as written. Under decision 6 it
  is judged as unwritten, as the requirement says. Any existing test that pins the old
  outcome is an approval request to the user, not a reason to narrow decision 6.
