# DEV-1847 design — second-order aggregation on the mode-axis substrate

## Context

See proposal.md for motivation. Substrate facts that shape the design:

- The parse gate `syntax.py:_validated_agg_source` rejects any aggregate or
  transform inside a functional aggregation's source; `first`/`last` dispatch
  aggregation-vs-transform by first-arg shape.
- Regroup discovery (`stage_planner._plan_regroups`) desugars partitioned
  aggregates into producers; nested producers are admitted by
  `_validate_nested_producer_plan`, which today rejects depth > 1 and
  non-strict-subset nested grains — two of the three DEV-1847 deferral sites;
  the third is `_reraise_nested_attach` (partition_by naming a computed
  dimension).
- DEV-1841 landed `to_many_handling` threaded through every recursive
  `plan_query`, uniform disposition classification for every (aggregate,
  unattributable dimension) pair, and the two-level association `ProducerKernel`
  (level 1: group by cell × entity key; level 2: ordinary aggregate over level-1
  rows).
- DEV-1853 landed the shared bidirectional join walker; determination questions
  resolve from cardinality labels in either direction.
- The guard ratchet pins `NotImplementedError` deferral sites to
  `guards.baseline: 8` in `architecture/index.yaml`, only ever lowered.

## Goals / Non-Goals

**Goals:** axiom 6's second-order clause true by construction on one kernel
family; the three DEV-1847 deferral sites removed (`guards.baseline` 8 → 5);
uniform mode-axis behavior at the re-aggregation seam.

**Non-Goals:** row-mixing operand sources (DEV-1859); transforms inside
aggregation sources, outer measure-local `filter=`, and the closure sweep
(DEV-1868); population inference interplay (DEV-1866); outer `window=`/ranked
re-aggregation kernels (typed error now).

## Decisions

1. **Discovery on typed keys at binding.** A re-aggregation is recognized when
   an `AggregateKey`'s source subtree resolves entirely to attached values
   (AggregateKeys, directly or under arithmetic/scalar composition) — engine P1,
   never text. The parse gate narrows to rejecting transforms and (per
   DEV-1859's boundary) mixed row/attached sources; pure-attached sources parse
   into the ordinary `AggCall` shape so binding sees structure, not spelling.
   `first`/`last` first-arg dispatch is untouched.
2. **One carrier construction, two joins.** The outer producer is the same
   two-level shape as DEV-1841's association kernel: level 1 materializes the
   operand dataset — the population's distinct union-grain cells with each
   constituent producer LEFT-joined null-safely on its own grain (the carrier;
   axiom 12 supplies the row set) — and level 2 is an ordinary aggregate over
   level-1 rows grouped by the outer grain. Attributable outer keys come from
   grain membership or the seeded determination walk (entity-key grain fields
   only, DEV-1853 walker); associate mode reuses the association join with
   entity = the inner grain tuple, whose uniqueness holds by construction.
   Alternative — a bespoke producer-over-producer path beside the association
   kernel — rejected: two parallel two-level implementations of one shape.
3. **The outer aggregation is an ordinary combined consumer.** Its value
   substitutes by structural placeholder and attaches per the normal
   row/combined rules (sql P10); its explicit `partition_by=` carries the
   existing combined-consumer and attributability rules; the inner operand is
   exempted from the combined-consumer rule exactly as the dimension role is.
4. **Arbitrary depth via recursion, not enumeration.**
   `_validate_nested_producer_plan` drops the depth-1 and strict-subset arms;
   admission is the general rule (complete-grain attach at every level,
   discovery inside a producer excluding roots at exactly the producer grain).
   Interning and the CTE-hoist already make depth structural; golden + scope
   assertions pin the flat `WITH`.
5. **Typed errors, ratchet untouched by additions.** New rejections (row-mixing
   source, outer `window=`/ranked, outer `filter=`) are `SlayerError`/
   `ValueError` subclasses with remedies — specified behavior outside the
   `NotImplementedError` ratchet. The three removed sites lower the baseline to
   5 and shrink `DEFERRAL_SITES` in the same commit.
6. **Degenerate warning is a typed payload.** New warning kind carrying operand
   grain, outer grain, and the `partition_by=` remedy; emitted once per semantic
   event under the existing producer-warning dedup (a producer interned across
   scopes warns once). Broadcast/associate warnings at the seam reuse DEV-1841's
   payloads unchanged.
7. **Architecture bundle in this PR.** `semantics.arc42.md` axiom-6 clause flips
   to `[enforced: test:...]`; `index.yaml` `guards.baseline: 5`; docs
   (`formulas.md`, aggregation examples, `slayer-query.md` skill) gain the
   re-aggregation surface.

## Risks / Trade-offs

- [Lifting depth guards exposes latent recursion bugs only at depth ≥ 3] →
  depth-3 executed tests plus shared-producer-across-depths interning tests;
  `assert_scope_closed` and flat-`WITH` assertions at every depth.
- [Carrier rows from the population differ from a target-rooted inner
  producer's native cells (e.g. cities with no host rows)] → pinned by spec:
  the population supplies the row set; the equivalence sweep vs the manual
  multi-stage encoding covers the agreement on filtered populations.
- [Null-safe cell identity varies by dialect] → executed null-grain tests on
  SQLite + DuckDB; golden coverage on every dialect emission path.
- [Warning volume at the seam (degenerate + broadcast may co-occur, e.g.
  keyless inner)] → both are one-per-event and name disjoint facts; accepted.

## Migration Plan

Pure feature lift: previously erroring queries now execute; no stored-artifact
migration. Golden divergences follow the divergence-ledger protocol. Rollback =
revert the PR.

One existing test changes behaviour: `test_expression_aggregations.py::test_nested_aggregation_rejected` asserts `sum(sum(amount))` raises, which this change makes legal (degenerate re-aggregation); it is re-pointed to a still-rejected shape under task 7.1.
