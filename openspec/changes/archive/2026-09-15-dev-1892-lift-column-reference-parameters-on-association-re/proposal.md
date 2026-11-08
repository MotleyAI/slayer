# Proposal: Lift column-reference parameters on association / re-aggregation outer aggregates

## Why

The two-level kernel (DEV-1841 association, DEV-1847 re-aggregation) picks only the
aggregate's own value into `_base`, so an aggregation parameter that references a column —
explicit (`weighted_avg(x, weight=qty)`) or a definition default
(`params: [{name: weight, sql: amount}]`) — fails closed at three sites even when the
parameter is constant per cell. Closure (`architecture/semantics.arc42.md` §2.9) forbids
refusing a well-typed term, and the home-dataset axioms (§2.2, §2.4) already say when a
parameter is well-typed: when the grain of the dataset the aggregation runs over determines
it. This change makes that the one rule and retires the three gates.

## What Changes

- **One type rule for parameters.** A parameter `P` of an aggregation over dataset `D` with
  grain `G` is legal iff `G` determines `P` — a grain member, an aggregate grained ⊆ `G`, or a
  column seeded from a grain member's model over provably to-one hops. The residue raises one
  typed error in the checker naming the parameter, the grain, and the remedy.
- **One lift.** A legal parameter is picked once per level-1 row (`MAX(P) AS _p<i>`) and
  level 2 references `_base._p<i>` — the same path whether level-1 rows come from a raw table
  (association) or a producer (re-aggregation); the kernel's two value-sourcing branches fold
  into that one pick path (derived-column expansion, `column_type` cast and owner-anchored
  measure-local filter preserved).
- **Aggregate-valued parameters become representable and bindable** (`weight=sum(w,
  partition_by=city)`); on a re-aggregation they ride the carrier as constituents; the
  combined-consumer partition-key exemption covers a re-aggregation's args/kwargs, not only
  its source.
- **Determination generalised, dimensions included (deliberate expansion):** an entity-key
  grain field seeds a to-one chain at any join path, so an outer dimension such as
  `customers.region` over an inner grain containing `customers.id` becomes attributable
  (exact, no broadcast warning) instead of unattributable.
- **Attached parameter on a row-level source** (`spend:weighted_avg(weight=sum(…,
  partition_by=…))`) is a typed error with the remedy; the row-attach mechanism lands in
  DEV-1859 (deferral documented there).
- Definition defaults are typed at plan time (owner-anchored), never at render time; the
  render-time gate is deleted.
- Raise-ledger rows swapped; goldens re-blessed under the divergence protocol; axiom 2
  (Home dataset) flips to enforced (approval-gated normative edit).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/semantics`: ADDED *Aggregation parameters are typed by the home dataset's grain*;
  MODIFIED *Second-order aggregation over attached values* (an entity-key grain field seeds
  attribution at any join path).
- `queries/attribution-modes`: MODIFIED *Association eligibility and input handling* — the
  column-reference-parameter exclusion is replaced by the parameter typing rule.
- `queries/partitioned-aggregates`: MODIFIED *Re-aggregation consumes attached operands as
  datasets* — the blanket outer-parameter rejection narrows to the typed residue; operand-grain
  parameters execute.
- `aggregations/expression-aggregation`: MODIFIED *Unsupported expression shapes fail with
  clear errors* — an attached parameter on a row-level source is a named typed error.

## Impact

- `slayer/core/keys.py` (`_AggregateArgValue` admits `AggregateKey`;
  `reaggregation_operand_keys` walks args/kwargs), `slayer/engine/binding.py`
  (`_bind_agg_arg` binds an `AggCall`), `slayer/engine/bind_inputs.py` (attached-on-raw check),
  `slayer/engine/elaborate_env.py` (two checks added, two deleted),
  `slayer/engine/join_safety.py` (`grain_determines`), `slayer/engine/compile/stages.py`
  (parameter resolution, both syntheses, dimension checks delegate),
  `slayer/ir/planned.py` (`AssociationProducerKernel.picked_params`),
  `slayer/sql/generator.py` (one pick path; default-param gate deleted).
- `tests/_dev1871_raise_ledger.py`; DEV-1841/1847 gate tests re-pointed; DEV-1841
  association goldens re-blessed; new `tests/test_dev1892_*` suites + golden baseline.
- `docs/concepts/queries.md`, `docs/concepts/formulas.md`,
  `docs/examples/07_aggregations/aggregations.md` (one sentence each);
  `architecture/semantics.arc42.md` axiom 2 (user-approval-gated); Linear DEV-1859 comment.
