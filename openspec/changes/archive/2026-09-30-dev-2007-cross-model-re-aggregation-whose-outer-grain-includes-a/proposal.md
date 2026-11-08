## Why

A cross-model re-aggregation whose outer grain holds a key the query root reaches only across a fanning hop — a joined to-many model's time bucket, a plain to-many column, or an associated dimension — raises a raw `IndexError` instead of computing, although the operand dataset (the re-aggregation's home, Axiom 2.4) determines or associates that key. The outer grain is judged correctly once against the operand grain, then judged again against the query root in two other places (the outer producer's discovery and the bind-time partition-key check), which disagree.

## What Changes

- A re-aggregation's outer grain — explicit `partition_by=` keys and the ungrained query-dimension default alike — is judged once, against the operand dataset's grain: a determined key is attributed even when the query root reaches it only across a fanning hop.
- The settled outer grain is carried as the outer aggregation's explicit grain, so no later compile step re-judges it from the query root; the `IndexError` becomes impossible for this shape and any residual misrouting fails as a named internal invariant.
- **BREAKING**: an explicit outer `partition_by=` key that the operand grain does not determine is a typed `PartitionKeyError` outside `associate` mode (it previously broadcast with a warning when the query root reached it over to-one hops), and is associated in `associate` mode (it previously raised when the query root reached it only across a fanning hop). An explicit outer key the operand grain determines now computes (it previously raised when the query root reached it only across a fanning hop).
- Ungrained outer dimensions the operand grain does not determine keep resolving per `to_many_handling` (broadcast with warning / associate / error), unchanged.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/partitioned-aggregates`: the re-aggregation requirement's outer `partition_by=` clause changes (explicit outer keys are judged against the operand grain; undetermined explicit keys error outside `associate`), and a new requirement pins outer-grain attribution against the operand dataset.

## Impact

- `slayer/engine/compile/stages.py` (`_synthesize_reaggregation_producer`), `slayer/engine/bind_inputs.py` (`_validate_partition_keys`), a new checker in `slayer/engine/elaborate_env.py`.
- `architecture/semantics.arc42.md` Axiom 8 wording (`host` → `home`) and an Axiom 7 enforcement tag (both approved).
- `docs/concepts/formulas.md` re-aggregation section (one sentence).
- New tests `tests/_reagg_outer_grain_fixtures.py`, `tests/test_reagg_outer_grain_attribution.py`.
