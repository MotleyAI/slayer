## Why

Four structural gaps left by DEV-1859 make the planner agree with itself by hand: producer-root discovery is spread over four walks whose outputs `_plan_regroups` reconciles by list arithmetic; every producer synthesis site hand-predicts whether its sub-plan needs nested discovery through two flags; the shift-family and non-shift transform checkers are two functions over one walk; and the parser decides `first`/`last` dispatch by AST shape, a second copy of the row/attached classifier. None of it changes specified behaviour except one closure wart it removes, and all of it is where the next bug of each class will land.

## What Changes

- One discovery walk (`slayer/engine/compile/discovery.py`) yields a `RootDisposition` per consumer occurrence (root, phase, routing, consumer names, declared type); `_plan_regroups` consumes that list, and the four walks, `CombinedConsumers` and the post-hoc list arithmetic are deleted. Attached inputs are opaque in discovery as they already are for safety, so `aggregate_input_closure` loses `descend_aggregates` (the DEV-1900 deferral). DEV-1958's shifted-producer candidates come from the same walk.
- One producer flag `in_producer` replaces `disable_host_rooted_isolation` and `enable_producer_regroups` on every planner entry point; nested discovery is always on inside a producer, and the sub-plan's one nesting rule is "a root nests iff its grain is a strict subset of the producer grain". The six caller-side predicates and `_answers_need_nested_regroups` are deleted.
- One transform-input checker `check_transform_inputs` with a per-op regime table replaces `check_transform_row_leaf` and `check_time_shift_input`; the row-leaf rule is judged per transform node, so the transform consuming the leaf is the one named, and no `first`/`last` exemption exists. Messages byte-identical; ledger rows re-anchored.
- `first`/`last` parse to one node (`AggCall`, like the colon spelling); the operand binds like any transform input and the bound key's type dispatches through one classifier (`is_attached_source`): attached operand → the series transform, row-grain operand → the ranked aggregation. `_is_mixed_agg_source` is deleted. Consequence: `first(rev)` over a saved measure `rev` is now the transform, as `cumsum(rev)` already is, instead of an unknown-reference error.

Behaviour-preserving otherwise: every existing golden byte-identical. The one intended plan-shape change (a local aggregate whose attached input's inner crosses a join compiles inline with the input row-attached, instead of through a host-rooted producer) is value-identical and pinned. Which transform a nested row-leaf error names moves from the outermost non-exempt transform to the one consuming the leaf.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `aggregations/functional-form`: "Ambiguous first and last names dispatch by argument shape" — dispatch is by the bound operand's type (row-level → aggregation, aggregate-valued → transform), so a saved-measure operand reads as the transform.

## Impact

- `slayer/engine/compile/` (new `discovery.py`; `stages.py` loses four walks, six predicates, two flags; `_plan_regroups` split into discovery → grouping → synthesis; `compile/__init__.py`, `plan.py`, `elaborate.py` take `in_producer`; DEV-1958's `compile/shift.py` consumes shift candidates), `slayer/ir/bound.py` (data shapes only), `slayer/engine/reference_closure.py` / `join_safety.py` (one closure mode), `slayer/engine/elaborate_env.py` (one checker), `slayer/engine/bind_inputs.py` (one call), `slayer/engine/syntax.py` / `binding.py` (one parse node, bind-time dispatch), `slayer/core/keys.py` (`is_attached_source`).
- Tests: new `tests/test_dev1903_*.py`; consented edits to eight existing tests (listed in tasks.md); `tests/_dev1871_raise_ledger.py` rows re-anchored.
- Architecture: `architecture/ir.arc42.md` §1 and `architecture/engine.arc42.md` P2 tag, each as an exact diff with explicit approval before landing.
- Depends on DEV-1958 having merged (its checker rename, own-grain rule and shifted producer are the baseline).
