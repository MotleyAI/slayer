## Why

An explicit `partition_by=` key that is not attributable from the aggregate's home is refused in every mode by a bind-time guard, while the same grain written as a query dimension resolves per `to_many_handling`. Rooted at `customers`, `sum(customers.spend, partition_by=orders.status)` raises even under the default, although `sum(customers.spend)` by `orders.status` associates and the orders-rooted cross-model twin associates too. A fanning time key on a local aggregate (the open part of DEV-1941) and a derived key such as `bad_pop = pop + region_events.value` are refused the same way, while `sum(pop)` by `bad_pop` returns per-cell values. Two code paths answer one question, whether a grain member is attributable from the home, and they disagree. Axiom 8's mode-invariant carve-out also contradicts Axiom 2.4 (the aggregation's own `partition_by=` is not an input) and 2.9.

## What Changes

- Every explicit `partition_by=` key not attributable from the aggregate's home resolves by the one mode gate, exactly like a query dimension. This covers plain and derived keys, local and cross-model aggregates, time and non-time keys, and every aggregation kind (plain, `first`/`last`, windowed, windowed `first`/`last`).
- Under the default and `"associate"`, such an aggregate computes at the declared grain over the distinct associated home rows, with no warning, because the grain was requested. A fanning time key associates per bucket, so a home row whose associated rows fall in several buckets counts once in each.
- Under `"broadcast"` and `"error"`, such an aggregate is refused with `PartitionKeyError`, local and cross-model alike. The suggestion names association as well as the cardinality remedy.
- The mode-invariant fanning-partition-key guard is removed. A key whose definition no dialect can analyse still fails closed in every mode.
- A rank-family transform's own `partition_by=` keeps its rule: it names operand-grain members and never associates on its own.
- Unchanged: attributable keys, the positional rules, the per-mode resolution of keys across a product hop, the input-safety rules for sources, parameters and ranking keys, and the window-axis `TimeAxisError`.
- `architecture/semantics.arc42.md` Axiom 8: the carve-out is replaced with the one-gate rule (approved wording in design.md).
- Additive: every affected query raises today.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/cross-model-aggregates`: the explicit-grain requirement covers local aggregates, with scenarios for plain, `first`/`last`, windowed and time keys, keys mixing attributable and unattributable members, and the refusal under broadcast/error.
- `queries/attribution-modes`: the "fails closed in every mode" requirement for fanning partition keys is replaced by "Unattributable explicit partition keys resolve per the mode". Error mode no longer carves out keys that fan from the host.
- `queries/transforms`: a shifted composite's leaf with an unattributable `partition_by=` key resolves per the mode.
- `queries/partitioned-aggregates`: an attached parameter or re-aggregation constituent whose own `partition_by=` fans from its own home resolves per the mode.

## Impact

- Code:
  - `slayer/engine/bind_inputs.py` and `slayer/engine/cardinality/relationship_safety.py`: the bind guard keeps only the unanalysable fail-closed check.
  - `slayer/engine/compile/discovery.py`: a local aggregate with an unattributable explicit key routes to the home-rooted synthesis.
  - `slayer/engine/compile/stages.py`: the mode gate takes local explicit keys.
  - `slayer/engine/elaborate_env.py`: one shared partition-key checker for ordinary aggregates, with generalised wording.
- Docs: `docs/concepts/queries.md` (association section; the cross-dataset `partition_by=` sentence).
- Architecture: Axiom 8 wording.
- Tests: a new executed-value suite on SQLite and DuckDB. Existing pins on the every-mode refusal are re-pointed, with approval.
- No stored-shape changes, no new dependencies, and no model changes.
