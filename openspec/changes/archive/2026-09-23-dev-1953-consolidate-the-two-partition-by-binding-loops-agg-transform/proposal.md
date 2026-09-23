## Why

A rank-family transform's own `partition_by=` binds through a second, hand-kept copy of
the aggregation `partition_by=` loop that lacks the computed-dimension alias branch, so
`rank(sum(amount), partition_by=ureg)` raises an unknown-reference error in every
position where the aggregation twin `sum(amount, partition_by=ureg)` resolves — although
`queries/computed-dimensions` already promises a computed dimension can be "used as a
transform partition". Axiom 11.2 (a rank-family key must name an operand-grain member)
is enforced nowhere: a non-member key silently widens the producer's grain today and,
once aliases resolve, dies with an internal error.

## What Changes

- One binding helper for aggregation and transform `partition_by=`: element iteration,
  computed-dimension alias resolution, column-kind validation, one contextual error
  naming the construct. A transform's own `partition_by=` then names a computed dimension
  in the measure, aggregation-parameter, filter, order and computed-dimension positions.
- The rank family's own `partition_by=` keys must be members of the transform's operand
  grain (Axioms 11.1, 11.2, 11.3b, 11.5), in every position; a non-member fails with a
  typed checker error naming the transform, the key, the operand grain and the remedy.
  **BREAKING** for three shapes that silently executed by widening the producer grain: a
  measure or filter `rank(sum(amount, partition_by=[city, region]), partition_by=product)`
  with `product` a query dimension, and a dimension-position transform whose own key is a
  query dimension outside its inner aggregates' grain.
- The parser rejects a repeated keyword argument on any call (aggregation and transform
  alike) instead of one side keeping the last occurrence and the other concatenating.
- An attach-carrying computed dimension as a transform's own key resolves but stays
  unsupported by the planner in the measure and aggregation-parameter positions; pinned
  by characterization tests for DEV-1960 (filter and order positions execute).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/partitioned-aggregates`: ADDED "Transform partition keys bind like aggregate
  partition keys" — alias resolution and the construct-naming error, every position.
- `queries/transforms`: ADDED "Rank-family partition keys are operand-grain members" —
  the Axiom 11.2 rule, its operand-grain definition and error precedence.
- `queries/computed-dimensions`: MODIFIED "Measure-dimension symmetry with grain
  self-containment" — one added scenario for a computed dimension used as a transform
  partition (requirement text unchanged).
- `aggregations/functional-form`: ADDED "Repeated keyword arguments are rejected".

## Impact

- `slayer/engine/binding.py` (the helper; `_bind_agg_partition_keys` and the inline
  transform loop retired), `slayer/engine/syntax.py` (repeated-keyword rejection),
  `slayer/core/keys.py` (`transform_operand_grain`), `slayer/engine/elaborate_env.py`
  (the membership checker), `slayer/engine/bind_inputs.py` (its call site after the
  partition-key rewrite), `tests/_dev1871_raise_ledger.py` (+1 checker row).
- Tests: new `tests/test_dev1953_partition_alias.py` and
  `tests/test_dev1953_partition_membership.py`; no existing test relaxed.
- Docs: two sentences in `docs/concepts/formulas.md`. Optional arc42 `enforced:` tag on
  Axiom 11 only with an explicit per-edit OK.
- Guards baseline, legacy-arrow baseline and the import model unchanged; DEV-1960 (child
  of DEV-1873, blocked by this change) owns the attach-carrying transform key.
