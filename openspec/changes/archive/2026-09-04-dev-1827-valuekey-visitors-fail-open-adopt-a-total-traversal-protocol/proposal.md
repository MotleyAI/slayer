# Proposal: ValueKey total traversal protocol (children/map_children)

## Why

`ValueKey` is a closed union of 11 kinds traversed by ~30 hand-rolled visitors, of which
only three fail closed; the rest silently treat an unhandled kind as a leaf/literal/
non-composite. Adding a union member (the DEV-1740 `ConditionalKey` episode) produced two
successive review rounds of real silent bugs; the trap is still armed for the next member.

## What Changes

- New traversal protocol on every `ValueKey` kind (plus `SqlExprKey`): `children()` and
  `map_children(fn)`, hand-written per kind, fail-closed via raising base-class defaults.
- A kind-policy registry (`KIND_POLICY`) in `core/keys.py` with consumer-named flags
  (`slottable`, `slot_composite`, `materialised_order`); the local policy tuples and the
  two `_VALUE_KEY_TYPES` copies become derived constants.
- Generic walkers/rewriters (`walk_value_keys`, `contains_aggregate`,
  `lower_sugar_transforms`, `rewrite_rank_partition_keys`, `_map_value_key`,
  `substitute_value_keys`, join-path discovery, HAVING validation,
  `_collect_base_aux_slot_ids`) route through the protocol; deliberately asymmetric
  visitors keep explicit dispatch with a documented asymmetry and a fail-closed raise tail.
- Renderer dispatch raises on an unhandled key kind instead of emitting a garbage literal.
- Latent bug fix: `lower_sugar_transforms` now reaches transforms nested under an `InKey`
  inside a scalar call (its hand-listed recurse tuple omitted `InKey`).
- Behavior otherwise preserved: golden SQL byte-identical, full non-integration suite green.

## Capabilities

None — pure internal refactor plus a fix that does not alter specified behavior
(`skip_specs: true`). No user-facing query semantics, API, or SQL output changes.

## Impact

- `slayer/core/keys.py` (protocol, registry, rewriter collapse)
- `slayer/engine/`: `binding.py`, `planning.py`, `stage_planner.py`, `ranked_planner.py`,
  `regroup_planner.py`, `prebound.py`, `aggregate_input_paths.py`,
  `column_filter_paths.py`, `response_meta.py` (audit)
- `slayer/sql/`: `generator.py`, `render/value_expr.py`, `render/row_expr.py`,
  `naming.py`, `sql_expr.py` (audit)
- New tests `tests/test_dev1827_value_key_traversal.py`; existing totality tests retained.
