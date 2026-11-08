# Grain as a first-class type with lattice operations

## Why

The query-semantics axioms treat the grain — an aggregate's dimension set — as the
aggregate's *type*: grains form a lattice under inclusion, the union grain is the join,
broadcast is the coarse→fine coercion. Today grains are ad-hoc `frozenset[ValueKey]`
values threaded through `regroup_planner` / `stage_planner` / attach admission, with
subset/union logic and broadcast-direction reasoning re-derived at each site. DEV-1841,
DEV-1847/DEV-1859 (grain-union discovery), and the DEV-1871 typed-core consolidation all
build on a single grain type, so it lands early.

## What Changes

- New `slayer/core/grain.py`: a frozen, hashable Pydantic `Grain` value type wrapping
  `keys: frozenset[ValueKey]`, with union/join, subgrain (coarser–finer) tests, and an
  explicit broadcast-direction predicate (coarse→fine only; the reverse names the
  second-order-aggregation remedy).
- The grain-valued frozenset plumbing in `slayer/engine/regroup_planner.py` and
  `slayer/engine/stage_planner.py` (root-grain computation, nested-attach admission,
  functional-determination pruning, producer grouping, cross-model safe grain) is
  retyped to `Grain`.
- Pure refactor: no behavior change, golden SQL byte-identical, all planner error
  messages byte-identical.
- Out of scope (recorded on DEV-1871): retyping `partition_keys` on
  `AggregateKey`/`TransformKey`; the slot-id grain representation in `slayer/sql`.

## Capabilities

### New Capabilities

None — pure refactor, no spec-level behavior change (`skip_specs: true`).

### Modified Capabilities

None.

## Impact

- `slayer/core/grain.py` (new), `slayer/engine/regroup_planner.py`,
  `slayer/engine/stage_planner.py`.
- Tests: new `tests/test_dev1867_grain.py`; `tests/test_dev1835_grain_prune.py` call
  sites wrapped in `Grain.of(...)`.
- No API, storage, docs, or architecture-model impact; layers contract untouched
  (`slayer.core` already claims the module).
