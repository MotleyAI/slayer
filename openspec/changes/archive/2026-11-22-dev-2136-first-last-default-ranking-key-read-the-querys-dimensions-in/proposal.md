## Why

A ranked `first` / `last` without an explicit ranking key takes its default from the
grain of the producer that computes it, not from the query's dimensions as the spec
says. A `partition_by=` coarser than the query, a `window=`, nesting inside a
second-order aggregation, or a cross-model producer therefore hides the query's
temporal dimension: `last(amount, partition_by=region)` over `[region, e]` and a month
bucket on `created_at` ranks by the model default (US = 40 instead of 60), and fails
outright when the model has no default.

## What Changes

- The default ranking key reads the host query's dimensions then time dimensions, in
  projection order, for every ranked producer — plain, `partition_by=`, `window=`,
  `time_shift` and nested in a second-order aggregation (the user query's dimensions,
  never the enclosing aggregation's grain); each stage uses its own dimensions.
- A cross-model producer reads each candidate home-routed and re-rooted into its
  home's coordinates, skipping one its home does not determine, then the home's
  `default_time_dimension`.
- A local candidate crossing a fanning or unproven hop, once chosen, fails closed with
  the ranked-input safety error, as the unpartitioned query does.
- Result change: partitioned, windowed, nested and cross-model `first` / `last`
  without a ranking key, in a query with a temporal dimension outside the producer's
  grain, rank by that dimension instead of the model default (or instead of failing).
  Documented in the 2.0 upgrade guide.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `aggregations/functional-form`: › Default ranking key reads the query's dimensions
  whatever the aggregation's `partition_by=`, `window=` or nesting, re-rooted for a
  cross-model home.
- `queries/cross-model-aggregates`: the input-safety requirement's ranking-key clause
  defers to › Default ranking key instead of naming the producer's own row dimension.

## Impact

- `slayer/ir/prebound.py` (`PreboundQuery` gains the query's ranking dimensions,
  threaded onto producer prebounds like `mode_policy`), `slayer/engine/bind_inputs.py`,
  `slayer/engine/compile/stages.py` (local, shifted, windowed and cross-model producer
  synthesis; producer-grain candidate plumbing removed), `slayer/engine/ranked_planner.py`.
- `docs/upgrading/2.0.md`.
- No API, storage or dependency change.
