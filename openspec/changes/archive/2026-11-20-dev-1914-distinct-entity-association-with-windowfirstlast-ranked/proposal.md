## Why

Association is a kernel *kind* chosen exclusively (association → trailing-window → ranked), so no reducer composes with it, and the local host-regroup path lets a windowed or ranked producer's grain fan past the mode axis. The results:

- A cross-model `first`/`last` or `window=` aggregate that needs association broadcasts by default and is refused under `associate`.
- A window whose time axis fans from its home is refused in every mode.
- A local windowed aggregate grouped by a fanning dimension silently double-counts in every mode, `error` included. For example, customers-rooted `sum(spend, window='1y')` by `orders.status` × signup month returns 200 for (ok, Jan); the correct value is 100.
- Local `first`/`last` ignores `broadcast` and `error`.
- The keyless list-child association falsely refuses a parameter its root row determines.

Axiom 8's "every aggregation is association-eligible" carries `[target: DEV-1914]`.

## What Changes

- Association becomes orthogonal to the reducer: every aggregate (plain, ranked, windowed, windowed-ranked; local or cross-model) with an unattributable grain member resolves by one mode gate — associate (the default), broadcast or error. Under association, each cell's distinct home entities are deduplicated first, then the reducer runs over them.
- Ranked association: per cell, `first`/`last` picks among the distinct associated home entities by a home-determined ranking key.
- Windowed association (the DEV-1909 cell-time reading): a home entity belongs to (bucket, cell) iff one of its associated rows carrying the cell's values has its time in the bucket's frame. The entity is counted once per cell, and a filter on the axis's branch binds to the same related row. A window whose time axis fans from its home now associates by default and under `associate`; under `broadcast` and `error` it keeps its typed error, which now names association as the remedy.
- **BREAKING (explicit modes only):**
  - A local `first`/`last` or windowed aggregate over an unattributable dimension broadcasts under `"broadcast"` and is refused under `"error"`, like every other aggregate.
  - The local windowed double-count is fixed in every mode.
- An explicit `partition_by=` key that needs association now associates `first`/`last` and windowed aggregates by default and under `associate`.
- The keyless list-child association treats a parameter its root row determines as determined.
- **Removed (unreleased, 2.0 dev line):** the broadcast fallback for association-ineligible aggregates. This covers the fallback mode flag, the `association_unsupported` field of the `broadcast` warning and its hint, and the window/first-last association refusal. The default now resolves exactly as `"associate"`.
- `architecture/semantics.arc42.md`: Axiom 8 flips to "Every aggregation associates…", and Law 5 drops the "association-eligible" qualifier (approved wording in design.md).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/attribution-modes`: the default no longer falls back; every aggregation is association-eligible, ranked and windowed included, with their per-cell readings; the list-child parameter rule; the broadcast-fallback requirement is removed; ranked re-aggregation inners associate.
- `queries/cross-model-aggregates`:
  - A window over a fanning time axis associates.
  - A ranked producer's grain resolves per the mode.
  - A host-regroup producer's grain is attributable, and an unattributable grain goes to the association producer.
  - Broadcast metadata loses the fallback reason.
- `queries/semantics`: attribution, loud degradation and dice–slice no longer distinguish association-ineligible aggregates.

## Impact

- Code:
  - `slayer/engine/compile/stages.py`: kernel decision, local routing, the association arm, windowed axis handling, fallback removal.
  - `slayer/engine/elaborate_env.py`: the window/first-last refusal is deleted; the time-axis error's suggestion changes.
  - `slayer/ir/planned.py`: the trailing-window kernel gains entity keys; the ranked kernel gains an association dedup.
  - `slayer/sql/generator.py`: entity dedup inside the trailing window and before ranking.
  - Mode policy, warnings and errors: the fallback field, its hint and the flag are removed.
  - `query_engine.py`: the warning merge key changes.
- Response shape: the `broadcast` warning loses `association_unsupported` (never released).
- Docs: `docs/concepts/queries.md` (mode table, warnings table, cross-dataset section) and `docs/upgrading/2.0.md`.
- Tests: new executed-value suite (SQLite and DuckDB) and dialect emission; existing pins on the refusals and the fallback are re-pointed.
- No new dependencies and no architecture-model changes.
