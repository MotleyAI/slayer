## Why

A `partition_by=` naming a **path-less derived** column whose definition crosses a
fanning join hop (e.g. `bad_pop = pop + region_events.value` over the 1:N
`regions → region_events` hop) escapes the partition-key attributability guard and
silently emits a multiplying window partition — `pop:sum(partition_by=bad_pop)` returns
the double-counted `200` for every cell instead of the honest per-region `100 / 200`.
The guard early-returns for any partition key whose *own* path is empty, judging on
spelling rather than the dependency closure, so the fanning hop hidden inside the derived
definition is never checked. The path-bearing spelling and the cross-model case already
fail closed; only this spelling leaks a silent wrong result.

## What Changes

- Guard a **path-less** partition key whose dependency **closure** crosses a fanning
  or unproven hop **from its host**, failing closed with an error that names the hop.
- **BREAKING (fail-closed):** such a key now raises in **every** `to_many_handling`
  mode — `broadcast`, `error`, **and** `associate` — because a key that fans from its
  own host can never be counted inline in any mode (an input-safety error, not a
  mode-resolved dimension-attribution concern). Queries that previously returned a
  silently-wrong value now raise.
- Preserve the mode-aware treatment of a partition key that is safe from its host but
  unattributable only from a further (cross-model) root (e.g. `partition_by=status` on
  a `customers`-rooted aggregate): it still errors under `broadcast`/`error` and
  associates under `associate`, unchanged.
- Improve the diagnostic for **both** spellings (path-less and the existing
  path-bearing case) to name the fanning hop via the closure-aware reason, instead of
  the less-useful "unreachable from the aggregate's root".
- Clarify semantics.arc42 Axiom 8 (normative harness) to distinguish the mode-invariant
  safety concern (a partition key whose closure fans from its host) from the mode-aware
  attribution concern (a safe dimension unattributable only from a further root).

Deferred (filed as DEV-1932, Case 1): making the local partitioned aggregate *associate
correctly* under `associate` rather than fail closed. This change only fails it closed.

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `queries/attribution-modes`: add the requirement that a partition key whose dependency
  closure crosses a fanning hop from its host fails closed in every mode; carve the
  existing "unattributable explicit `partition_by=` associates only under associate"
  clause to exclude that fanning-from-host case.

## Impact

- `slayer/engine/join_safety.py` — `assert_partition_key_attributable`: the path-less
  branch and the shared closure-aware reason.
- `architecture/semantics.arc42.md` — Axiom 8 clarification (normative harness; edited
  only with explicit approval of the exact diff).
- Tests: new `tests/test_dev1911_*` fail-closed suite across positions and modes.
- No API, wire-format, or dependency changes. No golden SQL changes for currently-valid
  queries (the affected queries previously produced silently-wrong results and now
  raise).
