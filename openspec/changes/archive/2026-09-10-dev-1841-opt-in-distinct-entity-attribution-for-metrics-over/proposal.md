# Proposal: to_many_handling — uniform attribution modes for unattributable dimensions

## Why

A metric sliced by a dimension its root cannot attribute (the path crosses a fanning or
unproven hop) today either broadcasts with a warning (cross-model) or — for local
aggregates, silently join-multiplies its inputs, violating the no-double-counting axiom
(verified: a customer with two same-status orders has their spend counted twice, no
warning). There is no way to ask for the semantically exact alternative: per-cell
aggregation over the distinct root entities associated with each cell. DEV-1841 closes
axiom 8's mode axis now that DEV-1853's bidirectional traversal supplies the association
substrate.

## What Changes

- New query-level field `to_many_handling: "broadcast" | "associate" | "error"`
  (default `"broadcast"`) governing how EVERY aggregate — cross-model and local —
  resolves query dimensions unattributable from its root.
- **BREAKING**: `SlayerQuery.strict` is retired. Query input containing `strict` fails
  with a typed error naming `to_many_handling="error"`; a storage migration
  (SlayerQuery v3→v4) rewrites stored queries. REST and MCP surfaces swap the parameter.
- `associate` mode: a new host-rooted two-level association producer computes exact
  per-cell values over distinct root entities (dedup by the root's unique key), for the
  full plain scalar aggregate family; `window=`/`first`/`last` combinations are a typed
  error. Explicit `partition_by=` naming an unattributable key becomes legal in
  `associate` (attributes at the declared grain).
- **BREAKING**: local aggregates sliced by fanning dimensions stop silently
  join-multiplying — they route through the same attribution classification (broadcast
  by default, with the warning), in every consumer context (measures, composites,
  filters, order, computed dimensions, nested producers).
- Warnings both ways: broadcast entries gain an unconditional dice–slice hint;
  associated entries warn about overlapping, non-additive cell populations;
  EXISTS-pushed filters gain a response-only informational entry (amends DEV-1840's
  silence); the broadcast reason for reachable-but-fanning dimensions is corrected
  (currently claims "unreachable").
- Filters keep EXISTS semi-join pushdown unconditionally in every mode; the association
  producer routes filters by the existing producer filter-routing rules with root =
  host. Ambiguous hops keep failing closed in both modes.
- `architecture/semantics.arc42.md`: axiom 8 reworded (per-aggregate override dropped)
  and its `[target: DEV-1841]` flipped to enforced test ids; law 5 (dice–slice)
  likewise.

## Capabilities

### New Capabilities

- `queries/attribution-modes`: the query-level `to_many_handling` knob — enum values
  and default, per-surface exposure (core/REST/MCP), `strict` retirement and storage
  migration, `error`-mode scope (broadcasts and excluded filters), association-mode
  eligibility and its typed unsupported-combination errors.

### Modified Capabilities

- `queries/semantics`: attribution-by-determination becomes mode-dependent;
  no-double-counting extends beyond producers to every aggregate reference (closing the
  local base-path hole); loud degradation gains the dice–slice hint and the associated
  warning; a dice–slice requirement is added for `associate` mode.
- `queries/cross-model-aggregates`: fan-out-safe grain becomes mode-dependent;
  broadcast metadata gains the hint field and the corrected fanning reason; the strict
  mode requirement is removed (superseded by `error` mode); explicit grain gains the
  `associate` exception; producer filter routing's "silently and without metadata"
  becomes the pushed-filter metadata entry.
- `queries/partitioned-aggregates`: the partition-key attributability requirement gains
  the `associate` exception.

## Impact

- `slayer/core`: `SlayerQuery` field swap; new warning payload kinds; typed errors.
- `slayer/engine`: attribution classification extended to local aggregates in all
  consumer contexts; new association producer kernel planning; mode threading through
  `PreboundQuery`/`StrictQueryCarrier` into nested producer planning; strict gate
  replaced by the `error`-mode gate.
- `slayer/sql`: emission of the association kernel (plain nested GROUP BY, portable to
  every supported dialect; no correlated subqueries, no ClickHouse version gate).
- `slayer/storage`: SlayerQuery v3→v4 migration.
- `slayer/api`, `slayer/mcp`: parameter swap.
- `architecture/index.yaml`: `queries` cross-cutting spec gains `storage` in `touches`.
- Tests: dev-1853 pinned local-shape test rewritten on richer data (approved);
  dev-1840 metadata-silence assertions updated (approved); local-shape goldens
  re-blessed; new association goldens across all dialect emission paths.
