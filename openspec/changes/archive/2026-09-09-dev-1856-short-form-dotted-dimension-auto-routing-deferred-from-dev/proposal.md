## Why

A Mode-B dotted reference that names only a target model and column (`Consumer.name`) is
rejected today, even when there is exactly one join route to that model. DEV-1780 shipped
the routing infrastructure (`JoinGraph.count_simple_paths`, `UnresolvableDimensionJoinError`)
but deliberately parked the feature under a strict-rejection-only decision. This change turns
the feature on: a short form auto-resolves to its full join path when that path is
determinable, and fails with a route-aware error otherwise.

## What Changes

- A Mode-B dotted ref whose first hop resolves to no incident edge (either orientation,
  DEV-1853) is **auto-routed** to the full path when the target is reached by exactly one
  route, or — among several routes — by exactly one fan-out-free route (oriented
  `provably_to_one` on every hop). Routes are counted over the bidirectional multigraph:
  reverse hops are candidates and parallel edges are distinct routes.
- Routed and suggested paths are **executable token sequences** (edge-name tokens across named
  parallel edges; a route crossing an unnamed parallel pair is never routed or suggested). A
  target directly adjacent via parallel edges stays DEV-1853's fail-closed
  `AmbiguousJoinPathError` — routing never sees it.
- Ambiguous (>=2 routes, not uniquely fan-out-free) and unreachable targets are rejected with
  `UnresolvableDimensionJoinError`; an ambiguous rejection carries a route-aware `suggested_path` when an
  executable full path exists, while an unreachable target (or one whose only routes cross an unnamed
  parallel pair) carries none.
- A **broken explicit chain** (>=2 hops with an unresolvable hop) is never auto-fixed; it is
  rejected and suggests the short form when the target is uniquely routable.
- Routing is uniform across dimensions, time dimensions, cross-model measures/aggregations,
  star aggregations, query filters, and ORDER BY. For dimensions and time dimensions the
  **result column key is the full routed path**, not the short form typed.
- Routing is **datasource-scoped** and **deferred past named-query stage boundaries** (a
  dotted ref in a downstream `StageSchema` stays `IllegalScopeReferenceError`).
- Schema-drift attribution and inline saved-measure naming/type become routing-aware so
  persisted short forms are tracked and surfaced under their full-path name.
- **BREAKING** (relative to the DEV-1780 strict-reject decision, not to any released spec):
  short forms that previously errored now resolve; the DEV-1780 `TestUnboundPathsReject`
  behaviour is superseded.

## Capabilities

### New Capabilities
- `queries/dotted-dimension-routing`: auto-resolution of a short-form dotted reference
  (target model + column) to its full datasource-scoped join path, the fan-out-free
  tie-break among ambiguous routes, the route-aware rejection taxonomy, and the uniform
  application across every Mode-B surface including result-key, saved-measure naming, and
  schema-drift attribution.

### Modified Capabilities
<!-- none: no existing corpus requirement mandates direct-join-only resolution; the
     strict-rejection was a retired DECISIONS.md/DEV-1780 decision, not a spec. Short forms
     are a new input shape the saved-measures / cross-model requirements never covered. -->

## Impact

- `slayer/engine/dimension_routing.py` (new pure module), `slayer/engine/binding.py`
  (`_walk_join_chain` routing + `BoundExpr.routed_dotted`), `slayer/engine/stage_planner.py`
  (dimension/time-dim/saved-measure naming derives the routed path), `slayer/engine/schema_drift.py`
  (routing-aware attribution).
- Reuses existing infra: post-1853 `JoinGraph` (multigraph counts, executable-token
  `shortest_path`), `core/join_walker` (`resolve_hop` / `neighbors` / `OrientedJoin`), oriented
  `join_safety.provably_to_one`, `UnresolvableDimensionJoinError`, the
  `_collect_referenced_models` bidirectional datasource closure.
- Docs: `docs/concepts/references.md`. Tests: new routing suites + rewrite of the DEV-1780
  strict-reject cases.
