## Why

DEV-1853 unified how join edges are *read* (`core/join_walker.py`), but about a dozen modules still run their
own BFS / DFS / route enumeration, each with its own policy on parallel edges, cardinality, revisits, the
spine and path spelling — so one graph question gets different answers depending on which walker asks.
Ingesting FK cycles (DEV-2073) makes these divergences common: named 2-cycles, directed 3-cycles,
latest-child pointers.

## What Changes

- One search layer, `slayer/core/join_search.py`, on top of `join_walker.neighbors`: BFS distances and
  reachability with a hop predicate (all / provably to-one), one shortest route with one tie-break (hop count,
  then fanning hops, then canonical tokens), bounded relevance-pruned route enumeration, route counting and
  depth-bounded path listing. Every search obeys the walker's spine rule (a path may end at the spine, never
  cross it) and emits canonical tokens. `engine/join_graph.py` (`JoinGraph`, `min_hops_root`) and the
  per-module copies (`safe_hops`, `_safe_routes`, `_safe_bfs_dist`, `_shortest_safe_route`, `_hop_distances`,
  `_extend_routes`, `to_one_reachable`, `_to_one_hops`, hand-rolled `resolve_hop` loops) are deleted.
- **Revisits are frame-local** (semantics Axiom 1): a revisit is judged within one spelled path — a query
  path from its root, a definition from its declaring model — and composing a definition onto a path is never
  a revisit. Already-resolved key paths are walked with revisits allowed; occurrence identity is the
  canonical path prefix. **BREAKING**: a dotted saved measure whose body crosses back to a model on the
  host chain now composes instead of failing (`_reject_round_trip` is deleted). Definition-default
  cancellation cancels to the nearest matching dataset.
- **Unaddressable parallel edges** (an unnamed edge whose canonical token matches another edge) are skipped
  by route emission with a diagnostic on every searching surface: `recommend_root_model` warnings, the
  short-form routing error reason, population inference reasons, the facade catalog log, the OSI report.
- Recommended and suggested paths spell a single named edge by its name (canonical), and an all-hops
  route prefers fewer fanning hops among equal-length routes. **BREAKING** for consumers parsing those
  strings.
- `recommend_root_model` judges column items with the same probe population inference uses.
- **BREAKING**: both save doors reject an ambiguous hop in the saved model's own Mode-A definitions
  (`Column.sql`, `Column.filter`, model filters); a save that changes the edge set warns about other
  models' Mode-A definitions it breaks (newly ambiguous or unresolvable).
- A user `cypher_filter` runs off the event loop under a fixed timeout.
- Dead code removed: `engine/agg_registry.py`; `filter_reachability.path_is_reachable`,
  `crossed_join_paths`, `compute_key_join_paths`.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/join-traversal`: route enumeration emits canonical, addressable paths with one tie-break and a
  diagnostic for skipped unaddressable edges; searches never cross the spine.
- `models/column-definitions`: revisits are frame-local; an ambiguous hop is rejected at save (no longer
  skipped); edge-set changes warn about broken dependent definitions.
- `queries/saved-measures`: round-trip rejection removed; dotted saved measures bind in their declaring
  frame and compose.
- `queries/dotted-dimension-routing`: routed and suggested paths are canonical; suggestion tie-break counts
  fanning hops; unaddressable-edge diagnostics.
- `queries/population`: directed-cycle inference scenario; recommendation shares the inference probe and
  reports unaddressable edges.
- `mcp/tool-surface`: `cypher_filter` execution is time-bounded.

## Impact

- Code: `slayer/core/join_walker.py`, new `slayer/core/join_search.py`; `slayer/engine/` (`dimension_routing`,
  `join_safety`, `population`, `binding`, `bind_inputs`, `param_binding`, `reference_closure`,
  `column_dependency`, `schema_drift`, `bundle_builder`, `ranked_planner`, `home`, `key_metadata`,
  `response_meta`, `compile/stages`, `query_engine`, `filter_reachability`; `join_graph`/`agg_registry`
  deleted); `slayer/sql/` (`column_expansion`, `scope`, `generator`); `slayer/ir/prebound.py`;
  `slayer/storage/base.py`; `slayer/facade/catalog.py`; `slayer/osi/converter.py`;
  `slayer/memories/resolver.py`; `slayer/search/graph.py`.
- arc42: semantics Axiom 1 gains the frame-local revisit clause (approved, applied).
- No new import arrows: the new module lives in `core`.
- Docs: joins, saved measures, column definitions.
