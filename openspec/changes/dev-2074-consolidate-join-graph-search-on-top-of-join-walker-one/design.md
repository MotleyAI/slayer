## Context

`core/join_walker.py` is the one edge reader (`neighbors`, `edges_between`, `resolve_hop`, `walk`). On top of it,
the search algorithms were re-implemented per module (see proposal.md › Why); the audit found six BFS /
shortest-route copies, three route enumerators with different "ambiguous" meanings, three token rules, two
spine policies and a dozen hand-rolled `resolve_hop` loops. Principles in force: system P1–P4 (layering and
model truth — the new module lives in `core`, so no new arrow), P9 (canonical paths), core P5 (structured
warnings), engine P3 / P10, semantics Axioms 1, 2.4, 12 and 15.

## Goals / Non-Goals

**Goals:** one search layer every walker calls; one addressability rule; one tie-break; one spine rule; one
revisit rule (frame-local); one save-time ambiguity verdict shared by both save doors.

**Non-Goals:** cross-call adjacency caching (`neighbors` stays O(models) per call; only per-search memoising);
self-referencing FKs (ingestion still skips them); naming ingested parallel edges (DEV-2073); changing
literal-edge-first resolution; save-time binding of Mode-B saved-measure formulas; the search graph's JOINS
edge export; `join_walker._detached_names` (the substrate's own bootstrap, which `neighbors` calls);
`storage/base._dunder_chain_is_join_walk` (a raw-dict legacy migration); `osi/converter._fact_root` (a
declared-direction FK reading, not a traversal).

## Decisions

**D1 — `slayer/core/join_search.py` beside the walker, not inside `join_walker.py`.** The walker answers "what
does this token mean"; search answers "which paths exist". Keeping them apart keeps the strict resolution
face small. Alternative (one module) rejected: the walker file would mix resolution and search again.
API: `HopFilter` (`ALL`, `TO_ONE`); `addressable_hops`, `unaddressable_pairs` → `UnaddressablePair`;
`distances` / `reachable`; `shortest_route` / `shortest_routes` → `Route` (frozen pydantic: oriented
`edges`, canonical `tokens`, `fanning`); `routes(..., cap, shortest_only)`; `count_routes`; `paths_from`.
Adjacency is memoised per search call (one `neighbors` per model).

**D2 — Addressable = the canonical token resolves back to exactly that edge.** One rule replaces
`JoinGraph._executable_tokens`, `safe_hops` and `_to_one_hops` (which disagreed on tokens and order). It
makes emitted paths canonical (P9) and makes the facade's "is this token the same edge" gap impossible.
`count_routes` alone counts every edge (spec: parallel edges are distinct routes); every emitting search walks
addressable hops only.

**D3 — Tie-break (hops, fanning hops, canonical tokens); preference for fan-out-free routes is a predicate
choice.** `suggested_path` = `shortest_route(TO_ONE)` else `shortest_route(ALL)`. Alternative (fanning first)
rejected in the interview: it duplicates the predicate and makes hop totals stop being distances.

**D4 — Spine rule = the walker's: a route may end at the spine, never cross it.** Applied by the search layer
itself, so no caller re-implements a sink. Spine routes keep Axiom 15's shortest-only ambiguity
(`routes(shortest_only=True)`); short-form routes keep the spec's all-simple-routes ambiguity.

**D5 — Frame-local revisits: two walk faces, chosen by name, never by flag.** `walk` / `terminal_model` stay
strict (raise `CircularJoinPathError` on a revisit) and serve spelling sites: binding of query-typed paths,
population probing, `_resolve_qualifiers` from a definition's owner, the save validators, saved-measure
binding in the host frame, `walk_cancelling`, typed-reference attribution in schema drift and memories.
New `follow` / `resolved_terminal` walk an already-resolved, possibly composed key path with revisits
allowed and serve every consumer of bound `ValueKey.path` / `source_anchor_path`. Separate names make the
choice explicit at each call site (a forgotten flag would silently reproduce today's `None`). Occurrence
identity is the canonical path prefix (Axiom 2: datasets are the root and the paths from it) — the SQL alias
already is; no caller may key per-occurrence state by model name.

**D6 — Cancellation searches the stack deepest-first.** Axiom 2.4's precedence is unchanged (incident edge
name → cancel → model-name hop); with composed owner paths a dataset can appear twice, and the innermost
occurrence is the one the definition's frame means. `_owner_stack` becomes `follow` over the trusted owner
path.

**D7 — Save-time ambiguity verdict.** The storage door's `validate_derived_columns` and the engine door share
one strict resolution of each Mode-A definition (`Column.sql`, `Column.filter`, model `filters`) from its
declaring model; ambiguity raises `DerivedColumnAmbiguousError(AmbiguousJoinPathError)` (model filters: the
base error naming the filter). Unloaded / unresolved stays skipped at the storage door, as specified.

**D8 — Dependent warnings by pre/post comparison.** When the saved model's edge set differs from the stored
version, each other model's Mode-A definition is resolved under the stored graph and under the proposed
graph; a resolved → ambiguous or resolved → unresolvable transition emits one `UserWarning` (the channel
`_warn_unnamed_parallel_edges` uses). Judging only the post-save graph cannot tell which pair a now-ambiguous
reference crossed.

**D9 — `recommend_root_model` column viability = `population.probe_item`.** The population spec already
requires one rule; `to_one_reachable` diverged (no literal-first, no route ambiguity). Recommend keeps its
deterministic tie pick, since a recommendation must recommend.

**D10 — Cypher filter isolation.** The query timeout is set once when the cached connection is created; a
per-cache-entry lock covers execute + full drain inside one `asyncio.to_thread` call and is also taken by
cache rebuild/close. Alternative (connection per call) rejected: rebuilding the graph per filter is the cost
the cache exists to avoid.

## Risks / Trade-offs

- [Composed-frame saved-measure values were the original reason `_reject_round_trip` existed] → the
  `pr-tests` stage proves the spec's values with executed oracles on SQLite and DuckDB before implementation;
  a wrong value is a plan problem and stops the flow, never a reason to re-add a refusal.
- [A missed consumer of composed paths silently degrades type / home / ranking] → task 2.x audits every
  `walk`, `terminal_model` and `resolve_hop` caller into strict or composed, and the frame-local tests assert
  type, format, home, broadcast warning, time-dimension ownership, drift attribution and cache-key equality.
- [Canonical tokens change recommended / suggested path strings] → **BREAKING** noted in the proposal; paths
  remain executable.
- [Save rejection of ambiguous definitions can fail an importer's save] → such a definition fails at every
  query anyway; the error names the remedy.
- [Dependent re-check cost] → runs only when the edge set changed.
