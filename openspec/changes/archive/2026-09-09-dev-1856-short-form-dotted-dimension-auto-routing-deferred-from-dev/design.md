## Context

See proposal.md — Why. Every user-typed Mode-B dotted join walk already funnels through one
binder choke point (`binding._walk_join_chain`, reached from `_resolve_dotted`,
`_resolve_dotted_star`, `_bind_agg`, `bind_time_dimension`, and the ORDER BY re-bind).
DEV-1853 (landed) is the substrate: hop tokens resolve through `core/join_walker.resolve_hop`
(edge-name first, then neighbour model name, either traversal orientation; parallel-edge hops
fail closed with `AmbiguousJoinPathError`), `bundle.referenced_models` is the datasource's
connected component (bidirectional closure), `JoinGraph` is the undirected multigraph whose
`count_simple_paths` treats parallel edges as distinct routes and whose `shortest_path` emits
executable token sequences (edge-name token where parallel edges need disambiguation; `None`
over unnamed parallel pairs), and `join_safety.provably_to_one(edge=OrientedJoin, ...)` judges
safety per traversal orientation (a reverse hop carries the inverted cardinality).

## Goals / Non-Goals

- Goal: routing lives at the single binder choke point; result name / type / opaque-guard /
  saved-measure metadata and schema-drift attribution stay consistent with it. Reverse-orientation
  hops are first-class route candidates (DEV-1853), and routing adopts DEV-1853's executability
  discipline wholesale: routed effective paths and suggestions are executable token sequences.
- Non-Goal: Mode-A raw model SQL (`column_expansion.py`) is untouched. Cross-model *expression*
  aggregation (`sum(Consumer.amount + fee)`) stays unsupported.

## Decisions

- **Route in the binder, derive naming from the bound key (Option B) — not a textual pre-pass (P).**
  Aggregation result keys already derive from the bound `AggregateKey` join path
  (`canonical_aggregate_alias`), so routed aggregations get full-path keys for free. P would have to
  re-serialize formulas including star aggregations (`count(Consumer.*)`), which do not round-trip
  through the `ast`-based formula parser — fragile. B keeps a single resolution surface.

- **`_walk_join_chain` returns `(terminal_model, effective_hop_path)`.** Routing triggers only when
  `resolve_hop` returns `None` on the first token (no incident edge in either orientation, no
  edge-name match): `len(hop_path)==1` routes (unique, or the one fan-out-free route among
  ambiguous); `len(hop_path)>=2` is a broken chain (rejected, short-form suggestion when routable).
  `AmbiguousJoinPathError` from a parallel-pair hop propagates untouched — an adjacent-but-ambiguous
  target is DEV-1853's failure, never routed. The distinct
  *edge-resolves-but-target-absent-from-bundle* failure stays `UnknownReferenceError` and is never
  routed. Alternative — a pre-bind routing pass — rejected (would need the mirroring this design
  removes).

- **Ambiguity uses two route counts, `cap=2` each.** The full-graph 0/1/>=2 trichotomy comes from
  the post-1853 `JoinGraph.count_simple_paths` as-is (parallel edges distinct). The fan-out-free
  count runs on a *directed* safe hop set built in the new `dimension_routing` module from
  `join_walker.neighbors` filtered by oriented `provably_to_one` — directed because one orientation
  of an edge may be to-one while the other fans out, which the undirected `JoinGraph` cannot
  express. Both the effective routed path and `suggested_path` follow `JoinGraph`'s
  executable-token discipline (edge-name token across named parallel edges; a hop across unnamed
  parallel edges is not executable, so such a route is never routed or suggested).

- **`BoundExpr.routed_dotted` is a whole-field-only signal.** Set by `bind_expr` /
  `bind_time_dimension` only when the top-level parsed expression is a single `DottedRef` that
  actually routed (`_canonical_if_routed` excludes self-prefix and direct joins). The naming layer
  uses `canonical = bound.routed_dotted or full`, so every non-routed ref — self-prefixed included —
  is byte-identical. Alternative — an original→canonical ref map or parsed-tree rewrite for nested
  refs — rejected as overkill: nested-routed *non-aggregate* measures still emit correct SQL but may
  keep a short auto-alias (documented; user sets an explicit `name`).

- **Saved-measure metadata returns the canonical routed text.** `_resolve_saved_measure_ref` becomes
  routing-aware and returns `(terminal_model, measure, canonical_ref)`; the public-name/type helpers
  consume it. A routing-aware walk alone is insufficient — the caller needs the effective path.

- **Schema-drift routing is datasource-scoped to match query time.** Drift's candidate set is
  `models_by_name` filtered to the stage source's `data_source` (names can collide across
  datasources), so a ref routes identically at drift time and query time.

## Risks / Trade-offs

- [Nested-routed non-aggregate measure keeps a short auto-alias] → documented; SQL is correct; user
  sets `name`. Aggregations unaffected (key-derived alias).
- [Routed result keys may contain edge-name tokens] → matches DEV-1853's named-path result keys
  (`orders.billing_customer.name`); the routed key is the executable path the user could type.
- [DEV-1780 `TestUnboundPathsReject` asserts the reversed decision] → rewritten with consent; the
  infra/`TestCountSimplePaths`/stage-scope/circular tests stay.

## Migration Plan

Additive at query time — no storage migration. Behaviour change is limited to inputs that previously
raised (short forms) now resolving, plus the DEV-1780 test rewrite. Rollback = revert the change.

## Open Questions

None — DEV-1853 (bidirectional traversal, named edges, fail-closed ambiguity) landed and is the
substrate; its executability rules are adopted wholesale rather than re-decided here.
