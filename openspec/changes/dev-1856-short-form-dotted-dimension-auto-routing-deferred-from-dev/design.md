## Context

See proposal.md — Why. Every user-typed Mode-B dotted join walk already funnels through one
binder choke point (`binding._walk_join_chain`, reached from `_resolve_dotted`,
`_resolve_dotted_star`, `_bind_agg`, `bind_time_dimension`, and the ORDER BY re-bind).
`bundle.referenced_models` is already the transitive datasource-scoped join closure
(`_collect_referenced_models`). The routing primitives (`JoinGraph.count_simple_paths` /
`shortest_path`) and the fan-out predicate (`join_safety.provably_to_one`) already exist and are
retained-but-unused. INNER reverse edges carry `invert_cardinality`, so `provably_to_one` reads
accurate per-direction arity.

## Goals / Non-Goals

- Goal: routing lives at the single binder choke point; result name / type / opaque-guard /
  saved-measure metadata and schema-drift attribution stay consistent with it.
- Non-Goal: Mode-A raw model SQL (`column_expansion.py`) is untouched. Cross-model *expression*
  aggregation (`sum(Consumer.amount + fee)`) stays unsupported. Bidirectional LEFT-join traversal is
  DEV-1853's concern (a review note is filed there).

## Decisions

- **Route in the binder, derive naming from the bound key (Option B) — not a textual pre-pass (P).**
  Aggregation result keys already derive from the bound `AggregateKey` join path
  (`canonical_aggregate_alias`), so routed aggregations get full-path keys for free. P would have to
  re-serialize formulas including star aggregations (`count(Consumer.*)`), which do not round-trip
  through the `ast`-based formula parser — fragile. B keeps a single resolution surface.

- **`_walk_join_chain` returns `(terminal_model, effective_hop_path)`.** On the first hop with *no
  declared join*: `len(hop_path)==1` routes (unique, or the one fan-out-free route among ambiguous);
  `len(hop_path)>=2` is a broken chain (rejected, short-form suggestion when routable). The distinct
  *declared-join-but-target-absent-from-bundle* failure stays `UnknownReferenceError` and is never
  routed. Alternative — a pre-bind routing pass — rejected (would need the mirroring this design
  removes).

- **Ambiguity uses two graphs, `count_simple_paths(cap=2)` on each.** Full graph gives 0/1/>=2; a
  `provably_to_one`-filtered subgraph gives the fan-out-free count. `cap=2` suffices (only the 0/1/>=2
  trichotomy is ever needed). The safe subgraph is built in the new `dimension_routing` module so
  `join_graph.py` stays dependency-light.

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
- [Duplicate joins to one target collapse to a single graph edge] → pre-existing first-wins walk
  behaviour, unchanged; belongs to DEV-1853's two-FK ambiguity policy. Noted, not fixed here.
- [DEV-1780 `TestUnboundPathsReject` asserts the reversed decision] → rewritten with consent; the
  infra/`TestCountSimplePaths`/stage-scope/circular tests stay.

## Migration Plan

Additive at query time — no storage migration. Behaviour change is limited to inputs that previously
raised (short forms) now resolving, plus the DEV-1780 test rewrite. Rollback = revert the change.

## Open Questions

None — the LEFT-join-reversibility interaction is deferred to DEV-1853 by an explicit review note, not
left open here.
