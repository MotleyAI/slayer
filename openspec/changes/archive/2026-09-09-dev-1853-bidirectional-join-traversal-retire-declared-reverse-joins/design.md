# Design — bidirectional join traversal

## Context

Reverse traversal exists today as three unshared mechanisms: `join_sync` storage
mirroring of INNER joins (`slayer/storage/join_sync.py`, wired for every backend in
`storage/base.py`; a private copy in `slayer/dbt/converter.py`), DEV-1840's
EXISTS-correlation inversion helper (`join_safety.resolve_correlation_hop`, sole call
site `stage_planner._reverse_hops`), and RLS's physical-hop flip (out of scope — RLS
is a post-generation physical-table transform). Every walker keys edges by
`target_model` and silently picks the first match. The architecture layer
(`architecture/`) forbids new `sql → engine` imports and the SQL generator is a
walker consumer, so the walker must sit in `core`. See proposal.md for motivation.

## Goals / Non-Goals

**Goals:** one traversal substrate whose answers are independent of which side
declares an edge; parity with the mirrored graph; fail-closed ambiguity everywhere;
an API that already serves DEV-1866 (determination checks over oriented hops) and
DEV-1856 (route enumeration).

**Non-Goals:** short-form auto-routing (DEV-1856); mode-axis vocabulary (DEV-1841);
RLS unification; lazy bundle loading (closure growth is accepted and noted below);
any change to `__`-alias emission.

## Decisions

- **D1 — Symmetric edges, no precedence (user-decided).** For hop X→Y the candidates
  are ALL edges connecting X and Y, whichever side declares them; ≥2 candidates fail
  closed via `AmbiguousJoinPathError` in both directions. The silent forward
  first-match is retired. Rejected alternative: stored-beats-inverted precedence —
  contradicts the symmetric-edge model and hides drifted mirrors.
- **D2 — Root-relative join type (user-decided).** LEFT = keep the root side whole,
  emitted in traversal direction; INNER = matched pairs only, symmetric (preserves
  mirrored-INNER behavior). Rejected: algebraic inversion (reverse of LEFT = RIGHT) —
  drops root rows and injects ghost rows, violating the cardinality invariant
  (arc42 principle 8).
- **D3 — Walker in `slayer/core/join_walker.py`.** `OrientedJoin` (frozen Pydantic:
  traversal source/target, oriented pairs, oriented cardinality, join type, name,
  declaring model) with two faces: total enumeration (`edges_between`, `neighbors` —
  never error; route counting, BFS closures, rendering) and strict resolution
  (`resolve_hop` — token matches edge name or opposite-endpoint model; `None` when
  absent so callers keep their own errors; raises on ambiguity) plus `walk` with the
  existing visited-guard. Placement follows arc42 principle 1 (no new `sql → engine`
  edge); DEV-1872 later absorbs it into `slayer/ir`.
- **D4 — Oriented provability.** `provably_to_one` takes the oriented edge: declared
  prong uses the oriented cardinality, structural prong checks traversal-target-side
  columns against that model's unique sets. DEV-1840's "inversion never classifies
  safe" carve-out is repealed; `resolve_correlation_hop` and `_reverse_hops`'s
  edge-finding are deleted, subsumed by the walker.
- **D5 — Edge names as path tokens (user-decided).** Optional `name` on `ModelJoin`,
  always usable, direction-agnostic. Token resolution order within a hop: edge name,
  then opposite-model name. Path consumers stop re-parsing tokens as model names:
  bound/planned metadata carries the resolved hop chain (terminal-model identity per
  path), consumed by response metadata, scope resolution, time-dimension ownership,
  ranked planning, and stage planning (Codex finding 1).
- **D6 — Edge-aware `JoinGraph`.** Adjacency stores edges, not name sets; parallel
  edges are distinct routes; `shortest_path`/recommendation emit executable paths
  (edge-name tokens where needed) or report unreachable (Codex finding 2).
- **D7 — Migration over raw dicts.** v10 dedup runs in the load path against raw
  peer documents (no recursive migrated loads), with order-independent edge identity
  and tiebreak: keep the to-one side, else the cardinality-carrying side, else
  lexicographic `(model, target)`. Missing/corrupt peers leave the document
  untouched. Both backends persist the result; repeated loads are no-ops
  (Codex finding 3).
- **D8 — `JoinSyncStorage` retirement is inventoried.** All importers of the wrapper
  (`search/graph.py`, storage sampling/search tests) move to backend-neutral
  behavior; unwrap/type-checks deleted with the module (Codex finding 4).
- **D9 — Audit reports both orientations** — one finding per edge with per-orientation
  provability, so planner classifications of inverted hops are explainable
  (Codex finding 5).
- **D10 — Divergence protocol** (DEV-1836 lineage): every behavior flip enumerated in
  `divergences.md` for approval; previously-mirrored INNER traversals stay
  byte-identical on unnamed paths (golden tripwire; named paths get semantic
  equivalence — Codex finding 7); dimension-level value/SQL divergences enumerated
  explicitly (selected/grouped dimensions, filters, null buckets, reverse-INNER row
  removal, multi-hop), using the DEV-1841 mode vocabulary for newly reachable
  unattributable dimensions.

## Risks / Trade-offs

- [Bundle/BFS closures grow to the datasource connected component] → accepted;
  measured only via existing perf-sensitive tests; lazy loading deferred.
- [Queries relying on silent first-match or drop+warn start erroring] → D10 ledger
  enumerates each flip; errors carry candidate edges and remediation.
- [Named-path tokens leak into a consumer that still assumes model names] → D5
  resolved-chain metadata plus named-edge tests across response metadata, time
  dimensions, ranking, filters, reroot, multi-stage SQL.
- [Migration mis-classifies a deliberate pair as a mirror] → exact-inverse test is
  strict (swapped pair-set, same type, inversion-consistent cardinality); anything
  else is kept and fails closed at traversal.

## Migration Plan

`SlayerModel` version 9 → 10: per-doc no-op bump registered in
`storage/migrations.py`; the cross-document dedup runs in
`_migrate_and_refine_on_load` (v9 precedent) per D7 and persists. Rollback: the
surviving edge is semantically complete, so pre-change code still reads it — only
mirrored reverse traversal (INNER pairs) would regress on rollback.

## Open Questions

None — interview and Codex review resolved all forks.
