# Bidirectional join traversal: retire declared reverse joins

## Why

A stored `orders → customers (many_to_one)` join already carries everything needed to
traverse `customers → orders` — swap the pairs, flip the cardinality — yet reverse
traversal today exists only where `join_sync` mirrors INNER joins (LEFT joins get no
mirror) plus a narrowly-scoped EXISTS-correlation inversion helper (DEV-1840). Three
unshared mechanisms, silent first-match on parallel edges, and physically duplicated
mirror edges that drift when one half is edited.

## What Changes

- A join becomes a **symmetric edge**: any declared join traverses in either direction;
  traversal orients `join_pairs` and flips `cardinality`; which model declares the edge
  is storage trivia. One shared walker (`slayer/core/join_walker.py`) serves binding,
  reroot, safety, spine building, route enumeration, and BFS closures.
- **BREAKING**: a hop connected by two or more edges fails closed (new
  `AmbiguousJoinPathError` naming the candidates) in BOTH directions — the silent
  first-match on parallel forward edges is retired.
- **BREAKING**: an ambiguous filter-pushdown correlation path errors instead of
  drop+warn.
- Join type becomes root-relative: LEFT emits LEFT in the traversal direction; INNER is
  symmetric; RIGHT is never emitted.
- Oriented cardinality participates in safety: an inverted declared `one_to_many` is a
  provable `many_to_one` (DEV-1840's "inversion never classifies safe" rule is
  repealed); inverted fan-out orientations feed the broadcast machinery unchanged.
- Optional edge `name`, usable as a path segment in either direction — the
  disambiguator for parallel edges.
- `join_sync` mirroring is retired (storage wrapper, dbt copy, reconciliation);
  a `SlayerModel` v10 load-path migration dedups exact-inverse stored pairs;
  save-time validation rejects newly declared exact inverses; the Cube importer dedups
  mutually-inverse declarations.
- `JoinGraph` becomes edge-aware (parallel edges are distinct routes);
  `recommend_root_model` emits executable paths (edge-name tokens where needed) and
  fails closed when none exists. Inspect/search surfaces list both orientations.
- Value/SQL divergences enumerated in `divergences.md` per the D10 protocol, using the
  DEV-1841 mode vocabulary for newly reachable unattributable dimensions.

## Capabilities

### New Capabilities

- `models/join-traversal`: symmetric-edge traversal — orientation and cardinality flip,
  root-relative join type, fail-closed ambiguity, edge names as path segments,
  bare-model-name uniqueness rule, mirror dedup migration, importer dedup.

### Modified Capabilities

- `models/join-cardinality`: arity is proven on the *oriented* edge; the "No synthesized
  traversal" scenario inverts into bidirectional traversal; Cube import gains the
  mutually-inverse dedup scenario; validation gains exact-inverse rejection, edge-name
  rules, and the unnamed-parallel-edges warning; the safety audit reports both
  orientations.
- `queries/cross-model-aggregates`: reverse-path resolution goes through the shared
  walker (the semi-join-only inversion carve-out disappears); an ambiguous correlation
  path fails closed instead of dropping the conjunct with a warning.
- `queries/saved-measures`: round-trip wording no longer presumes a declared reverse
  join.

## Impact

- `slayer/core`: new `join_walker.py`, `ModelJoin.name`, `AmbiguousJoinPathError`.
- `slayer/engine`: `binding`, `prebound`, `join_safety` (delete
  `resolve_correlation_hop`), `stage_planner` (delete `_reverse_hops` special-casing),
  `join_graph` (multigraph), `query_engine`, `source_bundle`, `column_dependency`,
  `agg_registry`, `column_expansion`, `ranked_planner`, `response_meta`,
  `schema_drift`, `memories/resolver` — path consumers read resolved hop identity, not
  re-parsed tokens.
- `slayer/sql`: `generator` spine building via the walker; `scope` terminal-model
  resolution.
- `slayer/storage`: delete `join_sync.py` + wiring; v10 migration; save-time join
  validation. `slayer/search/graph.py` drops its `JoinSyncStorage` import.
- `slayer/dbt`, `slayer/cube`, `slayer/osi`: mirror logic deleted / dedup added.
- Docs (`docs/concepts/models.md`, `references.md`, `docs/cube/cube_import.md`),
  `.claude/skills/slayer-models.md` + `slayer-query.md`, CLAUDE.md conventions line.
- Tests: `test_join_sync.py` and `test_join_cardinality_mirror.py` deleted;
  `test_dev1836_reverse_hop_reroot.py` fixtures reworked; flipped `test_dev1780` cases
  updated; goldens re-blessed (all approved).
- RLS is untouched (post-generation physical-table transform).
