# Tasks — bidirectional join traversal

## 1. Walker substrate (core)

- [x] 1.1 Add `ModelJoin.name` (optional, model-name identifier rules) and
      `AmbiguousJoinPathError` (candidates: declaring model, name, pairs,
      cardinality; remediation text); verify with model-validation unit tests.
- [x] 1.2 Create `slayer/core/join_walker.py`: `OrientedJoin`, `edges_between`,
      `neighbors`, `resolve_hop` (name-then-model token resolution, `None` on miss,
      raises on ambiguity), `walk` with visited-guard; verify with a dedicated unit
      suite covering orientation, symmetry, naming, ambiguity, and cycle rejection.
- [x] 1.3 Make `provably_to_one`/`safe_reachable` operate on `OrientedJoin`
      (declared prong = oriented cardinality; structural prong = traversal-target
      side); verify inverted 1:m→m:1 proof and inverted m:1→1:m fan-out by unit
      tests.

## 2. Consumers

- [x] 2.1 Route `binding._walk_join_chain` (+ edge-name tokens, Mode B) and
      `column_expansion._walk_exact` (Mode A) through the walker; verify reverse
      dotted refs and named hops bind in both expression layers.
- [x] 2.2 Route `prebound.walk_key_path` and the reroot machinery through the
      walker; delete `resolve_correlation_hop` and `_reverse_hops` edge-finding;
      verify `test_dev1836_reverse_hop_reroot.py` reworked to forward-only fixtures
      passes and no parallel traversal logic remains (grep gate in tests).
- [x] 2.3 Spine building in `sql/generator._build_from_and_joins` resolves hops via
      the walker, emits oriented ON pairs and root-relative join type (LEFT in
      traversal direction, INNER symmetric, never RIGHT); verify by generator
      goldens for reverse LEFT/INNER emission.
- [x] 2.4 Resolved-hop-chain metadata for path consumers: `response_meta`,
      `sql/scope`, generator time/dimension ownership, `ranked_planner`,
      `stage_planner` walks read terminal-model identity from the resolved chain,
      never re-parsed tokens; verify named-edge tests across response metadata, time
      dimensions, ranking, filters, reroot, and multi-stage SQL.
- [x] 2.5 Rebuild `JoinGraph` edge-aware (parallel edges = distinct routes;
      executable `shortest_path` with edge-name tokens; fail-closed recommendation);
      verify `recommend_root_model` suites incl. an ambiguous-pair fixture.
- [x] 2.6 Widen BFS closures (`source_bundle`, `column_dependency`, `agg_registry`,
      `query_engine._expand_join_graph`, `schema_drift`, `memories/resolver`) to the
      bidirectional component; verify reverse-hop binding, dotted saved-measure
      refs over reverse hops, and drift attribution tests.
- [x] 2.7 Ambiguous filter-pushdown correlation hops raise (both modes) instead of
      drop+warn; verify the reworked dev1747 ambiguous-drop fixture and strict-mode
      tests.
- [x] 2.8 Inspect/search surfaces list both orientations (`model_render`,
      `collection_render`, search `joins_to`); verify rendering snapshots/goldens.

## 3. Retirement, migration, validation, importers

- [x] 3.1 Inventory and delete `JoinSyncStorage`: module, `_wrap_join_sync` wiring,
      reconciliation, `search/graph.py` import, unwrap/type-checks, dbt
      `_mirror_inner_joins`, osi one-edge-per-target notes; verify the full unit
      suite imports and passes with the wrapper gone.
- [x] 3.2 `SlayerModel` v10: no-op per-doc bump + cross-document exact-inverse dedup
      over raw dicts in `_migrate_and_refine_on_load` (order-independent identity,
      to-one-side keep rule, tiebreak, missing-peer no-op, persisted); verify
      migration tests: dedup, drifted pair kept, either-load-order, idempotence,
      YAML + SQLite.
- [x] 3.3 Save-time validation: reject exact-inverse declarations and name
      collisions (model names, incident-edge duplicates); warn on unnamed parallel
      edges; audit reports both orientations per edge; verify validation unit tests.
- [x] 3.4 Cube importer dedups mutually-inverse declarations (contradictions import
      both); verify importer tests.

## 4. Parity, divergences, docs

- [x] 4.1 Mirror-parity suite: mirrored fixtures stripped to forward halves answer
      identically (reachability, SQL byte-identical on unnamed paths, executed
      values); verify suite green.
- [x] 4.2 D10 `divergences.md` ledger: enumerate value flips (broadcast→exact over
      inverted provable hops), error flips (first-match→fail-closed, drop+warn→
      error, flipped `test_dev1780` cases), dimension-level SQL/value cases
      (selected/grouped dims, filters, null buckets, reverse-INNER row removal,
      multi-hop), recommendation growth — with before/after values, mode
      vocabulary; verify every flipped test cites its ledger entry.
- [x] 4.3 Tier-1 integration coverage of a reverse-hop query (sqlite, duckdb,
      postgres at minimum); verify integration runs.
- [x] 4.4 Docs: `docs/concepts/models.md` joins section rewrite, `references.md`
      edge-name segments, `docs/cube/cube_import.md` dedup, `.claude/skills/`
      updates, CLAUDE.md conventions line; verify docs grep clean of retired
      concepts (`join_sync`, declared reverse join).
- [x] 4.5 Delete `tests/test_join_sync.py` + `tests/test_join_cardinality_mirror.py`
      (approved), re-bless affected goldens; run the full non-integration suite +
      ruff + the architecture enforcement bundle (lint-imports, arch_check, likec4
      validate, basedpyright); verify all green with no new baseline entries.
