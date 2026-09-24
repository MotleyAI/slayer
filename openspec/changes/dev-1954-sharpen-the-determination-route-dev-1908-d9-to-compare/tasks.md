## 1. Tests first (spec-tests stage)

- [x] 1.1 Write `tests/test_dev1954_canonical_paths.py` (unit tier; seeded in-process SQLite/DuckDB) covering: `canonical_path` (named/unnamed, both orientations, empty chain, multi-hop); bound keys for `ColumnKey`/`ColumnSqlKey`/`StarKey`/`TimeTruncKey` spelled by model name → canonical; filter + ORDER BY (incl. aggregate filter, functional ORDER BY on a time dim) intern to the SAME key/slot/join as the selected other spelling; saved measures (bare, self-prefixed, model-name, reverse-hop, auto-routed; with/without `name`); `Column.sql` + model filter spelled by model name → one join alias; definition default via `walk_cancelling`; `_lenient_path` over raw `customers.regions.x`, raw `customers.hr.x`, expanded `customers__regions.x`, and a fitted/mangled alias (no invented path); every `join-traversal` scenario with seeded-data values and no `BroadcastGrainWarning`; response metadata (`columns`, `attributes.dimensions`, `attributes.measures`) for both spellings incl. a named-edge time dim and a saved measure; schema-drift attribution of `regions`/`hr`/auto-routed forms to the same terminal (ambiguous → nothing); cache twin (one DB execution); D9 unit pin — the issue's repro with canonical spelling is attributable and reroots to the suffix. Verify: all fail (or error) on the current tree for the right reason.
- [x] 1.2 Write `tests/test_dev1954_stale_spelling.py` for every `dotted-dimension-routing` ADDED scenario: multistage downstream stale reference, query-backed consumer (query + other model's `Column.sql`), edge named later, ambiguous respelling fails, exact-name shadowing wins, one warning per position, cache-hit warning recombination both orders. Verify: all fail on the current tree for the right reason.
- [x] 1.3 Identify existing tests/goldens asserting typed-spelling result keys or model-name join aliases for named edges (e.g. `tests/test_dev1853_named_edges.py`); list each assertion change for the user's explicit OK before editing.

## 2. Canonical resolution

- [x] 2.1 Add `join_walker.canonical_path(chain)`; make `walk_cancelling` emit canonical tokens. Verify: 1.1 walker/default tests pass.
- [x] 2.2 Canonicalize `binding._walk_join_chain` (incl. auto-routing) and the star path; adjust `_canonical_if_routed` to fire on respelling. Verify: 1.1 binding/naming tests pass.
- [x] 2.3 Canonicalize `bind_inputs._resolve_saved_measure_ref` from the resolved chain. Verify: saved-measure tests pass.
- [x] 2.4 Canonicalize `column_expansion._resolve_qualifiers`, both branches of `_lenient_path`, `resolve_default_qualifier_path`, `resolve_default_reference_paths`. Verify: Column.sql / default / scanner tests pass.
- [x] 2.5 State the canonical-input contract in `_common_prefix_len` / `_route_via_common_prefix` / `home._longest_common_prefix` docstrings (one line each). Verify: end-to-end probe scenarios pass.

## 3. Stale-spelling slack rule

- [x] 3.1 Add `STALE_PATH_SPELLING` rule id and `StageColumn.respellings`; compute respellings in `stages._emit_stage_schema` via the same naming function. Verify: unit tests on emitted schemas.
- [x] 3.2 Add the flat-name resolver (reverse index, exact-first, unique fallback) and call it at `binding._resolve_ref`, `binding._resolve_terminal_leaf` (query-backed), `column_expansion._process_reference_site`; build keys/expansions from the matched canonical column. Verify: 1.2 resolution tests pass.
- [x] 3.3 Carry respellings onto the runtime query-backed virtual model's generated columns (not persisted; `build_flat_rename_wrapper` unchanged). Verify: query-backed consumer tests pass.
- [x] 3.4 Plumb resolver warnings through binding → `plan_stages` → prepared slack warnings, deduplicated per position; recombine on cache hit. Verify: warning + cache tests pass.

## 4. Finish

- [x] 4.1 One sentence in `docs/concepts/references.md` (path-segment bullet): a named edge's hop is canonically spelled by its edge name, so result keys use it whichever spelling is typed. Verify: docs build nav unchanged.
- [x] 4.2 Apply the user-approved existing-test / golden updates from 1.3. Verify: full `poetry run pytest -m "not integration"` green.
- [x] 4.3 `poetry run ruff check slayer/ tests/` clean; `openspec validate dev-1954-sharpen-the-determination-route-dev-1908-d9-to-compare --strict` green; `tools/arch_check.py` green.
