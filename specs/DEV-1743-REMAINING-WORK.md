# DEV-1743 — Remaining work (state as of the dotted-canonical-flip core landing)

> **STATUS 2026-08-26 — COMPLETE.** All work packages below (WP4–WP9) are
> implemented and green. Full non-integration suite: **13,228 passed, 0 failed**;
> ruff clean. Integration (Postgres/DuckDB): **533 passed, 0 code failures** (the
> only errors are the Docker-gated `test_metabase_e2e.py` fixture bootstrap
> timeout — an environment gate, not a code regression). Genuine-gap #1 (save-time
> cycle detection over dotted multi-hop paths) is CLOSED. Remaining non-code
> follow-up: re-execute `docs/examples/05_joins/joins_nb.ipynb`. The sections
> below are retained as the historical work log.

This file is the resume handoff. The **core of the flip is done and the entire
existing test suite is green**; what remains are the peripheral work-packages
whose fail-first tests were written up front (WP4–WP8), the docs sweep (WP9),
and a short cleanup list. The authoritative spec is the Linear **DEV-1743** body
(mirror in the session scratchpad `dev-1743-spec.md`); the original pre-impl
build plan is `specs/DEV-1743-IMPLEMENTATION-PLAN.md`.

## Snapshot

- Full non-integration suite: **13,225 passed**, 98 skipped, 2 xfailed,
  **21 failed** — and every one of the 21 is a DEV-1743 fail-first test for an
  un-built WP (list below). No other test fails. `ruff check slayer/ tests/`
  clean.
- Integration tests (Postgres/DuckDB) not run in this pass; their `__`-chain
  inputs were dotted, so they should be green, but **run them** before calling
  the feature done.

## DONE

- **WP1 — validators & parser relaxation.** `__` lifted from
  model/query/column/measure names; `.`/`:` still banned; `__slayer_` prefix
  reserved on all four name validators AND in the Mode-B parser (raw-input scan
  before `_preprocess_colons`, so `revenue:sum` still works, a literal
  `__slayer_agg_0__` is rejected). `_reject_dunder_in_ast` + `allow_dunder`
  removed; the `binding.py` `"__" in name` special-case removed (flat `__`
  columns resolve by ordinary exact-match). Tests: `test_dev1743_validators.py`
  (25), `test_dev1743_mode_b.py` (4). Flipped legacy locks: `test_syntax.py`,
  `test_models.py`, `test_schema_drift_typed.py`.
- **WP3 — join-alias registry.** `AliasAllocator.alias_for(root, path)`
  (`slayer/sql/naming.py`), memoized by `(root, path)`, per-root namespace,
  preferred spelling byte-identical to the legacy `"__".join`, then
  `fit_identifier` + fold-uniquify. A chain leaf `(a,b)`→`a__b` and a literal
  model `a__b`→`a__b_2` now mint DISTINCT aliases. Routed through it:
  `generator._join_alias`, `_build_from_and_joins`, `_joined_or_local_dim_expr`,
  `scope._anchor`. Tests: `test_dev1743_join_alias_registry.py` (4). Golden
  byte-stability verified (plain chains unchanged).
- **WP2 — the one Mode-A resolution door.** The heart of the flip. Two-tier
  resolver in `slayer/engine/column_expansion.py`:
  - **STRICT** `_resolve_qualifiers` (used by the expansion): exact-name-first
    (host / direct `__`-model), dotted chains (2–4-part `Column.parts` and 5+-part
    nested `exp.Dot`, [C4]), legacy `__` split-alias → `LegacyDunderAliasError`
    (D2, new class in `core/errors.py`), broken hop → `UnresolvableDimensionJoinError`.
  - **LENIENT** `_lenient_path` (used by the join-path SCANNER
    `collect_root_scope_joined_paths`): never raises D2 — it runs over
    already-expanded fragments whose qualifiers are legitimate internal `__`
    aliases, so it exact-then-naive-split-resolves and skips the unresolvable.
  - **Structural join discovery** via `crossed_paths` (an ordered sink, e.g.
    `ScopeFrame.join_paths`) threaded through `expand_derived_refs_sync` with
    `owner_path`, REPLACING the old "re-scan the emitted form" (which is
    ambiguous once a chain alias serializes to `__`). `scope.py` registers
    ColumnKey paths structurally and drops the `_register_join_paths(final)`
    re-scan; the generator's `_expand_derived_row_dims` / `_derived_paths`
    collect crossed paths instead of `_joined_paths_in_sql(expr)`.
  - **Reserved words**: `expand_derived_refs_sync` now prequotes reserved
    identifiers before parsing (idempotent) — `grant.amount` parses as a column,
    not an unsupported `Command`, so its join registers (DEV-1686 regression fix).
  - **Quote preservation**: `_requalify` mutates a `Column`'s qualifier in place
    so a quoted leaf (`customers."spend"`) keeps its quoting.
  - **DOT_PATH_IN_SQL retired**: `normalize_model` no longer rewrites dotted→`__`.
  - **Two doors [C5]**: generation-time (the resolver runs during SQL gen) AND
    save-time (`SlayerQueryEngine._validate_mode_a_join_paths` +
    `_preload_join_targets` in `query_engine.py`, run in `save_model`).
  - Tests: `test_dev1743_resolution.py` (9), and `test_dev1743_dialect_roundtrip.py`
    (now green as a WP1–3 side effect).
- **Agg-fragment resolution (WP2 completeness, was Task #16).** Custom-aggregation
  template fragments (string kwargs + model-default `AggregationParam.sql`) are
  resolved through the Mode-A door and the alias-rewritten AST is embedded via
  `ResolvedAggKwarg(kind="expr")` — a multi-hop dotted fragment
  `customers.regions.weight` now emits `customers__regions.weight`, not a
  dotted-unbound `regions`. Threaded through the host, `_cm_` CTE, and
  time-shift render paths (`_register_fragment_kwarg_joins` now returns the
  resolved map; `_build_agg_render_spec_from_planned` surfaces model-default
  fragments into `agg_kwargs_str`). One golden re-blessed
  (`dev1750_sql_baseline.json`, `host_rooted_wscaled_user_kwarg` — the kwarg
  VALUE is encoded in the `_cm_` CTE name and changed from `__` to dotted;
  semantically identical SQL).
- **Existing-test rewrite (the flip fallout).** ~20 files' `__`-chain INPUTS
  (`Column.sql`, `Column.filter`, `SlayerModel.filters`, measure `formula`)
  dotted to the new canonical; internal-alias assertions and result-keys left
  as `__`. Done across `test_sql_generator.py`, the dev1745/dev1750/dev1747/
  dev1748/dev1708 clusters, the misc set (agg_kwargs, filtered_local_isolation,
  cross_model_derived_columns, aggregate_input_paths, scope, stage2_host_migration,
  reference_semantics), `_cross_model_chain.py`, `test_dot_path_in_sql.py`
  (`TestNormalizeModelWiresDotPath` flipped to "preserved"), and both integration
  files.

## REMAINING (the 21 fail-first, by WP)

### WP4 — Storage migration v8 → v9  (`test_dev1743_migration.py`, 9 tests)
Bump `CURRENT_VERSIONS["SlayerModel"]` to 9; register a no-op v8→v9 converter
(`slayer/storage/v9_migration.py`, mirror `v8_migration.py`). Rewrite each `__`
qualifier in a stored model's `Column.sql`/`filter`/model `filters` → dotted, on
the RAW dict **before** `SlayerModel.model_validate` **[C6]**, in
`_migrate_and_refine_on_load` (`storage/base.py`). Resolve the naive split
against the model's own `joins` (first hop) and sibling RAW dicts (deeper hops)
— a NEW protected raw-load method on `StorageBackend` implemented by YAML +
SQLite, working through `JoinSyncStorage` **[C7]**. P2 = sqlglot
parse-and-re-serialize, changed fragments only. Gate live-schema refinement so
the version bump alone doesn't force it **[C1]** (a v8 model with no reachable
datasource must still migrate). Failing tests name every sub-behavior.

### WP5 — Save-time inspectors  (`test_dev1743_inspectors.py` 2 + OSI in importers 1)
Route `column_dependency.py:69`, `schema_drift.py:667` (audit `:543`/`:696`),
and OSI `_walk_join_alias` (`osi/converter.py:674-686`) through the shared
resolver. OSI keeps strict-D2 (P1): `_walk_join_alias` returns `None` for a
non-exact `__` alias (validate-and-drop, not a crash).
Tests: `test_dev1743_inspectors.py`, `test_dev1743_importers.py::test_osi_*` (2).

### WP6 — Ingestion (D3)  (`test_dev1743_ingestion_preserve.py`, 5 tests)
`_resolve_scanned_collisions` group by raw object name; storage-aware rename
pre-pass in `ingest_datasource_idempotent` using `_list_all_model_identities()`
— adopt the stored sanitized spelling ONLY when its `sql_table` resolves to the
SAME live object (**[C2]** full normalized schema+object identity, not bare
name); cascade renames over `ModelJoin.target_model` + `ModelAddition.model_name`;
invert `_repair_legacy_join_targets` premise (heal only when the raw-named target
doesn't exist and the sanitized one does). **Add during impl**: the C2
cross-schema Postgres test (needs the schema-identity seam this WP introduces).

### WP7 — Importers  (dbt/cube residue in `test_dev1743_importers.py`)
dbt: stop dropping hidden `__` models (`converter.py:293-336`); stop aborting on
`__`-named semantic models (`:447-458`, missing try/except `:174-200`). Cube:
verify `foo__bar` cubes/views build (should need no code change beyond WP1). OSI:
remove `"__"` from `_UNSAFE_MODEL_NAME_CHARS` (`osi/converter.py:63`).

### WP8 — Query-backed flatten collision (D5, [C9])  (`test_dev1743_flattening_collisions.py`, 3)
Add a local flatten-specific duplicate-name check in
`query_engine._expand_query_backed_model` firing immediately after deriving
`expected`, BEFORE `build_flat_rename_wrapper`. Keep the stage check
(`stage_planner.py`). The tests assert a clear "flatten"/"collision" message and
that it fires before the wrapper (sentinel monkeypatch).

### WP9 — Docs sweep (no failing tests)
Full tiered list in the Linear body §WP9. Highlights: `docs/concepts/references.md`,
`models.md` (incl. stale "currently 6" → 9), `ingestion.md`, `reference/cli.md`;
`docs/examples/05_joins/` md + notebook (re-execute); architecture
`slack-normalization.md` / `parsing.md`; `CLAUDE.md`;
`.claude/skills/slayer-models.md`; `slayer/memories/help_content/07_joins.md`;
append a `DECISIONS.md` entry. Flat-column docs STAY (D5). Every new/renamed doc
page must be linked in `zensical.toml` `nav`.

## GENUINE GAPS / DECISIONS SURFACED DURING THE FLIP

1. **Save-time cycle detection doesn't understand dotted multi-hop.**
   `test_column_dependency.py::test_save_model_detects_cycle_via_canonical_multihop_path_alias`
   is GREEN but only because its input was kept as the legacy `B__C.x` (the
   save-time cycle walk lenient-`__`-splits it). Dotting the input to `B.C.x`
   makes cycle detection SILENTLY MISS the cycle. Decide: teach the save-time
   cycle walk (in `slayer/storage`) the dotted multi-hop form, then dot the test;
   or accept `__` at that one internal surface. This is the only place the new
   dotted contract is not yet honoured by production. (Not counted in the 21 —
   the test passes as-is.)

## CLEANUP (retired-but-not-deleted; do in WP9 or a tidy-up commit)

- `slayer/core/refs.py::reject_user_dunder` — dead (no callers). Delete.
- `slayer/engine/normalization.py` — `_apply_dot_path_in_sql`,
  `_dot_path_root_scope_analysis`, `_normalize_column_dot_paths`,
  `_normalize_model_filter_dot_paths` are retired (no longer called by
  `normalize_model`). Delete them AND `tests/test_dot_path_in_sql.py::TestDotPathInSqlHelper`
  (which still exercises the dead helper). Keep the flipped
  `TestNormalizeModelWiresDotPath` (now asserts preservation).
- Reserve `__slayer_` prefix (P3): done on names + parser. If any other
  internal name surface should reserve it, add there.

## ARCHITECTURE NOTES (for whoever resumes — the non-obvious bits)

- **Two-tier resolver is load-bearing.** STRICT `_resolve_qualifiers` (expansion)
  raises D2; LENIENT `_lenient_path` (scanner) must NOT — it sees legitimate
  internal `__` aliases in already-expanded fragments. Do not merge them.
- **Never re-scan an emitted fragment for join paths.** Once a chain alias
  serializes to `customers__regions`, it is indistinguishable from a literal
  model of that name. Join discovery is STRUCTURAL via `crossed_paths` +
  `owner_path`. The one remaining lenient scanner (`collect_root_scope_joined_paths`)
  tolerates the ambiguity by design (non-collision cases only) and is fed raw
  forms where it matters.
- **The WP3 registry and the expansion must share the allocator instance.** In
  the normal path `ScopeFrame.allocator is SQLGenerator._gen_allocator`; the
  alias_resolver threaded into `expand_derived_refs_sync` is registry-backed so
  qualifiers match emitted JOIN aliases even under collision.
- **`alias_resolver=None` falls back to byte-identical `"__".join`** — safe for
  every non-collision case; only collisions need the registry.

## FILES CHANGED SO FAR (for the eventual commit; nothing committed yet)

Production: `core/errors.py`, `core/models.py`, `engine/binding.py`,
`engine/column_expansion.py`, `engine/normalization.py`, `engine/query_engine.py`,
`engine/stage_planner.py`, `engine/syntax.py`, `sql/generator.py`, `sql/naming.py`,
`sql/scope.py`.

Tests: the 12 staged `test_dev1743_*` files (+ fixtures) from the earlier
session; rewritten existing files (`test_sql_generator.py`, dev1745/dev1750/dev1747/
dev1748/dev1708 clusters, agg_kwargs, filtered_local_isolation,
cross_model_derived_columns, aggregate_input_paths, scope, stage2_host_migration,
reference_semantics, models, syntax, schema_drift_typed, dot_path_in_sql,
mode_a_door, `_cross_model_chain.py`, both integration files); re-blessed
`tests/golden/dev1750_sql_baseline.json`.

## VERIFICATION GATES (before declaring the feature done)

1. `poetry run pytest -m "not integration"` → only the 21 named fail-first fail
   (they turn green as WP4–WP8 land).
2. `poetry run pytest tests/integration -m integration` (Postgres/DuckDB).
3. `poetry run ruff check slayer/ tests/`.
4. Re-execute the joins notebook(s) touched in WP9.
5. Step 7 (ask before commit/push/PR), then `/process-reviews` loop.
