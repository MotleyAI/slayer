# DEV-1743 — Implementation plan (Step 6 onward)

Status at pause: **Steps 1–5 done.** Full fail-first test suite written, ruff-clean,
Codex-reviewed, gaps folded. 12 new test files staged (not committed). This file is the
remaining-work handoff. The authoritative spec is the **Linear issue DEV-1743 body**
(and the copy in the session scratchpad `dev-1743-spec.md`); this file is the ordered
build/repair checklist that makes the staged tests pass.

Branch: `egor/dev-1743-lift-the-double-underscore-ban-on-model-names-graph-guided`
(based on origin/main, which now contains the whole dev-1450 line).

## Decisions (fixed — do not re-litigate)
- **D1** Mode-A free SQL (`Column.sql`/`Column.filter`/`SlayerModel.filters`) is
  dotted-canonical. A table qualifier is either an EXACT name (host, or a directly-joined
  model literally named e.g. `a__b`) or a DOTTED chain. User input is never split on `__`.
- **D2** a `__` qualifier that does not exact-resolve but whose naive split walks the graph
  is a HARD ERROR naming the dotted replacement, at BOTH save time (`engine.save_model`)
  and generation time. No deprecation.
- **D3** fresh ingests preserve `__`; re-ingest matches exact-then-sanitized, stored name
  wins, adopting the stored sanitized spelling ONLY when its `sql_table` resolves to the
  same live object (full schema+object identity, not bare name).
- **D4** emitted join aliases are internal-only: per-generation allocator, length-fitted
  (`fit_identifier`), silently uniquified in case-fold space. NO `IdentifierCollisionError`
  for join aliases.
- **D5** query-backed/flat column names (`stores__name`) keep `__`; a flattening collision
  raises loudly (never silently uniquified).
- **P1** OSI import strict-D2 = stop silently splitting → unresolvable → drop/revert with a
  report entry (NOT a hard crash; importers validate-and-drop). `_walk_join_alias` returns
  None for a non-exact `__` alias.
- **P2** migration rewrites SQL via sqlglot parse-and-re-serialize, only changed fragments.
- **P3** `__slayer_` prefix reserved in model/column/measure/query validators AND the
  expression parser (raw-input scan BEFORE `_preprocess_colons` placeholder substitution).

## Build order (sequenced; each WP cites the tests it must turn green)

### WP1 — Validator & parser relaxation
- `slayer/core/models.py:110-116`: drop `_NO_DUNDER.check` from `_validate_model_name`
  (keep `_NO_DOT`/`_NO_COLON`); retire the `_NO_DUNDER` rule object (`:65-69`). Keep
  `sanitize_model_name` (WP6 fallback matching needs it).
- Add P3 `__slayer_` prefix rejection to the four name validators (model, column, measure,
  query). Model/query use `_validate_model_name`; column `_validate_column_name`; measure
  `_NAME_PATTERN` path.
- `slayer/engine/syntax.py`: remove `_reject_dunder_in_ast` gate (`:287-288`, `:624-651`)
  and the `allow_dunder` param (callers `stage_planner.py:759/855/884/936/946/2260/2297`).
  Replace with P3 raw-input scan: reject `__slayer_` in the RAW `text` BEFORE
  `_preprocess_colons` (which mints `__slayer_agg_N__` at `:687`). So `revenue:sum` keeps
  working, a literal `__slayer_agg_0__` is rejected.
- `slayer/engine/binding.py:584-599`: delete the `"__" in name` ModelScope special-case —
  `__`-bearing segments resolve by ordinary exact-match (this is what makes the D5
  flat-column reference bind in Mode-B queries).
- `slayer/core/refs.py:261-285`: delete dead `reject_user_dunder`.
- Tests: `test_dev1743_validators.py` (all), `test_dev1743_mode_b.py` (the parser-gated
  ones).

### WP3 — Join-alias registry (do BEFORE WP2; WP2's rewrite target consumes it)
- Extend `AliasAllocator` (`slayer/sql/naming.py:101`; sole ctor `SQLGenerator._new_allocator`
  `generator.py:673-680`) with `alias_for(root, path) -> str`, **memoized by `(root, path)`**
  [C8].
- Preferred spelling: bare target for single hop, `"__".join(path)` for multi-hop —
  BYTE-IDENTICAL to today (keep golden baselines green when nothing collides). Then
  `fit_identifier(preferred, limit=dialect.max_identifier_bytes)`, then uniquify in
  fold-normalized space (existing `folds_case`).
- Route every emitted-alias producer through it: `_build_from_and_joins` mint
  (`generator.py:6062-6065`), the duplicate walk in `_joined_or_local_dim_expr`
  (`:6139-6142`), `_host_grain_join_alias` (`:417`), `ScopeFrame._anchor` both branches
  (`scope.py:294`, `:318`), derived-column seams (`generator.py:7667`, `:7793`).
- Non-emitted bookkeeping strings (`binding.py:905`, `filter_reachability.py:141` incl. its
  memo key `:118`, `generator.py:8002`): switch to dot-joined or native tuples (injective).
- Leave the SELECT-alias/flat-name namespace (`planning.py:784-786` → `flat_name`) alone.
- Tests: `test_dev1743_join_alias_registry.py`, integration long-alias collision test.

### WP2 — The one Mode-A resolution door
- Replace naive split in `slayer/engine/column_expansion.py`
  (`_resolve_alias_to_join_segments` `:114-139`, `_walk_path_to_target_sync` `:203-231`)
  with the shared resolver:
  1. qualifier == source relation / host name → host.
  2. 2-part `q.leaf`: `q` exact-matches a direct join target → single hop `(q,)`. Else if
     `q` contains `__` and naive split fully walks → D2 fix-it error (unless shadowed by a
     fragment-local CTE/subquery alias — reuse shadow analysis `normalization.py:394-412`).
     Else opaque.
  3. ≥3-part `p0…leaf`: `p0` exact join target → walk the whole chain (each hop exact,
     hops may contain `__`); full walk → path tuple; later hop fails → error naming the hop.
     `p0` not a join target → opaque. Join-target-beats-schema precedence (document it).
- **Path extraction [C4]:** 2–4-part refs via `Column.parts`; 5+-part via unwrapping nested
  `exp.Dot`. Preserve physical `schema.table.column` opacity (current walk skips db/catalog,
  `column_expansion.py:260`,`:326`).
- Resolver returns structural path tuples; qualifier rewrite (`column_expansion.py:277`,
  `:288`) consults the WP3 registry (closes the tuple→string→tuple round-trip).
- Retire the `DOT_PATH_IN_SQL` dotted→dunder rewrite (`normalization.py:417-505`).
  **Save-time validation [C5]:** the full pass (D2, hop errors, shadow warnings) runs in
  `engine.save_model` fed by a resolved bundle the engine builds (it already loads
  referenced models for cycle validation). `normalize_model` keeps only graph-free checks.
  The generation-time door runs the SAME resolver (covers MCP/CLI/ingestion save paths that
  bypass `engine.save_model` — `mcp/server.py:1070/1440/1604`).
- Repoint dead `rule_doc_url` anchors (nonexistent `docs/agent_input_slack.md`) →
  `docs/architecture/slack-normalization.md`.
- Tests: `test_dev1743_resolution.py` (all).

### WP4 — Storage migration v8 → v9
- `migrations.py:18-24`: bump `CURRENT_VERSIONS["SlayerModel"]` to 9; register a no-op
  v8→v9 converter (module `slayer/storage/v9_migration.py`, mirror `v8_migration.py`).
- **[C1]** gate live-schema type refinement so the v9 version bump does not force it
  (`_migrate_and_refine_on_load`, `base.py:437`) — a v8 model with no reachable datasource
  must still migrate.
- The rewrite runs in `_migrate_and_refine_on_load` on the RAW dict **before**
  `SlayerModel.model_validate` **[C6]**: for each `__` qualifier in Column.sql/filter/model
  filters, resolve the naive split (first hop against the model's own `joins`, deeper hops
  by reading sibling RAW dicts) and rewrite to dotted (P2 sqlglot re-serialize, changed
  fragments only). Deterministic (pre-v9 data has no `__`-named models). Opaque qualifiers
  untouched.
- **[C7]** sibling raw-dict access via a NEW protected raw-load method on `StorageBackend`
  implemented by YAML and SQLite (do NOT reuse the CLI's `_load_raw_model_dict`); must work
  through `JoinSyncStorage` wrappers.
- Tests: `test_dev1743_migration.py` (all).

### WP5 — Save-time inspectors
- Route `column_dependency.py:69`, `schema_drift.py:667` (audit `:543`/`:696`), and OSI
  `_walk_join_alias` (`osi/converter.py:674-686`) through the WP2 shared resolver. OSI keeps
  strict-D2 = no silent split (P1).
- Tests: `test_dev1743_inspectors.py`, the OSI test in `test_dev1743_importers.py`.

### WP6 — Ingestion (D3)
- `_resolve_scanned_collisions` (`ingestion.py:1366-1409`): group key = raw object name;
  intra-scan collisions reduce to true cross-schema same-name pairs; update skip wording;
  retire `_collision_sort_key`'s `is_sanitized` element.
- New storage-aware rename pre-pass in `ingest_datasource_idempotent` before `fresh_by_name`
  (`:2464-2472`), using `_list_all_model_identities()`: keep R if stored R exists or no
  stored `sanitize(R)`; adopt the stored sanitized spelling ONLY when its `sql_table`
  resolves to the SAME live object — **[C2]** full normalized schema+object identity
  (SchemaRef-aware, default-schema resolution), NOT bare-name. Cascade renames over
  `scan.models`: inbound `ModelJoin.target_model` + `ModelAddition.model_name`
  (`:2168`,`:2248`).
- `_repair_legacy_join_targets` (`:1875-1913`): invert the premise; gate the heal to fire
  only when the raw-named target doesn't exist as a model and the sanitized one does.
- Tests: `test_dev1743_ingestion_preserve.py`. **Add during impl:** the C2 cross-schema
  Postgres test (needs the schema-identity seam this WP introduces).

### WP7 — Importers
- dbt: hidden regular models with `__` stop being dropped (`converter.py:293-336`);
  `__`-named semantic models stop aborting conversion (`:447-458`; missing try/except
  `:174-200`). `_DIMENSION_RE` greedy split (`filters.py:22-24`) unchanged (dbt input
  syntax).
- Cube: `foo__bar` cubes/views build (`cube/converter.py:235-247`,`:787-797`) — no code
  change beyond WP1; verify.
- OSI: remove `"__"` from `_UNSAFE_MODEL_NAME_CHARS` (`osi/converter.py:63`);
  `_walk_join_alias` per WP5/P1.
- Tests: `test_dev1743_importers.py`.

### WP8 — Query-backed flatten collision (D5, [C9])
- Add a local duplicate-name check with a flatten-specific message in
  `_expand_query_backed_model`, firing immediately after deriving `expected`
  (`query_engine.py:2940`) — BEFORE `build_flat_rename_wrapper`. Keep the stage check
  (`stage_planner.py:2597-2606`).
- Tests: `test_dev1743_flattening_collisions.py`.

### WP9 — Docs sweep
Full tiered list in the Linear body §WP9. Highlights: `docs/concepts/references.md`,
`models.md` (incl. stale "currently 6" → 9), `ingestion.md`, `reference/cli.md`;
`docs/examples/05_joins/` md + notebook (re-executed); architecture `slack-normalization.md`
/ `parsing.md`; `CLAUDE.md:53`; `.claude/skills/slayer-models.md`;
`slayer/memories/help_content/07_joins.md`; DECISIONS.md appended entry. Flat-column docs
(`queries.md:121`, `06_multistage`, `08_models`, `09_extending`) STAY (D5).

## Existing tests to FLIP during Step 6 (they encode the old contract)
These currently PASS (locking pre-flip behavior) and will FAIL once the flip lands. Rewrite
them to the new contract as part of "make the suite green":
- `tests/test_dot_path_in_sql.py` — the DOT_PATH_IN_SQL rule is RETIRED; rewrite to the
  save-time validation-pass contract (dotted canonical; `__` legacy → D2; shadow exempt;
  physical-ref opacity — this is the natural home for the opacity/shadow unit tests deferred
  from `test_dev1743_resolution.py`).
- `tests/test_error_messages.py:130-145` — `IllegalScopeReferenceError` "`__` reserved"
  message is removed/changed.
- `tests/test_ingestion_name_sanitize.py` — `TestDunderTableIngestion`,
  `TestCollisionPolicy`, `TestJoinTargetsUseModelNames`, `TestEmptyJoinListIsNotNoJoinList`,
  `TestSanitizedNamesDoNotLeakIntoColumns` assert the sanitize-drop behavior D3 reverses.
  `TestSanitizer` (the `sanitize_model_name` unit tests) STAYS.
- Golden SQL: generate a `tests/test_dev1743_golden_sql.py` (bind via `tests/_golden_harness.py`,
  model on `test_dev1750_golden_sql.py`) once emission is real; structure `_generate_one` so
  a `__`-model construction raise is recorded INSIDE the try (build models in-try).

## Verification gates
- After each WP: `poetry run pytest -m "not integration"` and fix regressions.
- Golden byte-stability: plain-chain aliases stay `customers` / `customers__regions`.
- Final: full non-integration suite green; integration (Postgres) for the value tests;
  re-run notebooks; `poetry run ruff check slayer/ tests/`.
- Then Step 7: ask before commit/push/PR. Then `/process-reviews` loop.

## Codex review notes folded (test-side, done) — for reference
C1 no-datasource migrate; C2 cross-schema (Postgres test deferred to WP6); C3 raw-scan
before placeholders; C4 nested-Dot deep chain; C5 save==generation door; C6 rewrite before
validate; C7 wrapper sibling-load; C8 (root,path) key; C9 collision-before-wrapper. Filter
surfaces + tightened D2 assertions + explicit ambiguity test added.
