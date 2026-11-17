## 1. Failing tests (pr-tests)

- [x] 1.1 Capture the no-result-change goldens on `main`'s code before any rename: rows, values, result keys and column order for single-stage, multi-stage, refinement, field-reference and order-item queries. Write them to `tests/fixtures/ossie_vocabulary/golden.json`; the golden test passes on the current code.
- [x] 1.2 Port the names-only parts of DEV-1607's `tests/test_ossie_vocabulary_*.py` (branch `egor/dev-1607-rename-columns-to-fields-to-be-consistent-with`). Drop every shape-change case: `join_pairs` split, `QueryMetric` / `QueryDimension`, `{metric, alias}`, `filter` as a string, unknown keys, datatype values. Verify each ported test maps to a scenario in `specs/compatibility/ossie-vocabulary/spec.md`.
- [x] 1.3 Write one test per scenario of `specs/compatibility/ossie-vocabulary/spec.md`, `specs/models/stored-upgrade/spec.md` and `specs/usage-reporting/collection/spec.md`, including:
  - the multi-form deprecation case;
  - a pre-rename embeddings sidecar in both backends, across a restart, with no re-embed and no content-hash change;
  - a warm pre-rename store's help topics through inspect, search, reseed and restart;
  - mixed v1, v2 and v3 usage spool files, plus a partial-send failure and claim recovery;
  - a v1 Ossie payload with a matching fingerprint;
  - the HTTP client reading `attributes.metrics` against a new server and through an old route.

  Every new test fails on the current code for the stated reason.
- [x] 1.4 Add the twin tests, parametrized over the explicit curated list of old names (design decision 1): classes, methods, attributes read and write, `__all__`. Add the type-checker probe that runs `la-typecheck` on a module using an old class, attribute and method under `reportDeprecated = "error"`. Both fail now.
- [x] 1.5 Existing tests that contradict the plan: delete `TestPreviousReleaseFieldSets` and `tests/_v1_4_1_field_sets.py` (design decision 17). Align the usage-reporting tests that pin schema 2 to schema 3. Ask the owner about any further contradicting test before changing it.

## 2. Groundwork

- [x] 2.1 Move the `living-architecture` pin to 0.11.0 in `pyproject.toml` (and `CONTRIBUTING.md` if it names it), then `poetry lock`. Verify `la-doctor` passes.
- [x] 2.2 Make `typing-extensions` a direct dependency and set `reportDeprecated = "error"` in `[tool.basedpyright]`. Fix every site the rule flags today in code. Verify `la-typecheck` reports no new errors and the baseline is untouched.
- [x] 2.3 Type the rename's blast radius with `la:make-refactor-target-compliant`, reusing DEV-1607's typing commits where a fresh compliance pass asks for them. The map ran with `--allow-blindspots`: the receivers it listed as blindspots (83) were respelled by hand, then typed after the fact so a later refactor over them is verifiable.
- [x] 2.4 Regenerate the rename list from the current `main` with the issue's word table and carve-outs. Drop the `core/formula.py` and `engine/join_safety.py` module entries (design decision 9). Verify the list against the issue's attachment; differences come only from commits `main` gained since 73e8ebb5.

## 3. Phase 1: rename (commits, all before phase 2)

- [ ] 3.1 Write `rename-map.yaml` in this change folder from the regenerated list, with the flags it is run with in a header comment: `--docs`, `--with` for duck-typed stand-ins, `--keep` for upgrade fixtures and older-version stored documents. Dry-run it clean, batching it if memory runs out, then apply it once. Verify the gate in 3.6.
- [x] 3.2 Rename the modules listed in the issue, except `core/formula.py` and `engine/join_safety.py` (its stub is repointed), with `dr-refactor rename` on a `from pkg import mod` import of each (`move-module` only moves a module between packages, keeping its basename). Leave each old path as a `raise_moved(…, removal="3.0")` stub. Verify the moved-module tripwire test covers each one.
- [ ] 3.3 Respell what the map cannot express with a throwaway pass that is not committed:
  - dict keys and YAML and JSON fixtures (excluding upgrade fixtures and older-version stored documents);
  - MCP tool names, REST routes and CLI commands and flags;
  - help topics `01_models` → `01_datasets` and `06_joins` → `06_relationships`;
  - messages, tool descriptions, help text, `docs/` and `examples/`.

  Re-execute the edited notebooks. Verify that a word-boundary sweep for the old words finds hits only inside the carve-outs.
- [x] 3.4 Respell the literal identifiers in `openspec/specs/` by a scripted direct edit (design decision 18). Show the owner the substitution table and the diff before committing.
- [x] 3.5 Apply the three approved arc42 edits verbatim (design decisions 4–6), extend `compatibility.touches` in `architecture/index.yaml` (design decision 19), and run `la-arch-diagrams`. Verify `la-arch-check` passes.
- [ ] 3.6 Commit phase 1. Verify that ruff, `la-typecheck` (no new errors) and `la-arch-check` pass. Old-spelling tests may still fail; phase 2 fixes them.

## 4. Phase 2: translate old to new

- [ ] 4.1 Add `slayer/core/vocabulary.py`: the rename tables, per-entity dict converters, one deprecation per distinct old form per document across nested parts, and `SpellingConflictError`. It imports only `core`. Verify the fresh-input and conflict scenario tests pass.
- [x] 4.2 Add the version steps SlayerModel 25→26, SlayerQuery 15→16, Memory 11→12 and Embedding 1→2, renumbering if another slice merged first. They are silent, they recurse through nested `source_queries` and memory queries, and they are never pinned for removal. Verify the stored-migration and 1.x upgrade-store tests pass.
- [x] 4.3 Rewrite the embeddings sidecar's `entity_kind` values in place, in one idempotent transaction on open, in both backends. Verify the sidecar test passes with no re-embed and no content-hash change.
- [ ] 4.4 Add the curated Python twins in the design decision 15 form, plus the storage hook bridge for 1.x backends. Verify the twin tests, the type-checker probe and the custom-backend test pass.
- [ ] 4.5 Surfaces:
  - REST: old `/models…` routes with a `Deprecation` header;
  - CLI: old commands and flags with a `Deprecated: …` stderr line;
  - MCP: old tool arguments, `inspect` kinds and sections, and help references converted;
  - help: `help.models` and `help.joins` added to `RETIRED_HELP_IDS`;
  - search: old kind filters and Cypher labels converted;
  - the HTTP client reads `attributes.metrics`;
  - `ResponseAttributes.measures` becomes a deprecated, never-serialized property;
  - environment: `SLAYER_METRIC_EXPANSION_DEPTH`, with the old `SLAYER_MEASURE_EXPANSION_DEPTH` read with a deprecation until 3.0.

  Verify the surface, search and response-attribute tests pass.
- [ ] 4.6 Move the Ossie `SLAYER` payload to version 2. The version-1 reader verifies the fingerprint over the content as written, then converts silently. Verify the export and import tests pass.
- [x] 4.7 Move the usage report to schema 3: `dataset_count`, new tokens and route templates. Spooled schema-2 batches map their renamed keys and tokens before unknown keys fold into `other`. Verify the usage-reporting tests pass, including the mixed spool case.
- [ ] 4.8 Add a section to `docs/upgrading/2.0.md` with a before and after for every renamed form, every old and new MCP tool name and module path, and the curated twin list. Verify every page is linked from `zensical.toml`.

## 5. Pairwise corpus

- [ ] 5.1 With a throwaway extractor that is not committed, write `tests/fixtures/vocabulary_corpus/<file>.json` (`pairs: [{kind, old, new}]`, `skipped: [{line, reason}]`) for every test file and fixture phase 1 changed, pairing `main`'s literals with the respelled ones by position (design decision 2). Verify every phase-1-changed file has a corpus file or a recorded reason it holds no document literal.
- [ ] 5.2 Add one parametrized pairwise test per corpus file: each `old` converts to exactly its `new`, with its deprecations. Verify all pass.

## 6. Gate

- [ ] 6.1 Run the local test scope (`la-affected-tests --base main`, plus goldens, fixtures and conftests this change reaches) and fix every failure.
- [ ] 6.2 Verify that ruff, `la-typecheck` (no new errors, baseline not rewritten), `la-arch-check` and LikeC4 validation pass.
- [ ] 6.3 Run a final word-boundary sweep for `model`, `column`, `measure`, `join` and `formula` over `slayer/`, `tests/`, `docs/` and `examples/`. Every hit is inside a carve-out, an upgrade path, a back-compat test, the engine A/B corpus under `tests/perf/compare/` (also fed to the pinned PyPI engine, so it keeps the old spelling until that pin moves past this release), or (in `tests/` only) a test function name, test class name or test-local variable.
- [ ] 6.4 Post a comment on DEV-1607 noting that its Ossie payload becomes version 3 and that it keeps the Ossie datatype values (design decision 20).
