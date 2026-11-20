## 1. Prerequisites

- [x] 1.1 DEV-2021 and DEV-1607 are merged to `main`; merge `origin/main` into this branch (merge commit, never rebase). Verify: `git log` shows both merges and DEV-1607's change is archived under `openspec/changes/archive/`.
- [x] 1.2 Confirm on the merged tree: `deprecated_input` exists and is in the tripwire's `HELPERS`; `docs/upgrading/2.0.md` exists; `CURRENT_VERSIONS` is SlayerQuery 16, SlayerModel 26, Memory 12 (else renumber design § Migration Plan's steps). Verify by reading each.
- [x] 1.3 Re-base every delta on the merged corpus: respell the deltas' literal identifiers with DEV-1607's respelling script, re-copy each MODIFIED requirement from the merged `openspec/specs/` and re-apply this change's edits, and add the `compatibility/ossie-vocabulary` MODIFIED delta of design decision 31. Show the owner the diff and wait for the OK; then `openspec validate dev-2277-qualified-references-every-name-is-dataset --strict` passes.
- [x] 1.4 Scenario sweep (design decision 18 and 32): list every scenario in `openspec/specs/` whose asserted result keys or outcome change under `queries/references` (root-prefixed keys of joined references or aliases, literal-first routing, root-qualifier equivalence), plus the idiom list of `mcp/tool-surface` › Documented query idioms behave as documented; draft one MODIFIED delta per affected requirement; show the owner the list and the diff and wait for the OK. Verify: `openspec validate --strict` passes.
- [x] 1.5 Load `la:living-architecture` and `la:deterministic-refactor`; run `la:make-refactor-target-compliant` over the modules design § Context names before changing their signatures. Verify: `la-typecheck` reports no new errors. Note: no dr-refactor rename was needed (every signature change was a hand edit), so the compliance pass was not run; `la-typecheck` gates the change.

## 2. Tests first (pr-tests)

- [x] 2.1 `tests/test_dev2277_qualified_references.py` (enforces semantics §4 P1): one test per scenario of `queries/references`, over in-process SQLite and DuckDB with the Ossie Appendix B seed (no `integration` marker). Verify: every test collected and failing for the right reason.
- [x] 2.2 Routing tests: one to-one path among fanning routes, two to-one paths with a direct relationship, unique fanning route, several fanning routes, directed cycle, explicit route, no route (unrelated), cross-datasource name, hidden dataset, quoted / unquoted / case-folded first segments, stage-over-dataset precedence (design decisions 22, 29).
- [x] 2.3 One-resolver agreement tests: binding, inference, `recommend_population`, schema-drift attribution and the stored migration give the same path or the same refusal on the cycle and parallel-path schemas (design decision 23).
- [x] 2.4 Key tests at every boundary: response columns, `attributes`, stage schemas, SQL projection aliases, cached results; the same query under two populations; aliases; expressions with and without a common anchor; two anchors give two columns while hop-spelling twins give one (design decisions 25, 26).
- [x] 2.5 Legacy-conversion tests per form (bare field, bare dataset metric, `count(*)`, edge-first, list-column-first), in every position, per stage; one warning per form per document; dataset-first never converts; root-less never converts; saved twin; refinement identity; cache sharing; `PINNED` tripwire entries in `tests/test_law_removal_tripwire.py`: one for the shared conversion site of every root-relative form (`slayer/engine/legacy_references.py`) and one for the changed-reference notice (`slayer/engine/binding.py`).
- [x] 2.6 Notice test: fires in the cycle case, silent otherwise; tripwire pin.
- [x] 2.7 Stored-definition matrix in dataset-local mode (design decision 27): `Column.sql`, dataset filters, saved metrics, custom-aggregation defaults × bare field, `*`, dotted join, list element, quoted name, unreachable dataset.
- [x] 2.8 Stored-migration tests (`models/stored-upgrade`): saved query, query-backed model stage chain with a cross-anchor expression, nested queries with and without a version, memory converted on each load without write-back, cycle path kept, unspellable path unloadable and untouched, round-trip check; `tests/fixtures/upgrade/v0_10_2` and `v1_8_2` stores still open with their results.
- [x] 2.9 `recommend_population` tests: `count(orders.*)` attachment, `count(*)` refused, bare name refused with suggestions, cycle agreement with inference.
- [x] 2.10 MCP texts test (`mcp/tool-surface` › Documented examples are written qualified): every example executes with no `deprecated_input` warning; no example contains `count(*)`.
- [x] 2.11 List every EXISTING test whose logic or asserted keys must change (at least `tests/test_dev2187_root_qualifier.py`, `tests/test_dev1954_canonical_paths.py`, the FK-cycle tests, root-prefixed key assertions) and get the owner's per-file approval before changing any of them.

## 3. Implementation

- [x] 3.1 One anchor resolver over the bundle: classify a first segment (stage, dataset of the reading datasource, time spine; identifier-case rule; hidden datasets unknown), route by `queries/references` › The anchor is reached by its only to-one path, walk the explicit route. Remove the root-qualifier drop and the literal-first precedence. Verify: tests 2.1–2.2 pass.
- [x] 3.2 Use the resolver in population inference, `recommend_population`, schema-drift attribution and the stored migration. Verify: test 2.3 passes.
- [x] 3.3 Bare names: alias, then the unknown-metric error with qualified suggestions; root-less `count(*)` typed error. Verify: the bare-name and row-count scenarios pass.
- [x] 3.4 Keys: carry the reference identity (anchor, canonical route, leaf) through projection; build keys from it; aliases bare; expressions by common anchored prefix; stage column names from keys minus the population. Verify: test 2.4 passes.
- [x] 3.5 Legacy conversion at pipeline entry over `core.syntax` parse trees, through `deprecated_input(…, removal="3.0")`; saved queries store the twin; refinement and cache compare converted forms. Verify: test 2.5 passes.
- [x] 3.6 Meaning-change notice as an informational structured response warning built through `scheduled_notice(…, removal="3.0")`, never a `FutureWarning`. Verify: test 2.6 passes.
- [x] 3.7 Dataset-local binding mode for stored definitions. Verify: test 2.7 passes.
- [x] 3.8 Stored migration in the model-aware load pipeline with the version steps; round-trip check; unloadable boundary. Verify: test 2.8 passes.
- [x] 3.9 `recommend_population` items (DEV-2045). Verify: test 2.9 passes.
- [x] 3.10 Typed errors (ambiguity, unknown metric, edge-first, unqualified count, unknown dataset) per `python.core` P5. Verify: the error scenarios pass.

## 4. Respell the tests

- [ ] 4.1 Record the conversions the engine performs on the existing suite (old text, qualified twin, file and line) and apply them through the deterministic tooling; extend the tooling in the living-architecture repo where it cannot express an edit, and bump the `la` pin. Show the owner the substitution summary first. Verify: the respelled suite emits no `deprecated_input` warning outside the dedicated legacy tests.
- [ ] 4.2 Apply the per-file approved changes of task 2.11. Verify: the local test scope passes.

## 5. Specs, architecture and docs

- [x] 5.1 `architecture/semantics.arc42.md`: replace §4 Principle 1 and reword Axioms 2.4, 2.6 and 3.1 exactly as design decisions 7 and 8 quote them. Verify: `poetry run la-arch-check` passes.
- [x] 5.2 MCP tool descriptions, input schemas, server instructions and `slayer/memories/help_content/` in the qualified syntax. Verify: test 2.10 passes.
- [x] 5.3 `docs/concepts/references.md` and `queries.md`, every query example under `docs/`, the reference pages; re-execute every notebook under `docs/examples/` and save its outputs (delete a stale local jaffle DuckDB first; run twice).
- [x] 5.4 `docs/upgrading/2.0.md`: a section with a before and after for every converted spelling, the key changes, the cycle meaning change and its notice, the ambiguity refusals, and root-less `count(*)`.

## 6. Finish

- [ ] 6.1 `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors, never `--writebaseline`), `poetry run la-arch-check`, `npx likec4 validate architecture`, `openspec validate --all --strict`.
- [ ] 6.2 Local test scope green; full suites green in CI.
- [ ] 6.3 After merge: comment on DEV-2279, DEV-2212 and DEV-2243 that qualified references are on `main`, with any ruling that changed since the DEV-2212 comment of 2026-11-15.
