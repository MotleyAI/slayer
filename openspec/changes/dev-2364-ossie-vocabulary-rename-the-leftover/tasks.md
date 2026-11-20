## 1. Prerequisites

- [x] 1.1 At the start of `pr-tests`: merge the current DEV-2277 head (local branch `egor/dev-2277-qualified-references-every-name-is-dataset`, or `origin/main` once DEV-2277 has landed) into this branch (merge commit, never rebase; quoted ref in the message). Verify: `git log` shows the merge and `poetry run pytest tests/test_vocabulary_identifier_guard.py` is green before any change (design Decision 11).
- [x] 1.2 Load `la:living-architecture` and `la:deterministic-refactor`. Verify: both skills loaded in the session.

## 2. Tests first (pr-tests)

- [x] 2.1 `tests/_vocabulary_guard.py`: delete `Straggler`, `VocabularyAllowlist.stragglers` and `guard_problems`' `ceiling` parameter (design Decision 10). Verify: `ruff` clean.
- [x] 2.2 `tests/test_vocabulary_identifier_guard.py` (alignment approved with the plan): delete `_ceiling`, `test_straggler_ceiling_is_declared`, `test_stale_straggler_is_red`, `test_straggler_count_is_capped_by_the_ceiling`; reduce `test_carve_out_and_straggler_are_green` to carve-outs; add the `removed-stragglers-key` case `{"stragglers": []}` to `test_malformed_allowlist_is_rejected` and a positive test that `{}` parses (Decision 17). Verify: the synthetic-source guard tests pass.
- [x] 2.3 `tests/fixtures/vocabulary_identifier_allowlist.yaml`: remove the `stragglers:` key and update the header comment; `architecture/index.yaml`: delete `x-vocabulary-stragglers: {baseline: 37}` and its trailing blank line (approved with the plan). Verify: `test_every_old_word_identifier_in_slayer_is_allowlisted` fails listing exactly the 37 former stragglers as unlisted.
- [x] 2.4 `tests/test_cube_report.py`: assert that `CubeIssueCategory.SEGMENT_AS_FIELD` serialises as `"segment_as_field"` in the report JSON (Decision 5). Verify: fails for the right reason (no such member).

## 3. `slayer/` stragglers (pr-implement)

- [x] 3.1 Rename each former straggler's SLayer-concept uses with `dr-refactor rename-map` (dry-run → review → `--apply`, the full gate), stand-ins triaged one by one with `--with` / `--without`. Verify: `la-typecheck` reports no new errors.
- [x] 3.2 Mixed-use names (e.g. `slayer/live_schema/ingestion.py: columns`, `slayer/sql/generator.py: column`): rename the straggler uses, add a `carve_outs` entry with its reason (Decision 9). Verify: the guard test passes.
- [x] 3.3 `SEGMENT_AS_COLUMN` → `SEGMENT_AS_FIELD = "segment_as_field"` in `slayer/cube/report.py` and its uses; before/after entry in `docs/upgrading/2.0.md` (Decisions 5, 13). Verify: test 2.4 passes.

## 4. `tests/` sweep

- [ ] 4.1 Probe the in-scope definitions (design Decision 3) with a throwaway scratchpad script; triage each hit (rename or keep, with reason). Verify: a triaged list exists before any edit.
- [ ] 4.2 Rename the triaged identifiers with `dr-refactor rename-map --docs` in batches, each with Decision 14's `--keep` list; rename the two helper modules without stubs; review every string and comment edit separately from the identifier edits. Verify: `la-typecheck` reports no new errors; `ruff` clean.
- [ ] 4.3 Rename `tests/fixtures/vocabulary_corpus/_cross_model_chain.json` and `_model_access_fixtures.json` to follow their modules and update their `_index.json` entries (Decision 16). Verify: `tests/test_vocabulary_corpus.py` passes.
- [ ] 4.4 Re-run the probe after the rename; write every remaining in-scope old-word definition into design.md's "Kept test identifiers" appendix as `file: name — reason` (Decision 15). Verify: every remaining hit has a reason.
- [ ] 4.5 Respell test-data name literals (Decision 4) by hand, per hit; re-bless goldens and key assertions that move only with a respelled name. Verify: the local test scope passes.

## 5. Finish

- [ ] 5.1 `poetry run ruff check slayer/ tests/`, `la-typecheck` (no new errors, never `--writebaseline`), `poetry run la-arch-check`, `npx likec4 validate architecture`, `openspec validate dev-2364-ossie-vocabulary-rename-the-leftover --strict`. Verify: all pass.
- [ ] 5.2 Local test scope (`la-affected-tests --base main` plus touched goldens and the corpus) green; full suites green in CI.
