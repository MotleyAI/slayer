## Context

See proposal.md › Why. DEV-2318's word table and carve-outs apply unchanged: model → dataset, column → field, measure → metric, join → relationship, formula → expression. Kept: physical and result-set columns, SQL `JOIN` as an operation, RLS / session-policy names, foreign formats (dbt, Cube, Metabase, OSI), Ossie's own spellings (`from_columns`, `to_columns`, `column_pairs`), Pydantic `model_*`, the embedding model, the storage layout (`models/`) and stored-document upgrade paths.

The guard (`tests/test_vocabulary_identifier_guard.py`, `tests/_vocabulary_guard.py`, `tests/fixtures/vocabulary_identifier_allowlist.yaml`) scans every binding in `slayer/`, locals included, and matches entries on file and name. It is green on the merged tree with 37 stragglers.

The branch carries DEV-2277 (in progress, local `7a5fb8aa9`) and `origin/main` (`abccc2074`). DEV-2277's test respell (its task 4.1) is still open, so later DEV-2277 commits touch the same test files.

Principles that apply: `system.arc42.md` §3 P1 (every top-level package claimed; none added), P2 (model truth; no runtime import edge changes, test modules are not modelled) and P6 (Pydantic, never dataclasses). No arc42 or LikeC4 edit; no new capability, so no spec attachment.

## Goals / Non-Goals

**Goals:**

- `slayer/`: zero stragglers; the guard green with `carve_outs` only.
- `tests/`: every in-scope definition (Decision 3) spelled in the new vocabulary or listed in the appendix with its reason.

**Non-Goals:**

- A guard over `tests/` (Decision 1).
- Locals anywhere in `tests/`, test function and class names, `test_*.py` file names, fixture data paths (Decisions 2, 6).
- `tests/perf/compare/` (the pinned PyPI engine reads its spelling).
- Re-triaging existing `slayer/` carve-outs.
- Read compatibility for old Cube report files (Decision 13).

## Decisions

1. **The `tests/` rename is a one-time sweep with no `tests/` guard,** "for now". Alternative: extend the guard to the in-scope `tests/` kinds with its own `carve_outs`. (user)
2. **Locals inside helpers and fixtures are exempt,** like test locals; only the helper interface (name, parameters, constants, class fields) is renamed. Alternative: rename them as in `slayer/`. (user)
3. **In scope in `tests/`** (except `tests/perf/compare/`), when naming a SLayer concept with an old word: helper function names; helper class names and their methods; non-test methods inside test classes; fixture names, with the parameters of the tests requesting them; module constants; class fields of helper and test classes; parameters of helpers, fixtures and helper methods (e.g. `joins=` of dataset-building fixtures → `relationships=`); `import … as` aliases; the helper modules `tests/_model_access_fixtures.py` → `tests/_dataset_access_fixtures.py` and `tests/_cross_model_chain.py` → `tests/_cross_dataset_chain.py`. (user, approved plan)
4. **Test-data name literals are in scope** (DEV-2347 decision 12): dataset and field name literals naming a SLayer concept with an old word are respelled (`no_such_model` → `no_such_dataset`, `hidden_model` → `hidden_dataset`, `via_join` → `via_relationship`). Kept: literals testing old spellings (legacy-conversion, deprecation, stored-upgrade tests), legacy-store fixtures, foreign formats and the guard's synthetic sources. Goldens and key assertions that move only with a respelled name are re-blessed. Alternative: out of scope. (user)
5. **`CubeIssueCategory.SEGMENT_AS_COLUMN = "segment_as_column"` becomes `SEGMENT_AS_FIELD = "segment_as_field"`,** with a before/after entry in `docs/upgrading/2.0.md`. Alternative: rename the member only and keep the value. (user)
6. **`test_*.py` file names and fixture data paths keep their words:** they are test names and mirror `slayer/` modules (`slayer/core/models.py` stays), Cube's layout (`cube_project/model/`) or the storage layout (`yaml_store/models/`). (default)
7. **Renamed test helper modules leave no `raise_moved` stub;** all 31 importers are updated. Stubs serve users' imports of `slayer.*`; `tests/` is not packaged. Alternative: stubs plus tripwire pins until 3.0. (user)
8. **No deprecated twins:** no in-scope name is on DEV-2318's curated twin list, and Storyline uses none (grep of `retyped_sql_columns`, `CubeIssueCategory`, `segment_as_column`). (default)
9. **A mixed-use name** (straggler and carve-out uses in one file, e.g. `live_schema/ingestion.py: columns`, `sql/generator.py: column`) **has its straggler uses renamed and moves to `carve_outs` with its reason,** per the DEV-2364 comment of 2026-11-19. (default)
10. **The straggler machinery is deleted outright once the list is empty:** `Straggler`, `VocabularyAllowlist.stragglers`, `guard_problems`' `ceiling`, the ceiling tests and `x-vocabulary-stragglers` in `architecture/index.yaml`. `extra="forbid"` then refuses a `stragglers:` key. The aligned guard tests and the `index.yaml` line deletion were approved with the plan. (default, user approved edits)
11. **DEV-2277 is merged again at the start of `pr-tests` and of `pr-implement`** (or `origin/main`, once DEV-2277 has landed). (default)
12. **`skip_specs: true`:** no specified behaviour changes. (default)
13. **The Cube report accepts only the new value;** old report files holding `segment_as_column` no longer validate into `CubeConversionResult`. SLayer only writes that file (`slayer/cli.py`), never reads it back. This is an unspecified output change, recorded here and in the upgrade notes. Alternatives: accept the old value with a deprecation until 3.0; keep the old value. (codex, user)
14. **Every `dr-refactor rename-map --docs` run `--keep`s the test files and directories that hold old spellings on purpose** (legacy-input and deprecation tests such as `tests/test_ossie_vocabulary_fresh_input.py`, stored-upgrade tests, `tests/fixtures/vocabulary_corpus/`, `tests/fixtures/upgrade/`, the guard's synthetic sources), along with archived OpenSpec changes. Every string and comment edit is reviewed separately from the identifier edits. (codex)
15. **After the rename, the AST probe is re-run** with Decision 3's exact scope and exemptions (a throwaway scratchpad script, not committed). Its output is this file's "Kept test identifiers" appendix; every remaining hit carries a reason. (codex)
16. **The two renamed helper modules' corpus files follow them:** `tests/fixtures/vocabulary_corpus/_cross_model_chain.json` → `_cross_dataset_chain.json` and `_model_access_fixtures.json` → `_dataset_access_fixtures.json`, with their `_index.json` `source` and `corpus` entries updated. (codex)
17. **The malformed-allowlist test gains a `removed-stragglers-key` case `{"stragglers": []}`,** and a positive test asserts that `{}` parses, so a missing `carve_outs` stays valid. (codex)

## Risks / Trade-offs

- [A carve-out renamed by mistake: a physical column, an SQL join, an RLS name, a dbt or Cube object] → per-hit triage against the carve-out table; `dr-refactor` stand-ins triaged one by one; goldens, message-pinning tests and the vocabulary corpus catch changed behaviour.
- [`--docs` rewrites a deliberate old spelling] → Decision 14's `--keep` list and the separate review of string edits.
- [Merge conflicts with DEV-2277's open test respell] → Decision 11; renames are re-applied from the map if a merge reverts them.
- [Old words return in new test helpers] → accepted (Decision 1).

## Migration Plan

No stored data or wire key changes. The Cube report category value changes (Decision 13). Rollback is a revert.

## Kept test identifiers

Filled in by task 4.4 from the post-rename probe: `file: name — reason`, grouped by reason.
