## 1. Base

- [x] 1.1 Merge the DEV-2277 branch (fast-forward to `7a5fb8aa9`) and origin/main into this branch; verify `git log` shows both.
- [ ] 1.2 At each later stage start, re-merge the DEV-2277 branch and origin/main (merge, never rebase); verify `git merge-base --is-ancestor` for both heads.

## 2. Guard test (pr-tests)

- [x] 2.1 Write `tests/test_field_expression_spelling_guard.py` (design decisions 2, 16, 17): AST scan of `tests/**/*.py` failing with `file:line` on a field-level `sql` spelling — `sql=` on `DatasetField`/`Column` (import aliases resolved) or on a test helper forwarding `sql` into one, a `"sql"` key in a field dict (in a `fields` list, a `model_validate` field payload, or `model_copy(update=…)` on a field) — outside an inline allowlist of `path::function` entries, each with expected count and reason; a count mismatch fails. Verify: it fails on the current tree, listing the expected sites.
- [x] 2.2 Detector unit tests on synthetic snippets for every form in 2.1 (positive and negative: `AggregationParam(sql=)`, `SlayerDataset(sql=)`, Cube models, Ossie `{sql, dialects}` objects are not flagged). Verify: they pass.
- [x] 2.3 Populate the allowlist with the deprecated-spelling tests (design decision 3, 18), each with its count and reason. Verify: every entry names a test whose assertion depends on the deprecated input.

## 3. Value pass (pr-implement)

- [x] 3.1 Scratch recording plugin wrapping `slayer.sql.definitions.with_converted_sql` (decision 14): per construction record the literal's AST node (file:line:col), test node id, original text, dataset, sibling set, `ds_type`, live column types, converted text. Run only the tests constructing a non-identifier site (its enclosing test, else one test reaching its helper or fixture; ~620 tests), never a whole suite. Verify: a record file covering every observed site.
- [x] 3.2 Static inventory of every field-level `sql` site in `tests/` (Python only; YAML unchanged, decision 5), classified by `unquoted_identifier` (decision 19); reconcile each non-identifier site as observed, statically classified, or deprecated-spelling exemption; extend each site's dialect set with the backends of CI-only test files importing its module (decision 15). Verify: zero unreconciled sites.
- [x] 3.3 Decide each non-identifier site by the end-to-end comparison (decisions 1, 11, 15): `resolved_dataset` as written vs with the replacement, every field of the dataset, every dialect in its set (unobserved backends offline with that `ds_type` and recorded live types); portable iff all equal after the same sqlglot normalisation, else one dialect entry per dialect; never-resolved sites portable; conflicting outputs per AST node flagged for call-site rewriting. Verify: a substitution summary (file:line, old → new, rule).
- [x] 3.4 Classify each `.sql` read on a `DatasetField`/`Column` (basedpyright references) outside deprecated-spelling tests and propose its intent-specific rewrite (decision 12); list tests whose expected value would change. Verify: entries in the summary.
- [ ] 3.5 Show the owner the substitution summary and the changed-expectation list; wait for consent on the latter. Verify: owner's answer recorded on the tracker issue.
- [x] 3.6 Apply the value pass, including dict-literal fields (key and value, decision 6) and raw payloads by version (decision 18); re-run every touched test. Verify: touched tests pass; no non-identifier field `sql` remains outside the allowlist.

## 4. Keyword pass (pr-implement)

- [x] 4.1 Load `la:deterministic-refactor`; dry-run `dr-refactor retarget --file slayer/core/models.py --symbol DatasetField.sql --target DatasetField.expression` with `--keep slayer --keep examples`, `--keep` per allowlisted site, `--without` per other class's `.sql`; `--allow-blindspots` for the untyped `.sql` receivers in tests, each checked to be an engine response (`execute`/`execute_sync` result), a sqlglot node or a `TypeProbe`, never a field; include helper `sql` parameters forwarding into a field (decision 7). Verify: the dry-run diff holds only constructor-keyword and dict-key edits (decision 13).
- [x] 4.2 Review, then `--apply`; run the skill's full gate. Verify: `la-typecheck` no new errors (baseline aligned by the tool, never `--write-baseline`), `dr-mock-lint` clean.
- [x] 4.3 `render_field_text` renders the field's `expression` (portable text and each dialect entry) as well as a deprecated `sql`; unit tests for both forms. Verify: `test_search_render.py` passes.
- [x] 4.4 The generator's three fallback reads of a field's deprecated `sql` (aggregate-spec builder, root value, bucket source in `slayer/sql/generator.py`) read `definition_sql`; a unit test on an unresolved `expression` field fails without it. Verify: `test_agg_render_spec.py` passes.

## 5. Finish

- [x] 5.1 Guard test green with only the allowlist; no field-level `sql=` / `"sql"` field key in `tests/` outside it. Verify: `poetry run pytest tests/test_field_expression_spelling_guard.py`.
- [x] 5.2 Local test scope per the flow (`la-affected-tests` + touched tests) passes; ruff, `la-typecheck`, `la-arch-check`, LikeC4 validation, `openspec validate --strict` clean. Verify: each command's output.
- [ ] 5.3 CI green, including the path-gated per-backend integration workflows (decision 20). Verify: `la-fetch-failed-pr-checks` reports none.
