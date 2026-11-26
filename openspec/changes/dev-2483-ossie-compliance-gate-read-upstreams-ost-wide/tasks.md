## 1. Corpus (pr-tests stage: the tests read it)

- [x] 1.1 Clone apache/ossie `main`, harvest (`scripts/ossie_schema_drift.py --harvest`), and re-vendor with `scripts/ossie_schema_drift.py --vendor <checkout>`; verify `tests/fixtures/ossie_corpus/compliance/wide_table/` holds `<id>.ost` scripts and no `cases/`, and `provenance.yaml` pins the new commit. Any drift beyond design.md Context items 1–3: triage per design decision 10, or stop and ask.
- [x] 1.2 Update corpus-pinned counts mechanically (e.g. the `examples/` count in `tests/test_ossie_schema_drift.py::test_files_come_from_the_provenance`, 10 → 13); verify the provenance tests' remaining assertions are unchanged in meaning.

## 2. Tests (written failing first)

- [x] 2.1 Reader unit tests (new module): synthetic `.ost` fixtures for a row case (header, rows, NULL, `""`, `"a|b"`, `" a"`, escaped quote, `\uXXXX`, number → Decimal), an error case (code before `:`, code at end of line), data/model from `connect`/`modelFile`, `skip(...)` ignored, `{|…|}`, `{_|…|_}` and tagged fences, `;` inside strings/raw strings/comments, `#` line inside a command, `;` followed by a comment not ending a command; failures naming script and line for: unknown command, zero or two `executeLower`, unknown `connect` name, `executeLower` model ≠ `modelFile` model, malformed table (border, width), error line not matching `E_[A-Z0-9_]+`, error case with a table. Verify they fail before the reader exists.
- [x] 2.2 Every vendored case script reads: a test collects all `compliance/wide_table/*.ost` under the corpus root and asserts each yields a case; `CASE_STATES` still equals the discovered names (`test_every_wide_table_case_is_declared`).
- [x] 2.3 Switch consumers to the reader API: `tests/test_ossie_gate.py`, `tests/test_ossie_gate_compile.py`, `tests/integration/test_integration_ossie_gate.py`, `tests/test_dev2243_wide_table_oracle.py` (scenarios a08, e16, a09 by script; every case names a known fixture), `tests/test_ossie_gate_corpus_root.py` (sentinel copies `a01_shared_dimension.ost` to `a99_sentinel.ost`), `tests/test_ossie_schema_drift.py::test_the_corpus_lists_every_semantic_model_source_and_the_oracles` (expects `.ost` cases with both `a*` and `e*`).
- [x] 2.4 Stale-file test: every file under the vendored corpus except `provenance.yaml` and `harvested/` is listed in the provenance.
- [x] 2.5 Workflow concurrency test: parses every `.github/workflows/*.yml`; every job whose concurrency group can resolve to `bigquery-shared-gcp-project` (case-insensitive, inside expressions too) sets `cancel-in-progress: false` and `queue: max`; the gate's `server` job's BigQuery leg resolves to that group. Verify it fails on the current `ossie-compliance.yml`.
- [x] 2.6 Oracle fix coverage: `tests/test_dev2243_corpus.py::test_every_metric_binds_and_compiles` on the re-vendored `harvested/e6812ba8ec56.yaml` and `harvested/adcaa27290a5.yaml` fails until decision 8 lands; completeness check `test_the_table_covers_the_spec_both_ways` fails until decision 9 lands.

## 3. Implementation

- [x] 3.1 The `.ost` reader in `tests/_ossie_gate_suite.py` (Pydantic `WideCase`; design decisions 4–7), replacing `cases`, `read_expected`, `case_query`; fixture map by `connect` name in `tests/_dev2217_fixtures.py`; engines keyed by loaded model in `tests/_ossie_gate_fixtures.py`. Verify 2.1–2.3 pass.
- [x] 3.2 `_expression_text` in `tests/test_dev2243_corpus.py` per decision 8. Verify 2.6's compile cases pass on every dialect.
- [x] 3.3 `KNOWN_PROSE` in `tests/_ossie_gate_spec.py` per decision 9. Verify the completeness check passes.
- [x] 3.4 `.github/workflows/ossie-compliance.yml`: add `queue: max` to the `server` job's concurrency; verify 2.5 passes.
- [x] 3.5 `docs/interfaces/ossie-compliance.md`: Layer 3 paragraph names upstream's case scripts; vendored revision updated; verify the compliance-page check passes.

## 4. Verification

- [x] 4.1 Local: the tests in scope (`la-affected-tests`) on the vendored corpus, and the in-process gate modules listed in `ossie-compliance.yml`'s `in-process` job with `SLAYER_OSSIE_CORPUS` at a fresh upstream clone (harvested); all pass.
- [ ] 4.2 `workflow_dispatch` of `ossie-compliance.yml` on this branch with `corpus=vendored`: every engine green.
- [ ] 4.3 `workflow_dispatch` with `corpus=upstream`: every engine and the in-process job green.
- [ ] 4.4 Tick DEV-2462's tasks 4.1–4.3 in `openspec/changes/archive/2026-11-25-dev-2462-ossie-layer-1-compliance-gate-every-must/tasks.md`.
