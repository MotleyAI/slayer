## 1. Tests (pr-tests)

- [x] 1.1 Write `tests/test_dev2477_model_metric_guard.py` with one test per scenario of `specs/models/semantic-model-documents/spec.md` › Explicit changes never break a model-scoped metric, over YAML and SQLite storage (the `STORAGES` / `DS` / `SALES` fixtures of `tests/_dev2243_fixtures.py`); verify each fails on the current code for the stated reason.
- [x] 1.2 Cover the explicit write paths of design decisions 5, 11, 13, 14: `save_dataset`, `save_dataset_extensions`, `delete_dataset`, `save_semantic_model_text` / `create_semantic_model` / `adopt_semantic_model`, `edit_semantic_model` (metric removal and replacement), `delete_semantic_model`, MCP `edit_dataset` with `new_data_source`, the engine's query-backed datasource move, and a `_validate=False` save; verify each refusal writes nothing.
- [x] 1.3 Cover the database-driven paths (ingest, `apply_drift_deletes`, `apply_untyped_findings`): they apply, warn with `StoredDependentUnresolvedWarning` naming each broken metric, move the transitive closure of affected core metrics into `SLAYER` (stored document passes Ossie validity), and the metrics load as unloadable with chain diagnostics.
- [x] 1.4 Cover hidden content through `AccessFilteredStorage` (delete, save, document save/edit/delete): a hidden broken metric gives `HiddenContentConflictError` with no hidden name in the message (`foreign_leaks`-style check).
- [x] 1.5 Cover surfaces: REST `DELETE /datasets/{name}` and `DELETE /semantic-models/...` answer 400 naming the metric (and a mapping break, previously 500); MCP `delete_dataset` / `delete_semantic_model` / `edit_dataset` / `edit_semantic_model` return the refusal text; CLI `datasets delete` / `semantic-models delete` exit non-zero printing it.
- [x] 1.6 Align the approved existing tests (design decision 3): the six `tests/test_dev2243_document_edits.py` dataset-delete tests remove `revenue_per_customer` first; `tests/test_dev2243_save_scope.py::TestBrokenDependents::test_renaming_a_referenced_field_warns_about_dependents` becomes `test_renaming_a_referenced_field_is_refused_naming_dependents`; `tests/test_dev2474_storage.py` follows the renamed error/warning. Any further existing test failing for the same reason is reported for approval, never changed silently.

## 2. One state, one guard (pr-implement)

- [x] 2.1 Generalise `MappingScope` into `DatasourceScope` in `slayer.core` (documents hold datasets and model-scoped metrics) and move the generic parts of `slayer/storage/ontology_documents.py` to `slayer/storage/dependents.py`, renaming `OntologyMappingBreakError` → `StoredDependentBreakError`, `BrokenMapping` → `BrokenDependent`, `OntologyMappingUnresolvedWarning` → `StoredDependentUnresolvedWarning` via `la:deterministic-refactor` (`dr-refactor` dry-run → review → `--apply`); verify `la-typecheck`, `dr-mock-lint` and the DEV-2474 tests pass.
- [x] 2.2 Add the per-reference resolver in `slayer/storage/model_metric_rules.py` and rebuild `check_model_metric`'s reference checks on it; verify the existing DEV-2243 authoring tests pass unchanged.
- [x] 2.3 Replace `_guard_mappings` with `_guard_dependents` (mapping check + metric check, transitive, new-problem diff), run unconditionally on every durable dataset and document write listed in 1.2; delete the `_delete_document_dataset` warning and `warn_broken_model_metrics`; verify 1.1/1.2 pass.
- [x] 2.4 Preflight the deletion guard in MCP `edit_dataset` moves and the engine's query-backed datasource move; verify the refused-move tests pass.

## 3. Database-driven writes and load (pr-implement)

- [x] 3.1 Generalise `without_dataset`'s metric move: a database-driven write moves the transitive closure of affected core metrics into the document's `SLAYER` entry; verify 1.3 passes, including Ossie validity of the stored text.
- [x] 3.2 Make `_admit_metrics` use the resolver: unresolved metrics (and their transitive readers) are recorded as metric load failures, never written back; verify 1.3 load assertions and `tests/test_law_load_never_writes.py` pass.
- [x] 3.3 Compare reachable unresolved references before and after a change (design decision 6); verify the "Making a metric read an unresolved metric is refused" scenario passes.

## 4. Access and surfaces (pr-implement)

- [x] 4.1 Extend `AccessFilteredStorage._unbroken` to broken metrics (hidden iff not in the caller's `visible_model_metrics`) and wrap every write path of 1.2 not yet wrapped; verify 1.4 passes.
- [x] 4.2 Map the break error and `HiddenContentConflictError` to 400 on REST deletes, return refusal text from the MCP tools, exit non-zero in the CLI; verify 1.5 passes.

## 5. Docs and gates (pr-implement)

- [x] 5.1 Update `docs/concepts/semantic-models.md` ("Deleting" paragraph and editing: refusals, database-driven warning, unloadable metrics), `docs/reference/rest-api.md` and `docs/reference/mcp.md` delete responses; verify no doc still says deletion warns about model-scoped metrics.
- [x] 5.2 Run the local test scope, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), and the enforcement bundle (`la-arch-check`, LikeC4 validation); verify all green.
