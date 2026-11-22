## 1. Tests (pr-tests stage)

- [x] 1.1 Write `tests/test_law_dataset_objects_validate_assignment.py` per design decision 9 (reachable-model walk from `SlayerDataset` / `DatasourceConfig` stopping at query-side models; frozen or `validate_assignment` + mixin; wrong-typed assignment raises and leaves the object unchanged; minimal-payload table failing on a missing entry; test-only model pinning the candidate commit); verify it fails on `main` for every non-frozen reachable model
- [x] 1.2 Write tests for every scenario in `specs/compatibility/ossie-vocabulary/spec.md`, including the `_from_sql` cases of design decision 5 (dict, prebuilt instance, post-construction `sql` assignment) and the `edit_model` rollback of decision 6; verify each fails on `main` for the right reason (refusal missing, not a fixture error)
- [x] 1.3 Write tests for every scenario in `specs/models/save-validation/spec.md` on YAML and SQLite storage, asserting the backend write is never called on refusal; verify each refusal scenario fails on `main` and the runtime-only scenario passes
- [x] 1.4 Ask the user's approval, naming each file and change, to align `tests/test_ossie_vocabulary_python_names.py:486` (`dataset.columns` case) and `:538` (they assign a field list dropping a relationship's key field); then align them

## 2. Atomic assignment

- [x] 2.1 Add `slayer/core/assignment.py` with `AtomicAssignment` (candidate commit and `assign`, design decision 4); give every in-scope model the mixin and `validate_assignment=True` (decision 8); verify the law test and the assignment scenarios pass
- [x] 2.2 Make the validators pure (design decision 5): sql shorthand on dict input in the wrap validator, `NumberFormat` symbol default in a before validator, element-type default by list-item copy; verify the construction-normalization and post-construction `sql` scenarios pass and `poetry run pytest tests/test_ossie_vocabulary_python_names.py` passes

## 3. Linked edits

- [x] 3.1 Route `edit_model`'s source switch (`slayer/mcp/server.py:1105-1125`, four attributes) and the deprecated `join_pairs` setter through `assign` (design decision 6); verify the source-switch and `join_pairs` scenarios pass
- [x] 3.2 Audit production code for other consecutive writes to attributes a cross-attribute check reads (e.g. grep `\.(sql_table|sql|source_queries|from_columns|to_columns|fields|data_source)\s*=` under `slayer/`); route each through `assign` with a test; record the sites found here; verify the audit list is recorded and each site's test passes
  - Sites: `slayer/dbt/converter.py` and `slayer/cube/converter.py` (`col.datatype, col.element_type = found`) → `col.assign(...)`. Both type a fresh field whose `element_type` is still unset, so no intermediate state fails today; covered by the existing converter tests. Every other assignment under `slayer/` writes one attribute.

## 4. Save-time re-validation

- [x] 4.1 Add the raw-value rebuild check (design decision 7) and call it first in `save_dataset` (when `_validate`), `save_dataset_extensions` and `save_datasource` in `slayer/storage/base.py`; verify the save-validation scenarios pass

## 5. Architecture, docs, coordination

- [x] 5.1 Add the approved principle (design decision 2, verbatim, numbered next after `main`'s highest) to `architecture/python.core.arc42.md` §3; verify `poetry run la-arch-check` passes
- [x] 5.2 Add a Python API entry to `docs/upgrading/2.0.md` (assignments validated; linked edits via `assign`; saves re-validate; in-place edits caught at save until DEV-2401); verify by reading the rendered section
- [x] 5.3 Comment on DEV-1695 naming `slayer/core/assignment.py`, `assign`, and the candidate commit replacing its snapshot/restore; verify the comment is posted

## 6. Gates

- [x] 6.1 Run the local test scope (`la-affected-tests --base main` plus storage, MCP and REST dataset/datasource tests), `poetry run ruff check slayer/ tests/`, `la-typecheck`, `poetry run la-arch-check` and `npx -y likec4@1.47.0 validate architecture`; verify all pass
