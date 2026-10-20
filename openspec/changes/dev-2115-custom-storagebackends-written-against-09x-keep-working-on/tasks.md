## 1. Architecture

- [x] 1.1 Add `specs ['storage']` to the `storage` node metadata in `architecture/model.c4` (user-approved edit); verify `poetry run la-arch-check` passes.

## 2. Tests (pr-tests stage)

- [x] 2.1 Create `tests/test_storage_backend_compat.py` (unit test, no `integration` marker) with a dict-backed in-memory test backend implementing every abstract hook. The 0.9.x variant has NO `_save_datasource_impl` anywhere before `StorageBackend` and a `_list_memories_rows` returning a list; the new-style variant implements `_save_datasource_impl` and returns `(rows, failed_rows)`. Verify the file collects.
- [x] 2.2 R1 tests, one per scenario in `specs/storage/custom-backends/spec.md`: definition-time `FutureWarning` content (class, `save_datasource`, `_save_datasource_impl`, 2.0); empty `__abstractmethods__` and instantiation; validation then the old body; an invalid granularity rejected before the old body; a 0.9.x child of a 0.9.x backend, with and without `super()`, validates, saves a valid datasource without recursion, and warns; a new-style backend gets no warning; a bundled-backend (`YAMLStorage` on `tmp_path`) subclass's `save_datasource` override runs untouched with no warning. Verify each test fails before implementation.
- [x] 2.3 R2 tests: `list_memories` and `load_memories` with 0, 1, 2 and 3 memories on the list-returning backend (confirm the 2-memory case fails on current `main` code); one `FutureWarning` per defining class across repeated calls under `warnings.simplefilter("always")`; `TypeError` naming the defining class for a dict, a generator, a 3-tuple, and 2-tuples whose items are not both lists; a new-style backend with one unloadable memory returns it as a failed row through `load_memories`, skips and reports it through `list_memories`, and emits no `FutureWarning`. Verify each test fails before implementation.

## 3. Implementation (pr-implement stage)

- [x] 3.1 Create `slayer/storage/legacy_backends.py` with the two adapters (design D2–D4): nearest-`_save_datasource_impl` classification with an adapter-made marker, rebind-and-delete for root 0.9.x classes, check-first wrapper for 0.9.x children, the `_list_memories_rows` async `functools.wraps` wrapper with a warned-once flag per defining class, and `FutureWarning` messages. Verify the R1/R2 tests pass.
- [x] 3.2 Add `StorageBackend.__init_subclass__(**kwargs)` in `slayer/storage/base.py`, calling `super().__init_subclass__(**kwargs)` then both adapters, with imports at the top. Verify the bundled YAML / SQLite / access-filtered backends still pass `tests/test_memories_storage.py` and `tests/test_model_access.py`.

## 4. Docs

- [x] 4.1 Rewrite the Custom Backends example in `docs/configuration/storage.md`: every abstract hook with its current signature (`_save_datasource_impl`; the memory hooks with `_list_memories_rows -> Loaded[Memory]`; `update_column_sampled`; `get_datasource_priority`, `_set_datasource_priority_raw`; the four embedding hooks) plus the imports it needs (`Loaded`, `Memory`, `Embedding`). Point the `check_datasource_id_collision` sentence at `_save_datasource_impl`. Verify a class copied from the example (bodies filled with `...`) has empty `__abstractmethods__`.
- [x] 4.2 Add the single-sentence upgrade note: "A backend written against 0.9.x that overrides `save_datasource` or returns a plain list from `_list_memories_rows` keeps working, with a `FutureWarning`, until 2.0 — move it to `_save_datasource_impl` and the `(memories, failed_rows)` return shape, and override only the hooks listed above." Verify by reading the rendered section.

## 5. Gates

- [x] 5.1 Run `poetry run pytest -m "not integration"` (full unit suite), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs the baseline) and `poetry run la-arch-check`; all green.
- [x] 5.2 Add a "Release note" line to the PR description (release notes files stay untracked).
