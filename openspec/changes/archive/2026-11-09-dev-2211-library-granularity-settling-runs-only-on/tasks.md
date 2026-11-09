## 1. Tests (pr-tests)

- [x] 1.1 In `tests/test_storage_backend_compat.py`, add a dict-backed custom backend whose `get_model`/`load_models` decode current-version model dicts through `_migrate_and_refine_on_load` and whose memory rows decode through `_memory_from_stored`, counting `list_datasources`, `get_datasource` and `_raw_datasource_dicts` calls; assert, model first on one fresh instance and memory first on another, that the settle ran (`_raw_datasource_dicts` called) and `list_datasources`/`get_datasource` were never called. Verify it fails on the unchanged code.
- [x] 1.2 In `tests/test_granularity_name_migration.py`, for both backends, a model-first and a memory-first load (no prior `get_datasource`) of a store defining `contains` sees `contains_granularity` and the stored datasource is renamed — reuse `TestContains` if it already covers this exactly, else add it.
- [x] 1.3 In `tests/test_granularity_name_migration.py`, for both backends: an unparseable datasource document (bad YAML / bad JSON) beside a datasource defining `contains`, model load first → succeeds and renames.
- [x] 1.4 Same as 1.3 with a datasource document that parses to a mapping which is not a valid datasource.
- [x] 1.5 In `tests/test_granularity_name_migration.py`, YAML only: `datasources/shop.yml` defining `contains`, no `shop.yaml`, model load first → `shop.yml` byte-unchanged and no `shop.yaml` created.

## 2. Implementation (pr-implement)

- [x] 2.1 `slayer/storage/base.py`: add `_raw_datasource_dicts()` (default `{}`) next to `_raw_memory_dicts`; rewrite `_settle_library_granularities` to loop over it with `contextlib.suppress(ValueError)` per document, keeping the lock, `_SETTLING` guard and `_granularity_renamed_docs`; verify tests 1.1–1.5 pass.
- [x] 2.2 `slayer/storage/sqlite_storage.py`: override via `_raw_rows_sync("SELECT name, data FROM datasources ORDER BY name")`; verify the SQLite cases of 1.2–1.4.
- [x] 2.3 `slayer/storage/yaml_storage.py`: override over `datasources/<name>.yaml` only, skipping names `_datasource_path` refuses, YAML parse errors and non-mapping documents; verify the YAML cases of 1.2–1.5.
- [x] 2.4 `docs/configuration/storage.md`: one sentence — stored-document upgrades that rewrite raw documents (such as renaming a granularity a newer release reserves) run only on backends exposing raw documents; a backend building its own configs is never asked to list or load datasources for them.

## 3. Gates

- [x] 3.1 `poetry run pytest -m "not integration"` passes in full.
- [x] 3.2 `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new errors vs baseline) pass.
- [x] 3.3 `poetry run la-arch-check` passes.
- [x] 3.4 The PR targets `1.x`, not `main`.
