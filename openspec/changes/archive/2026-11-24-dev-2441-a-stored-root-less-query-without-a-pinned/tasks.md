## 1. Tests first (pr-tests)

- [x] 1.1 Write one failing test per scenario of `specs/models/stored-upgrade/spec.md`, each on YAML and SQLite, in a new `tests/test_stored_rootless_datasource.py` over `tests/_stored_upgrade_fixtures.py` (`raw_store`, `memory_v2`, `seed_shop`, `in_datasource`) and `tests/_granularity_rename_fixtures.py` (`write_raw_store`, `memory_doc`, `granularity_definition`); entity-pinned memories carry `<datasource>.<dataset>[.<leaf>]` entities (an `entities` argument on `memory_v2` and `memory_doc`); verify each fails on HEAD for the reason the scenario names (the bare-memory repro fails with "names no population"). Gotcha: the repro's `status = 'ok'` logs DEV-2444's false grammar warning; do not assert on its absence.
- [x] 1.2 Unit-test the trigger (no population, spine spellings, exact stored spine spelling, stored or missing explicit population, inline dataset) and the raw datasource reader (dataset- and leaf-level entity pin; bare `<datasource>`, `memory:`, missing-dataset and other-case entities ignored; two datasources → none; else the only datasource; else none); verify they fail before the implementation exists.

## 2. Shared datasource reader

- [x] 2.1 Add the raw reader of a triggering memory's datasource (design decisions 2 and 6) in `slayer.storage`, over the raw memory dict and the dataset identities; verify task 1.2's tests pass.

## 3. The three readers

- [x] 3.1 `_memory_from_stored` passes a triggering memory's datasource as `data_source` to its conversion, and refuses a pending one without a datasource (decision 7); `_datasource_of` drops its inline spine branch; verify the single-datasource, entity-pin, literal, pinned-only and refusal scenarios pass, along with the existing `tests/test_stored_upgrade_filter_literals.py` and qualified-reference stored-upgrade tests.
- [x] 3.2 `_memory_data_sources` returns `[datasource]` for a triggering memory, or `[]` without one; verify the root-less, spine and entity-pinned custom-bucket scenarios pass.
- [x] 3.3 The memory check in `_rename_granularities` treats a triggering memory as belonging to its datasource only (no dataset or memory load); verify the root-less, spine and entity-pinned colliding-granularity scenarios and `tests/test_granularity_name_migration.py` pass.
- [x] 3.4 The missing-explicit-population scenario passes: no reader falls back, and the memory fails to load naming the population.

## 4. Finish

- [x] 4.1 Docs: add the rule in one sentence to the stored-query upgrade paragraph of `docs/upgrading/2.0.md` (an unpinned root-less or spine memory is read in the datasource its entities record, else the store's only datasource, else it fails to load).
- [x] 4.2 Run the local test scope (`la-affected-tests --base main` plus the stored-upgrade and granularity suites), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline) and `poetry run la-arch-check`; verify all are green.
