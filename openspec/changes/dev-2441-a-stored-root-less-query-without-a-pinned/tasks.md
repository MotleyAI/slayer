## 1. Tests first (pr-tests)

- [ ] 1.1 Write one failing test per scenario of `specs/models/stored-upgrade/spec.md`, each on YAML and SQLite, in a new `tests/test_stored_rootless_datasource.py` over `tests/_stored_upgrade_fixtures.py` (`raw_store`, `memory_v2`, `seed_shop`, `in_datasource`) and `tests/_granularity_rename_fixtures.py` (`write_raw_store`, `memory_doc`, `granularity_definition`); verify each fails on HEAD for the reason the scenario names (the bare-memory repro fails with "names no population"). Gotcha: the repro's `status = 'ok'` logs DEV-2444's false grammar warning; do not assert on its absence.
- [ ] 1.2 Unit-test the shared fallback (no names → every datasource; named → holders; priority then name; spine spelling shadowed by an exact stored spelling → no fallback; explicit missing population → no fallback) and the name reader (each location of design decision 7, raw at an old version and validated give the same names; an unparsable text gives none); verify they fail before the implementation exists.

## 2. Shared fallback and name reader

- [ ] 2.1 Add the fallback (design decisions 2 and 6) in `slayer.storage`, taking the dataset names a query names, the datasets by datasource and the priority; verify task 1.2's fallback tests pass.
- [ ] 2.2 Add the one name reader (design decision 7) for a validated query and for a raw stored query at its version, reading through `core.syntax` after canonicalising legacy spellings, never by regex; verify task 1.2's name-reader tests pass.

## 3. The three readers

- [ ] 3.1 `_datasource_of` calls the fallback when the trigger holds and drops its inline spine branch; verify the root-less, priority, literal, named-dataset, extension, name-order and no-other-datasource scenarios pass, along with the existing `tests/test_stored_upgrade_filter_literals.py` and qualified-reference stored-upgrade tests.
- [ ] 3.2 `_memory_data_sources` returns the fallback's datasource when the trigger holds; verify the root-less and spine custom-bucket scenarios pass.
- [ ] 3.3 The memory check in `_rename_granularities` treats a triggering memory as belonging to the fallback's datasource (priority read from storage, no dataset or memory load); verify the root-less and spine colliding-granularity scenarios and `tests/test_granularity_name_migration.py` pass.
- [ ] 3.4 The missing-explicit-population scenario passes: no reader falls back, and the memory fails to load naming the population.

## 4. Finish

- [ ] 4.1 Docs: if `docs/upgrading/2.0.md` or `docs/concepts/memories.md` describes how a stored memory's datasource is chosen on load, add the root-less/spine rule in one sentence; verify with a grep for "datasource priority" in `docs/`.
- [ ] 4.2 Run the local test scope (`la-affected-tests --base main` plus the stored-upgrade and granularity suites), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline) and `poetry run la-arch-check`; verify all are green.
