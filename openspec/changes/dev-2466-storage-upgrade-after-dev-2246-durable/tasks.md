## 1. Tests first (pr-tests)

- [x] 1.1 Rename record and map: tests for `renamed_from` set on load, a record deciding its former name, first record wins, the taken-name rule, warning only for a pending rename. YAML and SQLite. Verify: they fail on `main`'s code.
- [x] 1.2 Version gate: a dependent below the gate is renamed; a v30 dataset with the library `left(name, 2)` is read as written, for both a pending and a recorded rename; a nested stage is judged by its own version; a document without a version counts as old; an old document arriving after the upgrade is renamed. Verify: they fail before implementation.
- [x] 1.3 Library-growth guard (`tests/test_library_growth.py`): every reserved granularity name has a gate; a simulated reservation without a gate is caught; a simulated gate not strictly above the earlier gates is caught; the later-library-addition scenario with a gate above the current versions. Verify: they fail before implementation.
- [x] 1.4 Upgrade command: writes in any order; an interruption at either point converges; a conflicted dependent no longer holds the datasource (stored with the record, exit 1). Verify: they fail before implementation.
- [x] 1.5 Datasource save: a loaded datasource with a pending rename saves and stores the record; a config built without the record keeps it; `renamed_from` naming no reserved name is refused; a duplicate former name is refused, naming the second. YAML and SQLite, through the client save path and the MCP/REST edit paths. Verify: they fail before implementation.
- [x] 1.6 sql-mode drift and typing: a quoted base field is not dropped, is typed from its column, and is dropped with its metric when the column vanishes. Verify: they fail before implementation.
- [x] 1.7 `upgrade_datasets`: `[good, bad-datatype]` returns the good one and refuses the bad one with its reason; parity of the live-refinement and dialect reasons with `slayer storage upgrade`. Verify: they fail before implementation.
- [x] 1.8 Undecodable memory: `get_memory` raises a stored-document load error naming the id; listing collects it as a failure; a whole-store upgrade reports it and upgrades the rest (exit 1); a `--data-source` upgrade completes. Verify: they fail before implementation.
- [x] 1.9 Upgrade-needed flag: a current datasource with `user` warns and the upgrade stores `username`; a granularity without `origin` does not warn; the enumerating test over every `mode="before"` validator reachable from the stored models (`REWRITING` exercised and marking; `NOT_REWRITING` with a reason). Verify: they fail before implementation.
- [x] 1.10 Ask approval, naming each file, to align the existing tests that contradict the plan: the pending-rename refusal (`tests/test_datasource_save_pending_rename.py`), the dependents-first order and held datasource (`tests/test_storage_upgrade.py`), the gate-free rename expectations (`tests/test_granularity_name_migration.py`, `tests/test_law_load_never_writes.py`, `tests/test_dev1920_stored_colon_upgrade.py`, `tests/test_stored_rootless_datasource.py`), and the later-library-addition test. Gotcha: fixtures that store dependents at the current version with a reserved-named granularity must be written below the gate.

## 2. Rename record, map and gate

- [x] 2.1 `CustomGranularity.renamed_from`; the datasource 3 → 4 migration (version stamp only); `CURRENT_VERSIONS["DatasourceConfig"] = 4`. Verify: the 1.1 record tests pass.
- [x] 2.2 The rename map (pending plus records) and the per-name gate table in `slayer/storage/granularity_rename.py`; `renamed_datasource` records `renamed_from`. Verify: 1.1 passes.
- [x] 2.3 Apply the gate in the dataset, stage and memory rewrites (`_upgraded_dataset`, `_memory_with_granularities_renamed`, `rename_in_query` for nested stages). Verify: 1.2 passes.
- [x] 2.4 Rename warning only for pending renames. Verify: the 1.1 warning tests pass.
- [x] 2.5 The library-growth guard checks for gates. Verify: 1.3 passes.

## 3. Upgrade command and pure entry point

- [x] 3.1 Remove `_held_by`, the dependents-first split and the "waits for" detail from `upgrade_store`. Verify: 1.4 passes.
- [x] 3.2 One per-document upgrade-and-refusal helper used by `_planned` and `_upgrade_datasets`. Verify: 1.7 passes.

## 4. Datasource save

- [x] 4.1 `save_datasource` carries stored records over; delete `_check_no_pending_rename`; add the `renamed_from` rules to `check_granularities`. Verify: 1.5 passes.

## 5. Drift, memories and the change flag

- [x] 5.1 sql-mode drift and retyping match base fields by `physical_name`. Verify: 1.6 passes.
- [x] 5.2 Decode memory files inside the stored-document boundary; `_raw_memory_dicts` skips undecodable files. Verify: 1.8 passes.
- [x] 5.3 The stored-input rewrite marker and collector; mark `user` → `username`; OR the collector into the three `_upgraded_*` change flags. Verify: 1.9 passes.

## 6. Docs

- [x] 6.1 `docs/concepts/datasets.md` rename paragraph (record, old documents only); `docs/upgrading/2.0.md` (drop "until it does, saving that datasource is refused"); `docs/development.md` (a new reservation needs a strictly higher rename gate); document `renamed_from` wherever datasource granularities are documented. Verify: `rg -n "saving that datasource is refused" docs/` prints nothing, and every changed page is in `zensical.toml` nav.

## 7. Gates

- [x] 7.1 Local test scope green (`la-affected-tests --base main` plus the files above); `poetry run ruff check slayer/ tests/`; `la-typecheck` with no new errors; `la-arch-check`. Verify: all pass.
