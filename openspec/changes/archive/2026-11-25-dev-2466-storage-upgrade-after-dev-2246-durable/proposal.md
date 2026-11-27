## Why

Since loads never write, a half-upgraded store is a normal state. The library-granularity rename is the one upgrade step that depends on another document: its map is read from the stored datasource, and storing the upgraded datasource erases it. Correctness therefore depends on write order. An old document that arrives later, or an upgrade interrupted at the wrong moment, reads a former custom granularity with a library function's meaning. Several smaller defects from the same review also need fixing: sql-mode drift drops quoted base fields, one bad document fails a whole `upgrade_datasets` batch, an undecodable memory file stops the upgrade, a validator-level rewrite raises no upgrade warning, and three spec texts describe the wrong behaviour.

## What Changes

- A renamed custom granularity records its former name as `renamed_from`. The datasource version steps 3 → 4.
- The rename map is the pending reserved names plus the records. A dependent is renamed only if it was written below the versions current when that name was reserved. Today's names: dataset 30, query 19, memory 14. The library-growth guard requires a gate for every reserved name, and each later gate is strictly higher.
- `slayer storage upgrade` writes documents in any order and holds nothing back. The "waits for" outcome is removed.
- A datasource save adds a missing `renamed_from` from the stored document and is no longer refused while a rename is pending. **BREAKING** for a client that relied on the refusal. `renamed_from` must name a reserved name and be unique; otherwise the save is refused.
- sql-mode schema drift and live typing match a base field by the physical column it reads, so `"amount"` matches `amount`.
- `upgrade_datasets` refuses each failing document on its own, with the same reasons `slayer storage upgrade` gives.
- A memory file that is not UTF-8 is reported as an unreadable document. It no longer aborts loads or the upgrade.
- A stored-input rewrite done inside a model validator (a datasource's `user` → `username`) counts as a change, so the load warns and the upgrade persists it.
- Spec wording: the legacy spine-join scenario's result, `--dry-run`'s writes on open, and the flat-layout move filling `data_source`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `models/stored-upgrade`: the granularity rename (records, version gate, library-growth gate), the upgrade command (no write order, dry-run wording, unreadable memory), the datasource save (refusal removed, records carried over, save-time rules), the pure entry point (per-document refusals), the upgrade-needed warning (validator rewrites), the flat-layout scenario.
- `models/schema-drift`: sql-mode drift and retyping of quoted base fields.
- `cli/commands`: `--dry-run` wording.
- `queries/time-spine`: the result of a query over a model whose spine join was dropped.

## Impact

- Code: `slayer/core/granularity.py` (`renamed_from`, save-time rules), `slayer/core/models.py` (datasource validator marker), `slayer/storage/granularity_rename.py` (map and gates), `slayer/storage/base.py` (map, gate, datasource save, change flag), `slayer/storage/migrations.py` (datasource version 4), `slayer/storage/upgrade.py` (ordering and holds removed, shared refusal path), `slayer/storage/yaml_storage.py` (undecodable memories), `slayer/live_schema/schema_drift.py`.
- Stored format: datasources gain `renamed_from` on renamed granularities and step to version 4. A 2.0.x reader refuses a v4 datasource as newer than it supports.
- Tests: `tests/test_library_growth.py` gains the gate guard. Existing tests that pin the pending-rename save refusal, the dependents-first order or the held datasource are aligned.
- Docs: `docs/concepts/datasets.md`, `docs/upgrading/2.0.md`, `docs/development.md`, the datasource granularity reference.
