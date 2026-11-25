## Context

See proposal.md › Why. The current code:
- `granularity_renames` (`slayer/storage/granularity_rename.py`) maps the reserved-named granularities of one stored datasource dict.
- `_granularity_renames` (`slayer/storage/base.py`) reads that map once per top-level load and applies it to every dependent, whatever its version.
- `upgrade_store` (`slayer/storage/upgrade.py`) writes dependents first and holds a renaming datasource while a dependent is not stored (`_held_by`).
- `save_datasource` refuses while a rename is pending (`_check_no_pending_rename`, DEV-2220 decision 27).

Element principles that apply:
- system P5 (async-first) and P11 (fail at save time);
- storage P3 (versioned persistence), 3.1 (a load never writes), 3.2 (library growth never invalidates a stored artefact);
- live_schema P2 (one treatment per model source);
- core P8 (input refuses what it does not read: `renamed_from` becomes a read key).

No arc42, `model.c4` or `index.yaml` edit is needed. Every delta spec belongs to an existing spec group: `models`, `cli` and `queries` are cross-cutting groups in `index.yaml`.

## Goals / Non-Goals

**Goals:**
- Make the rename decide by each document's own version, as every other migration does, so any subset of upgrade writes is safe.
- Fix sql-mode drift and typing, `upgrade_datasets` refusals, undecodable memories, the upgrade-needed flag, and the spec wording.

**Non-Goals (user, 2026-11-25; listed untracked on DEV-2451 as YAML store races):**
- The yield under `memories.lock` and the in-process re-entry of its depth counter.
- Atomic compare-then-write on YAML datasets and datasources.
- A stale cached YAML dataset after a datasource file is edited by hand.
- A read-only store open for `--dry-run`. Its wording is corrected instead.

## Decisions

### The rename
- **Record.** `CustomGranularity.renamed_from: str | None`. `renamed_datasource` sets it on every entry it renames. Datasource version 4 has a registered 3 → 4 migration that only stamps the version.
- **Map.** Pending renames (`granularity_renames` over the stored names, as today) plus recorded ones (`renamed_from.casefold()` → name). A record overrides a pending entry for the same former name, and the first record wins when two share a former name. The map stays a function of the stored datasource dict alone, so it remains a pure function of stored documents.
- **Gates.** A table, from each name in `reserved_granularity_names()` to its `{SlayerModel, SlayerQuery, Memory}` gate. Every current name maps to 30 / 19 / 14. A dependent is renamed for former name `f` only when its pre-migration version is below `gate(f)` for its entity.
  - The dataset rewrite applies the gate to the document and separately to each nested stage, by its own version.
  - The memory rewrite applies it to the memory, and to its query when that carries a version.
  - Per-name gates are needed because a later reservation must not rename documents written after an earlier one.
- **Library-growth guard** (`tests/test_library_growth.py`):
  - every name in `reserved_granularity_names()` has a gate;
  - every gate beyond the current set is strictly higher than all earlier gates for all three entities (codex: an equal gate cannot tell documents written before a reservation from documents written after it);
  - simulated failures prove each check trips.
- **Upgrade command.** `_held_by`, the dependents-first split and the "waits for" detail are removed. Every plan is written as it comes. Unreadable outcomes stay.
- **Warning.** Logged only for pending renames, keyed as today.
- **Save.** `save_datasource` reads the stored raw datasource, computes its map, and for each `former → new` adds `renamed_from=former` to the incoming granularity named `new` (casefold) when that granularity lacks it and no incoming granularity records `former`; when it records another former name and none records `former`, the save is refused naming it. Then:
  - `check_granularities` adds the `renamed_from` rules: the value is a key of `reserved_granularity_names()`, and unique case-insensitively;
  - the save is written;
  - `_check_no_pending_rename` is deleted.

### Drift and typing
`_diff_sql_dataset_fields` and `sql_dataset_retype_findings` look a field up by `col.name`, then by `col.physical_name` when `col.is_base`. Today they fall back to `definition_sql.strip()`. This is the same notion table mode uses (`_diff_sql_table_columns`, `retype_findings`).

### One refusal path
One async helper upgrades a dataset document and translates every failure into a reason string:
- `LiveRefinementRequiredError` → the live-refinement guidance;
- a remaining field `sql` → the dialect guidance;
- anything else → `str(exc)`.

`_planned` (store) and `_upgrade_datasets` (snapshot) both call it. The dialect guidance differs only in its remedy wording (`--datasource-type` vs `datasource_type=`), passed in by the caller. Parity tests pin the same reason category.

### Undecodable memories
- `_read_memory_file` reads bytes and decodes inside `_memory_from_stored`'s `stored_document_boundary`, so a decode failure is a `StoredDocumentLoadError` naming the memory id. `_list_memories_rows` already collects those.
- `_raw_memory_dicts` skips an undecodable file, which `_unreadable_outcomes` then reports from the load failures on a whole-store run.

### Validator rewrites
A context-scoped collector in `slayer/core` (`stored_input_rewritten()` marks; `collecting_rewrites()` collects).
- `DatasourceConfig._apply_schema_migrations_and_aliases` marks when it moves `user`.
- `_upgraded_dataset`, `_upgraded_memory` and `_upgraded_datasource` validate inside the collector and OR it into `changes`.
- Default fills (`CustomGranularity._default_origin`) do not mark.
- One test walks every `mode="before"` model validator reachable from `SlayerDataset`, `SlayerQuery`, `Memory` and `DatasourceConfig` (nested models included). It requires each to be in a `REWRITING` set (which must mark) or a `NOT_REWRITING` set with a one-line reason. A `REWRITING` validator is exercised and must mark.

### Decisions log
1. Scope: proportionate, erring small. The three YAML races are listed untracked on DEV-2451; the durable rename and the flag stay here, with no new issue. Options weighed: full scope; descope the rename to DEV-2452 or to a new issue. (user)
2. A datasource save carries the stored rename records over and is never refused for a pending rename, which lifts DEV-2220 decision 27. Options weighed: carry over; refuse a save that drops a record. Accepted cost: a save that deletes the renamed granularity also deletes its record. (user)
3. `renamed_from` sits on the granularity entry, so it moves with the granularity in DEV-2452's migration. (DEV-2246 decision 36, user)
4. The version gate is per reserved name, and the library-growth guard requires a gate for every reserved name. (default, structural: storage 3.2)
5. A later gate is strictly higher than every earlier gate. An inclusive gate was rejected because it would rename documents written after a reservation. (codex, refined)
6. `renamed_from` must name a reserved name and be unique, checked at save. (default, system P11)
7. sql-mode drift and live typing match base fields by `physical_name`. (default, structural, live_schema P2)
8. One per-document upgrade-and-refusal path, keeping the live-refinement and dialect reasons, with parity tests. (default, structural; codex)
9. `--dry-run` is fixed by rewording, not by a read-only open. (default: the issue frames it as wording)
10. An undecodable memory is decoded inside the document boundary and kept as a failure by id. Tests cover `get_memory`, listing, whole-store and `--data-source` upgrades. (codex)
11. Validator-level rewrites are counted through one context marker, at any nesting depth, with an enumerating test. A dump comparison was rejected (DEV-2246 decision 38). (user; nesting: codex)
12. Rejected (codex): refuse a datasource save that omits a renamed granularity until every dependent is rewritten. It contradicts decision 2, and deleting a granularity breaks its references whichever meaning the old text gets.
13. Rejected (codex): count `CustomGranularity._default_origin` as a rewrite. Filling a default is not a legacy rewrite, and counting fills would flag every hand-written document.
14. Rejected (codex): edit storage arc42 3.1 to exempt the open-time layout moves. 3.1 governs loads; the move is a spec-licensed write on open, kept by DEV-2246 decision 16 when 3.1's wording was approved.
15. A save that would drop a stored record by giving its granularity another former name is refused; a record moved to another granularity is kept, not duplicated. Options weighed: refuse; let the client's record replace the stored one. (user; codex, CodeRabbit)

## Risks / Trade-offs

- [A 2.0.x reader meets a v4 datasource] → It refuses the datasource as newer than it supports. The upgrade guide says to upgrade every reader before running `slayer storage upgrade`.
- [Every 2.0 store warns once per datasource until upgraded (3 → 4)] → The normal effect of a version step. The warning names `slayer storage upgrade`.
- [A hand-edited document whose version is lowered below its gate gets renamed] → The same exposure every version-gated migration has.

## Migration Plan

Run `slayer storage upgrade` after moving to this release. It stores each datasource at version 4 with its records, in any order alongside its dependents. Rollback: a 2.0.x reader cannot read a v4 datasource, so keep a copy of the store before upgrading.
