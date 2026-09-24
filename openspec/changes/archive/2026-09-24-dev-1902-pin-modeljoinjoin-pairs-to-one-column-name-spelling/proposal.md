# Proposal: pin `ModelJoin.join_pairs` to one column-name spelling; physicalise once at emission

## Why

A column has two spellings — logical `Column.name` and physical `Column.sql` — and `ModelJoin.join_pairs` has no enforced spelling contract, so every consumer re-derives which it holds: the SQL generator and cardinality profiling emit the pairs raw as physical identifiers, join-safety treats a target as maybe-logical with a physical fallback and normalises grain leaves by hand, and the association kernel builds its entity keys from physical unique-key sets. The same bug class recurs at every new site (DEV-1892 shipped its FK seed wrong for a renamed source key and was patched locally), and a renamed primary key on an association root breaks today.

## What Changes

- `join_pairs` entries are `Column.name` spellings, obey the column-name rules, and MUST name a declared *base* column (no expression `sql`, no `filter`) on each side. The source side is enforced when the model is constructed (query-level extensions included), the target side at save time and by the join-safety audit; SQL emission fails closed on a key it cannot resolve. **BREAKING** for a stored model whose key column is undeclared or an expression column (it no longer loads / saves until repaired); a stored physical spelling of a renamed base column is migrated on load.
- One physical-spelling seam in `core` (`Column.physical_name`, `physical_join_pairs`), applied only where a reference becomes SQL (base joins, semi-join spines, cardinality profiling) or meets the live schema (schema drift, type refinement). Join-safety, the grain seeds and the association kernel compare logical names by plain equality; the ad-hoc `by_name` maps and `_physical_grain_leaves` are removed, and a double-quoted bare identifier counts as a base column everywhere.
- Persistence: `SlayerModel` v11. A load-time pass canonicalises stored physical spellings of bare-rename columns on both sides (peer-aware), runs before the exact-inverse join dedup and feeds the counterpart comparison, then writes back.
- Importers emit logical names: Cube keeps member names, synthesises a hidden base column for an ON operand naming no member, and drops a join whose ON operand is a non-base member (report warning); dbt synthesises a hidden base column for a foreign entity no dimension covers and skips a non-bare `expr` (warning); OSI checks both sides against the contract before appending a relationship; ingestion applies its one column rename to `join_pairs` too.
- Dead code removed: `ir.planned.JoinRequirement` (with its tests) and the dotted-`join_pairs` branch in ingestion.
- Docs: one sentence each; `system.arc42.md` §3 gains P16 "Logical names, physical once".

## Capabilities

### New Capabilities

- `models/join-keys`: the spelling contract of join keys — logical `Column.name`, base columns only, one physical spelling applied at emission, load-time canonicalisation of stored physical spellings, importer compliance.

### Modified Capabilities

None. `models/join-cardinality` › "Provable to-one arity per orientation" is spelling-neutral as written; the renamed-key scenarios attach to the new capability.

## Impact

- `slayer/core/models.py` (`is_base_column_sql`, `physical_column_sql`, `Column.is_base` / `physical_name`, `_check_join_keys`, `ModelJoin` validator, `SlayerModel` validator), `slayer/core/join_walker.py` (`physical_join_pairs`), `slayer/core/errors.py` (`JoinKeyError`).
- `slayer/engine/join_safety.py` (logical `_unique_key_sets`, seeds compare directly, audit finding), `slayer/engine/compile/stages.py` (physical `SemiJoinHop` pairs), `slayer/ir/planned.py` (`SemiJoinHop` doc, `JoinRequirement` removed), `slayer/ir/source_bundle.py` (overlay runs the join-key check), `slayer/sql/generator.py` (ON clause through the seam), `slayer/engine/query_engine.py` (profiling through `Column.physical_name`, `_validate_join_keys` at save), `slayer/engine/schema_drift.py` + `slayer/storage/type_refinement.py` (delegate to the `core` predicate / unquoting), `slayer/storage/migrations.py` + `slayer/storage/base.py` (v11, canonicalisation pass, counterpart comparison).
- `slayer/cube/converter.py`, `slayer/cube/refs.py` doc, `slayer/dbt/entities.py` + `slayer/dbt/converter.py`, `slayer/osi/converter.py`, `slayer/engine/ingestion.py`.
- Tests: new `tests/test_dev1902_join_key_spelling.py`, `tests/test_dev1902_join_key_contract.py`, `tests/test_dev1902_join_key_migration.py`, `tests/test_dev1902_golden_sql.py` (+ baseline); importer tests added in the existing Cube/dbt/OSI/ingestion modules; `tests/test_planned.py::TestJoinRequirement` deleted and `tests/test_cube_converter.py::test_join_member_resolves_to_physical_column` re-pinned (both with consent). Existing goldens expected byte-identical.
- Docs: `docs/concepts/models.md` (Joins sentence, `version` currently 11), `docs/cube/cube_import.md`, `slayer/mcp/server.py` join-dict doc. `architecture/system.arc42.md` §3 item 16 (approved wording in design.md).
