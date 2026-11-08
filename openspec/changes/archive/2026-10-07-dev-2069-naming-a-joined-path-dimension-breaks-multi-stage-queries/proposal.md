## Why

A computed dimension whose expression is a bare joined path (`{"name": "region_id", "expression": "customers.region_id"}`) loses its declared name: a single query keys it by the path (`orders.customers.region_id`), and a stage carrying it fails its own schema check, naming the source model instead of the stage. Three code paths decide a slot's output name independently and disagree on joined ROW slots.

## What Changes

- A user-named dimension keys by its name (`<model>.<name>`) even when its expression is a bare joined path; downstream stages and query-backed models see the bare name.
- An unnamed computed dimension over a bare joined path behaves exactly as the plain dotted dimension (canonical path key, `__`-flattened downstream name).
- Two dimension entries over the same joined path (one plain, one named) each project their own column.
- The stage schema-mismatch error names the stage by its user-facing name.
- One naming function in `slayer/sql/naming.py` decides every slot alias's result key; the generator, response metadata and stage schema all consume it.
- Docs: a renamed cross-model measure's key is `<model>.<name>` (the stale hop-path claim is corrected).

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/computed-dimensions`: adds the requirement that a named computed dimension keys by its name, including over a bare joined path, in single queries, stages and query-backed models.
- `queries/multi-stage`: adds the requirement that stage errors name the stage.

## Impact

- `slayer/sql/naming.py` (new `slot_result_key` / `slot_result_keys`), `slayer/sql/generator.py` (`_full_alias_for_slot`, regroup `_flat`), `slayer/sql/stage_wrapper.py` (`stage` argument), `slayer/engine/response_meta.py` (`_slot_result_keys` removed, `projection_result_keys` walks the projection), `slayer/engine/compile/stages.py` (`_emit_stage_schema`), `slayer/engine/query_engine.py` (virtual-model wrap).
- Result keys change only for user-named dimensions over a bare joined path and for a second alias of a joined ROW slot; all other keys are byte-identical.
- `docs/concepts/queries.md`.
