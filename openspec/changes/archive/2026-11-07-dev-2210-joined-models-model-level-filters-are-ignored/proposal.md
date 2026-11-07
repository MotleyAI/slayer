## Why

`SlayerModel.filters` are documented as always applied, yet they restrict a model's rows only when it roots a query or a producer; reached as a join target, a semi-join hop, a list child's parent or inside another definition, the model reads its bare table and the query silently returns wrong rows. A pre-filtered model is also SLayer's spelling of OSI's `EXISTS_IN(x, gold_accounts.x)`, which therefore gives wrong answers today.

## What Changes

- A model's filters define its dataset: every read of the model — root, join hop, semi-join hop, producer, list parent, another model's filter — sees only the rows passing them, restricted before any join, so a null-extended row means "no passing row".
- The one relation door emits that filtered dataset; the root-only WHERE path for model filters is removed. Root SQL for a filtered model becomes a derived table (no result change).
- A filter's joined reads restrict by association (correlated `EXISTS`) and are inlined as joins only when every hop they cross is proven to-one, so a filtered dataset never multiplies rows — also fixing the latent root multiplication across an unproven hop.
- Filters whose dataset reaches itself are refused with a new `ModelFilterCycleError`, at save time and at query time.
- Model-filter validation (no measure, no windowed column — now resolved against each reference's owning model, through derived columns) runs at save time and at query time for every model the query reads.
- **BREAKING**: a stored model whose filters form a cross-model cycle, or reference a measure or windowed column, is refused when saved and fails every query that reads it.

## Capabilities

### New Capabilities
- `models/model-filters`: what a model's `filters` mean — the filtered dataset every read sees, association lowering of joined reads, cycle refusal, and save/query-time validation.

### Modified Capabilities

## Impact

- `slayer/sql/generator.py`: `_emit_relation` becomes the filtered-dataset door over a private backing-relation emitter; the `mode_a_filters` WHERE lowering is removed.
- `slayer/ir/planned.py`: `ModeAFilter` / `PlannedQuery.mode_a_filters` removed; the root's filtered-ness becomes the existing host-gating fact.
- `slayer/engine/compile/stages.py`, `slayer/engine/elaborate_env.py`: query-time validation over the query's read set; empty-base and window `_src` planning consume the gating fact.
- `slayer/engine/column_dependency.py`, `slayer/storage/base.py`, `slayer/engine/query_engine.py`: save-time filter validation and cycle check at both save doors.
- `slayer/core/errors.py`: `ModelFilterCycleError`.
- Architecture: semantics Axiom 3.3, `python.sql` P16, `system` P18; law test `tests/test_law_relation_door.py`.
- Docs: `docs/concepts/models.md`, `docs/concepts/terminology.md`. Golden SQL for model-filtered queries re-blessed.
