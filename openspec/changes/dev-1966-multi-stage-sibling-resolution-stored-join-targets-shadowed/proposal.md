## Why

A multi-stage statement has no name scoping: a user stage name doubles as a model identity in one
flat bundle namespace and as a CTE name in the SQL, so a stage named `customers` silently replaces a
stored model's join target (`JoinKeyError` at render), and a stored query-backed model's private
stage names leak into the consumer's `WITH` (`duplicate CTE name`). Query-backed models are still
pre-rendered to SQL text in a closed scope, which is why an inline model's nested `source_queries`
cannot read a sibling and why the generator keeps a fail-closed deferral arm for them.

## What Changes

- Stage names become **query-local**: visible only inside their own query list, where they override
  a same-named model; stored definitions (stored joins, stored `source_queries`) never see them. A
  stored query-backed model's private stage names are local to that model.
- Internally every stage carries a minted identity; the user's spelling survives only in result
  keys, join edge names, warnings and errors.
- **BREAKING**: an inline `SlayerModel(source_queries=…)` used as any `source_model` is rejected at
  construction, with a hint to use named stages instead; a persisted model containing one fails to
  load with that error (no migration).
- A stored query-backed model consumed as a relation (source, stage source, join / cross-model
  target) is spliced into the statement's DAG as internal stages instead of being pre-rendered to
  SQL text; values and result keys equal the explicit splice, and warnings raised inside its stages
  are surfaced labelled with the model. The whole statement renders as one flat `WITH`.
- CTEs hoisted out of an embedded statement (e.g. a user `sql` model containing `WITH`) are renamed
  through the shared allocator, scope-aware, so no two statements' private names meet.
- **BREAKING**: a self- or transitively-referencing query-backed model raises one cycle error at
  execute, run-by-name, `save_model` and `get_column_types`; the cached-SQL fallback is removed.
- The generator's `NotImplementedError` deferral arm for query-backed models is replaced by a
  non-deferral invariant; its `DEFERRAL_SITES` entry goes and `guards.baseline` drops to 0.
- Absorbs DEV-1878 (inline rendering of query-backed models).

## Capabilities

### New Capabilities
- `queries/query-backed-inline`: consuming a stored query-backed model as a relation — the splice
  equivalence, single flat `WITH`, variable layering across nesting, extensions over a spliced
  root, warnings from spliced stages, and cycle rejection.

### Modified Capabilities
- `queries/multi-stage`: stage names are query-local (override same-named models, invisible to
  stored definitions, never leaked into result keys or messages); inline query-backed sources are
  rejected, replacing the nested-`source_queries` ordering scenario.
- `models/save-validation`: a self- or transitively-referencing query-backed model is rejected at
  save time.

## Impact

- `slayer/engine`: `query_engine` (`_prepare_pipeline` localize + splice + two-pass bundle build,
  run-by-name, cycle guard, `_expand_query_backed_model` confined to save / `get_column_types`),
  `bundle_builder`, `stage_ordering` (`localize_stages`, nested walk removed), `plan` (per-stage
  bundle stamped on the plan), `response_meta` (display names).
- `slayer/ir`: `source_bundle` (collision invariant), `planned` (per-stage bundle), `core.scope`
  `StageSchema.display_name`.
- `slayer/core/query.py`: inline query-backed source rejection.
- `slayer/sql/generator.py`: `_bundle_for_stage` deleted, scope-aware hoist renaming, deferral arm
  replaced.
- `architecture/`: `engine.arc42.md` new principle 11, `sql.arc42.md` principle 11 extended,
  `index.yaml` `guards.baseline` 1→0.
- Tests: multi-stage SQL goldens and subquery-shape assertions re-blessed; inline query-backed
  tests flipped to rejection.
- Docs: `docs/concepts/queries.md`, `docs/concepts/models.md`.
