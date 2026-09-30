## Context

Run-by-name (`SlayerQueryEngine._normalize_by_name` → `_stages_of_model`) splits a query-backed
model's `source_queries` into private stages plus a final stage, layers variables, and runs the list.
Only REST has a run-by-name body (`{"name": ...}`); other surfaces pass a bare string. The query
cache stores an entry's `original_input` and `variables` and refreshes by replaying them
(`_reexecute_entry`). Stages omitting `source_model` have their population inferred on every run
(`_infer_populations`, after stage identities are minted). The full base spec, worked examples and
verified engine facts are the Linear issue DEV-2001 body; this change follows it except where
noted below.

## Goals / Non-Goals

**Goals:** a refined run is the hand-written merged final stage (issue §2), reached through one pure
merge function and one normalization path; a saved query's population is fixed at save.

**Non-Goals:** refining a non-final stage; a refined saved query as a stage inside a query list;
saving a refinement as a new model in one call; model-scoped named views; the comparison-suite
probes (recorded on DEV-2019).

## Decisions

1. **Spelling: `refine` next to the name** (issue §3), not a self-contained `{"refines": ...}` query
   object: the name keeps its current place on every surface and `refine` is one key everywhere;
   REST's 1.0 body is only extended. The pairing constraint (refine only with a name) is a runtime
   `ValueError`.

2. **One internal input value.** At the top of `execute` / `evict`, a `str` plus `refine` becomes an
   engine-internal pydantic `SavedQueryRun(name, refinement: QueryRefinement | None)`; a bare `str`
   becomes `SavedQueryRun(name=...)`. `_normalize_input` dispatches on it and the cache stores it as
   `original_input`, so store, refresh and evict carry the refinement by construction (the issue's
   §6.2 "cache needs no change" was wrong: refresh replays `original_input` only).

3. **Pure core merge.** `QueryRefinement` and `refine_query(*, saved, refinement) -> SlayerQuery` live
   in `slayer/core/query.py` with no engine/storage imports (system P2); `RefinementConflictError`
   in `slayer/core/errors.py` uses the typed-error format (core P4). The `Annotated` field types of
   `measures`, `dimensions`, `order` are hoisted to module-level aliases shared by `SlayerQuery` and
   `QueryRefinement`. The merged result is rebuilt with `SlayerQuery.model_validate` so every
   validator runs on the whole; the round-trip MUST be lossless (an empty refinement yields the
   saved stage and the same SQL).

4. **Identity and equality.** Dimension identity canonicalizes only the column reference (extract
   the reference-level part of `strip_source_model_prefix`; never run it on a whole query, which also
   rewrites measures, filters, order). Entries sharing an identity collapse only when equal in every
   field; otherwise `RefinementConflictError` (consistent with the measure-naming rule for identical
   formulas with conflicting metadata). `date_range` compares post-validation values.

5. **Where the merge runs.** In `_normalize_by_name`, after `_stages_of_model` picks the final stage
   and before variable layering (engine P3: storage consulted once, bundle built after). The merged
   stage stays in the saved model's own list, so its private stage names stay local (engine P11).
   Other `_stages_of_model` callers (splicing, column-type probing) never refine.

6. **Population pinning at save** (engine P8 amended, user-approved). The save-time dry run already
   infers each rootless stage's population; the stored stages get those names as `source_model`.
   Because inference runs on minted identities while a stored string resolves to a same-named
   stage first (P11), a stage whose inferred model is named like a sibling stage is refused at save
   (rename remedy). Stored models saved before this change are not migrated (migrations are
   dict→dict, no engine): they pin on next save; a refined run-by-name pins them in memory from
   the unrefined stage first (same collision refusal); an unrefined run is unchanged.

7. **REST.** `QueryRequest.refine: QueryRefinement | None`; malformed refinement → FastAPI 422;
   route checks (refine without name; any flat query field supplied next to name, detected via
   `model_fields_set` so explicit `null` counts; conflict) → 400.

8. **Discoverability.** One helper builds a per-datasource reverse index
   model name → [(saved query name, description)] from the visible (non-hidden) models, matching
   any stage's `source_model_name`; with pinning every stored stage has one. Consumed by the full
   renderer as a new default section `saved_queries` (registered in the inspect section registry),
   by the compact skeleton (markdown + JSON), and by views that render skeletons. Unpinned legacy
   stages are not listed until re-saved.

## Risks / Trade-offs

- [Pinning changes stored query text] → approved P8 exception; the stored stage stays a valid
  hand-written query and runs to the same rows.
- [`population_inferred` for a pinned saved query now reports false] → it is now explicit; tests
  pinning `true` for rootless saved queries need a consented update.
- [Name-collision refusal rejects a previously savable shape] → rare and confusing shape; the error
  names the stage and the rename remedy.
- [R6: a refinement adding a second granularity renames the saved time key] → hand-written parity;
  documented.
