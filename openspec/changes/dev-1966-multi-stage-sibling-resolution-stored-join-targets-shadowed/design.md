## Context

See proposal.md — Why. Today a stage's user name is simultaneously its synthetic model's name in
the flat `referenced_models` list (`stage_bundle_with_siblings` drops any stored model it shadows),
its CTE name (`sql_table=<name>`), and the prefix of its result keys (`response_meta.projection_result_keys`
reads `source_relation`). The planner (`plan._stage_scope_and_bundle`) and the generator
(`generator._bundle_for_stage`) each derive a stage's model universe independently. Query-backed
models are expanded to SQL text before planning (`_expand_query_backed_model`) in a closed bundle,
and the generator hoists every `WITH` it finds (`_split_ast_ctes`), so private stage names of an
embedded pipeline land in the consumer's `WITH`. A stale DEV-1878 attempt
(branch `egor/dev-1878-render-query-backed-models-source_queries-inline-in-single`, commits
`c570bbfd` plan / `91777d40` impl) is reference material only; DEV-1967 since landed its
`StageSchema` grain and single stage→model builder.

## Goals / Non-Goals

**Goals:**
- One scoping rule (engine arc42 principle 11) implemented by one rewriter, applied at both levels:
  the user's statement and every spliced stored query-backed model.
- One decision site for each stage's model universe, read by both planner and renderer.

**Non-Goals:**
- Joins declared on a query-backed model (dropped today), per-context copies for conflicting
  variables, law-harness query-backed families — DEV-1918.
- Any change to the persisted query-backed cache contract (`columns`, `backing_query_sql`).

## Decisions

1. **Minted stage identities (M1).** One pure rewriter `localize_stages` in
   `slayer/engine/stage_ordering.py` gives every named non-root stage a reserved identity
   (`__slayer_stage_<name>` for user stages, `__slayer_qb__<model>__<stage>` for a spliced model's
   private stages; the spliced final stage keeps the model's own name) and rewrites only
   references the query writes: `source_model` strings, `ModelExtension.source_name`, and
   `joins[].target_model` of inline models and extensions. A rewritten join with no `name` gets
   `name=<old spelling>`, so dotted paths, canonical paths (edge name else target) and result keys
   keep the user spelling. Stored definitions are never rewritten — that is what makes a stored
   join to `customers` keep meaning the model. Alternative — rename only on collision: rejected,
   two code paths and the stage-vs-physical-table clash stays open.
2. **Where localization runs.** First step of `_prepare_pipeline` (before `_infer_populations`)
   and of the run-by-name path, after the list is validated and topo-sorted on user names (so
   ordering errors name user stages). Population inference then fills rootless stages with storage
   names, which never carry the reserved prefix and so can never be captured by a stage; its
   `SIBLING_STAGE` anchor check compares against the stages' user spellings.
3. **Display names are typed data (M2; Codex #7).** `StageSchema` gains `display_name` (user
   spelling, or `stage 'x' of model 'm'` provenance for spliced private stages) next to
   `relation_name` (identity). The engine sets it; result keys, warnings (incl. stale-spelling
   stage labels) and errors read it. Nothing reverse-parses prefixes and `sql` imports nothing
   from `engine`.
4. **Collision invariant (M3; Codex #1).** `stage_bundle_with_siblings` keeps de-duplicating the
   identical host entry production bundles carry in `referenced_models`, and raises an internal
   invariant error only when a stage identity meets a different model.
5. **One per-stage universe (M3b; Codex #5 partly accepted).** The planner stamps the per-stage
   `ResolvedSourceBundle` (an `ir` type the renderer already consumes) on `PlannedQuery`; the
   generator reads it and `_bundle_for_stage` is deleted. It holds only siblings planned earlier.
   A narrow new IR type was rejected: it would rewrite every generator bundle consumer for no
   layering gain. Response metadata for a stage-sourced root reads that stage's stamped bundle;
   stage columns carry their source column's label.
6. **Splicing (M7).** `_prepare_pipeline` builds the bundle, discovers stored query-backed models
   depth-first in every position (source, stage source, join / cross-model target, including
   inside spliced stages), localizes each model's stages under its scope, splices them, re-runs
   `topologically_order_stages` over the augmented list (it sees join-target deps), then rebuilds
   the bundle (two passes; the second reuses resolved models). A model reached from several places
   is spliced once. The spliced final stage carries its source model's default time dimension;
   user stages carry none. Alternative — keep text expansion + hoist renaming for query-backed
   models: rejected, it keeps nested scopes as text and the deferral arm alive.
7. **Lexical variables (M8).** Splicing records each spliced stage's enclosing-model
   `query_variables` stack; the sibling variable merge consults it (precedence in the
   `queries/query-backed-inline` spec). A model spliced once but reached from contexts whose
   effective variables disagree on a placeholder it uses fails closed (per-context copies: DEV-1918).
8. **Extensions (M9).** A root `ModelExtension` over a spliced root applies once through the
   extension-over-sibling planner path (`inline_extensions` is no longer re-applied after
   expansion on the execute path); measure-bearing stage extensions over query-backed models keep
   today's rejection.
9. **Warnings from spliced stages are surfaced (user decision).** Spliced stages' warnings are
   collected like any stage's and labelled via `display_name` with stage and model; dangerous
   conditions inside a stored model (to-many broadcast) must be visible to every consumer.
10. **One cycle guard (M10; Codex #6).** One exception type naming the ordered path, raised by one
    in-flight chain threaded through splicing, run-by-name (seeded with the run model), `save_model`
    and `get_column_types`. The cached-SQL short-circuit in `expand_query_backed_models_in_bundle`
    is deleted. Text expansion survives only for save-time cache population and
    `get_column_types`.
11. **Scope-aware hoist renaming (M5; Codex #2).** Every CTE hoisted out of an embedded statement
    is renamed through the shared allocator, the rename keyed by its defining `WITH` node and
    applied only to references bound to that definition (nested shadowing, alias reuse, recursive
    and quoted names preserved). Allocator-minted CTEs keep their names. Generalizes
    `_uniquify_producer_base_ctes`.
12. **Inline query-backed ban (S2).** A `SlayerQuery.source_model` validator rejects an inline
    `SlayerModel` with `source_queries`; because stored stages are `SlayerQuery`s the same check
    fires for `create_model_from_query` and on load. `stage_ordering._walk_spec` drops its nested
    `source_queries` branch.
13. **Generator arm (M11).** The `NotImplementedError` in `_build_from_clause_from_planned` becomes
    a non-deferral `ValueError` invariant; the `DEFERRAL_SITES` entry is removed and
    `guards.baseline` goes 1→0 atomically.

## Risks / Trade-offs

- [Every multi-stage golden changes CTE names] → mechanical re-bless; values and result keys are
  asserted unchanged.
- [Prefixed identities exceed dialect identifier limits] → identities pass through the existing
  over-limit fitting; a near-limit Postgres test covers CTE names.
- [Surfaced spliced-stage warnings are new output for existing consumers] → intended (decision 9);
  labelled so the model author is identifiable.
- [Two-pass bundle build doubles storage consults] → the second pass reuses the first pass's models.
- [Persisted models containing an inline query-backed source stop loading] → accepted (no
  migration); the error names the model and the remedy.

## Migration Plan

Single PR; no storage migration. Rollback = revert.
