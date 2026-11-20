## Context

See proposal.md › Why. Current state the approach builds on:

- `_kernel_decision` (`slayer/engine/compile/stages.py`) picks one kernel per producer, in the order association → trailing-window → ranked. `AssociationProducerKernel` is the two-level dedup + aggregate (`generator.py::_render_association_producer_body`). `RankedProducerKernel` ranks every joined row (`_render_ranked_cte_from_planned`). `TrailingWindowProducerKernel` range-joins `_base` (bucket endpoints) to `_src` (source rows) and reduces per bucket (`_render_window_metric_cte_from_planned`).
- `_synthesize_cross_dataset_producer` already serves both the cross-model aggregate and the plain local aggregate that needs association; its association arm calls `check_association_windowed_ranked`. Only its non-association arm builds `window_td_key`, after `check_windowed_time_axis_attributable`, and its endpoints come from `_window_endpoints`.
- Local windowed and ranked roots go through `_synthesize_local_regroup`, whose grain may fan. `_assert_local_regroup_safe` refuses a fanning window axis, but nothing applies the mode axis to fanning partition keys. That is how the local windowed double-count happens and why local `first`/`last` ignores the mode.
- The fallback: `ModePolicy.fallback`, `_association_unsupported`, `RegroupAttachPlan.association_unsupported`, `BroadcastGrainWarning(Payload).association_unsupported` with `association_fallback_hint`, and the `(location, metric, association_unsupported)` merge key in `query_engine.py`.
- The keyless list-child kernel (`list_rows`) has no entity keys, so `_AssociationEntity.determines_param` judges parameters against an empty grain.

## Goals / Non-Goals

**Goals:**
- One mode gate for every aggregate with an unattributable grain member, whether plain, ranked, windowed or windowed-ranked, and whether local or cross-model.
- Association as entity dedup composed with any reducer: one rule, rendered by one dedup level per kernel.
- No association-ineligible aggregation, and no fallback.

**Non-Goals:**
- Deprecating `"broadcast"` (DEV-2385).
- An explicit fanning **time** `partition_by=` key on a grained inner (DEV-1941).
- Association along the time-spine axis (DEV-2218 #4).
- A windowed re-aggregation outer: its own refusal stays.
- A re-aggregation ranked outer: it parses as the `last` transform, so nothing is needed.

## Decisions

The planning decisions log, tags preserved.

1. **Local first/last and windowed aggregates follow the mode axis.** Options were (A) the mode axis like every aggregate, or (B) keep the mode-invariant fanned grain. Chose A for Axiom 8, local/cross-model parity, and removing the route that bypasses the mode axis. Under explicit `broadcast`/`error`, a local first/last over a fanning dimension now broadcasts or refuses. (user)
2. **The list-child false refusal is in scope, and the fallback is removed outright.** Options were (A) fix it and drop the fallback, or (B) keep the parameter reason as the last ineligibility, with an Axiom 8 carve-out and a new issue. Chose A: the fallback's only remaining trigger was the false refusal, and neither the fallback nor its field ever shipped (introduced after `v1.9.0`; the package is `2.0.0.dev0`), so no deprecation is needed. (user)
3. **Axiom 8 wording** (user, approved wording): the sentence "Every aggregation is **association-eligible** [target: DEV-1914]; an ineligible one broadcasts under the default, warning with the reason, and is refused under an explicit associate [enforced: test:tests/test_dev2216_associate_default.py];" becomes "Every aggregation associates — a windowed one per bucket, over the home rows with an associated row in the bucket's frame [enforced: test:tests/test_dev1914_ranked_windowed_association.py];" (the rest of Axiom 8 is unchanged).
4. **Law 5 wording** (user, approved wording): "5. **Dice–slice**: filtering to `d = v` equals slicing the `v` cell of a groupby on `d` — exact for every aggregation under the default and associate; broadcast consciously trades this law away and must warn." (tags unchanged)
5. **Association is orthogonal to the reducer.** The kernel decision becomes *reducer* (plain | ranked | trailing-window, optionally ranked) × *associate*, decided by one mode gate in the home-rooted synthesis. Local windowed and ranked roots with an unattributable grain member are routed there, like plain ones. The local host regroup keeps only attributable grains. Structural fix: no reducer can skip association, and no path around the mode axis remains. (default)
6. **Every associated reducer runs over deduplicated entity rows**, ranked included. Level 1 groups (grain, entity) and picks the value, the ranking key and the parameters once per entity. Ranked: NULL ranking keys are dropped, then the rows are ranked and rank 1 is picked. Alternative rejected: no dedup for picks (they are duplicate-invariant), because it ties association to a per-reducer property every future ranked-family reducer would have to re-prove. (codex, user; reverses the planning default)
7. **Windowed association dedups inside the trailing window.** `_src` carries the entity key columns (plus the list-position / row-identity keys, through the same `identified_datasets` relation door). The levels are:
   1. the range join of `_base` to `_src` on the frame;
   2. group by (bucket, cell, entity), picking the value, ranking key and parameters once;
   3. the reducer per (bucket, cell), or the ranked pick for windowed `first`/`last`.

   An empty frame's LEFT-JOIN NULL row survives as one entity-less row, so it reduces to the aggregation's empty value. (default; extended by codex 2)
8. **The associated-window IR contract.** The trailing-window kernel of an associated window carries:
   - `endpoints`: the population's cells at the producer grain, in host coordinates, built by `_window_endpoints` exactly as the host-rooted window builds them today;
   - `endpoint_keys`;
   - the frame-axis key in home coordinates, read in `_src`: the home row's time when attributable, the rerooted related-row time when it fans;
   - the entity keys.

   This holds for local, cross-model, filtered and explicit-`partition_by=` shapes. Endpoints are always set for an associated window, never only for row-restricted producers. (codex)
9. **Precedence for a fanning window axis.** The active window axis is resolved and judged before the general grain-mode decision. Default/`associate` take the association arm. `broadcast`/`error` raise the existing `TimeAxisError`, whose suggestion now names omitting `to_many_handling` among the remedies. This holds for local and cross-model aggregates, and in error mode it takes precedence over the generic broadcast refusal. (codex; default for keeping `TimeAxisError`)
10. **An empty frame takes the empty value**, the NULL bucket included: 0 for the count family, the empty list for `collect`, NULL otherwise. This is Axiom 4 as the window renderer already implements it. The planning shorthand "NULL bucket → NULL" held only for `sum`. Buckets are the population's cells. (codex)
11. **The same-row axis binding** is proven by an adversarial fixture order: an out-of-frame `ok` order beside an in-frame non-`ok` order. The axis predicate and the branch filter read one `_src` relation, never an `EXISTS` beside it. (codex)
12. **Associated ranked and windowed aggregates warn like plain association.** For a fanning axis, the warning names the time dimension; an explicit `partition_by=` grain never warns. (default)
13. **The ranking key's input-safety rule is unchanged.** An implicit key resolving to the fanning time dimension (`last(spend, window='1y')` by `orders.ordered_at`) still fails closed in every mode, and its remedy is an explicit home key. (default)
14. **The parameter inventory replaces `_association_unsupported`.** Every parameter shape either associates or raises its established typed error before SQL generation: field-valued custom parameters, definition defaults, attached aggregate/transform parameters, safe to-one paths, fanning parameters (input safety in every mode), list-child rows, keyless identities. The list-child entity grain is the root row. (codex)
15. **`broadcast` deprecation is split out** into DEV-2385, which has its own worktree. (user)
16. **Re-aggregation outers need nothing.** A ranked outer over an aggregate parses as the `last` transform; the windowed outer keeps `ReaggregationError`. (default)

## Risks / Trade-offs

- [Explicit-mode behaviour change for local first/last and windows] → Approved (decision 1). The spec states the new broadcast/refusal outcomes, and the release notes for the 2.0 line mention it.
- [Golden SQL churn: the ranked association adds a dedup level, and local fanning-grain first/last moves to the association producer] → Only shapes whose kernel changes diverge. Every other golden stays byte-identical; each divergence is reviewed in `pr-implement`.
- [Larger intermediate for windowed association: the range join of endpoints × rows, then a group by] → Accepted for correctness. It is the same order as the plain window's range join, plus one GROUP BY.
- [Removing a 2.0-dev response field (`association_unsupported`)] → It was never released. `docs/upgrading/2.0.md` and `docs/concepts/queries.md` drop it in this change.

## Migration Plan

None: there are no stored-shape changes. Queries previously refused or broadcast by the fallback now execute associated values.
