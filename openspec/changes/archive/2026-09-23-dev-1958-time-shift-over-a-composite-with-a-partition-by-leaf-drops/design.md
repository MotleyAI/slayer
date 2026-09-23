# Design — DEV-1958 shifted evaluation as a producer

## Context

See proposal.md — Why. Load-bearing current state: `_emit_time_shift_ctes_for_planned`
(`slayer/sql/generator.py`) hand-renders the re-aggregation shifted CTE — it re-resolves
joins, fragment kwargs, column filters and casts from the keys, relabels each source
row's bucket forward by the shift, strips frame bounds itself, and reads a bare
placeholder's outer producer at the row's own bucket. It never sees the planner's
structural decisions (nested producers for partitioned / ranked / windowed leaves,
association mode, semi-join pushdown), which is the whole defect class. The series
regime already reads a materialised relation and looks the consumer's shifted bucket
up on the base side (`DEV-1868` D4). Producers are planned by `_plan_regroups` +
`compile_synthesized` (`slayer/engine/compile/stages.py`) and rendered through
`_render_producer_split`; interning is `regroup_producer_identity`; frame bounds are
classified by `slayer/core/time_bounds.py`.

## Goals / Non-Goals

**Goals:** one evaluator — the shifted relation is a plan; one join-back for both
regimes; the defect class (a leaf's own structure ignored by the shifted evaluation)
impossible by construction; net-negative generator code.

**Non-Goals:** the series regime's semantics (untouched); moving cross-model composite
leaves out of the series regime (the classifier stays as is); any change to how
frame bounds are classified.

## Decisions

**D1 — The re-aggregation shifted relation is a planner-synthesized producer.** For each
non-series `time_shift` slot, `compile/shift.py` synthesizes a producer prebound with
`_regroup_producer_prebound`: grain = the query's projected dimensions and time dimensions
(unshifted, consumer order and names, `main_time_key` = the axis); one measure = the
transform input; inherited filters = the stratum-0 field masks minus the date-range masks,
each passed through `strip_frame_bounds` over the query's time-dimension raw columns;
the parent `population_filters` forwarded so a host-rooted restriction (semi-join,
association) applies inside. Compiled via `compile_synthesized(in_producer=True)` so every
leaf takes its existing route (nested `_cm_`, target-rooted producer, ranked / windowed
kernel, checker errors). Alternatives rejected: a render-level per-leaf shifted sub-select
(a third hand-written evaluator, sql P10 violated in spirit); planner-synthesized
per-leaf producers attached into the hand-rendered CTE (keeps the second evaluator and
answers mixed-grain grouping by hand — exactly what DEV-1868 D4 refused).

**D2 — Per-leaf rule: re-evaluate or carry.** The synthesis walks every aggregate-valued
constituent of the input. A bare local aggregate is re-evaluated (its grain is the query
grain, which contains the axis). A placeholder resolves to its original key through the
regroup substitution map and is re-evaluated when its grain contains the axis or it is
ranked or windowed; it is carried when its grain lacks the axis. Re-evaluated leaves are
restored to their original keys in the producer measure; carried leaves stay placeholders
and their outer `RegroupAttachPlan` objects are threaded into the sub-plan via a new
`compile_synthesized(carried_attaches=…)` keyword, so interning renders them once and the
producer reads the in-frame value (Axiom 10: a cell without the axis has no shifted twin).
A cross-model leaf never reaches here (series regime).

**D3 — One join-back: consumer-side lookup for both regimes.** The producer is keyed by the
unshifted bucket; the sjoin reads it at `trunc(base.bucket + offset)` (the series regime's
existing expression, re-truncated for unaligned shifts) with equality on every other grain
pair. Value-identical for aligned shifts; for an unaligned shift the re-aggregation regime
now reads the bucket containing the offset instant instead of relabelling source bucket
starts forward. Chosen because the relabel form cannot host carried or nested leaves
without grouping by their values (the many-to-one fan-out the lookup was introduced to
avoid) and because a shift-free producer is what makes two offsets intern to one CTE.

**D4 — Plan model: a `"shifted"` attach phase, not a new plan kind.** `RegroupAttachPlan`
gains `attach_phase="shifted"` with `substitutions=[]`, `shift_of: SlotId` and
`answer_slot_id: SlotId`; kernel per the existing rule (plain, or ranked / trailing-window
when the answer is a bare ranked / windowed aggregate). Validators: `shift_of` names a
non-series `time_shift` slot of this plan, `answer_slot_id` belongs to the attach's
producer, substitutions are empty, no duplicate or orphan shifted attach, no shifted
attach for a series slot. Every existing walker (stage-order validation, diagnostics,
interning, rendering dedup) covers the phase; the row / combined renderers filter by phase
and ignore it. `ValueSlot.series` stays the D6 planner fact.

**D5 — Interning canonicalization.** The shifted producer's answer is named by the same
canonical rule as a combined regroup producer (`public_alias_by_agg` from the canonical
aggregate alias for a bare aggregate, the transform's declared name for a composite), so a
bare-aggregate shift without a frame mask interns with the base's own producer and the
sjoin reads that `_cm_` directly; two offsets over one input intern to one producer; a
frame mask or a differing population disposition yields a distinct producer by fingerprint.

**D6 — Own-grain nesting rule.** In `_plan_regroups`'s `in_producer` branch a ranked or
windowed root at exactly the producer's own grain nests when — and only when — it is a
strict constituent of the producer's composite answer, never when it is the answer itself
(the kernel renders that). Fixes the internal error today's `time_shift(amount:last / 2, -1)`
hits; `time_shift(w90 / 2, -1)` then renders its windowed leaf through the nested producer.

**D7 — CTE dependencies (sql P6).** The emitter renders the shifted producer with
`shifted_<alias>` pushed as the active split consumer, exactly as the combined-attach door
does, so hoisted nested CTEs and reuse edges land on that node; the sjoin declares
`[chain_tail, shifted_<alias>]`.

**D8 — One identity-deduplicated nested-plan traversal.** `_walk_regroup_attaches` yields
each attach object once and every nested-plan consumer (`plan_has_semi_join_filters`,
`_iter_plans_with_producers`, the warning collectors) routes through it, so a carried
attach reachable from both the outer plan and the shifted producer reports once.

**D9 — The checker is total.** `check_non_shift_transform_row_leaf` becomes
`check_transform_row_leaf` (shift-family skip removed); `_check_shift_family_key` keeps
only the boolean-input rule. One message — the existing grain-refining message extended
with the `source_queries` remedy — so the DEV-1846 substring pins stay green.

**D10 — Helpers move down, not sideways.** Periods / granularity parsing becomes
`shift_offset_of(key)` in `slayer/core/keys.py` (shared by planner and emitter);
`_series_mode` has one definition in `compile/shift.py` that staging imports;
`_shift_preserves_bucket_starts` stays for the lookup.

## Risks / Trade-offs

- [Every time_shift golden moves] → class-(b) re-bless per the divergence protocol, values
  unchanged except the fixed cases, enumerated in `divergences.md`.
- [Shape tests pin the relabel form] → re-pinned to the lookup form, per file with consent;
  joins / WHERE / partition-column assertions keep passing since the producer body carries
  the same joins and filters.
- [Unaligned-shift corner changes] → unpinned today; the series regime already behaves
  this way; documented in one sentence.
- [Carried attach reachable twice] → D8.
- [Producer over a StageSchema root] → covered by the stage scenario; windowed-over-stage
  producers already prove the path.
- [Performance: frame-free producer scans the whole table] → same as today's shifted CTE;
  unchanged.

## Migration Plan

Pure planner / generator / checker change behind the existing query surface; no storage or
API migration. Rollback = revert the PR. The bare-row-column rejection is a documented
breaking change with a remedy in the error text.
