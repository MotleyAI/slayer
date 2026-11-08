## Context

See proposal.md — Why. Today a **local** partitioned measure isolates via `classify_isolation` (`engine/isolation.py:163`, `partition_keys is not None` → `HOST_ROOTED`) → `_dispatch_filtered_local` / `_plan_filtered_local` (partition-narrow arm builds a nested `PreboundQuery` at the partition subset) → `CrossModelAggregatePlan(rerooted_plan=…)`, rendered at the combined SELECT by `_render_with_cross_model_plans`. The DEV-1825 regroup primitive already renders the same *shape* for computed dimensions via `attach_phase="row"` (host-rooted producer joined into the base FROM, consumed via reserved-leaf `ColumnKey` substitution); `attach_phase="combined"` is declared but unimplemented. Hard constraint: golden SQL byte-identical vs `tests/golden/dev1739_sql_baseline.json`.

## Goals / Non-Goals

**Goals:**
- Migrate the LOCAL partitioned-measure join-back onto `RegroupAttachPlan(attach_phase="combined")`, retiring the local partition arms of the DEV-1503/DEV-1709 path.
- Byte-identical `local/*` golden; `cross_model/*` and `guard/*` unchanged.
- Establish the combined-assembly render as a shared seam future graft migrations reuse.

**Non-Goals (design-level):**
- Cross-model partitioned measures, cross-phase interning, and combined-attach composition with other isolated features — deferred to DEV-1824 (see the DEV-1824 comment).

## Decisions

- **D1 — Render by generalizing `_render_with_cross_model_plans`, not a from-scratch method.** Byte-identity requires the proven combined machinery: producer bodies use dotted, quoted output aliases and consumer-public-alias aggregate names (the rerooted-plan render), composites need slot-classification + local-dependency promotion into `_base`, and hidden/order-only + outer-WHERE routing already live there. A dedicated method reusing the flat-rename `_prepare_regroup_attaches` would diverge on all three (Codex F1/F2/F5). *Alternative rejected:* dedicated primitive-owned method — cleaner on paper but re-solves solved problems and risks byte-divergence. The primitive still owns discovery / IR / producer synthesis / substitution; only the combined-assembly render is shared.
- **D2 — LOCAL-only scope; keep `_narrow_shared_grain_to_partition`.** Cross-model partitioned measures are path-bearing and return `TARGET_ROOTED` before the local `partition_keys` branch (Codex-confirmed), so removing that branch does not reroute them; their migration needs a target-rooted producer + producer CTE-hoist (DEV-1824).
- **D3 — Unified substitution-first desugar.** Generalize discovery (`_plan_dimension_regroups` → `_plan_regroups`) to find partitioned aggregates in measures / composites / order specs (→ combined) as well as computed dimensions (→ row); group by (`partition_keys`, attach-phase); synthesize one host-rooted producer per group via `plan_query(disable_host_rooted_isolation=True)`; substitute each consumed aggregate → placeholder `ColumnKey`. Same-phase interning (measures sharing a partition set → one producer, N outputs) falls out of the grouping.
- **D4 — Producer aggregate naming mirrors the current path (F1).** Public alias when the partitioned aggregate is a directly-named measure; canonical when it is a composite leaf. Thread the public alias into producer synthesis.
- **D5 — Guards run on an original-tree snapshot before substitution/producer planning (F3).** window= / first-last / nested-transform / in-filter guards fire on the immutable pre-substitution trees with the exact DEV-1739 messages; the post-substitution computed-dimension filter pass (DEV-1825) is retained for legitimate filters over computed dimensions.
- **D6 — Dispatch deferrals raise `NotImplementedError` citing DEV-1824.** Combined attach + {cm/wm/rk/transform}; row+combined coexistence (already raises today — status-quo, not a new restriction).

## Risks / Trade-offs

- Byte-identity of the generalized combined path → reuse the exact cm combined machinery; regenerate `local/*` goldens with `SLAYER_UPDATE_GOLDEN=1` and diff vs the committed baseline before blessing; surface any unavoidable divergence to the reviewer.
- Guard-ordering regression (a guarded shape stops raising, or a legitimate computed-dim filter wrongly raises) → the original-tree snapshot + a mixed test pinning both directions.
- Touching `_render_with_cross_model_plans` destabilizing cross-model → the `cross_model/*` goldens are the regression net; the generalization is additive (a new join-back source); cm-only paths unchanged.
- Producer `_cm_` naming seed / alias drift → reproduce `_cm_orders__<canonical>` + the dotted public-alias body; verified against golden.
