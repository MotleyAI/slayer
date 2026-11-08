# Stage 2 — local family unification + attach-dedup on the regroup primitive

## Why

Stage 1/1a left two LOCAL isolation families off the regroup primitive: bare windowed measures (`_wm_`, e.g. `amount:sum(window='1y')`) and bare local first/last (`_rk_`, e.g. `amount:last`). Their bespoke renderer arms are why nine compatibility-matrix cells fail closed (row attaches × windowed/ranked measures, computed-dimension grain anchoring), why the DEV-1504 G4/G5/G6/G7 guards forbid windowed measures in transforms/composites/filters, and why `time_shift` over a ranked aggregate is unrenderable. Separately, the same aggregate consumed in both attach roles ships as duplicate producers (DEV-1824 D10), and a transform-in-dimension mixing a windowed or first/last inner aggregate with a different-grain sibling stays fail-closed behind the DEV-1839 union-grain machinery.

## What Changes

- **Plan-time desugar**: a bare windowed / first-last measure (also in order and filter roles) becomes a regroup combined attach whose producer grain is the full projected query grain — the windowed rolling axis entering as the D5 synthesized time bucket. Public result keys, aliases, and response schemas are unchanged.
- **Producers generalize**: uniformly self-contained (single `_cm_` naming; a windowed-collapse mirrors the ranked one), may carry internal LOCAL row attaches (built from original pre-substitution dimension expressions), and anchor any dimension key in their grain. The windowed `_src` / ranked inner selects render regroup-aware.
- **Consumer-level deletion**: the `_wm_`/`_rk_` renderer wiring, `IsolationKind.WINDOWED`/`RANKED_HOST` branches, `OrderScope.WINDOWED_CTE`/`RANKED_CTE`, and the row-attach × windowed/ranked coexistence arm are removed. The computation kernels survive producer-internal. Cross-model (`RANKED_TARGET`, `_cm_`) is untouched (stage 3).
- **Guard dissolution**: G4/G5/G6/G7 (+ the post-projection mixed-filter twin) and `time_shift`-over-ranked become supported shapes; G1/G8/G2 and the ranked no-ranking-column error stay; G3 and the cross-model `partition_by` messages re-point to DEV-1836. The `change`/`change_pct`-over-attached internal-error defect is fixed.
- **General attach-dedup pass** (plan-time): one producer per structural identity (canonical producer spec equality), serving N attaches across both phases; subsumes D10 and collapses bare-vs-explicit-partition twins.
- **Mixed-grain lift for windowed / first-last inner aggregates** (assumes DEV-1839 merged): effective grains everywhere — windowed = `partition_by ∪ {active time bucket}`; the synthesized bucket IS the consumer's bucketed time dimension for all grain purposes.
- Golden divergences (all bare-windowed shapes; ranked near-identical modulo CTE name) are enumerated and batch-approved before re-blessing.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/partitioned-aggregates`: bare windowed / first-last measures join the partitioned-aggregate composition surface (transforms, composites, filters, coexistence with row attaches); producer dedup becomes a specified property; the coexistence requirement extends to the migrated families.
- `queries/computed-dimensions`: row attaches coexist with windowed/ranked measures; any dimension key is legal in a producer grain; the different-grains-in-one-transform requirement extends to windowed / first-last inner aggregates via effective grains and the synthesized time bucket.

## Impact

- `slayer/engine/stage_planner.py` (desugar, producer synthesis, dedup, guard rework), `slayer/engine/isolation.py`, `slayer/engine/ranked_planner.py`, `slayer/engine/planned.py` (producer/attach IR split), `slayer/engine/regroup_planner.py` (effective grains), `slayer/sql/generator.py` (consumer-arm deletion, regroup-aware kernels, windowed collapse), `slayer/sql/render/order_terms.py`.
- Tests: 10 matrix cells flip (incl. the DEV-1839-added `mixed` family's wm/rk); guard tests reverse direction; `_wm_`/`_rk_` SQL-shape suites rewritten preserving intent; golden baselines re-blessed per approved batch; perf corpus re-recorded.
- Docs: `docs/architecture/composable-attach.md`, `docs/concepts/formulas.md`, `docs/concepts/queries.md`, `.claude/skills/slayer-query.md`.
- Depends on DEV-1839 (merged before implementation); cross-model families remain DEV-1836.
