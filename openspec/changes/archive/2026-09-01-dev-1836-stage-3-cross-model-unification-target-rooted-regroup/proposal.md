# Stage 3 — cross-model unification: target-rooted regroup producers, fan-out-safe grains, retire classify_isolation

## Why

Cross-model aggregates are the last measure families off the regroup primitive: they ride bespoke `_cm_`/`_rk_` planner+renderer arms dispatched by `classify_isolation`, which is why cross-model sources are illegal in dimension expressions and `window=`, why the row-attach × cross-model matrix cells fail closed, and why intermediate-hop dims raise. Worse, the existing reachability tests are arity-blind: a dimension or filter reached through a 1:N join is pulled into a re-rooted `_cm_` sub-plan and silently fans it out (double-counted values) — the fan-out/chasm-trap defect class. This change migrates the cross-model families onto target-rooted producers and, in the process, gives every metric provably fan-out-safe semantics (solving DEV-1689 and the correctness half of DEV-1738).

## What Changes

- **Safety predicate**: a join hop is provably ≤1 iff its target-side join columns cover a declared PK/unique set (structural proof) or `Join.cardinality` declares `many_to_one`/`one_to_one`; unknown = unsafe (fail-closed). Evaluated over existing stored edges only — never synthesizes traversal. Replaces the arity-blind reachability tests for value paths.
- **Unified compilation rule**: every aggregate roots at the model whose rows it aggregates, computes at the fan-out-safe subset of its requested grain (explicit `partition_by` or the query dims), and broadcasts across the rest — with per-measure response metadata, or an error under the new `SlayerQuery.strict` flag. **BREAKING (approved, enumerated)**: arity-unsafe grain members that previously joined in silently now broadcast; unsafe aggregate inputs and unsafe explicit `partition_by` members now error.
- **Migration**: `_cm_` forward/re-rooted, cross-model `_rk_`, and cross-model partitioned aggregates become target-rooted regroup producers, guarded by a post-discovery total-routing invariant (every aggregate leaf: inline | producer | explicit rejection). The residual host-rooted routes (crossing `Column.filter` inputs, host-grain wraps, filtered-local) still ride `cross_model_planner.py` / `classify_isolation`; deleting those plus `CrossModelAggregatePlan`, `_narrow_shared_grain_to_partition`, and the cross-model exclusion in `combined_partitioned_aggregates` is deferred to DEV-1838 (decision B1).
- **Guards fall with positive coverage**: cross-model sources in dimension expressions and `window=`, row attach × cross-model measure (DEV-1837 matrix cells), producer-needs-cross-model-CTE, intermediate-hop shared grain.
- **Metadata enablement**: DEV-1689 (PK/uniqueness stamped onto query-backed virtual models when the backing stage provably dedups), Cube importer maps `relationship` → `Join.cardinality`, and `validate_models`/import reports flag unproven or profiling-contradicted joins.
- Golden baselines stay byte-identical except individually approved batches; executed values change only in the enumerated, approved semantics flips.

## Capabilities

### New Capabilities

- `queries/cross-model-aggregates`: cross-model aggregate composition — target-rooted producers, the safe-grain/broadcast semantics, strict mode, broadcast metadata, producer filter inheritance, and the fan-out-safety guarantees for every metric.
- `models/join-cardinality`: how join arity is declared, structurally proven, imported (Cube), propagated onto query-backed models (DEV-1689), and validated.

### Modified Capabilities

- `queries/partitioned-aggregates`: cross-model sources join the full partitioned-aggregate surface (`partition_by`, `window=`, `first`/`last`, transforms, composites, filters); explicit partition keys must be fan-out-safe; the coexistence requirement extends to the migrated cross-model families; the cardinality requirement extends to nested attaches inside target-rooted producers.
- `queries/computed-dimensions`: cross-model aggregate sources become legal in dimension expressions (grain self-containment relaxes from "local to the query's source" to "fan-out-safe from the aggregate's root"); computed dimensions coexist with cross-model measures; the deferral-guard requirement sheds the lifted guards.

## Impact

- `slayer/engine/`: new safety-predicate module; `stage_planner.py` (generalized discovery, producer synthesis, total-routing invariant), `regroup_planner.py`, `planned.py` (warning/broadcast fields on `RegroupAttachPlan`; `CrossModelAggregatePlan` deleted), `isolation.py` (retired; `may_inline_crossing_inputs` seam survives), `cross_model_planner.py` + `ranked_planner.py` cross-model arm (deleted/migrated), `query_engine.py` (warning collector traversal, DEV-1689 stamping), `cardinality.py` (consumed at plan time).
- `slayer/sql/generator.py`: `_cm_` consumer arms deleted; producers render via the existing recursive path.
- `slayer/core/query.py` (`strict` flag), response warning kind `broadcast` (REST/MCP pass-through).
- `slayer/cube/converter.py` (relationship mapping), `validate_models` + import reports.
- Tests: DEV-1837 matrix cells flip; `_cm_` SQL-shape suites rewritten preserving intent; goldens re-blessed per approved batch; new safety/broadcast/strict suites; perf corpus re-recorded.
- Docs: `docs/architecture/composable-attach.md`, `docs/concepts/queries.md`, `docs/concepts/formulas.md`, `docs/concepts/models.md`, `.claude/skills/slayer-query.md`.
- Follow-ups filed: DEV-1840 (EXISTS filter pushdown), DEV-1841 (distinct-entity attribution), DEV-1842 (dotted saved measures); DEV-1738 re-scoped to perf consumption.
