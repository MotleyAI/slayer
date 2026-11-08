## Why

DEV-1825 landed a first-class regroup primitive (`RegroupAttachPlan`) to replace per-feature isolated-CTE + join-back grafts, wiring its `attach_phase="row"` arm for computed dimensions. Its acceptance requires at least one existing graft migrated end-to-end. DEV-1739's partitioned-measure join-backs are that first migration: routing them through the primitive's `attach_phase="combined"` arm retires their bespoke isolation code and validates the primitive on a real consumer.

## What Changes

- Wire `attach_phase="combined"`: a planner-synthesized producer attaches at the combined SELECT (post-aggregation), substituting for the consumer's partitioned-measure aggregate slot — the position `CrossModelAggregatePlan` occupies today.
- Reroute the **local** partitioned-measure trigger from the DEV-1503/DEV-1709 host-rooted isolation path to a combined regroup attach: remove `classify_isolation`'s `partition_keys is not None` branch (`engine/isolation.py`) and add a fail-closed assert that no local partitioned aggregate reaches the aggregate loop.
- Retire the partition-specific arms of `_dispatch_filtered_local` / `_plan_filtered_local` (`engine/cross_model_planner.py`); non-partition filtered-local behavior unchanged.
- Generalize the regroup desugar to discover partitioned aggregates in measures / composites / order specs (not just computed dimensions) and generalize `_render_with_cross_model_plans` to render combined regroup attaches as first-class join-back producers.
- **Out of scope (deferred to DEV-1824):** cross-model partitioned measures (`_narrow_shared_grain_to_partition` is **kept**); cross-phase interning; combined-attach composition with cross-model/windowed/ranked/transform measures.
- No BREAKING changes: golden SQL byte-identical against `tests/golden/dev1739_sql_baseline.json`; result keys / meta / warnings unchanged.

## Capabilities

### New Capabilities
<!-- None: this is an internal refactor. The response contract (result keys, meta,
     guard messages, warnings) is unchanged; only the internal plan object and render
     routing change. skip_specs: true is set in .openspec.yaml. -->

### Modified Capabilities
<!-- None at the spec level, and the DEV-1739 golden cases stay byte-identical. Two
     SQL-level behaviors change INTENTIONALLY (recorded in DECISIONS.md), neither a
     specified-behavior promise: an ORDER-BY-only partitioned aggregate now isolates
     and sorts at its partition grain (the DEV-1739 path silently sorted by the finer
     per-row total), and same-partition-set measures now intern into one producer with
     N outputs. Response contract (result keys / meta / warnings) is unchanged. -->

<!-- NOTE: the earlier "byte-identical generated SQL" framing was corrected here (CR):
     it holds for the DEV-1739 golden baseline, not for the two ORDER BY / interning
     cases above, which are approved divergences. -->

## Impact

- **Code:** `engine/isolation.py`, `engine/cross_model_planner.py` (retire local partition arms), `engine/regroup_planner.py` + `engine/stage_planner.py` (generalized discovery / desugar / guard-ordering), `sql/generator.py` (generalize `_render_with_cross_model_plans` for combined attaches).
- **Tests:** `tests/test_dev1739_execution.py` / `_golden_sql.py` / `_guards.py` reused unchanged as the acceptance net; new planner / interning / guard-preservation / dispatch-deferral / routing tests.
- **No** change to public API, response shape, dependencies, or the `cross_model/*` and `guard/*` golden baselines.
