# Design: dev-1850-keyless-grain-dual-role-cross-model-partitioned-aggregate

## Context

See proposal.md — Why. Two divergences between the local and cross-model
combined-consumer paths cause the crashes:

1. The bind-time strict partition-key validation (`_validate_partition_keys`,
   `slayer/engine/stage_planner.py`) exempts computed-dimension aggregates
   (`_dim_agg_keys`); its "also consumed combined → validate strictly" set
   `_combined_consumer_keys` comes from `combined_partitioned_aggregates`
   (`slayer/engine/regroup_planner.py`), which excludes cross-model sources.
2. The cross-model combined discovery `_discover_cross_model_combined`
   (`stage_planner.py`) walks non-dimension measures + orders + filters
   WITHOUT the local walk's row-routing exclusions (dim-role refs excluded
   from order-name walks and filter walks; top-level raw ORDER target
   included), so it manufactures a combined attach whose join-back has no
   host slot and dies at render.

The user-approved semantics decision (D1 below) was interviewed and the
issue's original "hidden host key slot" fix rejected; probes and rationale
live in the Linear issue comment trail for DEV-1850.

## Goals / Non-Goals

- Goal: exact local/cross-model behavior parity for every combined-consumer
  and row-scope-reference shape of a keyless partitioned aggregate.
- Goal: one discovery walk, one set of routing asymmetries — no second copy
  to drift again.
- Non-goal: making any keyless combined-consumer shape execute.
- Non-goal: changing bare (non-partitioned) cross-model aggregate behavior,
  warnings, error message wording, or any golden SQL.

## Decisions

### D1 — Clean error, not keyless support
The issue's likely-fix (synthesize a hidden host grain slot, trim in POST)
refines the result grain: a hidden GROUP BY key turns the 2-row `[sband]`
result into 4 rows and splits other measures' values, violating the
"Attachment preserves cardinality structurally" requirement, and the renderer
explicitly treats hidden row slots materialising into GROUP BY as
grain-corruption. Alternatives (aggregated MIN/MAX join guard with NULL on
mixed groups; producer re-aggregation) are data-dependent or arbitrary per
aggregation. Chosen: the documented local rule, extended uniformly.

### D2 — One unified combined-consumer discovery in `regroup_planner`
`combined_partitioned_aggregates` generalizes into a single walk over
non-dimension measures, order specs, and filters, emitting explicit buckets:

- local partitioned combined consumers (today's first return, order kept),
- cross-model partitioned combined consumers,
- cross-model bare aggregates (behavior unchanged, no exclusions),
- public-alias map and declared-type map (today split between the local walk
  and `_discover_cross_model_combined`).

Row-routing exclusions (`row_agg_set`) apply ONLY to partitioned aggregates
that actually have a row role. `_discover_cross_model_combined` retires; its
call site consumes the unified result. Alternative considered: mirror the
exclusions into the cross-model walk and keep two functions — rejected, it
re-creates the divergence this change fixes.

First-seen discovery order is preserved for every kept element, so producer
CTE naming and goldens are unaffected; the only removed elements are ones
whose shapes previously raised.

### D3 — Validation union at bind time
`_combined_consumer_keys` = local ∪ cross-model partitioned buckets. The
lenient dimension-role exemption predicate and the TD-bucket rewrite arm are
untouched; the existing error text already renders dotted cross-model keys
correctly (verified by probe).

### D4 — `local_discovery` split survives
The unified API keeps the producer-recursion contract: with
`local_discovery=False` the local buckets are empty while cross-model
discovery still runs. The blinding tests in `test_dev1836_total_routing.py`
re-point their monkeypatches at the unified seam (blind the cross-model
buckets), asserting the same total-routing invariant.

### D5 — Renderer backstop stays
The "missing a host / producer grain slot" RuntimeError remains: it still
guards producer-side grain-alias resolution and any future planner drift.

## Risks / Trade-offs

- [Refactor regresses alias/declared-type extraction] → existing pins
  `tests/test_cross_model_rename_dev1448.py` (renamed cross-model measures;
  `test_declared_type_casts_producer_column`) fail loudly.
- [Unified walk changes local discovery order] → goldens + `_cm_` count pins
  across the dev1824/1835/1838 suites; any golden diff is investigated, not
  re-blessed.
- [Exclusions accidentally applied to bare cross-model aggregates] → explicit
  buckets (D2) + full non-integration suite.
- [Parity drifts again later] → clean-error tests parametrized over paired
  (local, cross-model) formulas asserting identical error shape.

## Migration Plan

Pure planner-internal change; no storage, API, or model-version impact.
Rollback = revert the commit.
