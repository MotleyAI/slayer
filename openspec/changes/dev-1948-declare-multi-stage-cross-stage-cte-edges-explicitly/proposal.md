## Why

Stage→sibling edges are computed twice with different rules: the engine orders a
query list by every sibling reference, then `plan_stages` re-sorts it following
`source_model` only — so a valid list whose stage joins a sibling
(`[x, c(src x), b(joins c), root]`) fails with `target 'c' not in source bundle`.
The same edges never reach the SQL generator as plan data, so the multi-stage
`WITH` orders stage→sibling and root→stage reads (and a stage body's reuse of an
earlier stage's producer) by insertion order rather than by declared dependencies
(sql arc42 P6).

## What Changes

- One stage-edge extractor (`stage_ordering.stage_sibling_reads`) and one ordering
  function (`topologically_order_stages`) serve ordering, planning and emission;
  `compile/stages._topo_sort` is deleted.
- `plan_stages` orders stages with `topologically_order_stages` and records each
  stage's sibling reads on a new typed `PlannedQuery.stage_reads`.
- `generate_planned_stages` declares every cross-stage edge from `stage_reads`
  (stage relations, their hoisted CTEs, the root's entries), records a stage body's
  producer reuse, and fails closed on a planned list that is not in dependency order.
- Emitted SQL for every currently-working query is byte-identical.

## Capabilities

### New Capabilities
- `queries/multi-stage`: ordering of a multi-stage query list by its sibling references.

### Modified Capabilities

## Impact

- `slayer/engine/stage_ordering.py`, `slayer/engine/plan.py`,
  `slayer/engine/compile/stages.py`, `slayer/engine/query_engine.py`
- `slayer/ir/planned.py` (`PlannedQuery.stage_reads`)
- `slayer/sql/generator.py` (`generate_planned_stages`),
  `slayer/sql/render/cte_assembly.py` (`CteEntry.depends_on` doc)
- `tests/_dev1871_raise_ledger.py` (two `_topo_sort` rows removed with the function)
