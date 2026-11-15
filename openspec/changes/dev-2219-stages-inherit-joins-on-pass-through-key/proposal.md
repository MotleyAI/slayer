## Why

A stage's output is flat today: a later stage sees only its columns, never the relationships of the datasets its keys came from. A rollup grouped by `orders.customer_id` cannot reach `customers` without declaring the relationship again in a query extension. Slice 7 of SLayer 2.0 (umbrella DEV-2212) makes a stage keep the edges its pass-through key columns carry.

## What Changes

- A stage inherits every edge touching a dataset it reads, in either direction and wherever the edge was declared (stored, an extension of that stage, or inherited by an earlier stage), when it passes the edge's key columns through unchanged. A time bucket, a cast or a computed column is not a pass-through.
- An inherited edge's cardinality is re-derived: the target side keeps the source edge's arity, and the stage side is "one" exactly when the stage's grain makes it unique on the key.
- Declared edges win: an identical declared edge absorbs the inherited one, and a declared edge of the same name replaces it. Distinct parallel edges follow the existing parallel-edge rules.
- A stage read by a later stage binds as the dataset its schema defines, through the one model scope. The flat stage scope is retired: a dotted reference whose walk fails gets the general unknown-reference error with the flat suggestion, and short-form routing works from a stage root.
- **BREAKING** A dataset reached through a stage's pass-through key was an unrelated dataset (broadcast, with a warning); it is now joined.
- **BREAKING** (fix) Stage ordering and each stage's join graph come from one extractor that also sees the siblings an expression names. A stage's join graph holds only the siblings it names, so routing never passes through an unnamed sibling, and a stage reading a sibling only through an expression no longer depends on the stages' alphabetical order.
- `inspect` and the model summary list a saved query-backed model's inherited edges, marked as inherited.
- `architecture/python.engine.arc42.md` P5 and its rationale sentence are reworded (approved).

## Capabilities

### New Capabilities
(none)

### Modified Capabilities
- `queries/multi-stage`: ordering and join-graph membership read every sibling a stage names; new requirement "Stages inherit edges on pass-through keys".
- `queries/query-backed-inline`: a saved relation's declared relationships win over inherited ones and ordinary stages are no longer flat; new requirement for listing inherited edges in `inspect`.
- `queries/extensions`: a later stage sees a stage's output schema including the edges it inherited from extension relationships.
- `queries/dotted-dimension-routing`: routing applies from a stage root; a downstream dotted reference follows the general flattened-column refusal.

## Impact

- **Code:** `slayer/core/scope.py` (`StageColumn` pass-through origin, `StageSchema` inherited edges), `slayer/engine/compile/stages.py` (schema emission), a new engine module computing inheritance, `slayer/ir/source_bundle.py` (`model_from_stage_schema`, `resolve_scope`, sibling threading), `slayer/engine/plan.py`, `slayer/engine/stage_ordering.py`, the `StageSchema`-scope branches in `slayer/engine/{binding,bind_inputs,elaborate_env,extensions,key_metadata}.py` and `compile/stages.py`, `slayer/inspect/model_render.py`.
- **Results:** queries whose stage passes through a key to a dataset they read change from broadcast to joined values.
- **Docs:** `docs/concepts/queries.md`, `docs/concepts/models.md` (query-backed models), `docs/reference/mcp.md`, help memories, `docs/upgrading/2.0.md`.
- **Depends on:** DEV-2021 (merged) and DEV-1607 (implemented after it merges; this change uses its vocabulary). DEV-2277 runs in parallel; whichever merges second respells the other's scenarios.
