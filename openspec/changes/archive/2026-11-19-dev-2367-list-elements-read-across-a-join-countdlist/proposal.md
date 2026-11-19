## Why

Reading a list column's elements from a dataset other than its population breaks two ways: an association homed on a list child (e.g. `count(contractors.skills.*)` by `assignments.id`) fails with a binder error or `ScopeLeakError`, and an aggregation homed on a list child is refused from the parent's population yet computed from another (`w(contractors.rate)` with default `skills.value`: refused from `contractors`, 230 from `assignments`). Both violate the home-dataset axioms: an aggregate's value never depends on which dataset roots the query.

## What Changes

- A list child that is an association home is told apart by its parent row's identity (primary key, else per-query row identity) plus the element's position in the list, so every association homed on a list child runs; repeated elements count separately.
- The element position is emitted inside the parent's own rows, only when association needs it, on every native-list dialect; the parent is never joined back.
- Every aggregate is routed by its resolved home, not by its source anchor: an aggregation homed below the query root is computed at that home from any population, with the same value or the same typed refusal. This also fixes non-list home-below-root shapes (e.g. `sum(rate * assignments.id)` from `contractors`), refused today.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/list-children`: "Association across a list hop needs no key" gains the list-child-home identity (parent identity + element position).
- `queries/semantics`: a new requirement that the home, not the query root, decides where an aggregation is computed.

## Impact

- Planner: `slayer/engine/compile/stages.py` (association entity, producer routing), `slayer/engine/compile/discovery.py`, `slayer/core/keys.py`, `slayer/engine/elaborate_env.py`, `slayer/engine/cardinality/relationship_safety.py`, `slayer/engine/population/home.py` (home classification).
- SQL: `slayer/sql/generator.py` (list-child root emission), `slayer/sql/dialects/*.py` (`list_relation` position).
- Docs: one sentence in `docs/concepts/queries.md`. arc42: enforcement tags on semantics Axioms 2 and 8 (approved).
- No API or storage change; queries that failed or were refused now compute.
