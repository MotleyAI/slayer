## Why

A cross-model `count` over a to-many child returns NULL instead of 0 for a parent with
no children (`count(orders.id)` from `customers` → NULL for an orderless customer). The
aggregate is computed in a producer rooted where its rows live and attached back by a
null-safe LEFT JOIN; an absent producer row reads as NULL on every attach path, so the
count family's empty-set value is lost in every position (projection, filter, order,
composite, dimension, re-aggregation, transform operand) and in every mode.

## What Changes

- Introduce the aggregation's **empty value**: 0 for the built-in count family
  (`count`, `count_distinct`, `count_distinct_approx`, including `count(x.*)`), NULL for
  every other aggregation, custom aggregation, model-level formula override, and
  transform.
- A population cell with no home rows for an attached aggregate takes that aggregate's
  empty value, in every position and every `to_many_handling` mode, including windowed
  and associate producers and re-aggregation carriers.
- Consequences: a measure filter `count(orders.id) = 0` matches childless parents;
  re-aggregating per-parent counts includes the zeros (`avg(count(orders.id,
  partition_by=[name]))` counts orderless customers as 0).
- Unchanged: non-count aggregations stay NULL on empty cells; a transform cell with no
  value (e.g. `time_shift`'s missing prior bucket) stays NULL; no population cells are
  fabricated.
- Normative harness edits (approved): `semantics.arc42.md` Axioms 4 and 10,
  `sql.arc42.md` P10.
- Docs: generalise the empty-set sentence in `docs/concepts/formulas.md` and the MCP
  help text.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/cross-model-aggregates`: adds the requirement that empty cells take the
  aggregation's empty value.

## Impact

- `slayer/core/enums.py` (empty-value definition), `slayer/ir/planned.py`
  (`RegroupSubstitution.empty_value`, required), `slayer/engine/compile/stages.py`
  (every attach synthesiser stamps it; owner-resolved override detection shared with
  `slayer/engine/binding.py` / `slayer/engine/agg_registry.py`), `slayer/sql/generator.py`
  and `slayer/sql/scope.py` (one attached-value record/builder for combined and row
  attaches).
- Result values change for queries hitting childless parents with count-family
  aggregates (NULL → 0) and for re-aggregations over them.
- `architecture/semantics.arc42.md`, `architecture/sql.arc42.md`, `docs/concepts/formulas.md`,
  `slayer/mcp/server.py` help text.
