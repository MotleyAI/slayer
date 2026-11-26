## Why

An aggregate with no `partition_by=` has the query grain (semantics Axioms 5 and 11.1), but
inside a nested layer the planner reads it at that layer's grouping. A transform whose
operand mixes such an aggregate with a coarser one therefore returns a wrong value
(`rank(sum(orders.amount) / sum(orders.amount, partition_by=orders.region), direction='desc')`
is 1 in every row) or fails with an internal planner error (`cumsum`, `lag`, `lead`), and
several nested shapes return wrong values silently (DEV-2416).

## What Changes

- The planner takes a transform's producer grain from the one definition the checker
  already uses, so an inner aggregate with no `partition_by=` counts at the query grain.
- One rule gives a nested aggregate with no `partition_by=` the query grain wherever the
  enclosing layer's grain may differ from the query's: under a transform inside an
  aggregation's source or parameters, under a transform whose operand grain differs from
  the query grain, and in the operand of a `first` / `last` over an aggregate. It applies
  to every kind of aggregate (same-dataset, cross-dataset, windowed) at any depth. It
  replaces two helpers that covered same-dataset unwindowed aggregates only.
- A grain given by that rule stays unwritten for the mode axis: it associates with the
  `associated` warning, broadcasts under the deprecated `broadcast` mode and is refused
  under `error` exactly as the same aggregate standing alone, and no message names a
  `partition_by=` the user did not write.
- Fixed wrong values, each now equal to its explicit-grain spelling: the rank family and
  the time-ordered transforms over a mixed operand; a transform over a finer operand with
  a windowed or cross-dataset inner, as an aggregation source or in a `first` / `last`;
  a transform over a second-order aggregation whose inner has no `partition_by=`
  (`rank(max(sum(orders.amount), partition_by=orders.region), direction='desc')`).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/transforms`: adds the requirement "An ungrained inner aggregate types at the
  query grain".

## Impact

- `slayer/core/keys.py`: transform producer grain, the nested-default pass, a
  defaulted-grain marker on the aggregate key.
- `slayer/engine/bind_inputs.py`: where the pass runs.
- `slayer/engine/compile/stages.py`, `slayer/engine/compile/discovery.py`,
  `slayer/engine/cardinality/relationship_safety.py`, `slayer/core/refs.py`: readers that
  tell a written grain from a defaulted one, and naming.
- `architecture/semantics.arc42.md`: one enforcement tag on Axiom 11.
- No API, storage or docs change. Queries that work today keep their values, warnings
  and errors.
