## Why

A measure-typed filter holding a re-aggregation (`max(sum(orders.amount, partition_by=[orders.region, orders.city])) > 60`) works on its own, but fails as soon as a sibling metric reads across a relationship (`sum(customers.spend)`, `sum(items.qty)`): the re-aggregation's internal `__regroup__` placeholder is resolved inside the cross-dataset producer, which does not carry it (on `main`: `UnknownReferenceError: Cannot resolve reference 'orders.__regroup__0__max'`; earlier a database binder error). A sibling metric must not decide whether a filter evaluates (semantics Law 3, Axiom 9, Axiom 14), and `queries/positions` › Filter conjuncts stratify already says only stratum-0 field conjuncts feed producers.

## What Changes

- Every producer that inherits the population's filters (local regroup, re-aggregation, cross-dataset including its association arm, and the host population disposal) reads ONE selection: the filters the checker typed field-typed at stratum 0, chosen by index from the checker's typing, never re-derived from a filter rewritten by placeholder substitution.
- The cross-dataset path's `phase == ROW` filter selection retires; a measure-typed or stratum>0 filter whose phase a re-aggregation substitution lowered no longer reaches a cross-dataset producer.
- No behaviour beyond the bug fix changes: the specified stratification already holds for every filter without a re-aggregation.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — the behaviour is already specified (`queries/positions` › Filter conjuncts stratify, Measure mask prunes cells only); this change makes the engine conform. `skip_specs: true`.

## Impact

- `slayer/engine/compile/stages.py` (producer filter inheritance: `_desugar_regroups`, `_regroup_inherited_filters`, `dispose_population_filters`, cross-dataset synthesis and `_ProducerSynthesisContext`).
- New unit tests on SQLite and DuckDB; no API, storage, docs or dependency change.
