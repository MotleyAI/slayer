## Why

A value the query compiles only to evaluate a filter or an order key can silently become a `GROUP BY` key of the host `_base` relation, so the result comes back at the wrong grain with no error (Axiom 12, sql P12). Two live shapes: a row filter on a computed dimension's partition key (`customer_id is not null` beside `case when sum(amount, partition_by=customer_id) > … end`) returns one row per customer instead of one per band; and an ORDER BY on a non-dimension column beside a `first`/`last` measure returns one row per sort-key value. The cause is structural: whether a plan is grouped, and what its grain is, are re-derived ad hoc at several sites that disagree.

## What Changes

- The planner stamps one grain fact per plan: the slots of the dimension / time-dimension positions, or none in raw-row mode. Every consumer that today re-derives the grain or "is grouped" (ORDER BY wrap classification, ranked / trailing-window kernel grains, transform auto-grain, frame-bound columns, the attach-covers-producer-grain check, the stage schema grain) reads it.
- An attach joined inside `_base` no longer forces its join key to be a `_base` column.
- A plan invariant, checked at plan construction and again at render: in a grouped plan every row-level value `_base` must project is a grain member or determined by the grain; anything else fails loudly instead of widening the grain.
- Result behaviour: the two shapes above return one row per dimension combination; every previously correct query keeps its SQL byte-identical.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/semantics`: the Grain guarantee requirement gains the rule that a value compiled only to evaluate a filter, an order key or an attach join never changes the result's grain, with scenarios for both shapes.

## Impact

- `slayer/ir/planned.py` (`PlannedQuery.grain` replaces `distinct_dimension_values`; the grain-determination invariant).
- `slayer/engine/compile/stages.py`, `slayer/engine/compile/staging.py` (grain computation, ORDER BY wrap, attach join-key columns, producer-grain check).
- `slayer/sql/generator.py` (kernel / transform grain reads, render-time invariant check).
- Tests: new executed SQLite + DuckDB tests and plan-level tests; one existing assertion (`tests/test_dev1645_invalid_postgres_sql.py` ranked-scope ORDER BY) moves to the MAX-wrapped form.
- No public API, docs or persistence change.
