## Why

`agg(x) FILTER (WHERE c)` on a model-defined custom aggregation (`Aggregation.formula`) masks only `{value}` and column-valued parameters, so anything else its formula reads — `COUNT(*)`, a bare column — still sees every row: `rows(amount) FILTER (WHERE status = 'paid')` with `rows = COUNT(*)` counts all orders, and `SUM({value}) / COUNT(DISTINCT id)` divides the paid amount by every order. The FILTER clause promises SQL row semantics; input masking delivers them only for built-in aggregations, which skip NULL inputs.

## What Changes

- A FILTERed **non-built-in** aggregation restricts its rows rather than its inputs: it compiles as a producer whose WHERE carries `c`, attached back by the null-safe LEFT JOIN on its complete grain. Local, cross-model and stage sources; every consumer position; composes with `partition_by=`, `window=`, query row filters and joined-model conditions.
- **Built-in aggregations are unchanged** — they keep the input mask, a pure lowering optimisation.
- A grouped cell with rows but none passing `c` reads the custom aggregation's empty value, NULL; with no grain it reads the formula's value over no rows (unchanged from today's ungrouped behaviour).
- Condition typing, the rejection of aggregates / transforms / measures inside `c`, and result-key naming are unchanged. FILTER on a second-order aggregation stays refused (DEV-2191).
- One sentence in `docs/concepts/formulas.md`.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `aggregations/functional-form`: "An aggregation accepts a FILTER clause" — the restricted-inputs list becomes every row the aggregation reads, custom formulas' direct reads included, and the empty-cell value of a FILTERed custom aggregation is stated.

## Impact

- `slayer/engine/binding.py` (`_bind_filtered_agg`): non-built-in keys carry the condition on `AggregateKey.filter`.
- `slayer/core/keys.py`: one predicate deciding row restriction; `AggregateKey.filter` comment.
- `slayer/engine/compile/discovery.py`, `slayer/engine/compile/stages.py`: routing to a producer, the condition in the producer's own row filters, producer grouping by condition, window buckets from the population.
- `slayer/sql/generator.py`: fail-closed guard against a filtered non-built-in key reaching formula rendering.
- Tests: `tests/test_dev2186_custom_filter.py`, `tests/test_dev2186_postgres.py`, fixture additions in `tests/_dev2101_fixtures.py`.
- Docs: `docs/concepts/formulas.md`.
