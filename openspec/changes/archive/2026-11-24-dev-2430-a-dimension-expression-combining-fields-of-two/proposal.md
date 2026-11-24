## Why

Axiom 2.2 makes a row-level expression illegal when no dataset determines all its operands, yet only aggregation sources and time dimensions check it, and differently: a dimension or order key combining fields of two sibling to-many datasets (`orders.amount * returns.amount` from `customers`) is accepted and pairs every order with every return, while a legal parent-plus-child time dimension is refused. The owner ruled (2026-11-23) to refuse such expressions in every value position; the refusal is breaking and gates 2.0.

## What Changes

- **BREAKING** A dimension (plain or computed), time dimension or row-level order key whose operands no single dataset determines over provably to-one hops fails with a typed input-safety error naming two operands and their fanning or unproven hops, in every `to_many_handling` mode, every stage, named or inferred population, raw-row mode included.
- A time dimension over a parent-plus-child expression (`case when customers.region = 'EAST' then orders.ordered_at end` from `customers`) is now homed on the child and associated, as the same expression already is as a dimension.
- Field filters are unchanged: a conjunct with sibling operands keeps restricting by association.
- Spec text only: the host-grain wrap is no longer called an exception to No double counting; it is always `min` / `max`, which duplicated rows cannot change.
- Axiom 2.2 gains the field-filter carve-out; Axiom 2 gains the new test's enforcement tag.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/semantics`: new requirement *Row-level expressions are homed or refused*; *No double counting* loses the host-grain exception sentence.
- `queries/cross-model-aggregates`: *Unsafe aggregate inputs fail closed* restates the host-grain wrap as duplicate-insensitive `min` / `max`.
- `queries/time-dimensions`: *Time dimensions accept row-level expressions* refuses sibling operands naming both, and homes a parent-plus-child expression on the child.

## Impact

- `slayer/engine/population/home.py`: one Axiom 2.2 row-level home resolver sharing candidate generation and determination with `aggregate_home`; `row_expression_crossing` retired.
- `slayer/engine/elaborate_env.py`: one row-level home check replacing `check_time_dimension_home`.
- `slayer/engine/bind_inputs.py`: dimensions, time dimensions and row-level order keys call the check.
- `architecture/semantics.arc42.md` (approved wording), `tests/_dev1871_raise_ledger.py`, `tests/test_dev2119_expression_time_dimensions.py` (approved alignment), new `tests/test_dev2430_row_expression_home.py`.
- Docs: computed-dimensions page, `docs/upgrading/2.0.md`.
