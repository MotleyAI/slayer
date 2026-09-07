# Proposal: consecutive_periods accepts null-test predicates

## Why

`_PREDICATE_COMPARISON_OPS` in `slayer/sql/generator.py` omits `is` / `is not`,
though the Mode-B DSL produces exactly those `ArithmeticKey` ops for
`is None` / `is not None`. Consequently `consecutive_periods` wrongly rejects
compound predicates containing a null test ("'and' / 'or' / 'not' require
boolean-shaped operands") and mis-renders a bare top-level null test through the
value path, wrapping a boolean in `... IS NOT NULL AND ... <> 0`, which fails on
strictly-typed dialects. Additionally (found in plan review), the emitter's
`COALESCE(<predicate>, FALSE)` wrapper is invalid T-SQL (predicates are not
scalar values there) and redundant everywhere it is used — every use site is a
`CASE WHEN` condition, where NULL already acts as false.

## What Changes

- Add `"is"` / `"is not"` to `_PREDICATE_COMPARISON_OPS` so null tests classify
  as boolean-shaped in the `consecutive_periods` gate and emitter.
- Extend the gate's error message and `_is_boolean_shaped`'s docstring with the
  null-test shape.
- Drop the redundant `COALESCE(<predicate>, FALSE)` wrapper in
  `_emit_consecutive_periods_ctes_for_planned`; use the predicate directly as
  the `CASE WHEN` condition (identical semantics, valid T-SQL).
- Docs: add null tests and `BETWEEN` to the accepted-shapes sentence in
  `docs/concepts/formulas.md`.
- Tests: execution tests (SQLite + DuckDB) for null-test predicates, golden SQL
  cases across five dialects, a boolean-in-value-position negative anchor;
  re-bless existing `consecutive_periods` golden entries (COALESCE removal). If
  the SQL Server strict-xfail `test_consecutive_periods_with_boolean_predicate`
  now passes (verified against a live SQL Server), remove its xfail marker in
  the same change.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/transforms`: "Composite-input consecutive_periods" gains null tests
  (`is None` / `is not None`) as an accepted predicate shape and drops the
  boolean-as-scalar wrapper from emitted SQL; "consecutive_periods predicate
  typing contract" adds the null test to the boolean-shaped definition (accepted
  at top level and under connectives, rejected in value positions).

## Impact

- `slayer/sql/generator.py` — `_PREDICATE_COMPARISON_OPS`, `_assert_cp_shape`
  message, `_is_boolean_shaped` docstring, `pred_in_case` construction in
  `_emit_consecutive_periods_ctes_for_planned`.
- `docs/concepts/formulas.md` — one sentence.
- `tests/test_dev1854_null_test_predicates.py` (new),
  `tests/test_dev1846_golden_sql.py` + `tests/golden/dev1846_sql_baseline.json`
  (two new cases; existing cp entries re-blessed),
  `tests/golden/dev1750_sql_baseline.json` (existing cp entries re-blessed),
  `tests/integration/test_integration_sqlserver.py` (xfail marker, conditional
  on live verification).
