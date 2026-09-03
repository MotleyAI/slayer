# Design

## Context

See proposal.md — Why. All classification flows through one set:
`_PREDICATE_COMPARISON_OPS` feeds `_is_boolean_shaped`, used by both the
`_walk_cp_predicate` validation gate and the emitter's `predicate_is_boolean`
branch, so a single-set change fixes both symptoms. `in` / `not in` are not
affected (they bind to `InKey`, already boolean-shaped); no other op is missing.

## Decisions

- **One-set fix, no walk changes.** Post-fix an `is` node's children keep
  `expect="value"` (its operands are values, like any comparison), which is
  already the `_walk_cp_predicate` behaviour for non-connective ops.
- **Drop `COALESCE(<predicate>, FALSE)` instead of dialect-splitting it**
  (Codex plan-review finding). Every `pred_in_case` use site is a `CASE WHEN`
  condition, where NULL is already not-true — the wrapper is semantically
  redundant and is invalid T-SQL (predicates are not scalar values there).
  `pred_in_case` collapses to `predicate`. Alternative considered: keep the
  wrapper and emit a T-SQL-specific form — rejected, more code for zero
  semantic gain.
- **`value_expr.py`'s `_COMPARISON_OPS` stays untouched.** It only drives
  cosmetic operand parenthesisation in filter families; SQL's `IS` binds looser
  than arithmetic and the row-expression renderer's precedence table already
  covers `exp.Is`.

## Risks / Trade-offs

- [COALESCE removal changes every existing cp golden entry] → mechanical
  re-bless via the harness `ALLOWED_DELTAS` loop; semantics pinned by the
  SQLite + DuckDB execution suite and the Postgres integration tests.
- [SQL Server strict xfail `test_consecutive_periods_with_boolean_predicate`
  may now xpass and fail the integration suite] → at implement time, run it
  against a live SQL Server (Docker) if available; remove the marker only on a
  verified pass, otherwise keep it and record the outcome.
