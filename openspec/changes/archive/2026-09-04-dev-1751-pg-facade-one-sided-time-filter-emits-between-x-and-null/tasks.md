# Tasks: dev-1751-pg-facade-one-sided-time-filter-emits-between-x-and-null

## 1. Tests first (spec-tests stage — all fail before implementation)

- [x] 1.1 Rewrite `test_half_open_gte_lifts_to_date_range_lo` (tests/facade/test_translator.py) to assert no `date_range` and `filters == ["ordered_at >= '2024-01-01'"]`; rename to describe verbatim behavior; verify it fails against current code
- [x] 1.2 Rewrite `test_combined_half_open_gte_and_lte_set_both_bounds` to assert no `date_range` and both comparators verbatim in `filters`; rename; verify it fails against current code
- [x] 1.3 Add translator tests: one-sided `<=`, strict `<` alone, strict `>` alone, paired `>= AND <=` (no lift), reversed operand `'2024-01-01' <= ordered_at`, and mixed `BETWEEN ... AND ts >= ...` (BETWEEN lifts, comparator verbatim); verify each fails or errors against current code where behavior differs
- [x] 1.4 Add execution test (DuckDB): facade-translated one-sided filter returns the correct non-empty row set end-to-end; verify it fails (returns zero rows) against current code — plus (Codex test review) a strict-upper-boundary execution case and exact-edge BETWEEN inclusivity rows
- [x] 1.5 Add planner fail-closed tests through the public planning path: `['2024-01-01', None]`, `[None, '2024-12-31']`, `[None, None]` each raise `ValueError` naming the dimension — on a plain model AND on a multi-stage (`source_queries`) model; verify they fail against current code — message asserted to name the dimension, the received range, and the one-sided-filter suggestion (Codex test review)
- [x] 1.6 Confirm existing guards still pass unchanged: `test_between_lifts_to_date_range` and the DEV-1745 wrong-length warning tests

## 2. Implementation (spec-implement stage)

- [x] 2.1 Facade: delete `_lift_time_comparator` and the GTE/GT/LTE/LT branch in `_classify_where_conjunct`; simplify `_apply_where` to direct `td.date_range = [lo, hi]` (drop the merge + `# type: ignore[assignment]`); verify tasks 1.1-1.4 tests pass
- [x] 2.2 Planner: raise `ValueError` for a 2-element `date_range` with a `None` bound, checked before the `ModelScope` skip in the date_range loop; verify task 1.5 tests pass
- [x] 2.3 Run the full non-integration suite (`poetry run pytest -m "not integration"`) and fix any failures
- [x] 2.4 Docs: fix `docs/interfaces/flight-sql.md` comparator row (verbatim filters, not "same lift"); add the two-bound `date_range` contract sentence to `docs/concepts/queries.md`; grep docs/skills for other stale mentions of comparator lifting; verify by re-grep
- [x] 2.5 Run `poetry run ruff check slayer/ tests/` and fix any issues
