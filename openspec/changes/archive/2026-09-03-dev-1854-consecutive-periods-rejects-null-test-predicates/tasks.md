## 1. Failing tests first (spec-tests stage)

- [x] 1.1 Create `tests/test_dev1854_null_test_predicates.py` (dev1846 fixture
      pattern: shared builders from `tests/_dev1846_fixtures`, local
      `exec_engine`/`_q`/`_by_store_month` helpers) with execution tests:
      `hi_rev:sum is not None` by store → A 1,2,3 / B 1,2,0;
      `hi_rev:sum is None` by store → A 0,0,0 / B 0,0,1;
      compound `hi_rev:sum is not None and cost:sum > 0` → A 1,2,3 / B 1,2,0;
      dimension `store is not None` → 1,2,3 both. Verify: compound + both
      top-level aggregate tests FAIL pre-fix (ValueError from the gate is the
      expected pre-fix failure for the compound; assertion setup errors are not
      acceptable).
- [x] 1.2 Add negative anchor in the same file:
      `consecutive_periods((hi_rev:sum is None) + 1)` expects ValueError
      "boolean-shaped predicate cannot appear in a value position". Verify: it
      FAILS pre-fix (no raise happens today).
- [x] 1.3 Add golden cases `cp/is_null_top`
      (`consecutive_periods(hi_rev:sum is None)`, monthly, no store dim) and
      `cp/is_not_null_and` (the compound) to `tests/test_dev1846_golden_sql.py`.
      Verify: golden run reports the two new ids as missing/raising pre-fix.
- [x] 1.4 Add the new test file to git (`git add` the specific new file).
      Note (verified pre-fix): `test_top_level_is_null_by_store` passes on
      SQLite/DuckDB (the truthiness wrapper is coincidentally correct there);
      its pre-fix failure lives in the golden case `cp/is_null_top`. All other
      new tests fail pre-fix for the planned reasons. Codex test review: no
      findings.

## 2. Production fix (spec-implement stage)

- [x] 2.1 Add `"is"`, `"is not"` to `_PREDICATE_COMPARISON_OPS` in
      `slayer/sql/generator.py`; extend the `_assert_cp_shape` bool-branch
      error message and `_is_boolean_shaped` docstring with the null-test
      shape. Verify: tests 1.1/1.2 pass.
- [x] 2.2 Drop the `COALESCE(<predicate>, FALSE)` wrapper in
      `_emit_consecutive_periods_ctes_for_planned` (`pred_in_case` collapses to
      `predicate`). Verify: full unit suite passes; no `COALESCE(` around cp
      predicates in regenerated golden SQL.
- [x] 2.3 Re-bless `tests/golden/dev1846_sql_baseline.json` via the harness
      loop (new `cp/is_*` entries + existing cp entries losing the COALESCE
      wrapper; `ALLOWED_DELTAS` emptied again afterwards). Verify: golden test
      green with no pending deltas. Also re-blessed
      `tests/golden/dev1750_sql_baseline.json` — its two `cp/*` boolean-predicate
      cases carried the same wrapper.
- [x] 2.4 SQL Server xfail: no local SQL Server run possible (libodbc missing),
      so per James's call CI adjudicates: the strict-xfail marker is REMOVED
      (the bare-boolean-projection path it described is gone post-DEV-1846, and
      re-blessed T-SQL golden shows predicates only in CASE WHEN conditions, so
      keeping it would XPASS-fail); the integration-sqlserver workflow verifies
      on the PR, restore the marker only if it fails there.
- [x] 2.5 Update `docs/concepts/formulas.md` accepted-shapes sentence (add null
      tests and `BETWEEN`). Verify: grep shows the sentence lists comparison,
      null test, BETWEEN, IN, connective.
- [x] 2.6 Run the full non-integration suite
      (`poetry run pytest -m "not integration"`) and
      `poetry run ruff check slayer/ tests/`; fix anything red. Verify: both
      green (15191 passed, ruff clean).
