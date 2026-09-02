# Tasks — DEV-1846

## 1. Tests first (TDD — land failing for the right reason)

- [ ] 1.1 Create `tests/test_dev1846_composite_transforms.py` with a SQLite+DuckDB
      param fixture and hand-computed dataset; verify the file collects and the
      fixture executes a trivial query on both engines
- [ ] 1.2 time_shift executed-value tests: ratio shift by month+dim; change/change_pct
      over `revenue:sum / *:count` with per-store partition reset; scalar-wrap
      missing-bucket → NULL; two differently-parameterized leaves (fragment kwarg +
      column filter); crossing-fragment-kwarg leaf join registration — each initially
      failing with the current fail-closed error
- [ ] 1.3 consecutive_periods executed-value tests: numeric delta truthiness;
      `change(x) > 0` growth streak; bare `cumsum(x)`; `round(a:sum) >= 10`;
      top-level BETWEEN / IN / negated IN / and / or / not incl. NULL groups;
      nested IN under a boolean connective — each initially failing
- [ ] 1.4 Typing-contract tests: `iif(pred, 1, 0)` accepted;
      `(a:sum>0)+(b:sum>0)` rejected; `coalesce(a:sum>0, 0)` rejected;
      `lower(name:max)` rejected — ValueError messages name shape + remedy
- [ ] 1.5 Fail-closed matrix: ts nested transform / mixed composite / pure-row
      composite / cross-model leaf — each asserted IDENTICAL on the plain path and
      with a cross-model sibling (uniform-gate scenario)
- [ ] 1.6 Planner unit tests: `_iter_slot_deps` recurses into BetweenKey/InKey
      (top-level and nested under ScalarCallKey/ArithmeticKey), asserting column
      leaves surface as slot deps
- [ ] 1.7 Golden test `tests/test_dev1846_golden_sql.py` + baseline
      `tests/golden/dev1846_sql_baseline.json` over postgres/sqlite/duckdb/tsql/
      bigquery for the lifted shapes (recorded once implementation lands; test
      committed first, baseline blessed in 3.4)
- [ ] 1.8 Verify the whole new suite fails ONLY with the current fail-closed errors
      (no setup failures): `poetry run pytest tests/test_dev1846_composite_transforms.py -x`

## 2. Implementation

- [ ] 2.1 Fix `_iter_slot_deps` BetweenKey/InKey recursion; verify 1.6 passes
- [ ] 2.2 Hoist + rename the generator gate to `_validate_transform_input_shapes`
      (above kernel-body/combined-attaches returns); implement the new acceptance
      rules (ts aggregate-only composites; cp full tree + typing contract incl.
      string-family rejection); delete the dead `deferred` walk and its two raises;
      verify 1.4/1.5 pass
- [ ] 2.3 time_shift emitter: extract the shifted leaf-agg builder (per-leaf
      DEV-1750 registration + synth + `_build_agg` + leaf cast), render input via
      `render_value_key` + `CompositeFacilities`, allocate internal alias for
      composites; verify 1.2 passes and existing time_shift suites stay green
- [ ] 2.4 consecutive_periods emitter: one alias-context render + boolean/value
      wrap by contract; verify 1.3 passes and existing cp suites stay green
- [ ] 2.5 Guard hygiene: partition arm → descriptive RuntimeError; delete dead
      explicit-partition_keys loop + stale C6 comments (generator + binding.py:1099);
      reword window-dispatch fallthrough without stage markers; verify
      `grep "7b\.11" slayer/sql/generator.py` is empty
- [ ] 2.6 Flip pinned tests (approved): `test_dev1750_guard_lift.py` composite raise
      → renders+executes; narrow `test_dev1838_sweep.py` ALLOWED_EXPRESSIVENESS;
      verify both files pass

## 3. Verification and blessing

- [ ] 3.1 Full non-integration suite green:
      `poetry run pytest -m "not integration"`
- [ ] 3.2 Byte-identity audit for single-leaf time_shift SQL (N=1 collapse):
      existing golden baselines unchanged, or divergences enumerated as class (b)
      in divergences.md with values proven unchanged
- [ ] 3.3 Write `divergences.md` in this change folder: errors→values table per
      lifted shape (measured on SQLite), class-(b) list, surviving-error inventory
- [ ] 3.4 Bless goldens: re-bless dev1750 error entries to SQL; record
      dev1846 baseline (SLAYER_UPDATE_GOLDEN=1); verify golden suites pass clean
- [ ] 3.5 Lint clean: `poetry run ruff check slayer/ tests/`

## 4. Docs

- [ ] 4.1 Update `docs/concepts/formulas.md` (nesting section, consecutive_periods
      section, transform table notes) and grep `.claude/skills/slayer-*.md` +
      `docs/` for stale unsupported-shape claims; verify no doc still calls the
      lifted shapes unsupported
- [ ] 4.2 Confirm no new docs page (nav untouched) and no Linear-issue references
      added outside the change folder
