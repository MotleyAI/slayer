# Tasks

## 1. Failing test suite (spec-tests stage)

Tests live in `tests/test_dev1879_formula_measure_naming.py` and
`tests/dialects/test_dev1879_alias_battery.py`; Codex-reviewed against the
plan (no blockers; its three should-fixes applied).

- [x] 1.1 Unit tests for derived result keys of an unnamed arithmetic measure
      (`logo_churn:sum / logo_bop:sum` → `mart.logo_churn_sum_logo_bop_sum`)
      and an unnamed transform measure (`time_shift(...)` →
      `..._time_shift_cmrr_eop_sum_1_year`); verify they FAIL before the fix.
      (Both FAIL pre-fix on the malformed keys.)
- [x] 1.2 Formatting-insensitivity test (spaced vs unspaced formula → same key)
      and long-formula hash-fold test; verify against
      `auto_name_from_expression`'s documented fold shape.
- [x] 1.3 Unchanged-path pin tests: explicit `name`, saved measure, plain
      `col:agg` / `*:count` keys byte-identical; verify they PASS before the
      fix (regression pins). (Also pins expression-source, cross-model, and
      parametric aggregates, asserting emitted `AS "<key>"` aliases too.)
- [x] 1.4 Collision test: two different unnamed formulas deriving one key →
      error naming both formulas and the `set 'name'` remedy; identical
      formulas merge without error. (Pre-fix: `/` vs `*` keys differ so the
      guard never fires; identical/spacing-variant duplicates ERROR today via
      the downstream stage-column flatten collision, `stage_planner.py:4042`
      — the merge scenario needs the implementation to dedup them.)
- [x] 1.5 Raw-formula referencing test: `order` (and a filter) referencing the
      unnamed composite by its formula text resolves; verify it fails or
      passes pre-fix and note which. (Resolution WORKS pre-fix — ORDER BY /
      HAVING are generated; only the derived-key assertions fail.)
- [x] 1.6 BigQuery SQL-generation test: every projection alias for unnamed
      formula measures in emitted BigQuery SQL is a valid identifier after
      `rewrite_emitted_sql`; verify it FAILS before the fix. (FAILS pre-fix;
      on BigQuery the unmangled junk alias even trips `ScopeLeakError`.)
- [x] 1.7 Fixed battery: unnamed-measure shapes (arithmetic, transform,
      transform-of-composite, mixed literal, cross-model composite,
      parametric agg) × every Tier-1 + Tier-2 dialect — assert (a)
      dialect-valid derived aliases in emitted SQL, (b) identical result keys
      across dialects. If it exposes a pre-existing alias bug outside this
      change's shapes, STOP and surface it before widening scope.
      (No pre-existing alias bug surfaced; the only carve-out is percentile's
      loud NotImplementedError on mysql/tsql — capability gap, cells skipped.)
- [x] 1.8 List any existing tests asserting old derived keys (e.g.
      `tests/test_slack_normalization.py:174`) and get explicit consent
      before modifying them. (The result-key sweep found none; at implement
      time two tests in `tests/test_cross_model_rename_dev1448.py` turned out
      to pin old keys via emitted SQL aliases — updated to the sanitized keys
      with explicit consent, intent assertions unchanged.)

## 2. Implementation (spec-implement stage)

- [x] 2.1 Rewrite the `_canonical_alias_for_formula` fallback
      (`slayer/engine/stage_planner.py:3907-3937`): tighten the plain
      `col:agg` branch to an `AGG_REF_RE` fullmatch and route everything else
      through `auto_name_from_expression(canonical_measure_text(parsed))`;
      verify tasks 1.1-1.7 pass. (Also dedups identical unnamed formulas in
      the declared-measures loop so respellings merge per task 1.4.)
- [x] 2.2 Audit other callers of `_canonical_alias_for_formula` for
      `bound=None`/`parsed=None` paths; verify no caller still reaches the
      old text heuristics with a composite. (Only production caller is
      `stage_planner.py:3807`, always passing both; the dev-1744 test callers
      pass AggregateKey-rooted `bound` and never hit the text fallback.)
- [x] 2.3 Update `docs/concepts/formulas.md` auto-naming sentence (one concise
      sentence + example); grep `.claude/skills/slayer-*.md` for stale naming
      claims; verify docs build/nav untouched (page already linked). (Skills
      carry no stale derived-key claims.)
- [x] 2.4 Run the full non-integration suite
      (`poetry run pytest -m "not integration"`) and the lint/arch bundle
      (`ruff`, `lint-imports`, `arch_check`, `basedpyright`); verify green.
      (16076 passed; basedpyright required typed constructors in the new
      tests and repairs to 7 errors PR #377 had left in
      `tests/test_slack_normalization.py`; its baseline shrank by 11.)
