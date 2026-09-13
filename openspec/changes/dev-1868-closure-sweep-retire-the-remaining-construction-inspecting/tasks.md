# Tasks — DEV-1868 closure sweep

## 1. Characterization (before any lift)

- [ ] 1.1 Guard-bypassed characterization tests for W1 (cross-model first/last ×
  `partition_by`): monkeypatch/skip the checker raise, run the shape on SQLite, record
  per-stage outcome (D1); verified by the test file documenting pass or the exact failing
  stage.
- [ ] 1.2 Characterize W2 and W3 the same way (transform-nested cross-model partitioned;
  scalar-call composite with cross-model operand incl. remote+local, remote+literal,
  multi-remote); verified by recorded outcomes driving tasks 3.x.

## 2. Failing test suite (spec-tests stage)

- [ ] 2.1 New executed-value tests, SQLite + DuckDB, hand-computed oracles on the
  orders→customers→regions fixture stack: `tests/test_dev1868_ranked_cm_exec.py` (W1:
  measure/filter/order/dimension positions), `tests/test_dev1868_transform_cm_exec.py`
  (W2 incl. `change(customers.spend:last(partition_by=region))`),
  `tests/test_dev1868_composite_cm_exec.py` (W3 scenarios from the cross-model-aggregates
  delta); verified: all fail before implementation, pass after.
- [ ] 2.2 Series-shift executed tests `tests/test_dev1868_series_shift_exec.py`:
  nested-transform input, cross-model composite leaf, aggregate-typed predicate input,
  NULL-outside-series edge (incl. date_range boundary vs re-aggregation regime),
  filters-applied-once assertion, `change`-over-predicate rejection; verified failing
  then passing.
- [ ] 2.3 Plan-structure tests (F5/F6): producer count / substitution ownership / attach
  phase / complete join grain for W2 (incl. shared inner between two consumers); planner
  assertions that every composite shape becomes a combined slot and never reaches
  `_composite_agg_builder`; verified failing then passing.
- [ ] 2.4 Residue negative tests (F7): transform-without-aggregate, ungrained-aggregate,
  mixed grained/ungrained under one dimension transform — pinning the two new `ValueError`
  messages; update `tests/test_dev1740_regroup_guards.py:66` and re-bless
  `tests/golden/dev1740_regroup_baseline.json` message entries (D10 class d, enumerated);
  verified by pytest.
- [ ] 2.5 Law-pool extension (D8): add cross-model partitioned operands to
  `OPERANDS`/`MEASURE_POOL` + `OPERAND_GRAIN` in `tests/_law_harness.py`; verified by law
  suites collecting the new shapes (initially failing on the guards).
- [ ] 2.6 Compile-level dialect coverage (F8): generated-SQL tests for the new shapes
  across Tier-1 dialects; golden tripwire asserting existing legal `time_shift` forms stay
  byte-identical (F9); verified by pytest.

## 3. Implementation

- [ ] 3.1 W1: remove `check_partitioned_measures` raise 1; fix only the
  characterization-documented failing stage (if any); verified by 2.1 green.
- [ ] 3.2 W2: remove raise 2; wire the inner cross-model partitioned aggregate's desugar
  to `_synthesize_cross_model_producer` so the transform consumes the placeholder (D2);
  verified by 2.1/2.3 green.
- [ ] 3.3 W3: desugar cross-model composite operands to placeholders before phase
  classification (D3); replace `_composite_agg_builder`'s raise with an internal
  invariant; fix the stale `_render_with_cross_model_plans` comment (`generator.py:2645`);
  verified by 2.1/2.3 green.
- [ ] 3.4 Series-shift emission mode (D4): materialised-series branch in
  `_emit_time_shift_ctes_for_planned` (alias-based projection, no join discovery, no
  re-WHERE); accept nested-transform / cross-model-placeholder / aggregate-typed-predicate
  inputs in the classifier; delete the belt RuntimeError (`generator.py:5315`); verified
  by 2.2 green.
- [ ] 3.5 Residue split + P9 relocation (D6, D7): two `ValueError`s in
  `check_computed_dimension`; new `check_time_shift_input` checker hosting the row-leaf
  arm; generator validator reduced to internal invariants; verified by 2.4 green and the
  raise-parity scan.
- [ ] 3.6 Mechanics (W8): remove 4 `DEFERRAL_SITES` entries; `guards.baseline` 5 → 1;
  delete `check_partitioned_measures` + call site + dead helpers; update
  `tests/_dev1871_raise_ledger.py` rows; verified by
  `tests/test_law_guard_ratchet.py` + `tests/test_dev1871_raise_parity.py` green.
- [ ] 3.7 Full unit suite + lint green (`poetry run pytest -m "not integration"`,
  `poetry run ruff check slayer/ tests/`, basedpyright no new errors); goldens re-blessed
  per D10 with per-class enumeration in the PR description.

## 4. Docs, architecture, Linear

- [ ] 4.1 Docs: one-sentence updates in `docs/concepts/formulas.md` / `queries.md` where
  the lifted restrictions are stated; verified by grep for stale restriction wording.
- [ ] 4.2 arc42 (user-approval-gated, exact diff presented first): axiom 9 tag
  `[target: DEV-1868]` → `[enforced: test:…]` + residue-sentence extension in
  `architecture/semantics.arc42.md`; verified by `tools/arch_check.py` green after OK.
- [ ] 4.3 Linear: amend DEV-1873 residue list (transform-in-dimension split rule); comment
  on DEV-1859 (row-leaf arm relocated to the checker); verified by posted
  comments/edits.
