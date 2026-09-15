## 1. Fixtures and failing tests (spec-tests stage)

- [x] 1.1 Extend `tests/_dev1847_fixtures.py` ADDITIVELY: `quantity` and
      `unit_price` columns on `sales`, the same 15 rows widened in both the
      sqlite and duckdb seeds, values making row-weighted ≠ pure-reagg ≠
      `sum(quantity * unit_price)`; hand-computed dev1859 oracle constants in
      the module; verify by a fixture smoke test and the untouched dev1847
      suites staying green.
- [x] 1.2 Write `tests/test_dev1859_row_mixed_exec.py` (SQLite + DuckDB,
      failing): base oracle by region; ungrained inner; coalesce-NULL pin on a
      surviving row; row-filter inheritance (population + constituents);
      count/count_distinct; cross-model inner constituent; filter/order
      positions; grain-self-contained dimension position; cardinality
      invariant; no-placeholder-leak; `assert_scope_closed`.
- [x] 1.3 Write the outer-modifier tests (failing): outer `partition_by=`,
      outer `window='90d'`, `wavg(..., weight=qty)` row-valued parameter, a
      model-defined custom aggregation, and `corr(mixed, qty)` — executed
      values each.
- [x] 1.4 Write `tests/test_dev1859_plan_structure.py` (failing): mixed root
      not among re-aggregation roots; each attached constituent a row-phase
      attach; outer slot stays an aggregate; no producer GROUP BY contains the
      placeholder or the row leaf; `first(mixed)` keeps the
      not-supported-over-an-expression error.
- [x] 1.5 Write `tests/test_dev1859_transform_row_leaf.py` (failing):
      rejection matrix parametrized over the derived non-shift set (all
      transform ops − shift family; classifier-membership assertion) ×
      {bare leaf, composite, predicate, mixed-with-attached}; projected-grain-key
      legality (`rank(weight)` with `weight` a dimension, executed);
      shift-family bare-leaf regime unchanged (`time_shift(weight,-1)`,
      `change(weight)`); plan-time raise (no SQL generated); ledger pins.
- [x] 1.6 CONSENTED test re-points (approval recorded at spec-plan, apply in
      stage 3): `tests/test_dev1847_gate.py::test_mixed_row_and_attached_rejected`
      and `tests/test_expression_aggregations.py::TestExpressionErrors::
      test_mixed_row_and_attached_source_rejected` flip from rejection to
      acceptance/repoint; verify both suites green after stage 3.
- [x] 1.7 Failing executed test (design decision 8): `rank(<CASE expr>)` with
      the same CASE projected as a computed dimension executes at the query
      grain (2 rows, band 2 -> rank 1) — today a transform-layer RuntimeError.

## 2. Leg A — parse gate, classifier, row-grain branch

- [x] 2.1 Delete the mixed-source branch in `syntax.py:_validated_agg_source`
      (+docstring line), keeping the nested-transform raise; verify by the 1.2
      shapes parsing and `sum(cumsum(x) - 1)` still rejected.
- [x] 2.2 Add the mixed-source classifier beside `operand_aggregates` in
      `slayer/core/keys.py` (row-level leaves of an aggregation source) and
      exclude mixed roots from `_discover_reaggregation_roots`; verify by 1.4
      plan-structure tests.
- [x] 2.3 Branch `_plan_regroups`: mixed roots register inner constituents as
      row-phase attaches and stay inline as expression aggregates over the
      placeholder-rewritten source, `_assert_attach_covers_producer_grain` on
      every attach, inherited filters threaded; verify by 1.2 executed tests.
- [x] 2.4 Scope `check_reaggregation_no_window` /
      `check_reaggregation_no_column_param` to pure roots and wire the mixed
      outer through `partition_by=`/`window=`/parameter producers (row-attaches
      in their sub-plans); verify by 1.3 tests.
- [x] 2.5 Narrow the first/last first-arg dispatch (design decision 7): a
      MIXED composite routes to the aggregation path so the existing
      "not supported over an expression" rule fires; a pure attached arg
      keeps the transform routing; verify by 1.4's `first(mixed)` test and
      the untouched `test_dev1847_gate.py::TestFirstLastDispatchUnchanged`.

## 3. Leg B — the non-shift transform type rule

- [x] 3.1 Generalize the checker walk in `elaborate_env.py` to the derived
      non-shift transform set with consumer-grain context from
      `bind_query_inputs` (reject only leaves that refine the projected grain);
      new typed message with the aggregate-the-leaf remedy, no issue reference;
      verify by 1.5.
- [x] 3.2 Add the ledger row(s) in `tests/_dev1871_raise_ledger.py` for the new
      raise; verify by the raise-parity test.
- [x] 3.3 Expression-equality computed-dimension leaves (design decision 8):
      the checker's projected-grain-key predicate includes projected
      computed-dimension keys; `_transform_layer_deps_ready` checks the whole
      input key against `slot_id_by_key` before descending, and the step
      render reads that slot's alias; verify by 1.7 on both dialects.

## 4. Leg C — attached parameters (GATED on DEV-1892 merged to main)

- [ ] 4.1 Merge origin/main once DEV-1892 lands; re-verify its landed names
      (kwarg binding, `grain_determines`/`check_parameter_determined`, the
      level-1 `_p<i>` pick) against the follow-up Linear comment; re-run
      `openspec validate dev-1859-aggregation-sources-mixing-row-level-columns-with-attached --strict`
      and reconcile the shared expression-aggregation delta if drifted.
- [ ] 4.2 Write failing executed tests on `tests/_dev1840_fixtures.py`:
      associate-mode weighted_avg with the attached weight (populations ok
      {c1,c2,c3,c5,c6}, new {c1,c2,c4}; c4 = NULL-region cell total), the
      ordinary-mode twin, and the undetermined-parameter rejection staying.
- [ ] 4.3 Lift DEV-1892's attached-parameter rejection; row-attach the
      parameter producer into the input relation (association level 1 via
      producer regroups; ordinary kernel via the leg-A path); update the 1892
      ledger row and residue test; verify by 4.2 and
      `docs/concepts/queries.md` associate sentence updated.

## 5. Goldens, harness, docs

- [ ] 5.1 Bless golden SQL for the new shapes; verify existing baselines
      byte-stable (divergences individually approved per the ledger protocol).
- [ ] 5.2 Flip `architecture/semantics.arc42.md:45` `[target: DEV-1859]` →
      `[enforced: test:tests/test_dev1859_row_mixed_exec.py]` — present the
      exact one-line diff for approval first (normative harness); verify by
      `poetry run python tools/arch_check.py`.
- [x] 5.3 Update `docs/concepts/formulas.md` (~:74-76): narrow the rejection
      sentence to nested transforms and add one sentence + example for the
      mixed source; verify by grepping docs for the old "mix" phrasing.
- [ ] 5.4 Run the full non-integration suite, the SQLite/DuckDB integration
      files touched, ruff, and the enforcement bundle; then
      `openspec validate dev-1859-aggregation-sources-mixing-row-level-columns-with-attached --strict`.
