## 1. Tests first (pr-tests)

- [ ] 1.1 Boolean parameters: write execution tests for every new `aggregations/boolean-inputs` scenario on the idiom fixture (`tests/_mcp_idiom_fixtures.py`). Cover boolean `weight=`, boolean `other=` (`covar_samp`, `corr`), a boolean parameter under FILTER, and the shapes `window=`, `partition_by=`, cross-model from `customers`, and associate. Run on SQLite and DuckDB, and on Postgres through pytest-postgresql (unit suite, no integration marker; see the TCP-only sandbox note in memory). Expected values are computed from the fixture rows. Verify that each test fails on the current code.
- [ ] 1.2 Write per-dialect emission tests showing that `weighted_avg(amount, weight=flag)` and `covar_samp(amount, other=flag)` take the parameter's integer form on every supported dialect, plus a custom-aggregation test (formula `SUM(CASE WHEN {flag} THEN {value} END)`) showing that `{flag}` reaches the template as a boolean and executes on DuckDB and Postgres. Verify that the emission tests fail on the current code.
- [ ] 1.3 FILTER naming: write execution tests for every new `aggregations/functional-form` scenario:
  - result keys for constant and variable conditions;
  - a case/whitespace respelling;
  - a filtered `first` beside a plain `first`, unnamed and named (the named pair must give the correct distinct values);
  - the in-formula differences for `first` and `last`;
  - `status = 'open'` vs `status <> 'open'`;
  - a joined-condition `count(*)` beside `count(*)`;
  - ORDER BY by the formula text.

  Add unit tests showing that `canonical_aggregate_alias` differs for a key with and without `filter` in every profile. Verify that each test fails on the current code.
- [ ] 1.4 Branch check: write execution tests for every `queries/mixed-type-coercion` "Value-returning conditionals share one type" scenario, plus a function matrix. The matrix covers `iif`, searched CASE, `coalesce`, `ifnull`, `greatest` and `least`, each × NULL absorption / INT-DOUBLE widening / DATE-TIMESTAMP widening / BOOLEAN beside a number / TEXT-number rejection. Run on DuckDB and SQLite, and in every position: dimension, measure inside an aggregate, filter, order, a FILTER condition, and a stage. Assert the error class `ScalarOperandTypeError` and that both type names appear in the message. Verify that the rejection tests fail on the current code.
- [ ] 1.5 Inferred types: write tests showing that `greatest(status, amount)` is typed DOUBLE in the response metadata and in a later stage's schema, and that `CASE WHEN amount > 1 THEN amount ELSE 0.5 END` is typed DOUBLE. Add a test showing that an untyped / `UNKNOWN` column branch is not reported as a mismatch. Verify that each fails, or pins current behaviour, as appropriate.

## 2. Boolean parameters (pr-implement)

- [ ] 2.1 Add a required `input_type: Optional[DataType]` to `ResolvedAggKwarg` and fill it at all five construction sites (`_resolve_agg_kwargs_for_key`, both arms of `_agg_param_exprs`, the windowed `_w_p<i>` pick, the association `_p<i>` pick) from `aggregation_source_type`. Verify that basedpyright reports no new errors and that tests 1.1–1.2 still fail only on the lowering.
- [ ] 2.2 Make `_resolve_agg_param` return `aggregate_input(value=…, aggregation=spec.aggregation, input_type=kwarg.input_type)`. Verify that tests 1.1 and 1.2 pass.

## 3. FILTER naming (pr-implement)

- [ ] 3.1 Move the text-path alias derivation (the text branch of `_canonical_alias_for_formula`) into `slayer/sql/naming.py`, and route every unnamed formula whose parsed tree contains a FILTER through it. Formulas with variables keep their template text. Verify the result-key tests in 1.3.
- [ ] 3.2 Include `key.filter` in `canonical_aggregate_alias` in every profile as a `_filter_<expression name>` hint. Make the ranked producer path (and any other path that keys producers or slots by alias) key by structural key identity, with the allocator disambiguating coinciding hints. Verify all of 1.3, including the `status = 'open'` / `status <> 'open'` pair and the named `fo`/`f` values.

## 4. Conditional branch check (pr-implement)

- [ ] 4.1 Make `value_type` / `_scalar_value_type` in `slayer/core/keys.py` return the joined type of `iif`, `coalesce`, `ifnull`, `greatest` and `least` through `join_conditional_branch_types`. This must never raise: on a mismatch it returns `None`, and `DataType.UNKNOWN` counts as untyped. Verify the existing `tests/test_dev1740_conditional_typing.py` and new unit cases.
- [ ] 4.2 Add `check_conditional_branches(roots, column_type)` to `slayer/engine/elaborate_env.py`. It raises `ScalarOperandTypeError` naming the call and both types and suggesting a CAST. Call it after the coercion pass in `bind_inputs.py` over every position's roots, dimensions and time dimensions included, for model and stage scopes. Verify test 1.4.
- [ ] 4.3 Route `measure_key_type`'s conditional arm through `value_type`, and delete `_branch_type` and `_literal_data_type` from `slayer/engine/key_metadata.py`. Verify that the full unit suite passes.
- [ ] 4.4 Derive every inferred (non-explicit) measure and dimension type from the post-coercion key. Verify test 1.5 (response metadata and stage schema agree).

## 5. Docs, principles and gates (pr-implement)

- [ ] 5.1 Add one sentence each to the docs:
  - in `docs/concepts/formulas.md`, the FILTER section (the result key is the formula's text) and the Conditionals section (the branch-type rule covers `coalesce`, `ifnull`, `greatest` and `least`, on every dialect);
  - in the aggregation docs: boolean `weight=` / `other=` read as 0/1, custom aggregations receive raw booleans.

  Verify that `docs/` has no remaining stale wording, e.g. a FILTER key described as "includes the condition".
- [ ] 5.2 The `architecture/system.arc42.md` §3 item 17 "One typing" [target: DEV-2180] has already landed in pr-plan (user, approved wording). Verify that `poetry run la-arch-check` is OK.
- [ ] 5.3 Run the full unit suite (`poetry run pytest -m "not integration"`), `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new errors against the baseline). Verify that all three are green.
