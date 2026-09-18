## 1. Failing suites (spec-tests stage)

Fixture rows: North Jan 10 / Feb 20 / Mar 30, South Jan 5 / Feb 15, West Feb NULL;
`X` = `amount:sum(partition_by=[region, ordered_at])`.

- [ ] 1.1 `tests/test_dev1832_fixtures_smoke.py`: derive every new oracle from the raw rows — `sum(amount * last(X))` (375 / 825 / 900), `sum(amount * min(X, partition_by=region))`, the empty-grain `sum(amount * last(amount:sum(partition_by=ordered_at)))`, `weighted_avg(amount, weight=min(X, partition_by=region))`, the mixed+combined `sum(amount * min(X, partition_by=region)) + amount:sum(partition_by=region)` over `[region]`, and the windowed `sum(rank(amount:sum(window='90d', partition_by=region)))`; verify the smoke test passes on the current tree
- [ ] 1.2 `tests/test_dev1832_transform_source.py`: flip `test_collapse_mixed_with_row_leaf_fails_closed` → executed 375 / 825 / 900 on SQLite + DuckDB; add the hand-written, empty-grain, parameter, mixed+combined, filter-only and ORDER-BY-only cases; plan-structure pins (one re-aggregation producer at its own grain row-attached on its keys — zero join pairs for the empty grain — never attached at the enclosing level, one flat `WITH`, `assert_scope_closed`, no placeholder leak, cardinality invariant); broadcast- and error-mode tests with the fanning-dimension fixture; verify each fails on the current tree (the guard or the grain-cover assertion)
- [ ] 1.3 Same file: flip `test_windowed_inner_fails_closed` → executed hand-derived values, exactly three buckets, every value non-NULL, both engines, plus pins that the nested producer's projected grain and join keys contain the exact bucket key, one flat `WITH`, closed scopes, no leak; verify it fails on the current tree with the windowed time-dimension error
- [ ] 1.4 Same file: move `test_cross_model_grained_inner_fails_closed` into a narrowly named permanent-boundary class whose docstring states the Axiom 7 reason and asserts the error names `ordered_at`; add the to-one positive pin `sum(cumsum(amount:sum(partition_by=[customers.tier, ordered_at])))` rooted at `orders`; verify the boundary test passes and probe the positive pin on the current tree
- [ ] 1.5 `tests/test_dev1832_golden_sql.py`: add named cases `lifted/mixed_reagg`, `lifted/mixed_collapse`, `lifted/windowed_inner` (recorded raises until the implementation lands); verify the baseline diff is their addition only
- [ ] 1.6 `tests/_dev1871_raise_ledger.py`: remove the `check_collapsing_transform_not_row_mixed` row; verify `tests/test_dev1871_raise_parity.py` is red on the current tree (the raise still exists)

## 2. Mixed source + re-aggregation constituent (D1–D3, D6)

- [ ] 2.1 Delete `elaborate_env.check_collapsing_transform_not_row_mixed`, its import and its call in `bind_inputs.py`; verify the collapse test now fails on the grain-cover assertion and `test_dev1871_raise_parity.py` is green
- [ ] 2.2 `compile/stages._plan_regroups`: route a re-aggregation attached input of a row-attach root through `_synthesize_reaggregation_producer` in an attached-constituent context (projection = the constituent's own grain; query-dimension rule skipped, attributability kept); verify the hand-written and collapse executed cases pass on SQLite + DuckDB
- [ ] 2.3 Join pairs from the synthesized producer's projected grain (`ordered_pks` → row slots), zero pairs for an empty grain (scalar cross join, no empty `ON`); verify the empty-grain case and the plan-structure pins pass on both engines
- [ ] 2.4 Parameter, mixed+combined, position-parity and mode cases; verify all of 1.2 green and that `_assert_broadcast_coherence` still fires for the combined term (the mixed+combined pin)

## 3. Windowed inner under a transform constituent (D4)

- [ ] 3.1 Thread the query's active time dimension into the nested transform-root producer via `_regroup_producer_prebound` so the bucket is in its GROUP BY, projection and join pairs; verify 1.3 passes on both engines and every existing windowed and transform pin is unchanged
- [ ] 3.2 If the probe shows the shape is undefined rather than unplumbed: stop and escalate — never a silent boundary

## 4. Boundary, docs, accounting, gates

- [ ] 4.1 Confirm 1.4 green (boundary class and positive pin); verify the error message still names `ordered_at` and the remedy with no deferral wording
- [ ] 4.2 Docs: one sentence in `docs/concepts/formulas.md` — re-aggregation constituents in a mixed source and windowed inners under a transform constituent now execute, while a target-homed inner naming a host time axis stays a typed error; verify by grep that no deferred / follow-up wording for these shapes remains
- [ ] 4.3 Record the three named golden cases as SQL through `ALLOWED_DELTAS`; verify every other golden is byte-identical
- [ ] 4.4 Confirm no arc42 or `index.yaml` edit is needed (`guards.baseline` unchanged); run `poetry run python tools/arch_check.py`; verify green
- [ ] 4.5 `poetry run pytest -m "not integration" -n auto`, `ruff check slayer/ tests/`, conventions gate, `basedpyright` (baseline holds or shrinks); verify all green, then the Codex working-tree pass before push
