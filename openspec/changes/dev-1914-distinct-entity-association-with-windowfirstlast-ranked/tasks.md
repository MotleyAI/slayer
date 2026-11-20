## 1. Tests (pr-tests stage)

- [ ] 1.1 Create `tests/test_dev1914_ranked_windowed_association.py`: an executed-value suite on SQLite and DuckDB (DEV-1900 fixtures, extended by appending orders after seeding as `_dev1910` does) covering every scenario of the attribution-modes ADDED requirements, "Ranked and windowed association" and "Local first/last and windowed aggregates follow the mode axis", with the oracles in those scenarios. Verify: the module collects and every test fails on the current code for the stated reason (refusal, fallback broadcast, or multiplied value).
- [ ] 1.2 Add the adversarial same-row fixture: a customer with an in-frame non-`ok` order and an out-of-frame `ok` order (design decision 11). Use it in the customers-rooted and orders-rooted fanning-axis window tests with `orders.status = 'ok'`. Verify: the hand-computed oracle differs from the split-`EXISTS` reading.
- [ ] 1.3 Add the unequal-fan-out and NULL-ranking-key fixture for ranked association: the highest-key entity has one related row and others several; some keys are NULL; run against a keyless home too. Verify: expected picks are hand-computed.
- [ ] 1.4 Add the empty-frame tests: associated windowed `count(*)`, `count(<field>)`, `sum`, a custom aggregation and `collect` at the NULL bucket, at a missing month, and at a bucket whose source rows a filter removed (design decision 10). Verify: 0 / 0 / NULL / NULL / empty list.
- [ ] 1.5 Add the precedence tests: a fanning-axis window under `broadcast` and `error`, local and cross-model, pins `TimeAxisError`, its location, and a suggestion naming association. Also pin the implicit fanning ranking key `last(spend, window='1y')` as the input-safety error in every mode. Verify: tests fail today only where the suggestion lacks the association remedy.
- [ ] 1.6 Add the list-child parameter tests in the DEV-2137 list fixture: `weighted_avg(contractors.rate, weight=contractors.rate)` by `contractors.skills.value` gives go 43.33, sql 16.67, by default and under `associate`, keyed and keyless. Add the parameter-shape inventory (design decision 14) against keyed and keyless homes in every mode. Verify: each case either executes or raises its established typed error.
- [ ] 1.7 Add the parity tests: every position (measure, measure filter, order key, explicit `partition_by=`) reads the same associated ranked and windowed value; cardinality neutrality; dice–slice for the ranked and the windowed pair (semantics delta). Verify: executed on SQLite and DuckDB.
- [ ] 1.8 Add the dialect emission tests: a ranked and a windowed association query render on every Tier-1 dialect, and execute on the locally runnable engines (Postgres via `pytest-postgresql` included).
- [ ] 1.9 Align the existing tests that contradict the plan, each with the user's per-file approval:
  - `tests/test_dev1909_population_pushdown.py::test_windowed_fanning_axis_fails_closed` → associate/default execute; broadcast/error keep the error.
  - `tests/test_dev1841_association_errors.py::test_window_with_association_fails`.
  - `tests/test_dev1836_ranked_windowed.py::test_windowed_requires_attributable_td`.
  - The fallback tests in `tests/test_dev2216_associate_default.py` (first/last and window fallback, each reason reported, requested grain, ranked inner), `tests/test_dev2216_mode_surface.py` and `tests/test_dev1836_warning_collector.py`.
  - `tests/test_dev2216_associate_default.py::test_local_ranked_aggregate_never_raises`.
  - The `check_association_windowed_ranked` row in `tests/_dev1871_raise_ledger.py`.

  Verify: each aligned test fails on the current code.

## 2. Mode gate and routing (engine)

- [ ] 2.1 In `slayer/engine/compile/stages.py`, replace the exclusive `_kernel_decision` with reducer × associate (design decision 5). Route local windowed and ranked roots with an unattributable grain member to the home-rooted synthesis, leaving the local host regroup only attributable grains. Verify: the local-mode-axis tests from 1.1 pass, and the local windowed double-count is gone.
- [ ] 2.2 Resolve the active window axis before the grain-mode decision (design decision 9): association arm under default/`associate`; `TimeAxisError` with the association remedy under `broadcast`/`error`, in `slayer/engine/elaborate_env.py::check_windowed_time_axis_attributable` and both call sites. Verify: the 1.5 tests pass.
- [ ] 2.3 Delete `check_association_windowed_ranked` and its call site. Verify: the raise-ledger test passes with its row removed.

## 3. Kernels (IR + SQL)

- [ ] 3.1 Ranked association: in `slayer/ir/planned.py` and `slayer/sql/generator.py`, add a level-1 entity dedup that picks one value and one ranking key per entity, drops NULL keys, then ranks and picks rank 1 (design decision 6). Verify: the 1.1 ranked tests and the 1.3 fan-out tests pass.
- [ ] 3.2 Windowed association: the trailing-window kernel gains entity keys (`list_rows` and row identity reused) and the IR contract of design decision 8 (endpoints always set, frame-axis key in home coordinates). `_render_window_metric_cte_from_planned` gains the (bucket, cell, entity) dedup level, keeping the empty-frame NULL row and carrying the ranking key for windowed `first`/`last` (design decision 7). Verify: the 1.1 windowed tests, 1.2, 1.4 and 1.8 pass.
- [ ] 3.3 List-child parameters: `_AssociationEntity.determines_param` judges a `list_rows` association against the root row (design decision 14). Verify: the 1.6 tests pass.

## 4. Fallback removal

- [ ] 4.1 Remove `ModePolicy.fallback`, `_association_unsupported`, `RegroupAttachPlan.association_unsupported`, the `association_unsupported` field of `BroadcastGrainWarning` / its payload, `association_fallback_hint`, and the fallback part of the warning merge key in `slayer/engine/query_engine.py`. Verify: the aligned DEV-2216 tests pass, and the stale names are gone from `slayer/` (checked via LSP references).

## 5. Docs and harnesses

- [ ] 5.1 Apply the approved arc42 edits to `architecture/semantics.arc42.md` verbatim (design decisions 3 and 4). Verify: `poetry run la-arch-check` passes.
- [ ] 5.2 Update `docs/concepts/queries.md`: the mode-table row, the warnings-table `broadcast` entry (drop `association_unsupported`), and the cross-dataset section's "cannot associate yet" paragraph, which gains the ranked and windowed association readings. Update `docs/upgrading/2.0.md` (drop the fallback sentence and state the local first/last / window mode-axis change). Verify: grep for `association_unsupported` and "cannot associate" in `docs/` returns nothing.
- [ ] 5.3 Run the local test scope (`la-affected-tests --base main` plus the goldens of changed kernels), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs the baseline) and the enforcement bundle (`la-arch-check`, LikeC4 validate). Verify: all green; each golden divergence is reviewed individually.
