## 1. Tests (pr-tests)

- [ ] 1.1 Add the refusal scenarios of `specs/queries/semantics/spec.md` to `tests/test_dev2212_raw_rows.py` on SQLite and DuckDB (list child: DuckDB only, via `BASKETS_STATEMENTS`): fanning plain dimension in all four `to_many_handling` modes, computed dimensions, time dimension, list child, unknown-cardinality hop, derived field via closure, stage population across an inherited edge — each asserting `DistinctDimensionValuesError` and that the message names the entry and the hop; verify they fail on the current code.
- [ ] 1.2 Add the controls: determined dimensions (4 rows from `orders`), whole list column (2 rows), local derived field (4 rows), stage population own column (4 rows), grouped mode unchanged (5 rows); verify they pass on the current code.
- [ ] 1.3 Add the precedence tests: a time dimension expression reading `orders.ordered_at` and `returns.amount` from `customers` raises `UnsafeRelationshipInputError`; inferred counterparts (computed, time dimension, list child) raise `PopulationInferenceError`; an unanalysable population-derived field raises `UnanalyzableDependencyError` (find a definition `key_closure` returns `None` for).
- [ ] 1.4 Calibration from the plan probe (main @ 2081bb8ac): the fanning plain, computed, mixed, time-dimension, list-child, stage and unproven-named cases all return rows today (5 / 5 / 5 / 5 / 3 / 5 / 4); a fanning filter already returns 3 rows. Dimension entries take `field` + `alias`; query lists go to `engine.execute([...])`.

## 2. Implementation (pr-implement)

- [ ] 2.1 Generalise the closure-crossing helper in `slayer/engine/population/home.py` to walk from a given root (the population) as well as from the expression's anchor; verify the existing time-dimension home tests still pass.
- [ ] 2.2 Add the raw-row determination rule to `slayer/engine/elaborate_env.py` (raises `DistinctDimensionValuesError` naming entry + hop; unanalysable → `check_input_dependencies_analyzable`), and call it from `slayer/engine/bind_inputs.py` for every dimension and time dimension when `distinct_dimension_values` is false, after `check_time_dimension_home`; verify tasks 1.1–1.3 pass.
- [ ] 2.3 Add the new raise site's row to `tests/_dev1871_raise_ledger.py`; verify `tests/test_dev1871_raise_parity.py` passes.
- [ ] 2.4 Widen the `DistinctDimensionValuesError` docstring in `slayer/core/errors.py` (concise).

## 3. Docs

- [ ] 3.1 `docs/concepts/queries.md` › Raw rows › Rules: a bullet that every dimension and time dimension must be determined by the population over provably to-one hops (no fanning, unknown-cardinality or list-child hop), else `DistinctDimensionValuesError`; verify `tests/test_doc_examples.py` passes.
- [ ] 3.2 `slayer/memories/help_content/07_queries.md`: one sentence on the same rule.
- [ ] 3.3 `docs/upgrading/2.0.md` Part 1: a breaking-change entry with the repro, the 1.x vs 2.0 outcome and the remedies (name the finer dataset as `population`, declare the hop's cardinality, or drop the flag).

## 4. Gates

- [ ] 4.1 Run the local test scope (`la-affected-tests --base main` plus the raw-row, raise-parity and doc-example tests), `poetry run ruff check slayer/ tests/`, `la-typecheck`, and `poetry run la-arch-check`; all green.
