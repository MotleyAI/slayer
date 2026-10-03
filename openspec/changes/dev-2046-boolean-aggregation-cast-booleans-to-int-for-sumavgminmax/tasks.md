## 1. Tests (pr-tests stage — every test must fail on the pre-change code)

- [ ] 1.1 Shared fixture `tests/_dev2046_fixtures.py`: a model with a BOOLEAN `flag` (rows `true, true, false, NULL, false` across two groups), INT `amount` (`10, 20, 30, NULL, …`), TEXT `status`, a derived BOOLEAN column, a joined parent model, seeded for SQLite and DuckDB; verify the fixture imports and seeds both engines
- [ ] 1.2 `boolean_valued` unit tests over every key kind, incl. negatives (`coalesce(flag, amount)`, `greatest(flag, amount)`) and `nullif(flag, false)`; verify they fail on the pre-change code (function absent)
- [ ] 1.3 Classifier / type / format unit tests: `aggregated_type`, `stage_measure_type`, `measure_key_type` (column and expression sources), `_infer_aggregated_format` (boolean sum INT/INTEGER, avg DOUBLE/PERCENT, explicit column format wins, min/max BOOLEAN)
- [ ] 1.4 Facade agreement test: every metric `data_type` for BOOLEAN / INT / DOUBLE / TEXT / TIMESTAMP columns × every aggregation (custom included) equals the engine's type
- [ ] 1.5 Emitted-SQL tests on every Tier-1 and Tier-2 dialect for sum/avg/min/max over a boolean column, a derived boolean column, `coalesce(flag, false)` and `amount > 15`: integer input inside the aggregate, min/max cast back, no `CAST(SUM(...) AS BOOLEAN)`; count family / first / last / custom over a boolean unchanged
- [ ] 1.6 Execution tests on SQLite + DuckDB (hand-computed): sum 2, avg 0.5, min/max, count 4 / count_distinct 2; predicate sources `sum/avg/count(amount > 15)` = 2 / 0.6667 / 3; HAVING `flag:max = true` and `flag:sum > 1`; `sum(coalesce(flag, false))`
- [ ] 1.7 Execution tests for composition: cross-model `sum(orders.flag)`, two-stage re-aggregation (sum and max), `cumsum(flag:sum)`, `sum(flag, window='90d')`, `sum(flag, partition_by=region)`, association-mode pick over a boolean
- [ ] 1.8 IN / BETWEEN sources (DEV-1970): `sum(status in (…))`, `sum(amount between 10 and 20)`, `sum(count(amount) in (1, 2))`, `sum(count(amount) between 1 and 2)`, `count_distinct` over a row-level IN — executed on SQLite + DuckDB, never a pydantic `ValidationError`
- [ ] 1.9 Binding tests: `sum(coalesce(flag, false))`, `sum(amount > 15)` accepted; `median(flag)`, `median(amount > 15)`, `stddev_samp(coalesce(flag, false))` rejected with a typed error; `avg` on a BOOLEAN column validates without `allowed_aggregations`
- [ ] 1.10 Flip `tests/test_dev1847_gate.py:51-52` (row-level `sum(amount > 45)`) from rejection to acceptance (user-approved)
- [ ] 1.11 SQL Server emission tests: projected `sum(amount) > 50` and aggregate inputs `sum(amount > 15)` / `count(amount > 15)` render the BIT value form; WHERE / HAVING / JOIN / CASE conditions stay bare; Postgres projection stays bare
- [ ] 1.12 Postgres execution tests (`tests/integration`, pytest-postgresql, marked integration): sum/avg/min/max over a boolean column and over `amount > 15`, HAVING on both, association pick
- [ ] 1.13 SQL Server execution tests in the `integration-sqlserver` suite for the 1.11 shapes (CI only — no local ODBC driver)

## 2. Boolean-type authority and typing

- [ ] 2.1 Add `boolean_valued` to `slayer/core/keys.py`; delete `_expression_is_confidently_boolean` and route the binding gate through it; verify 1.2 and 1.9 pass
- [ ] 2.2 `classify_aggregation` takes `source_type`; thread it through `aggregated_type`, `stage_measure_type`, `measure_key_type`, `_infer_aggregated_format` (PERCENT default for boolean avg); verify 1.3 passes
- [ ] 2.3 Delete the facade's `_agg_output_type`; metric `data_type` reads the engine typing; verify 1.4 passes
- [ ] 2.4 `avg` joins `DEFAULT_AGGREGATIONS_BY_TYPE[BOOLEAN]`; `_reject_non_numeric_expression_agg` exempts the boolean default set; verify 1.9 passes

## 3. Grammar and sources

- [ ] 3.1 `_AGG_SOURCE_KINDS` gains `Cmp` and `BoolOp`; verify 1.10 passes
- [ ] 3.2 `AggregateKey` source union admits `InKey` / `BetweenKey` through bind → plan → render on row-level and re-aggregation paths; unsupported shapes raise a typed SLayer error; verify 1.8 passes and the `binding.py` basedpyright baseline entry is gone (baseline only shrinks)

## 4. Emission

- [ ] 4.1 Aggregate-application helper in `slayer/sql/render/aggregates.py` (integer lowering for sum/avg/min/max over BOOLEAN; min/max cast back via `declared_cast_type`); verify its unit tests
- [ ] 4.2 Route `generator._build_agg`, `value_expr._render_builtin_aggregate` (and the HAVING seam) and the association producer's level-1 pick through the helper, input type from `boolean_valued`; verify 1.5–1.7 and 1.12 pass
- [ ] 4.3 SQL Server predicate-value rewrite in `slayer/sql/dialects/tsql.py` over the assembled statement; verify 1.11 passes

## 5. Docs and gates

- [ ] 5.1 `docs/concepts/models.md`: per-type table adds `avg` for `boolean`; one sentence on how booleans are aggregated (sum counts trues, avg is the share, min/max stay boolean)
- [ ] 5.2 `docs/concepts/formulas.md` (aggregated-expression grammar): one sentence that comparisons, `in` and `between` can be aggregated (`sum(amount > 15)`)
- [ ] 5.3 Full unit suite (`poetry run pytest -m "not integration"`), integration suite with the CI invocation, `ruff check`, `basedpyright` (no new errors), `la-arch-check` — all green
- [ ] 5.4 Comment on DEV-1970 that it is delivered by DEV-2046's PR
