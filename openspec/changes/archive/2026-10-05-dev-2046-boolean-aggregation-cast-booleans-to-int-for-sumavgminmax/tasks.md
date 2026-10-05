## 1. Tests (pr-tests stage — every test must fail on the pre-change code)

- [x] 1.1 Shared fixture `tests/_dev2046_fixtures.py`: a model with a BOOLEAN `flag` (rows `true, true, false, NULL, false` across two groups), INT `amount` (`10, 20, 30, NULL, …`), TEXT `status`, a derived BOOLEAN column, a joined parent model, seeded for SQLite and DuckDB; verify the fixture imports and seeds both engines
- [x] 1.2 `boolean_valued` unit tests over every key kind, incl. negatives (`coalesce(flag, amount)`, `greatest(flag, amount)`) and `nullif(flag, false)`; verify they fail on the pre-change code (function absent)
- [x] 1.3 Classifier / type / format unit tests: `aggregated_type`, `stage_measure_type`, `measure_key_type` (column and expression sources), `_infer_aggregated_format` (boolean sum INT/INTEGER, avg DOUBLE/PERCENT, explicit column format wins, min/max BOOLEAN)
- [x] 1.4 Facade agreement test: every metric `data_type` for BOOLEAN / INT / DOUBLE / TEXT / TIMESTAMP columns × every aggregation (custom included) equals the engine's type
- [x] 1.5 Emitted-SQL tests on every Tier-1 and Tier-2 dialect for sum/avg/min/max over a boolean column, a derived boolean column, `coalesce(flag, false)` and `amount > 15`: integer input inside the aggregate, min/max cast back, no `CAST(SUM(...) AS BOOLEAN)`; count family / first / last / custom over a boolean unchanged
- [x] 1.6 Execution tests on SQLite + DuckDB (hand-computed): sum 2, avg 0.5, min/max, count 4 / count_distinct 2; predicate sources `sum/avg/count(amount > 15)` = 2 / 0.6667 / 3; HAVING `max(flag) = true` and `sum(flag) > 1`; `sum(coalesce(flag, false))`
- [x] 1.7 Execution tests for composition: cross-model `sum(orders.flag)`, two-stage re-aggregation (sum and max), `cumsum(sum(flag))`, `sum(flag, window='90d')`, `sum(flag, partition_by=region)`, association-mode pick over a boolean
- [x] 1.8 IN / time-point sources (DEV-1970): `sum(status in (…))`, `sum(status not in (…))`, `sum(ordered_at >= '2025-02')`, `sum(count(amount) in (1, 2))`, `sum(max(ordered_at) >= '2025-02')`, `count_distinct` over a row-level IN — executed on SQLite + DuckDB with the boolean result types, never a pydantic `ValidationError`
- [x] 1.9 Binding tests: `sum(coalesce(flag, false))`, `sum(amount > 15)` accepted; `median(flag)`, `median(amount > 15)`, `stddev_samp(coalesce(flag, false))` rejected with a typed error; `avg` on a BOOLEAN column validates without `allowed_aggregations`
- [x] 1.10 Flip `tests/test_dev1847_gate.py:51-52` (row-level `sum(amount > 45)`) from rejection to acceptance (user-approved)
- [x] 1.11 SQL Server emission tests: projected `sum(amount) > 50` and aggregate inputs `sum(amount > 15)` / `count(amount > 15)` render the BIT value form; WHERE / HAVING / JOIN / CASE conditions stay bare; Postgres projection stays bare
- [x] 1.12 Postgres execution tests (`tests/integration`, pytest-postgresql, marked integration): sum/avg/min/max over a boolean column and over `amount > 15`, HAVING on both, association pick
- [x] 1.13 SQL Server execution tests in the `integration-sqlserver` suite for the 1.11 shapes (CI only — no local ODBC driver)
- [x] 1.14 Re-bless the `consecutive_periods` predicate-shape error strings in `tests/golden/dev1846_sql_baseline.json` without `BETWEEN`

## 2. Boolean-type authority and typing

- [x] 2.1 Add `boolean_valued` to `slayer/core/keys.py`; delete `_expression_is_confidently_boolean` and route the binding gate through it; verify 1.2 and 1.9 pass
- [x] 2.2 `classify_aggregation` takes `source_type`; thread it through `aggregated_type`, `stage_measure_type`, `measure_key_type`, `_infer_aggregated_format` (PERCENT default for boolean avg); verify 1.3 passes
- [x] 2.3 Delete the facade's `_agg_output_type`; metric `data_type` reads the engine typing; verify 1.4 passes
- [x] 2.4 `avg` joins `DEFAULT_AGGREGATIONS_BY_TYPE[BOOLEAN]`; `_reject_non_numeric_expression_agg` exempts the boolean default set; verify 1.9 passes

## 3. Grammar and sources

- [x] 3.1 `_AGG_SOURCE_KINDS` gains `Cmp` and `BoolOp`; verify 1.10 passes
- [x] 3.2 Admit `InKey` (via `EXPRESSION_SOURCE_KINDS` and `_AggregateSource`, through bind → plan → render) and `TimePointCmpKey` (at bind and in `_AggregateSource` only, lowered before compilation) on row-level and re-aggregation paths; unsupported shapes raise a typed SLayer error; verify 1.8 passes and the `binding.py` basedpyright baseline entry is gone (baseline only shrinks)

## 4. Emission

- [x] 4.1 Aggregate-application helper in `slayer/sql/render/aggregates.py` (integer lowering for sum/avg/min/max over BOOLEAN; min/max cast back via `declared_cast_type`); verify its unit tests
- [x] 4.2 Route `generator._build_agg`, `value_expr._render_builtin_aggregate` (and the HAVING seam) and the association producer's level-1 pick through the helper, input type from `boolean_valued`; verify 1.5–1.7 and 1.12 pass
- [x] 4.3 SQL Server predicate-value rewrite in `slayer/sql/dialects/tsql.py` over the assembled statement; verify 1.11 passes

## 5. Docs and gates

- [x] 5.1 `docs/concepts/models.md`: per-type table adds `avg` for `boolean`; one sentence on how booleans are aggregated (sum counts trues, avg is the share, min/max stay boolean)
- [x] 5.2 `docs/concepts/formulas.md` (aggregated-expression grammar): one sentence that comparisons and `in` can be aggregated (`sum(amount > 15)`)
- [x] 5.3 Drop the nonexistent `BETWEEN` from `consecutive_periods`: the input sentence in `docs/concepts/formulas.md` and the predicate-shape error strings in `slayer/sql/generator.py`; verify 1.14 passes
- [x] 5.4 Full unit suite (`poetry run pytest -m "not integration"`), integration suite with the CI invocation, `ruff check`, `basedpyright` (no new errors), `la-arch-check` — all green
- [x] 5.5 Comment on DEV-1970 that it is delivered by DEV-2046's PR and that its `BETWEEN` half is moot (Mode B has no `BETWEEN`; the internal `BetweenKey` was deleted by DEV-1999)

## 6. Plan amendment (user-approved 2026-10-05)

- [x] 6.1 Tests: `tests/test_dev2046_all_null_inputs.py` — every built-in over all-NULL numeric and boolean inputs, locally, cross-model, in a stage, window, partition and association, on SQLite + DuckDB (association fixture row added); `test_comparison_by_region` west oracle → `None`
- [x] 6.2 Tests: statistical aggregations over booleans accepted with hand-computed values (`median(flag)` 0.5, `median(amount > 15)` 1, `stddev_samp(coalesce(flag, false))` ≈ 0.5477, `percentile`, `weighted_avg`); BOOLEAN default set = numeric set; emission tests for the stat / dialect-hook / formula builders taking the integer
- [x] 6.3 Tests: booleans in numeric positions — `flag * amount`, `(amount > 15) + 1`, `round(flag)`, `coalesce(flag, 0)`, `iif(x, flag, 2)`, `flag = 1`, `(sum(amount) > 50) + (count(*) > 1)` executed on SQLite + DuckDB + Postgres; T-SQL arithmetic operand is the INT of the BIT value
- [x] 6.4 Tests: `sum(True)`, `avg(True)`, `max(True)`, `count(True)`, `sum(1 > 2)` executed (row count / 1.0 / true / row count / 0)
- [x] 6.5 Flip the existing rejection tests to acceptance (`test_expression_aggregations.py` boolean gates, `test_dev1846_composite_transforms.py` arithmetic / scalar-argument, `test_dev1854_null_test_predicates.py` null test plus 1, `test_dev2046_boolean_exec.py` statistical rejection, `test_dev2046_boolean_emission.py` T-SQL arithmetic operand); `test_dev1744_naming_allocator.py` no-leaf fixture → nested aggregate; `test_agg_render_spec.py` pins 12 fields
- [x] 6.6 Re-bless goldens per protocol: `ts/series_in_pred::tsql`; `reject/cp_boolean_numeric` (5 dialects) → `cp/boolean_numeric` emitted SQL
- [x] 6.7 Implement decisions 3 (value-read lowering for every numeric aggregation), 7, 8, 9; verify 6.1–6.6 pass
- [x] 6.8 `architecture/semantics.arc42.md` Axiom 4 all-NULL clause + its enforced tag (approved edit)
- [x] 6.9 Docs: `docs/concepts/models.md` boolean row = numeric set and the sentence says every numeric aggregation; `docs/concepts/formulas.md` one sentence on booleans in arithmetic, and the `consecutive_periods` input sentence drops the value-position rejection
- [x] 6.10 Comment on DEV-1972 that expression sources now reach the builders as AST
- [x] 6.11 `weighted_avg` skips a NULL value's weight and never divides as integers (user-approved 2026-10-05): template fix; re-derive the `weighted_avg` oracles that counted NULL values' weights; `test_parameters_not_masked_by_source_filter` checks the unmasked weight through the custom `wavg`; MODIFIED `models/column-filters` scenario
- [x] 6.12 T-SQL avg reads its input as a float (integer AVG truncates): dialect flag in the aggregate-application helper; emission + SQL Server execution tests; re-bless the 10 T-SQL goldens whose AVG reads a non-DOUBLE input, per protocol
