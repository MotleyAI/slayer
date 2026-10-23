## 1. Tracker records (pr-plan)

- [x] 1.1 Plan approved (scope = all six column positions; dimension as text; default ranking key admits temporal expression dimensions; per-leaf re-bucketing; spine refusal only for expressions reading the spine) and Codex-reviewed; verify this change validates with `openspec validate --strict`

## 2. Failing tests (pr-tests)

Write tests from the specs of this change. Scenarios needing DEV-2101 forms (`CAST`, `INTERVAL`) fail for that reason too until DEV-2101 is merged into this branch; prefer `coalesce` / existing date functions where a scenario allows it.

- [x] 2.1 `queries/time-dimensions` scenarios (expression time dimensions incl. cross-model, fanning leaf, non-temporal, undetermined type, aggregate inside, stage scope; every time position incl. `date_range`, `whole_periods_only` DATE vs TIMESTAMP with explicit / mirrored / synthesized bounds, `window=`, `change`, two-stage re-bucketing, parity with a derived column; names, result keys, `main_time_dimension` by name, `partition_by=` by name, collisions; per-leaf re-bucketing; text dump and schema; legacy object input; functional forms and order key), executed on SQLite and DuckDB; verify they fail
- [x] 2.2 `queries/time-points` granularity-call scenarios (expression argument in filter and computed dimension, non-temporal argument, wrong shapes); verify they fail where behaviour is new
- [x] 2.3 `queries/time-spine` scenarios (spine expression refused; fact expression time dimension in P); verify they fail
- [x] 2.4 `queries/partitioned-aggregates` expression partition-key scenarios (computed-dimension equivalence, combined-position error, finer-grain exemption, rank family, time-dimension source incl. the ambiguity error, unproven hop, aggregate-bearing key) and the mixed-source `first` scenario; verify they fail
- [x] 2.5 `aggregations/functional-form` scenarios (default ranking key incl. a temporal computed dimension and an expression time dimension's source; expression ranking key; fanning leaf in a ranking key for local and target-rooted producers; cross-model ranked aggregate with an expression key; separate vs shared producers by ranking key) and `aggregations/expression-aggregation` scenarios (`first` / `last` over expression and mixed sources, `first(1)`, expression parameters incl. positional, `corr`, cross-model, undetermined); verify they fail
- [x] 2.6 Ranked kernel coverage: expression value, expression ranking key, both together, joined leaves, attached constituent, windowed `first` / `last` with an expression key — executed values on SQLite and DuckDB; emission on every Tier-1 dialect; verify they fail
- [x] 2.7 `models/stored-upgrade` scenarios (object → text in source queries, saved queries and memories as YAML and SQLite rows; text untouched; unversioned client query untouched; 0.10.2 corpus); verify they fail
- [x] 2.8 Retarget, with the user's approval of each, the tests pinning refusals this change lifts, listing each in the commit message: `tests/test_time_points_exec.py:384` (`month(upper(code))` → date-operand type error), `tests/test_dev1883_errors.py:28` (`month(upper(x))` leaves the construction-time wrong shapes → binding temporal error), `tests/test_dev2015_time_dimensions.py:58` (`fiscal_year(upper(id))` → temporal error), `tests/test_dev1832_transform_source.py:300`, `tests/test_dev1903_first_last_dispatch.py:158` (incl. `first(1)`), `tests/test_dev1832_cross_model_exec.py:306`, `tests/test_dev1859_plan_structure.py:218` (`first` / `last` over an expression → executed values). Ranking-key error texts keep the substrings `ranks by a column` / `ranks by at most one column`, so `tests/test_agg_positional_params.py` and `tests/test_dev1946_transform_parameter.py` stay unchanged
- [x] 2.9 Codex review of the tests against this change

## 3. Prerequisite

- [x] 3.1 Merge DEV-2101 (merged to main, else its branch) into this branch; verify `slayer/core/syntax.py` and `CastKey` exist and the full unit suite is green before any implementation

## 4. One bucket key

- [x] 4.1 Rename `TimeTruncKey.column` → `source` with `la:deterministic-refactor`; verify `la-typecheck`, `dr-mock-lint` and the full unit suite
- [x] 4.2 Widen `source` to any row-level key; move `column_leaf` / `column_path` callers (`generator.py`, `stages.py`, `spine.py`, `bind_inputs.py`, `time_bounds.py`, `naming.py`, `ranked_planner.py`, `value_expr.py`) to the key's references; render truncation over any row-level source; verify golden baselines byte-identical

## 5. Time dimensions

- [x] 5.1 `TimeDimension.dimension` as text, legacy object / `column` alias coercion, `gran(expr)` functional forms, `name`, dedupe / strip / merge / variables / facade rebuilders, name uniqueness; verify 2.1 construction scenarios
- [x] 5.2 Binding of expression time dimensions (home, row-level check), checker typing with the source → type map, `date_range`, `whole_periods_only`, frame bounds, `window=` / transforms, result keys and names, `main_time_dimension` and `partition_by=` by name; verify 2.1 and 2.2
- [x] 5.3 Every `full_name` / `name` / `model` consumer (`unrelated.py`, `model_reads.py`, `schema_drift.py`, `population.py`, `time_spine.py`) on parsed references; verify their suites
- [x] 5.4 Stage boundary: expression time dimensions emit a typed, bucketed `StageColumn` without single-column provenance; verify the two-stage scenarios
- [x] 5.5 Per-leaf re-bucketing; verify its scenarios
- [x] 5.6 Spine: refuse expressions reading `time_spine.timestamp`, admit other expression time dimensions into P; verify 2.3

## 6. Partition keys, ranking, parameters

- [x] 6.1 `_bind_partition_keys` over row-level expressions, computed-dimension / bucket equivalence, source → bucket mapping with the ambiguity rule, aggregate-bearing key error with the computed-dimension remedy; verify 2.4
- [x] 6.2 `explicit_ranking_key` (renamed from `explicit_ranking_time_arg`) over any row-level argument with both Axiom 2.4 safety paths; `_fold_positional_agg_args` ranking-key rule; default ranking key precedence; verify 2.5 ranking scenarios
- [x] 6.3 `first` / `last` over expression and mixed sources: lift the `_bind_agg` refusal; `_ranked_value_expr` / `_ranked_scope_expr` through the row-expression renderer with joined paths and row-attach substitution; verify 2.5 and 2.6
- [x] 6.4 `_bind_agg_arg` over row-level expressions with parameter typing by the home grain; verify 2.5 parameter scenarios
- [x] 6.5 Rewrite the remedies pointing at an inline `ModelExtension` column (`binding.py` partition-key error, `help_content/02_workflow.md`); verify `grep` finds no such remedy

## 7. Migration

- [x] 7.1 Stored-only step (SlayerQuery, SlayerModel, Memory; versions after DEV-2101's) rewriting object-form time dimensions to text; verify 2.7 and the 0.10.2 corpus

## 8. Architecture

- [x] 8.1 Apply H1 and H2 exactly as in design.md with `enforced: test:` tags naming this change's tests; verify `la-arch-check`

## 9. Docs and agent surfaces

- [x] 9.1 `docs/concepts/queries.md` (TimeDimension text form and `name`, partition keys), `formulas.md` (`partition_by=`, `first` / `last` value and ranking key, expression parameters, default ranking key), `time.md` (spine and expressions); verify no doc shows the object form of a time dimension
- [x] 9.2 MCP `Field` descriptions (`TimeDimension`, `main_time_dimension`) and `slayer/memories/help_content/*`; verify `tests/test_mcp_surface_budget.py` and the documented-idioms tests
- [x] 9.3 Release-notes draft (untracked, never staged): the text dump of `TimeDimension.dimension`, Python attribute access, the default-ranking-key result change, the lifted refusals

## 10. Gates

- [ ] 10.1 `poetry run pytest -m "not integration"`, the CI integration invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `la-arch-check`; verify all green
