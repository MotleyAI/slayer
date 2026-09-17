# Tasks

## 1. Tests first (TDD — all initially failing, one per spec scenario)

- [ ] 1.1 `tests/test_time_granularity_lattice.py`: the full 9×9 `TimeGranularity.nests_into` matrix (equal, the four nesting chains, transitivity, week/month and week/week_sunday rejected both ways); fails until 2.1
- [ ] 1.2 `tests/test_dev1471_stage_time_dimensions.py`: binder-level stage cases (a stage `TimeDimension` binds to `TimeTruncKey(ColumnKey(path=(), leaf))` with the `StageColumn` facts; unknown → `UnknownReferenceError`; dotted → `IllegalScopeReferenceError`), checker-level cases (finer, non-nesting both ways, non-temporal, untyped) asserting `TimeDimensionColumnError` and both granularities in the message, and executed-value cases on SQLite + DuckDB (same-granularity multi-hop flat name, coarser `year` over `month`, raw column, the cohort shape with the `partition_by` auto-name plus same-column filter at every granularity, three-stage chain, stage dimension + stage bucket grain); fails until 2.x
- [ ] 1.3 `tests/test_dev1471_stage_date_range.py`: stage `date_range` restricts the outer stage (executed on SQLite + DuckDB; SQL carries the BETWEEN on the stage column in the outer WHERE) and on a coarser re-bucketing; fails until 2.6
- [ ] 1.4 `tests/test_dev1471_stage_time_axis.py`: `time_shift`, `change`, `cumsum`, `last`, windowed sum over a stage time dimension (executed on SQLite + DuckDB, hand-computed values), the multi-hop `time_shift` shape, two stage time dimensions → remedy error, `main_time_dimension` selects; `gran(col)` order key on a stage and `.month`/`.year` suffixed stage keys; fails until 2.7
- [ ] 1.5 `tests/test_dev1471_sqlite_temporal_cast.py`: `created_at:max` by customer, `created_at:max(partition_by=customer_id)`, and a non-identifier derived TIMESTAMP column projected and `max`-aggregated all return full dates on SQLite and DuckDB; SQLite SQL has no `CAST(... AS TIMESTAMP)` around them; Postgres/DuckDB/T-SQL/BigQuery SQL for the same shapes is byte-identical before and after; fails until 2.8
- [ ] 1.6 Existing tests (approved edits): `tests/test_time_dimensions_planner.py` and `tests/test_dev1450fix_derived_time_dimension.py` direct calls move to `.bound.value_key`; `test_stage_schema_scope_rejected` retired (its class now covered by 1.2); `test_non_temporal_column_rejected` re-pointed at `check_time_dimension_column`; the two `DEV-1471` skips in `tests/test_nested_dag_cross_stage_refs.py` removed; `tests/_dev1871_raise_ledger.py` gains the two `TimeDimensionColumnError` rows; verify each file's targeted run is red only where implementation is pending

## 2. Implementation

- [ ] 2.1 `slayer/core/enums.py`: `TimeGranularity.nests_into`; verify 1.1 green
- [ ] 2.2 `slayer/core/errors.py`: `TimeDimensionColumnError`; `slayer/core/scope.py`: `StageColumn.granularity`; `slayer/ir/bound.py`: `BoundTimeDimension`; verify imports and `tests/test_errors_hierarchy.py` still green
- [ ] 2.3 `slayer/engine/elaborate_env.py`: `check_time_dimension_column` (temporal rule incl. untyped fail-closed; nesting rule naming both granularities + remedy); verify the checker-level cases in 1.2 and the ledger parity test green
- [ ] 2.4 `slayer/engine/binding.py`: column-facts helper with model and stage arms, `bind_time_dimension` returns `BoundTimeDimension`, the binder's temporal `ValueError` deleted; verify the binder-level cases in 1.2 and 1.6's direct-call files green
- [ ] 2.5 `slayer/engine/compile/stages.py`: `_emit_stage_schema` stamps `granularity` from a `TimeTruncKey` slot; `slayer/engine/bind_inputs.py`: checker call after each projected time dimension, `.bound` at the order-key and active-TD sites; verify the executed cases in 1.2 green on SQLite + DuckDB
- [ ] 2.6 `slayer/engine/bind_inputs.py`: delete the `date_range` scope skip, `_build_date_range_filter` accepts either scope; verify 1.3 green
- [ ] 2.7 `slayer/engine/bind_inputs.py`: active time dimension resolves on a `StageSchema` (`_resolve_main_time_dimension` with optional model, default only with a model); verify 1.4 green and the two un-skipped nested-DAG tests green
- [ ] 2.8 `slayer/sql/dialects/base.py` + `sqlite.py`: `declared_cast_type`; `slayer/sql/generator.py`: `_cast_declared` helper routed through `_slot_cast_type`, `_resolve_sql`, `_derived_column_expr`; verify 1.5 green and re-bless only SQLite goldens that pinned the temporal cast (list them in the PR)

## 3. Docs and spec hygiene

- [ ] 3.1 One sentence each in `docs/concepts/queries.md` (TimeDimension section: stage columns bind; same/nesting-coarser only), `docs/concepts/formulas.md` (transform time-dimension requirement: a stage's own entry counts), `docs/examples/06_multistage_queries/multistage_queries.md` (outer stage re-buckets by year); verify by reading and `grep` that no doc still says stages cannot declare time dimensions
- [ ] 3.2 `openspec validate dev-1471-cross-stage-time-dimensions-re-bindable-time-dim-on --strict` green after any spec wording adjustments made during implementation

## 4. Verification

- [ ] 4.1 Full non-integration suite green: `poetry run pytest -m "not integration" -n auto`
- [ ] 4.2 `poetry run ruff check slayer/ tests/`, `poetry run python tools/arch_check.py`, `poetry run basedpyright` (no new errors vs baseline), conventions gate clear; guards baseline in `architecture/index.yaml` unchanged
- [ ] 4.3 Record the DEV-1929 deferral pointer and the OpenSpec change id on the Linear issue (done at plan time); PR only after DEV-1883's PR #402 lands, per the stacking rule
