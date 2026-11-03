Port from the branch (see design.md › Context for refs) keeping its symbol names, module layout and test-file names (design D4). Each test file ports the branch's file of the same name at `adc0d8a7`, minus out-of-scope cases (design › Non-Goals), with cases using `BETWEEN`, `IS DISTINCT FROM`, `json_value`, typed `DATE '…'` literals or `CAST(… AS ARRAY)` rewritten or dropped.

## 1. Failing tests (pr-tests stage)

- [ ] 1.1 Fixtures: `tests/_dev2120_fixtures.py` (list subset only: `LIST_STORAGE`, `ListStorage`, `contractors_engine`, `contractors_model`, `stored_contractors`, `elements`, `list_cases`, `check_library`), `tests/_dev2137_fixtures.py`; verify they import cleanly once the production symbols exist
- [ ] 1.2 `tests/test_dev2137_list_columns.py` and `tests/test_dev2137_element_types.py`: every `models/list-columns` scenario incl. the sql-mode element-type requirement, eligibility errors with remedies, the eight-dialect plan-time refusals (parametrised over SQLite, MySQL, MariaDB, SQL Server, Redshift, Databricks, Spark, Oracle — design D19), and element-type threading through stages / re-rooted producers / result metadata; verify they fail
- [ ] 1.3 `tests/test_dev2137_list_functions.py`, the list subset of `tests/test_dev2169_postgres.py`, and `tests/test_dev2120_library_matrix.py` as the per-dialect list matrix (list functions, array literal, `[]`, list variables, `collect`, `list_relation`, `list_exists`; executed on DuckDB and Postgres, refused on SQLite, emitted on every native-list dialect): every `queries/list-functions` scenario incl. `contains` on text naming `LIKE`, A′ (numeric string literal converted; `'five'`, a TEXT column and `['1', 2]` refused; decimals, exponents, whitespace and non-finite spellings — design D17), mixed numerics, Postgres physical array types, and list-variable edge cases (`[]`, nested list, repeated placeholder, list placeholder in a non-list function — design D15); verify they fail
- [ ] 1.4 `tests/test_dev2137_list_children.py`: every `queries/list-children` scenario incl. the filter across another to-many hop and no self-join per dialect; verify they fail
- [ ] 1.5 `tests/test_dev2137_collect.py`: every `aggregations/collect` scenario incl. BigQuery exact numerics (emission), the model's own `collect` formula on SQLite and DuckDB, and the untyped empty list at aggregate and join-back; verify they fail
- [ ] 1.6 `tests/test_dev2137_facade_lists.py`: every `facade/list-values` scenario; verify they fail
- [ ] 1.7 `tests/test_dev2137_profiling.py`, `tests/test_dev2137_schema_drift.py`, `tests/test_drift_list_retype.py`: the `models/sample-profiling` and `models/schema-drift` deltas incl. the twice-re-ingested field-effects scenario (design D14) and the sql-mode no-drift scenarios; verify they fail
- [ ] 1.8 `tests/test_library_name_shadowing.py`, `tests/test_library_name_shadow_warnings.py` (save, load, query, dry run, EXPLAIN, cached response, REST, MCP — design D16), `tests/test_granularity_name_migration.py` (YAML and SQLite parity — design D19), `tests/test_library_growth.py`, the `collect` part of `tests/test_builtin_aggregation_overrides.py`, `tests/_library_names_fixtures.py`, `tests/_granularity_rename_fixtures.py`, and the `tests/test_dev1883_reserved_agg_names.py` edits: every `models/aggregation-names` and `models/stored-upgrade` delta scenario; verify they fail
- [ ] 1.9 `ArrayLiteralKey` traversal / substitution tests ported from PR #518's generic key tests (design D12) and `tests/test_law_database_door.py` cases for Snowflake inference, re-type on re-ingest, list profiling and sql-mode cursor typing (design D13); verify they fail
- [ ] 1.10 Live suites in main's integration files: BigQuery 5 (incl. `REPEATED` ingestion), Snowflake 5 (incl. element inference), ClickHouse 4 (≥ 25.8), Trino 3, each incl. the mixed-numeric case; verify they fail where the backend is available

## 2. Types and metadata

- [ ] 2.1 `DataType.ARRAY`, `is_list`, `ELEMENT_TYPES`, `type_display`; `Column.element_type` + `check_element_type`, `list_operations_hint`, `column_type_text`; `SlayerModel.list_columns()` / `_list_of` / `_list_children`; `DEFAULT_AGGREGATIONS_BY_TYPE[ARRAY] = [count]` and the save-time check; verify the metadata tests in 1.2 pass
- [ ] 2.2 Thread `element_type` through `StageColumn`, `ValueSlot`, `DeclaredMeasure`, `model_from_stage_schema` and `aggregation_result_type` returning `(type, element)`; verify the threading tests in 1.2 pass

## 3. Ingestion, cursor typing and drift

- [ ] 3.1 Native list mapping in main's `sql/dialects/reflection.py` (`ReflectedColumn.element_type`, `column_from_sa_type`, `column_from_type_string`, `_sa_list_item`, `_sa_element_type`, `sa_data_types`, `list_db_type`, `info_schema_list_element`); verify the ingestion tests pass
- [ ] 3.2 Snowflake untyped-ARRAY element inference through the ingesting engine's `SqlClient` (`list_element_types_query` / `list_element_type` hooks; never `sa_engine.connect()` — design D13); verify its tests and `tests/test_law_database_door.py` pass
- [ ] 3.3 Cursor typing and row fetch in `sql/client.py` (`CATEGORY_DATA_TYPES`, `"list"` in `_map_type_code`, Postgres array OIDs, tuples → lists in `_fetch_rows`, per-dialect `result_readers`, `sql/dialects/_clickhouse_cells.py`); sql-mode list columns get `element_type: UNKNOWN` unless declared (design D18); verify the sql-mode tests pass
- [ ] 3.4 Drift: `ListRetypeFinding`, `list_retype_findings`, `REPORT_ONLY_FINDINGS`, `SchemaDriftError(findings=)`; re-ingest `_retyped_list` for any non-list base column (keep description / label / meta / sql / hidden / keys; keep only `count`; drop `format` and granularity); verify 1.7 passes

## 4. Eligibility, list functions and the array literal

- [ ] 4.1 `ListTypeError`; `ListTyping`, `check_list_operands`, `check_list_dimension`, `_check_list_*`, `_check_native_lists`, wired through `_list_typing` in `bind_inputs.py` in the branch's order (check, then `coerce_list_arguments` in `_map_bound_keys` — design D17); `native_lists` / `database_name` ClassVars, `False` on the eight non-list dialects; verify the eligibility and refusal tests pass
- [ ] 4.2 `contains`, `contains_any`, `contains_all`, list `length` in main's `SCALAR_SPECS`; `boolean_valued` covers the three booleans; `list_valued`, `is_list_call`, `literal_type`, `value_type`, `list_element_type`; `contains` list-only with the `LIKE` remedy (design D1); A′ in `_same_family` / `coerce_list_arguments` (design D2, D17); verify the typing tests pass
- [ ] 4.3 Dialect hooks `build_list_call`, `list_each`, `list_length`, `list_contains`, `list_overlaps`, `list_contains_all` with overrides in postgres (incl. canonical-array casts), duckdb, bigquery, snowflake, clickhouse, trino; rendered from `render_scalar_call`; verify the list matrix passes
- [ ] 4.4 `ArrayLit` in `engine/syntax._convert`, `canonical_measure_text`, `_walk_parsed`; `ArrayLiteralKey`, `_bind_array_literal`, `render_array_literal`; audit every key-kind dispatch site PR #518 touched so the new kind is handled or fails closed (design D12); verify 1.9's traversal tests pass
- [ ] 4.5 List variables: `_LIST_ARG_FUNCTIONS` / `_list_slot_ordinals` re-homed into `core/query.substitute_variables` (design D15); verify the list-variable tests pass

## 5. List children

- [ ] 5.1 `LIST_ELEMENT_COLUMN`, `list_child_name`, `list_child`, `with_list_children`, `_lateral_edge`, `_list_edges`, `is_lateral_edge`, `OrientedJoin.lateral`; `_reject_opaque_elements`; a list child built once per parent (`649ecc2b`); verify the resolution tests pass
- [ ] 5.2 Element filters by EXISTS (`SemiJoinHop.lateral`, `SemiJoinFilter.outer_list_only`, `_leaf_list_predicates`, `_element_test`, `list_exists` per dialect); verify the filter tests pass
- [ ] 5.3 Element aggregates, histogram, association and no fan-out (`generator._list_relation` / `_list_join`, `compile/stages._forward_lateral` / `_is_parent_hop` / `_reached_by_root_lists`, `AssociationProducerKernel.list_rows`, `scope_check._array_joined` / `_is_lateral`, `list_relation` per dialect); the filter across another to-many hop and the ClickHouse < 25.8 refusal (`02a1fc10`); verify 1.4 and the cross-model-aggregates delta pass
- [ ] 5.4 Stage element access (`_stage_list_scope`, `_stage_element`, `stage_column_element`, `key_element_type`); verify the stage tests pass
- [ ] 5.5 Save-time hop-name collision (`storage/base._check_list_hop_collisions`); verify the collision tests pass

## 6. collect

- [ ] 6.1 `LIST_AGGREGATIONS`, `AggregationValueClass.LIST`, the `AGG_REGISTRY` entry; `build_collect` / `filtered_collect` / `empty_list` per dialect; `EmptyList` and `_empty_value_expr` (untyped empty list at aggregate and join-back, `4322c840`); BigQuery exact numerics; a formula-backed `collect` stays the model's own; verify 1.5 passes

## 7. Library-name collisions

- [ ] 7.1 Lift the scalar-name check from `Aggregation._reject_transform_names`; `shadows_library_function`, case-resolving `aggregation_definition` with `AmbiguousAggregationError`; parse deferral in `engine/syntax.py` (`ScalarCall.written` / `written_args`, `AggCall.library_error`; scalar keyword-argument, arity and method-call refusals deferred to binding — design D11); binder resolves the shadow against the owning model; verify `test_library_name_shadowing.py` passes
- [ ] 7.2 Shadow warning in `core/warnings.py` and its plumbing through save, load and every response incl. dry run, EXPLAIN, cached, REST and MCP (design D16); verify `test_library_name_shadow_warnings.py` passes
- [ ] 7.3 `storage/granularity_rename.py` and the on-load rename in `storage/base.py` (both backends; dependants persisted first; converging); `reserved_granularity_names()` covers the new names; verify `test_granularity_name_migration.py` and `test_library_growth.py` pass

## 8. Profiling, facades and the MCP surface

- [ ] 8.1 Element profiling (`engine/profiling._profile_type` / `_values_ref`); verify the profiling tests pass
- [ ] 8.2 `core/format.list_json_text`; ARRAY → text in `facade/datatypes.py`, `pg_facade/types.py`, `flight/types.py`; list cells in `flight/handlers.py` and `mcp/server._format_csv`; REST/MCP JSON lists; verify 1.6 passes
- [ ] 8.3 `model_render._render_column_type` / `_choose_sample_agg`, `search/render`, `collection_render`; query-tool field-description clauses in `core/query.py`; `element_type` in `edit_model`'s column-key list; MCP query-schema budget; verify the rendering and `tests/test_mcp_surface_budget.py` tests pass

## 9. Architecture, docs and help

- [ ] 9.1 Apply the four approved arc42 edits verbatim (design D5); verify `poetry run la-arch-check` passes
- [ ] 9.2 Docs, list-column content only, worded against main's grammar: `docs/concepts/{models,formulas,queries,references,ingestion,terminology}.md`, `docs/database-support.md`, `docs/interfaces/{pg-facade,flight-sql}.md`, `docs/reference/mcp.md`; help topics 01 / 02 / 03 / 06 / 07; verify the help-memory tests pass and no new page needs a `zensical.toml` entry

## 10. Verification

- [ ] 10.1 Full unit suite (`poetry run pytest -m "not integration"`), the CI integration invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new baseline entries) and `poetry run la-arch-check`; all green
