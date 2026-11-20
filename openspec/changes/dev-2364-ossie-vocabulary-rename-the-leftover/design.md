## Context

See proposal.md › Why. DEV-2318's word table and carve-outs apply unchanged: model → dataset, column → field, measure → metric, join → relationship, formula → expression. Kept: physical and result-set columns, SQL `JOIN` as an operation, RLS / session-policy names, foreign formats (dbt, Cube, Metabase, OSI), Ossie's own spellings (`from_columns`, `to_columns`, `column_pairs`), Pydantic `model_*`, the embedding model, the storage layout (`models/`) and stored-document upgrade paths.

The guard (`tests/test_vocabulary_identifier_guard.py`, `tests/_vocabulary_guard.py`, `tests/fixtures/vocabulary_identifier_allowlist.yaml`) scans every binding in `slayer/`, locals included, and matches entries on file and name. It is green on the merged tree with 37 stragglers.

The branch carries DEV-2277 (in progress, local `7a5fb8aa9`) and `origin/main` (`abccc2074`). DEV-2277's test respell (its task 4.1) is still open, so later DEV-2277 commits touch the same test files.

Principles that apply: `system.arc42.md` §3 P1 (every top-level package claimed; none added), P2 (model truth; no runtime import edge changes, test modules are not modelled) and P6 (Pydantic, never dataclasses). No arc42 or LikeC4 edit; no new capability, so no spec attachment.

## Goals / Non-Goals

**Goals:**

- `slayer/`: zero stragglers; the guard green with `carve_outs` only.
- `tests/`: every in-scope definition (Decision 3) spelled in the new vocabulary or listed in the appendix with its reason.

**Non-Goals:**

- A guard over `tests/` (Decision 1).
- Locals anywhere in `tests/`, test function and class names, `test_*.py` file names, fixture data paths (Decisions 2, 6).
- `tests/perf/compare/` (the pinned PyPI engine reads its spelling).
- Re-triaging existing `slayer/` carve-outs.
- Read compatibility for old Cube report files (Decision 13).

## Decisions

1. **The `tests/` rename is a one-time sweep with no `tests/` guard,** "for now". Alternative: extend the guard to the in-scope `tests/` kinds with its own `carve_outs`. (user)
2. **Locals inside helpers and fixtures are exempt,** like test locals; only the helper interface (name, parameters, constants, class fields) is renamed. Alternative: rename them as in `slayer/`. (user)
3. **In scope in `tests/`** (except `tests/perf/compare/`), when naming a SLayer concept with an old word: helper function names; helper class names and their methods; non-test methods inside test classes; fixture names, with the parameters of the tests requesting them; module constants; class fields of helper and test classes; parameters of helpers, fixtures and helper methods (e.g. `joins=` of dataset-building fixtures → `relationships=`); `import … as` aliases; the helper modules `tests/_model_access_fixtures.py` → `tests/_dataset_access_fixtures.py` and `tests/_cross_model_chain.py` → `tests/_cross_dataset_chain.py`. (user, approved plan)
4. **Test-data name literals are in scope** (DEV-2347 decision 12): dataset and field name literals naming a SLayer concept with an old word are respelled (`no_such_model` → `no_such_dataset`, `hidden_model` → `hidden_dataset`, `via_join` → `via_relationship`). Kept: literals testing old spellings (legacy-conversion, deprecation, stored-upgrade tests), legacy-store fixtures, foreign formats and the guard's synthetic sources. Goldens and key assertions that move only with a respelled name are re-blessed. Alternative: out of scope. (user)
5. **`CubeIssueCategory.SEGMENT_AS_COLUMN = "segment_as_column"` becomes `SEGMENT_AS_FIELD = "segment_as_field"`,** with a before/after entry in `docs/upgrading/2.0.md`. Alternative: rename the member only and keep the value. (user)
6. **`test_*.py` file names and fixture data paths keep their words:** they are test names and mirror `slayer/` modules (`slayer/core/models.py` stays), Cube's layout (`cube_project/model/`) or the storage layout (`yaml_store/models/`). (default)
7. **Renamed test helper modules leave no `raise_moved` stub;** all 31 importers are updated. Stubs serve users' imports of `slayer.*`; `tests/` is not packaged. Alternative: stubs plus tripwire pins until 3.0. (user)
8. **No deprecated twins:** no in-scope name is on DEV-2318's curated twin list, and Storyline uses none (grep of `retyped_sql_columns`, `CubeIssueCategory`, `segment_as_column`). (default)
9. **A mixed-use name** (straggler and carve-out uses in one file, e.g. `live_schema/ingestion.py: columns`, `sql/generator.py: column`) **has its straggler uses renamed and moves to `carve_outs` with its reason,** per the DEV-2364 comment of 2026-11-19. (default)
10. **The straggler machinery is deleted outright once the list is empty:** `Straggler`, `VocabularyAllowlist.stragglers`, `guard_problems`' `ceiling`, the ceiling tests and `x-vocabulary-stragglers` in `architecture/index.yaml`. `extra="forbid"` then refuses a `stragglers:` key. The aligned guard tests and the `index.yaml` line deletion were approved with the plan. (default, user approved edits)
11. **DEV-2277 is merged again at the start of `pr-tests` and of `pr-implement`** (or `origin/main`, once DEV-2277 has landed). (default)
12. **`skip_specs: true`:** no specified behaviour changes. (default)
13. **The Cube report accepts only the new value;** old report files holding `segment_as_column` no longer validate into `CubeConversionResult`. SLayer only writes that file (`slayer/cli.py`), never reads it back. This is an unspecified output change, recorded here and in the upgrade notes. Alternatives: accept the old value with a deprecation until 3.0; keep the old value. (codex, user)
14. **Every `dr-refactor rename-map --docs` run `--keep`s the test files and directories that hold old spellings on purpose** (legacy-input and deprecation tests such as `tests/test_ossie_vocabulary_fresh_input.py`, stored-upgrade tests, `tests/fixtures/vocabulary_corpus/`, `tests/fixtures/upgrade/`, the guard's synthetic sources), along with archived OpenSpec changes. Every string and comment edit is reviewed separately from the identifier edits. (codex)
15. **After the rename, the AST probe is re-run** with Decision 3's exact scope and exemptions (a throwaway scratchpad script, not committed). Its output is this file's "Kept test identifiers" appendix; every remaining hit carries a reason. (codex)
16. **The two renamed helper modules' corpus files follow them:** `tests/fixtures/vocabulary_corpus/_cross_model_chain.json` → `_cross_dataset_chain.json` and `_model_access_fixtures.json` → `_dataset_access_fixtures.json`, with their `_index.json` `source` and `corpus` entries updated. (codex)
17. **The malformed-allowlist test gains a `removed-stragglers-key` case `{"stragglers": []}`,** and a positive test asserts that `{}` parses, so a missing `carve_outs` stays valid. (codex)

## Risks / Trade-offs

- [A carve-out renamed by mistake: a physical column, an SQL join, an RLS name, a dbt or Cube object] → per-hit triage against the carve-out table; `dr-refactor` stand-ins triaged one by one; goldens, message-pinning tests and the vocabulary corpus catch changed behaviour.
- [`--docs` rewrites a deliberate old spelling] → Decision 14's `--keep` list and the separate review of string edits.
- [Merge conflicts with DEV-2277's open test respell] → Decision 11; renames are re-applied from the map if a merge reverts them.
- [Old words return in new test helpers] → accepted (Decision 1).

## Migration Plan

No stored data or wire key changes. The Cube report category value changes (Decision 13). Rollback is a revert.

## Kept test identifiers

Every in-scope definition (Decision 3) in `tests/` still spelled with an old word after the rename, from the post-rename probe, grouped by reason. Parameters are written `function(parameter)`.

### SQL JOIN as an operation

- `tests/_dev1746_fixtures.py`: `join_alias_sequence`, `base_cte_join_sequence`
- `tests/_dev1747_fixtures.py`: `base_from_join_aliases`, `grain_join_back_predicates`
- `tests/_dev1853_fixtures.py`: `chain_datasets(join_type)`
- `tests/_dev1948_fixtures.py`: `semi_join_list`
- `tests/_engine_helpers.py`: `_join_aliases`
- `tests/_mcp_idiom_fixtures.py`: `ANTI_JOIN`
- `tests/dialects/test_server_profile_hooks.py`: `_EXISTS_BESIDE_ARRAY_JOIN`, `_ARRAY_JOIN_ALONE`
- `tests/facade/test_translator.py`: `_join_catalog`, `_join_catalog(parent_join_target)`, `_join_catalog(parent_join_pairs)`, `_join_catalog(parent_join_type)`, `_metabase_join_sql`, `_metabase_join_sql(join_target_table)`, `_metabase_join_sql(join_alias)`, `_STORES_WITH_METRIC_JOIN`
- `tests/integration/test_integration_clickhouse.py`: `_SEMI_JOIN_QUERY`, `_SEMI_JOIN_ROWS`, `_assert_semi_join_runs_with_setting`
- `tests/test_cross_model_derived_columns.py`: `_join_aliases`
- `tests/test_dev1743_join_alias_registry.py`: `_join_table_to_alias`, `_all_join_aliases`
- `tests/test_dev1746_consumer_materialization.py`: `_JOIN`
- `tests/test_dev1853_join_walker.py`: `_forward_pair(join_type)`
- `tests/test_dev1859_scope_render.py`: `_join_count_to`
- `tests/test_dev1903_producer_flag.py`: `_semi_join_and_mask_counts`
- `tests/test_dev2102_query_inner_joins.py`: `_vip(join_type)`
- `tests/test_dev2189_statement_consumers.py`: `UNNEST_BESIDE_SEMI_JOIN`, `_ARRAY_JOIN_BELOW_EXISTS`
- `tests/test_join_cardinality_producers.py`: `_metabase_join_sql`
- `tests/test_scope_check.py`: `SQL_ARRAY_JOIN_IN_EXISTS`, `SQL_ARRAY_JOIN_ALIAS_OUTSIDE`
- `tests/test_stage2_host_migration.py`: `_ordered_join_aliases`
- `tests/test_stored_upgrade_join_keys.py`: `_join_on`

### SQL column in a SQL AST, statement or the SQL facade

- `tests/_dev1747_fixtures.py`: `aggregate_funcs_over(column)`
- `tests/facade/test_translator_time_bound_instants.py`: `_revenue(column)`
- `tests/pg_facade/test_bind_empty_string_classifier.py`: `column_type_index`
- `tests/test_dev1733_order_only_transform_composite.py`: `_cte_aggregate_sql(column)`
- `tests/test_dev1747_grouped_joined_order.py`: `_wrap_functions(column)`
- `tests/test_dev1747_order_resolver.py`: `_order_columns`
- `tests/test_dev1839_union_dim_execution.py`: `_last_part(column)`
- `tests/test_dev1965_statement_assembly.py`: `_column_names`
- `tests/test_dev2172_honest_catalogs.py`: `_sql_column`, `_select_pg(column)`, `_select_flight(column)`, `_thing_rows(column)`
- `tests/test_dev2172_wire_values.py`: `TEXT_COLUMNS`
- `tests/test_dev2367_list_elements_across_joins.py`: `_list_columns`
- `tests/test_importer_identifier_fold.py`: `_columns_read`
- `tests/test_nested_dag_cross_stage_refs.py`: `_outer_column_refs`
- `tests/test_sql_generator.py`: `_RankingOrder.columns`
- `tests/test_table_names.py`: `_key_columns`

### Embedding model

- `tests/test_embedding_retriever.py`: `_RecordingEmbedBatch.__call__(model)`, `_CountingStorage.get_embeddings_for_canonical_ids(embedding_model_name)`
- `tests/test_embeddings_storage.py`: `_embed(model)`
- `tests/test_ossie_vocabulary_custom_backend.py`: `_Base.get_embedding(embedding_model_name)`, `_Base.list_embeddings(embedding_model_name)`
- `tests/test_ossie_vocabulary_embeddings.py`: `_CountingEmbed.__call__(model)`
- `tests/test_ossie_vocabulary_help_topics.py`: `EMBEDDING_MODEL`
- `tests/test_sidecar_embedding_store.py`: `_embed(model)`
- `tests/test_storage_backend_compat.py`: `_DictStore.get_embedding(embedding_model_name)`, `_DictStore.list_embeddings(embedding_model_name)`

### Foreign format (dbt, Cube, Metabase, OSI)

- `tests/_dev2171_fixtures.py`: `import_dbt(semantic_models)`
- `tests/_osi_helpers.py`: `rel(from_columns)`, `rel(to_columns)`, `legacy(models)`
- `tests/test_dbt_converter.py`: `_project_with_orphan(extra_column_descriptions)`
- `tests/test_dbt_converter_inline_sql.py`: `_semantic_model`
- `tests/test_dbt_filters.py`: `_build_registry(models)`
- `tests/test_dbt_relation_quoting.py`: `_import(semantic_models)`
- `tests/test_dev1836_cube_cardinality.py`: `_project(join_kwargs)`
- `tests/test_dev1853_cube_importer.py`: `_project(customers_join_sql)`
- `tests/test_dev2171_cube_import.py`: `TestJoinedCubesPhysicalColumn.JOIN`, `TestJoinKeys.JOIN`
- `tests/test_dev2171_dbt_import.py`: `sm(measures)`, `typed(columns)`
- `tests/test_dev2171_importers_preserve_meaning.py`: `DBT_MODELS`
- `tests/test_dev2175_importers.py`: `JOIN`
- `tests/test_importer_identifier_fold.py`: `_dbt(semantic_models)`
- `tests/test_importer_source_names.py`: `_dbt(semantic_models)`
- `tests/test_osi_schema.py`: `_MODEL_FOR_DEF`

### Names the removed legacy plan shape

- `tests/test_dev1838_invariant.py`: `_LegacyCrossModelPlan`, `_LegacyCrossModelPlan.cte_root_model`, `_LegacyCrossModelPlan.target_model`

### Not the vocabulary word

- `tests/_server_shutdown_script.py`: `_join`
- `tests/test_engine_factory.py`: `TestCacheConcurrency._join_all`

### Physical database or result-set column

- `tests/_arrow_types.py`: `ArrowColumn`, `ArrowTable.column`
- `tests/_dev1737_fixtures.py`: `TableSpec.columns`
- `tests/_dev1747_fixtures.py`: `response_column_values`
- `tests/_dev1836_equiv_driver.py`: `_physical_columns`
- `tests/_dev1994_fixtures.py`: `col(columns)`, `by(columns)`
- `tests/_dev2119_fixtures.py`: `column`
- `tests/_dev2120_fixtures.py`: `CONTRACTOR_COLUMNS`, `LIST_COLUMNS`
- `tests/_dev2172_fixtures.py`: `PgResult.columns`
- `tests/_dev2210_fixtures.py`: `by_id(column)`
- `tests/_dev2219_fixtures.py`: `column`
- `tests/_gateway_harness.py`: `RecordingClient.get_result_column_types`, `RecordingClient.reflect_columns`
- `tests/_reagg_outer_grain_fixtures.py`: `col(columns)`
- `tests/_reserved_alias_probe.py`: `unquoted_alias_failures(column)`
- `tests/dialects/test_bigquery.py`: `_EchoTypesClient.get_result_column_types`
- `tests/integration/test_integration_flight.py`: `_columns_of`
- `tests/test_catalog_read_recovery.py`: `_PoisoningInspector.get_columns`
- `tests/test_connection_default_schema.py`: `_LayoutClient.reflect_columns`
- `tests/test_consecutive_periods_calendar.py`: `_ticks_columns`
- `tests/test_default_result_order.py`: `_column`
- `tests/test_dev1712_order_only_hidden_slots.py`: `_outer_select_columns`, `_outer_order_by_columns`
- `tests/test_dev1713_naming.py`: `_outer_select_columns`
- `tests/test_dev1733_order_only_transform_composite.py`: `_outer_select_columns`
- `tests/test_dev1966_query_backed_inline.py`: `_assert_parity(columns)`
- `tests/test_dev2015_custom_granularities.py`: `_series(column)`
- `tests/test_dev2015_transforms.py`: `_rows(column)`
- `tests/test_dev2021_profiling.py`: `_COLUMNS`
- `tests/test_dev2172_honest_catalogs.py`: `_ArrowColumn`, `_ArrowTable.column`, `_flight_columns`
- `tests/test_dev2175_importers.py`: `readers_of(column)`
- `tests/test_dev2277_one_resolver.py`: `column`
- `tests/test_dev2277_stored_definitions.py`: `column`
- `tests/test_dialect_reflection_hooks.py`: `_ColumnsRaisingInspector`, `_ColumnsRaisingInspector.get_columns`, `_is_info_schema_columns`
- `tests/test_engine_client_factory.py`: `ProfileClient.get_result_column_types`, `ProfileClient.reflect_columns`
- `tests/test_idempotent_ingestion.py`: `_create_probe_workspace_db(column)`
- `tests/test_importer_source_names.py`: `_ProbeRecorder.get_result_column_types`, `_probe_statements(columns)`, `_Collected.column`
- `tests/test_ingestion.py`: `_Inspector.get_columns`, `_columns_connection`, `_reflect_columns`, `_fk(column)`
- `tests/test_ingestion_clickhouse.py`: `_InspectorClient.reflect_columns`, `_introspect_columns`, `_introspect_columns(columns)`
- `tests/test_ingestion_comments.py`: `_CommentConnection.__init__(columns)`, `_CommentConnection._columns`, `_ColumnsRaisingInspector`, `_ColumnsRaisingInspector.get_columns`, `_fallback_comments(columns)`, `_mock_inspector(columns)`, `_HookClient.reflect_columns`
- `tests/test_ingestion_schema_qualification.py`: `_duck_columns`
- `tests/test_law_database_door.py`: `SnowflakeListClient.get_result_column_types`, `SnowflakeListClient.reflect_columns`
- `tests/test_mcp_server.py`: `TestDataProfileRetryScope._CountResult.columns`
- `tests/test_osi_export_document.py`: `_FailingClient.get_result_column_types`, `_FailingClient.reflect_columns`
- `tests/test_ossie_vocabulary_response_attributes.py`: `METRIC_COLUMN`
- `tests/test_projection_trim.py`: `_outer_select_columns`
- `tests/test_result_column_type_drivers.py`: `_DESCRIBE_COLUMNS`
- `tests/test_result_readers.py`: `_Result.__init__(columns)`, `_Result._columns`
- `tests/test_retype_finding.py`: `_YearGateway.reflect_columns`
- `tests/test_save_time_sql_validation.py`: `_CapturingClient.get_result_column_types`
- `tests/test_sql_client_conformance.py`: `_EmptyForMissing.reflect_columns`, `_StatementsOnly.get_result_column_types`, `_IntegersAsDouble.get_result_column_types`, `_Probe160Only.get_column_types`
- `tests/test_sql_client_contract.py`: `_Unavailable.get_result_column_types`, `_Unavailable.reflect_columns`
- `tests/test_sqlite_catalog_reads.py`: `_inspector_columns`
- `tests/test_sqlite_memories_pk_migration.py`: `_id_column_type`
- `tests/test_sqlite_probe.py`: `_insert_typed(column)`
- `tests/test_table_names.py`: `_SnowflakeDatabaseClient.reflect_columns`
- `tests/test_validate_models.py`: `_live_orders(columns)`, `_create_sqlite_with_mixed_storage(column)`

### Session-policy (RLS) name

- `tests/_dev2189_fixtures.py`: `_join`, `_join(joins)`, `stub_has_column`, `stub_has_column(column)`, `stub_column_present`, `stub_column_present(column)`, `reference_scoped_sql(has_column)`
- `tests/integration/test_integration_rls.py`: `rls_join_storage`, `_join_policy`
- `tests/test_client_policy.py`: `_join_policy`
- `tests/test_dev2189_rls_parity.py`: `_join_policy`, `_join_policy(joins)`
- `tests/test_dev2210_model_filter_rls.py`: `COLUMN_POLICY`, `JOIN_POLICY`
- `tests/test_policy_models.py`: `_join_rule`, `_join_ruleset`
- `tests/test_session_policy.py`: `_apply(has_column)`, `has_column_factory`, `_boom_probe(column)`, `_join_ruleset`
- `tests/test_session_policy_engine.py`: `_patch_columns`, `_join_policy`, `join_engine`

### Storage layout or stored-document version key

- `tests/_library_names_fixtures.py`: `PREVIOUS_MODEL_VERSION`
- `tests/_stored_upgrade_fixtures.py`: `yaml_model_path`
- `tests/integration/test_notebooks.py`: `JAFFLE_MODELS_DIR`
- `tests/test_dev1743_migration.py`: `models_mod`
- `tests/test_dev2021_stored_conversion.py`: `_MODEL_V`
- `tests/test_dev2216_stored_mode_migration.py`: `PREVIOUS_MODEL`
- `tests/test_dev2277_one_resolver.py`: `PRE_MODEL`
- `tests/test_dev2277_stored_upgrade_repairs.py`: `MODEL_V`, `PRE_MODEL`
- `tests/test_storage_type_refinement.py`: `V4Store.model_path`, `storage_with_v4_model`
- `tests/test_v4_migration.py`: `_create_legacy_sqlite_models_table`
- `tests/test_yaml_storage_cache.py`: `_core_models`, `_write_legacy_model`

### Upgrade path: old stored shape or old input spelling

- `tests/_granularity_rename_fixtures.py`: `events_doc(column_granularity)`
- `tests/_stored_upgrade_fixtures.py`: `orders_v10(joins)`, `funcstyle_rank_order(raw_formula)`, `raw_store(models)`
- `tests/fixtures/upgrade/v0_10_2/generate.py`: `_stage_model`, `_date_range_model`, `_filter_model`, `_over(measures)`, `_over(model)`
- `tests/fixtures/upgrade/v1_8_2/generate.py`: `_STORED_MEASURES`, `_over(measures)`, `_over(model)`
- `tests/test_dev1853_migration.py`: `_customers_v9(joins)`, `_orders_v9(joins)`, `_fwd(join_type)`, `_rev(join_type)`
- `tests/test_dev1902_join_key_migration.py`: `_customers(joins)`, `_orders(joins)`
- `tests/test_dev1920_stored_colon_upgrade.py`: `_model`, `MODEL_MEASURES`, `_measures`, `_measures(formulas)`, `KEYED_MEASURES`
- `tests/test_dev2021_queries_carry_no_mode_a.py`: `_ext_column`, `_ext_column(column)`
- `tests/test_dev2021_stored_conversion.py`: `_ext(columns)`, `_conversion_warnings(column)`, `_no_extension_columns`
- `tests/test_dev2101_stored_upgrade.py`: `_model_doc`, `_model_doc(formula)`, `_model_formula`
- `tests/test_dev2102_stored_upgrade.py`: `_assert_no_extension_columns`
- `tests/test_dev2175_stored_upgrade.py`: `store_over(models)`
- `tests/test_dev2215_stored_upgrade.py`: `_COLUMNS`, `store(models)`
- `tests/test_dev2277_stored_migration.py`: `OLD_RATIO_COLUMN`, `store(models)`
- `tests/test_dev2277_stored_upgrade_repairs.py`: `store(models)`
- `tests/test_dev2280_stored_upgrade.py`: `COLUMNS`, `store(models)`
- `tests/test_granularity_name_migration.py`: `_store(models)`, `MODEL_POSITIONS`
- `tests/test_osi_parser.py`: `_legacy_model`
- `tests/test_ossie_shapes_stored.py`: `pre_dataset(columns)`
- `tests/test_ossie_vocabulary_fresh_input.py`: `assert_measures_reported`
- `tests/test_rank_direction_migration.py`: `_model_dict`, `_model_dict(measures)`, `_migrated_formula`, `_migrated_formula(formula)`
- `tests/test_stored_upgrade_distinct_and_gate.py`: `_COLUMNS`, `_model_doc`, `_model_doc(measures)`, `_formulas`
- `tests/test_stored_upgrade_refinement_gate.py`: `_items(columns)`
- `tests/test_stored_upgrade_spine_date_only.py`: `model_doc`, `model_doc(columns)`, `model_doc(joins)`, `store(models)`, `add_other_datasource(models)`, `seed_other(models)`
