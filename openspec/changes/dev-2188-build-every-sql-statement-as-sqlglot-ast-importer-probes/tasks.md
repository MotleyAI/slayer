## 1. Tests first (pr-tests)

- [x] 1.1 `tests/test_law_sql_built_as_ast.py`: the law per design.md (forms, keyword list, shape exemptions incl. the one-interpolation parse exemption); planted cases — f-string / `+` / `%` / `.format` SELECT each fail, each exemption shape passes; verify it FAILS on the current tree, listing every site in design.md's site table and nothing else (calibrate the shape against the tree; no file allowlist)
- [x] 1.2 Spec scenarios of `specs/importers/meaning-preservation/spec.md` as importer tests (dbt, cube, OSI) on SQLite, DuckDB and Postgres (pytest-postgresql; unit, no integration marker): mixed-case schema-qualified, reserved-word `order`, catalog-qualified (DuckDB attached catalog), quoted name containing a dot (kept separate from a genuine three-part name), expression probe over each, unreadable / non-read-only source SQL; verify each fails today (reserved-word base columns and the DuckDB catalog name already pass: regression guards)
- [x] 1.3 Row-bound matrix: `SqlDialect.type_probe` renders SQLite `LIMIT 1`, Postgres/DuckDB/others `LIMIT 0`, T-SQL `TOP 0` with no trailing `LIMIT`; the door accepts only `TypeProbe` (a raw `str` is a basedpyright error and a runtime refusal); `_described` runs exactly one statement
- [x] 1.4 `LiveSource.probe` over `sql_table=` and `sql=` sources with a non-literal expression composes `FROM <source>`; parse failure → `None`
- [x] 1.5 Engine model typing (`QueryEngine.get_column_types`) emits today's wrapper shape (`SELECT * FROM (...) AS _types LIMIT n` / `TOP`) with policy applied; save-time trial and `sql_result_types` still type sql-mode models
- [x] 1.6 Drift probe over a mixed-case / reserved-word source table still classifies drift vs broken SQL as today
- [x] 1.7 Facade catalog: execution test with several rows, mixed-case and quoted column names, values containing `?` and `'`; placeholders render as DuckDB `?` and values never appear in the SQL text
- [x] 1.8 Session-setting hooks: each dialect's `statement_timeout` / ClickHouse `correlated_setting_probe` / Snowflake `USE` renders the same statement text as today (modulo quoting); `test_law_ast_dialect_hooks.py` covers the renamed hooks
- [x] 1.9 SQLite introspection probes, own-store `?` queries, pg wire command tag and demo loader keep behaviour (existing tests green; add one where none covers the path)

## 2. Building blocks

- [x] 2.1 `parse_table` and `parse_query` in `slayer/sql/render/parse.py`; move the read-only root / modifying-node constants there and make `classify_model_sql` use the shared check; verify 1.2/1.4 parse cases and existing `classify_model_sql` tests pass
- [x] 2.2 `SQLGenerator._to_table` and `query_engine._side_stats_sql` use `parse_table` with their dialect; verify goldens unchanged or re-blessed for quoting only
- [x] 2.3 `SqlDialect.type_probe` + `type_probe_rows` (SQLite 1); delete `_NEEDS_ROW_FOR_TYPES` / `_TSQL_DB_TYPES` / `_build_type_probe_sql`; verify 1.3
- [x] 2.4 `TypeProbe`, `render_type_probe`, `sql_model_trial_query` in `client.py`; the type door (sync + async) takes `TypeProbe`, executes verbatim, one execution returns names + categories; delete `build_sql_model_trial_query`; verify 1.3
- [x] 2.5 `generate_planned_stages(..., type_probe=True)` wraps the composed AST via `dialect.type_probe` before its single render, settings attached to the outer statement; verify 1.5

## 3. Sites

- [x] 3.1 `LiveSource` (relation AST, `_described`, `probe`, `_reflect` via `SchemaRef`); `introspect_table_to_model` takes `ref: SchemaRef | None`, callers migrated; verify 1.2, 1.4
- [x] 3.2 OSI typing through `LiveSource`; delete `_probe_expression_type`'s twin; verify OSI tests + 1.2 OSI cases
- [x] 3.3 `schema_drift._sql_model_source_refs` yields AST nodes; probe via `render_type_probe`; verify 1.6 and drift tests
- [x] 3.4 `QueryEngine.get_column_types`, `_trial_execute_sql_source`, `ingestion.sql_result_types` migrated; verify 1.5
- [x] 3.5 `sqlite_introspect` probes as sqlite AST; verify SQLite ingestion tests
- [x] 3.6 `facade/catalog_sql.py` DDL / INSERT / empty subquery as AST; verify 1.7 and facade tests
- [x] 3.7 Dialect hooks `statement_timeout` / `correlated_setting_probe` return AST, client renders; Snowflake `USE` via `exp.Use`; verify 1.8
- [x] 3.8 pg wire tag typed (`encode_command_complete(command=, rows=)`); own-store queries with `exp.Placeholder`; demo loader as DuckDB AST; verify 1.9 and the demo notebook/tests

## 4. Close-out

- [x] 4.1 Apply the approved arc42 edit verbatim: system §3.6 → `[review] [enforced: test:tests/test_law_sql_built_as_ast.py]`; verify `poetry run la-arch-check`
- [ ] 4.2 Law green with no allowlist (1.1 passes); full unit suite, CI integration invocation, `ruff check slayer/ tests/`, `basedpyright` (no new errors) all green
- [x] 4.3 Docs: grep `docs/` for `get_column_types` / probe wording touched by the API change; update only user-facing text (one sentence each)
