## 1. Tests first (pr-tests stage)

- [ ] 1.1 Write failing tests for every scenario in `specs/sql/client-contract/spec.md` (SQLite / DuckDB / Postgres live via the local engines and `pytest-postgresql`; MySQL, Snowflake, SQL Server hooks via description-row and fake-cursor unit tests; SQL Server describe + fallback classifier via fakes; live legs in the existing integration files) and verify each fails for the stated reason
- [ ] 1.2 Write failing tests for `specs/sql/execution/spec.md` (sync URL derivation for asyncpg / aiomysql URLs; a live Postgres ping, reflect and probe over a `postgresql+asyncpg://` datasource) and verify they fail
- [ ] 1.3 Write failing tests for `specs/sql/statement-assembly/spec.md` (BIGINT target per dialect snapshot, no double cast, live values above 2³¹ on DuckDB and Postgres; fixed-purpose INT casts unchanged) and verify they fail
- [ ] 1.4 Write failing tests for the new native-type-preservation scenario (row-level derived `NUMERIC(38,0)` column, value above 2⁶³ on Postgres; SQLite keeps its cast) and verify they fail
- [ ] 1.5 Write failing tests for the OSI scenarios, tighten `tests/test_osi_metrics.py::test_operand_outside_the_formula_language_falls_back_to_a_hidden_column` to `helper.type == DataType.INT` and the other loose `in (INT, DOUBLE)` OSI assertions the probe now settles, and verify they fail
- [ ] 1.6 Write failing tests for the deprecated 1.5.0 wrappers (category values + `DeprecationWarning`) and for name-mapper parity (the same type name gives the same `DataType` through catalog strings, SA types and the probe, incl. `DATE32`, `UInt8`, containers, opaque allow-list, SQL Server `timestamp`), and verify they fail
- [ ] 1.7 Migrate existing category-string assertions and fakes (`tests/test_sql_client.py`, `test_sql_client_in_memory_async.py`, `test_dev1933_verbatim_execution.py`, `test_clickhouse_statement_timeout.py`, `test_statement_timeout_client.py`, `test_gateway_harness.py`, `test_sql_client_snowflake.py`, `tests/dialects/test_snowflake.py`, `test_mcp_server.py`, `test_save_time_sql_validation.py`, `test_sql_generator.py`, mode-A/B variable tests, the integration files that assert categories) to the typed probe or to the deprecated wrapper, mechanically only

## 2. Types and the shared name mapper

- [ ] 2.1 Add `ResultColumnType` to `slayer/sql/dialects/reflection.py` and re-export it from `slayer.sql.client`; verify imports resolve and `la-arch-check` stays green
- [ ] 2.2 Merge `_INFO_SCHEMA_TYPE_MAP` and `_SA_TYPE_MAP` into one name table behind the catalog-string mapper, with wrapper peeling, dialect/class overrides, container shapes, the opaque allow-list and exact numerics by scale; `_sa_type_to_data_type` reads the same table; verify the parity tests (1.6) and the existing reflection tests pass

## 3. Dialect-owned cursor descriptions

- [ ] 3.1 Replace `map_cursor_type_code` with `describe_cursor_column(description_row)` on `SqlDialect` (base: string code → name mapper) and move the Postgres OID table (with names and typmod scale) into `dialects/postgres.py`, shared by Redshift; verify the Postgres hook tests pass
- [ ] 3.2 Move the MySQL / MariaDB field-code table into `dialects/mysql.py` with the length rules for `TINY` / `BIT` and `DECIMAL` scale; verify the MySQL hook tests pass
- [ ] 3.3 Rework `dialects/snowflake.py`'s hook to `FieldType` + precision/scale with `NUMBER(p,s)` names; verify the Snowflake tests pass
- [ ] 3.4 Add the tsql describe hook (`EXEC sp_describe_first_result_set` AST from an `exp.National` literal), the 11500–11541 fallback classifier and the pyodbc Python-type fallback; delete `_ODBC_SQL_TYPE_MAP`; verify the tsql unit tests pass
- [ ] 3.5 Move the SQLite `typeof`-count statement and verdict into `dialects/sqlite.py` as AST, used by both the result probe and `sqlite_introspect.probe_sqlite_integer_column` (rebuilt as AST); verify the SQLite probe tests and the existing affinity-probe tests pass

## 4. Client and engine plumbing

- [ ] 4.1 Rewrite `_extract_types_from_cursor` into the per-column typed reader (hook → base mapper → TEXT; generic first-row fallback), add `get_result_column_types` on the `SqlClient` protocol, `SlayerSQLClient`, `DelegatingSqlClient`, and `get_result_column_types_sync`; the `type_probe_uses_sync_driver` flag routes Postgres/Redshift probes through the sync engine; verify the client-contract tests pass
- [ ] 4.2 Build the synchronous engine for an async-only driver URL on the type's default sync driver in `engine_factory`; verify the `sql/execution` tests pass
- [ ] 4.3 Add `SlayerQueryEngine.get_result_column_types(model_name, data_source=None)`; keep `get_column_types_sync`, `SlayerSQLClient.get_column_types` and `SlayerQueryEngine.get_column_types` as deprecated category wrappers over one `DataType`→category function, tagged for 2.0 removal; verify the wrapper tests pass
- [ ] 4.4 Update the conformance suite's column-type check to the typed probe; verify `tests/test_sql_client_conformance.py` passes on SQLite, DuckDB and Postgres

## 5. Rendering

- [ ] 5.1 Centralise the logical INT → `BIGINT` cast target in `slayer/sql/render/value_expr.py` for insertion and idempotence; verify the statement-assembly tests pass
- [ ] 5.2 Set native-type preservation for row-level derived columns with an exact-numeric `db_type` (non-explicit type; SQLite keeps its cast); verify the preservation tests pass
- [ ] 5.3 Re-bless the golden SQL baselines, reviewing that every diff is only `AS INT` → `AS BIGINT` or a dropped inferred cast around an exact-numeric derived column

## 6. Consumers

- [ ] 6.1 `schema_drift`: delete `_CURSOR_CATEGORY_TO_DATATYPE`, read `.type` from the typed probe; verify the drift tests pass
- [ ] 6.2 OSI importer: SQL-mode columns, derived fields and hidden helpers take `.type` and store `db_type` when exact-numeric or opaque; verify the OSI tests pass
- [ ] 6.3 `model_render._choose_sample_agg` reads the typed probe; verify the inspect tests pass

## 7. Docs and gates

- [ ] 7.1 Update `docs/configuration/sql-clients.md` (contract row, delegating example) and add one sentence to the schema-drift docs on type-mismatch entries for old OSI imports; verify the docs nav check and doc-example tests pass
- [ ] 7.2 Run the full unit suite, `ruff check slayer/ tests/`, `basedpyright` (no new errors vs the baseline) and `la-arch-check`; all green
- [ ] 7.3 Run the integration suite with the CI invocation; all green on the locally runnable backends
