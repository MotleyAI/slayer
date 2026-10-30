## 1. Failing tests (pr-tests stage)

- [x] 1.1 `tests/test_dev2137_list_columns.py`: metadata validation, ingestion mapping per dialect (BigQuery `REPEATED`, Postgres, DuckDB incl. the INFORMATION_SCHEMA fallback, ClickHouse wrappers, Trino, Snowflake structured and inferred, nested → `UNKNOWN`), eligibility errors with remedies (incl. custom aggregations and expression sources), `count(list)`, the NULL-element rule, the SQLite / MySQL / SQL Server refusals, re-ingest re-typing, inspect / search / summary rendering, and Snowflake `json_value` on `ARRAY` — the `models/list-columns` scenarios; verify they fail before implementation
- [x] 1.2 Library matrix: add list cases (`contains`, `contains_any`, `contains_all`, `length`, the array literal, `[]`, list variables, mixed-type coercion) to `tests/test_dev2120_library_matrix.py` with a list table in `tests/_dev2120_fixtures.py`, executed on DuckDB, refused on SQLite, emitted on every Tier-1 dialect; plus `tests/test_dev2137_list_functions.py` for the remaining `queries/list-functions` scenarios; verify they fail
- [x] 1.3 `tests/test_dev2137_list_children.py`: every `queries/list-children` scenario on DuckDB (keyless model, EXISTS filters, element dimension, broadcast warning, keyless association incl. identical parent rows and the list-plus-keyed mix keeping the key requirement, no fan-out, to-one chains, derived lists, stage lists incl. `collect`, save-time collisions from both sides, no self-join in the emitted SQL for every native-list dialect); verify they fail
- [x] 1.4 `tests/test_dev2137_collect.py`: every `aggregations/collect` scenario (distinct sorted, mixed and all-NULL, eligibility, list-function operands, stage use, refusal as a dimension, MCP JSON list, emission per dialect); verify they fail
- [x] 1.5 `tests/test_dev2137_facade_lists.py`: catalog keeps list models; JSON text over pg wire, Flight and the JDBC path, incl. escaping, booleans, dates and NULL; verify they fail
- [x] 1.6 `tests/test_dev2137_schema_drift.py` and `tests/test_dev2137_profiling.py`: the re-type finding (validation, query-time `SchemaDriftError`, sql-mode models) and element profiling (text, numeric, opaque skipped); verify they fail
- [x] 1.7 Element-type threading: multi-stage tests proving `element_type` survives stages, re-rooted producers and result metadata; verify they fail
- [x] 1.8 Integration: Postgres (pytest-postgresql), ClickHouse and Trino suites execute the list-function, list-child and `collect` cases live; BigQuery and Snowflake secret-gated suites likewise; verify they fail where the backend is available

## 2. Types and metadata

- [x] 2.1 Add `DataType.ARRAY` and `Column.element_type` with validation (D1), and extend `TestDataTypeValuesMatchSqlglot`; verify the metadata tests in 1.1 pass
- [x] 2.2 Thread `element_type` through `StageColumn`, `ValueSlot`, bound key metadata, `aggregation_result_type` and the other type-inference functions (D1); verify 1.7 passes

## 3. Ingestion and migration

- [x] 3.1 Map native list types per dialect, with `db_type` kept and the DuckDB INFORMATION_SCHEMA fallback parsing `T[]` (D10); verify the ingestion tests in 1.1 pass
- [x] 3.2 Snowflake untyped `ARRAY` element inference at ingest, re-inferred only while `UNKNOWN` (D10); verify its tests pass
- [x] 3.3 Re-ingest re-types `TEXT` list columns, keeping metadata (D11); verify the re-type tests in 1.1 pass
- [x] 3.4 Schema drift: re-type finding kind in validation and `SchemaDriftError`, and list-aware sql-model live typing (D11); verify 1.6 drift tests pass
- [x] 3.5 Replace the unmapped-type fallback warning text (D10); verify ruff passes

## 4. Eligibility and dialect support

- [x] 4.1 List-type checks in the checker: not a dimension, not comparable, `IS NULL` allowed, one list-type gate on every aggregation node's typed source, every error naming its remedy (D8); verify the eligibility tests pass
- [x] 4.2 Save-time `allowed_aggregations` check on list columns; verify its test passes
- [x] 4.3 `SqlDialect` list support flag and one plan-time refusal for every list construct on SQLite, MySQL and SQL Server (D15); verify the refusal tests pass

## 5. List functions and the array literal

- [x] 5.1 Grammar and the array-literal key kind with `children()` / `map_children()`, `[]` typed by context, list variables (D4); verify the literal tests pass
- [x] 5.2 Overloaded `contains` / `length` and new `contains_any` / `contains_all` in the library, typing and coercion of arguments (D3); verify the typing tests pass
- [x] 5.3 `ArrayFn` dialect hooks with explicit lowerings per Tier-1 native-list dialect, NULL elements ignored (D2, D3); verify the library matrix (1.2) passes

## 6. List children

- [x] 6.1 Lateral keyless edge kind in the join walker, `L.value` resolution, `count(L.*)`, opaque elements refused (D5); verify the resolution tests in 1.3 pass
- [x] 6.2 Emission: lateral unnest hop, correlated EXISTS (ClickHouse `arrayExists`), producers rooted at the child with no join back, one AST dialect hook per native-list dialect (D5); verify the filter, dimension, no-fan-out and no-self-join tests pass
- [x] 6.3 Keyless association across list hops, key requirement kept for other to-many hops (D6); verify the association tests pass
- [x] 6.4 Composition through to-one chains, derived lists and stage lists; verify those tests pass
- [x] 6.5 Save-time collision check from both sides (D12); verify the collision tests pass
- [x] 6.6 Flip `semantics.arc42.md` Axiom 3.2 `[target: DEV-2137]` → `[enforced: test:tests/test_dev2137_list_children.py]` (approved); verify `poetry run la-arch-check` passes

## 7. collect

- [x] 7.1 `collect` built-in with the list value class, the `build_collect` hook with explicit NULL filtering per dialect, empty value the empty list (D7); verify 1.4 passes

## 8. Results, facades, profiling and the MCP surface

- [x] 8.1 Normalise driver arrays to lists at result fetch; JSON text in CSV / markdown (D9); verify the MCP JSON scenario passes
- [x] 8.2 Facade catalogs map `ARRAY` to text; one shared JSON serialiser for pg wire, Flight and JDBC (D9); verify 1.5 passes
- [x] 8.3 Element profiling through the child (D13); verify the profiling tests pass
- [x] 8.4 Rendering in `inspect`, `search` and `models_summary` with `element_type`, element samples and the operation hint (D14); verify the rendering test passes
- [x] 8.5 One clause each in the `filters` and `to_many_handling` field descriptions; verify `tests/test_mcp_surface_budget.py` passes

## 9. Docs and help

- [x] 9.1 `docs/concepts/models.md` (list type, element type, eligibility, re-ingest), `references.md` / `formulas.md` (list functions, array literal, `collect`), `queries.md` (element access, `associate` histogram, `contains_all` vs single-filter `AND`, `NOT contains` for absence), `docs/database-support.md` (native-list backends, BigQuery NULL lists); verify every touched page is in the `zensical.toml` nav
- [x] 9.2 `help.queries` "Functions" and `help.joins` list-child section (D14); verify the help-memory tests pass

## 10. Comparison matrix

- [x] 10.1 Add `customers.tags VARCHAR[]` with the edge cases to `examples/comparisons/dataset.sql` and each engine's model files; verify every existing probe still matches with `examples/comparisons/run_all.sh`
- [x] 10.2 Add the Q27 probes to `probes.yaml` for SLayer, Malloy, Cube and MetricFlow (D16); verify each engine's runner reports the Q27 results
- [x] 10.3 Add row Q27 to `matrix.yaml` with verdicts and caveats from the probe results, and regenerate `matrix.json` with `export_matrix.py`; verify the export is current

## 11. Verification

- [x] 11.1 Run the full unit suite (`poetry run pytest -m "not integration"`), the CI integration invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors) and `poetry run la-arch-check`; all green
