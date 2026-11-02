## 1. Tests first (pr-tests stage)

- [x] 1.1 Write failing tests for every scenario in `specs/sql/execution/spec.md`, `specs/sql/client-contract/spec.md` and `specs/models/stored-upgrade/spec.md`; verify each fails for the right reason on the current code (unit tests for SQLite/DuckDB/pytest-postgresql; Snowflake/ClickHouse/BigQuery scenarios under their integration files and markers)
- [x] 1.2 Write `tests/test_law_database_door.py` (design decision 17) and verify it lists today's direct-access sites outside the trusted boundary
- [x] 1.3 Build the routing harness: a full-gateway fake `SqlClient` over a fixture plus a fixture that patches `engine_factory.get_engine`, `sa.inspect` and raw driver connects to raise; verify a plain engine query fails under it today
- [x] 1.4 Codex-review the tests against the plan; resolve findings with the user

## 2. Client contract (`slayer/sql/`)

- [x] 2.1 Add the `SqlClient` protocol, typed errors (`SqlClientError`, `DatabaseUnavailable`, `AuthenticationRefused`, `ObjectNotFound`), `ObjectListing`, and narrow `ReflectedColumn.type` to `DataType`; verify basedpyright shows `SlayerSQLClient` satisfying the protocol
- [x] 2.2 Move the SA type-mapping helpers from `engine/ingestion.py` to `sql/dialects/reflection.py`; verify "Default reflection keeps today's types" and the existing ingestion suites pass
- [x] 2.3 Implement the metadata methods on `SlayerSQLClient` over the dialect hooks (worker thread), with exception mapping to typed errors; verify the client-contract typed-failure and dialect scenarios
- [x] 2.4 Rebuild the SQLite integer-affinity probe on `execute` with the identical algorithm; verify the SQLite sampling-rules scenario
- [x] 2.5 Add `DelegatingSqlClient`; verify the delegating-client scenarios
- [x] 2.6 Add `slayer/sql/conformance.py` (`run_conformance`, per-dialect fixtures, capability gating); verify conformance passes on SQLite, DuckDB and Postgres and catches the broken clients

## 3. Engine construction and ownership

- [x] 3.1 `SlayerQueryEngine(client_factory=...)`, `ClientFactory` alias, cache key `(datasource name, connection identity)`; verify the one-client-per-identity and factory-object scenarios
- [x] 3.2 Remove the borrower `aclose()` in `_detect_datasource_joins`; verify "Join profiling leaves the shared client open"
- [x] 3.3 Add `SlayerQueryEngine.run_sync` (cancellation-safe cleanup) and route `execute_sync` through it; verify "Synchronous calls repeat safely" and "Closing the engine reaches factory clients"
- [x] 3.4 Async RLS: pre-resolve column presence through the client, then the pure sync rewrite; verify the policy scenarios (present, absent, unavailable, not found) and the existing forced-filter suites

## 4. Thread the client through every live-access site

- [x] 4.1 Drift: `validate_datasource` and helpers, `_LiveConnection`, `DriftSnapshot`, SQLite drift probes take a client; delete `_sql_client_for`; listing-based deletion only; verify the drift-verdict scenarios, "Validation before the first query cannot bypass the factory", and the existing schema-drift suites (tests updated mechanically to pass an engine-owned client)
- [x] 4.2 `schema_scope` / `introspect_utils`: live reads via the client, pure scope logic kept; verify the BigQuery/DuckDB dialect scenarios and existing scope suites
- [x] 4.3 Ingestion (`ingest_datasource_report`, `ingest_datasource_idempotent`, `list_ingestable_objects`, `introspect_table_to_model`, `_get_schemas`, …) via the client; verify the gateway ingestion scenario and existing ingestion suites
- [x] 4.4 dbt and OSI converters: async metadata collection + pure sync conversion; update CLI/API callers; verify the gateway import scenarios, incl. a call from a thread running an event loop
- [x] 4.5 MCP (`_test_connection`, `_fetch_tables`, create/describe/ingest tools) and REST through the server's engine; verify the MCP loop-responsiveness and gateway scenarios
- [x] 4.6 CLI: every command runs its whole workflow in one `engine.run_sync` (incl. `validate-models --cardinality`, `datasources test`, `ingest`, `import-dbt`, `import-osi`, `storage migrate-types`); verify the CLI scenario on Postgres and existing CLI suites
- [x] 4.7 Verify the door law test passes and the routing harness shows no direct access across all R1 scenarios

## 5. Storage refinement (S3)

- [x] 5.1 The engine's storage view supplies the client source to storage loads through a context variable; refinement reads only the model's table through it; "entry present, no source" raises the typed error; verify the stored-upgrade scenarios
- [x] 5.2 Non-owning per-engine storage view exposed as `engine.storage`; verify two engines sharing one storage survive one engine closing
- [x] 5.3 Route CLI/MCP/REST/service model loads through `engine.storage`; verify the 0.10.2 corpus scenario through raw and engine storage

## 6. Entry points

- [x] 6.1 `SlayerClient` local mode accepts `client_factory`, remote mode rejects it; verify the two `SlayerClient` scenarios
- [x] 6.2 `create_app` (both engines), `create_mcp_server`, pg-facade and Flight builders accept and pass the factory; verify "Server builders pass the factory to every engine"

## 7. Architecture and docs

- [x] 7.1 Apply the approved `python.sql.arc42.md` §3.13 wording and `system.arc42.md` §3.17 verbatim (design.md); verify `poetry run la-arch-check` and `npx -y likec4@1.47.0 validate architecture` pass
- [x] 7.2 Write `docs/configuration/sql-clients.md` (contract, minimal gateway, full gateway, entry points, conformance) and add it to the `zensical.toml` nav; verify the docs build lists it
- [x] 7.3 One sentence each on `client_factory` in `docs/reference/python-client.md`, `docs/interfaces/python-client.md`, `docs/getting-started/python.md`; verify every new example snippet runs

## 8. Gates

- [x] 8.1 Full unit suite `poetry run pytest -m "not integration"` green
- [x] 8.2 Integration suite (CI invocation) green; Snowflake/ClickHouse/BigQuery scenarios green in their path-gated workflows
- [x] 8.3 `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new errors vs baseline) clean
- [x] 8.4 After merge, ask the user whether to close DEV-2117 as done by this change
