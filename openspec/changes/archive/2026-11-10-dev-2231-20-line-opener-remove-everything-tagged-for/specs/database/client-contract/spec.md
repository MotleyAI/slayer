## REMOVED Requirements

### Requirement: The 1.5.0 category probes keep working and warn
**Reason**: Scheduled for removal in SLayer 2.0, with the dialects' 1.6.0 classification tables they read.
**Migration**: Call `get_result_column_types` (or `get_result_column_types_sync` over an engine; `SlayerQueryEngine.get_result_column_types` per model), which returns each column's SLayer type and database type name.

### Requirement: A 1.6.0 client's category probe keeps working and warns
**Reason**: Scheduled for removal in SLayer 2.0; a client whose only probe is `get_column_types` is no longer adapted, and `DelegatingSqlClient.get_column_types` is gone.
**Migration**: Implement `get_result_column_types` on the client; a `DelegatingSqlClient` subclass inherits it from the wrapped client.

### Requirement: A client written before explain keeps working and warns
**Reason**: Scheduled for removal in SLayer 2.0; a client without `explain` is no longer run through its statement execution, and the dialect's single-text EXPLAIN builder is gone.
**Migration**: Implement `explain(statements)` on the client; a `DelegatingSqlClient` subclass inherits it from the wrapped client. Call the dialect's `explain_statements()` instead of `build_explain_sql()`.

## MODIFIED Requirements

### Requirement: The client contract is executable

SLayer SHALL ship a conformance suite that runs against any client over a database loaded with SLayer's fixture and reports every failed check by name. It SHALL check statement round-trips (rows, column types, and literals containing `:name` and `%` reaching the database verbatim), explain (plan rows returned for a read-only fixture query, and the client's next statement returning data rows), every metadata operation against the fixture's known facts, the missing-object failure and unchanged statement errors, and lifecycle (usable after an asynchronous close; a synchronous close is idempotent). Checks for metadata a database cannot represent SHALL be capability-gated rather than failed. The suite SHALL NOT require a test framework to run.

#### Scenario: The default client conforms on every local backend
- **WHEN** the suite runs against the default client on SQLite, DuckDB and Postgres, each loaded with the fixture
- **THEN** every applicable check passes, and checks a backend cannot represent are reported as not applicable

#### Scenario: A broken client is caught
- **WHEN** the suite runs against a client that reports a missing table as having no columns, against one that cannot run statements after an asynchronous close, against one without `explain`, and against one whose explain leaves its session returning plan rows
- **THEN** the report names each failed check

#### Scenario: A statements-only delegating client conforms
- **WHEN** the suite runs against a delegating client that overrides only statement execution and column-type probing
- **THEN** every applicable check passes

#### Scenario: The conformance suite checks the current contract
- **WHEN** `run_conformance` is given a client serving only the 1.6.0 `get_column_types`
- **THEN** its `get_result_column_types` check fails

