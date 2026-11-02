# sql/client-contract Specification

## Purpose
Defines the contract every SQL client — the default one or a host-supplied gateway — must meet: statement and metadata semantics, typed failures, delegation, and an executable conformance suite that proves an implementation honours it.

## Requirements

### Requirement: Client failures are typed

A client SHALL report an unreachable database as an unavailable failure, rejected credentials as an authentication failure, and a metadata read of a schema or table that does not exist as a not-found failure. Any other statement failure SHALL reach the caller as the database's own error with its message unchanged. Schema drift SHALL treat an object as deleted only when it is absent from a successful listing of its schema; a failure reading a listed object, including a not-found failure, SHALL yield no verdict for it. A row-level-security column check that cannot be confirmed SHALL refuse the query.

#### Scenario: An unreachable client refuses validation
- **WHEN** models are validated through a client that reports its database unavailable
- **THEN** validation fails with an unavailable error and reports no model as deleted

#### Scenario: A table missing from the listing is drift
- **WHEN** a model's table is absent from a successful listing of its schema
- **THEN** validation reports the model's table as deleted

#### Scenario: A listed but unreadable table yields no verdict
- **WHEN** a model's table appears in the listing but reading its columns fails with a not-found failure, a permission error, or an unsupported-reflection error
- **THEN** validation reports no drift verdict for that model

#### Scenario: An unconfirmable policy column refuses the query
- **WHEN** a query runs under a column-scoped session policy and the client reports its database unavailable while checking the column
- **THEN** the query is refused

#### Scenario: A database error reaches the caller unchanged
- **WHEN** a statement fails in the database with a syntax error
- **THEN** the caller receives that error with the database's message unchanged

#### Scenario: Rejected credentials are reported as such
- **WHEN** the default client's database rejects its credentials
- **THEN** the client raises an authentication failure and discards its cached connections

### Requirement: Reflected metadata uses SLayer types

Column metadata returned by a client SHALL carry SLayer data types, a float flag, the database's type name where it is opaque or exact-numeric, and the column comment; it SHALL NOT carry driver-library type objects. Ingestion and drift SHALL produce the same model types and drift verdicts through any conforming client as through the default client over the same database.

#### Scenario: Default reflection keeps today's types
- **WHEN** a table with integer, exact-numeric, float, text, timestamp, boolean, UUID and opaque columns is ingested through the default client
- **THEN** the model's column types, formats and recorded database types are those ingestion produced before this change

### Requirement: The default client keeps each dialect's connection behaviour

The default client's metadata reads SHALL resolve locations, defaults and session state exactly as SLayer's introspection did before this change, for every dialect.

#### Scenario: Snowflake session overrides apply to metadata reads
- **WHEN** a Snowflake datasource configures a role, warehouse, database and schema, and the default client lists objects and reflects columns over several pooled connections
- **THEN** every connection used has the configured role, warehouse, database and schema applied

#### Scenario: BigQuery resolves bare names in the URL dataset
- **WHEN** a BigQuery datasource whose URL names a dataset is asked for its default schema while its project holds a second dataset
- **THEN** the default schema is the URL's dataset, and no request is made to resolve it

#### Scenario: DuckDB attached catalogs stay distinct
- **WHEN** a DuckDB datasource has an attached catalog holding a schema and table with the same names as ones in the main catalog, each with different columns and keys
- **THEN** reflecting each location returns that location's own columns and keys

#### Scenario: A ClickHouse server profile denying correlated subqueries is honoured
- **WHEN** the ClickHouse server profile reports correlated subqueries disabled, and a query needs a correlated EXISTS semi-join
- **THEN** the query is refused with the existing explanation

#### Scenario: In-memory SQLite is one database per owning engine
- **WHEN** two query engines use the same in-memory SQLite datasource, and one creates a table through its client
- **THEN** that engine's later metadata reads and queries see the table, and the other engine's do not

### Requirement: A delegating client forwards everything it does not override

A delegating client SHALL forward every client operation, including closing, to the client it wraps, so a subclass that overrides only some operations serves those and leaves the rest to the wrapped client. Operations of the wrapped client SHALL never be routed through the subclass's overrides.

#### Scenario: A statements-only gateway
- **WHEN** a delegating client over a default client overrides only statement execution and column-type probing, and an engine using it runs a query and validates its models
- **THEN** the query's statements reach the override, while metadata reads and the wrapped client's own internal statements go to the wrapped client

#### Scenario: Closing a delegating client closes the wrapped client
- **WHEN** a delegating client is closed
- **THEN** the wrapped client is closed

### Requirement: The client contract is executable

SLayer SHALL ship a conformance suite that runs against any client over a database loaded with SLayer's fixture and reports every failed check by name. It SHALL check statement round-trips (rows, column types, and literals containing `:name` and `%` reaching the database verbatim), every metadata operation against the fixture's known facts, the typed failures, and lifecycle (usable after an asynchronous close; a synchronous close is idempotent). Checks for metadata a database cannot represent SHALL be capability-gated rather than failed. The suite SHALL NOT require a test framework to run.

#### Scenario: The default client conforms on every local backend
- **WHEN** the suite runs against the default client on SQLite, DuckDB and Postgres, each loaded with the fixture
- **THEN** every applicable check passes, and checks a backend cannot represent are reported as not applicable

#### Scenario: A broken client is caught
- **WHEN** the suite runs against a client that reports a missing table as having no columns, and against one that cannot run statements after an asynchronous close
- **THEN** the report names each failed check

#### Scenario: A statements-only delegating client conforms
- **WHEN** the suite runs against a delegating client that overrides only statement execution and column-type probing
- **THEN** every applicable check passes
