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

SLayer SHALL ship a conformance suite that runs against any client over a database loaded with SLayer's fixture and reports every failed check by name. It SHALL check statement round-trips (rows, column types, and literals containing `:name` and `%` reaching the database verbatim), every metadata operation against the fixture's known facts, the missing-object failure and unchanged statement errors, and lifecycle (usable after an asynchronous close; a synchronous close is idempotent). Checks for metadata a database cannot represent SHALL be capability-gated rather than failed. The suite SHALL NOT require a test framework to run.

#### Scenario: The default client conforms on every local backend
- **WHEN** the suite runs against the default client on SQLite, DuckDB and Postgres, each loaded with the fixture
- **THEN** every applicable check passes, and checks a backend cannot represent are reported as not applicable

#### Scenario: A broken client is caught
- **WHEN** the suite runs against a client that reports a missing table as having no columns, and against one that cannot run statements after an asynchronous close
- **THEN** the report names each failed check

#### Scenario: A statements-only delegating client conforms
- **WHEN** the suite runs against a delegating client that overrides only statement execution and column-type probing
- **THEN** every applicable check passes

### Requirement: Column-type probes report SLayer types and database type names

A client's column-type probe SHALL return, for every result column of a statement, a SLayer data type and the database's own name for the column's type, without fetching the statement's data beyond what type certification needs. The type SHALL be the one reflection gives the same database type: integer types INT; exact-numeric types INT at scale 0 and DOUBLE at a positive or unknown scale; floating types DOUBLE; character types TEXT; boolean types BOOLEAN; date types DATE; timestamp types TIMESTAMP; container types and unrecognised types as reflection maps them; and only the types reflection treats as opaque UNKNOWN. A probe SHALL NOT report INT for a column it has not proven integer: on a database whose cursor reports no types, a number is INT only when every value in a sample of at most 100 000 rows is an integer and the sample is not saturated, and DOUBLE otherwise. The database type name SHALL be present whenever the driver names the type or reports a code SLayer can name, carrying precision and scale for an exact-numeric type when the database reports them, and SHALL be absent only when the driver supplies no type name. The probe's answer for a datasource SHALL NOT depend on whether it was requested from synchronous or asynchronous code.

#### Scenario: Integer results are INT
- **WHEN** a statement selecting an integer column, the length of a text column, and a count is probed on SQLite, DuckDB and Postgres
- **THEN** every column is INT, and on DuckDB and Postgres each carries the database's integer type name

#### Scenario: Exact numerics are typed by scale
- **WHEN** a statement selecting a `NUMERIC(10,0)` and a `NUMERIC(10,2)` column is probed on DuckDB and on Postgres
- **THEN** the first is INT and the second DOUBLE, and each type name carries its precision and scale

#### Scenario: Floats stay DOUBLE
- **WHEN** a statement selecting a double-precision column and an average is probed
- **THEN** both columns are DOUBLE

#### Scenario: Postgres answers the same from either path
- **WHEN** the same Postgres statement with integer, `NUMERIC(12,0)` and `NUMERIC(12,3)` columns is probed from asynchronous code and through the synchronous entry point
- **THEN** both answers are identical, including the type names

#### Scenario: SQLite certifies integers from the data
- **WHEN** SQLite statements are probed whose numeric column holds only integers, mixes integers and reals, holds more than 100 000 integer rows, holds integer-looking text, or returns no rows
- **THEN** the column is INT, DOUBLE, DOUBLE, TEXT and TEXT respectively, and no column carries a type name

#### Scenario: Containers are not integers
- **WHEN** a DuckDB statement selecting an `INTEGER[]` list and a struct with an integer field is probed
- **THEN** neither column is INT, and each is typed as reflection types the same database type

#### Scenario: Opaque and unrecognised types
- **WHEN** a Postgres statement selecting a `json`, a `jsonb`, a `uuid` and a user-defined enum column is probed
- **THEN** the `json` column is UNKNOWN, the other three are TEXT, and only the enum column has no type name

#### Scenario: SQL Server reports declared type names
- **WHEN** a SQL Server statement selecting `int`, `decimal(10,2)`, `datetime2` and `nvarchar(50)` columns is probed
- **THEN** the columns are INT, DOUBLE, TIMESTAMP and TEXT, each carrying the declared type name

#### Scenario: SQL Server falls back when it cannot describe a statement
- **WHEN** a SQL Server statement whose result SQL Server cannot describe without running it (a temporary table) is probed
- **THEN** the columns are still typed from the driver's values, carry no type name, and any other statement error reaches the caller unchanged

#### Scenario: MySQL boolean and tiny integers
- **WHEN** a MySQL statement selecting a `BOOL`, a `TINYINT`, a `BIT(1)` and a `BIT(8)` column is probed
- **THEN** the columns are BOOLEAN, INT, BOOLEAN and TEXT

### Requirement: The 1.5.0 category probes keep working and warn

The column-type probes released in 1.5.0 — the synchronous entry point over an engine, the default client's probe, and the query engine's per-model probe — SHALL keep returning one of `number`, `string`, `time` or `boolean` per column, derived from the typed probe (INT and DOUBLE as `number`; TEXT and UNKNOWN as `string`; DATE and TIMESTAMP as `time`; BOOLEAN as `boolean`), and SHALL emit a deprecation warning naming the typed replacement.

#### Scenario: A 1.5.0 caller
- **WHEN** code written for 1.5.0 probes a statement with an integer and a text column through the synchronous entry point
- **THEN** it receives `number` and `string` and a deprecation warning naming the typed probe
