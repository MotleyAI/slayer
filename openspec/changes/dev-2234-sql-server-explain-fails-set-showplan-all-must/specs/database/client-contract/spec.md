## ADDED Requirements

### Requirement: Explain runs the dialect's plan statements and restores the session

An explain query SHALL be expressed by its dialect as one plan statement — the statement whose rows are the plan — and, where the dialect's plan mode is session state, a statement switching that mode on and one switching it off; the session statements SHALL be built as SQL AST. SQL Server's plan statement SHALL be the bare query between `SET SHOWPLAN_ALL ON` and `SET SHOWPLAN_ALL OFF`; every other dialect SHALL keep its single EXPLAIN statement, the dialect's EXPLAIN keywords before the query. The engine SHALL run an explain query only through the client's explain operation, never through statement execution. A client's explain operation SHALL run the statements on one connection, in order, each verbatim and each as its own batch, fetch the plan rows in full, and return them; it SHALL run the switch-off statement whether the plan statement succeeds, fails or is cancelled. A connection whose switch-off is not confirmed SHALL be discarded, never reused. A failure of the plan statement SHALL reach the caller as statement execution reports it (typed, else the database's own error unchanged), taking precedence over a switch-off failure; a transient failure SHALL be retried on a fresh connection, as statement execution retries. A dialect without an EXPLAIN SHALL refuse an explain query with an explain-unsupported error before any database access.

#### Scenario: SQL Server returns its plan
- **WHEN** a query on a SQL Server datasource is executed with `explain=True`
- **THEN** the response's rows are SQL Server's plan rows, carrying a `StmtText` column, and the response's SQL is the query's SQL

#### Scenario: Single-statement dialects keep their EXPLAIN
- **WHEN** a query is explained on SQLite, DuckDB and Postgres
- **THEN** each database receives exactly one statement, its EXPLAIN keywords before the query's SQL, and the response's rows are the plan it returns

#### Scenario: The session is restored after a plan
- **WHEN** a SQL Server query is explained over a pool holding one connection, and a query then runs on the same datasource
- **THEN** the query returns its data rows, not plan rows

#### Scenario: The session is restored after a failing plan statement
- **WHEN** a SQL Server explain's plan statement fails in the database, and a query then runs over the same single pooled connection
- **THEN** the explain raises the database's error unchanged, and the query returns its data rows

#### Scenario: An unconfirmed switch-off discards the connection
- **WHEN** an explain's switch-off statement fails, or the explain is cancelled while its plan or switch-off statement runs
- **THEN** the connection that ran it is discarded and never serves another statement, and when the plan statement also failed its error is the one raised

#### Scenario: A transient failure is retried on a fresh connection
- **WHEN** an explain's plan statement fails with a transient connection error and the switch-off also fails
- **THEN** the failed connection is discarded before the retry, and the retry runs the whole statement sequence on another connection

#### Scenario: A dialect without EXPLAIN refuses
- **WHEN** a query on a BigQuery datasource is executed with `explain=True`
- **THEN** it raises an explain-unsupported error, which is a `ValueError`, and no statement reaches the client

### Requirement: A client written before explain keeps working and warns

A client without its own explain operation — one built from scratch without `explain`, or a `DelegatingSqlClient` subclass that overrides statement execution but not `explain`, at any depth of subclassing — SHALL have an explain query whose dialect needs no session statements run through its statement execution, as the single plan statement, with a deprecation warning naming `explain` and SLayer 2.0. An explain query needing session statements on such a client SHALL be refused with an explain-unsupported error, and no statement SHALL reach the client. A client serving `explain` — the default client, a plain `DelegatingSqlClient`, or a subclass overriding `explain` — SHALL be used unchanged, with no warning. The deprecated single-text EXPLAIN builder on a dialect SHALL keep returning its single statement for dialects that need no session statements, raise an explain-unsupported error for the others, and warn, naming its replacement and SLayer 2.0.

#### Scenario: A client built from scratch without explain
- **WHEN** an engine's client factory returns a client implementing every contract member except `explain`, and a SQLite query is explained
- **THEN** the client's statement execution receives the EXPLAIN statement, the plan rows are returned, and a deprecation warning names `explain` and SLayer 2.0

#### Scenario: A statements-only gateway written before explain
- **WHEN** the factory returns a `DelegatingSqlClient` subclass overriding statement execution but not `explain`, directly or through an intermediate subclass, and a SQLite query is explained
- **THEN** the EXPLAIN statement reaches that override, not the wrapped client, with a deprecation warning

#### Scenario: Session-mode explain on a client written before explain
- **WHEN** such a client serves a SQL Server datasource and a query is explained
- **THEN** an explain-unsupported error is raised and the client receives no statement

#### Scenario: A current client is used unchanged
- **WHEN** the client is the default client, a plain `DelegatingSqlClient`, or a subclass overriding `explain`, and a query is explained
- **THEN** its explain operation serves it, and no deprecation warning is emitted

#### Scenario: The deprecated single-text builder
- **WHEN** the deprecated EXPLAIN builder of the Postgres, the SQL Server and the BigQuery dialect is called
- **THEN** Postgres returns its single EXPLAIN statement, SQL Server and BigQuery raise an explain-unsupported error, and each call warns, naming the replacement

## MODIFIED Requirements

### Requirement: A delegating client forwards everything it does not override

A delegating client SHALL forward every client operation, including explain and closing, to the client it wraps, so a subclass that overrides only some operations serves those and leaves the rest to the wrapped client. Operations of the wrapped client SHALL never be routed through the subclass's overrides.

#### Scenario: A statements-only gateway
- **WHEN** a delegating client over a default client overrides only statement execution, explain and column-type probing, and an engine using it runs a query, explains it and validates its models
- **THEN** the query's statements and the explain's statements reach the overrides, while metadata reads and the wrapped client's own internal statements go to the wrapped client

#### Scenario: A plain delegating client forwards explain
- **WHEN** a plain delegating client over a default client explains a query
- **THEN** the wrapped client's explain operation runs it

#### Scenario: Closing a delegating client closes the wrapped client
- **WHEN** a delegating client is closed
- **THEN** the wrapped client is closed

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
