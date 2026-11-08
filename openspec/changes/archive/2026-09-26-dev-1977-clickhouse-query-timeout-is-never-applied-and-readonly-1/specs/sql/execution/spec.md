## MODIFIED Requirements

### Requirement: Rendered SQL executes verbatim
The client SHALL pass every rendered statement to the database driver unchanged
and with no parameter set: a `:word` sequence SHALL never be read as a bind
parameter, and a `%` SHALL never be read as a format directive. This SHALL hold
on every execution path — asynchronous and synchronous query execution and the
column-type probe — and SHALL NOT be relaxed to carry a statement timeout: a
timeout travels beside the statement, never inside it.

#### Scenario: Regex group inside a string literal
- **WHEN** a query's rendered SQL contains the string literal
  `'(?i)(?:too complicated|too complex)'`
- **THEN** the query executes on both the asynchronous and the synchronous
  path and returns rows carrying that literal unchanged

#### Scenario: Percent sign in a literal
- **WHEN** rendered SQL contains a `%` inside a literal — a `LIKE '%x%'`
  pattern or a `'%Y-%m'` format string — on a driver whose placeholders use `%`
- **THEN** the database receives a single `%` and the query returns the rows
  the pattern selects

#### Scenario: Type probe on the same SQL
- **WHEN** the column-type probe runs over SQL containing the literal `'(?:x)'`
- **THEN** it infers the column types without a bind-parameter error

#### Scenario: Ad-hoc regex column end to end
- **WHEN** a query extends its model with a column whose SQL contains
  `(?i)(?:too complicated|too complex)` and runs through the query engine
- **THEN** the query succeeds and the column's values reflect that expression

#### Scenario: ClickHouse statement reaches the server byte-identical
- **WHEN** a query with a statement timeout runs on a ClickHouse datasource and
  its SQL uses ClickHouse-specific spellings (`toStartOfMonth(d)`, `x::Int32`,
  a trailing `-- comment`)
- **THEN** the statement text the server logs is byte-identical to the SQL the
  client was given

## ADDED Requirements

### Requirement: Statement timeout is applied per dialect
Every query execution and column-type probe SHALL run under the caller's
statement timeout, applied the way the datasource's database accepts it and on
the synchronous and asynchronous paths alike. Datasources whose type is absent
or unrecognised SHALL be treated like Postgres. A database with no timeout
mechanism SHALL run the statement with no timeout and no warning. Where a
timeout cannot be applied and the database is not best-effort, the statement
SHALL fail with the database's error rather than run unbounded.

#### Scenario: MySQL
- **WHEN** a query runs with a 120-second timeout on a `mysql` datasource
- **THEN** the session's `max_execution_time` is 120000 milliseconds before
  the query runs

#### Scenario: MariaDB
- **WHEN** a query runs with a 120-second timeout on a `mariadb` datasource
- **THEN** the session's `max_statement_time` is 120 seconds before the query
  runs, and no `max_execution_time` statement is sent

#### Scenario: Postgres and untyped datasources
- **WHEN** a query runs with a 120-second timeout on a `postgres` datasource,
  a datasource with no type, or one with an unrecognised type
- **THEN** a transaction-local `statement_timeout` of 120000 milliseconds is
  in force for the query

#### Scenario: Snowflake
- **WHEN** a query runs with a 120-second timeout on a `snowflake` datasource
- **THEN** the session's `STATEMENT_TIMEOUT_IN_SECONDS` is 120 before the
  query runs

#### Scenario: ClickHouse timeout is enforced
- **WHEN** `SELECT sleep(3)` runs with a 1-second timeout on a ClickHouse
  datasource whose user may change settings
- **THEN** the query fails with ClickHouse's `TIMEOUT_EXCEEDED` error

#### Scenario: ClickHouse SQL's own setting wins
- **WHEN** a query whose SQL ends in `SETTINGS max_execution_time = 5` runs
  with a 1-second timeout on ClickHouse and sleeps for 2 seconds
- **THEN** the query succeeds

#### Scenario: ClickHouse setting does not outlive its call
- **WHEN** a timed query has run on a pooled ClickHouse connection and a later
  statement runs on the same connection outside the client's timed paths
- **THEN** that statement's `max_execution_time` is the connection's prior
  value, including when the timed query raised

#### Scenario: Type probe is timed on every dialect
- **WHEN** the column-type probe runs on a datasource whose database has a
  timeout mechanism
- **THEN** a 60-second timeout is in force for the probe, and on databases
  that guard the probe with a read-only transaction the guard is still in
  force

#### Scenario: Untyped datasource probe keeps the read-only guard
- **WHEN** the column-type probe runs on a datasource with no type
- **THEN** it runs inside a read-only transaction, as on Postgres

#### Scenario: Database without a timeout mechanism
- **WHEN** a query runs on a DuckDB datasource
- **THEN** no timeout statement is sent and the response carries no timeout
  warning

#### Scenario: Rejected timeout on a fail-closed database
- **WHEN** a MySQL datasource rejects the timeout statement
- **THEN** the query is not run and the database's error is raised

### Requirement: A skipped timeout is reported
Whenever a query runs without the statement timeout its caller asked for, the
client SHALL report it as a structured `statement_timeout_skipped` warning naming
the datasource, the timeout, and the reason, surfaced as a Python warning and —
for a query answered through the query engine — on the response's `warnings`.

#### Scenario: ClickHouse readonly user
- **WHEN** queries run on a ClickHouse datasource whose user has `readonly = 1`
- **THEN** each query succeeds without SLayer's timeout, the user's read-only
  level is checked once for the engine, and each response carries a
  `statement_timeout_skipped` warning with reason `readonly_user` whose message
  names the `readonly = 2` remedy

#### Scenario: ClickHouse readonly level 2
- **WHEN** a query runs on a ClickHouse datasource whose user has `readonly = 2`
- **THEN** the timeout is applied and no timeout warning is reported

#### Scenario: Postgres rejects the timeout
- **WHEN** a Postgres-typed datasource rejects the timeout statement
- **THEN** the failed statement does not abort the query, the query returns
  its rows, and the response carries a `statement_timeout_skipped` warning with
  reason `timeout_rejected`

#### Scenario: Probe without a timeout
- **WHEN** the column-type probe runs without its timeout for either reason
- **THEN** the probe still infers the column types and a Python warning with
  the same payload is emitted
