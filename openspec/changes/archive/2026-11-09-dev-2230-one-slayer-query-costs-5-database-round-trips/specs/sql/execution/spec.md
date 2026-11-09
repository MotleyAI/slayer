## MODIFIED Requirements

### Requirement: Statement timeout is applied per dialect
Every query execution and column-type probe SHALL run under the caller's
statement timeout, applied the way the datasource's database accepts it and on
the synchronous and asynchronous paths alike. A timeout set by a statement SHALL
be set once per connection and re-set only when the value a call needs differs
from the value in force on that connection, except on a datasource marked
`transaction_pooler`, where it is set in each statement's transaction. Datasources whose type is absent
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
- **THEN** the session's `statement_timeout` is 120000 milliseconds before the
  query runs

#### Scenario: Snowflake
- **WHEN** a query runs with a 120-second timeout on a `snowflake` datasource
- **THEN** the session's `STATEMENT_TIMEOUT_IN_SECONDS` is 120 before the
  query runs

#### Scenario: The timeout is set once per connection
- **WHEN** three queries with a 120-second timeout run one after another on
  one pooled `postgres`, `mysql` or `snowflake` connection
- **THEN** the timeout statement is sent before the first query only

#### Scenario: A changed timeout is re-set once
- **WHEN** a 60-second column-type probe runs between two 120-second queries
  on one pooled connection
- **THEN** one timeout statement precedes the probe and one precedes the second
  query, and each runs under its own timeout

#### Scenario: A replaced connection sets its timeout afresh
- **WHEN** a pooled connection whose timeout was set is invalidated and a new
  connection serves the next query
- **THEN** the timeout statement is sent on the new connection before that
  query

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

#### Scenario: Trino
- **WHEN** a query runs with a 120-second timeout on a `trino` datasource
- **THEN** the query runs with the session property `query_max_run_time` set to
  120 seconds, and no `SET SESSION` statement is sent

#### Scenario: Trino timeout is enforced
- **WHEN** a query that runs longer than 2 seconds runs with a 1-second timeout
  on a `trino` datasource
- **THEN** the query fails with Trino's `EXCEEDED_TIME_LIMIT` error

#### Scenario: Trino setting does not outlive its call
- **WHEN** a timed query has run on a pooled Trino connection and a later
  statement runs on the same connection outside the client's timed paths
- **THEN** that statement's `query_max_run_time` is the connection's prior
  value — unset if it had none, the configured value if the datasource set one —
  including when the timed query raised

#### Scenario: Type probe is timed on every dialect
- **WHEN** the column-type probe runs on a datasource whose database has a
  timeout mechanism
- **THEN** a 60-second timeout is in force for the probe, and on databases
  that guard the probe with a read-only transaction the guard is still in
  force

#### Scenario: Untyped datasource probe keeps the read-only guard
- **WHEN** the column-type probe runs on a datasource with no type
- **THEN** it runs inside a read-only transaction, as on Postgres

#### Scenario: A failed probe leaves no transaction open
- **WHEN** the column-type probe's SQL fails on a Postgres datasource
- **THEN** its read-only transaction is rolled back, and the next query on that
  connection runs outside any transaction under its own timeout

#### Scenario: Database without a timeout mechanism
- **WHEN** a query runs on a DuckDB datasource
- **THEN** no timeout statement is sent and the response carries no timeout
  warning

#### Scenario: Rejected timeout on a fail-closed database
- **WHEN** a MySQL datasource rejects the timeout statement
- **THEN** the query is not run and the database's error is raised

#### Scenario: A rejected best-effort timeout is not re-sent
- **WHEN** a Postgres-typed datasource rejects the timeout statement and two
  more queries then run on the same connection
- **THEN** the rejected statement is not sent again on that connection, and
  each query's response carries the `statement_timeout_skipped` warning with
  reason `timeout_rejected`

### Requirement: The query engine owns the clients it builds

A query engine SHALL be the only party that closes the clients it built; code that borrows a client SHALL NOT close it. Closing a query engine, synchronously or asynchronously, SHALL close every client it built, including factory clients, and SHALL release every connection those clients hold, including those held for synchronous calls; the clients SHALL remain usable afterwards. Every synchronous entry point SHALL run its whole workflow in one bridged event loop and, at its end, SHALL release the idle connections that hold a local database file, also when the workflow raises or is cancelled; connections to networked databases SHALL stay open for later calls.

#### Scenario: Join profiling leaves the shared client open
- **WHEN** join-cardinality profiling runs and a query on the same datasource follows on the same engine
- **THEN** profiling never closes the client, and the query reuses it

#### Scenario: Closing the engine reaches factory clients
- **WHEN** an engine whose factory built two clients is closed asynchronously, then synchronously, and then executes a query
- **THEN** both clients were closed each time, and the query succeeds on a usable client

#### Scenario: A multi-step CLI command uses one event loop
- **WHEN** `slayer validate-models --cardinality` runs on a Postgres datasource reached through an asynchronous driver
- **THEN** drift validation and join profiling both complete, and no join is reported as failed to scan

#### Scenario: Synchronous calls repeat safely
- **WHEN** a synchronous client method is called three times in a row, once from inside a running event loop, and once with a workflow that raises
- **THEN** each call returns or raises its own result, and after each the idle connections holding a local database file are released

#### Scenario: Closing an engine releases its synchronous connections
- **WHEN** a query engine that served synchronous calls on a Postgres datasource is closed
- **THEN** no connection it opened remains open on the server

## ADDED Requirements

### Requirement: A warm statement costs one request
On a warm connection — open, initialised, idle for less than 60 seconds, and with the statement
timeout the call needs already in force — the client SHALL send the database exactly one request per
statement it executes: no liveness check, no transaction begin or end, and no session or timeout
statement. This SHALL hold on the asynchronous path, on the synchronous path, and for synchronous
calls bridged onto an asynchronous driver, on every Tier-1 networked database. A datasource opted
into a transaction-mode pooler is the one exception.

#### Scenario: Postgres warm statements
- **WHEN** three statements run in a row on a `postgres` datasource through an asynchronous driver
  in one event loop, through synchronous calls on psycopg, and through synchronous calls on psycopg2
- **THEN** on each path the server logs exactly one statement for each statement after the first

#### Scenario: Every Tier-1 networked database
- **WHEN** three statements run in a row on a `mysql`, `sqlserver`, `clickhouse`, `trino`,
  `bigquery` or `snowflake` datasource
- **THEN** the database receives exactly one request for each statement after the first

#### Scenario: Snowflake session overrides are set once per connection
- **WHEN** a Snowflake datasource configures a role, warehouse, database and schema, and several
  statements run on one pooled connection
- **THEN** the `USE` statements are sent once, when the connection opens, and every statement runs
  with the configured role, warehouse, database and schema

### Requirement: Synchronous calls reuse connections
Synchronous entry points SHALL run their workflows on one long-lived event loop per process, so a
connection opened during one synchronous call SHALL serve later synchronous calls on the same
datasource. The end of a synchronous workflow SHALL NOT close an in-memory database. A forked child
process SHALL use its own event loop and connections and SHALL neither use nor close its parent's.
A synchronous call made from code already running on that event loop SHALL complete without
deadlock. Context variables set by the caller SHALL be visible inside the workflow. Interrupting a
synchronous call SHALL cancel its work and SHALL return to the caller once that work's cleanup has
run or a bounded wait has passed; a statement already running on a driver thread finishes there and
then returns its connection to the pool.

#### Scenario: No reconnect across synchronous calls
- **WHEN** a client executes three statements synchronously on a `postgres` datasource whose driver
  is psycopg
- **THEN** the server logs one connection and no repeated session initialisation

#### Scenario: Synchronous workflows keep networked connections
- **WHEN** two synchronous query-engine workflows run one after the other on a networked datasource
- **THEN** the second runs on the connection the first opened

#### Scenario: A local database file is released after a synchronous workflow
- **WHEN** a synchronous workflow over a file-backed DuckDB datasource finishes
- **THEN** another process can open that database file for writing

#### Scenario: An in-memory database survives synchronous workflows
- **WHEN** a synchronous workflow creates a table in an in-memory SQLite datasource and a second
  synchronous workflow on the same engine reads it
- **THEN** the second workflow sees the table and its rows

#### Scenario: A forked child uses its own connections
- **WHEN** a process that has run synchronous calls forks, and both the child and the parent then
  run a synchronous call on the same datasource
- **THEN** both calls succeed, the child's runs on a connection it opened, and the parent's
  connections stay usable

#### Scenario: A synchronous call from inside the bridge loop
- **WHEN** a workflow running on the long-lived event loop calls a synchronous entry point
- **THEN** that call completes and returns its result

#### Scenario: Context variables reach the workflow
- **WHEN** a caller sets a context variable and then makes a synchronous call
- **THEN** the workflow reads the caller's value

#### Scenario: An interrupted synchronous call cleans up
- **WHEN** a synchronous call is interrupted while its statement runs, and another synchronous call
  follows
- **THEN** the interruption reaches the caller, every connection is back in its pool once the
  interrupted statement ends, and the following call succeeds

#### Scenario: Process exit leaves nothing open
- **WHEN** a process that has run synchronous calls on networked and file-backed datasources ends
- **THEN** every connection it opened is closed and no resource warning is emitted

### Requirement: Idle connections are checked before reuse
A pooled connection idle for longer than 60 seconds SHALL be checked for liveness before it serves a
statement, and one that fails the check SHALL be replaced by a new connection before the statement
runs. A connection idle for less than 60 seconds, and a newly opened connection, SHALL NOT be
checked.

#### Scenario: A dead idle connection is replaced
- **WHEN** the server has closed a pooled connection that has been idle for more than 60 seconds,
  and a statement runs
- **THEN** the statement succeeds on a new connection and no error reaches the caller

#### Scenario: A recently used connection is not checked
- **WHEN** a statement runs on a connection idle for less than 60 seconds
- **THEN** no liveness check is sent

#### Scenario: A new connection is not checked
- **WHEN** a statement runs on a connection the pool has just opened, including one that replaced a
  connection which failed its check
- **THEN** no liveness check is sent on it

### Requirement: A transaction-mode pooler is an explicit opt-in
A datasource configuration SHALL accept a `transaction_pooler` flag, false by default. When it is
true on a `postgres` or `postgresql` datasource, every statement SHALL run inside its own transaction
under a transaction-local statement timeout, so no setting SLayer makes outlives the statement.
Saving a datasource that sets the flag with any other type, or with no type, SHALL be refused with
an error naming the type; connecting such a datasource whose resolved driver is not a PostgreSQL
driver SHALL fail with the same error. Stored datasources without the flag SHALL load unchanged.

#### Scenario: A pooler datasource keeps the timeout transaction-local
- **WHEN** a query runs with a 120-second timeout on a `postgres` datasource whose
  `transaction_pooler` is true
- **THEN** a transaction-local `statement_timeout` of 120000 milliseconds is in force for the
  query, and after it the session's `statement_timeout` is unchanged

#### Scenario: The flag is refused on other types
- **WHEN** a `mysql` datasource, and a datasource with no type, are saved with `transaction_pooler`
  true
- **THEN** each save is refused with an error naming the type, and nothing is stored

#### Scenario: Stored datasources load without the flag
- **WHEN** a datasource stored before this change is loaded
- **THEN** its `transaction_pooler` is false and it is not rewritten
