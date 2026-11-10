# database/execution Specification

## Purpose
Governs how rendered SQL reaches the database: the client hands every statement
to the driver verbatim, so no character sequence in model or query SQL is
reinterpreted on the way to the database.

## Requirements

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

### Requirement: One execution door
The client SHALL execute every statement — rendered queries and its own session
statements (timeouts, read-only transactions) alike — through one verbatim
driver-level path; no statement SHALL pass through a text-compilation layer
that parses placeholders.

#### Scenario: Module guard
- **WHEN** the client module's source is inspected
- **THEN** it constructs no text clause and issues no driver-level execute
  outside the single verbatim helper pair

### Requirement: Engines have one disposing owner
Every database engine SHALL be disposed by exactly one owner. The shared engine factory SHALL
dispose every engine it evicts or drops — resetting its cache SHALL always dispose the engines it
held. A query engine SHALL dispose the private in-memory engines of its clients when it is closed;
closing SHALL be idempotent and SHALL leave the query engine usable. The per-call teardown of
loop-bound asynchronous engines SHALL NOT dispose synchronous engines. A query engine that becomes
unreachable without being closed SHALL still have its private in-memory engines disposed when it
is collected.

#### Scenario: Cache reset disposes every held engine
- **WHEN** the engine factory cache is reset while it holds engines
- **THEN** each held engine is disposed exactly once and the cache is empty

#### Scenario: Closing a query engine disposes only its private engines
- **WHEN** a query engine that executed against an in-memory SQLite datasource and a file-backed
  datasource is closed
- **THEN** the in-memory client's private engine is disposed, the factory-owned file engine is
  not, and a second close is a no-op

#### Scenario: A closed query engine is reusable
- **WHEN** a closed query engine executes a query again
- **THEN** the query succeeds, and an in-memory SQLite datasource starts from an empty database

#### Scenario: Per-call async teardown spares synchronous engines
- **WHEN** a query engine's per-call teardown runs after a synchronous execution
- **THEN** the client's synchronous engine is the same object afterwards and its data is intact

#### Scenario: An unreferenced query engine leaks nothing
- **WHEN** a query engine holding a private in-memory engine becomes unreachable without `close()`
  and the garbage collector runs on CPython 3.13 or newer
- **THEN** no `unclosed database` ResourceWarning is emitted

### Requirement: In-memory SQLite is one database per owner and per datasource
Within one owner — a query engine's client, or the shared engine factory — statements over an
in-memory SQLite datasource SHALL see one database regardless of the thread that executes them,
and closing that database from any thread SHALL succeed. In the shared engine factory, two
in-memory SQLite datasources with different names SHALL never share a database.

#### Scenario: Cross-thread coherence through the factory
- **WHEN** a table is created and populated through the factory's engine for an in-memory
  datasource on one thread and a statement over that datasource then runs on a worker thread
- **THEN** the worker-thread statement sees the table and its rows

#### Scenario: Per-datasource isolation in the factory
- **WHEN** two in-memory SQLite datasources with different names are used through the factory in
  one process
- **THEN** a table created through one is not visible through the other

#### Scenario: Reset closes an in-memory engine used from several threads
- **WHEN** an in-memory factory engine was used from more than one thread and the cache is reset
- **THEN** no connection-close error is logged and no `unclosed database` warning is emitted

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

### Requirement: Structured-config connection URLs are stable
A datasource configured by fields rather than a `connection_string` SHALL produce a connection URL
whose scheme names the type's default driver where the type declares one; a datasource type that no
registered dialect declares SHALL keep `<type>://` as its URL scheme.

#### Scenario: Known types keep their scheme
- **WHEN** structured postgres, mysql, mariadb, clickhouse and sqlserver datasources build their URLs
- **THEN** the schemes are `postgresql+psycopg`, `mysql+pymysql`, `mysql+pymysql`,
  `clickhouse+http` and `mssql+pyodbc`

#### Scenario: Unknown type keeps its own scheme
- **WHEN** a structured datasource of an unregistered type `foo` builds its URL
- **THEN** the URL scheme is `foo`, not the Postgres fallback dialect's scheme

### Requirement: Trino authenticates over HTTPS
A `trino` datasource whose connection URL carries credentials the Trino driver authenticates
with — a password, an `access_token`, a `cert` together with a `key`, or `externalAuthentication`
— SHALL connect over HTTPS on any port. An `http_scheme` query parameter of `https` or `http` SHALL
take precedence over that rule and SHALL NOT reach the driver as a URL parameter; any other value
SHALL fail when the engine is built, with an error naming the value. A URL with neither credentials
nor `http_scheme` SHALL keep the driver's default scheme. Every other URL component and query
parameter SHALL reach the driver unchanged.

#### Scenario: Password on a non-443 port
- **WHEN** a `trino` datasource with a username and password on port 8443 is used
- **THEN** the connection uses HTTPS

#### Scenario: Each credential kind selects HTTPS
- **WHEN** a `trino` connection string carries an `access_token`, both `cert` and `key`, or
  `externalAuthentication`
- **THEN** the connection uses HTTPS

#### Scenario: Incomplete certificate pair is not a credential
- **WHEN** a `trino` connection string carries `cert` but no `key`, and no other credential
- **THEN** the driver's default scheme is used

#### Scenario: Explicit scheme wins
- **WHEN** a `trino` connection string with a password carries `http_scheme=http` and
  `allow_insecure_auth=true`
- **THEN** the connection uses HTTP, `allow_insecure_auth` reaches the driver, and `http_scheme`
  does not reach it as a URL parameter

#### Scenario: Explicit HTTPS without credentials
- **WHEN** a `trino` connection string without credentials carries `http_scheme=https`
- **THEN** the connection uses HTTPS

#### Scenario: Unknown scheme fails
- **WHEN** a `trino` connection string carries `http_scheme=ftp`
- **THEN** building the engine fails with an error naming `ftp`

#### Scenario: No credentials keeps the driver default
- **WHEN** a `trino` datasource without credentials or `http_scheme` is used on port 8080
- **THEN** the connection uses the driver's default scheme, HTTP

#### Scenario: Reserved characters survive
- **WHEN** a `trino` datasource's password contains `@`, `/` and `+`
- **THEN** the driver receives the password unchanged

### Requirement: Every live database access goes through an engine-owned client

Every read of or statement against a user datasource — query and explain statements, column-type probes, cache refresh scans, join-cardinality profiling, server-profile probes, schema-drift listing and reflection, row-level-security column checks, ingestion, legacy type refinement, the MCP datasource tools and the dbt/OSI importers' table introspection — SHALL go through a client obtained from a query engine. A query engine SHALL build its clients only through its client factory when one is given, else as the default client, and SHALL reuse one client per datasource name and connection identity. No call order SHALL let a client from any other source serve a later access. Without a client factory, observable behaviour SHALL be unchanged.

#### Scenario: Query statements reach the factory client
- **WHEN** an engine built with a client factory executes a query and an explain query
- **THEN** the query's statement reaches the factory client's statement execution and the explain's statements reach its explain operation, each verbatim, and no direct database connection is opened

#### Scenario: Engine-internal statements reach the factory client
- **WHEN** that engine runs a column-type probe, a cache refresh scan, join-cardinality profiling, and a query on a ClickHouse datasource that needs the server profile
- **THEN** every statement and the profile request reach the factory's client

#### Scenario: A gateway with no direct access serves every feature
- **WHEN** every direct connection path is made to fail, and an engine whose factory returns a gateway client answering statements and metadata from a fixture runs: ingestion of a datasource, model validation, a query under a column-scoped session policy, the load of a model stored below version 8, the MCP create-datasource and describe-datasource tools, and dbt and OSI imports that introspect tables
- **THEN** each succeeds with the results the default client gives over the same fixture

#### Scenario: Validation before the first query cannot bypass the factory
- **WHEN** an engine with a client factory validates the models of a datasource that has a SQL-backed model, and then executes a query on that datasource
- **THEN** the query runs on the factory's client

#### Scenario: One client per datasource and connection identity
- **WHEN** an engine with a client factory runs several queries on one datasource, and one query on each of two datasources with different names that share one connection URL
- **THEN** the factory is called once for the first datasource and once for each of the other two

#### Scenario: A factory need not extend the default client
- **WHEN** the factory returns an object that implements the client contract without extending the default client
- **THEN** queries and metadata reads run through it

#### Scenario: Only the default client touches the database driver
- **WHEN** the package source is inspected
- **THEN** database engines, inspectors and driver connections for user datasources are created or used only inside the default client and the connection and dialect code it owns; SLayer's own storage databases, facade-local catalogs and demo data generation are exempt

### Requirement: The query engine owns the clients it builds

A query engine SHALL be the only party that closes the clients it built; code that borrows a client SHALL NOT close it. Closing a query engine, synchronously or asynchronously, SHALL close every client it built, including factory clients, and SHALL release the connections those clients own (a synchronous close those on the bridge loop, an asynchronous close those on its own loop); pooled connections of the shared engine cache stay open for other clients. The clients SHALL remain usable afterwards. Every synchronous entry point SHALL run its whole workflow in one bridged event loop and, at its end, SHALL release the idle connections that hold a local database file, also when the workflow raises or is cancelled; connections to networked databases SHALL stay open for later calls.

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
- **WHEN** a query engine that served synchronous calls on a Postgres datasource whose driver is
  psycopg (the default) is closed
- **THEN** no connection it opened remains open on the server

### Requirement: Live database access never blocks the event loop

Live database access reached from asynchronous code SHALL run without blocking the event loop.

#### Scenario: Legacy refinement keeps the loop responsive
- **WHEN** a model stored below version 8 is loaded on a non-SQLite datasource whose client answers metadata slowly, and separately on a SQLite datasource whose integer-affinity probe is slow
- **THEN** a concurrent task keeps advancing while each load runs

#### Scenario: MCP datasource tools keep the loop responsive
- **WHEN** the MCP create-datasource, describe-datasource and empty-result ingest tools run against a slow client
- **THEN** a concurrent task keeps advancing while each runs

### Requirement: Entry points accept the client factory

`SlayerQueryEngine`, `SlayerClient` in local mode, the REST and MCP server builders, and the Postgres-facade and Flight engine builders SHALL accept a client factory and SHALL pass it to every query engine they build. `SlayerClient` in remote mode SHALL reject a client factory with a `ValueError`. CLI commands SHALL use the default client.

#### Scenario: The local Python client passes the factory through
- **WHEN** `SlayerClient` is built with storage and a client factory and runs a query
- **THEN** the query runs on the factory's client

#### Scenario: Remote mode rejects a factory
- **WHEN** `SlayerClient` is built with a URL and a client factory
- **THEN** construction raises a `ValueError`

#### Scenario: Server builders pass the factory to every engine
- **WHEN** the REST, MCP, Postgres-facade and Flight builders are given a client factory and serve a query and a datasource-describing request
- **THEN** every engine they build uses the factory's clients

### Requirement: A client connects through one driver
A datasource's default SQL client SHALL connect through exactly one database driver: the driver of
the engine its caller supplied, else the driver its connection URL names, else — for a URL that
names none — the type's default driver. The client SHALL NOT rewrite a connection URL to another
driver; the URL's query parameters SHALL reach that driver unchanged.

When the driver can run asynchronously, the client SHALL run statements asynchronously on it and
SHALL serve every metadata read — connectivity checks, reflection, object listings, keys, comments,
the server profile and column-type probes — over the same driver, without building any other
engine. When the driver cannot run asynchronously, the client SHALL run statements and metadata
reads on that driver synchronously without blocking the event loop. On an event loop the driver's
asynchronous mode cannot run on (psycopg on Windows' default proactor event loop), the client SHALL
use the same driver synchronously, from the same URL, without blocking the event loop.

#### Scenario: Each named Postgres driver serves every operation
- **WHEN** a postgres datasource's URL is `postgresql+psycopg2://u@h/d?sslmode=disable`,
  `postgresql+asyncpg://u@h/d?ssl=disable`, `postgresql+psycopg://u@h/d?sslmode=disable` or
  `postgresql+pg8000://u@h/d`, and its client pings, reflects and lists a table, probes a
  statement's column types and executes a statement
- **THEN** each operation succeeds, and every connection the client opens uses the named driver with
  the URL's query parameters unchanged

#### Scenario: A URL naming no driver connects with psycopg
- **WHEN** a postgres datasource's URL is `postgresql://u@h/d?sslmode=disable&connect_timeout=5` and
  its client runs the same operations
- **THEN** each succeeds over psycopg, asynchronously, with `sslmode=disable` and `connect_timeout=5`
  reaching the driver unchanged

#### Scenario: A sync-only driver builds no asynchronous engine
- **WHEN** a client whose URL names `psycopg2`, `pg8000`, `pymysql` or `mysqldb`, or a mysql
  datasource whose URL names no driver, runs statements and metadata reads from asynchronous code
- **THEN** every one runs on that driver synchronously without blocking the event loop, and no
  asynchronous engine is built

#### Scenario: An async-capable driver builds no second engine
- **WHEN** a client whose URL names `psycopg`, `asyncpg` or `aiomysql` runs every statement and
  metadata operation
- **THEN** no synchronous engine is built for that datasource

#### Scenario: A named asynchronous MySQL driver serves metadata
- **WHEN** a mysql datasource's URL is `mysql+aiomysql://…` and its client pings, reflects a table,
  probes a statement's column types and executes a statement
- **THEN** each succeeds over aiomysql

#### Scenario: A loop the driver refuses runs it synchronously
- **WHEN** a client whose driver is psycopg is used from an event loop psycopg's asynchronous mode
  refuses
- **THEN** statements and metadata reads run on psycopg synchronously from the same URL, and the
  event loop is not blocked

#### Scenario: A caller's engine is the only driver
- **WHEN** a client wraps an engine its caller supplied and runs statements and metadata reads
- **THEN** every one runs on that engine, and no other engine is built

#### Scenario: An async SQLite driver is refused
- **WHEN** a SQLite datasource's URL names `aiosqlite`
- **THEN** connecting raises an error naming the built-in `sqlite://` driver, and no engine is built

### Requirement: Async engines are held per event loop
A client's asynchronous engine SHALL belong to the event loop that built it: a client used from
several event loops SHALL hold one engine per loop and SHALL never use an engine or its connections
from another loop. Releasing a client's loop-bound resources SHALL release only the running loop's
engine. An engine whose event loop has closed SHALL be dropped without connection I/O from another
loop. One client SHALL serve several event loops on several threads at once.

#### Scenario: A synchronous bridge after asynchronous use
- **WHEN** asynchronous code on a running event loop executes a statement through an engine-owned
  client of a Postgres datasource, and the query engine's synchronous entry point then runs a
  statement on the same datasource from inside that loop
- **THEN** both return the same rows, no warning about an unclosed connection or transport is
  emitted, and the client keeps serving statements on the caller's loop afterwards

#### Scenario: Releasing one loop keeps the other
- **WHEN** a client has served statements on two event loops and its loop-bound resources are
  released on one of them
- **THEN** the other loop's statements keep succeeding on that loop's engine

#### Scenario: A closed loop's engine is dropped
- **WHEN** a client served statements on an event loop that has since closed, and is then used on a
  new event loop
- **THEN** the statement succeeds, and none of the closed loop's connections is used from the new loop

#### Scenario: Two loops on two threads at once
- **WHEN** one client serves statements from two event loops running on two threads at the same time
- **THEN** every statement succeeds

#### Scenario: MySQL follows the same rule
- **WHEN** the synchronous-bridge scenario runs on a mysql datasource whose URL names `aiomysql`
- **THEN** it has the same outcome

### Requirement: A connection URL that names no driver uses the type's default driver
For a datasource type that declares a default driver (postgres/postgresql: psycopg; mysql/mariadb:
pymysql; clickhouse: http; sqlserver/mssql/tsql: pyodbc), a connection URL — the datasource's
`connection_string`, or the URL built from its structured fields — whose backend is one the type
serves (postgres/postgresql: `postgresql`; mysql/mariadb: `mysql` and `mariadb`; clickhouse:
`clickhouse`; sqlserver/mssql/tsql: `mssql`) and which names no driver SHALL connect with the type's
default driver on the URL's own backend, whatever the installed SQLAlchemy's own default driver for
that backend is. Credentials, host, port, database and query parameters SHALL be preserved.

A URL that names a driver, whose backend the type does not serve, or that cannot be parsed as a
URL, and every URL of a type without a declared default driver, SHALL be used exactly as given. A
stored `connection_string` SHALL NOT be rewritten.

#### Scenario: Bare Postgres URL uses psycopg
- **WHEN** a postgres datasource's `connection_string` is
  `postgresql://u:p%40ss@h:5432/d?sslmode=require`
- **THEN** it connects as `postgresql+psycopg://u:p%40ss@h:5432/d?sslmode=require`

#### Scenario: Bare MySQL and MariaDB URLs use pymysql
- **WHEN** a mysql datasource's `connection_string` is `mysql://u:p%40ss@h/d?charset=utf8mb4`, or a
  mariadb datasource's is `mariadb://u@h/d`
- **THEN** they connect as `mysql+pymysql://u:p%40ss@h/d?charset=utf8mb4` and
  `mariadb+pymysql://u@h/d` respectively

#### Scenario: ClickHouse and SQL Server URLs use their default driver
- **WHEN** a clickhouse datasource's `connection_string` is `clickhouse://u@h/d`, or a sqlserver
  datasource's is `mssql://u@h/d`
- **THEN** they connect as `clickhouse+http://u@h/d` and `mssql+pyodbc://u@h/d` respectively

#### Scenario: A named driver is kept
- **WHEN** a postgres datasource's `connection_string` is `postgresql+psycopg2://u@h/d` or
  `postgresql+asyncpg://u@h/d`, or a mysql datasource's is `mysql+mysqldb://u@h/d`
- **THEN** the connection URL is exactly the `connection_string`

#### Scenario: Types without a default driver are untouched
- **WHEN** a sqlite, duckdb, snowflake, trino, redshift or unregistered-type datasource connects,
  including a sqlite `:memory:` datasource
- **THEN** its connection URL is exactly as given or as built today

#### Scenario: A foreign backend is untouched
- **WHEN** a postgres datasource's `connection_string` is `mysql://u@h/d`
- **THEN** the connection URL is exactly the `connection_string`

#### Scenario: Resolution does not depend on SQLAlchemy's defaults
- **WHEN** a datasource of any type that declares a default driver connects with a URL naming no
  driver, or with structured fields
- **THEN** SQLAlchemy resolves that URL to the type's declared default driver

#### Scenario: Stored configuration is not rewritten
- **WHEN** a datasource with `connection_string` `postgresql://u@h/d` is used and then read back
- **THEN** its `connection_string` is still `postgresql://u@h/d`

### Requirement: Cancelling a client's blocking database call returns at once

When the default SQL client runs a blocking database call (a statement, a metadata read or a ping)
over a datasource's own connection settings, cancelling the task that awaits it SHALL raise the
cancellation in that task at once, without waiting for the call to finish, and the event loop SHALL
keep running meanwhile. The abandoned call SHALL finish on its own, and its later result or error
SHALL NOT reach the caller or the event loop's exception handler, also when the loop has closed
before the call finishes. Cancelling a statement SHALL start no further retry attempt. An
uncancelled call SHALL return its own result or raise its own error, and SHALL leave no worker
thread running.

#### Scenario: A timeout bounds a blocking metadata read
- **WHEN** a client lists the objects of a SQLite datasource whose file another connection holds
  under an exclusive lock, bounded by `asyncio.wait_for` with a 0.2 s timeout
- **THEN** the caller gets `TimeoutError` long before the database's own busy timeout, and a
  concurrent task keeps advancing while the call is pending

#### Scenario: A timeout bounds a blocking statement
- **WHEN** a client executes a statement on a SQLite datasource whose file another connection holds
  under an exclusive lock, bounded by `asyncio.wait_for` with a 0.2 s timeout
- **THEN** the caller gets `TimeoutError` long before the database's own busy timeout, and a
  concurrent task keeps advancing while the call is pending

#### Scenario: A cancelled call raises the cancellation
- **WHEN** the task awaiting a blocking call is cancelled, either while the call runs or before it
  has started
- **THEN** the task raises `CancelledError` at once, never the call's later result or error, and a
  call cancelled before it started never runs

#### Scenario: The abandoned call's outcome is dropped
- **WHEN** a cancelled call later fails or returns, while its event loop still runs or after the
  loop has closed
- **THEN** nothing is reported to the loop's exception handler, and the call's worker thread ends

#### Scenario: Closing the client right after a cancellation
- **WHEN** a client is closed immediately after one of its blocking calls was cancelled, while that
  call still blocks
- **THEN** closing returns without waiting for the call or raising, and the abandoned call ends on
  its own

#### Scenario: Cancelling a retried statement starts no further attempt
- **WHEN** a statement is cancelled while an attempt runs or while it waits to retry
- **THEN** the caller gets the cancellation and no further attempt starts

#### Scenario: An uncancelled call is unchanged
- **WHEN** a blocking call runs to completion without cancellation, returning a result or raising
  an error
- **THEN** the caller gets that result or that same error, and the call's worker thread has ended
  when the call returns

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

### Requirement: A database connection serves one thread at a time
Every database connection SLayer opens SHALL be used by one thread at a time. Concurrent calls over an
in-memory SQLite datasource SHALL wait for its one connection rather than share it, and SHALL still see
one database; a nested request for that connection from the thread already holding it SHALL fail at
once with an error naming the nesting rather than wait. Every engine SLayer builds, for every
datasource type and for its async engines, SHALL hand each connection to one caller at a time.

#### Scenario: Concurrent calls over an in-memory SQLite datasource
- **WHEN** many statements over one in-memory SQLite datasource are awaited concurrently, each slow
  enough that they would overlap
- **THEN** no two threads use its connection at once, and each statement returns its own rows

#### Scenario: Concurrent calls over a file-backed SQLite datasource
- **WHEN** many statements over one file-backed SQLite datasource are awaited concurrently, each slow
  enough that they would overlap
- **THEN** no connection is used by two threads at once, and each statement returns its own rows

#### Scenario: A nested in-memory checkout fails at once
- **WHEN** a thread holding the in-memory SQLite datasource's connection asks its engine for a
  connection again
- **THEN** the request fails at once with an error naming the nested use

#### Scenario: Every engine hands a connection to one caller at a time
- **WHEN** SLayer builds the synchronous engine, and the async engine where the type has one, for each
  Tier 1 database it supports
- **THEN** each engine's pool hands each connection to one caller at a time

### Requirement: A missing driver raises a typed error with an install hint
When connecting to a datasource needs a Python driver module or SQLAlchemy dialect plugin that is
not installed — whether the client runs it synchronously or asynchronously, or it is a dialect's own
lazily imported driver — SLayer SHALL raise a missing-driver error that is both a SLayer error and
an import error. Its message SHALL name the datasource, its type, and the missing module (or, for a
missing dialect plugin, the URL scheme whose plugin could not be loaded), and SHALL carry an install
hint:
- when the connection URL selects the type's default driver and the type has a SLayer extra, the hint
  SHALL be `pip install 'motley-slayer[<extra>]'` with that type's extra (postgres/postgresql, mysql,
  mariadb, clickhouse, sqlserver/mssql/tsql, snowflake, bigquery, trino);
- when the URL selects SLayer's default driver for a type without an extra, the hint SHALL tell the
  user to install the named module or plugin and point to the datasource documentation;
- when the URL names any other driver — including psycopg2, asyncpg and aiomysql, which no SLayer
  extra installs — the hint SHALL tell the user to install the driver named in their
  `connection_string`, and SHALL NOT recommend a SLayer extra.

A SLayer extra SHALL install only its type's default driver. A missing driver SHALL NOT be replaced
by another driver. An import failure that is not attributable to the datasource's driver or dialect
plugin SHALL propagate unchanged.

#### Scenario: Missing default Postgres driver names the extra
- **WHEN** a postgres datasource with a structured config is used and `psycopg` is not importable
- **THEN** a missing-driver error is raised naming the datasource, type `postgres`, module
  `psycopg`, and `pip install 'motley-slayer[postgres]'`

#### Scenario: Every extra-backed type names its own extra
- **WHEN** the default driver or dialect plugin of each of mysql, mariadb, clickhouse, sqlserver,
  snowflake, bigquery and trino is not importable and an engine is built for that type
- **THEN** each raises a missing-driver error carrying that type's `motley-slayer[<extra>]` hint

#### Scenario: Missing dialect plugin names the scheme
- **WHEN** a clickhouse datasource is used and the ClickHouse SQLAlchemy plugin is not installed
- **THEN** the missing-driver error names the `clickhouse+http` scheme and
  `pip install 'motley-slayer[clickhouse]'`

#### Scenario: A missing non-default driver is not attributed to the extra
- **WHEN** a postgres datasource whose URL names `asyncpg` or `psycopg2`, or a mysql datasource whose
  URL names `aiomysql`, executes a statement and that driver is not importable
- **THEN** a missing-driver error naming the driver tells the user to install the driver named in
  their `connection_string`, does not mention a `motley-slayer[...]` extra, and the statement is not
  run through any other driver

#### Scenario: Custom driver is not misattributed to the extra
- **WHEN** a postgres datasource's `connection_string` is `postgresql+pg8000://…` and `pg8000` is
  not importable
- **THEN** the missing-driver error names `pg8000`, tells the user to install the driver named in
  their `connection_string`, and does not mention `motley-slayer[postgres]`

#### Scenario: Extras install only the default driver
- **WHEN** the `postgres`, `mysql` and `all` extras are read from the package metadata
- **THEN** `postgres` installs psycopg and `mysql` installs pymysql, and no extra installs psycopg2,
  asyncpg or aiomysql

#### Scenario: Tier-2 type gets a generic hint
- **WHEN** a redshift datasource is used and its SQLAlchemy dialect plugin is not installed
- **THEN** a missing-driver error names the `redshift` scheme, tells the user to install it, and
  points to the datasource documentation

#### Scenario: Lazily imported vendor driver
- **WHEN** a Snowflake datasource configured by `connection_name`, or a BigQuery datasource
  configured with `oauth_credentials_json`, is used and its vendor client library is not importable
- **THEN** a missing-driver error carrying that type's extra hint is raised

#### Scenario: Unrelated import failure propagates
- **WHEN** engine construction fails with an import error for a module that is not the
  datasource's driver or dialect plugin
- **THEN** that original import error propagates unchanged

#### Scenario: Surfaces carry the hint
- **WHEN** a missing-driver error reaches a handler that reports SLayer errors
- **THEN** the reported message contains the install hint

### Requirement: A caller's engine serves synchronous calls on the caller's thread
A client wrapping an engine its caller supplied SHALL run that engine's blocking work, during a synchronous SLayer call, on the thread that made the call.

#### Scenario: A caller's in-memory engine serves a synchronous call
- **WHEN** a caller creates an in-memory SQLite engine, creates and fills a table on it, and runs a statement through a client wrapping it from a synchronous SLayer call
- **THEN** the statement reads that table's rows, and the engine's connections are checked out on the caller's thread
