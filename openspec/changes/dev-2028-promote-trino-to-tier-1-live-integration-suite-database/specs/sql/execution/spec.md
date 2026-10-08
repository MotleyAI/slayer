## MODIFIED Requirements

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

#### Scenario: Database without a timeout mechanism
- **WHEN** a query runs on a DuckDB datasource
- **THEN** no timeout statement is sent and the response carries no timeout
  warning

#### Scenario: Rejected timeout on a fail-closed database
- **WHEN** a MySQL datasource rejects the timeout statement
- **THEN** the query is not run and the database's error is raised

### Requirement: A missing database driver raises a typed, actionable error
When connecting to a datasource needs a Python driver module or SQLAlchemy dialect plugin that is
not installed — on the synchronous path, the asynchronous path, or a dialect's own lazily imported
driver — SLayer SHALL raise a missing-driver error that is both a SLayer error and an import error.
Its message SHALL name the datasource, its type, and the missing module (or, for a missing dialect
plugin, the URL scheme whose plugin could not be loaded), and SHALL carry an install hint:
- when the connection URL selects SLayer's default driver for a type that has a SLayer extra
  (postgres/postgresql, mysql, mariadb, clickhouse, sqlserver/mssql/tsql, snowflake, bigquery,
  trino), the hint SHALL be `pip install 'motley-slayer[<extra>]'` with that type's extra;
- when the URL selects SLayer's default driver for a type without an extra, the hint SHALL tell the
  user to install the named module or plugin and point to the datasource documentation;
- when the URL names a driver other than SLayer's default, the hint SHALL tell the user to install
  the driver named in their `connection_string`, and SHALL NOT recommend a SLayer extra.

An import failure that is not attributable to the datasource's driver or dialect plugin SHALL
propagate unchanged.

#### Scenario: Missing sync driver names the extra
- **WHEN** a postgres datasource with a structured config is executed synchronously and `psycopg2`
  is not importable
- **THEN** a missing-driver error is raised naming the datasource, type `postgres`, module
  `psycopg2`, and `pip install 'motley-slayer[postgres]'`

#### Scenario: Every extra-backed type names its own extra
- **WHEN** the default driver or dialect plugin of each of mysql, mariadb, clickhouse, sqlserver,
  snowflake, bigquery and trino is not importable and an engine is built for that type
- **THEN** each raises a missing-driver error carrying that type's `motley-slayer[<extra>]` hint

#### Scenario: Missing dialect plugin names the scheme
- **WHEN** a clickhouse datasource is used and the ClickHouse SQLAlchemy plugin is not installed
- **THEN** the missing-driver error names the `clickhouse+http` scheme and
  `pip install 'motley-slayer[clickhouse]'`

#### Scenario: Missing async driver raises instead of falling back
- **WHEN** a postgres datasource executes asynchronously and `asyncpg` is not importable
- **THEN** a missing-driver error naming `asyncpg` and `pip install 'motley-slayer[postgres]'` is
  raised, and the statement is not run through the synchronous driver

#### Scenario: Custom driver is not misattributed to the extra
- **WHEN** a postgres datasource's `connection_string` is `postgresql+pg8000://…` and `pg8000` is
  not importable
- **THEN** the missing-driver error names `pg8000`, tells the user to install the driver named in
  their `connection_string`, and does not mention `motley-slayer[postgres]`

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

## ADDED Requirements

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
