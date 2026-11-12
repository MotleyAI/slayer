## ADDED Requirements

### Requirement: Datasource connections are read-only where the backend allows
Every connection SLayer opens to a datasource SHALL refuse writes at the database wherever the
datasource's backend is live-tested and has a read-only mode, as follows.
- A file-backed DuckDB database SHALL be opened read-only.
- A file-backed SQLite connection SHALL refuse writes to its main, attached and temporary databases.
- A Postgres connection SHALL make every transaction read-only by default from connection start on
  psycopg, psycopg2, asyncpg and pg8000.
- On a Postgres datasource marked `transaction_pooler`, every transaction SHALL instead begin
  read-only, with no session setting.
- A MySQL or MariaDB session SHALL be read-only from connection start.
- A ClickHouse request SHALL carry `readonly = 2` when the user's own `readonly` level is 0. The
  level SHALL be read once per engine. A user at level 1 or 2 SHALL be sent no `readonly` setting.

These guarantees SHALL hold:
- on the synchronous and asynchronous paths;
- on engines a dialect builds itself;
- on every new pooled connection, including after a reconnect;
- for statements and metadata reads alike.

None of these mechanisms SHALL add a request per statement; a Postgres transaction's read-only
start SHALL replace its ordinary start.

A write sent through an engine SLayer built SHALL fail with the database's own error. Settings
SLayer adds SHALL merge with the connection URL's own: every other value the URL carries SHALL
reach the driver unchanged. A database that rejects the read-only setting SHALL fail the
connection with its own error.

Some cases are refused or exempt:
- A MySQL or MariaDB connection URL that sets its own `init_command` SHALL be refused when the
  engine is built, with a typed error naming the datasource.
- A ClickHouse connection URL naming a driver other than `clickhouse+http` SHALL be refused when
  the engine is built, with an unsupported-driver error whose hint names `clickhouse+http`.
- A Postgres connection URL naming a driver other than psycopg, psycopg2, asyncpg or pg8000 SHALL
  be refused when the engine is built, with an unsupported-driver error whose hint names
  `postgresql+psycopg`.
- In-memory SQLite and DuckDB databases SHALL stay writable.
- An engine a caller supplies SHALL NOT be modified.
- Datasources of the other types SHALL connect as before. These are SQL Server, Snowflake,
  BigQuery, Trino, Presto, Athena, Databricks, Spark, Oracle and Redshift.

#### Scenario: A write through a DuckDB engine fails
- **WHEN** an `INSERT` runs through the engine SLayer builds for a file-backed DuckDB datasource
- **THEN** it fails with DuckDB's read-only error and the file's data is unchanged

#### Scenario: A SQLite connection refuses writes to every database it holds
- **WHEN** a connection of the engine SLayer builds for a file-backed SQLite datasource attaches a
  second database file, then writes to the main database, the attached one and a new temporary table
- **THEN** each write fails with SQLite's read-only error, both files are unchanged, and the
  dialect's functions still evaluate on that connection

#### Scenario: Postgres refuses writes on every driver and path
- **WHEN** an `INSERT` runs through the synchronous engine SLayer builds for a Postgres datasource
  whose URL names psycopg, psycopg2 or pg8000, and through the asynchronous engine for psycopg and
  asyncpg
- **THEN** each fails with Postgres's read-only-transaction error, including on a connection in
  autocommit, and `default_transaction_read_only` is `on` on each connection

#### Scenario: Postgres read-only costs no request
- **WHEN** three queries run on one pooled connection of a Postgres datasource
- **THEN** the server receives the same statements it received for those queries before this
  change

#### Scenario: A Postgres URL's own options are kept
- **WHEN** a Postgres datasource's URL passes `options=-c search_path=other` and a query runs
- **THEN** the connection's `search_path` is `other` and `default_transaction_read_only` is `on`

#### Scenario: Pooler transactions begin read-only
- **WHEN** queries and a write run on a Postgres datasource marked `transaction_pooler`, through
  psycopg, psycopg2, pg8000 and asyncpg
- **THEN** every transaction the server logs begins read-only, the write fails with the
  read-only-transaction error, no session-level setting is sent, and the server receives no
  statement beyond those it received for the same calls before this change

#### Scenario: MySQL and MariaDB refuse writes
- **WHEN** an `INSERT` runs through the engine SLayer builds for a MySQL and for a MariaDB datasource
- **THEN** each fails with the database's read-only-transaction error, and queries keep running
  under their statement timeout

#### Scenario: A MySQL URL with its own init command is refused
- **WHEN** a MySQL datasource's URL sets `init_command` and an engine is built for it
- **THEN** a typed error naming the datasource is raised and no connection is opened

#### Scenario: An unrestricted ClickHouse user is made read-only
- **WHEN** a ClickHouse datasource whose user has `readonly = 0` runs an `INSERT` through the engine
  SLayer builds, after a query has run on the same engine
- **THEN** the `INSERT` fails with ClickHouse's read-only error, the query ran under its statement
  timeout, and the user's level was read once for the engine

#### Scenario: A ClickHouse metadata read on a fresh engine is read-only
- **WHEN** the first use of a fresh engine for a ClickHouse datasource whose user has `readonly = 0`
  is a metadata read, and an `INSERT` follows on the same engine
- **THEN** the metadata read succeeds and the `INSERT` fails with ClickHouse's read-only error

#### Scenario: Restricted ClickHouse users are left alone
- **WHEN** queries run on ClickHouse datasources whose users have `readonly = 1` and `readonly = 2`
- **THEN** no `readonly` setting is sent, both succeed, the `readonly = 1` user's responses carry
  the `readonly_user` timeout warning as before, and the `readonly = 2` user's queries run under
  their timeout

#### Scenario: Another ClickHouse driver is refused
- **WHEN** a ClickHouse datasource's URL names `clickhouse+native` or `clickhouse+asynch` and an
  engine is built for it
- **THEN** an unsupported-driver error is raised whose hint names `clickhouse+http`

#### Scenario: Another Postgres driver is refused
- **WHEN** an engine is built for a Postgres datasource whose URL names `psycopg2cffi`
- **THEN** an unsupported-driver error is raised whose hint names `postgresql+psycopg`

#### Scenario: Every engine builder applies it
- **WHEN** SLayer builds, for a file-backed DuckDB, a file-backed SQLite and a Postgres datasource,
  the shared factory's engine, the client's asynchronous engine where the driver has one, and a
  replacement connection after the first is invalidated
- **THEN** a write through each engine and through the replacement connection fails

#### Scenario: In-memory databases stay writable
- **WHEN** an in-memory SQLite and an in-memory DuckDB datasource create and fill a table through
  their client and then read it
- **THEN** the read returns the rows written

#### Scenario: A caller's engine is not modified
- **WHEN** a client wraps a writable engine its caller built over a SQLite file
- **THEN** the caller's own writes through that engine succeed

#### Scenario: Backends without a read-only mode connect as before
- **WHEN** engines are built for SQL Server, Snowflake, BigQuery, Trino, Oracle and Redshift
  datasources
- **THEN** their connection arguments and connection-time statements are the ones they were before
  this change

### Requirement: A missing local database file is refused
A file-backed SQLite or DuckDB datasource whose database file does not exist SHALL be refused, before
any connection is opened, with a typed database-unavailable error naming the datasource and the
path. No file SHALL be created. This SHALL hold for every way such a URL can name the file: a
relative path (resolved against the working directory), an absolute path, and a SQLite `file:` URI.
An in-memory form SHALL NOT be treated as a file.

#### Scenario: A missing DuckDB file
- **WHEN** a DuckDB datasource names a file that does not exist and a query runs
- **THEN** a typed database-unavailable error naming the datasource and the path is raised, and no
  file is created

#### Scenario: A missing SQLite file in each URL form
- **WHEN** SQLite datasources name a missing file by relative path, by absolute path and by a
  `file:` URI, and each runs a query
- **THEN** each raises the typed database-unavailable error and no file is created

#### Scenario: In-memory forms are not files
- **WHEN** SQLite datasources use `sqlite://`, `sqlite:///:memory:` and a `file:` URI with
  `mode=memory`, and a DuckDB datasource uses `duckdb:///:memory:`, and each runs a query
- **THEN** each query runs on an in-memory database

## MODIFIED Requirements

### Requirement: Synchronous calls reuse connections
Synchronous entry points SHALL run their workflows on one long-lived event loop per process, so a
connection opened during one synchronous call SHALL serve later synchronous calls on the same
datasource. The end of a synchronous workflow — a query engine's synchronous call, or the default
client's synchronous statement execution on whichever driver path it takes — SHALL release the idle
connections that hold a local database file, and SHALL NOT close an in-memory database. A forked child
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

#### Scenario: The default client's synchronous call releases a local database file
- **WHEN** the default client executes a statement synchronously on a file-backed DuckDB datasource
  and the call returns
- **THEN** the same process can open that database file with a different configuration, and the
  client's next synchronous statement on it succeeds

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
