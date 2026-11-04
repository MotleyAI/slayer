## ADDED Requirements

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

### Requirement: A bundled non-default driver warns until 2.0
A client whose driver is one a SLayer extra installs but not the type's default driver
(postgres/postgresql: psycopg2 and asyncpg; mysql/mariadb: aiomysql) SHALL emit a deprecation
warning naming the driver and the extra and stating that from SLayer 2.0 the extra no longer
installs it. A client on the type's default driver, or on a driver no SLayer extra installs, SHALL
NOT emit it.

#### Scenario: psycopg2 warns
- **WHEN** a postgres datasource whose URL names `psycopg2` builds its client's connection
- **THEN** a deprecation warning names `psycopg2` and `motley-slayer[postgres]`

#### Scenario: asyncpg warns
- **WHEN** a postgres datasource whose URL names `asyncpg` builds its client's connection
- **THEN** a deprecation warning names `asyncpg` and `motley-slayer[postgres]`

#### Scenario: aiomysql warns
- **WHEN** a mysql datasource whose URL names `aiomysql` builds its client's connection
- **THEN** a deprecation warning names `aiomysql` and `motley-slayer[mysql]`

#### Scenario: Default and unbundled drivers are silent
- **WHEN** clients connect with a postgres URL naming no driver or naming `psycopg`, a mysql URL
  naming `pymysql`, and a postgres URL naming `pg8000`
- **THEN** none emits the deprecation warning

### Requirement: A missing driver raises a typed, actionable error
When connecting to a datasource needs a Python driver module or SQLAlchemy dialect plugin that is
not installed — whether the client runs it synchronously or asynchronously, or it is a dialect's own
lazily imported driver — SLayer SHALL raise a missing-driver error that is both a SLayer error and
an import error. Its message SHALL name the datasource, its type, and the missing module (or, for a
missing dialect plugin, the URL scheme whose plugin could not be loaded), and SHALL carry an install
hint:
- when the connection URL selects a driver that a SLayer extra installs for the type — its default
  driver, or a bundled non-default driver (postgres/postgresql: psycopg2 and asyncpg; mysql/mariadb:
  aiomysql) — the hint SHALL be `pip install 'motley-slayer[<extra>]'` with that type's extra
  (postgres/postgresql, mysql, mariadb, clickhouse, sqlserver/mssql/tsql, snowflake, bigquery,
  trino);
- when the URL selects SLayer's default driver for a type without an extra, the hint SHALL tell the
  user to install the named module or plugin and point to the datasource documentation;
- when the URL names a driver no SLayer extra installs, the hint SHALL tell the user to install the
  driver named in their `connection_string`, and SHALL NOT recommend a SLayer extra.

A missing driver SHALL NOT be replaced by another driver. An import failure that is not attributable
to the datasource's driver or dialect plugin SHALL propagate unchanged.

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

#### Scenario: Missing bundled driver names the extra and is not replaced
- **WHEN** a postgres datasource whose URL names `asyncpg` executes a statement and `asyncpg` is not
  importable
- **THEN** a missing-driver error naming `asyncpg` and `pip install 'motley-slayer[postgres]'` is
  raised, and the statement is not run through any other driver

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

## MODIFIED Requirements

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

## REMOVED Requirements

### Requirement: The asynchronous path honours the user's driver choice
**Reason**: A client no longer switches a URL between a synchronous and an asynchronous driver; it
connects through the one driver its URL names, else the type's default ("A client connects through
one driver").
**Migration**: A URL naming `psycopg2` or `pymysql` now runs statements on that driver in a worker
thread; name an asynchronous driver (`postgresql+psycopg`, `postgresql+asyncpg`, `mysql+aiomysql`),
or for Postgres name none, to run statements asynchronously.

### Requirement: An async-only driver URL still gets a synchronous engine
**Reason**: An asynchronous driver now serves metadata reads itself ("A client connects through one
driver"); no synchronous engine on another driver is built.
**Migration**: None needed: a URL naming `asyncpg` or `aiomysql` keeps working for statements and
metadata, with its query parameters read by that driver alone.

### Requirement: A missing database driver raises a typed, actionable error
**Reason**: Reworked as "A missing driver raises a typed, actionable error": the extra hint now
covers the bundled non-default drivers, and the default Postgres driver is psycopg.
**Migration**: None needed; the error type and hint shapes are unchanged.

### Requirement: A connection URL that names no driver uses SLayer's default driver
**Reason**: Reworked as "A connection URL that names no driver uses the type's default driver": the
Postgres default driver is psycopg, and the asynchronous-path cross-reference is gone.
**Migration**: A Postgres URL naming no driver connects with psycopg; install
`motley-slayer[postgres]` (which installs it), or name `postgresql+psycopg2` to keep psycopg2.
