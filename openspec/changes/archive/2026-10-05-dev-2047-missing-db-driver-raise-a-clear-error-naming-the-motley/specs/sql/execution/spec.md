## ADDED Requirements

### Requirement: A missing database driver raises a typed, actionable error
When connecting to a datasource needs a Python driver module or SQLAlchemy dialect plugin that is
not installed — on the synchronous path, the asynchronous path, or a dialect's own lazily imported
driver — SLayer SHALL raise a missing-driver error that is both a SLayer error and an import error.
Its message SHALL name the datasource, its type, and the missing module (or, for a missing dialect
plugin, the URL scheme whose plugin could not be loaded), and SHALL carry an install hint:
- when the connection URL selects SLayer's default driver for a type that has a SLayer extra
  (postgres/postgresql, mysql, mariadb, clickhouse, sqlserver/mssql/tsql, snowflake, bigquery), the
  hint SHALL be `pip install 'motley-slayer[<extra>]'` with that type's extra;
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
  snowflake and bigquery is not importable and an engine is built for that type
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

### Requirement: The asynchronous path honours the user's driver choice
For a datasource type with a native asynchronous driver (postgres/postgresql: asyncpg;
mysql/mariadb: aiomysql), the asynchronous connection SHALL be derived from the datasource's
connection URL as follows, preserving every other URL part (credentials, host, port, database,
query):
- a URL that names an asynchronous-capable driver SHALL be used unchanged;
- a URL that names no driver, or names the type's default synchronous driver (postgres: psycopg2;
  mysql/mariadb: pymysql), SHALL use the type's native asynchronous driver;
- a URL that names any other synchronous-only driver, or whose backend is not the type's own, SHALL
  run on the synchronous path in a worker thread.

A datasource type without a native asynchronous driver SHALL run on the synchronous path in a
worker thread.

#### Scenario: psycopg v3 is kept
- **WHEN** a postgres datasource's `connection_string` is `postgresql+psycopg://u:p@h:5432/d`
- **THEN** the asynchronous connection uses psycopg with the same URL, and asyncpg is not required

#### Scenario: Plain and default-sync URLs move to asyncpg
- **WHEN** a postgres datasource's URL is `postgresql://u:p@h:5432/d?sslmode=require` or
  `postgresql+psycopg2://u:p@h:5432/d?sslmode=require`
- **THEN** the asynchronous connection URL is `postgresql+asyncpg://u:p@h:5432/d?sslmode=require`

#### Scenario: MySQL follows the same rule
- **WHEN** a mysql datasource's URL is `mysql://…` or `mysql+pymysql://…`, or is
  `mysql+asyncmy://…`
- **THEN** the first two use aiomysql and the third is used unchanged

#### Scenario: Another named sync driver stays synchronous
- **WHEN** a postgres datasource's URL is `postgresql+pg8000://…`, or a mysql datasource's URL is
  `mysql+mysqldb://…`
- **THEN** statements run on the synchronous path in a worker thread with the named driver

#### Scenario: Type without an async driver
- **WHEN** a Snowflake datasource executes asynchronously
- **THEN** statements run on the synchronous path in a worker thread

### Requirement: Structured-config connection URLs are stable
A datasource configured by fields rather than a `connection_string` SHALL produce the same
connection URL as before this change for every type; a datasource type that no registered dialect
declares SHALL keep `<type>://` as its URL scheme.

#### Scenario: Known types keep their scheme
- **WHEN** structured postgres, mysql, mariadb and clickhouse datasources build their URLs
- **THEN** the schemes are `postgresql`, `mysql+pymysql`, `mysql+pymysql` and `clickhouse+http`

#### Scenario: Unknown type keeps its own scheme
- **WHEN** a structured datasource of an unregistered type `foo` builds its URL
- **THEN** the URL scheme is `foo`, not the Postgres fallback dialect's scheme
