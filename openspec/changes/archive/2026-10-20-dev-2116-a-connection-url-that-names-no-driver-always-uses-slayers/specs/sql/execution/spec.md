## ADDED Requirements

### Requirement: A connection URL that names no driver uses SLayer's default driver
For a datasource type that declares a default driver (postgres/postgresql: psycopg2; mysql/mariadb:
pymysql; clickhouse: http; sqlserver/mssql/tsql: pyodbc), a connection URL — the datasource's
`connection_string`, or the URL built from its structured fields — whose backend is one the type
serves (postgres/postgresql: `postgresql`; mysql/mariadb: `mysql` and `mariadb`; clickhouse:
`clickhouse`; sqlserver/mssql/tsql: `mssql`) and which names no driver SHALL connect with the type's
default driver on the URL's own backend, whatever the installed SQLAlchemy's own default driver for
that backend is. Credentials, host, port, database and query parameters SHALL be preserved.

A URL that names a driver, whose backend the type does not serve, or that cannot be parsed as a
URL, and every URL of a type without a declared default driver, SHALL be used exactly as given. A
stored `connection_string` SHALL NOT be rewritten.

#### Scenario: Bare Postgres URL uses psycopg2
- **WHEN** a postgres datasource's `connection_string` is
  `postgresql://u:p%40ss@h:5432/d?sslmode=require`
- **THEN** it connects as `postgresql+psycopg2://u:p%40ss@h:5432/d?sslmode=require`

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
- **WHEN** a postgres datasource's `connection_string` is `postgresql+psycopg://u@h/d` or
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

#### Scenario: Missing default driver names the extra
- **WHEN** a postgres datasource's `connection_string` is `postgresql://u@h/d` and `psycopg2` is not
  importable
- **THEN** a missing-driver error naming `psycopg2` and `pip install 'motley-slayer[postgres]'` is
  raised

#### Scenario: Stored configuration is not rewritten
- **WHEN** a datasource with `connection_string` `postgresql://u@h/d` is used and then read back
- **THEN** its `connection_string` is still `postgresql://u@h/d`

## MODIFIED Requirements

### Requirement: The asynchronous path honours the user's driver choice
For a datasource type with a native asynchronous driver (postgres/postgresql: asyncpg;
mysql/mariadb: aiomysql), the asynchronous connection SHALL be derived from the datasource's
connection URL — after a URL naming no driver has been given the type's default driver — as follows,
preserving the backend and every other URL part (credentials, host, port, database, query):
- a URL that names an asynchronous-capable driver SHALL be used unchanged;
- a URL that names the type's default synchronous driver (postgres: psycopg2; mysql/mariadb:
  pymysql) SHALL use the type's native asynchronous driver;
- a URL that names any other synchronous-only driver, or whose backend is not one the type serves,
  SHALL run on the synchronous path in a worker thread.

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

#### Scenario: MariaDB keeps its backend
- **WHEN** a mariadb datasource's URL is `mariadb://u@h/d` or `mariadb+pymysql://u@h/d`
- **THEN** the asynchronous connection URL is `mariadb+aiomysql://u@h/d`

#### Scenario: Another named sync driver stays synchronous
- **WHEN** a postgres datasource's URL is `postgresql+pg8000://…`, or a mysql datasource's URL is
  `mysql+mysqldb://…`
- **THEN** statements run on the synchronous path in a worker thread with the named driver

#### Scenario: Type without an async driver
- **WHEN** a Snowflake datasource executes asynchronously
- **THEN** statements run on the synchronous path in a worker thread

### Requirement: Structured-config connection URLs are stable
A datasource configured by fields rather than a `connection_string` SHALL produce a connection URL
whose scheme names the type's default driver where the type declares one; a datasource type that no
registered dialect declares SHALL keep `<type>://` as its URL scheme.

#### Scenario: Known types keep their scheme
- **WHEN** structured postgres, mysql, mariadb, clickhouse and sqlserver datasources build their URLs
- **THEN** the schemes are `postgresql+psycopg2`, `mysql+pymysql`, `mysql+pymysql`,
  `clickhouse+http` and `mssql+pyodbc`

#### Scenario: Unknown type keeps its own scheme
- **WHEN** a structured datasource of an unregistered type `foo` builds its URL
- **THEN** the URL scheme is `foo`, not the Postgres fallback dialect's scheme
