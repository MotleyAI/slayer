## REMOVED Requirements

### Requirement: A bundled non-default driver warns until 2.0
**Reason**: From SLayer 2.0 the extras no longer install psycopg2, asyncpg or aiomysql, which the warning announced; each extra installs only its type's default driver.
**Migration**: Depend on psycopg2, asyncpg or aiomysql directly when a `connection_string` names it.

### Requirement: A missing driver raises a typed, actionable error
**Reason**: Restated in "A missing driver raises a typed error with an install hint": no extra installs a non-default driver any more, so only the default driver's hint names an extra.
**Migration**: None.

## ADDED Requirements

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
