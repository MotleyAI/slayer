# Datasources

Datasources configure database connections. They are stored as individual YAML files in the `datasources/` directory.

## YAML Format

```yaml
# slayer_data/datasources/my_postgres.yaml
name: my_postgres
type: postgres
host: localhost
port: 5432
database: myapp
username: myuser
password: mypassword
schema_name: public          # Optional: default schema
```

Or with a connection string:

```yaml
name: my_db
type: postgres
connection_string: postgresql://user:pass@host:5432/dbname
```

When configuring individual connection fields, enter credentials exactly as issued. SLayer URL-encodes
reserved characters when building the connection string. When supplying `connection_string` directly,
percent-encode reserved characters within credential components yourself; do not encode the URL's
structural delimiters.

## Environment Variables

Use `${VAR_NAME}` references for credentials — resolved at read time from the process environment:

```yaml
name: my_postgres
type: postgres
host: ${DB_HOST}
port: 5432
database: ${DB_NAME}
username: ${DB_USER}
password: ${DB_PASSWORD}
```

## Supported Database Types

SLayer uses [sqlglot](https://github.com/tobymao/sqlglot) for dialect-aware SQL generation. Databases are supported at two tiers:

### Database Drivers

#### First-class support

These databases are verified by integration tests and runnable Docker examples. Regressions are caught in CI.

| Type | Install Extra | Connection String |
|------|---------------|-------------------|
| `sqlite` | (built-in, no extra needed) | `sqlite:///path/to/db.sqlite` |
| `postgres` / `postgresql` | `motley-slayer[postgres]` | `postgresql://user:pass@localhost:5432/db` |
| `mysql` / `mariadb` | `motley-slayer[mysql]` | `mysql+pymysql://user:pass@localhost:3306/db` |
| `clickhouse` | `motley-slayer[clickhouse]` | `clickhouse+http://user:pass@localhost:8123/db` |
| `duckdb` | `motley-slayer[duckdb]` | `duckdb:///path/to/db.duckdb` |
| `snowflake` | `motley-slayer[snowflake]` | `snowflake://?connection_name=default` (TOML-driven) or `snowflake://user:pw@account/db/schema?warehouse=wh&role=role` (inline). See [Snowflake](#snowflake) below. |

#### Additional support

SQL generation is covered by unit tests, but not verified against live instances. Install the appropriate SQLAlchemy driver manually.

| Type | SQLAlchemy Driver | Install |
|------|-------------------|---------|
| `bigquery` | `sqlalchemy-bigquery` | `pip install sqlalchemy-bigquery` |
| `redshift` | `sqlalchemy-redshift` + `redshift_connector` | `pip install sqlalchemy-redshift redshift-connector` |
| `trino` / `presto` / `athena` | `trino` or `PyAthena` | `pip install trino` or `pip install PyAthena` |
| `databricks` / `spark` | `databricks-sql-connector` | `pip install databricks-sql-connector` |
| `oracle` | `oracledb` | `pip install oracledb` |
| `mssql` / `sqlserver` / `tsql` | `pyodbc` (auto-generated strings) or `pymssql` (manual `connection_string` only) | `pip install pyodbc` or `pip install pymssql` |

!!! warning "SQL Server — requires SQL Server 2022+"
    SLayer uses `DATETRUNC` for time-dimension queries, which was introduced in SQL Server 2022 (version 16.0).
    SQL Server 2019 and earlier will return an error on time-dimension queries.
    The Docker example uses `mcr.microsoft.com/mssql/server:2022-latest`.

!!! warning "SQL Server — TrustServerCertificate"
    Auto-generated SQL Server connection strings include `TrustServerCertificate=yes`, which disables
    TLS certificate validation. This is correct for local development and Docker environments that use
    self-signed certificates, but **must not be used in production** — it allows a man-in-the-middle
    attack on the database connection. For production, supply a `connection_string` field directly with
    a valid CA certificate chain, or configure your SQL Server instance with a certificate signed by a
    trusted CA and omit `TrustServerCertificate`.

!!! note
    BigQuery, ClickHouse, and similar analytical warehouses typically don't have foreign keys, so auto-ingestion won't discover joins. Define joins manually in your model YAML. Snowflake is an exception — it stores declarative (non-enforced) FK constraints AND exposes them via the Inspector, so auto-ingestion discovers joins like Postgres / MySQL / SQLite.

### Snowflake

The recommended path is the named-connection form, which delegates auth to
`snowflake.connector.connect(connection_name=...)` reading
`~/.snowflake/connections.toml`. This is the only path that supports key-pair,
OAuth, SSO, and MFA.

```toml
# ~/.snowflake/connections.toml
[default]
account = "jp13593"           # Snowflake account identifier, NOT a hostname
user = "YOUR_USER"
password = "YOUR_PASSWORD"
warehouse = "COMPUTE_WH"
database = "SLAYER_DEMO"
schema = "PUBLIC"
```

```yaml
# datasources/sf.yaml
name: sf
type: snowflake
connection_name: default
```

Or the inline form (host stores the Snowflake account identifier):

```yaml
name: sf
type: snowflake
host: jp13593
username: YOUR_USER
password: YOUR_PASSWORD
database: SLAYER_DEMO
schema_name: PUBLIC
warehouse: COMPUTE_WH
role: PUBLIC
```

Both forms flow through a shared engine factory that wires a per-connection
`USE WAREHOUSE / USE ROLE / USE DATABASE / USE SCHEMA` listener when those
typed fields are set. The `connection_name` profile's defaults are overridden
by anything you set on the DatasourceConfig.

Statement-level timeout is enforced via
`ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = N` on the connection.

!!! warning "Snowflake identifier casing"
    Snowflake stores unquoted identifiers in uppercase but resolves them
    case-insensitively. sqlglot's snowflake dialect emits bare lowercase
    identifiers, which resolve correctly against uppercase storage.
    Mixed-case names like `"Revenue"` get double-quoted by sqlglot and
    become case-sensitive — they must match the stored case exactly.

!!! tip
    If your database isn't listed but is supported by sqlglot, it may already work — SLayer falls back to Postgres-style SQL by default. Try it and [open an issue](https://github.com/MotleyAI/slayer/issues) if you hit a problem.

## Field Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique datasource name |
| `type` | string | No | Database type (see above) |
| `host` | string | No | Database host (default: localhost) |
| `port` | int | No | Database port |
| `database` | string | No | Database name |
| `username` | string | No | Database username |
| `password` | string | No | Database password |
| `connection_string` | string | No | Full connection string (alternative to individual fields) |
| `credentials_json` | string | No | Inline BigQuery service-account JSON; masked in REST and CLI datasource detail output |
| `schema_name` | string | No | Default schema name |

!!! note
    Both `username` and `user` field names are accepted. The `user` alias is automatically mapped to `username` for compatibility with common database tooling conventions.

## Ingesting at Startup

To run idempotent auto-ingestion across every configured datasource each time `slayer serve` or `slayer mcp` boots, pass `--ingest-on-startup` (or set `SLAYER_INGEST_ON_STARTUP=1`). See [Ingesting at Startup](../concepts/ingestion.md#ingesting-at-startup) for the full contract.

## Connection Testing

When creating a datasource via MCP (`create_datasource`) or `describe_datasource`, SLayer automatically tests the connection and reports success or failure with actionable error hints.

Common error hints:

| Error | Hint |
|-------|------|
| No password supplied | Check that username and password are correct |
| Database does not exist | Verify the database name |
| Connection refused | Check that the server is running and the port is correct |
| Host not found | Check the host address |
