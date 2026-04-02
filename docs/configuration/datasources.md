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

### First-class support

These databases are verified by integration tests and runnable Docker examples. Regressions are caught in CI.

| Type | Install Extra | Connection Driver | Example |
|------|---------------|-------------------|---------|
| `postgres` / `postgresql` | `pip install motley-slayer[postgres]` | `postgresql://` | `postgresql://user:pass@localhost:5432/db` |
| `mysql` / `mariadb` | `pip install motley-slayer[mysql]` | `mysql+pymysql://` | `mysql+pymysql://user:pass@localhost:3306/db` |
| `clickhouse` | `pip install motley-slayer[clickhouse]` | `clickhouse+http://` | `clickhouse+http://user:pass@localhost:8123/db` |
| `sqlite` | (built-in) | `sqlite:///` | `sqlite:///path/to/db.sqlite` |

### Additional support

These databases have SQL generation covered by unit tests, but are not verified against live instances yet.

| Type | Notes |
|------|-------|
| `snowflake` | Analytical/cloud warehouse; no foreign keys (like ClickHouse), so auto-ingestion won't discover joins |
| `bigquery` | Analytical/cloud warehouse; no foreign keys, same caveat as Snowflake |
| `redshift` | Postgres-based cloud warehouse; FKs are informational only (not enforced) |
| `duckdb` | Fully Postgres-compatible; great for local analytics and testing |
| `trino` / `presto` / `athena` | Federated query engines; no FKs, schema depends on the underlying connector |
| `databricks` / `spark` | Spark SQL-based; no FKs |
| `oracle` / `mssql` / `sqlserver` / `tsql` | Broadly compatible with Postgres feature set |

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
| `schema_name` | string | No | Default schema name |

!!! note
    Both `username` and `user` field names are accepted. The `user` alias is automatically mapped to `username` for compatibility with common database tooling conventions.

## Connection Testing

When creating a datasource via MCP (`create_datasource`) or `describe_datasource`, SLayer automatically tests the connection and reports success or failure with actionable error hints.

Common error hints:

| Error | Hint |
|-------|------|
| No password supplied | Check that username and password are correct |
| Database does not exist | Verify the database name |
| Connection refused | Check that the server is running and the port is correct |
| Host not found | Check the host address |
