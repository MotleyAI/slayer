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

| Type | Install Extra | Connection Driver | Example |
|------|---------------|-------------------|---------|
| `postgres` / `postgresql` | `pip install agentic-slayer[postgres]` | `postgresql://` | `postgresql://user:pass@localhost:5432/db` |
| `mysql` / `mariadb` | `pip install agentic-slayer[mysql]` | `mysql+pymysql://` | `mysql+pymysql://user:pass@localhost:3306/db` |
| `clickhouse` | `pip install agentic-slayer[clickhouse]` | `clickhouse+http://` | `clickhouse+http://user:pass@localhost:8123/db` |
| `sqlite` | (built-in) | `sqlite:///` | `sqlite:///path/to/db.sqlite` |
| `bigquery` | Via connection string | BigQuery adapter | Via connection string |
| `snowflake` | Via connection string | Snowflake adapter | Via connection string |

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
