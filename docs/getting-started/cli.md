# CLI Setup — Terminal Users

Query your database from the command line. No Python code needed — just install and go.

## Install

```bash
uv tool install motley-slayer
```

For databases other than SQLite, add the driver extra (see [full list](../configuration/datasources.md#database-drivers)):

```bash
uv tool install 'motley-slayer[postgres]'
```

## Connect a database

Create a datasource — either from a YAML file or inline:

```bash
# Inline (quick setup — use ${ENV_VAR} for secrets)
slayer datasources create-inline my_pg \
  --type postgres \
  --host localhost \
  --database myapp \
  --username analyst \
  --password-stdin

# Or from a YAML file
slayer datasources create datasource.yaml
```

YAML file format:

```yaml
# datasource.yaml
name: my_pg
type: postgres
host: localhost
port: 5432
database: myapp
username: analyst
password: ${DB_PASSWORD}
```

Test the connection:

```bash
slayer datasources test my_pg
# OK — connected to 'my_pg' (postgres).
```

## Ingest models

Auto-generate models from your database schema:

```bash
slayer ingest --datasource my_pg
# Ingested: orders (6 dims, 12 measures)
# Ingested: customers (4 dims, 5 measures)
# Ingested: regions (3 dims, 2 measures)
```

Optionally filter tables:

```bash
slayer ingest --datasource my_pg --schema public --include orders,customers
slayer ingest --datasource my_pg --exclude migrations,django_session
```

## Query

```bash
# Count orders by status
slayer query '{"source_model": "orders", "fields": ["*:count"], "dimensions": ["status"]}'

# From a file
slayer query @query.json

# Output as JSON (pipe-friendly)
slayer query @query.json --format json

# Preview the generated SQL without running it
slayer query @query.json --dry-run

# Show execution plan
slayer query @query.json --explain
```

## Explore models

```bash
slayer models list
slayer models show orders
slayer datasources list
```

## Verify it works

After install + ingest, this should return data:

```bash
slayer query '{"source_model": "orders", "fields": ["*:count"]}'
```

Expected output:

```
orders.count
------------
42

1 row(s)
```

If you see "Model 'orders' not found", check that `slayer ingest` ran successfully and that `--storage` points to the right location.

## Start a server (optional)

If you also want a REST API or MCP endpoint:

```bash
slayer serve                           # REST API at http://localhost:5143
slayer serve --storage slayer.db       # Using SQLite storage
```

See the [CLI Reference](../reference/cli.md) for all commands and flags.
