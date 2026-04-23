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

## Quick demo: Jaffle Shop

If you just want to kick the tyres, spin up the bundled Jaffle Shop dataset in one command:

```bash
slayer datasources create demo --ingest
slayer query '{"source_model": "orders", "fields": ["*:count"]}'
```

This generates ~1 year of synthetic coffee-shop data into a local DuckDB file under your storage directory and ingests the models (`customers`, `orders`, `order_items`, `products`, `stores`, `supplies`, `tweets`). Re-running is idempotent — the DuckDB is reused if it already exists. Override the years with `--years N`.

The demo requires `duckdb` and [`jafgen`](https://github.com/rossbowen/jaffle-shop-generator) (a git-only install):

```bash
pip install duckdb
pip install 'git+https://github.com/rossbowen/jaffle-shop-generator.git@09557a1118b000071f8171aa97d54d5029bf0f0b'
```

You can also preload the demo at server startup:

```bash
slayer serve --demo         # API server on :5143 with the demo ready to query
slayer mcp --demo           # MCP server with the demo ready for your agent
```

## Connect a database

Point `slayer datasources create` at a connection URL. The datasource name is derived from the database portion of the URL (override with `--name`). Pass `--ingest` to also auto-generate models in one shot:

```bash
# Postgres (use ${ENV_VAR} to keep secrets out of shell history)
slayer datasources create postgresql://analyst:${DB_PASSWORD}@localhost/myapp --ingest

# SQLite / DuckDB — name comes from the filename stem
slayer datasources create sqlite:///path/to/app.db --ingest

# Override the auto-derived name
slayer datasources create duckdb:///tmp/data.duckdb --name warehouse --ingest
```

Test the connection:

```bash
slayer datasources test myapp
# OK — connected to 'myapp' (postgres).
```

## Ingest models

`--ingest` on `create` is shorthand for creating the datasource and immediately running ingestion. To re-ingest an existing datasource later:

```bash
slayer ingest --datasource myapp
# Ingested: orders (6 dims, 12 measures)
# Ingested: customers (4 dims, 5 measures)
# Ingested: regions (3 dims, 2 measures)
```

Optionally filter tables:

```bash
slayer ingest --datasource myapp --schema public --include orders,customers
slayer ingest --datasource myapp --exclude migrations,django_session
```

The same `--schema`, `--include`, and `--exclude` flags work on `datasources create --ingest` too.

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
