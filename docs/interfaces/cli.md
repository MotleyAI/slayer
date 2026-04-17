# CLI

SLayer provides a command-line interface for server management, querying, and model operations.

## Storage

All commands accept a `--storage` flag to specify where models and datasources are stored:

```bash
# YAML files in a directory (default)
slayer serve --storage ./slayer_data

# SQLite database file (auto-detected by .db/.sqlite/.sqlite3 extension)
slayer serve --storage slayer.db
```

The default is `./slayer_data` (YAML). Override with `$SLAYER_STORAGE` or `$SLAYER_MODELS_DIR` env vars.

The legacy `--models-dir` flag still works but is deprecated in favor of `--storage`.

## Commands

### `slayer serve`

Start the HTTP server (REST API + MCP SSE endpoint at `/mcp/sse`).

```bash
slayer serve
slayer serve --host 0.0.0.0 --port 8080
slayer serve --storage slayer.db
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `5143` | Port number |
| `--storage` | `./slayer_data` | Storage path (directory for YAML, .db file for SQLite) |

### `slayer mcp`

Run SLayer as an MCP server using stdio transport. This command is **not meant to be run manually** — it is spawned by an AI agent (Claude Code, Cursor, etc.) as a subprocess. To set it up, register the command with your agent:

```bash
# Register with Claude Code (the agent will spawn the process)
claude mcp add slayer -- slayer mcp --storage ./slayer_data

# If slayer is in a virtualenv, use the full executable path:
#   claude mcp add slayer -- $(poetry env info -p)/bin/slayer mcp --storage /abs/path/to/slayer_data
```

For MCP over HTTP (SSE), use `slayer serve` instead — it exposes MCP at `/mcp/sse` alongside the REST API.

| Flag | Default | Description |
|------|---------|-------------|
| `--storage` | `./slayer_data` | Storage path |

### `slayer query`

Execute a query from the terminal.

```bash
# Inline JSON
slayer query '{"source_model": "orders", "fields": ["*:count"], "dimensions": ["status"]}'

# From a file
slayer query @query.json

# JSON output
slayer query '{"source_model": "orders", "fields": ["*:count"]}' --format json

# Preview SQL without executing
slayer query '{"source_model": "orders", "fields": ["count"]}' --dry-run

# Show execution plan
slayer query @query.json --explain
```

| Flag | Default | Description |
|------|---------|-------------|
| `--storage` | `./slayer_data` | Storage path |
| `--format` | `table` | Output format: `table` or `json` |
| `--dry-run` | | Generate SQL without executing |
| `--explain` | | Run EXPLAIN ANALYZE on the query |

### `slayer ingest`

Auto-generate models from a datasource.

```bash
slayer ingest --datasource my_postgres
slayer ingest --datasource my_postgres --schema public
slayer ingest --datasource my_postgres --include orders,customers
slayer ingest --datasource my_postgres --exclude migrations,django_session
```

| Flag | Required | Description |
|------|----------|-------------|
| `--datasource` | Yes | Datasource name |
| `--schema` | No | Database schema to inspect |
| `--include` | No | Comma-separated tables to include |
| `--exclude` | No | Comma-separated tables to exclude |
| `--storage` | No | Storage path |

### `slayer import-dbt`

Import dbt Semantic Layer definitions into SLayer.

```bash
slayer import-dbt ./my_dbt_project --datasource my_postgres
slayer import-dbt ./my_dbt_project --datasource my_postgres --include-hidden-models
```

| Flag | Required | Description |
|------|----------|-------------|
| `dbt_project_path` | Yes | Path to the dbt project root (or a models directory) |
| `--datasource` | Yes | SLayer datasource name for the imported models |
| `--no-strict-aggregations` | No | Don't restrict measures to their dbt-defined aggregation types |
| `--include-hidden-models` | No | Also import regular dbt models (those not wrapped by a `semantic_model`) as hidden SLayer models via SQL introspection. Requires the `dbt` extra. |
| `--storage` | No | Storage path |

### `slayer models`

Manage models.

```bash
slayer models list
slayer models show orders
slayer models create model.yaml
slayer models delete orders
```

### `slayer datasources`

Manage datasources.

```bash
slayer datasources list
slayer datasources show my_postgres   # credentials masked
```

### `slayer help`

Show SLayer's conceptual help — the same content the MCP `help()` tool returns.
Intended to complement the schema/reference pages: it covers how concepts
compose (query evaluation order, transform trade-offs, cross-model measures,
the three meanings of "last") rather than restating field-by-field schemas.

```bash
slayer help                  # intro (core entities, query shape, key invariants)
slayer help queries          # deep dive on query anatomy
slayer help transforms       # cumsum, time_shift, lag/lead trade-offs
slayer help --help           # argparse-level help lists every topic
```

Topics: `queries`, `formulas`, `aggregations`, `transforms`, `time`, `filters`,
`joins`, `models`, `extending`, `workflow`. Content lives in
`slayer/help/topics/*.md` and is discovered dynamically — dropping a new `.md`
in that directory adds a topic with no Python changes. See the corresponding
concept docs for full treatments: [queries](../concepts/queries.md),
[formulas](../concepts/formulas.md), [models](../concepts/models.md),
[ingestion](../concepts/ingestion.md).
