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

If unset, SLayer uses a platform-appropriate path: `~/.local/share/slayer` on Linux, `~/Library/Application Support/slayer` on macOS, `%LOCALAPPDATA%\slayer` on Windows. Override with `$SLAYER_STORAGE`.

Another environment variable, `SLAYER_INGEST_ON_STARTUP`, mirrors the `--ingest-on-startup` flag on `slayer serve` / `slayer mcp` — truthy values (`1`, `true`, `yes`, case-insensitive) enable boot-time idempotent auto-ingestion across every configured datasource. See [Ingesting at Startup](../concepts/ingestion.md#ingesting-at-startup).

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
| `--storage` | platform-appropriate path | Storage path (directory for YAML, `.db` file for SQLite) |
| `--demo` | off | Spin up the bundled Jaffle Shop DuckDB datasource and ingest its models on startup. Idempotent; requires the `duckdb` extra and `jafgen`. |
| `--ingest-on-startup` | off | Walk every configured datasource and run idempotent auto-ingestion before the port opens. Per-datasource errors are logged to stderr and never abort startup. Also enabled by `SLAYER_INGEST_ON_STARTUP=1`. |

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
| `--storage` | platform-appropriate path | Storage path (directory for YAML, `.db` file for SQLite) |
| `--demo` | off | Spin up the bundled Jaffle Shop DuckDB datasource and ingest its models on startup. Idempotent; requires the `duckdb` extra and `jafgen`. |
| `--ingest-on-startup` | off | Walk every configured datasource and run idempotent auto-ingestion before stdio JSON-RPC starts. Per-datasource errors are logged to stderr and never abort startup. Also enabled by `SLAYER_INGEST_ON_STARTUP=1`. |

### `slayer query`

Execute a query from the terminal.

```bash
# Inline JSON
slayer query '{"source_model": "orders", "measures": ["*:count"], "dimensions": ["status"]}'

# From a file
slayer query @query.json

# Run a saved query-backed model by name
slayer query monthly_revenue

# Pass runtime variables (always overrides query.variables / model.query_variables)
slayer query monthly_revenue --variables region=US --variables threshold=100
slayer query @query.json --variables-json '{"region": "US"}'

# JSON output
slayer query '{"source_model": "orders", "measures": ["*:count"]}' --format json

# Preview SQL without executing
slayer query '{"source_model": "orders", "measures": ["*:count"]}' --dry-run

# Show execution plan
slayer query @query.json --explain
```

The positional argument is interpreted as:

- a JSON query if it starts with `{` or `[`,
- a file path if it starts with `@`,
- otherwise, a **model name** — runs the stored backing query for the named query-backed model.

| Flag | Default | Description |
|------|---------|-------------|
| `--storage` | platform-appropriate path | Storage path (directory for YAML, `.db` file for SQLite) |
| `--format` | `table` | Output format: `table` or `json` |
| `--dry-run` | | Generate SQL without executing |
| `--explain` | | Run EXPLAIN ANALYZE on the query |
| `--variables KEY=VALUE` | | Runtime variable, repeatable. Overrides `query.variables` and `model.query_variables`. |
| `--variables-json '{...}'` | | Runtime variables from a JSON object. Mutually exclusive with `--variables`. |

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
