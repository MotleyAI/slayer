# CLI

SLayer provides a command-line interface for server management, querying, and model operations.

## Commands

### `slayer serve`

Start the HTTP server (REST API + MCP SSE endpoint at `/mcp/sse`).

```bash
slayer serve --models-dir ./slayer_data
slayer serve --host 0.0.0.0 --port 8080 --models-dir ./slayer_data
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `5143` | Port number |
| `--models-dir` | `./slayer_data` | Storage directory |

### `slayer mcp`

Run SLayer as an MCP server using stdio transport. This command is **not meant to be run manually** — it is spawned by an AI agent (Claude Code, Cursor, etc.) as a subprocess. To set it up, register the command with your agent:

```bash
# Register with Claude Code (the agent will spawn the process)
claude mcp add slayer -- slayer mcp --models-dir ./slayer_data

# If slayer is in a virtualenv, use the full executable path:
#   claude mcp add slayer -- $(poetry env info -p)/bin/slayer mcp --models-dir /abs/path/to/slayer_data
```

For MCP over HTTP (SSE), use `slayer serve` instead — it exposes MCP at `/mcp/sse` alongside the REST API.

| Flag | Default | Description |
|------|---------|-------------|
| `--models-dir` | `./slayer_data` | Storage directory |

### `slayer query`

Execute a query from the terminal.

```bash
# Inline JSON
slayer query '{"source_model": "orders", "fields": [{"formula": "count"}], "dimensions": [{"name": "status"}]}'

# From a file
slayer query @query.json

# JSON output
slayer query '{"source_model": "orders", "fields": [{"formula": "count"}]}' --format json
```

| Flag | Default | Description |
|------|---------|-------------|
| `--models-dir` | `./slayer_data` | Storage directory |
| `--format` | `table` | Output format: `table` or `json` |

### `slayer ingest`

Auto-generate models from a datasource.

```bash
slayer ingest --datasource my_postgres --schema public --models-dir ./slayer_data
```

| Flag | Required | Description |
|------|----------|-------------|
| `--datasource` | Yes | Datasource name |
| `--schema` | No | Database schema to inspect |
| `--models-dir` | No | Storage directory |

### `slayer models`

Manage models.

```bash
slayer models list --models-dir ./slayer_data
slayer models show orders
slayer models create model.yaml
slayer models delete orders
```

### `slayer datasources`

Manage datasources.

```bash
slayer datasources list --models-dir ./slayer_data
slayer datasources show my_postgres   # credentials masked
```
