# MCP Setup — AI Agents

Connect your AI agent (Claude Code, Cursor, etc.) to your database through SLayer's MCP server. No Python knowledge required.

## Prerequisites

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) — the fast Python package manager. SLayer runs via `uvx` with no separate install step.

## Connect to your agent

### Claude Code

Register SLayer as an MCP server — Claude Code will spawn it automatically when needed:

```bash
claude mcp add slayer -- uvx --from 'motley-slayer[mcp]' slayer mcp --storage ./slayer_data
```

For databases other than SQLite, add the driver extra (see [full list](../configuration/datasources.md#database-drivers)):

```bash
claude mcp add slayer -- uvx --from 'motley-slayer[mcp,postgres]' slayer mcp --storage ./slayer_data
```

### Other agents (JSON config)

Most MCP-compatible agents accept a JSON server configuration. Add this to your agent's MCP config file:

```json
{
  "mcpServers": {
    "slayer": {
      "command": "uvx",
      "args": ["--from", "motley-slayer[mcp,postgres]", "slayer", "mcp", "--storage", "./slayer_data"]
    }
  }
}
```

Replace `postgres` with your database driver, or use `motley-slayer[all]` for all supported databases.

### Remote / shared server

SLayer also supports HTTP/SSE transport for running on a different machine, in Docker, or sharing between multiple agents. See the [MCP Reference](../reference/mcp.md#sse-remote) for details.

### Verify

```bash
claude mcp list
```

## Connect a database

The recommended approach is to drop a datasource YAML file into your storage folder. This keeps credentials out of the agent conversation and lets you use environment variable references.

Create a file at `slayer_data/datasources/mydb.yaml`:

```yaml
name: mydb
type: postgres
host: ${DB_HOST}
port: 5432
database: ${DB_NAME}
username: ${DB_USER}
password: ${DB_PASSWORD}
schema_name: public
```

`${...}` references are resolved from environment variables at read time. Set them in your shell before starting the agent, or use a `.env` file with your agent's environment configuration.

Datasource configs are **hot-reloaded** — you can add or edit YAML files while the server is running, and the next MCP tool call will pick up the changes. No restart needed.

Once the datasource file is in place, ask your agent:

> "Ingest models from the mydb datasource and show me what's available"

The agent will call `ingest_datasource_models` to generate models from the database schema, then `datasource_summary` to list them.

You can also create datasources conversationally via the `create_datasource` MCP tool — see the [MCP Reference](../reference/mcp.md#datasource-management) for details.

## Verify it works

Ask your agent:

> "List the available SLayer models"

The agent should call `datasource_summary` and return a list of your tables/models. If it says "no models found", check that:

1. The `--storage` path matches where your datasource YAML files are
2. Models have been ingested (via `ingest_datasource_models` or `create_datasource` with auto-ingest)
3. Environment variables referenced in the datasource config are set

## Alternative: permanent install

If you prefer a traditional install instead of `uvx`:

```bash
uv tool install 'motley-slayer[mcp,postgres]'
claude mcp add slayer -- slayer mcp --storage ./slayer_data
```
