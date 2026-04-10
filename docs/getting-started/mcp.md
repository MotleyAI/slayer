# MCP Setup — AI Agents

Connect your AI agent (Claude Code, Cursor, etc.) to your database through SLayer's MCP server. No Python knowledge required.

## Install

```bash
uv tool install motley-slayer
```

For databases other than SQLite, add the driver extra (see [full list](../configuration/datasources.md#database-drivers)):

```bash
uv tool install 'motley-slayer[postgres]'
```

## Connect to your agent

### Claude Code (stdio — recommended)

```bash
claude mcp add slayer -- slayer mcp --storage ./slayer_data
```

If SLayer is in a virtualenv, use the full path to the executable:

```bash
claude mcp add slayer -- $(which slayer) mcp --storage /absolute/path/to/slayer_data
```

### Remote agents (HTTP/SSE)

Start the server, then point your agent at the SSE endpoint:

```bash
slayer serve --storage ./slayer_data

# In another terminal / agent config:
claude mcp add slayer-remote --transport sse --url http://localhost:5143/mcp/sse
```

## Connect a database

Once the agent is connected, it handles everything conversationally. A typical exchange:

> **You:** Connect to my Postgres database at localhost, database "myapp", user "analyst"
>
> **Agent:** *calls `create_datasource` → auto-ingests models → calls `datasource_summary`*
>
> "Connected! I found 4 tables: orders (12 dims, 8 measures), customers (5 dims, 3 measures), ..."
>
> **You:** How many orders per status?
>
> **Agent:** *calls `query(source_model="orders", fields=["*:count"], dimensions=["status"])`*

The agent uses these MCP tools in order:

1. `create_datasource` — connect to DB (auto-ingests models by default)
2. `datasource_summary` — discover available models and their schemas
3. `inspect_model` — see dimensions, measures, and sample data for a model
4. `query` — run queries

See the [MCP Reference](../reference/mcp.md) for the full tools list.

## Verify it works

Ask your agent:

> "List the available SLayer models"

The agent should call `datasource_summary` and return a list of your tables/models. If it says "no models found", check that:

1. The `--storage` path is correct
2. You've connected a datasource (or the agent has via `create_datasource`)
3. Models were ingested (auto-ingest runs by default with `create_datasource`)
