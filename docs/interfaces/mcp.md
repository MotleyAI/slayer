# MCP Server

SLayer runs as an [MCP](https://modelcontextprotocol.io/) server, allowing AI agents (Claude, Cursor, etc.) to discover and query data conversationally.

## Transports

SLayer supports two MCP transports. Both expose the exact same tools — the only difference is how the agent connects.

### Stdio (local)

The agent spawns SLayer as a subprocess and communicates via stdin/stdout. You do **not** run `slayer mcp` manually — the agent launches it. You only need to register the command with your agent.

**Claude Code setup:**

```bash
claude mcp add slayer -- slayer mcp --models-dir ./slayer_data
```

If `slayer` is installed in a virtualenv (e.g. via Poetry), use the full path to the executable so the agent can find it regardless of working directory:

```bash
# Find the virtualenv path
poetry env info -p
# e.g. /home/user/.venvs/slayer-abc123

# Register with the full path
claude mcp add slayer -- /home/user/.venvs/slayer-abc123/bin/slayer mcp --models-dir /path/to/slayer_data
```

### SSE (remote)

MCP over HTTP via Server-Sent Events. You run `slayer serve` yourself — it exposes both the REST API and the MCP SSE endpoint on the same port:

```bash
# 1. Start the server
slayer serve --models-dir ./slayer_data
# REST API at http://localhost:5143/
# MCP SSE at http://localhost:5143/mcp/sse
```

Then, in a separate terminal, register the remote endpoint with your agent:

```bash
# 2. Connect the agent
claude mcp add slayer-remote --transport sse --url http://localhost:5143/mcp/sse
```

This is useful when SLayer runs on a different machine, in Docker, or when multiple agents need to share the same server.

### Verify

```bash
claude mcp list
```

## Tools Reference

### Datasource Management

| Tool | Description |
|------|-------------|
| `create_datasource` | Create a DB connection, test it, and auto-ingest models (set `auto_ingest=false` to skip). |
| `list_datasources` | List configured datasources (no credentials shown). |
| `describe_datasource` | Show details, test connection, list available schemas. |
| `list_tables` | List tables in a database before ingesting. |
| `edit_datasource` | Edit an existing datasource config. |
| `delete_datasource` | Remove a datasource config. |

### Model Management

| Tool | Description |
|------|-------------|
| `datasource_summary` | List all datasources and their models with schemas (dimensions, measures). Returns JSON. |
| `inspect_model` | Detailed model info with sample data. Params: `model_name`, `num_rows` (default 3), `show_sql` (default false). |
| `create_model` | Create a new model from table/SQL definition. |
| `edit_model` | Edit an existing model in one call. Params: `model_name` (required), `description`, `data_source`, `default_time_dimension` (optional metadata), `add_measures` (list), `add_dimensions` (list), `remove` (list of names). |
| `delete_model` | Delete a model entirely. |

### Querying

| Tool | Description |
|------|-------------|
| `query` | Execute a semantic query. See [Queries](../concepts/queries.md) for format. |

**`query` parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `model` | string | Model name (required) |
| `fields` | list[dict] | Data columns: measures, arithmetic, transforms. E.g. `[{"formula": "count"}, {"formula": "revenue / count", "name": "aov", "label": "Average Order Value"}, {"formula": "cumsum(revenue)"}]`. Each field has an optional `label` for human-readable display. Supports nesting: `{"formula": "change(cumsum(revenue))"}` |
| `dimensions` | list[str] | Dimension names, e.g. `["status"]`. When using the engine directly, dimensions accept an optional `label` via `ColumnRef(name="status", label="Order Status")`. |
| `filters` | list[str] | Filter formula strings, e.g. `["status == 'active'", "amount > 100"]`. Supports operators (`==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `is None`, `is not None`, `like`, `not like`), boolean logic (`and`, `or`, `not`), and inline transform expressions (`"change(revenue) > 0"`). Filters on measures are automatically routed to HAVING. |
| `time_dimensions` | list[dict] | Time grouping. Each entry supports an optional `label` for display. |
| `order` | list[dict] | Sorting, e.g. `[{"column": "count", "direction": "desc"}]` |
| `limit` | int | Max rows |
| `offset` | int | Skip rows |
| `whole_periods_only` | bool | Snap date filters to time bucket boundaries, exclude the current incomplete time bucket |
| `show_sql` | bool | Include the generated SQL in the response for debugging |

### Ingestion

| Tool | Description |
|------|-------------|
| `ingest_datasource_models` | Auto-generate models from DB schema with rollup joins. Params: `datasource_name`, `include_tables`, `schema_name`. |

## Typical Agent Workflows

### Connect and explore a new database

```
1. create_datasource(name="mydb", type="postgres", host="localhost", database="app", username="user", password="pass")
   # auto_ingest=true by default — models are generated automatically
2. datasource_summary()                            # see what was generated
3. inspect_model(model_name="orders")          # see schema + sample data
```

To explore first without auto-ingesting:

```
1. create_datasource(name="mydb", type="postgres", host="localhost", database="app", username="user", password="pass", auto_ingest=false)
2. describe_datasource(name="mydb")           # verify connection, see schemas
3. list_tables(datasource_name="mydb", schema_name="public")  # explore tables
4. ingest_datasource_models(datasource_name="mydb", schema_name="public")
5. datasource_summary()                            # see what was generated
```

### Query data

```
1. datasource_summary()                            # discover models
2. inspect_model(model_name="orders")          # see schema + sample data
3. query(model="orders", fields=[{"formula": "count"}], dimensions=["status"], limit=10)
```

### Customize a model

```
1. edit_model(
     model_name="orders",
     add_measures=[{"name": "avg_amount", "sql": "amount", "type": "avg"}],
     add_dimensions=[{"name": "priority", "sql": "priority", "type": "string"}],
     remove=["amount_sum"]
   )
```
