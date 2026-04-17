# MCP Server

SLayer runs as an [MCP](https://modelcontextprotocol.io/) server, allowing AI agents (Claude, Cursor, etc.) to discover and query data conversationally.

## Quick Start

The fastest way to run SLayer is via `uvx` — no install needed. You only need [uv](https://docs.astral.sh/uv/getting-started/installation/).

**Claude Code:**

```bash
claude mcp add slayer -- uvx --from 'motley-slayer[postgres]' slayer mcp --storage ./slayer_data
```

**JSON config** (Claude Desktop, Cursor, and other MCP-compatible agents):

```json
{
  "mcpServers": {
    "slayer": {
      "command": "uvx",
      "args": ["--from", "motley-slayer[postgres]", "slayer", "mcp", "--storage", "./slayer_data"]
    }
  }
}
```

Replace `postgres` with your database driver (see [full list](../configuration/datasources.md#database-drivers)), or use `motley-slayer[all]` for all supported databases. SQLite and MCP work out of the box with the base install.

See the [Getting Started guide](../getting-started/mcp.md) for full setup instructions including SSE/remote and permanent install options.

## Transports

SLayer supports two MCP transports. Both expose the exact same tools.

### Stdio (local — recommended)

The agent spawns SLayer as a subprocess and communicates via stdin/stdout. You do **not** run `slayer mcp` manually — the agent launches it. The `claude mcp add` and JSON config examples above both use this transport.

### SSE (remote)

MCP over HTTP via Server-Sent Events. You run `slayer serve` yourself — it exposes both the REST API and the MCP SSE endpoint on the same port:

```bash
uvx --from 'motley-slayer[postgres]' slayer serve --storage ./slayer_data
# REST API at http://localhost:5143/
# MCP SSE at http://localhost:5143/mcp/sse
```

Then register the remote endpoint with your agent:

```bash
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
| `describe_datasource` | Show details, test connection, list available schemas, and (by default) list tables in the given or default schema. Params: `name`, `list_tables` (default `true`), `schema_name` (empty = dialect default). |
| `edit_datasource` | Edit an existing datasource config. |
| `delete_datasource` | Remove a datasource config. |
| `ingest_datasource_models` | Auto-generate models from DB schema with rollup joins. Params: `datasource_name`, `include_tables`, `schema_name`. |

### Model Management

| Tool | Description |
|------|-------------|
| `models_summary` | Brief summary of all non-hidden models in a datasource: each model's name, description, a `name`+`description` table of its dimensions and measures, and the list of models it joins to. No types, values, or joined-model field expansion — call `inspect_model` for those. Params: `datasource_name`, `format` (default `"markdown"`; also `"json"`). |
| `inspect_model` | Complete view of a single model: metadata with row count, any model-level or measure-level filters, dimensions table (with a `sampled` column — distinct values for string/boolean dims, `min .. max` for number/date/time dims), measures table (with `allowed_aggregations`, `filter`, `label`, `description`, `sql`), custom aggregations, joins, all fields reachable via joins up to depth 5, and a sample-data table. Every markdown table auto-prunes all-empty columns and collapses to a comma-separated backticked list when only one column remains. Params: `model_name`, `num_rows` (default 3), `show_sql` (default false — include SQL for the sample-data query), `format` (default `"markdown"`; also `"json"`). |
| `create_model` | Create a model from a table/SQL definition or from a query. Pass `sql_table`/`sql` with `dimensions`/`measures` for table-based, or pass `query` (a SLayer query dict) to auto-introspect dimensions and measures from the query result. |
| `edit_model` | Edit an existing model in one call. Supports upsert for dimensions, measures, aggregations, and joins (create if new, update if existing). Also manages scalar metadata and filters. See params below. |
| `delete_model` | Delete a model entirely. |

### Querying

| Tool | Description |
|------|-------------|
| `query` | Execute a semantic query. See [Queries](../concepts/queries.md) for format. |

**`query` parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `source_model` | string | Model name (required) |
| `fields` | list | Data columns: measures, arithmetic, transforms. E.g. `["count", {"formula": "revenue / count", "name": "aov", "label": "Average Order Value"}, "cumsum(revenue)"]`. Each field has an optional `label` for human-readable display. Supports nesting: `"change(cumsum(revenue))"` |
| `dimensions` | list | Dimension names, e.g. `["status"]`. When using the engine directly, dimensions accept an optional `label` via `{"name": "status", "label": "Order Status"}`. |
| `filters` | list[str] | Filter formula strings, e.g. `["status = 'active'", "amount > 100"]`. Supports operators (`=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `IS NULL`, `IS NOT NULL`, `LIKE`, `NOT LIKE`), boolean logic (`AND`, `OR`, `NOT`), and inline transform expressions (`"change(revenue) > 0"`). Filters on measures are automatically routed to HAVING. |
| `time_dimensions` | list[dict] | Time grouping. Each entry supports an optional `label` for display. |
| `order` | list[dict] | Sorting, e.g. `[{"column": "count", "direction": "desc"}]` |
| `limit` | int | Max rows |
| `offset` | int | Skip rows |
| `whole_periods_only` | bool | Snap date filters to time bucket boundaries, exclude the current incomplete time bucket |
| `show_sql` | bool | Include the generated SQL in the response for debugging |
| `dry_run` | bool | Generate and return the SQL without executing it |
| `explain` | bool | Run EXPLAIN ANALYZE and return the query plan |
| `format` | string | Output format: `"markdown"` (default, compact), `"json"` (structured), or `"csv"` (most compact). Case-insensitive |

## Typical Agent Workflows

### Connect and explore a new database

```
1. create_datasource(name="mydb", type="postgres", host="localhost", database="app", username="user", password="pass")
   # auto_ingest=true by default — models are generated automatically
2. models_summary(datasource_name="mydb")      # see what was generated
3. inspect_model(model_name="orders")          # see schema + sample data
```

To explore first without auto-ingesting:

```
1. create_datasource(name="mydb", type="postgres", host="localhost", database="app", username="user", password="pass", auto_ingest=false)
2. describe_datasource(name="mydb", schema_name="public")  # verify connection + list tables
3. ingest_datasource_models(datasource_name="mydb", schema_name="public")
4. models_summary(datasource_name="mydb")      # see what was generated
```

### Query data

```
1. list_datasources()                              # pick a datasource
2. models_summary(datasource_name="mydb")      # discover its models
3. inspect_model(model_name="orders")          # see schema + sample data
4. query(source_model="orders", fields=["count"], dimensions=["status"], limit=10)
```

### Customize a model

```
1. edit_model(
     model_name="orders",
     measures=[{"name": "avg_amount", "sql": "amount"}],
     dimensions=[{"name": "priority", "sql": "priority", "type": "string"}],
     remove={"measures": ["amount_sum"]}
   )
```

Upsert semantics: if a measure/dimension/aggregation/join with that name already exists, only the provided fields are updated. To remove entities, use the `remove` dict keyed by type (`"dimensions"`, `"measures"`, `"aggregations"`, `"joins"`).