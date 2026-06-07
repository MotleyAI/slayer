# MCP Setup — AI Agents

Connect your AI agent (Claude Code, Cursor, etc.) to your database through SLayer's MCP server. No Python knowledge required.

## Prerequisites

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) — the fast Python package manager. SLayer runs via `uvx` with no separate install step.

## Connect to your agent

### Claude Code

Register SLayer as an MCP server — Claude Code will spawn it automatically when needed:

```bash
claude mcp add slayer -- uvx --from 'motley-slayer[advanced_search]' slayer mcp --ingest-on-startup
```

`--ingest-on-startup` walks every configured datasource on boot and runs idempotent auto-ingestion before the MCP channel opens, so models are available on the agent's first tool call. Drop it to defer ingestion to a manual `ingest_datasource_models` call.

The `advanced_search` extra enables semantic search over models and memories. When it's installed **and** a provider API key is in the environment (`OPENAI_API_KEY` by default; override the embedding model with `SLAYER_EMBEDDING_MODEL=voyage/voyage-3` + `VOYAGE_API_KEY`, etc.), the boot-time ingest pass also refreshes per-entity embeddings — hash-skipped, so steady-state boots make zero embedding API calls. Without the extra (or without a provider key), search and ingest still work; the embedding channel is silently disabled.

For databases other than SQLite, add the driver extra alongside (see [full list](../configuration/datasources.md#database-drivers)):

```bash
claude mcp add slayer -- uvx --from 'motley-slayer[postgres,advanced_search]' slayer mcp --ingest-on-startup
```

### Other agents (JSON config)

Most MCP-compatible agents accept a JSON server configuration. Add this to your agent's MCP config file:

```json
{
  "mcpServers": {
    "slayer": {
      "command": "uvx",
      "args": ["--from", "motley-slayer[postgres,advanced_search]", "slayer", "mcp", "--ingest-on-startup"],
      "env": {
        "OPENAI_API_KEY": "sk-..."
      }
    }
  }
}
```

Replace `postgres` with your database driver, or use `motley-slayer[all]` for all supported databases (every driver plus `advanced_search`).

### Remote / shared server

SLayer also supports HTTP/SSE transport for running on a different machine, in Docker, or sharing between multiple agents. See the [MCP Reference](../reference/mcp.md#sse-remote) for details.

### Verify

```bash
claude mcp list
```

## Connect a database

The recommended approach is to drop a datasource YAML file into your storage folder. This keeps credentials out of the agent conversation and lets you use environment variable references.

Create a file in the `datasources/` subdirectory of your [storage path](../configuration/storage.md) (e.g. `~/.local/share/slayer/datasources/mydb.yaml`):

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

Once the datasource file is in place, the recommended setup runs MCP with `--ingest-on-startup` (above), which walks every configured datasource on every boot and runs idempotent auto-ingestion before the MCP channel opens. Models are then available on the agent's first tool call — no `ingest_datasource_models` call required.

If you didn't start with `--ingest-on-startup`, fall back to asking your agent:

> "Ingest models from the mydb datasource and show me what's available"

The agent will call `ingest_datasource_models` to generate models from the database schema, then `models_summary(datasource_name="mydb")` to list them.

You can also create datasources conversationally via the `create_datasource` MCP tool — see the [MCP Reference](../reference/mcp.md#datasource-management) for details.

## Verify it works

Ask your agent:

> "List the available SLayer models"

The agent should call `list_datasources` and then `models_summary(datasource_name="mydb")` and return a list of your tables/models. If it says "no models found", check that:

1. The `--storage` path matches where your datasource YAML files are
2. Models have been ingested (via `slayer mcp --ingest-on-startup`, `ingest_datasource_models`, or `create_datasource` with auto-ingest)
3. Environment variables referenced in the datasource config are set

## Search & memories

Once a datasource is ingested, the agent has access to two extra tools beyond `query`:

- `search` — three-channel retrieval (BM25 over memory tags + Tantivy full-text + optional dense embeddings) fused into a single ranked list of memory hits and canonical entity discovery hits.
- `save_memory` / `forget_memory` — persist or remove free-form notes tagged with canonical entities (e.g. `mydb.orders.amount`) so the next session inherits the context.

Try it conversationally:

> "Save a memory: 'orders.is_returned is in {0,1,NULL}; treat NULL as not returned' linked to mydb.orders.is_returned"
>
> "Search for what we know about orders.is_returned"

The agent calls `save_memory` then `search` and gets back a `SearchResponse` with `results: [...]` — each hit carries a `kind` discriminator (`"memory"` for prior notes, `"column"` / `"model"` / etc. for entity discovery hits) and a `score`.

The same `search` call also accepts `cypher_filter` for graph-shaped narrowing (full openCypher with `advanced_search` installed, naive `MATCH (n:Label) RETURN n.id AS id` kind-filter without). The embedding channel needs `motley-slayer[advanced_search]` plus a provider API key — without those, search degrades to BM25 + Tantivy with a single warning in `SearchResponse.warnings`.

See [Search](../concepts/search.md), [Memories](../concepts/memories.md), and the [MCP Reference](../reference/mcp.md#memories-semantic-search) for full signatures.

## Alternative: permanent install

If you prefer a traditional install instead of `uvx`:

```bash
uv tool install 'motley-slayer[postgres,advanced_search]'
claude mcp add slayer -- slayer mcp --ingest-on-startup
```
