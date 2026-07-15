# CLI

SLayer provides a command-line interface for server management, querying, and model operations.

## Storage

All commands accept a `--storage` flag to specify where models and datasources are stored. When omitted, SLayer uses a platform-appropriate default (`~/.local/share/slayer` on Linux, `~/Library/Application Support/slayer` on macOS, `%LOCALAPPDATA%\slayer` on Windows). See [Storage](../configuration/storage.md) for full details on backends, resolution, and overrides.

The `SLAYER_INGEST_ON_STARTUP` environment variable mirrors the `--ingest-on-startup` flag on `slayer serve` / `slayer mcp` — truthy values (`1`, `true`, `yes`, case-insensitive) enable boot-time idempotent auto-ingestion across every configured datasource. See [Ingesting at Startup](../concepts/ingestion.md#ingesting-at-startup).

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
| `--storage` | [platform default](../configuration/storage.md) | Storage path (directory for YAML, `.db` file for SQLite) |
| `--demo` | off | Spin up the bundled Jaffle Shop DuckDB datasource and ingest its models on startup. Idempotent; `duckdb` and `jafgen` ship as core dependencies, so no extra install is needed. |
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
| `--storage` | [platform default](../configuration/storage.md) | Storage path (directory for YAML, `.db` file for SQLite) |
| `--demo` | off | Spin up the bundled Jaffle Shop DuckDB datasource and ingest its models on startup. Idempotent; `duckdb` and `jafgen` ship as core dependencies, so no extra install is needed. |
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
| `--storage` | [platform default](../configuration/storage.md) | Storage path (directory for YAML, `.db` file for SQLite) |
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

### `slayer import-osi`

Import OSI (Open Semantic Interchange) configs into SLayer. See [Importing OSI configs](../osi/osi_import.md).

```bash
slayer import-osi ./osi_configs --datasource my_postgres
slayer import-osi ./model.yaml --datasource my_postgres --dialect SNOWFLAKE
```

| Flag | Required | Description |
|------|----------|-------------|
| `osi_path` | Yes | Path to an OSI file or directory (`.yaml`/`.yml`/`.json`) |
| `--datasource` | Yes | SLayer datasource name (must be reachable — types come from live introspection) |
| `--dialect` | No | OSI expression dialect to read (default `ANSI_SQL`); falls back to another SQL dialect when absent |
| `--storage` | No | Storage path |

### `slayer models`

Manage models.

```bash
slayer models list
slayer models show orders
slayer models create model.yaml
slayer models delete orders
```

### `slayer inspect`

Point-lookup of an entity by reference + kind — no ranking, no bundled memories (use `slayer search` for an entity *in context*). Pass two or more references for a same-kind batch (DEV-1612).

```bash
slayer inspect jaffle_shop.orders --type model
slayer inspect jaffle_shop.orders.order_total --type column --no-compact
slayer inspect memory:42 --type memory --no-compact
slayer inspect jaffle_shop.orders --type model --format json
slayer inspect jaffle_shop.orders.order_total jaffle_shop.orders.order_id --type column --no-compact  # batch
```

`--type` is required (`datasource` / `model` / `column` / `measure` / `aggregation` / `memory`) and applies to every reference. The compact default is a schema skeleton for `--type model` (column / measure / aggregation names + joins, zero DB calls) and description-only for the other kinds; `--no-compact` returns the full render. Passing multiple references returns one `## <canonical>` block per reference, in input order (a JSON array under `--format json`), with per-reference error isolation. See the [CLI reference](../reference/cli.md#slayer-inspect) for all flags.

### `slayer datasources`

Manage datasources.

```bash
slayer datasources list
slayer datasources show my_postgres   # credentials masked
```

### `slayer search`

Run semantic search over memories and canonical entities. Three retrieval channels run in parallel — BM25 over memory entity tags, Tantivy full-text over memories ∪ entities, and (with `motley-slayer[advanced_search]` plus a provider API key) dense embeddings — and are RRF-fused into a single ranked list. See [Search](../concepts/search.md).

```bash
# Entity-driven
slayer search --entity jaffle_shop.orders.order_total

# Question-driven
slayer search --question "What stores are in jaffle_shop?"

# Inline query / from a file (auto-extracts canonical entities)
slayer search --query '{"source_model": "orders", "measures": ["order_total:sum"]}'
slayer search --query @draft_query.json

# Narrow to one datasource
slayer search --question "lifetime spend" --datasource jaffle_shop

# Graph-narrow with cypher_filter (naive form, always available)
slayer search --question "Brooklyn POS" --cypher-filter 'MATCH (n:Memory) RETURN n.id AS id'

# JSON output for piping
slayer search --question "lifetime spend" --format json
```

| Flag | Default | Description |
|------|---------|-------------|
| `--entity ENT` (repeatable) | | Canonical entity string (`<ds>`, `<ds>.<model>`, `<ds>.<model>.<leaf>`, `memory:<id>`). |
| `--query JSON_OR_@FILE` | | Inline SLayer query (or `@path.json`); entities auto-extracted. |
| `--question TEXT` | | Free-text question. Drives Tantivy + embeddings. |
| `--datasource DS` | | Pre-narrow every channel to ids rooted at the named datasource. |
| `--cypher-filter CYPHER` | | Graph pre-filter. Full openCypher with the `advanced_search` extra (LadybugDB property graph: `Memory` / `Datasource` / `Model` / `ModelColumn` / `Measure` / `Aggregation` nodes; `MENTIONS` / `CONTAINS` / `JOINS` edges). Without the extra, only the naive `MATCH (n:Label1:Label2…) RETURN n.id AS id` form is accepted; richer Cypher raises with an install hint. |
| `--max-results N` | `10` | Cap applied after RRF fusion and the `cypher_filter` allowlist. |
| `--format` | `text` | `text` (newline-grouped human output) or `json` (full `SearchResponse`). |

#### `slayer search refresh-samples`

Re-profile and persist `Column.sampled` / `sampled_values` / `distinct_count` for table-backed models. Per-column failures are reported but do not abort.

```bash
slayer search refresh-samples
slayer search refresh-samples --data-source jaffle_shop
slayer search refresh-samples --data-source jaffle_shop --model orders --model customers
```

### `slayer memory`

Manage the agent-memory layer. See [Memories](../concepts/memories.md).

```bash
# Save a learning (--entities is a single comma-separated string; --query is mutually exclusive)
slayer memory save \
  --learning "orders.is_returned in {0,1,NULL}; treat NULL as not returned" \
  --entities mydb.orders.is_returned \
  --id kb.returns.null-handling

slayer memory save \
  --learning "Top customers by lifetime spend" \
  --query @top_customers.json \
  --id kb.top-customers

slayer memory forget kb.returns.null-handling
```

| Subcommand | Flag | Description |
|------------|------|-------------|
| `save` | `--learning TEXT` (required) | The free-form note. |
| `save` | `--entities ENT,ENT,…` | Comma-separated canonical entity strings. Mutually exclusive with `--query`; one of the two is required. |
| `save` | `--query JSON_OR_@FILE` | Inline SLayer query (or `@path.json`). Entities auto-extracted and the query is persisted on the memory. |
| `save` | `--id ID` | User-pinned canonical memory id. Forbidden charset: `:`, `/`, `?`, `#`, whitespace, ASCII control. Omit to auto-allocate (`max(int-shaped id) + 1`). Duplicate id → unconditional upsert; `created_at` preserved. |
| `forget` | `<id>` (positional) | Memory id. Cascade-strips every `memory:<id>` reference to it from every other memory's `entities` list. |

### Conceptual help

SLayer's conceptual help ships as a predefined set of **help memories**
(`memory:help.intro` … `memory:help.workflow`) — read them with `inspect`, or
find the relevant one with `search`:

```bash
slayer inspect memory:help.intro --type memory        # overview + the query shape
slayer inspect memory:help.transforms --type memory   # cumsum, time_shift, lag/lead trade-offs
slayer search --question "how do transforms work"     # surface the relevant topic
```

`memory:help.intro` lists the deep-dive topics (`memory:help.queries`,
`memory:help.formulas`, `memory:help.aggregations`, `memory:help.transforms`,
`memory:help.time`, `memory:help.filters`, `memory:help.joins`,
`memory:help.models`, `memory:help.extending`, `memory:help.workflow`). The
topics complement the schema/reference pages: they cover how concepts compose
(query evaluation order, transform trade-offs, cross-model measures, the three
meanings of "last") rather than restating field-by-field schemas. See the
corresponding concept docs for full treatments:
[queries](../concepts/queries.md), [formulas](../concepts/formulas.md),
[models](../concepts/models.md), [ingestion](../concepts/ingestion.md).
