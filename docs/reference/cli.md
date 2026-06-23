# CLI

SLayer provides a command-line interface for server management, querying, and model operations.

## Storage

All commands accept a `--storage` flag to specify where models and datasources are stored. When omitted, SLayer uses a platform-appropriate default (`~/.local/share/slayer` on Linux, `~/Library/Application Support/slayer` on macOS, `%LOCALAPPDATA%\slayer` on Windows). See [Storage](../configuration/storage.md) for full details on backends, resolution, and overrides.

## Commands

### `slayer serve`

Start the HTTP server (REST API + MCP SSE endpoint at `/mcp/sse`).

```bash
slayer serve
slayer serve --host 0.0.0.0 --port 8080
slayer serve --storage slayer.db
slayer serve --demo                  # auto-ingest the bundled Jaffle Shop demo first
slayer serve --ingest-on-startup     # run idempotent ingest over every configured datasource first
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `5143` | Port number |
| `--storage` | [platform default](../configuration/storage.md) | Storage path (directory for YAML, `.db` file for SQLite) |
| `--demo` | off | Generate and ingest the bundled Jaffle Shop demo before starting (idempotent). |
| `--ingest-on-startup` | off | Walk every configured datasource and run idempotent auto-ingestion before the port opens. Per-datasource errors are logged to stderr and never abort startup. Also enabled by `SLAYER_INGEST_ON_STARTUP=1`. |

### `slayer mcp`

Run SLayer as an MCP server using stdio transport. This command is **not meant to be run manually** — it is spawned by an AI agent (Claude Code, Cursor, etc.) as a subprocess. To set it up, register the command with your agent:

```bash
# Register with Claude Code (the agent will spawn the process)
claude mcp add slayer -- slayer mcp --ingest-on-startup --storage ./slayer_data

# If slayer is in a virtualenv, use the full executable path:
#   claude mcp add slayer -- $(poetry env info -p)/bin/slayer mcp --ingest-on-startup --storage /abs/path/to/slayer_data
```

For MCP over HTTP (SSE), use `slayer serve` instead — it exposes MCP at `/mcp/sse` alongside the REST API.

| Flag | Default | Description |
|------|---------|-------------|
| `--storage` | [platform default](../configuration/storage.md) | Storage path (directory for YAML, `.db` file for SQLite) |
| `--demo` | off | Generate and ingest the bundled Jaffle Shop demo before starting (idempotent). |
| `--ingest-on-startup` | off | Walk every configured datasource and run idempotent auto-ingestion before stdio JSON-RPC starts. Per-datasource errors are logged to stderr and never abort startup. Also enabled by `SLAYER_INGEST_ON_STARTUP=1`. |

### `slayer query`

Execute a query from the terminal.

```bash
# Inline JSON
slayer query '{"source_model": "orders", "measures": ["*:count"], "dimensions": ["status"]}'

# From a file
slayer query @query.json

# JSON output
slayer query '{"source_model": "orders", "measures": ["*:count"]}' --format json

# Preview SQL without executing
slayer query '{"source_model": "orders", "measures": ["*:count"]}' --dry-run

# Show execution plan
slayer query @query.json --explain
```

| Flag | Default | Description |
|------|---------|-------------|
| `--storage` | [platform default](../configuration/storage.md) | Storage path (directory for YAML, `.db` file for SQLite) |
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
| `--include-hidden-models` | No | Also import regular dbt models (those not wrapped by a `semantic_model`) as hidden SLayer models via SQL introspection. Requires the `dbt` extra (`pip install 'motley-slayer[dbt]'`). See [dbt Import](../dbt/dbt_import.md#regular-dbt-models-hidden-import). |
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
slayer datasources test my_postgres
slayer datasources delete my_postgres
```

#### `slayer datasources create`

Create a datasource from a connection URL. The name is derived from the database portion of the URL (or the filename stem for SQLite/DuckDB) unless `--name` is passed. Pass `--ingest` to create and ingest in a single step.

```bash
slayer datasources create postgresql://user:${DB_PW}@localhost/analytics
slayer datasources create postgresql://localhost/analytics --ingest
slayer datasources create sqlite:///path/to/app.db --name analytics --ingest
slayer datasources create demo --ingest        # bundled Jaffle Shop demo
```

| Flag | Required | Description |
|------|----------|-------------|
| `connection_string` | Yes | Database URL (e.g. `postgresql://…`, `mysql+pymysql://…`, `sqlite:///path/to/file.db`, `duckdb:///…`, `clickhouse+http://…`). `${ENV_VAR}` references are resolved at use time. Pass the literal `demo` to spin up the bundled Jaffle Shop demo DuckDB. |
| `--name` | No | Override the auto-derived name (default for the demo: `jaffle_shop`) |
| `--description` | No | Human-readable description |
| `--ingest` | No | Run auto-ingestion immediately after creating the datasource |
| `--schema` | No | (with `--ingest`) Schema to ingest from |
| `--include` | No | (with `--ingest`) Comma-separated tables to include |
| `--exclude` | No | (with `--ingest`) Comma-separated tables to exclude |
| `--years` | No | (demo only) Years of synthetic data to generate (default: 2) |
| `-y`, `--yes` | No | Overwrite existing datasource / colliding models without prompting |
| `--storage` | No | Storage path |

The demo path generates a DuckDB at `<storage>/demo/jaffle_shop.duckdb` and is idempotent — re-running reuses the existing file. `duckdb` and `jafgen` are core dependencies of `motley-slayer`, so the demo works after a single `pip install motley-slayer` with no extras needed.

If a datasource with the same name already exists, or (with `--ingest`) any generated model name collides with a stored model, SLayer prompts for confirmation. Use `--yes` for non-interactive use.

### `slayer inspect`

Point-lookup of a single entity by reference and kind. Returns the rendered
detail for **exactly one** entity — no ranking, no bundled memories (use
`slayer search` for an entity *in context*).

```bash
slayer inspect jaffle_shop.orders --type model
slayer inspect jaffle_shop.orders.order_total --type column --no-compact
slayer inspect jaffle_shop.orders.customers.region --type column --no-compact  # join path → owning model
slayer inspect memory:42 --type memory --no-compact
slayer inspect jaffle_shop.orders --type model --format json
```

| Flag | Required | Description |
|------|----------|-------------|
| `reference` | Yes | Entity reference: canonical id, bare name, join path (resolved to the owning model), or `memory:<id>`. |
| `--type` | Yes | Entity kind: `datasource`, `model`, `column`, `measure`, `aggregation`, or `memory`. Disambiguates same-named entities and asserts the kind. |
| `--no-compact` | No | Return the full render instead of the description-only default. |
| `--format` | No | `markdown` (default) or `json`. |
| `--num-rows` | No | (model only) Sample-data rows. Ignored with a warning for other kinds. |
| `--show-sql` | No | (model only) Include generated SQL. No-op for column/measure/aggregation; warned for datasource/memory. |
| `--section` | No | (model only, repeatable) Restrict to a section subset. Ignored with a warning for other kinds. |
| `--descriptions-max-chars` | No | Truncate description fields. Applies to every kind. |
| `--storage` | No | Storage path. |

### `slayer search`

Run semantic search over memories and canonical entities (datasources, models, columns, named measures, custom aggregations). Three retrieval channels run in parallel — BM25 over memory entity tags, Tantivy full-text over memories ∪ entities, and (with the `advanced_search` extra plus a provider API key) dense embeddings — and are RRF-fused into a single ranked list. See [Search](../concepts/search.md).

```bash
# Entity-driven
slayer search --entity jaffle_shop.orders.order_total

# Question-driven
slayer search --question "What stores are in jaffle_shop?"

# Query-driven (auto-extracts the entities the query references)
slayer search --query @draft_query.json

# Inline query JSON
slayer search --query '{"source_model": "orders", "measures": ["order_total:sum"]}'

# Narrow to one datasource
slayer search --question "lifetime spend" --datasource jaffle_shop

# Graph-narrow with cypher_filter (naive form, always available)
slayer search --question "Brooklyn POS" --cypher-filter 'MATCH (n:Memory) RETURN n.id AS id'

# Graph-narrow with cypher_filter (full openCypher; requires the advanced_search extra)
slayer search --question "store rev" --cypher-filter \
  "MATCH (d:Datasource {name: 'jaffle_shop'})-[:CONTAINS]->(m:Model)-[:CONTAINS]->(c:ModelColumn) RETURN c.id AS id"

# JSON output for piping
slayer search --question "lifetime spend" --format json
```

| Flag | Default | Description |
|------|---------|-------------|
| `--entity ENT` (repeatable) | | Canonical entity string (`<ds>`, `<ds>.<model>`, `<ds>.<model>.<leaf>`, `memory:<id>`). Pass multiple times to combine. Drives the BM25 channel. |
| `--query JSON_OR_@FILE` | | Inline SLayer query (or `@path.json`). Entities are auto-extracted from `source_model`, dimensions, measures, time dims, and filters. |
| `--question TEXT` | | Free-text question. Drives Tantivy + embeddings. |
| `--datasource DS` | | Pre-narrow every channel to ids rooted at the named datasource. Unknown name raises. |
| `--cypher-filter CYPHER` | | Pre-narrow all three channels via a graph query. Full openCypher with `advanced_search` (LadybugDB property graph with `Memory` / `Datasource` / `Model` / `ModelColumn` / `Measure` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `JOINS` edges). Without the extra, only the naive `MATCH (n:Label1:Label2…) RETURN n.id AS id` form is accepted; anything richer raises with an install hint. |
| `--max-results N` | `10` | Cap applied after RRF fusion and the `cypher_filter` allowlist. |
| `--format` | `text` | `text` (newline-grouped human output) or `json` (full `SearchResponse`). |

Each result row prints `kind`, `id`, `score`, and a one-line preview of `text`. Memory hits with `query is not None` are saved example queries; column hits include the structured `sampled_values` snapshot (top 50 by frequency) and a `Distinct count: N` line when cardinality overflows. Unresolved input entities surface as warnings rather than errors.

### `slayer search refresh-samples`

Re-profile and persist `Column.sampled` / `sampled_values` / `distinct_count` for table-backed models. Per-column failures are reported but do not abort.

```bash
slayer search refresh-samples
slayer search refresh-samples --data-source jaffle_shop
slayer search refresh-samples --data-source jaffle_shop --model orders --model customers
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-source X` | all | Limit the refresh to one datasource. |
| `--model M` | all | Repeatable; limit to specific models. |

### `slayer memory`

Manage the agent-memory layer. See [Memories](../concepts/memories.md).

```bash
# Save a learning (--entities is a single comma-separated string)
slayer memory save \
  --learning "orders.is_returned in {0,1,NULL}; treat NULL as not returned" \
  --entities jaffle_shop.orders.is_returned

# Save with a pinned id and multiple entities
slayer memory save \
  --learning "Brooklyn POS changed late 2024" \
  --entities jaffle_shop.orders.order_total,jaffle_shop.stores.name \
  --id kb.brooklyn-pos

# Save with an inline query (mutually exclusive with --entities)
slayer memory save \
  --learning "Top customers by lifetime spend" \
  --query @top_customers.json \
  --id kb.top-customers

# Forget by id
slayer memory forget kb.brooklyn-pos
```

| Subcommand | Flag | Description |
|------------|------|-------------|
| `save` | `--learning TEXT` (required) | The free-form note. |
| `save` | `--entities ENT,ENT,…` | Comma-separated canonical entity strings. `memory:<id>` is valid for cross-memory refs. Mutually exclusive with `--query`; one of the two is required. |
| `save` | `--query JSON_OR_@FILE` | Inline SLayer query (or `@path.json`). Entities are auto-extracted and the query is persisted on the memory. Mutually exclusive with `--entities`. |
| `save` | `--id ID` | User-pinned canonical memory id. Forbidden charset: `:`, `/`, `?`, `#`, whitespace, ASCII control. Omit to auto-allocate (`max(int-shaped id) + 1`). Duplicate id → unconditional upsert; `created_at` preserved. |
| `forget` | `<id>` (positional) | Memory id. Cascade-strips every `memory:<id>` reference to it from every other memory's `entities` list. |
