# MCP Server

SLayer runs as an [MCP](https://modelcontextprotocol.io/) server, allowing AI agents (Claude, Cursor, etc.) to discover and query data conversationally.

## Transports

SLayer supports two MCP transports. Both expose the exact same tools — the only difference is how the agent connects.

### Stdio (local)

The agent spawns SLayer as a subprocess and communicates via stdin/stdout. You do **not** run `slayer mcp` manually — the agent launches it. You only need to register the command with your agent.

**Claude Code setup:**

```bash
claude mcp add slayer -- slayer mcp --storage ./slayer_data
```

If `slayer` is installed in a virtualenv (e.g. via Poetry), use the full path to the executable so the agent can find it regardless of working directory:

```bash
# Find the virtualenv path
poetry env info -p
# e.g. /home/user/.venvs/slayer-abc123

# Register with the full path
claude mcp add slayer -- /home/user/.venvs/slayer-abc123/bin/slayer mcp --storage /path/to/slayer_data
```

### SSE (remote)

MCP over HTTP via Server-Sent Events. You run `slayer serve` yourself — it exposes both the REST API and the MCP SSE endpoint on the same port:

```bash
# 1. Start the server
slayer serve --storage ./slayer_data
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
| `describe_datasource` | Show details, test connection, list available schemas, and (by default) list tables in the given or default schema. Params: `name`, `list_tables` (default `true`), `schema_name` (empty = dialect default). |
| `edit_datasource` | Edit an existing datasource config. |
| `delete_datasource` | Remove a datasource config. |

### Model Management

| Tool | Description |
|------|-------------|
| `models_summary` | Brief summary of all non-hidden models in a datasource: each model's name, description, a `name`+`description` table of its **columns** and **measures** (named formulas), and the list of models it joins to. No types, values, or joined-model field expansion — call `inspect_model` for those. Params: `datasource_name`, `format` (default `"markdown"`; also `"json"`). |
| `inspect_model` | Complete view of a single model: metadata with row count, any model-level or column-level filters, **columns table** (with a `sampled` column — distinct values for string/boolean columns, `min .. max` for number/date/time columns), **measures table** of named formulas (with `formula`, `label`, `description`), custom aggregations, joins, all fields reachable via joins (default depth 5), and a sample-data table. Every Markdown table auto-prunes all-empty columns and collapses to a comma-separated backticked list when only one column remains. Params: `model_name`, `num_rows` (default 3), `show_sql` (default false — include SQL for the sample-data query, the custom-SQL block, model-level filters, the cached backing-query SQL, and aggregation formulas/param SQL), `format` (default `"markdown"`; also `"json"`), `sections` (subset of `["columns", "measures", "aggregations", "joins", "reachable_fields", "samples"]` — default `None`/`[]` renders all six; the first four collapse to a one-line backticked CSV of names when omitted, `reachable_fields`/`samples` are dropped entirely, unknown names emit a footer warning. A non-empty list of *only* unknown names resolves to no sections — "all six" is reserved for `None`/`[]` so a typo can't silently trigger the full payload), `descriptions_max_chars` (when set, truncate each description longer than this with the suffix `"... [truncated]"` (prefixed by a space); applies to model, columns, measures, and aggregations; must be `>= 0`), `reachable_fields_depth` (max BFS depth in path segments — default 5, allowed range `[0, 20]`; ignored when `reachable_fields` is not in `sections`). When any section is trimmed, a quoted-Markdown footer lists what was shown / names-only / omitted with a hint on how to re-call. JSON output mirrors this with `<section>_names` siblings and top-level `omitted_sections`, `names_only_sections`, `unknown_sections` arrays. |
| `create_model` | Create a model from a table/SQL definition or from a query. Pass `sql_table`/`sql` with `columns` (and optional named-formula `measures`) for table-based, or pass `query` (a SLayer query dict) to save it as a query-backed model whose `columns` + `backing_query_sql` are populated by a save-time dry-run. |
| `edit_model` | Edit an existing model in one call. Upserts `columns`, `measures` (named formulas), `aggregations`, `joins` (pass the new entries; existing names are updated, new ones are added). Also accepts `description`, `data_source`, `default_time_dimension`, `sql_table`/`sql`/`source_queries`, `query_variables`, `hidden`, `meta`, `add_filters`/`remove_filters`, and `remove: {"columns": [...], "measures": [...], "aggregations": [...], "joins": [...]}` for entity removal. |
| `delete_model` | Delete a model entirely. |

### Querying

| Tool | Description |
|------|-------------|
| `query` | Execute a semantic query. See [Queries](../concepts/queries.md) for format. |

**`query` parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `source_model` | string | Model name (required) |
| `measures` | list | Aggregated values: column-aggregations, arithmetic, transforms. E.g. `["*:count", {"formula": "revenue:sum / *:count", "name": "aov", "label": "Average Order Value"}, "cumsum(revenue:sum)"]`. Each entry has an optional `label` for human-readable display. Supports nesting: `"change(cumsum(revenue:sum))"`. Bare names resolve to saved `ModelMeasure` formulas on the model. |
| `dimensions` | list | Dimension names, e.g. `["status"]`. When using the engine directly, dimensions accept an optional `label` via `{"name": "status", "label": "Order Status"}`. |
| `filters` | list[str] | Filter formula strings, e.g. `["status = 'active'", "amount > 100"]`. Supports operators (`=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `IS NULL`, `IS NOT NULL`, `LIKE`, `NOT LIKE`), boolean logic (`AND`, `OR`, `NOT`), and inline transform expressions (`"change(revenue:sum) > 0"`). Filters on measures are automatically routed to HAVING. |
| `time_dimensions` | list[dict] | Time grouping. Each entry supports an optional `label` for display. |
| `order` | list[dict] | Sorting, e.g. `[{"column": "*:count", "direction": "desc"}]` |
| `limit` | int | Max rows |
| `offset` | int | Skip rows |
| `whole_periods_only` | bool | Snap date filters to time bucket boundaries, exclude the current incomplete time bucket |
| `show_sql` | bool | Include the generated SQL in the response for debugging |
| `dry_run` | bool | Generate and return the SQL without executing it |
| `explain` | bool | Run EXPLAIN ANALYZE and return the query plan |
| `format` | string | Output format: `"markdown"` (default, compact), `"json"` (structured), or `"csv"` (most compact). Case-insensitive |

### Ingestion

| Tool | Description |
|------|-------------|
| `ingest_datasource_models` | Auto-generate models from DB schema with rollup joins. Params: `datasource_name`, `include_tables`, `schema_name`. |

### Conceptual Help

| Tool | Description |
|------|-------------|
| `help` | Return SLayer concept explanations that complement the schema-focused tool docstrings. Call without arguments for the intro; pass `topic="..."` for a deep dive. The tool description lists every available topic — no exploratory call needed. |

Available topics and what they cover (content lives in `slayer/help/topics/*.md`, discovered dynamically):

| Topic | Covers |
|-------|--------|
| `queries` | Anatomy of a [query](../concepts/queries.md); evaluation order; dimensions vs [time dimensions](../concepts/queries.md#timedimension) on the same column; `main_time_dimension` disambiguation |
| `formulas` | The [formula mini-language](../concepts/formulas.md) shared by `measures` and `filters`; colon syntax; arithmetic; nesting |
| `aggregations` | Built-in and [custom aggregations](../examples/07_aggregations/aggregations.md); `first`/`last` time-column resolution; `allowed_aggregations` |
| `transforms` | `cumsum`, `time_shift`, `change`, `lag`, `rank`, `last()` — trade-offs and nesting ([time post](../examples/04_time/time.md)) |
| `time` | Granularities, `date_range`, `whole_periods_only`, the three meanings of "last" |
| `filters` | Operators; auto-routing to HAVING / post-filter; filtered measures; [model-level filters](../concepts/models.md#model-filters) |
| `joins` | Dot syntax and the `__` alias convention; cross-model measures and diamond joins ([joins post](../examples/05_joins/joins.md), [joined measures](../examples/05_joined_measures/joined_measures.md)) |
| `models` | Source modes (`sql_table`, `sql`, `source_queries`); query-backed models, `query_variables`, cached `backing_query_sql`; result column naming; `default_time_dimension`; hidden models ([models ref](../concepts/models.md)) |
| `extending` | `ModelExtension`, query lists, `create_model_from_query` (with `variables=`), run-by-name via `query` tool ([multistage post](../examples/06_multistage_queries/multistage_queries.md)) |
| `workflow` | Tool-chaining playbook, query-iteration tips, common-error decoder |

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
4. query(source_model="orders", measures=["*:count"], dimensions=["status"], limit=10)
```

### Customize a model

```
1. edit_model(
     model_name="orders",
     add_measures=[{"name": "amount", "sql": "amount"}],
     add_dimensions=[{"name": "priority", "sql": "priority", "type": "string"}],
     remove=["old_measure"]
   )
```