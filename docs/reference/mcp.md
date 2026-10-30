# MCP Server

SLayer runs as an [MCP](https://modelcontextprotocol.io/) server, allowing AI agents (Claude, Cursor, etc.) to discover and query data conversationally.

## Quick Start

The fastest way to run SLayer is via `uvx` — no install needed. You only need [uv](https://docs.astral.sh/uv/getting-started/installation/).

**Claude Code:**

```bash
claude mcp add slayer -- uvx --from 'motley-slayer[postgres]' slayer mcp --ingest-on-startup
```

**JSON config** (Claude Desktop, Cursor, and other MCP-compatible agents):

```json
{
  "mcpServers": {
    "slayer": {
      "command": "uvx",
      "args": ["--from", "motley-slayer[postgres]", "slayer", "mcp", "--ingest-on-startup"]
    }
  }
}
```

`--ingest-on-startup` runs idempotent auto-ingestion across every configured datasource before the stdio channel opens, so models are available on the agent's first tool call. Drop it (or set `SLAYER_INGEST_ON_STARTUP=0`) to defer ingestion to a manual `ingest_datasource_models` call.

`--always-load-query` (or `SLAYER_MCP_ALWAYS_LOAD_QUERY=1`) marks the `query` tool with Claude Code's `anthropic/alwaysLoad` flag, so it is never deferred behind tool search.

Set `SLAYER_NOW` (e.g. `2025-07-15T12:00:00`) in the server's environment to pin the "now" that relative time filters like `'last 3 months'` resolve against, for reproducible answers over a fixed dataset.

Replace `postgres` with your database driver (see [full list](../configuration/datasources.md#database-drivers)), or use `motley-slayer[all]` for all supported databases. SQLite and MCP work out of the box with the base install.

See the [Getting Started guide](../getting-started/mcp.md) for full setup instructions including SSE/remote and permanent install options.

## Transports

SLayer supports two MCP transports. Both expose the exact same tools.

### Stdio (local — recommended)

The agent spawns SLayer as a subprocess and communicates via stdin/stdout. You do **not** run `slayer mcp` manually — the agent launches it. The `claude mcp add` and JSON config examples above both use this transport.

### SSE (remote)

MCP over HTTP via Server-Sent Events. You run `slayer serve` yourself — it exposes both the REST API and the MCP SSE endpoint on the same port:

```bash
uvx --from 'motley-slayer[postgres]' slayer serve --ingest-on-startup
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
| `set_datasource_priority` | Set the datasource priority list (most preferred first; `[]` clears it): a bare model name that exists in several datasources resolves to the first listed datasource that has it, else errors as ambiguous. Params: `priority`. |
| `get_datasource_priority` | Return the datasource priority list, or `[]` when none is set. |
| `ingest_datasource_models` | Auto-generate models from DB schema with rollup joins. Params: `datasource_name`, `include_tables`, `schema_name`. Reports `Skipped` objects and `Hidden` [recognised internals](cli.md#recognised-internals) alongside the models it created — including on a no-op re-ingest. Use `edit_model("<model>", data_source="<datasource>", hidden=False)` to surface a hidden one — internals collide across datasources, so the bare form can un-hide the wrong model. |

### Model Management

| Tool | Description |
|------|-------------|
| `models_summary` | Brief summary of all non-hidden models in a datasource. `compact=True` (default) gives each model's name, description, column count, measure names and join targets; `compact=False` adds the full tables of its **columns** (`name`, `type`, `description`) and **measures** (named formulas). Neither form includes distinct values or sample data — call `inspect_model` for those. For multi-hop discovery (fields reachable via joins from a given model), use the `search` tool with `cypher_filter` for graph queries. Params: `datasource_name`, `format` (default `"markdown"`; also `"json"`), `compact` (default true). |
| `inspect` | Single-entity point lookup: returns the rendered detail for **exactly one** entity by `reference` + required `entity_type` (`datasource`/`model`/`column`/`measure`/`aggregation`/`memory`). No fusion/ranking/cypher and no bundled memories — use `search` instead when you want an entity surfaced *in context*. `reference` accepts canonical ids, bare names, join paths (`orders.customers.region` → resolved to the owning model), and `memory:<id>`; it is normalised via the shared resolver and the normalised id is echoed in the JSON shape. `entity_type` disambiguates the 3-part canonical collision (a name shared by, e.g., a column and an aggregation) and asserts the resolved kind (mismatch → detailed error). Renders hidden entities. Params: `reference`, `entity_type`, `compact` (default true), `format` (`"markdown"` default, `"json"`), and the model-only `num_rows`/`show_sql`/`sections`/`descriptions_max_chars` (applied for `entity_type="model"`; `descriptions_max_chars` applies to every kind; the others are ignored with a warning for non-model kinds, `show_sql` a silent no-op for column/measure/aggregation). **`compact=True`** renders description-only for leaf/datasource/memory kinds (`compact` never shortens a `help.*` memory; `descriptions_max_chars` still does), and a cheap **schema skeleton** (column/measure/aggregation **names** + join targets, **zero DB calls**) for the `model` kind — markdown always emits the four `Columns:`/`Measures:`/`Aggregations:`/`Joins to:` lines (`_(none)_` when empty); JSON always carries `column_names`/`measure_names`/`aggregation_names`/`joins_to`. **`compact=False`** reuses the full `inspect_model` rendering (sample rows, SQL, sections) for the `model` kind, and renders a per-model skeleton for each visible model (sorted by name; `models: [...]` in JSON) for the `datasource` kind. JSON `text` is present **iff non-empty** (omitted under `compact=True` for every kind except a `help.*` memory). Three-tier escalation: `models_summary(compact)` (column count) < `inspect(model, compact=True)` (column names) < `inspect(model, compact=False)` (full). **Batch:** `reference` also accepts a **list** of ids — a homogeneous-kind batch (one `entity_type` for all). A single `str` keeps single-id output byte-for-byte; a list returns one block per id in input order, each echoing its resolved canonical id (a `## <canonical>` header per block in markdown, a JSON array under `format="json"`). A one-element list is still batch-framed. Per-id resolution errors are isolated (in JSON a failed id is a `{"reference", "error"}` object). **Collection:** omit `reference` (or pass `null`/`[]`) to list a whole kind. `entity_type="model"` lists all models grouped by datasource — `compact=True` (default) is a terse one-liner per model (`- \`name\` (N cols; joins: ...)` under a `# Datasource: \`<ds>\` — <N> model(s)` header); `compact=False` is the full `models_summary` block per datasource. `entity_type="datasource"` lists all datasources (`compact=True` == `list_datasources`; `compact=False` adds descriptions + per-model skeletons). Only `model`/`datasource` support the collection view; other kinds raise. This subsumes `models_summary` / `list_datasources` (kept as thin aliases). JSON collection form is `{"entity_type", "collection": true, "datasources": [...], "warnings": []}`. |
| `inspect_model` | **DEPRECATED — use `inspect`.** Complete view of a single model: metadata with row count (and a `**meta:**` bullet when the model has `meta` set), any model-level or column-level filters, **columns table** (with a `sampled` column — distinct values for string/boolean columns, `min .. max` for number/date/time columns — and a `meta` cell when set), **measures table** of named formulas (with `formula`, `label`, `description`, `meta`), custom aggregations (with `meta`), direct joins, and a sample-data table. Every Markdown table auto-prunes all-empty columns (so the `meta` column is hidden when no entity has meta) and collapses to a comma-separated backticked list when only one column remains. Params: `model_name`, `num_rows` (default 3), `show_sql` (default false — include SQL for the sample-data query, the custom-SQL block, model-level filters, and aggregation formulas/param SQL), `format` (default `"markdown"`; also `"json"`), `sections` (subset of `["columns", "measures", "aggregations", "joins", "samples", "learnings", "saved_queries"]` — default `None`/`[]` renders all seven; sections in the first four collapse to a one-line backticked CSV of names when omitted, `samples`/`learnings`/`saved_queries` are dropped entirely, unknown names emit a footer warning. A non-empty list of *only* unknown names resolves to no sections — "all seven" is reserved for `None`/`[]` so a typo can't silently trigger the full payload), `descriptions_max_chars` (when set, truncate each description longer than this with the suffix `"... [truncated]"` (prefixed by a space); applies to model, columns, measures, and aggregations; must be `>= 0`). When any section is trimmed, a quoted-Markdown footer at the end of the response lists what was shown / names-only / omitted, with a hint on how to fetch more. The JSON form mirrors this: trimmed sections appear as `<section>_names: [...]` siblings, fully omitted ones are absent, and top-level `omitted_sections`, `names_only_sections`, `unknown_sections` arrays are added when non-empty. For multi-hop reachability use the `search` tool. |
| `create_model` | Create a model from a table/SQL definition or from a query. Pass `sql_table`/`sql` with `columns` (and optional named-formula `measures` and custom `aggregations`) for table-based, or pass `query` (a SLayer query dict) to save it as a query-backed model whose `columns` are populated by a save-time dry-run; `access_tags` sets the access groups allowed to see it. |
| `edit_model` | Edit an existing model in one call. Upserts `columns`, `measures` (named formulas), `aggregations` and `joins` by name (only the given fields change). Also accepts `description`, `data_source` (required when the name exists in several datasources), `new_data_source`, `default_time_dimension`, `sql_table` / `sql` / `source_queries`, `query_variables`, `hidden`, `access_tags`, `meta`, `add_filters` / `remove_filters`, and `remove: {"columns": [...], "measures": [...], "aggregations": [...], "joins": [...], "join_edges": [...]}`, applied before the upserts. See [Customize a model](#customize-a-model). |
| `delete_model` | Delete a model entirely. |
| `validate_models` | Diff the stored models against the live database schemas (read-only) and return, as JSON, the deletes needed to keep them valid. Params: `data_source` (omit for all). See [Schema Drift](../concepts/schema-drift.md). |
| `recommend_root_model` | Recommend the root model (`source_model`) for a set of items, with each item's reference path from it. Params: `items`, `data_source`, `root_hint`, `format` (`"markdown"` / `"json"`). See [Choosing a root model](../concepts/queries.md#choosing-a-root-model). |

### Querying

| Tool | Description |
|------|-------------|
| `query` | Execute a semantic query. The `query` argument mirrors the engine: a model name (run a query-backed model by name), a single query object, or a list of query objects — a multi-stage DAG whose stages reference one another via `source_model` or `source_model.joins[].target_model` (there is no top-level `joins` field), with non-root stages auto-sorted by the engine (their order doesn't matter) and the last entry the returned root (place the intended root last). Plus the wrappers `variables` / `refine` (model-name form only: clauses merged into the saved query's final stage) / `show_sql` / `dry_run` / `explain` / `format`. Without an explicit `limit` on the root query the response is capped at 20 rows with a truncation notice via the warnings channel. For a query object or multi-stage list the cap is pushed into the generated SQL as `LIMIT 21` (so truncation is detectable); a model run by name is capped response-side with its stored SQL untouched. See [Queries](../concepts/queries.md) and [Multistage Queries](../examples/06_multistage_queries/multistage_queries.md). |

**`query` tool arguments:**

| Param | Type | Description |
|-------|------|-------------|
| `query` | string \| object \| list[object] | A model name, a single query object (fields below), or a list of query objects forming a multi-stage DAG (each stage takes the same fields plus an optional `name`); an invalid argument reports only the errors of the form its JSON type selects. Required. |
| `variables` | dict | Values for `{var}` placeholders in every query expression (measures, dimensions, time dimensions, filters, order, `date_range` bounds) and in a model's SQL; a placeholder binds a literal value only. Precedence: runtime > named-stage > outer-query > model `query_variables`. |
| `show_sql` | bool | Include the generated SQL in the response for debugging |
| `dry_run` | bool | Generate and return the SQL without executing it |
| `explain` | bool | Run the database's EXPLAIN and return the query plan |
| `format` | string | Output format: `"markdown"` (default, compact), `"json"` (structured), or `"csv"` (most compact). Case-insensitive. Warnings (including the truncation notice) and field attributes stay machine-safe: a trailing `Warnings:` block (attributes footer just before it) in markdown, leading `#` comment lines in csv, and inside the json payload — which becomes `{"data", "warnings"?, "attributes"?}` instead of a bare array once either is present |

**Query object fields** (the shape of `query`, and of each stage in the list form):

| Field | Type | Description |
|-------|------|-------------|
| `source_model` | string \| ModelExtension \| SlayerModel | Model name (string), inline `ModelExtension` dict (`{"source_name": "orders", "columns": [...], "joins": [...], "measures": [...]}` — extend a saved model with extras for this query), or inline `SlayerModel` dict (`{"name": "ad_hoc", "sql_table": "...", "data_source": "...", "columns": [...]}` — define a model ad-hoc). Required. |
| `measures` | list | Aggregated values: column-aggregations, arithmetic, transforms. E.g. `["count(*)", {"formula": "sum(revenue) / count(*)", "name": "aov", "label": "Average Order Value"}, "cumsum(sum(revenue))"]`. Each entry has an optional `label` for human-readable display. Supports nesting: `"change(cumsum(sum(revenue)))"`. Bare names resolve to saved `ModelMeasure` formulas on the model. |
| `dimensions` | list | Dimension names or dotted paths, e.g. `["status", "customers.regions.name"]`, or computed dimensions `{"expression": "month(created_at)", "name": "month"}`. When using the engine directly, dimensions accept an optional `label` via `{"name": "status", "label": "Order Status"}`. |
| `filters` | list[str] | Filter formula strings, e.g. `["status = 'active'", "amount > 100"]`. Supports operators (`=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, `IS [NOT] TRUE` / `FALSE`, `IS [NOT] DISTINCT FROM`, `LIKE`, `ILIKE`, regex `~`, JSON `->>`, `CAST(x AS type)`), the list functions `contains` / `contains_any` / `contains_all`, boolean logic (`AND`, `OR`, `NOT`), and inline transform expressions (`"change(revenue) > 0"`). Filters on measures are automatically routed to HAVING. |
| `time_dimensions` | list[dict \| str] | Time grouping. Each entry is a dict whose `dimension` may be any DATE / TIMESTAMP expression, with an optional `name`, or the string form `"month(created_at)"`; it also supports an optional `label` for display and a [`date_range`](../concepts/time.md#date_range): one time point or a `[lower, upper]` pair. |
| `main_time_dimension` | string | Name of the time dimension transforms (`change` / `lag` / …) key off, overriding auto-detection when a query has multiple time dimensions. |
| `order` | list[dict] | Sorting, e.g. `[{"column": "count(*)", "direction": "desc", "nulls": "last"}]`; `column` is a result name or any aggregate or transform expression, and `nulls` (`"first"` / `"last"`) is optional |
| `limit` | int | Max rows, trusted verbatim; without it the response is capped at 20 rows with a truncation notice |
| `offset` | int | Skip rows |
| `whole_periods_only` | bool | Snap time bounds to bucket boundaries and exclude the current incomplete bucket — see [Time](../concepts/time.md#whole_periods_only) |
| `distinct_dimension_values` | bool | Default `true` — auto-dedup dim-only queries (`GROUP BY <dim/td aliases>`). Set `false` to emit raw rows (no top-level `GROUP BY`); rejects any measure reference in `measures` / `filters` / `order`. |
| `to_many_handling` | str | Default `"broadcast"` — how an aggregate resolves query dimensions [unattributable from its root](../concepts/queries.md#cross-model-measures): `broadcast` (repeat and warn), `associate` (per-cell value over the distinct associated entities), or `error` (refuse). A query-object field; the retired `strict` flag is rejected with this remedy. |
| `variables` | dict | Per-stage `{var}` values, scoped to this stage; overridden by the top-level `variables` arg (see precedence above). |
| `name` | string | List form only — names a stage so sibling stages can reference it via `source_model` (every non-final stage must be named). |

### Memories + semantic search

Memories are free-form notes the agent saves against canonical entity strings (`<ds>`, `<ds>.<model>`, `<ds>.<model>.<leaf>`, or `memory:<id>`). `search` is the only retrieval surface and returns memories **and** entity discovery hits in one flat list. See [Memories](../concepts/memories.md) and [Search](../concepts/search.md).

| Tool | Description |
|------|-------------|
| `search` | Up to three-channel retrieval over memories and canonical entities (datasource / model / column / measure / aggregation), RRF-fused (k=60) into a single ranked list. |
| `save_memory` | Persist a free-form `learning` tagged with canonical entities or an inline `SlayerQuery`. |
| `forget_memory` | Delete a memory by id. Cascade-strips every other memory's `memory:<id>` ref to it. |

**`search` parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `entities` | list[str] | Canonical entity strings (`mydb.orders.amount`, `memory:42`, …) or aggregated forms (`sum(revenue)` — the aggregation is stripped to the column). Drives the BM25 channel. Unresolved tokens emit warnings, not errors. |
| `query` | dict \| SlayerQuery | Inline query; its `source_model`, dimensions, measures, time dims, and filters are walked for canonical entities. |
| `question` | str | Free-text question. Drives the Tantivy full-text channel and (when available) the dense-embedding channel. |
| `datasource` | str | When set, every channel pre-filters to canonical ids rooted at that datasource. Unknown name → error. |
| `cypher_filter` | str | Graph pre-filter applied to all three channels. Full openCypher when the `advanced_search` extra is installed (LadybugDB property graph with `Memory` / `Datasource` / `Model` / `ModelColumn` / `Measure` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `JOINS` edges; read-only — `CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`, `DROP`, `CALL` are rejected). Without the extra, only the naive form `MATCH (n:Label1:Label2…) RETURN n.id AS id` is accepted as a label/kind filter; anything richer raises with an install hint. |
| `max_results` | int | Cap applied **after** RRF fusion and after the `cypher_filter` narrowing, so it counts surviving items only. Default `10`. |

**Response shape (`SearchResponse`):**

```json
{
  "results": [
    {"kind": "memory",  "id": "42", "score": 0.13, "text": "...", "matched_entities": ["mydb.orders.amount"], "query": null},
    {"kind": "column",  "id": "mydb.orders.amount", "score": 0.11, "text": "...", "matched_entities": [], "query": null},
    {"kind": "model",   "id": "mydb.orders",        "score": 0.09, "text": "...", "matched_entities": [], "query": null}
  ],
  "resolved_input_entities": ["mydb.orders.amount"],
  "warnings": []
}
```

`kind` is one of `"memory"`, `"datasource"`, `"model"`, `"column"`, `"measure"`, `"aggregation"`. For memory hits, `id` is the raw memory id (suitable for `forget_memory`); `hit.query is not None` marks a saved example query. Column hits carry the column's structured sample-value snapshot — the top 50 `sampled_values` are rendered as a JSON array (so values containing commas survive); overflow columns (> 50 distinct) are marked `50+ distinct` in the text snapshot. `SearchService` refreshes any column hit whose profile is stale on the fly.

**`save_memory` parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `learning` | str | The free-form note. |
| `linked_entities` | list[str] \| dict (SlayerQuery) | Canonical entity strings (strict resolution; `memory:<id>` is valid for cross-memory refs) **or** an inline `SlayerQuery` dict whose entities are auto-extracted and which is persisted on the memory. |
| `id` | str (optional) | User-pinned canonical memory id. Forbidden charset: `:`, `/`, `?`, `#`, whitespace, ASCII control. Omit to let the allocator assign the next int-shaped id (`max(int-shaped id) + 1`, never less than `"1"`). Duplicate id → unconditional upsert; `created_at` is preserved. |

**`forget_memory` parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `id` | str | Memory id. Cascade strips every `memory:<id>` ref to it from every other memory's `entities` list and drops the matching embedding row. |

**Cypher filter examples.** Naive form (always available):

```
MATCH (n:Memory) RETURN n.id AS id          # memory hits only
MATCH (n:Column:Measure) RETURN n.id AS id  # column + named-measure hits only
```

Full openCypher (requires `advanced_search`):

```
MATCH (d:Datasource {name: 'mydb'})-[:CONTAINS]->(m:Model)-[:CONTAINS]->(c:ModelColumn)
RETURN c.id AS id
```

## Help memories

SLayer's conceptual help is not a tool: it ships as a predefined set of **help memories** seeded into storage on server startup. Read the entry point with `inspect(reference="memory:help.intro", entity_type="memory")`; it lists the deep-dive topics, each read the same way, and `search(question="…")` also surfaces the relevant one. `compact` never shortens a `help.*` memory in `inspect`; `descriptions_max_chars` still does.

| Topic id | Covers |
|----------|--------|
| `memory:help.intro` | What SLayer is, the judgment calls queries require, and the deep-dive topics (the query syntax itself is on the `query` tool's schema fields) |
| `memory:help.models` | Authoring [models](../concepts/models.md): source, columns, saved measures, custom aggregations, joins, model filters, result keys, query-backed models |
| `memory:help.workflow` | Discovery, query method, filter literals, building and verifying a result, common-error decoder, connecting a database |
| `memory:help.aggregations` | Every aggregation, expressions and booleans, empty-input values, result names, `partition_by`, `window`, nesting |
| `memory:help.transforms` | Every transform, the time axis, ranking, limits |
| `memory:help.time` | Time points, `date_range` and look-back, granularity calls |
| `memory:help.joins` | Dotted paths, choosing the root, joined aggregates (broadcast vs associate), patterns such as anti-joins |
| `memory:help.queries` | Query shapes, stages and their column names, raw rows, functions, variables, debugging |

## Typical Agent Workflows

### Connect and explore a new database

```
1. create_datasource(name="mydb", type="postgres", host="localhost", database="app", username="user", password="pass")
   # auto_ingest=true by default — models are generated automatically
2. models_summary(datasource_name="mydb")      # see what was generated
3. inspect(reference="orders", entity_type="model", compact=False)  # see schema + sample data
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
3. inspect(reference="orders", entity_type="model", compact=False)  # see schema + sample data
4. query(query={"source_model": "orders", "measures": ["count(*)"], "dimensions": ["status"], "limit": 10})
```

### Customize a model

```
1. edit_model(
     model_name="orders",
     columns=[{"name": "priority", "sql": "priority", "type": "TEXT"}],
     measures=[{"name": "aov", "formula": "sum(revenue) / count(*)", "label": "Average Order Value"}],
     remove={"columns": ["legacy_field"]}
   )
```

Upsert semantics: if a column/measure/aggregation/join with that name already exists, only the provided fields are updated. To remove entities, use the `remove` dict keyed by type (`"columns"`, `"measures"`, `"aggregations"`, `"joins"`, `"join_edges"`); a join is matched by its name, else its target and `join_pairs`, else the only join to its target, and two joins to one target need edge names (removable by name, or exactly via `join_edges: [{target_model, name, join_pairs}]`). `measures` here are named formulas (`{formula, name, label, description}`) — the row-level `sql` definitions live under `columns`.