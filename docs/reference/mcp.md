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

`--ingest-on-startup` runs idempotent auto-ingestion across every configured datasource before the stdio channel opens, so datasets are available on the agent's first tool call. Drop it (or set `SLAYER_INGEST_ON_STARTUP=0`) to defer ingestion to a manual `ingest_datasource` call.

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
| `create_datasource` | Create a DB connection, test it, and auto-ingest datasets (set `auto_ingest=false` to skip). |
| `list_datasources` | List configured datasources (no credentials shown). |
| `describe_datasource` | Show details, test connection, list available schemas, and (by default) list tables in the given or default schema. Params: `name`, `list_tables` (default `true`), `schema_name` (empty = dialect default). |
| `edit_datasource` | Edit an existing datasource config. |
| `delete_datasource` | Remove a datasource config. |
| `set_datasource_priority` | Set the datasource priority list (most preferred first; `[]` clears it): a bare dataset name that exists in several datasources resolves to the first listed datasource that has it, else errors as ambiguous. Params: `priority`. |
| `get_datasource_priority` | Return the datasource priority list, or `[]` when none is set. |
| `ingest_datasource` | Auto-generate datasets from DB schema with rollup relationships. Params: `datasource_name`, `include_tables`, `schema_name`. Reports `Skipped` objects and `Hidden` [recognised internals](cli.md#recognised-internals) alongside the datasets it created — including on a no-op re-ingest. Use `edit_dataset("<dataset>", data_source="<datasource>", hidden=False)` to surface a hidden one — internals collide across datasources, so the bare form can un-hide the wrong dataset. |

### Dataset Management

| Tool | Description |
|------|-------------|
| `datasets_summary` | Brief summary of all non-hidden datasets in a datasource. `compact=True` (default) gives each dataset's name, description, field count, metric names and relationship targets; `compact=False` adds the full tables of its **fields** (`name`, `datatype`, `description`) and **metrics** (named expressions). Neither form includes distinct values or sample data — call `inspect` (`entity_type="dataset"`, `compact=False`) for those. For multi-hop discovery (fields reachable via relationships from a given dataset), use the `search` tool with `cypher_filter` for graph queries. Params: `datasource_name`, `format` (default `"markdown"`; also `"json"`), `compact` (default true). |
| `inspect` | Single-entity point lookup: returns the rendered detail for **exactly one** entity by `reference` + required `entity_type` (`datasource`/`dataset`/`field`/`metric`/`aggregation`/`memory`). No fusion/ranking/cypher and no bundled memories — use `search` instead when you want an entity surfaced *in context*. `reference` accepts canonical ids, bare names, relationship paths (`orders.customers.region` → resolved to the owning dataset), and `memory:<id>`; it is normalised via the shared resolver and the normalised id is echoed in the JSON shape. `entity_type` disambiguates the 3-part canonical collision (a name shared by, e.g., a field and an aggregation) and asserts the resolved kind (mismatch → detailed error). Renders hidden entities. Params: `reference`, `entity_type`, `compact` (default true), `format` (`"markdown"` default, `"json"`), and the dataset-only `num_rows`/`show_sql`/`sections`/`descriptions_max_chars` (applied for `entity_type="dataset"`; `descriptions_max_chars` applies to every kind; the others are ignored with a warning for non-dataset kinds, `show_sql` a silent no-op for field/metric/aggregation). **`compact=True`** renders description-only for leaf/datasource/memory kinds (`compact` never shortens a `help.*` memory; `descriptions_max_chars` still does), and a cheap **schema skeleton** (field/metric/aggregation **names** + relationship targets, **zero DB calls**) for the `dataset` kind — markdown always emits the four `Fields:`/`Metrics:`/`Aggregations:`/`Relationships to:` lines (`_(none)_` when empty); JSON always carries `field_names`/`metric_names`/`aggregation_names`/`relationships_to`. **`compact=False`** renders the full dataset view (sample rows, SQL, sections) for the `dataset` kind, and renders a per-dataset skeleton for each visible dataset (sorted by name; `datasets: [...]` in JSON) for the `datasource` kind. The full dataset view (`compact=False`) shows metadata with row count (and a `**meta:**` bullet when the dataset has `meta` set), any dataset-level or field-level filters, **fields table** (with a `sampled` column — distinct values for string/boolean fields, `min .. max` for number/date/time fields — and a `meta` cell when set), **metrics table** of named expressions (with `expression`, `label`, `description`, `meta`), custom aggregations (with `meta`), direct relationships (a query-backed dataset's inherited edges included, marked `inherited`; in JSON with `inherited_from` and a top-level `inherited_relationships_error`, the reason when they could not be derived), and a sample-data table. Every Markdown table auto-prunes all-empty columns (so the `meta` column is hidden when no entity has meta) and collapses to a comma-separated backticked list when only one column remains. Its params: `num_rows` (default 3), `show_sql` (default false — include SQL for the sample-data query, the custom-SQL block, dataset-level filters, and aggregation expressions/param SQL), `sections` (subset of `["fields", "metrics", "aggregations", "relationships", "samples", "learnings", "saved_queries"]` — default `None`/`[]` renders all seven; sections in the first four collapse to a one-line backticked CSV of names when omitted, `samples`/`learnings`/`saved_queries` are dropped entirely, unknown names emit a footer warning. A non-empty list of *only* unknown names resolves to no sections — "all seven" is reserved for `None`/`[]` so a typo can't silently trigger the full payload), `descriptions_max_chars` (when set, truncate each description longer than this with the suffix `"... [truncated]"` (prefixed by a space); applies to dataset, fields, metrics, and aggregations; must be `>= 0`). When any section is trimmed, a quoted-Markdown footer at the end of the response lists what was shown / names-only / omitted, with a hint on how to fetch more. The JSON form mirrors this: trimmed sections appear as `<section>_names: [...]` siblings, fully omitted ones are absent, and top-level `omitted_sections`, `names_only_sections`, `unknown_sections` arrays are added when non-empty. JSON `text` is present **iff non-empty** (omitted under `compact=True` for every kind except a `help.*` memory). Three-tier escalation: `datasets_summary(compact)` (field count) < `inspect(dataset, compact=True)` (field names) < `inspect(dataset, compact=False)` (full). **Batch:** `reference` also accepts a **list** of ids — a homogeneous-kind batch (one `entity_type` for all). A single `str` keeps single-id output byte-for-byte; a list returns one block per id in input order, each echoing its resolved canonical id (a `## <canonical>` header per block in markdown, a JSON array under `format="json"`). A one-element list is still batch-framed. Per-id resolution errors are isolated (in JSON a failed id is a `{"reference", "error"}` object). **Collection:** omit `reference` (or pass `null`/`[]`) to list a whole kind. `entity_type="dataset"` lists all datasets grouped by datasource — `compact=True` (default) is a terse one-liner per dataset (`- \`name\` (N fields; relationships: ...)` under a `# Datasource: \`<ds>\` — <N> dataset(s)` header); `compact=False` is the full `datasets_summary` block per datasource. `entity_type="datasource"` lists all datasources (`compact=True` == `list_datasources`; `compact=False` adds descriptions + per-dataset skeletons). Only `dataset`/`datasource` support the collection view; other kinds raise. A query-backed dataset's entry in `datasets_summary` and in both dataset collection views also carries its inherited edges with their source edge (`inherited_relationships` in JSON) and why they could not be derived (`inherited_relationships_error`). This subsumes `datasets_summary` / `list_datasources` (kept as thin aliases). JSON collection form is `{"entity_type", "collection": true, "datasources": [...], "warnings": []}`. |
| `create_dataset` | Create a dataset from a table/SQL definition or from a query. Pass `sql_table`/`sql` with `fields` (and optional named-expression `metrics` and custom `aggregations`) for table-based, or pass `query` (a SLayer query dict) to save it as a query-backed dataset whose `fields` are populated by a save-time dry-run; `access_tags` sets the access groups allowed to see it. |
| `edit_dataset` | Edit an existing dataset in one call. Upserts `fields`, `metrics` (named expressions), `aggregations` and `relationships` by name (only the given fields change). Also accepts `description`, `data_source` (required when the name exists in several datasources), `new_data_source`, `default_time_dimension`, `sql_table` / `sql` / `source_queries`, `query_variables`, `hidden`, `access_tags`, `meta`, `add_filters` / `remove_filters`, and `remove: {"fields": [...], "metrics": [...], "aggregations": [...], "relationships": [...], "relationship_edges": [...]}`, applied before the upserts. See [Customize a dataset](#customize-a-dataset). |
| `delete_dataset` | Delete a dataset entirely. |
| `validate_datasets` | Diff the stored datasets against the live database schemas (read-only) and return, as JSON, the drift findings: the deletes needed to keep them valid plus report-only `retype` and `narrowed_access` entries. Params: `data_source` (omit for all). See [Schema Drift](../concepts/schema-drift.md). |
| `recommend_population` | Recommend the population (root dataset) for a set of items, with each item's reference path from it. Params: `items`, `data_source`, `root_hint`, `format` (`"markdown"` / `"json"`). See [Choosing a root dataset](../concepts/queries.md#choosing-a-root-dataset). |

### Querying

| Tool | Description |
|------|-------------|
| `query` | Execute a semantic query. The `query` argument mirrors the engine: a dataset name (run a query-backed dataset by name), a single query object, or a list of query objects — a multi-stage DAG whose stages reference one another via `population` or `population.relationships[].to` (there is no top-level `relationships` field), with non-root stages auto-sorted by the engine (their order doesn't matter) and the last entry the returned root (place the intended root last). Plus the wrappers `variables` / `refine` (dataset-name form only: clauses merged into the saved query's final stage) / `show_sql` / `dry_run` / `explain` / `format`. Without an explicit `limit` on the root query the response is capped at 20 rows with a truncation notice via the warnings channel. For a query object or multi-stage list the cap is pushed into the generated SQL as `LIMIT 21` (so truncation is detectable); a dataset run by name is capped response-side with its stored SQL untouched. See [Queries](../concepts/queries.md) and [Multistage Queries](../examples/06_multistage_queries/multistage_queries.md). |

**`query` tool arguments:**

| Param | Type | Description |
|-------|------|-------------|
| `query` | string \| object \| list[object] | A dataset name, a single query object (fields below), or a list of query objects forming a multi-stage DAG (each stage takes the same fields plus an optional `name`); an invalid argument reports only the errors of the form its JSON type selects. Required. |
| `variables` | dict | Values for `{var}` placeholders in every query expression (metrics, dimensions, time dimensions, filter, order, `date_range` bounds) and in a dataset's SQL; a placeholder binds a literal value only. Precedence: runtime > named-stage > outer-query > dataset `query_variables`. |
| `show_sql` | bool | Include the generated SQL in the response for debugging |
| `dry_run` | bool | Generate and return the SQL with the run's warnings, attributes and inferred population, without executing it; under `format="json"` the reply is `{"sql", "warnings"?, "attributes"?, "population"?, "population_inferred"?}` (each only when present) |
| `explain` | bool | Run the database's EXPLAIN and return the query plan |
| `format` | string | Output format: `"markdown"` (default, compact), `"json"` (structured), or `"csv"` (most compact). Case-insensitive. Warnings (including the truncation notice), field attributes and an inferred population stay machine-safe: a trailing `Warnings:` block (attributes and population footers just before it) in markdown, leading `#` comment lines in csv, and inside the json payload — which becomes `{"data", "warnings"?, "attributes"?, "population"?, "population_inferred"?}` instead of a bare array once any is present. In json a list is a JSON array and a decimal a number with its exact digits; csv / markdown carry lists and JSON values as JSON text; everywhere a UUID is its string, bytes are `\x` + lowercase hex, an interval is an ISO-8601 duration and NaN / ±Infinity is `null` inside JSON |

**Query object fields** (the shape of `query`, and of each stage in the list form):

| Field | Type | Description |
|-------|------|-------------|
| `population` | string | A stored dataset or stage name. Omit it to infer the population. |
| `extensions` | list[DatasetExtension] | Query-scoped `metrics` and `relationships` added to any dataset the query reads, one entry per dataset: `[{"dataset": "customers", "relationships": [...], "metrics": [...]}]`; a relationship repeating an existing edge is that edge, and an `inner` relationship keeps only the population rows with a match ([details](../concepts/queries.md#query-extensions)). A per-query field is an expression in the position that uses it. |
| `metrics` | list | Aggregated values: field aggregations, arithmetic, transforms. E.g. `["count(orders.*)", {"metric": "sum(orders.revenue) / count(orders.*)", "alias": "aov", "label": "Average Order Value"}, "cumsum(sum(orders.revenue))"]`. Each entry has an optional `label` for human-readable display. Supports nesting: `"change(cumsum(sum(orders.revenue)))"`. A saved `DatasetMetric` is written `<dataset>.<metric>`. |
| `dimensions` | list | Dimension names or dotted paths, e.g. `["orders.status", "customers.regions.name"]`, or computed dimensions `{"field": "month(orders.created_at)", "alias": "month_of_year"}`. A dimension takes an optional `label` via `{"field": "orders.status", "label": "Order Status"}`. |
| `filter` | str \| list[str] | A condition string, or a list of them all of which must hold, e.g. `"orders.status = 'active'"` or `["orders.status = 'active'", "orders.amount > 100"]`. Supports operators (`=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, `IS [NOT] TRUE` / `FALSE`, `IS [NOT] DISTINCT FROM`, `LIKE`, `ILIKE`, regex `~`, JSON `->>`, `CAST(x AS type)`), the list functions `contains` / `contains_any` / `contains_all`, boolean logic (`AND`, `OR`, `NOT`), and inline transform expressions (`"change(sum(orders.revenue)) > 0"`). Filters on metrics are automatically routed to HAVING. |
| `time_dimensions` | list[dict \| str] | Time grouping. Each entry is a dict whose `field` may be any date or time expression, with an optional `alias`, or the string form `"date_trunc('month', orders.created_at)"`; it also supports an optional `label` for display and a [`date_range`](../concepts/time.md#date_range): one time point or a `[lower, upper]` pair. |
| `main_time_dimension` | string | Name of the time dimension transforms (`change` / `lag` / …) key off, overriding auto-detection when a query has multiple time dimensions. |
| `order` | list[dict] | Sorting, e.g. `[{"field": "count(orders.*)", "direction": "desc", "nulls": "last"}]`; `field` is a result name or any aggregate or transform expression, and `nulls` (`"first"` / `"last"`) is optional |
| `limit` | int | Max rows, trusted verbatim; without it the response is capped at 20 rows with a truncation notice |
| `offset` | int | Skip rows |
| `whole_periods_only` | bool | Snap time bounds to bucket boundaries and exclude the current incomplete bucket — see [Time](../concepts/time.md#whole_periods_only) |
| `distinct_dimension_values` | bool | Default `true` — auto-dedup dim-only queries (`GROUP BY <dim/td aliases>`). Set `false` to emit raw rows (no top-level `GROUP BY`); rejects any metric reference in `metrics` / `filter` / `order`. |
| `to_many_handling` | str | Optional — how an aggregate resolves query dimensions [unattributable from its root](../concepts/queries.md#cross-dataset-metrics): omitted (per-cell value over the distinct associated rows, broadcasting with a warning where association is unsupported), `associate` (the same, refusing the unsupported), `broadcast` (repeat and warn), or `error` (refuse). A query-object field. |
| `variables` | dict | Per-stage `{var}` values, scoped to this stage; overridden by the top-level `variables` arg (see precedence above). |
| `name` | string | List form only — names a stage so sibling stages can reference it via `population` (every non-final stage must be named). |

### Memories + semantic search

Memories are free-form notes the agent saves against canonical entity strings (`<ds>`, `<ds>.<dataset>`, `<ds>.<dataset>.<leaf>`, or `memory:<id>`). `search` is the only retrieval surface and returns memories **and** entity discovery hits in one flat list. See [Memories](../concepts/memories.md) and [Search](../concepts/search.md).

| Tool | Description |
|------|-------------|
| `search` | Up to three-channel retrieval over memories and canonical entities (datasource / dataset / field / metric / aggregation), RRF-fused (k=60) into a single ranked list. |
| `save_memory` | Persist a free-form `learning` tagged with canonical entities or an inline `SlayerQuery`. |
| `forget_memory` | Delete a memory by id. Cascade-strips every other memory's `memory:<id>` ref to it. |

**`search` parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `entities` | list[str] | Canonical entity strings (`mydb.orders.amount`, `memory:42`, …) or aggregated forms (`sum(revenue)` — the aggregation is stripped to the field). Drives the BM25 channel. Unresolved tokens emit warnings, not errors. |
| `query` | dict \| SlayerQuery | Inline query; its `population`, dimensions, metrics, time dims, and filter are walked for canonical entities. |
| `question` | str | Free-text question. Drives the Tantivy full-text channel and (when available) the dense-embedding channel. |
| `datasource` | str | When set, every channel pre-filters to canonical ids rooted at that datasource. Unknown name → error. |
| `cypher_filter` | str | Graph pre-filter applied to all three channels. Full openCypher when the `advanced_search` extra is installed (LadybugDB property graph with `Memory` / `Datasource` / `Dataset` / `DatasetField` / `Metric` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `RELATIONSHIPS` edges; read-only — `CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`, `DROP`, `CALL` are rejected). Without the extra, only the naive form `MATCH (n:Label1:Label2…) RETURN n.id AS id` is accepted as a label/kind filter; anything richer raises with an install hint. |
| `max_results` | int | Cap applied **after** RRF fusion and after the `cypher_filter` narrowing, so it counts surviving items only. Default `10`. |

**Response shape (`SearchResponse`):**

```json
{
  "results": [
    {"kind": "memory",  "id": "42", "score": 0.13, "text": "...", "matched_entities": ["mydb.orders.amount"], "query": null},
    {"kind": "field",   "id": "mydb.orders.amount", "score": 0.11, "text": "...", "matched_entities": [], "query": null},
    {"kind": "dataset", "id": "mydb.orders",        "score": 0.09, "text": "...", "matched_entities": [], "query": null}
  ],
  "resolved_input_entities": ["mydb.orders.amount"],
  "warnings": []
}
```

`kind` is one of `"memory"`, `"datasource"`, `"dataset"`, `"field"`, `"metric"`, `"aggregation"`. For memory hits, `id` is the raw memory id (suitable for `forget_memory`); `hit.query is not None` marks a saved example query. Field hits carry the field's structured sample-value snapshot — the top 50 `sampled_values` are rendered as a JSON array (so values containing commas survive); overflow fields (> 50 distinct) are marked `50+ distinct` in the text snapshot. `SearchService` refreshes any field hit whose profile is stale on the fly.

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
MATCH (n:DatasetField:Metric) RETURN n.id AS id  # field + named-metric hits only
```

Full openCypher (requires `advanced_search`):

```
MATCH (d:Datasource {name: 'mydb'})-[:CONTAINS]->(m:Dataset)-[:CONTAINS]->(c:DatasetField)
RETURN c.id AS id
```

## Help memories

SLayer's conceptual help is not a tool: it ships as a predefined set of **help memories** seeded into storage on server startup. Read the entry point with `inspect(reference="memory:help.intro", entity_type="memory")`; it lists the deep-dive topics, each read the same way, and `search(question="…")` also surfaces the relevant one. `compact` never shortens a `help.*` memory in `inspect`; `descriptions_max_chars` still does.

| Topic id | Covers |
|----------|--------|
| `memory:help.intro` | What SLayer is, the judgment calls queries require, and the deep-dive topics (the query syntax itself is on the `query` tool's schema fields) |
| `memory:help.datasets` | Authoring [datasets](../concepts/datasets.md): source, fields, saved metrics, custom aggregations, relationships, dataset filters, result keys, query-backed datasets |
| `memory:help.workflow` | Discovery, query method, filter literals, building and verifying a result, common-error decoder, connecting a database |
| `memory:help.aggregations` | Every aggregation, expressions and booleans, empty-input values, result names, `partition_by`, `window`, nesting |
| `memory:help.transforms` | Every transform, the time axis, ranking, limits |
| `memory:help.time` | Time points, `date_range` and look-back, time buckets |
| `memory:help.relationships` | Dotted paths, choosing the root, joined aggregates (broadcast vs associate), patterns such as anti-joins |
| `memory:help.queries` | Query shapes, stages and their column names, raw rows, functions, variables, debugging |

## Typical Agent Workflows

### Connect and explore a new database

```
1. create_datasource(name="mydb", type="postgres", host="localhost", database="app", username="user", password="pass")
   # auto_ingest=true by default — datasets are generated automatically
2. datasets_summary(datasource_name="mydb")    # see what was generated
3. inspect(entity_type="dataset", reference="mydb.orders", compact=False)  # see schema + sample data
```

To explore first without auto-ingesting:

```
1. create_datasource(name="mydb", type="postgres", host="localhost", database="app", username="user", password="pass", auto_ingest=false)
2. describe_datasource(name="mydb", schema_name="public")  # verify connection + list tables
3. ingest_datasource(datasource_name="mydb", schema_name="public")
4. datasets_summary(datasource_name="mydb")    # see what was generated
```

### Query data

```
1. list_datasources()                              # pick a datasource
2. datasets_summary(datasource_name="mydb")    # discover its datasets
3. inspect(entity_type="dataset", reference="mydb.orders", compact=False)  # see schema + sample data
4. query(query={"population": "orders", "metrics": ["count(orders.*)"], "dimensions": ["orders.status"], "limit": 10})
```

### Customize a dataset

```
1. edit_dataset(
     dataset_name="orders",
     fields=[{"name": "priority", "sql": "priority", "datatype": "String"}],
     metrics=[{"name": "aov", "expression": "sum(revenue) / count(*)", "label": "Average Order Value"}],
     remove={"fields": ["legacy_field"]}
   )
```

Upsert semantics: if a field/metric/aggregation/relationship with that name already exists, only the provided keys are updated. To remove entities, use the `remove` dict keyed by type (`"fields"`, `"metrics"`, `"aggregations"`, `"relationships"`, `"relationship_edges"`); a relationship is matched by its name, else its target, `from_columns` and `to_columns`, else the only relationship to its target, and two relationships to one target need edge names (removable by name, or exactly via `relationship_edges: [{to, name, from_columns, to_columns}]`). `metrics` here are named expressions (`{expression, name, label, description}`) — the row-level `sql` definitions live under `fields`.