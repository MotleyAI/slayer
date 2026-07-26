---
description: Overview of SLayer — a lightweight semantic layer for AI agents. Use when you need to understand SLayer capabilities or architecture.
---

# SLayer Overview

SLayer is a lightweight, agent-first semantic layer. Instead of writing raw SQL, agents describe data they want (measures, dimensions, filters) and SLayer generates and executes SQL.

## Architecture

- **SlayerQueryEngine** — central orchestrator. Its `_enrich()` method resolves a SlayerQuery + SlayerModel into an EnrichedQuery (fully resolved SQL expressions), then passes it to SQLGenerator for SQL generation
- **SQLGenerator** — takes an EnrichedQuery (not SlayerQuery) and converts it to SQL via sqlglot (dialect-aware: postgres, mysql, bigquery, etc.)
- **SlayerSQLClient** — executes SQL via SQLAlchemy with retry logic and statement timeouts
- **Storage** — YAML or SQLite backends for model and datasource configs
- **Ingestion** — auto-generates models from DB schema with rollup-style FK joins (denormalized LEFT JOINs). It can be triggered manually (`slayer ingest`, `ingest_datasource_models`, `POST /ingest`) or **on every server boot** via `slayer serve --ingest-on-startup` / `slayer mcp --ingest-on-startup` (also `SLAYER_INGEST_ON_STARTUP=1`, or `create_app/create_mcp_server(ingest_on_startup=True)` programmatically). It is idempotent and continues on per-datasource failures.
- **Interfaces** — MCP server (stdio via `slayer mcp`, SSE via `slayer serve` at `/mcp/sse`), REST API (FastAPI on port 5143), Python SDK, and two read-only wire-protocol facades for BI tools: Arrow Flight SQL (`slayer flight-serve`, port 5144) and Postgres (`slayer pg-serve`, port 5145; the connection `database` selects the SLayer datasource)

## Key Models

- **SlayerModel** — has one of three source modes: `sql_table` (physical table), `sql` (explicit SQL), or `source_queries` (query-backed — rows are the result of saved `SlayerQuery` stages). Optional `query_variables` defaults for `{var}` placeholders. Engine-managed `columns` and `backing_query_sql` cache for query-backed models. Defined in YAML or auto-generated.
- **SlayerQuery** — specifies model, measures, dimensions, time_dimensions, filters, order, limit, variables
- **DatasourceConfig** — DB connection details with `${ENV_VAR}` resolution

Query-backed models support two access patterns: **run by name** (`engine.execute("monthly_revenue", variables={...})` runs the stored backing query) and **as a source_model** (`{"source_model": "monthly_revenue", ...}` in another query). Variable precedence: runtime kwarg > stage > outer query > model defaults.

- **SessionPolicy** (Row-Level Security, DEV-1578) — immutable, agent-invisible forced column filter passed at engine/local-client init: `SlayerQueryEngine(storage, policy=SessionPolicy(data_filters=[ColumnFilterRule(column="organization_uuid", value=...)]))`. Silently scopes every query (base, joins, CTEs, sql-mode, query-backed, profiling) to one tenant by wrapping each physical table in a filtered sub-query at the final-SQL layer. Scalar value→`=`, list→`IN`; `on_unapplicable="block"|"pass"` for tables lacking the column; unconfirmable presence fails closed. Local-engine only (HTTP `policy=` raises). See [row-level-security.md](../../docs/concepts/row-level-security.md).

## MCP Tools

Discovery: `list_datasources`, `models_summary`, `inspect` (point lookup by `reference` + required `entity_type`; the model path carries sample data; `reference` accepts a single id, a same-kind **list** for a batched lookup, or **`None`/omitted** for the whole **collection** at that kind — `entity_type="model"` lists all models grouped by datasource, `entity_type="datasource"` lists all datasources, subsuming `models_summary` / `list_datasources`). `inspect_model` is DEPRECATED — use `inspect`.
Querying: `query`, `recommend_root_model` (given `model.column` / `model.metric` items, introspects the join graph to recommend the query `source_model` + each item's join path from it; optional `root_hint` forces an intended root when it reaches every item; returns partial-root `coverage` when no single model reaches all items)
Model editing: `create_model`, `edit_model`, `delete_model`
Datasources: `create_datasource`, `list_datasources`, `describe_datasource` (includes table listing by default), `edit_datasource`, `delete_datasource`, `set_datasource_priority`
Ingestion: `ingest_datasource_models`
Schema drift: `validate_models` (read-only diff against live schema; surfaces `SchemaDriftError` cleanups)
Memory write side: `save_memory`, `forget_memory` (per-entity learnings indexed by canonical entity strings — see [memories.md](../../docs/concepts/memories.md))
Search: `search` (three-channel: entity-overlap BM25 over memory tags + tantivy full-text + optional dense embedding similarity, RRF-fused into a single flat `SearchResponse.results: List[SearchHit]` (DEV-1532) with a `kind` discriminator — `"memory"` / `"datasource"` / `"model"` / `"column"` / `"measure"` / `"aggregation"`; query-bearing memories are still memory hits, distinguished by `hit.query is not None`. Optional `cypher_filter` pre-narrows all three channels: full openCypher when `advanced_search` is installed, naive `MATCH (n:Label) RETURN n.id AS id` kind-filter otherwise. Embeddings also require the `advanced_search` extra and degrade gracefully when unavailable — see [search.md](../../docs/concepts/search.md))

## Package Structure

```
slayer/
  core/       — DataType, SlayerModel, SlayerQuery, formula parser (formula.py), etc.
  sql/        — SQLGenerator, SlayerSQLClient
  engine/     — SlayerQueryEngine, EnrichedQuery, auto-ingestion with rollup joins
  storage/    — YAMLStorage, SQLiteStorage, StorageBackend protocol
  api/        — FastAPI server
  mcp/        — MCP server (FastMCP)
  client/     — Python SDK (remote + local mode)
  cli.py      — CLI entry point (serve, mcp, query, ingest, models, datasources)
```
