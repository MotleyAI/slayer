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
- **Ingestion** — auto-generates models from DB schema with rollup-style FK joins (denormalized LEFT JOINs)
- **Interfaces** — MCP server (stdio via `slayer mcp`, SSE via `slayer serve` at `/mcp/sse`), REST API (FastAPI on port 5143), Python SDK

## Key Models

- **SlayerModel** — has one of three source modes: `sql_table` (physical table), `sql` (explicit SQL), or `source_queries` (query-backed — rows are the result of saved `SlayerQuery` stages). Optional `query_variables` defaults for `{var}` placeholders. Engine-managed `columns` and `backing_query_sql` cache for query-backed models. Defined in YAML or auto-generated.
- **SlayerQuery** — specifies model, measures, dimensions, time_dimensions, filters, order, limit, variables
- **DatasourceConfig** — DB connection details with `${ENV_VAR}` resolution

Query-backed models support two access patterns: **run by name** (`engine.execute("monthly_revenue", variables={...})` runs the stored backing query) and **as a source_model** (`{"source_model": "monthly_revenue", ...}` in another query). Variable precedence: runtime kwarg > stage > outer query > model defaults.

## MCP Tools

Discovery: `list_datasources`, `models_summary`, `inspect_model` (with sample data)
Querying: `query`
Model editing: `create_model`, `edit_model`, `delete_model`
Datasources: `create_datasource`, `list_datasources`, `describe_datasource` (includes table listing by default), `edit_datasource`, `delete_datasource`, `set_datasource_priority`
Ingestion: `ingest_datasource_models`
Schema drift: `validate_models` (read-only diff against live schema; surfaces `SchemaDriftError` cleanups)
Memory write side: `save_memory`, `forget_memory` (per-entity learnings indexed by canonical entity strings — see [memories.md](../../docs/concepts/memories.md))
Search: `search` (three-channel: entity-overlap BM25 over memories + tantivy full-text over memories ∪ entities + optional dense embedding similarity, RRF-fused; embeddings require the `embedding_search` extra and degrade gracefully when unavailable; partitions query-bearing memories into `example_queries` — see [search.md](../../docs/concepts/search.md))

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
