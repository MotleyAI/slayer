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

- **SlayerModel** — maps a table/subquery to dimensions and measures. Defined in YAML or auto-generated.
- **SlayerQuery** — specifies model, fields, dimensions, time_dimensions, filters, order, limit
- **DatasourceConfig** — DB connection details with `${ENV_VAR}` resolution

## MCP Tools

Discovery: `datasource_summary`, `inspect_model` (with sample data)
Querying: `query`
Model editing: `create_model`, `edit_model`, `delete_model`
Datasources: `create_datasource`, `list_datasources`, `describe_datasource`, `list_tables`, `edit_datasource`, `delete_datasource`
Ingestion: `ingest_datasource_models`

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
