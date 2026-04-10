# SLayer — Semantic Layer for AI Agents

A lightweight, open-source semantic layer by [MotleyAI](https://github.com/motleyai). Instead of writing raw SQL, agents describe what data they want — measures, dimensions, filters — and SLayer generates and executes the query.

[GitHub](https://github.com/MotleyAI/slayer) · [PyPI](https://pypi.org/project/motley-slayer/) · [Getting Started](getting-started.md)

## Key Features

- **Agent-first design** — MCP, Python SDK, and REST API interfaces
- **Datasource-agnostic** — first-class support for Postgres, MySQL, ClickHouse, SQLite, and DuckDB; additional support for Snowflake, BigQuery, Oracle, Redshift, and more via sqlglot
- **`fields` API** — derived metrics with formulas, transforms (`cumsum`, `time_shift`, `change`), and inline transform filters
- **Auto-ingestion with rollup joins** — Connect to a DB, introspect schema, generate denormalized models with FK-based LEFT JOINs automatically
- **Incremental model editing** — Add/remove measures and dimensions without replacing the full model
- **Lightweight** — Minimal dependencies, easy to set up and extend

## Quick Install

```bash
pip install motley-slayer[all]
```

## How It Works

```
Agent ─→ MCP / REST API / Python SDK
              │
         SlayerQuery (model, fields, dimensions, filters)
              │
         SlayerQueryEngine (resolves model definitions from storage)
              │
         EnrichedQuery (resolved SQL expressions, model metadata)
              │
         SQLGenerator (sqlglot AST → dialect-aware SQL)
              │
         SlayerSQLClient (SQLAlchemy → database)
              │
         SlayerResponse (data, columns, sql)
```

**SlayerQuery** is the user-facing query — just names and references, no SQL.
**EnrichedQuery** (`slayer/engine/enriched.py`) is the engine-internal form — every measure and dimension carries its fully resolved SQL expression, aggregation type, and model context. This separation means new datasource clients only need to translate EnrichedQuery, not understand model resolution.

## Next Steps

- [Getting Started](getting-started/index.md) — pick your interface and get running in minutes
- [MCP Setup](getting-started/mcp.md) — connect AI agents to your database
- [CLI Setup](getting-started/cli.md) — query from the terminal
- [REST API Setup](getting-started/rest-api.md) — build apps in any language
- [Models](concepts/models.md) — understand dimensions and measures
- [Queries](concepts/queries.md) — query format reference with examples
