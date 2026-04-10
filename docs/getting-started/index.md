# Getting Started

SLayer is a semantic layer that sits between your database and whatever consumes the data — AI agents, apps, scripts, or dashboards. You define your data model once (or let SLayer auto-generate it), and consumers query using measures, dimensions, and filters instead of writing SQL.

## Which interface is right for you?

| I want to... | Use | Guide |
|---|---|---|
| Connect an AI agent (Claude, Cursor) to my database | **MCP Server** | [MCP Setup](mcp.md) |
| Query from the terminal or scripts | **CLI** | [CLI Setup](cli.md) |
| Build an app that queries data (any language) | **REST API** | [REST API Setup](rest-api.md) |
| Use SLayer as a Python library | **Python SDK** | [Python Setup](python.md) |

All four interfaces use the same query language and the same models — pick the one that fits your workflow. You can use multiple interfaces simultaneously (e.g., MCP for your agent + REST API for your dashboard).

## Supported Databases

SLayer works with most SQL databases. The base install includes SQLite support (no extras needed).

| Database | Install | Status |
|---|---|---|
| SQLite | included | Fully tested |
| PostgreSQL | `motley-slayer[postgres]` | Fully tested |
| MySQL / MariaDB | `motley-slayer[mysql]` | Fully tested |
| ClickHouse | `motley-slayer[clickhouse]` | Fully tested |
| DuckDB | `motley-slayer[duckdb]` | Fully tested |
| Snowflake, BigQuery, Redshift, Trino, Databricks, MS SQL, Oracle | Covered by sqlglot | SQL generation tested |

## Next Steps

After setting up your interface, explore:

- [Terminology](../concepts/terminology.md) — key terms and concepts
- [Models](../concepts/models.md) — define custom dimensions and measures
- [Queries](../concepts/queries.md) — query structure and parameters
- [Formulas](../concepts/formulas.md) — transforms, arithmetic, filters
- [Examples](../examples/01_dynamic/dynamic.md) — interactive notebooks
