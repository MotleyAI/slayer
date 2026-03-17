# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## What is SLayer?

SLayer (Semantic Layer) is a lightweight, open-source (MIT) semantic layer for AI agents, built by MotleyAI. Instead of writing raw SQL, agents describe what data they want — measures, dimensions, filters — and SLayer generates and executes the query.

Default API port: **5143**.

## Package Structure

```
slayer/
  core/           # Domain models and enums
    enums.py      # DataType, TimeGranularity, OrderDirection
    models.py     # SlayerModel (has default_time_dimension), Dimension, Measure, DatasourceConfig
    query.py      # SlayerQuery, ColumnRef, TimeDimension, OrderItem
    formula.py    # Formula parser (Python ast-based) for `fields` API
  sql/            # SQL generation and execution
    generator.py  # SQLGenerator — sqlglot-based, dialect-aware SQL generation
    client.py     # SlayerSQLClient — SQLAlchemy execution with retry
  engine/         # Query orchestration
    query_engine.py  # SlayerQueryEngine — central orchestrator
    ingestion.py     # Auto-ingestion with rollup-style FK joins
    enriched.py      # EnrichedQuery — fully resolved query for SQL generation
  storage/        # Model and datasource persistence
    base.py          # StorageBackend ABC
    yaml_storage.py  # YAMLStorage — files in models/ and datasources/ dirs
    sqlite_storage.py # SQLiteStorage — single SQLite DB
  api/server.py   # FastAPI REST API
  mcp/server.py   # MCP server (FastMCP)
  client/slayer_client.py  # Python SDK (remote + local mode)
  cli.py          # CLI entry point (serve, mcp, query, ingest, models, datasources)
```

## Common Commands

```bash
# Install with all extras
poetry install -E all

# Run unit tests (excludes integration tests)
poetry run pytest

# Run SQLite integration tests
poetry run pytest tests/test_integration.py -m integration

# Run Postgres integration tests (auto-spawns temp Postgres via pytest-postgresql)
poetry run pytest tests/test_integration_postgres.py -m integration

# Run a specific test file
poetry run pytest tests/test_sql_generator.py -v

# Start API server
poetry run slayer serve --models-dir ./slayer_data

# Start MCP server
poetry run slayer mcp --models-dir ./slayer_data

# Lint
poetry run ruff check slayer/ tests/
```

## Key Conventions

- Python 3.11+, Pydantic v2 for all models
- Use `poetry run` for all Python commands
- Use keyword arguments for functions with more than 1 parameter
- Imports at the top of files
- SQL generation uses sqlglot AST building (not string concatenation)
- Dimension/measure SQL uses bare column names (e.g., `"amount"`); `${TABLE}` for complex expressions
- Queries support `fields` — list of `{"formula": "...", "name": "..."}` parsed by `slayer/core/formula.py`
- Available formula functions: cumsum, lag, lead, change, change_pct, rank, last (FIRST_VALUE window), time_shift (self-join CTE by calendar time bucket)
- Functions needing time ordering use resolution chain: query time_dimensions (if exactly 1) -> model default_time_dimension -> error
- SlayerModel has optional `default_time_dimension` field for time-dependent formula resolution
- SQLite dialect uses STRFTIME instead of DATE_TRUNC (handled automatically by sqlglot)
- Result column keys use `model_name.column_name` format (e.g., `"orders.count"`)
- Datasource configs support `${ENV_VAR}` references resolved at read time
- Integration tests are marked with `@pytest.mark.integration` and skip when DB is unavailable

## Testing

- Unit tests: `tests/test_models.py`, `test_sql_generator.py`, `test_storage.py`, `test_sqlite_storage.py`, `test_mcp_server.py`
- Integration tests (SQLite): `tests/test_integration.py`
- Integration tests (Postgres): `tests/test_integration_postgres.py` — uses pytest-postgresql (auto-spawns temp Postgres)
- Shared fixtures in `tests/conftest.py`
