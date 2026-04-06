# Development

## Setup

```bash
git clone https://github.com/motleyai/slayer.git
cd slayer
poetry install -E all
```

## Running Tests

```bash
# Unit tests (no database required)
poetry run pytest

# Postgres integration tests (auto-spawns temporary Postgres via pytest-postgresql)
poetry run pytest tests/integration/test_integration_postgres.py -m integration

# DuckDB integration tests (no Docker, runs in-process)
poetry run pytest tests/integration/test_integration_duckdb.py -m integration

# SQLite integration tests
poetry run pytest tests/integration/test_integration.py

# Specific test file
poetry run pytest tests/test_mcp_server.py -v
```

## Linting

```bash
poetry run ruff check slayer/ tests/
```

## Project Structure

```
slayer/
  core/
    enums.py              # DataType, TimeGranularity, OrderDirection
    models.py             # SlayerModel, Dimension, Measure, DatasourceConfig
    query.py              # SlayerQuery, ColumnRef, TimeDimension, OrderItem
    formula.py            # Formula parser (Python ast-based) for `fields` API
  sql/
    generator.py          # SQLGenerator — sqlglot AST-based SQL generation
    client.py             # SlayerSQLClient — SQLAlchemy execution with retry
  engine/
    query_engine.py       # SlayerQueryEngine — central orchestrator
    ingestion.py          # Auto-ingestion with rollup-style FK joins
    enriched.py           # EnrichedQuery — fully resolved query for SQL generation
  storage/
    base.py               # StorageBackend ABC
    yaml_storage.py       # YAML file storage
    sqlite_storage.py     # SQLite storage
  api/server.py           # FastAPI REST API
  mcp/server.py           # MCP server (FastMCP)
  client/slayer_client.py # Python SDK
  cli.py                  # CLI entry point (serve, mcp, query, ingest, models, datasources)

tests/
  test_models.py          # Core model tests
  test_sql_generator.py   # SQL generation tests
  test_storage.py         # YAML storage tests
  test_sqlite_storage.py  # SQLite storage tests
  test_mcp_server.py      # MCP server tool tests
  integration/
    test_integration.py           # SQLite integration tests
    test_integration_postgres.py  # Postgres integration + rollup tests
    test_integration_duckdb.py    # DuckDB integration tests
    test_jaffle_shop_duckdb.py    # Jaffle Shop DuckDB example tests
    test_jaffle_shop_notebook.py  # Jaffle Shop notebook tests
  conftest.py             # Shared fixtures
```

## Key Conventions

- Python 3.11+, Pydantic v2 for all models
- Use `poetry run` for all Python commands
- Use keyword arguments for functions with more than 1 parameter
- SQL generation uses sqlglot AST building (not string concatenation)
- Dimension/measure SQL uses bare column names; `model_name.column_name` for complex expressions
- Result column keys: `model_name.column_name` format
- Integration tests marked with `@pytest.mark.integration`

## Publishing

PyPI publishing is available via GitHub Actions (`Actions → Publish to PyPI → Run workflow`). Choose `testpypi` or `pypi` as target. Uses OIDC trusted publishing — no API tokens required.

Requires configuring the trusted publisher on [pypi.org](https://pypi.org) (Settings → Publishing → Add GitHub provider).
