# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## What is SLayer?

SLayer (Semantic Layer) is a lightweight, open-source (MIT) semantic layer for AI agents, built by MotleyAI. Instead of writing raw SQL, agents describe what data they want — measures, dimensions, filters — and SLayer generates and executes the query.

Default API port: **5143**.
 
## Common Commands

```bash
# Install with all extras
poetry install -E all

# Run unit tests (excludes integration tests)
poetry run pytest

# Run SQLite integration tests
poetry run pytest tests/integration/test_integration.py -m integration

# Run Postgres integration tests (auto-spawns temp Postgres via pytest-postgresql)
poetry run pytest tests/integration/test_integration_postgres.py -m integration

# Run DuckDB integration tests (no Docker, runs in-process)
poetry run pytest tests/integration/test_integration_duckdb.py -m integration

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
- Dimension/measure SQL uses bare column names (e.g., `"amount"`); use `model_name.column_name` for complex expressions (e.g., `"orders.amount * orders.quantity"`)
- Auto-ingestion generates: numeric non-ID columns get `_sum`, `_avg`, `_min`, `_max`, `_distinct` measures; non-numeric non-ID columns get `_distinct` (COUNT DISTINCT) and `_count` (COUNT non-null) measures; `count` measure always added; joined table PKs get `tablename.count` (COUNT DISTINCT)
- Queries support `fields` — list of `{"formula": "...", "name": "...", "label": "..."}` parsed by `slayer/core/formula.py`. `label` is an optional human-readable display name (also supported on `ColumnRef` and `TimeDimension`)
- Available formula functions: cumsum, time_shift, change, change_pct, rank, last (FIRST_VALUE window), lag, lead. time_shift, change, and change_pct always use self-join CTEs (no edge NULLs, gap-safe). time_shift uses row-number-based join without granularity, date-arithmetic-based with granularity. lag/lead use LAG/LEAD window functions directly (more efficient but produce NULLs at edges)
- Filters can reference computed field names or contain inline transform expressions (e.g., `"change(revenue) > 0"`, `"last(change(revenue)) < 0"`). These are auto-extracted as hidden fields and applied as post-filters on the outer query
- Models can have explicit `joins` to other models (LEFT JOINs). Cross-model measures use dotted syntax (`customers.avg_score`) and multi-hop (`customers.regions.name`). Joins are auto-resolved by walking the join graph. Transforms work on cross-model measures (`cumsum(customers.avg_score)`)
- **Path-based table aliases**: Joined tables use `__`-delimited path aliases in SQL to disambiguate diamond joins. In queries, dots denote paths (`customers.regions.name`); in model SQL definitions, `__` denotes the table alias (`customers__regions.name`). For diamond joins (same table reached via different paths, e.g., `orders → customers → regions` AND `orders → warehouses → regions`), each path gets a unique alias (`customers__regions` vs `warehouses__regions`). Ingestion auto-detects diamond joins via FK graph BFS
- `SlayerQuery.source_model` accepts a model name, inline `SlayerModel`, or `ModelExtension` (extends a model with extra dims/measures/joins). `create_model_from_query()` saves a query as a permanent model
- Models can have `filters` (always-applied WHERE conditions, e.g., `"deleted_at is None"`)
- **Core principle**: adding a measure/field must never affect result cardinality or other fields' values — achieved via CTEs, sub-queries, and correct JOIN dimensions
- Functions needing time ordering: single time_dimensions entry is used automatically; with 2+ time dimensions, `main_time_dimension` disambiguates (or model's `default_time_dimension` if among query's time dims); with none, falls back to model default
- SlayerModel has optional `default_time_dimension` field for time-dependent formula resolution
- SQLite dialect uses STRFTIME instead of DATE_TRUNC (handled automatically by sqlglot)
- See "Database Support" section below for dialect tiers and testing expectations
- Result column keys use `model_name.column_name` format (e.g., `"orders.count"`). For multi-hop joined dimensions, the full path is included: `"orders.customers.regions.name"`
- Datasource configs support `${ENV_VAR}` references resolved at read time
- Integration tests are marked with `@pytest.mark.integration` and skip when DB is unavailable

## Database Support

SLayer uses sqlglot for dialect-aware SQL generation. Databases are supported at two tiers:

**Tier 1 — fully tested** (integration tests + Docker examples, must not regress):
- **SQLite** — integration tests in `tests/integration/test_integration.py`, embedded example
- **Postgres** — integration tests in `tests/integration/test_integration_postgres.py`, Docker example
- **DuckDB** — integration tests in `tests/integration/test_integration_duckdb.py` (no Docker, runs in-process)
- **MySQL** — Docker example with `verify.py`
- **ClickHouse** — Docker example with `verify.py`

**Tier 2 — code-covered** (unit tests for SQL generation, no live instance verification):
- Snowflake, BigQuery, Redshift, Trino/Presto, Databricks/Spark, MS SQL Server, Oracle

Dialect mapping lives in `query_engine.py:_dialect_for_type()`. Dialect-specific SQL lives in `generator.py` — mainly `_build_date_trunc` (SQLite branch) and `_build_time_offset_expr` (date arithmetic for shifted CTEs). Calendar-based time shifts use timestamp offset inside DATE_TRUNC with simple equality joins (no per-dialect join logic). All other SQL differences are handled by sqlglot transpilation. When adding a new dialect: add it to `_dialect_for_type`, add a `_build_time_offset_expr` branch if it doesn't use Postgres-style `INTERVAL`, and add parametrized tests in `TestMultiDialectGeneration`.

## Testing

- Unit tests: `tests/test_models.py`, `test_sql_generator.py`, `test_storage.py`, `test_sqlite_storage.py`, `test_mcp_server.py`
- Integration tests (SQLite): `tests/integration/test_integration.py`
- Integration tests (Postgres): `tests/integration/test_integration_postgres.py` — uses pytest-postgresql (auto-spawns temp Postgres)
- Integration tests (DuckDB): `tests/integration/test_integration_duckdb.py` — uses duckdb directly (no Docker)
- Shared fixtures in `tests/conftest.py`
