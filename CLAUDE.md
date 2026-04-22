# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## What is SLayer?

SLayer (Semantic Layer) is a lightweight, open-source (MIT) semantic layer for AI agents, built by MotleyAI. Instead of writing raw SQL, agents describe what data they want — measures, dimensions, filters — and SLayer generates and executes the query.

Default API port: **5143**.

When generating SLayer query examples or answering questions about SLayer syntax and capabilities, always read the documentation files in `docs/` first (especially `docs/concepts/queries.md`, `docs/concepts/formulas.md`, `docs/concepts/models.md`, and `docs/examples/`) to understand the current syntax and features.

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

# Start API server (uses platform default storage path, override with --storage)
poetry run slayer serve

# Start MCP server
poetry run slayer mcp

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
- Models, dimensions, and measures have an optional `meta: Dict[str, Any]` field for arbitrary user-defined JSON metadata. Persisted in storage, editable via MCP (`edit_model`), HTTP API, and CLI.
- **Measures and aggregations are separate concepts**: Measures are named row-level SQL expressions (e.g., `{name: "revenue", sql: "amount"}`). Aggregations (sum, avg, count, etc.) are specified at query time using **colon syntax**: `"revenue:sum"`, `"*:count"`, `"price:weighted_avg(weight=quantity)"`. Built-in aggregations: sum, avg, min, max, count, count_distinct, first, last, weighted_avg, median. Custom aggregations can be defined at model level in the `aggregations` list.
- **`*:count`** for COUNT(*) — `*` means "all rows", `count` is just a regular aggregation. `col:count` = COUNT(col) for non-nulls. No magic "count" keyword.
- Measures can have `allowed_aggregations` whitelist — validated at model creation and query time
- Auto-ingestion generates one measure per non-ID column (e.g., `{name: "amount", sql: "amount"}`). Old-style `_sum`/`_avg` suffixed measures are no longer generated. `*:count` is always available without a measure definition. Columns named "count" get measure name "count_value" to avoid collision.
- **Backward compat**: old `type` field on measures (e.g., `type: sum`) is still accepted (deprecated). Bare measure names in formulas (e.g., `"revenue"`) work when the measure has a deprecated `type` — emits `DeprecationWarning`.
- Queries support `fields` — list of `{"formula": "...", "name": "...", "label": "..."}` parsed by `slayer/core/formula.py`. `label` is an optional human-readable display name (also supported on `ColumnRef` and `TimeDimension`)
- **Result column naming**: `revenue:sum` → `orders.revenue_sum` (colon becomes underscore). `*:count` → `orders.count` (star-colon prefix stripped). When converting queries to models (`create_model_from_query`), the same colon-to-underscore mapping applies.
- **Response attributes**: `SlayerResponse.attributes` is a `ResponseAttributes` with `.dimensions` and `.measures` dicts, each mapping column alias → `FieldMetadata(label, format)`. Split by type so consumers can distinguish dimension metadata from measure metadata.
- Available formula transforms: cumsum, time_shift, change, change_pct, rank, first (FIRST_VALUE window ASC), last (FIRST_VALUE window DESC), lag, lead. time_shift uses a self-join CTE where the shifted sub-query has the time column expression offset by INTERVAL (calendar-based, gap-safe). change and change_pct are desugared at enrichment time into a hidden time_shift + arithmetic expression. lag/lead use LAG/LEAD window functions directly (more efficient but produce NULLs at edges)
- Filters can reference computed field names or contain inline transform expressions (e.g., `"change(revenue:sum) > 0"`, `"last(change(revenue:sum)) < 0"`). These are auto-extracted as hidden fields and applied as post-filters on the outer query
- Filters support `{variable}` placeholders substituted from `query.variables: Dict[str, Any]`. Values must be str/number, inserted as-is. `{{`/`}}` for literal braces. Undefined variables raise errors.
- Models can have explicit `joins` to other models (LEFT JOINs). Cross-model measures use dotted syntax with colon aggregation (`customers.revenue:sum`) and multi-hop (`customers.regions.name`). Joins are auto-resolved by walking the join graph. Transforms work on cross-model measures (`cumsum(customers.revenue:sum)`)
- **Path-based table aliases**: Joined tables use `__`-delimited path aliases in SQL to disambiguate diamond joins. In queries, dots denote paths (`customers.regions.name`); in model SQL definitions, `__` denotes the table alias (`customers__regions.name`). For diamond joins (same table reached via different paths, e.g., `orders → customers → regions` AND `orders → warehouses → regions`), each path gets a unique alias (`customers__regions` vs `warehouses__regions`). Auto-ingestion creates only direct joins (one per FK on the source table); multi-hop paths are resolved at query time by walking each intermediate model's own joins
- `SlayerQuery.source_model` accepts a model name, inline `SlayerModel`, or `ModelExtension` (extends a model with extra dims/measures/joins). `create_model_from_query()` saves a query as a permanent model
- Models can have `filters` (always-applied WHERE conditions, e.g., `"deleted_at IS NULL"`)
- **Core principle**: adding a measure/field must never affect result cardinality or other fields' values — achieved via CTEs, sub-queries, and correct JOIN dimensions
- Functions needing time ordering: single time_dimensions entry is used automatically; with 2+ time dimensions, `main_time_dimension` disambiguates (or model's `default_time_dimension` if among query's time dims); with none, falls back to model default
- SlayerModel has optional `default_time_dimension` field for time-dependent formula resolution
- SQLite dialect uses STRFTIME instead of DATE_TRUNC (handled automatically by sqlglot)
- See "Database Support" section below for dialect tiers and testing expectations
- Result column keys use `model_name.column_name` format (e.g., `"orders.count"`). For multi-hop joined dimensions, the full path is included: `"orders.customers.regions.name"`
- Datasource configs support `${ENV_VAR}` references resolved at read time
- Integration tests are marked with `@pytest.mark.integration` and skip when DB is unavailable
- NEVER use dataclasses, if you want to use dataclasses, use Pydantic classes instead. 

## Async Architecture

- **Engine is async-first**: `SlayerQueryEngine.execute()` is `async`. Use `execute_sync()` for CLI/notebooks/scripts.
- **Storage backends are async**: All `StorageBackend` methods are `async def`. YAMLStorage uses sync I/O inside async (fast local files). SQLiteStorage uses `asyncio.to_thread`. Future Postgres storage can use true async (asyncpg).
- **SQL client**: Uses native async drivers for Postgres (`asyncpg`) and MySQL (`aiomysql`). Falls back to `asyncio.to_thread` for SQLite, DuckDB, ClickHouse. Connection pools are cached per `SlayerSQLClient` instance.
- **Tests use `pytest-asyncio`** with `asyncio_mode = "auto"` — test functions can be `async def` and `await` directly.
- **Sync wrappers**: `run_sync()` in `async_utils.py` bridges async→sync for CLI and MCP tools. Handles both "no event loop" and "inside Jupyter" cases.

## CLI

- All commands accept `--storage` (directory for YAML, `.db` file for SQLite). Defaults to platform-appropriate path (`~/.local/share/slayer` on Linux, `~/Library/Application Support/slayer` on macOS). Override with `$SLAYER_STORAGE` env var. Legacy `--models-dir` still works.
- `slayer query` supports `--dry-run` (preview SQL) and `--explain` (execution plan, dialect-aware).
- `slayer datasources create-inline` supports `--password-stdin` for secure credential input.
- `slayer datasources test` verifies connectivity.
- MCP `query()` tool has a `format` parameter: `"markdown"` (default), `"json"`, or `"csv"`.

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

Dialect mapping lives in `query_engine.py:_dialect_for_type()`. Dialect-specific SQL lives in `generator.py` — mainly `_build_date_trunc` (SQLite branch) and `_build_time_offset_expr` (date arithmetic for shifted CTEs). Calendar-based time shifts use timestamp offset inside DATE_TRUNC with simple equality joins (no per-dialect join logic). All other SQL differences are handled by sqlglot transpilation. When adding a new dialect: add it to `_dialect_for_type`, add a `_build_time_offset_expr` branch if it doesn't use Postgres-style `INTERVAL`, and add parametrised tests in `TestMultiDialectGeneration`.

## Testing

**Important**: Always use `poetry run` to run tests — this ensures the correct Poetry-managed virtualenv is used (not the system or conda Python).

```bash
# Run ALL tests (unit + integration)
poetry run pytest tests/ -m "integration or not integration" -v

# Run unit tests only (default, excludes integration)
poetry run pytest

# Run all integration tests
poetry run pytest tests/integration/ -m integration

# Run specific integration suite
poetry run pytest tests/integration/test_integration.py -m integration        # SQLite
poetry run pytest tests/integration/test_integration_postgres.py -m integration  # Postgres
poetry run pytest tests/integration/test_integration_duckdb.py -m integration    # DuckDB
```

- Unit tests: `tests/test_models.py`, `test_sql_generator.py`, `test_storage.py`, `test_sqlite_storage.py`, `test_mcp_server.py`
- Integration tests (SQLite): `tests/integration/test_integration.py`
- Integration tests (Postgres): `tests/integration/test_integration_postgres.py` — uses pytest-postgresql (auto-spawns temp Postgres)
- Integration tests (DuckDB): `tests/integration/test_integration_duckdb.py` — uses duckdb directly (no Docker)
- Shared fixtures in `tests/conftest.py`

## Linting

**ALWAYS run the linter at the end of every task and fix any issues before finishing.**

```bash
poetry run ruff check slayer/ tests/
```

To auto-fix fixable issues:
```bash
poetry run ruff check --fix slayer/ tests/
```

## Documentation Requirements

**ALWAYS update documentation when making API or user-facing changes.** Check and update ALL of these locations:

1. **`CLAUDE.md`** — Key Conventions, Async Architecture, CLI, Database Support sections
2. **`docs/`** — concept docs (`models.md`, `queries.md`, `formulas.md`, `ingestion.md`), getting-started guides, reference docs
3. **`.claude/skills/`** — `slayer-query.md`, `slayer-models.md`, `slayer-overview.md`
4. **`docs/configuration/`** — datasources, storage backends

When renaming a field, adding a parameter, or changing response structure, **grep all docs and skills** for the old name and update every occurrence.
